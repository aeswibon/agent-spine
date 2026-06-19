use std::sync::{Arc, Mutex};

use serde_json::Value;
use thiserror::Error;

use crate::brain_router::BrainRouter;
use crate::router::RouterAction;
use crate::state::StateError;
use crate::supervisor::Supervisor;
use crate::workflow::NodeKind;
use crate::{ExecutionId, StateSnapshot, Transition, ValidatedWorkflow, WorkflowState};

fn merge_json(a: &mut Value, b: &Value) {
    if a.is_object() && b.is_object() {
        if let (Some(a_obj), Some(b_obj)) = (a.as_object_mut(), b.as_object()) {
            for (k, v) in b_obj {
                if let Some(a_v) = a_obj.get_mut(k) {
                    merge_json(a_v, v);
                } else {
                    a_obj.insert(k.clone(), v.clone());
                }
            }
        }
    } else {
        *a = b.clone();
    }
}

/// Orchestrates the execution of a `ValidatedWorkflow`.
pub struct Executor<S: WorkflowState> {
    workflow: ValidatedWorkflow,
    state_store: Arc<Mutex<S>>,
    supervisor: Supervisor,
    brain: Option<BrainRouter>,
}

impl<S: WorkflowState> Executor<S> {
    /// Create a new executor (without agent-brain).
    pub fn new(
        workflow: ValidatedWorkflow,
        state_store: Arc<Mutex<S>>,
        supervisor: Supervisor,
    ) -> Self {
        Self {
            workflow,
            state_store,
            supervisor,
            brain: None,
        }
    }

    /// Attach agent-brain integration to this executor.
    #[must_use]
    pub fn with_brain(mut self, brain_cwd: Option<std::path::PathBuf>) -> Self {
        let name = self.workflow.definition().name().to_owned();
        self.brain = Some(BrainRouter::new(name, brain_cwd));
        self
    }

    fn append_snapshot(
        store: &Arc<Mutex<S>>,
        snapshot: StateSnapshot,
    ) -> Result<(), ExecutorError> {
        let mut guard = store.lock().map_err(|_| ExecutorError::PoisonedLock)?;
        guard.append(snapshot).map_err(ExecutorError::State)?;
        Ok(())
    }

    fn enrich_from_brain(&mut self, node_name: &str, node_payload: &mut Value) {
        if let Some(brain) = self.brain.as_mut() {
            if let Some(resp) = brain.enrich_payload(node_name, "Agent", None, node_payload) {
                if let Ok(brain_value) = serde_json::to_value(&resp) {
                    if let Some(obj) = node_payload.as_object_mut() {
                        obj.insert("_brain".into(), brain_value);
                    }
                }
            }
        }
    }

    fn log_trajectory(&mut self, exec_id: &str, node_id: &str, outcome: &str) {
        if let Some(brain) = self.brain.as_mut() {
            brain.store_trajectory(exec_id, node_id, outcome, None);
        }
    }

    fn evaluate_brain_route(
        &mut self,
        source: &str,
        target: &str,
        payload: &Value,
    ) -> RouterAction {
        if let Some(brain) = self.brain.as_mut() {
            brain.evaluate_transition(source, target, payload)
        } else {
            RouterAction::Continue
        }
    }

    /// Execute the workflow from start to finish.
    pub async fn run(&mut self, initial_payload: Value) -> Result<ExecutionId, ExecutorError> {
        let execution_id = ExecutionId::new();
        let exec_id_str = execution_id.to_string();
        let workflow_name = self.workflow.definition().name().to_owned();
        tracing::info!("Starting execution of workflow '{}'", workflow_name);

        let mut current_snapshot = StateSnapshot::initial(execution_id, initial_payload);

        Self::append_snapshot(&self.state_store, current_snapshot.clone())?;

        self.log_trajectory(&exec_id_str, "init", "escalated");

        let nodes = self.workflow.definition().nodes().to_vec();
        let edges = self.workflow.definition().edges().to_vec();
        let start_node = self.workflow.definition().start_node().to_owned();

        let mut current_node_names = vec![start_node];

        loop {
            current_node_names.sort();
            current_node_names.dedup();

            // ── Phase 1: Prepare node payloads (with optional brain enrichment) ──
            let mut node_tasks: Vec<NodeTask> = Vec::with_capacity(current_node_names.len());

            for node_name in &current_node_names {
                let node = nodes
                    .iter()
                    .find(|n| n.name() == *node_name)
                    .expect("node must exist in workflow");

                let mut node_payload = current_snapshot.payload().clone();

                if matches!(node.kind(), NodeKind::Agent | NodeKind::Checkpoint) {
                    self.enrich_from_brain(node_name, &mut node_payload);
                }

                node_tasks.push(NodeTask {
                    name: node_name.clone(),
                    kind: node.kind().clone(),
                    retry_policy: node.retry_policy(),
                    description: node.description().map(String::from),
                    payload: node_payload,
                });
            }

            // ── Phase 2: Execute all nodes in parallel ──
            let mut join_set = tokio::task::JoinSet::new();

            for task in node_tasks {
                let supervisor = self.supervisor.clone();
                let workflow_name = self.workflow.definition().name().to_owned();
                tracing::debug!("Spawning task for node '{}'", task.name);

                join_set.spawn(async move {
                    let node_kind = task.kind.to_string();
                    let description = task.description.clone();
                    let next_payload = match task.kind {
                        NodeKind::Agent | NodeKind::Checkpoint => {
                            let mut retries = 0;
                            let max_retries = task.retry_policy.max_attempts;
                            let base_backoff = task.retry_policy.backoff_ms;
                            loop {
                                match supervisor
                                    .delegate(
                                        task.name.clone(),
                                        node_kind.clone(),
                                        description.clone(),
                                        workflow_name.clone(),
                                        task.payload.clone(),
                                        Some(std::time::Duration::from_secs(30)),
                                    )
                                    .await
                                {
                                    Ok(res) => break res,
                                    Err(e) => {
                                        if retries >= max_retries {
                                            tracing::error!(
                                                "Node '{}' failed after {} retries: {}",
                                                task.name,
                                                max_retries,
                                                e
                                            );
                                            return Err(ExecutorError::SupervisorFailed);
                                        }
                                        retries += 1;
                                        let backoff_ms = base_backoff * 2u64.pow(retries);
                                        tracing::warn!(
                                            "Node '{}' failed: {}. Retrying ({}/{}) in {}ms...",
                                            task.name,
                                            e,
                                            retries,
                                            max_retries,
                                            backoff_ms
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(
                                            backoff_ms,
                                        ))
                                        .await;
                                    }
                                }
                            }
                        }
                        NodeKind::Verify => task.payload,
                        NodeKind::ApprovalGate => {
                            let result = supervisor
                                .delegate(
                                    task.name.clone(),
                                    node_kind.clone(),
                                    description.clone(),
                                    workflow_name.clone(),
                                    task.payload,
                                    None,
                                )
                                .await
                                .map_err(|_| ExecutorError::SupervisorFailed)?;

                            if result.get("approved").and_then(Value::as_bool) != Some(true) {
                                tracing::error!(
                                    "Human rejected execution at ApprovalGate '{}'",
                                    task.name
                                );
                                return Err(ExecutorError::ExecutionRejected);
                            }
                            result
                        }
                    };
                    Ok::<_, ExecutorError>((task.name, next_payload))
                });
            }

            // ── Phase 3: Collect results ──
            let mut branch_results = Vec::new();
            while let Some(res) = join_set.join_next().await {
                let (node_name, next_payload) = res.expect("task panicked")?;
                tracing::debug!("Node '{}' resolved", node_name);
                branch_results.push((node_name, next_payload));
            }

            tracing::info!("Fan-in sync complete for level: {:?}", current_node_names);

            // ── Phase 4: Log trajectories for completed nodes ──
            for (node_name, payload) in &branch_results {
                let outcome = if is_failure_payload(payload) {
                    "failure"
                } else {
                    "success"
                };
                self.log_trajectory(&exec_id_str, node_name, outcome);
            }

            // Merge payloads from all parallel branches
            let mut final_payload = current_snapshot.payload().clone();
            for (_, payload) in &branch_results {
                merge_json(&mut final_payload, payload);
            }

            // ── Phase 5: Determine next nodes (with optional brain routing) ──
            let mut next_node_names = Vec::new();
            let mut escalate = false;

            for (node_name, payload) in &branch_results {
                let outgoing: Vec<_> = edges.iter().filter(|e| e.from() == *node_name).collect();

                if outgoing.is_empty() {
                    continue;
                }

                for edge in outgoing {
                    let next_node_name = edge.to().to_owned();

                    match self.evaluate_brain_route(node_name, &next_node_name, payload) {
                        RouterAction::Escalate(target) => {
                            tracing::warn!("Brain Router: Escalating task for node '{}'.", target);
                            escalate = true;
                        }
                        RouterAction::Continue => {}
                    }

                    next_node_names.push(next_node_name);
                }
            }

            if escalate && let Some(obj) = final_payload.as_object_mut() {
                obj.insert("escalation_required".to_string(), Value::Bool(true));
            }

            // ── Phase 6: Persist and advance ──
            if next_node_names.is_empty() {
                tracing::info!("Execution {:?} reached terminal state", execution_id);
                let transition = Transition::new(current_node_names.join(", "), "END");

                current_snapshot = current_snapshot
                    .transition(transition, final_payload)
                    .map_err(|_| ExecutorError::InvalidTransition)?;

                Self::append_snapshot(&self.state_store, current_snapshot)?;

                self.log_trajectory(&exec_id_str, "complete", "success");
                break;
            }

            let transition =
                Transition::new(current_node_names.join(", "), next_node_names.join(", "));

            current_snapshot = current_snapshot
                .transition(transition, final_payload)
                .map_err(|_| ExecutorError::InvalidTransition)?;

            Self::append_snapshot(&self.state_store, current_snapshot.clone())?;

            current_node_names = next_node_names;
        }

        Ok(execution_id)
    }
}

/// A prepared node task with its payload (enriched before spawning).
struct NodeTask {
    name: String,
    kind: crate::workflow::NodeKind,
    retry_policy: crate::workflow::RetryPolicy,
    description: Option<String>,
    payload: Value,
}

/// Check if a payload signals a failed outcome.
fn is_failure_payload(payload: &Value) -> bool {
    payload.get("success").and_then(Value::as_bool) == Some(false)
        || payload.get("error").and_then(|v| v.as_str()).is_some()
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("state storage error: {0}")]
    State(#[from] StateError),
    #[error("invalid transition produced during execution")]
    InvalidTransition,
    #[error("state store lock was poisoned")]
    PoisonedLock,
    #[error("supervisor interaction failed")]
    SupervisorFailed,
    #[error("execution rejected at approval gate")]
    ExecutionRejected,
}
