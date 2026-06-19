use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use thiserror::Error;

use crate::brain_router::BrainRouter;
use crate::router::{ConfidenceRouter, RouterAction};
use crate::state::StateError;
use crate::supervisor::Supervisor;
use crate::workflow::NodeKind;
use crate::{ExecutionId, StateSnapshot, Transition, ValidatedWorkflow, WorkflowState};

/// Helper to deeply merge two JSON values.
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
    #[allow(dead_code)]
    router: ConfidenceRouter,
    brain: BrainRouter,
}

impl<S: WorkflowState> Executor<S> {
    /// Create a new executor for the given workflow (without agent-brain).
    pub fn new(
        workflow: ValidatedWorkflow,
        state_store: Arc<Mutex<S>>,
        supervisor: Supervisor,
        router: ConfidenceRouter,
    ) -> Self {
        let workflow_name = workflow.definition().name().to_owned();
        Self {
            workflow,
            state_store,
            supervisor,
            router,
            brain: BrainRouter::new(workflow_name, None),
        }
    }

    /// Create a new executor with agent-brain integration.
    pub fn with_brain(
        workflow: ValidatedWorkflow,
        state_store: Arc<Mutex<S>>,
        supervisor: Supervisor,
        router: ConfidenceRouter,
        brain_cwd: Option<PathBuf>,
    ) -> Self {
        let workflow_name = workflow.definition().name().to_owned();
        Self {
            workflow,
            state_store,
            supervisor,
            router,
            brain: BrainRouter::new(workflow_name, brain_cwd),
        }
    }

    /// Non-async helper to append a snapshot — avoids `MutexGuard` across await boundaries.
    fn append_snapshot(
        store: &Arc<Mutex<S>>,
        snapshot: StateSnapshot,
    ) -> Result<(), ExecutorError> {
        let mut guard = store.lock().map_err(|_| ExecutorError::PoisonedLock)?;
        guard.append(snapshot).map_err(ExecutorError::State)?;
        Ok(())
    }

    /// Execute the workflow from start to finish.
    pub async fn run(&mut self, initial_payload: Value) -> Result<ExecutionId, ExecutorError> {
        let execution_id = ExecutionId::new();
        let exec_id_str = execution_id.to_string();
        tracing::info!(
            "Starting execution of workflow '{}'",
            self.workflow.definition().name()
        );

        let mut current_snapshot = StateSnapshot::initial(execution_id, initial_payload);

        // Persist initial state (non-async to avoid MutexGuard Send issue)
        Self::append_snapshot(&self.state_store, current_snapshot.clone())?;

        // Log execution start trajectory
        self.brain
            .store_trajectory(&exec_id_str, "init", "escalated", None)
            .await;

        let definition = self.workflow.definition();
        let nodes = definition.nodes();
        let edges = definition.edges();

        let mut current_node_names = vec![definition.start_node().to_owned()];

        // State machine execution
        loop {
            // Deduplicate to prevent double-execution of the same node in a level
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

                // Enrich payload with brain recommendations before delegation
                if matches!(node.kind(), NodeKind::Agent | NodeKind::Checkpoint)
                    && let Some(resp) = self
                        .brain
                        .enrich_payload(node_name, "Agent", node.description(), &node_payload)
                        .await
                    && let Some(obj) = node_payload.as_object_mut()
                    && let Ok(brain_value) = serde_json::to_value(&resp)
                {
                    obj.insert("_brain".to_string(), brain_value);
                }

                node_tasks.push(NodeTask {
                    name: node_name.clone(),
                    kind: node.kind().clone(),
                    retry_policy: node.retry_policy(),
                    payload: node_payload,
                });
            }

            // ── Phase 2: Execute all nodes in parallel ──
            let mut join_set = tokio::task::JoinSet::new();

            for task in node_tasks {
                let supervisor = self.supervisor.clone();
                tracing::debug!("Spawning task for node '{}'", task.name);

                join_set.spawn(async move {
                    let next_payload = match task.kind {
                        NodeKind::Agent | NodeKind::Checkpoint => {
                            let mut retries = 0;
                            let max_retries = task.retry_policy.max_attempts;
                            let base_backoff = task.retry_policy.backoff_ms;
                            loop {
                                match supervisor
                                    .delegate(
                                        task.name.clone(),
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
                                .delegate(task.name.clone(), task.payload, None)
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
                self.brain
                    .store_trajectory(&exec_id_str, node_name, outcome, None)
                    .await;
            }

            // Merge payloads from all parallel branches
            let mut final_payload = current_snapshot.payload().clone();
            for (_, payload) in &branch_results {
                merge_json(&mut final_payload, payload);
            }

            // ── Phase 5: Determine next nodes (with brain routing) ──
            let mut next_node_names = Vec::new();
            let mut escalate = false;

            for (node_name, payload) in &branch_results {
                let outgoing: Vec<_> = edges.iter().filter(|e| e.from() == *node_name).collect();

                if outgoing.is_empty() {
                    continue;
                }

                for edge in outgoing {
                    let next_node_name = edge.to().to_owned();

                    // Use BrainRouter first, fallback to ConfidenceRouter
                    match self
                        .brain
                        .evaluate_transition(node_name, &next_node_name, payload)
                        .await
                    {
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

                self.brain
                    .store_trajectory(&exec_id_str, "complete", "success", None)
                    .await;
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
