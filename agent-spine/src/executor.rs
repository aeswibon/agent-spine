use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use regex::Regex;
use serde_json::Value;
use thiserror::Error;

use crate::brain_router::BrainRouter;
use crate::condition;
use crate::router::RouterAction;
use crate::state::StateError;
use crate::supervisor::Supervisor;
use crate::workflow::NodeKind;
use crate::{ExecutionId, StateSnapshot, Transition, ValidatedWorkflow, WorkflowState};

/// Configuration for snapshot payload limits and secrets redaction.
#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    /// Maximum payload size in bytes before truncation or rejection.
    /// `0` means unlimited (default).
    pub max_payload_bytes: usize,
    /// Field name patterns (substring match) to redact from payload at write time.
    /// The field value is replaced with `"[REDACTED]"`.
    pub secrets_redact: Vec<String>,
    /// Pre-compiled regex patterns derived from `secrets_redact`.
    redact_regexes: Vec<Regex>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 0,
            secrets_redact: vec![
                "api_key".into(),
                "token".into(),
                "password".into(),
                "secret".into(),
                "private_key".into(),
                "authorization".into(),
            ],
            redact_regexes: Vec::new(),
        }
    }
}

impl SnapshotConfig {
    /// Build config and pre-compile redaction regexes.
    #[must_use]
    pub fn new(max_payload_bytes: usize, secrets_redact: Vec<String>) -> Self {
        let mut config = Self {
            max_payload_bytes,
            secrets_redact,
            redact_regexes: Vec::new(),
        };
        config.compile_regexes();
        config
    }

    fn compile_regexes(&mut self) {
        self.redact_regexes = self
            .secrets_redact
            .iter()
            .filter_map(|p| Regex::new(&format!("(?i){}", regex::escape(p))).ok())
            .collect();
    }

    /// Redact matching fields in a payload value (mutates in place).
    pub fn redact(&self, value: &mut Value) {
        match value {
            Value::Object(map) => {
                let mut to_redact: Vec<String> = Vec::new();
                for key in map.keys() {
                    for re in &self.redact_regexes {
                        if re.is_match(key) {
                            to_redact.push(key.clone());
                            break;
                        }
                    }
                }
                for key in &to_redact {
                    if let Some(v) = map.get_mut(key) {
                        *v = Value::String("[REDACTED]".into());
                    }
                }
                for v in map.values_mut() {
                    self.redact(v);
                }
            }
            Value::Array(arr) => {
                for v in arr.iter_mut() {
                    self.redact(v);
                }
            }
            _ => {}
        }
    }

    /// Check whether payload exceeds the configured limit.
    /// Returns `None` if within bounds (or unlimited).
    pub fn check_payload_size(&self, payload: &Value) -> Option<ExecutorError> {
        if self.max_payload_bytes == 0 {
            return None;
        }
        let size = serde_json::to_string(payload).map(|s| s.len()).unwrap_or(0);
        if size > self.max_payload_bytes {
            return Some(ExecutorError::PayloadTooLarge {
                size,
                max: self.max_payload_bytes,
            });
        }
        None
    }
}

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
    snapshot_config: SnapshotConfig,
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
            snapshot_config: SnapshotConfig::default(),
        }
    }

    /// Attach agent-brain integration to this executor.
    #[must_use]
    pub fn with_brain(mut self, brain_cwd: Option<std::path::PathBuf>) -> Self {
        let name = self.workflow.definition().name().to_owned();
        self.brain = Some(BrainRouter::new(name, brain_cwd));
        self
    }

    /// Set snapshot configuration (payload limits, secrets redaction).
    #[must_use]
    pub fn with_snapshot_config(mut self, config: SnapshotConfig) -> Self {
        self.snapshot_config = config;
        self
    }

    fn prepare_and_append_snapshot(
        &self,
        store: &Arc<Mutex<S>>,
        mut snapshot: StateSnapshot,
    ) -> Result<(), ExecutorError> {
        // Apply secrets redaction before persisting
        let payload = snapshot.payload_mut();
        self.snapshot_config.redact(payload);

        // Check payload size limits
        if let Some(err) = self.snapshot_config.check_payload_size(payload) {
            return Err(err);
        }

        let mut guard = store.lock().map_err(|_| ExecutorError::PoisonedLock)?;
        guard.append(snapshot).map_err(ExecutorError::State)?;
        Ok(())
    }

    fn enrich_from_brain(&mut self, node_name: &str, node_kind: &str, node_payload: &mut Value) {
        if let Some(brain) = self.brain.as_mut() {
            let description = self
                .workflow
                .definition()
                .nodes()
                .iter()
                .find(|n| n.name() == node_name)
                .and_then(|n| n.description());
            if let Some(provenance) =
                brain.get_provenance(node_name, node_kind, description, node_payload)
            {
                if let Ok(prov_value) = serde_json::to_value(&provenance) {
                    if let Some(obj) = node_payload.as_object_mut() {
                        obj.insert("_brain_provenance".into(), prov_value);
                    }
                }
            }
        }
    }

    fn log_trajectory(
        &mut self,
        exec_id: &str,
        node_id: &str,
        outcome: &str,
        task_kind: Option<&str>,
    ) {
        if let Some(brain) = self.brain.as_mut() {
            brain.store_trajectory_full(exec_id, node_id, outcome, task_kind, None);
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

        self.prepare_and_append_snapshot(&self.state_store, current_snapshot.clone())?;

        self.log_trajectory(&exec_id_str, "init", "started", None);

        let nodes = self.workflow.definition().nodes().to_vec();
        let edges = self.workflow.definition().edges().to_vec();
        let start_node = self.workflow.definition().start_node().to_owned();

        // Pre-compute incoming edge counts for Join nodes (barrier tracking)
        let mut join_incoming: HashMap<String, usize> = HashMap::new();
        for edge in &edges {
            if let Some(to_node) = nodes.iter().find(|n| n.name() == edge.to()) {
                if matches!(to_node.kind(), NodeKind::Join) {
                    *join_incoming.entry(edge.to().to_owned()).or_insert(0) += 1;
                }
            }
        }
        let mut join_completed: HashMap<String, usize> = HashMap::new();

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

                if matches!(
                    node.kind(),
                    NodeKind::Agent | NodeKind::Checkpoint | NodeKind::Router
                ) {
                    let node_kind_str = node.kind().to_string();
                    self.enrich_from_brain(node_name, &node_kind_str, &mut node_payload);
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
                        NodeKind::Agent | NodeKind::Checkpoint | NodeKind::Router => {
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
                        NodeKind::Fork | NodeKind::Join => task.payload,
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
                let node_kind = nodes
                    .iter()
                    .find(|n| n.name() == *node_name)
                    .map(|n| n.kind().to_string());
                self.log_trajectory(&exec_id_str, node_name, outcome, node_kind.as_deref());
            }

            // Merge payloads from all parallel branches
            let mut final_payload = current_snapshot.payload().clone();
            for (_, payload) in &branch_results {
                merge_json(&mut final_payload, payload);
            }

            // ── Phase 5: Determine next nodes (condition eval, brain routing, Join barriers) ──
            let mut next_node_names = Vec::new();
            let mut escalate = false;

            for (node_name, payload) in &branch_results {
                let outgoing: Vec<_> = edges.iter().filter(|e| e.from() == *node_name).collect();

                if outgoing.is_empty() {
                    continue;
                }

                for edge in outgoing {
                    let next_node_name = edge.to().to_owned();

                    // Evaluate conditional edges — skip if condition is false
                    if let Some(cond) = edge.condition() {
                        if !condition::evaluate(cond, payload) {
                            tracing::debug!(
                                "Skipping edge '{}' -> '{}': condition '{cond}' is false",
                                node_name,
                                next_node_name,
                            );
                            continue;
                        }
                    }

                    match self.evaluate_brain_route(node_name, &next_node_name, payload) {
                        RouterAction::Escalate(target) => {
                            tracing::warn!("Brain Router: Escalating task for node '{}'.", target);
                            escalate = true;
                        }
                        RouterAction::Continue => {}
                    }

                    // Join barrier: only schedule Join when all incoming edges complete
                    if let Some(expected) = join_incoming.get(&next_node_name) {
                        let completed = join_completed.entry(next_node_name.clone()).or_insert(0);
                        *completed += 1;
                        if *completed < *expected {
                            tracing::debug!(
                                "Join '{}' waiting: {}/{} branches completed",
                                next_node_name,
                                completed,
                                expected,
                            );
                            continue;
                        }
                        tracing::info!(
                            "Join '{}' barrier satisfied: all {} branches complete",
                            next_node_name,
                            expected,
                        );
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

                self.prepare_and_append_snapshot(&self.state_store, current_snapshot)?;

                self.log_trajectory(&exec_id_str, "complete", "success", None);
                break;
            }

            let transition =
                Transition::new(current_node_names.join(", "), next_node_names.join(", "));

            current_snapshot = current_snapshot
                .transition(transition, final_payload)
                .map_err(|_| ExecutorError::InvalidTransition)?;

            self.prepare_and_append_snapshot(&self.state_store, current_snapshot.clone())?;

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
    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },
}
