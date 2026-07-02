use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use regex::Regex;
use serde_json::Value;
use thiserror::Error;
use tracing::Instrument;

use crate::cancellation::CancelToken;

use crate::brain_router::BrainRouter;
use crate::budget_gate::{self, BudgetGate};
use crate::condition;
use crate::router::RouterAction;
use crate::state::StateError;
use crate::supervisor::Supervisor;
use crate::workflow::NodeKind;
use crate::{ExecutionId, StateSnapshot, Transition, ValidatedWorkflow, WorkflowState};

use super::resilience::{self, DlqEntry, exponential_backoff_ms};
use super::{ReadyQueue, Scheduled, SchedulerPlan};

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
    budget_gate: BudgetGate,
    snapshot_config: SnapshotConfig,
    cancel_token: CancelToken,
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
            budget_gate: BudgetGate::from_env(),
            snapshot_config: SnapshotConfig::default(),
            cancel_token: CancelToken::new(),
        }
    }

    /// Attach a budget gate (defaults to `AUTONOMIC_HEART_URL` / `AUTONOMIC_BUDGET_GATE`).
    #[must_use]
    pub fn with_budget_gate(mut self, gate: BudgetGate) -> Self {
        self.budget_gate = gate;
        self
    }

    /// Attach a cancel token for graceful shutdown.
    #[must_use]
    pub fn with_cancel_token(mut self, token: CancelToken) -> Self {
        self.cancel_token = token;
        self
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

    #[tracing::instrument(skip(self, store), fields(seq = snapshot.sequence()))]
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

    #[tracing::instrument(skip(self, node_payload))]
    fn enrich_from_brain(&mut self, node_name: &str, node_kind: &str, node_payload: &mut Value) {
        if let Some(brain) = self.brain.as_mut() {
            let description = self
                .workflow
                .definition()
                .nodes()
                .iter()
                .find(|n| n.name() == node_name)
                .and_then(|n| n.description());

            // Phase 2: Get node-specific context from brain
            let workflow_name = self.workflow.definition().name();
            if let Some(ctx) = brain.get_context_for_node(
                node_kind,
                node_name,
                description,
                workflow_name,
                &format!(
                    "Execute node '{}' (kind={}) in workflow '{}'",
                    node_name, node_kind, workflow_name
                ),
            ) {
                if let Some(obj) = node_payload.as_object_mut() {
                    let rules_json = serde_json::to_value(&ctx.items).unwrap_or_default();
                    obj.insert("_brain_rules".into(), rules_json);
                    obj.insert("_brain_tokens".into(), serde_json::json!(ctx.tokens_used));
                }
            }

            // Also store provenance (existing behavior)
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

    #[tracing::instrument(skip(self))]
    fn log_trajectory(
        &mut self,
        exec_id: &str,
        node_id: &str,
        outcome: &str,
        task_kind: Option<&str>,
        model_used: Option<&str>,
        payload_snapshot: Option<&Value>,
        error_message: Option<&str>,
    ) {
        if let Some(brain) = self.brain.as_mut() {
            brain.store_trajectory_full(
                exec_id,
                node_id,
                outcome,
                task_kind,
                None,
                model_used,
                None,
                payload_snapshot,
                error_message,
            );
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
    #[tracing::instrument(skip(self, initial_payload), fields(workflow = %self.workflow.definition().name()))]
    pub async fn run(&mut self, initial_payload: Value) -> Result<ExecutionId, ExecutorError> {
        let execution_id = ExecutionId::new();
        let exec_id_str = execution_id.to_string();
        let workflow_name = self.workflow.definition().name().to_owned();
        tracing::info!("Starting execution of workflow '{}'", workflow_name);

        let mut current_snapshot = StateSnapshot::initial(execution_id, initial_payload);

        self.prepare_and_append_snapshot(&self.state_store, current_snapshot.clone())?;

        self.log_trajectory(&exec_id_str, "init", "started", None, None, None, None);

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
            // Check for cancellation before each execution cycle
            if self.cancel_token.is_cancelled() {
                tracing::warn!("Execution cancelled, saving final snapshot...");
                let final_transition = Transition::new(current_node_names.join(", "), "CANCELLED");
                let cancelled_payload = {
                    let mut p = current_snapshot.payload().clone();
                    if let Some(obj) = p.as_object_mut() {
                        obj.insert("cancelled".into(), Value::Bool(true));
                    }
                    p
                };
                current_snapshot = current_snapshot
                    .transition(final_transition, cancelled_payload)
                    .map_err(|_| ExecutorError::InvalidTransition)?;
                self.prepare_and_append_snapshot(&self.state_store, current_snapshot)?;
                self.log_trajectory(
                    &exec_id_str,
                    "cancelled",
                    "cancelled",
                    None,
                    None,
                    None,
                    None,
                );
                break;
            }

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
                    NodeKind::Agent
                        | NodeKind::Checkpoint
                        | NodeKind::Router
                        | NodeKind::Debate
                        | NodeKind::Vote
                        | NodeKind::Sandbox
                        | NodeKind::Hydrate
                ) {
                    let node_kind_str = node.kind().to_string();
                    self.enrich_from_brain(node_name, &node_kind_str, &mut node_payload);
                }

                node_tasks.push(NodeTask {
                    name: node_name.clone(),
                    kind: node.kind().clone(),
                    retry_policy: node.retry_policy(),
                    timeout_secs: node.timeout_secs(),
                    description: node.description().map(String::from),
                    escalation_model: node.escalation_model().map(String::from),
                    sandbox_config: node.sandbox_config().cloned(),
                    payload: node_payload,
                });
            }

            // ── Phase 1b: Predictive token budget gate (agent-heart) ──
            for task in &node_tasks {
                if budget_gate::requires_budget_check(&task.kind) {
                    let tokens = budget_gate::estimated_tokens_from_payload(&task.payload);
                    let kind = task.kind.to_string();
                    self.budget_gate
                        .check_node(&kind, tokens)
                        .await
                        .map_err(|e| match e {
                            budget_gate::BudgetGateError::Frozen { reason } => {
                                ExecutorError::BudgetFrozen { reason }
                            }
                        })?;
                }
            }

            // ── Phase 2: Execute nodes by critical-path priority ──
            //
            // Instead of spawning all tasks at once (FIFO-ish), compute a per-node critical-path
            // priority score and dispatch highest-CPP tasks first. This reduces starvation when
            // fast validation nodes share the same runtime pool with slow LLM nodes.
            let node_kinds: Vec<(String, NodeKind)> = node_tasks
                .iter()
                .map(|t| (t.name.clone(), t.kind.clone()))
                .collect();
            let plan = SchedulerPlan::compute(&node_kinds, self.workflow.definition().edges());

            let mut queue = ReadyQueue::new();
            for task in node_tasks {
                let cpp = plan.cpp.get(&task.name).copied().unwrap_or(0);
                queue.push(Scheduled {
                    name: task.name.clone(),
                    cpp,
                    payload: task,
                });
            }

            // Concurrency cap: avoid swamping the runtime with low-value tasks when one heavy
            // critical-path node is runnable.
            let max_in_flight = std::cmp::max(2, num_cpus::get());
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_in_flight));

            let mut join_set = tokio::task::JoinSet::new();
            while let Some(scheduled) = queue.pop() {
                let task = scheduled.payload;
                let supervisor = self.supervisor.clone();
                let workflow_name = self.workflow.definition().name().to_owned();
                let exec_id_for_dlq = exec_id_str.clone();
                tracing::debug!(
                    "Spawning task for node '{}' (cpp={})",
                    task.name,
                    scheduled.cpp
                );

                let permit = semaphore.clone().acquire_owned().await.expect("semaphore");

                let span = tracing::info_span!("node_exec", node = %task.name, kind = %task.kind);
                join_set.spawn(
                    async move {
                        let _permit = permit;
                        let node_kind = task.kind.to_string();
                        let description = task.description.clone();
                        let next_payload = match task.kind {
                            NodeKind::Agent
                            | NodeKind::Checkpoint
                            | NodeKind::Router
                            | NodeKind::Hydrate
                            | NodeKind::Debate
                            | NodeKind::Vote => {
                                let mut retries = 0;
                                let max_retries = task.retry_policy.max_attempts;
                                let base_backoff = task.retry_policy.backoff_ms;
                                let node_timeout =
                                    std::time::Duration::from_secs(task.timeout_secs);
                                let circuit = resilience::CircuitBreaker::shared(5);
                                let escalation_model = task.escalation_model.clone();
                                loop {
                                    let mut payload = task.payload.clone();
                                    // Inject escalation model if this is an escalation retry
                                    if retries > max_retries {
                                        if let Some(ref model) = escalation_model {
                                            if let Some(obj) = payload.as_object_mut() {
                                                obj.insert(
                                                    "_escalation_model".into(),
                                                    Value::String(model.clone()),
                                                );
                                            }
                                            tracing::warn!(
                                                "Escalating '{}' to model '{}'",
                                                task.name,
                                                model,
                                            );
                                        } else {
                                            tracing::error!(
                                                "Node '{}' failed after {} retries (no escalation model)",
                                                task.name,
                                                max_retries,
                                            );
                                            return Err(ExecutorError::SupervisorFailed);
                                        }
                                    }

                                    // For Debate/Vote/Sandbox, inject node-kind hints
                                    if task.kind == NodeKind::Debate {
                                        if let Some(obj) = payload.as_object_mut() {
                                            obj.insert(
                                                "_node_role".into(),
                                                Value::String("debate_coder".into()),
                                            );
                                        }
                                    } else if task.kind == NodeKind::Vote {
                                        if let Some(obj) = payload.as_object_mut() {
                                            obj.insert(
                                                "_node_role".into(),
                                                Value::String("vote".into()),
                                            );
                                        }
                                    } else if task.kind == NodeKind::Sandbox {
                                        if let Some(obj) = payload.as_object_mut() {
                                            obj.insert(
                                                "_node_role".into(),
                                                Value::String("sandbox".into()),
                                            );
                                        }
                                    }

                                    if circuit.is_open() {
                                        tracing::error!(
                                            "Node '{}' circuit open — skipping execution",
                                            task.name
                                        );
                                        return Err(ExecutorError::SupervisorFailed);
                                    }

                                    match supervisor
                                        .delegate(
                                            task.name.clone(),
                                            node_kind.clone(),
                                            description.clone(),
                                            workflow_name.clone(),
                                            payload,
                                            Some(node_timeout),
                                        )
                                        .await
                                    {
                                        Ok(res) => {
                                            circuit.record_success();
                                            break res;
                                        }
                                        Err(e) => {
                                            if circuit.record_failure() {
                                                tracing::error!(
                                                    "Node '{}' circuit opened after repeated failures",
                                                    task.name
                                                );
                                            }
                                            if retries >= max_retries
                                                && escalation_model.is_none()
                                            {
                                                tracing::error!(
                                                    "Node '{}' failed after {} retries: {}",
                                                    task.name,
                                                    max_retries,
                                                    e
                                                );
                                                let dlq = DlqEntry {
                                                    execution_id: exec_id_for_dlq.clone(),
                                                    workflow_name: workflow_name.clone(),
                                                    node_name: task.name.clone(),
                                                    node_kind: node_kind.clone(),
                                                    error: e.to_string(),
                                                    attempts: retries,
                                                    payload: task.payload.clone(),
                                                    failed_at: chrono::Utc::now().to_rfc3339(),
                                                };
                                                let _ = resilience::publish_dlq(&dlq).await;
                                                return Err(ExecutorError::SupervisorFailed);
                                            }
                                            if retries > max_retries {
                                                tracing::error!(
                                                    "Node '{}' failed after escalation retry: {}",
                                                    task.name,
                                                    e
                                                );
                                                return Err(ExecutorError::SupervisorFailed);
                                            }
                                            retries += 1;
                                            let backoff_ms =
                                                exponential_backoff_ms(base_backoff, retries);
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
                            NodeKind::Sandbox => {
                                let cmd = task
                                    .payload
                                    .get("_sandbox_command")
                                    .and_then(Value::as_str)
                                    .unwrap_or("echo 'no command provided'");
                                let image = task
                                    .sandbox_config
                                    .as_ref()
                                    .map(|c| c.image.clone())
                                    .unwrap_or_else(|| "ubuntu:24.04".into());
                                let duration =
                                    std::time::Duration::from_secs(task.timeout_secs);
                                let workdir = std::env::current_dir().ok();
                                const DEFAULT_MEM_MB: u32 = 256;
                                const DEFAULT_CPU: f32 = 1.0;
                                let sandbox_result = {
                                    #[cfg(feature = "nats")]
                                    {
                                        if agent_body_core::default_nats_url().is_some() {
                                            crate::async_sandbox::run_sandbox_via_jetstream(
                                                "",
                                                cmd,
                                                workdir.as_deref(),
                                                duration,
                                                Some(DEFAULT_MEM_MB),
                                                Some(DEFAULT_CPU),
                                                Some("subprocess"),
                                            )
                                            .await
                                        } else {
                                            crate::sandbox::run_sandbox_hardened(
                                                cmd,
                                                &image,
                                                duration,
                                                workdir.as_deref(),
                                                DEFAULT_MEM_MB,
                                                DEFAULT_CPU,
                                            )
                                            .await
                                        }
                                    }
                                    #[cfg(not(feature = "nats"))]
                                    {
                                        crate::sandbox::run_sandbox_hardened(
                                            cmd,
                                            &image,
                                            duration,
                                            workdir.as_deref(),
                                            DEFAULT_MEM_MB,
                                            DEFAULT_CPU,
                                        )
                                        .await
                                    }
                                };
                                match sandbox_result {
                                    Ok(result) => {
                                        let mut p = task.payload.clone();
                                        if let Some(obj) = p.as_object_mut() {
                                            obj.insert(
                                                "_sandbox_stdout".into(),
                                                Value::String(result.stdout),
                                            );
                                            obj.insert(
                                                "_sandbox_stderr".into(),
                                                Value::String(result.stderr),
                                            );
                                            obj.insert(
                                                "_sandbox_exit_code".into(),
                                                Value::Number(
                                                    serde_json::Number::from(result.exit_code),
                                                ),
                                            );
                                        }
                                        p
                                    }
                                    Err(e) => {
                                        let mut p = task.payload.clone();
                                        if let Some(obj) = p.as_object_mut() {
                                            obj.insert(
                                                "_sandbox_error".into(),
                                                Value::String(e),
                                            );
                                        }
                                        p
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
                    }
                    .instrument(span),
                );
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
                let model_used = payload.get("_model").and_then(|v| v.as_str());
                self.log_trajectory(
                    &exec_id_str,
                    node_name,
                    outcome,
                    node_kind.as_deref(),
                    model_used,
                    Some(payload),
                    None,
                );
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

                self.log_trajectory(&exec_id_str, "complete", "success", None, None, None, None);
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
    timeout_secs: u64,
    description: Option<String>,
    escalation_model: Option<String>,
    sandbox_config: Option<crate::workflow::SandboxConfig>,
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
    #[error("execution cancelled by signal")]
    Cancelled,
    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },
    #[error("token budget frozen: {reason}")]
    BudgetFrozen { reason: String },
}
