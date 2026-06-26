use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::sync::Semaphore;
use tracing;

use crate::ExecutionId;
use crate::executor::Executor;
use crate::state::SqliteStateStore;
use crate::supervisor::Supervisor;
use crate::workflow::{ValidatedWorkflow, WorkflowDefinition};

/// Tracks the live state of a running workflow execution in serve mode.
#[derive(Clone, Debug)]
pub struct RunningWorkflow {
    pub execution_id: String,
    pub workflow_name: String,
    pub status: String,
    pub current_nodes: Vec<String>,
    pub created_at: String,
}

/// Manages lifecycle of in-process workflow executions for the gRPC server.
///
/// The `WorkflowManager` owns the `Supervisor` and spawns executor tasks.
/// External callers (gRPC handlers, dashboard) query status and subscribe
/// to events through the supervisor. Each execution uses its own SQLite
/// connection from the same database file.
///
/// Concurrency is limited by a `tokio::sync::Semaphore`. When `max_concurrent`
/// executions are in flight, `submit()` returns immediately with a "queued"
/// status.
pub struct WorkflowManager {
    pub supervisor: Supervisor,
    executions: Arc<Mutex<HashMap<String, RunningWorkflow>>>,
    db_path: PathBuf,
    brain_enabled: bool,
    semaphore: Arc<Semaphore>,
    max_concurrent: usize,
}

impl Clone for WorkflowManager {
    fn clone(&self) -> Self {
        Self {
            supervisor: self.supervisor.clone(),
            executions: Arc::clone(&self.executions),
            db_path: self.db_path.clone(),
            brain_enabled: self.brain_enabled,
            semaphore: Arc::clone(&self.semaphore),
            max_concurrent: self.max_concurrent,
        }
    }
}

impl WorkflowManager {
    /// Create a new workflow manager with default concurrency (1000 max).
    pub fn new(db_path: PathBuf, brain_enabled: bool) -> Self {
        Self::with_concurrency_limit(db_path, brain_enabled, 1000)
    }

    /// Create a new workflow manager with a max concurrency limit.
    ///
    /// When `max_concurrent` executions are in flight, new submissions
    /// are queued (status: "queued") and started as slots open up.
    pub fn with_concurrency_limit(
        db_path: PathBuf,
        brain_enabled: bool,
        max_concurrent: usize,
    ) -> Self {
        let supervisor = Supervisor::new();
        Self {
            supervisor,
            executions: Arc::new(Mutex::new(HashMap::new())),
            db_path,
            brain_enabled,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
        }
    }

    /// Return the configured max concurrency limit.
    pub fn concurrency_limit(&self) -> usize {
        self.max_concurrent
    }

    /// Return the current number of in-flight (running) executions.
    pub fn in_flight_count(&self) -> usize {
        self.max_concurrent
            .saturating_sub(self.semaphore.available_permits())
    }

    /// Submit a workflow YAML for execution.
    ///
    /// Returns the execution ID immediately (execution runs in background).
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML cannot be parsed or validated.
    pub fn submit_yaml(
        &self,
        yaml_content: &str,
        initial_payload: Value,
    ) -> Result<String, String> {
        let def = WorkflowDefinition::from_yaml(yaml_content)
            .map_err(|e| format!("failed to parse workflow: {e}"))?;
        let validated = def
            .validate()
            .map_err(|e| format!("invalid workflow: {e}"))?;
        self.submit(validated, initial_payload)
    }

    /// Submit a validated workflow for execution.
    ///
    /// When the concurrency limit is reached, the workflow is queued and
    /// started as soon as a slot becomes available.
    pub fn submit(
        &self,
        workflow: ValidatedWorkflow,
        initial_payload: Value,
    ) -> Result<String, String> {
        let name = workflow.definition().name().to_owned();
        let execution_id = ExecutionId::new();
        let exec_id_str = execution_id.to_string();
        let cloned_id = exec_id_str.clone();
        let supervisor = self.supervisor.clone();
        let semaphore = Arc::clone(&self.semaphore);

        let exec_store = match SqliteStateStore::new(&self.db_path) {
            Ok(s) => Arc::new(Mutex::new(s)),
            Err(e) => return Err(format!("failed to open state store: {e}")),
        };

        let mut executor = Executor::new(workflow, exec_store, supervisor.clone());
        if self.brain_enabled {
            executor = executor.with_brain(None);
        }

        let running = RunningWorkflow {
            execution_id: cloned_id.clone(),
            workflow_name: name.clone(),
            status: "queued".to_owned(),
            current_nodes: Vec::new(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        {
            let mut execs = self.executions.lock().unwrap();
            execs.insert(cloned_id.clone(), running);
        }

        // Spawn the workflow execution — waits for semaphore permit
        let execs = Arc::clone(&self.executions);
        tokio::spawn(async move {
            // Acquire a concurrency slot (waits if limit reached)
            let _permit = semaphore.acquire().await;

            {
                if let Ok(mut guard) = execs.lock() {
                    if let Some(entry) = guard.get_mut(&cloned_id) {
                        entry.status = "running".to_owned();
                    }
                }
            }

            tracing::info!("Workflow '{}' started (execution_id: {})", name, cloned_id);

            match executor.run(initial_payload).await {
                Ok(_id) => {
                    tracing::info!("Workflow '{}' completed", name);
                    if let Ok(mut guard) = execs.lock() {
                        if let Some(entry) = guard.get_mut(&cloned_id) {
                            entry.status = "completed".to_owned();
                        }
                    }
                    supervisor.emit(crate::supervisor::WorkflowEvent::WorkflowCompleted {
                        execution_id: cloned_id.clone(),
                        workflow_name: name,
                    });
                }
                Err(e) => {
                    tracing::error!("Workflow '{}' failed: {e}", name);
                    if let Ok(mut guard) = execs.lock() {
                        if let Some(entry) = guard.get_mut(&cloned_id) {
                            entry.status = "failed".to_owned();
                            entry.current_nodes = Vec::new();
                        }
                    }
                    supervisor.emit(crate::supervisor::WorkflowEvent::WorkflowFailed {
                        execution_id: cloned_id.clone(),
                        workflow_name: name.clone(),
                        error: e.to_string(),
                    });
                }
            }
        });

        Ok(exec_id_str)
    }

    /// List all executions (running, queued, completed, or failed).
    pub fn list_executions(&self) -> Vec<RunningWorkflow> {
        let guard = self.executions.lock().unwrap();
        guard.values().cloned().collect()
    }

    /// Get the status of a specific execution.
    pub fn execution_status(&self, execution_id: &str) -> Option<RunningWorkflow> {
        let guard = self.executions.lock().unwrap();
        guard.get(execution_id).cloned()
    }
}
