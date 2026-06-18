use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::oneshot;

/// The Supervisor manages paused graph executions, delegating them to IDE agents.
#[derive(Default, Clone)]
pub struct Supervisor {
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
}

impl Supervisor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Suspend execution and wait for the IDE agent to provide the next payload.
    pub async fn delegate(&self, node_name: String, payload: Value) -> Result<Value, SupervisorError> {
        let (tx, rx) = oneshot::channel();
        
        {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            if pending.contains_key(&node_name) {
                return Err(SupervisorError::AlreadyPending(node_name));
            }
            // In a real system, we might emit an event here (e.g. over IPC)
            // so the IDE knows a task is pending. For now, we just wait.
            pending.insert(node_name.clone(), tx);
        }

        // Wait asynchronously for the IDE agent to provide the result via `.resume()`
        rx.await.map_err(|_| SupervisorError::Dropped(node_name))
    }

    /// Provide the result for a pending task, resuming its execution in the executor.
    pub fn resume(&self, node_name: &str, result: Value) -> Result<(), SupervisorError> {
        let tx = {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            pending.remove(node_name).ok_or_else(|| SupervisorError::NotPending(node_name.to_owned()))?
        };

        tx.send(result).map_err(|_| SupervisorError::Dropped(node_name.to_owned()))
    }

    /// Get a list of currently pending tasks waiting for IDE intervention.
    pub fn pending_tasks(&self) -> Vec<String> {
        self.pending.lock()
            .map(|guard| guard.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("task for node '{0}' is already pending")]
    AlreadyPending(String),
    #[error("no pending task for node '{0}'")]
    NotPending(String),
    #[error("the execution channel for node '{0}' was dropped")]
    Dropped(String),
    #[error("supervisor lock is poisoned")]
    Poisoned,
}
