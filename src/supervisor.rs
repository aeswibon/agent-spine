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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Suspend execution and wait for the IDE agent to provide the next payload.
    #[tracing::instrument(skip(self, _payload), fields(node = %node_name))]
    pub async fn delegate(
        &self,
        node_name: String,
        _payload: Value,
    ) -> Result<Value, SupervisorError> {
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            if pending.contains_key(&node_name) {
                return Err(SupervisorError::AlreadyPending(node_name));
            }
            pending.insert(node_name.clone(), tx);
            tracing::info!("Agent task suspended and waiting for delegation result");
        }

        // Wait asynchronously with a 30s timeout
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx).await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(_)) => {
                tracing::warn!("Agent channel dropped");
                Err(SupervisorError::Dropped(node_name))
            }
            Err(_) => {
                tracing::warn!("Agent task timed out after 30 seconds");
                if let Ok(mut pending) = self.pending.lock() {
                    pending.remove(&node_name);
                }
                Err(SupervisorError::Timeout(node_name))
            }
        }
    }

    /// Provide the result for a pending task, resuming its execution in the executor.
    #[tracing::instrument(skip(self, result), fields(node = %node_name))]
    pub fn resume(&self, node_name: &str, result: Value) -> Result<(), SupervisorError> {
        let tx = {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            pending
                .remove(node_name)
                .ok_or_else(|| SupervisorError::NotPending(node_name.to_owned()))?
        };

        tracing::info!("Resuming agent task with external result");
        tx.send(result)
            .map_err(|_| SupervisorError::Dropped(node_name.to_owned()))
    }

    /// Get a list of currently pending tasks waiting for IDE intervention.
    #[must_use]
    pub fn pending_tasks(&self) -> Vec<String> {
        self.pending
            .lock()
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
    #[error("the execution channel for node '{0}' timed out")]
    Timeout(String),
    #[error("supervisor lock is poisoned")]
    Poisoned,
}
