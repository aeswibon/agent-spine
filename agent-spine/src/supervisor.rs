use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::{broadcast, oneshot};

/// Events emitted during workflow execution for IDE/UI consumption.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkflowEvent {
    /// A node has started executing.
    NodeStarted {
        node_name: String,
        node_kind: String,
        description: Option<String>,
        workflow_name: String,
    },
    /// A node completed successfully.
    NodeCompleted {
        node_name: String,
        node_kind: String,
    },
    /// A node failed.
    NodeFailed {
        node_name: String,
        node_kind: String,
        error: String,
    },
    /// A node is waiting for human intervention (ApprovalGate).
    PendingApproval {
        node_name: String,
        description: Option<String>,
        payload: Value,
    },
    /// The entire workflow finished.
    WorkflowCompleted {
        execution_id: String,
        workflow_name: String,
    },
    /// The workflow failed.
    WorkflowFailed {
        execution_id: String,
        workflow_name: String,
        error: String,
    },
}

/// Metadata tracked alongside each pending task for IDE briefing.
#[derive(Debug)]
pub struct PendingTaskMeta {
    pub sender: oneshot::Sender<Value>,
    pub payload: Value,
    pub node_kind: String,
    pub description: Option<String>,
    pub workflow_name: String,
}

/// Read-only snapshot of pending task metadata (without the sender).
#[derive(Clone, Debug)]
pub struct PendingTaskInfo {
    pub node_name: String,
    pub node_kind: String,
    pub description: Option<String>,
    pub workflow_name: String,
    pub payload: Value,
}

/// The Supervisor manages paused graph executions, delegating them to IDE agents.
#[derive(Clone)]
pub struct Supervisor {
    pending: Arc<Mutex<HashMap<String, PendingTaskMeta>>>,
    event_tx: broadcast::Sender<WorkflowEvent>,
}

impl Default for Supervisor {
    fn default() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
        }
    }
}

impl Supervisor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to workflow events.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<WorkflowEvent> {
        self.event_tx.subscribe()
    }

    /// Emit an event (swallows send errors if no receivers).
    fn emit(&self, event: WorkflowEvent) {
        let _ = self.event_tx.send(event);
    }

    /// Suspend execution and wait for the IDE agent to provide the next payload.
    #[tracing::instrument(skip(self, payload), fields(node = %node_name))]
    pub async fn delegate(
        &self,
        node_name: String,
        node_kind: String,
        description: Option<String>,
        workflow_name: String,
        payload: Value,
        timeout: Option<std::time::Duration>,
    ) -> Result<Value, SupervisorError> {
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            if pending.contains_key(&node_name) {
                return Err(SupervisorError::AlreadyPending(node_name));
            }
            pending.insert(
                node_name.clone(),
                PendingTaskMeta {
                    sender: tx,
                    payload: payload.clone(),
                    node_kind: node_kind.clone(),
                    description: description.clone(),
                    workflow_name: workflow_name.clone(),
                },
            );
            tracing::info!("Agent task suspended and waiting for delegation result");
            self.emit(WorkflowEvent::NodeStarted {
                node_name: node_name.clone(),
                node_kind: node_kind.clone(),
                description: description.clone(),
                workflow_name: workflow_name.clone(),
            });
        }

        if let Some(duration) = timeout {
            match tokio::time::timeout(duration, rx).await {
                Ok(Ok(res)) => {
                    self.emit(WorkflowEvent::NodeCompleted {
                        node_name: node_name.clone(),
                        node_kind: node_kind.clone(),
                    });
                    Ok(res)
                }
                Ok(Err(_)) => {
                    tracing::warn!("Agent channel dropped");
                    let nn = node_name.clone();
                    self.emit(WorkflowEvent::NodeFailed {
                        node_name,
                        node_kind,
                        error: "channel dropped".to_owned(),
                    });
                    Err(SupervisorError::Dropped(nn))
                }
                Err(_) => {
                    tracing::warn!("Agent task timed out after {} seconds", duration.as_secs());
                    if let Ok(mut pending) = self.pending.lock() {
                        pending.remove(&node_name);
                    }
                    let nn = node_name.clone();
                    self.emit(WorkflowEvent::NodeFailed {
                        node_name,
                        node_kind,
                        error: "timeout".to_owned(),
                    });
                    Err(SupervisorError::Timeout(nn))
                }
            }
        } else {
            tracing::info!(
                "Waiting indefinitely for human intervention on '{}'",
                node_name
            );
            self.emit(WorkflowEvent::PendingApproval {
                node_name: node_name.clone(),
                description,
                payload,
            });
            match rx.await {
                Ok(res) => {
                    self.emit(WorkflowEvent::NodeCompleted {
                        node_name: node_name.clone(),
                        node_kind,
                    });
                    Ok(res)
                }
                Err(_) => {
                    tracing::warn!("Agent channel dropped");
                    let nn = node_name.clone();
                    self.emit(WorkflowEvent::NodeFailed {
                        node_name,
                        node_kind,
                        error: "channel dropped".to_owned(),
                    });
                    Err(SupervisorError::Dropped(nn))
                }
            }
        }
    }

    /// Provide the result for a pending task, resuming its execution in the executor.
    #[tracing::instrument(skip(self, result), fields(node = %node_name))]
    pub fn resume(&self, node_name: &str, result: Value) -> Result<(), SupervisorError> {
        let task = {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            pending
                .remove(node_name)
                .ok_or_else(|| SupervisorError::NotPending(node_name.to_owned()))?
        };

        tracing::info!("Resuming agent task with external result");
        task.sender
            .send(result)
            .map_err(|_| SupervisorError::Dropped(node_name.to_owned()))
    }

    /// Auto-resolve a pending task with the given result payload.
    #[tracing::instrument(skip(self), fields(node = %node_name))]
    pub fn auto_resolve(&self, node_name: &str, result: Value) -> Result<(), SupervisorError> {
        let task = {
            let mut pending = self.pending.lock().map_err(|_| SupervisorError::Poisoned)?;
            pending
                .remove(node_name)
                .ok_or_else(|| SupervisorError::NotPending(node_name.to_owned()))?
        };

        tracing::info!("Auto-resolving agent task for '{}'", node_name);
        task.sender
            .send(result)
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

    /// Get detailed metadata about a pending task (for IDE briefing).
    #[must_use]
    pub fn pending_task_info(&self, node_name: &str) -> Option<PendingTaskInfo> {
        self.pending.lock().ok().and_then(|guard| {
            guard.get(node_name).map(|meta| PendingTaskInfo {
                node_name: node_name.to_owned(),
                node_kind: meta.node_kind.clone(),
                description: meta.description.clone(),
                workflow_name: meta.workflow_name.clone(),
                payload: meta.payload.clone(),
            })
        })
    }

    /// Get the stored payload for a pending task, if available.
    #[must_use]
    pub fn pending_payload(&self, node_name: &str) -> Option<Value> {
        self.pending
            .lock()
            .ok()
            .and_then(|guard| guard.get(node_name).map(|t| t.payload.clone()))
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
