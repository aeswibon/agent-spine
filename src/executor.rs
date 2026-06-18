use std::sync::{Arc, Mutex};

use serde_json::Value;
use thiserror::Error;

use crate::router::{ConfidenceRouter, RouterAction};
use crate::state::StateError;
use crate::supervisor::Supervisor;
use crate::workflow::NodeKind;
use crate::{ExecutionId, StateSnapshot, Transition, ValidatedWorkflow, WorkflowState};

/// Orchestrates the execution of a `ValidatedWorkflow`.
pub struct Executor<S: WorkflowState> {
    workflow: ValidatedWorkflow,
    state_store: Arc<Mutex<S>>,
    supervisor: Supervisor,
    router: ConfidenceRouter,
}

impl<S: WorkflowState> Executor<S> {
    /// Create a new executor for the given workflow.
    pub const fn new(
        workflow: ValidatedWorkflow,
        state_store: Arc<Mutex<S>>,
        supervisor: Supervisor,
        router: ConfidenceRouter,
    ) -> Self {
        Self {
            workflow,
            state_store,
            supervisor,
            router,
        }
    }

    /// Execute the workflow from start to finish.
    pub async fn run(&mut self, initial_payload: Value) -> Result<ExecutionId, ExecutorError> {
        let execution_id = ExecutionId::new();
        let mut current_snapshot = StateSnapshot::initial(execution_id, initial_payload);

        // Persist initial state
        {
            let mut store = self
                .state_store
                .lock()
                .map_err(|_| ExecutorError::PoisonedLock)?;
            store
                .append(current_snapshot.clone())
                .map_err(ExecutorError::State)?;
        }

        let definition = self.workflow.definition();
        let nodes = definition.nodes();
        let edges = definition.edges();

        let mut current_node_name = definition.start_node().to_owned();

        // State machine execution
        loop {
            let node = nodes
                .iter()
                .find(|n| n.name() == current_node_name)
                .expect("node must exist in workflow");

            // Execute node based on kind
            let next_payload = match node.kind() {
                NodeKind::Agent => {
                    // Delegate to supervisor / IDE hook
                    self.supervisor
                        .delegate(
                            current_node_name.clone(),
                            current_snapshot.payload().clone(),
                        )
                        .await
                        .map_err(|_| ExecutorError::SupervisorFailed)?
                }
                NodeKind::Verify => {
                    // TODO: Run PRM or compiler loop
                    current_snapshot.payload().clone()
                }
                NodeKind::Checkpoint => {
                    // Pause and wait for Human-In-The-Loop
                    self.supervisor
                        .delegate(
                            current_node_name.clone(),
                            current_snapshot.payload().clone(),
                        )
                        .await
                        .map_err(|_| ExecutorError::SupervisorFailed)?
                }
            };

            let transition = Transition::new(
                current_snapshot
                    .transition_edge()
                    .map_or("START", |t| t.to()),
                &current_node_name,
            );

            // Check Confidence Router before updating the current node
            let mut escalate = false;

            // Determine next node
            let outgoing: Vec<_> = edges
                .iter()
                .filter(|e| e.from() == current_node_name)
                .collect();
            if outgoing.is_empty() {
                // Terminal node reached
                current_snapshot = current_snapshot
                    .transition(transition, next_payload)
                    .map_err(|_| ExecutorError::InvalidTransition)?;

                let mut store = self
                    .state_store
                    .lock()
                    .map_err(|_| ExecutorError::PoisonedLock)?;
                store
                    .append(current_snapshot)
                    .map_err(ExecutorError::State)?;
                break;
            } else if outgoing.len() > 1 {
                return Err(ExecutorError::MultipleOutgoingEdges(current_node_name));
            } else {
                let next_node_name = outgoing[0].to().to_owned();

                match self.router.evaluate_transition(
                    &current_node_name,
                    &next_node_name,
                    &next_payload,
                ) {
                    RouterAction::Escalate(target) => {
                        println!(
                            "Confidence Router: Escalating task for node '{target}' to frontier model."
                        );
                        escalate = true;
                    }
                    RouterAction::Continue => {}
                }

                current_node_name = next_node_name;
            }

            // Inject escalation hint into payload if needed
            let mut final_payload = next_payload;
            if escalate && let Some(obj) = final_payload.as_object_mut() {
                obj.insert("escalation_required".to_string(), Value::Bool(true));
            }

            current_snapshot = current_snapshot
                .transition(transition, final_payload)
                .map_err(|_| ExecutorError::InvalidTransition)?;

            // Persist state transition
            {
                let mut store = self
                    .state_store
                    .lock()
                    .map_err(|_| ExecutorError::PoisonedLock)?;
                store
                    .append(current_snapshot.clone())
                    .map_err(ExecutorError::State)?;
            }
        }

        Ok(execution_id)
    }
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("state storage error: {0}")]
    State(#[from] StateError),
    #[error("invalid transition produced during execution")]
    InvalidTransition,
    #[error("state store lock was poisoned")]
    PoisonedLock,
    #[error("node {0} has multiple outgoing edges without a router condition")]
    MultipleOutgoingEdges(String),
    #[error("supervisor interaction failed")]
    SupervisorFailed,
}
