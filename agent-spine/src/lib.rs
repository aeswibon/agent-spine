#![allow(
    clippy::collapsible_if,
    clippy::io_other_error,
    clippy::too_many_arguments
)]
pub mod agent;
pub mod api;
#[cfg(feature = "nats")]
pub mod async_sandbox;
pub mod autonomic_api;
pub mod brain_router;
pub mod budget_gate;
pub mod cancellation;
pub(crate) mod condition;
pub mod event;
pub mod executor;
pub mod global_workspace;
pub mod idempotency;
#[cfg(feature = "nats")]
pub mod jetstream;
#[cfg(feature = "nats")]
pub mod jetstream_bridge;
pub mod log;
pub mod mcp_bridge;
pub mod mcp_server;
pub mod meta_router;
pub mod registry_workflow;
pub mod router;
pub mod sandbox;
pub mod setup;
pub mod state;
pub mod supervisor;
pub mod update;
pub mod wake_on_call;
pub mod workflow;
pub mod workflow_manager;
pub mod workflows;

mod execution;
mod snapshot;
mod transition;

pub use brain_router::BrainProvenance;
pub use execution::ExecutionId;
pub use executor::{ExecutorError, SnapshotConfig};
pub use idempotency::{IdempotencyRecord, IdempotencyStore, SqliteIdempotencyStore};
pub use snapshot::StateSnapshot;
pub use transition::Transition;
pub use workflow::{
    NodeKind, ValidatedWorkflow, WorkflowDefinition, WorkflowEdge, WorkflowNode,
    WorkflowValidationError,
};
pub use workflow_manager::WorkflowManager;

/// Read and append immutable workflow snapshots.
pub trait WorkflowState: Send {
    /// Persist a snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot violates append-only ordering or
    /// the store cannot represent its sequence.
    fn append(&mut self, snapshot: StateSnapshot) -> Result<(), state::StateError>;

    /// Return the ordered snapshot history for an execution.
    fn history(&self, execution_id: ExecutionId) -> Vec<StateSnapshot>;

    /// List all unique execution IDs stored.
    fn list_executions(&self) -> Result<Vec<ExecutionId>, state::StateError>;
}
