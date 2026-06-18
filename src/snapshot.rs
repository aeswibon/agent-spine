use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::{ExecutionId, Transition};

/// An immutable point-in-time workflow state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StateSnapshot {
    id: Uuid,
    execution_id: ExecutionId,
    parent_id: Option<Uuid>,
    sequence: u64,
    transition: Option<Transition>,
    payload: Value,
}

impl StateSnapshot {
    /// Create the root snapshot for an execution.
    #[must_use]
    pub fn initial(execution_id: ExecutionId, payload: Value) -> Self {
        Self {
            id: Uuid::now_v7(),
            execution_id,
            parent_id: None,
            sequence: 0,
            transition: None,
            payload,
        }
    }

    /// Produce a new snapshot without modifying the current snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::EmptyNodeName`] when either endpoint of the
    /// transition is empty.
    pub fn transition(
        &self,
        transition: Transition,
        payload: Value,
    ) -> Result<Self, SnapshotError> {
        if transition.from().trim().is_empty() || transition.to().trim().is_empty() {
            return Err(SnapshotError::EmptyNodeName);
        }

        Ok(Self {
            id: Uuid::now_v7(),
            execution_id: self.execution_id,
            parent_id: Some(self.id),
            sequence: self.sequence + 1,
            transition: Some(transition),
            payload,
        })
    }

    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    #[must_use]
    pub const fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    #[must_use]
    pub const fn parent_id(&self) -> Option<Uuid> {
        self.parent_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn transition_edge(&self) -> Option<&Transition> {
        self.transition.as_ref()
    }

    #[must_use]
    pub const fn payload(&self) -> &Value {
        &self.payload
    }
}

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("transition node names must not be empty")]
    EmptyNodeName,
}
