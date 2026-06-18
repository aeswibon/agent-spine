use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use thiserror::Error;

use crate::{ExecutionId, StateSnapshot, WorkflowState};

/// Append-only state adapter used by tests and early engine development.
#[derive(Default)]
pub struct InMemoryStateStore {
    snapshots: HashMap<ExecutionId, Vec<StateSnapshot>>,
}

impl WorkflowState for InMemoryStateStore {
    fn append(&mut self, snapshot: StateSnapshot) -> Result<(), StateError> {
        let history = self.snapshots.entry(snapshot.execution_id()).or_default();
        let expected_sequence = u64::try_from(history.len()).map_err(|_| StateError::Overflow)?;

        if snapshot.sequence() != expected_sequence {
            return Err(StateError::InvalidSequence {
                expected: expected_sequence,
                actual: snapshot.sequence(),
            });
        }

        history.push(snapshot);
        Ok(())
    }

    fn history(&self, execution_id: ExecutionId) -> Vec<StateSnapshot> {
        self.snapshots
            .get(&execution_id)
            .cloned()
            .unwrap_or_default()
    }
}

/// A file-backed append-only state store using JSONL.
/// Useful for time-travel debugging and persistent executions.
pub struct FileStateStore {
    base_dir: PathBuf,
}

impl FileStateStore {
    /// Create a new file-backed state store.
    pub fn new(base_dir: impl Into<PathBuf>) -> Result<Self, StateError> {
        let base_dir = base_dir.into();
        fs::create_dir_all(&base_dir).map_err(|e| StateError::Io(e))?;
        Ok(Self { base_dir })
    }

    fn file_path(&self, execution_id: ExecutionId) -> PathBuf {
        // We use the string representation of ExecutionId's inner UUID.
        // ExecutionId wraps a UUID which implements Display via serde if we serialize it,
        // but it might not implement Display directly.
        // Assuming we can serialize it to string. Let's just use JSON serialization or debug format for now.
        // Wait, ExecutionId doesn't have a public getter for Uuid right now, but we can serialize it to string.
        let file_name = format!(
            "{}.jsonl",
            serde_json::to_string(&execution_id)
                .unwrap_or_else(|_| "unknown".to_string())
                .trim_matches('"')
        );
        self.base_dir.join(file_name)
    }
}

impl WorkflowState for FileStateStore {
    fn append(&mut self, snapshot: StateSnapshot) -> Result<(), StateError> {
        let path = self.file_path(snapshot.execution_id());

        let history = self.history(snapshot.execution_id());
        let expected_sequence = u64::try_from(history.len()).map_err(|_| StateError::Overflow)?;

        if snapshot.sequence() != expected_sequence {
            return Err(StateError::InvalidSequence {
                expected: expected_sequence,
                actual: snapshot.sequence(),
            });
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| StateError::Io(e))?;

        let json = serde_json::to_string(&snapshot).map_err(|e| StateError::Serialization(e))?;
        writeln!(file, "{}", json).map_err(|e| StateError::Io(e))?;

        Ok(())
    }

    fn history(&self, execution_id: ExecutionId) -> Vec<StateSnapshot> {
        let path = self.file_path(execution_id);
        let mut history = Vec::new();

        if let Ok(file) = fs::File::open(&path) {
            let reader = BufReader::new(file);
            for line in reader.lines().map_while(Result::ok) {
                if let Ok(snapshot) = serde_json::from_str(&line) {
                    history.push(snapshot);
                }
            }
        }

        history
    }
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("snapshot sequence mismatch: expected {expected}, received {actual}")]
    InvalidSequence { expected: u64, actual: u64 },
    #[error("snapshot history exceeded supported sequence range")]
    Overflow,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
