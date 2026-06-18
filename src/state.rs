use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use rusqlite::{Connection, params};
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
        fs::create_dir_all(&base_dir).map_err(StateError::Io)?;
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
            .map_err(StateError::Io)?;

        let json = serde_json::to_string(&snapshot).map_err(StateError::Serialization)?;
        writeln!(file, "{json}").map_err(StateError::Io)?;

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

/// A SQLite-backed state store for robust, atomic persistence.
pub struct SqliteStateStore {
    conn: Connection,
}

impl SqliteStateStore {
    /// Create a new SQLite-backed state store.
    ///
    /// # Errors
    /// Returns an error if the database connection fails or tables cannot be created.
    pub fn new(path: impl AsRef<std::path::Path>) -> Result<Self, StateError> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                snapshot_data TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(execution_id, sequence)
            )",
            [],
        )?;
        Ok(Self { conn })
    }
}

impl WorkflowState for SqliteStateStore {
    fn append(&mut self, snapshot: StateSnapshot) -> Result<(), StateError> {
        // We use the JSON-serialized execution_id since it wraps a UUID.
        let execution_id_str = serde_json::to_string(&snapshot.execution_id())
            .unwrap_or_else(|_| "unknown".to_string())
            .trim_matches('"')
            .to_string();

        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM snapshots WHERE execution_id = ?1")?;
        let count_i64: i64 = stmt.query_row([&execution_id_str], |row| row.get(0))?;
        let expected_sequence = count_i64 as u64;

        if snapshot.sequence() != expected_sequence {
            return Err(StateError::InvalidSequence {
                expected: expected_sequence,
                actual: snapshot.sequence(),
            });
        }

        let json = serde_json::to_string(&snapshot).map_err(StateError::Serialization)?;
        self.conn.execute(
            "INSERT INTO snapshots (execution_id, sequence, snapshot_data) VALUES (?1, ?2, ?3)",
            params![execution_id_str, snapshot.sequence(), json],
        )?;

        Ok(())
    }

    fn history(&self, execution_id: ExecutionId) -> Vec<StateSnapshot> {
        let execution_id_str = serde_json::to_string(&execution_id)
            .unwrap_or_else(|_| "unknown".to_string())
            .trim_matches('"')
            .to_string();

        let mut history = Vec::new();

        if let Ok(mut stmt) = self.conn.prepare(
            "SELECT snapshot_data FROM snapshots WHERE execution_id = ?1 ORDER BY sequence ASC",
        ) {
            let snapshot_iter = stmt.query_map([&execution_id_str], |row| row.get::<_, String>(0));

            if let Ok(iter) = snapshot_iter {
                for json_res in iter {
                    if let Ok(json) = json_res {
                        if let Ok(snapshot) = serde_json::from_str(&json) {
                            history.push(snapshot);
                        }
                    }
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
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}
