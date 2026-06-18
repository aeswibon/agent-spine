use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Declarative workflow definition loaded from YAML.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowDefinition {
    name: String,
    version: u32,
    start_node: String,
    #[serde(default)]
    nodes: Vec<WorkflowNode>,
    #[serde(default)]
    edges: Vec<WorkflowEdge>,
}

impl WorkflowDefinition {
    /// Create a workflow definition in memory.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        version: u32,
        start_node: impl Into<String>,
        nodes: Vec<WorkflowNode>,
        edges: Vec<WorkflowEdge>,
    ) -> Self {
        Self {
            name: name.into(),
            version,
            start_node: start_node.into(),
            nodes,
            edges,
        }
    }

    /// Load and parse a workflow definition from a YAML file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, WorkflowValidationError> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).map_err(|source| WorkflowValidationError::Read {
            path: path.to_path_buf(),
            source,
        })?;

        serde_yaml::from_str(&content).map_err(|source| WorkflowValidationError::Parse { source })
    }

    /// Parse a workflow definition from YAML text.
    pub fn from_yaml(content: &str) -> Result<Self, WorkflowValidationError> {
        serde_yaml::from_str(content).map_err(|source| WorkflowValidationError::Parse { source })
    }

    /// Validate the workflow as a state machine.
    pub fn validate(self) -> Result<ValidatedWorkflow, WorkflowValidationError> {
        if self.name.trim().is_empty() {
            return Err(WorkflowValidationError::EmptyWorkflowName);
        }

        if self.version == 0 {
            return Err(WorkflowValidationError::InvalidVersion { version: 0 });
        }

        if self.nodes.is_empty() {
            return Err(WorkflowValidationError::MissingNodes);
        }

        let mut node_indexes = HashMap::with_capacity(self.nodes.len());
        for (index, node) in self.nodes.iter().enumerate() {
            let name = node.name.trim();
            if name.is_empty() {
                return Err(WorkflowValidationError::EmptyNodeName { index });
            }

            if node_indexes.insert(name.to_owned(), index).is_some() {
                return Err(WorkflowValidationError::DuplicateNodeName {
                    name: name.to_owned(),
                });
            }
        }

        if !node_indexes.contains_key(&self.start_node) {
            return Err(WorkflowValidationError::MissingStartNode {
                name: self.start_node.clone(),
            });
        }

        for edge in &self.edges {
            if !node_indexes.contains_key(edge.from()) {
                return Err(WorkflowValidationError::UnknownNode {
                    name: edge.from().to_owned(),
                });
            }
            if !node_indexes.contains_key(edge.to()) {
                return Err(WorkflowValidationError::UnknownNode {
                    name: edge.to().to_owned(),
                });
            }
        }

        Ok(ValidatedWorkflow {
            definition: self,
        })
    }

    /// Return the workflow name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the workflow version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Return the start node.
    #[must_use]
    pub fn start_node(&self) -> &str {
        &self.start_node
    }

    /// Return the declared nodes.
    #[must_use]
    pub fn nodes(&self) -> &[WorkflowNode] {
        &self.nodes
    }

    /// Return the declared edges.
    #[must_use]
    pub fn edges(&self) -> &[WorkflowEdge] {
        &self.edges
    }
}

/// A validated workflow state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedWorkflow {
    definition: WorkflowDefinition,
}

impl ValidatedWorkflow {
    /// Return the validated workflow definition.
    #[must_use]
    pub fn definition(&self) -> &WorkflowDefinition {
        &self.definition
    }
}

/// A node in the declarative workflow graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowNode {
    name: String,
    kind: NodeKind,
    #[serde(default)]
    description: Option<String>,
}

impl WorkflowNode {
    /// Create an agent node.
    #[must_use]
    pub fn agent(name: impl Into<String>) -> Self {
        Self::new(name, NodeKind::Agent)
    }

    /// Create a node with the given kind.
    #[must_use]
    pub fn new(name: impl Into<String>, kind: NodeKind) -> Self {
        Self {
            name: name.into(),
            kind,
            description: None,
        }
    }

    /// Attach a human-readable description to the node.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Return the node name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the node kind.
    #[must_use]
    pub const fn kind(&self) -> &NodeKind {
        &self.kind
    }
}

/// The role a node plays in the workflow.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Agent,
    Checkpoint,
    Verify,
}

/// A directed edge between two workflow nodes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowEdge {
    from: String,
    to: String,
}

impl WorkflowEdge {
    /// Create a directed edge between two nodes.
    #[must_use]
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }

    /// Return the source node.
    #[must_use]
    pub fn from(&self) -> &str {
        &self.from
    }

    /// Return the destination node.
    #[must_use]
    pub fn to(&self) -> &str {
        &self.to
    }
}

#[derive(Debug, Error)]
pub enum WorkflowValidationError {
    #[error("workflow name must not be empty")]
    EmptyWorkflowName,
    #[error("workflow version must be greater than zero")]
    InvalidVersion { version: u32 },
    #[error("workflow must declare at least one node")]
    MissingNodes,
    #[error("workflow node name must not be empty at index {index}")]
    EmptyNodeName { index: usize },
    #[error("duplicate workflow node name: {name}")]
    DuplicateNodeName { name: String },
    #[error("start_node references unknown node: {name}")]
    MissingStartNode { name: String },
    #[error("workflow edge references unknown node: {name}")]
    UnknownNode { name: String },
    #[error("failed to read workflow definition from {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse workflow definition: {source}")]
    Parse {
        #[source]
        source: serde_yaml::Error,
    },
}
