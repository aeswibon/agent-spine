use serde_json::Value;
use std::collections::HashMap;

/// The ConfidenceRouter monitors execution loops and escalates tasks that repeatedly fail verification.
#[derive(Default, Clone, Debug)]
pub struct ConfidenceRouter {
    /// Maps a node name to its consecutive failure count.
    failure_counts: HashMap<String, u32>,
    /// The threshold before escalating to a frontier model.
    max_retries: u32,
}

impl ConfidenceRouter {
    /// Create a new router with a specific failure threshold.
    pub fn new(max_retries: u32) -> Self {
        Self {
            failure_counts: HashMap::new(),
            max_retries,
        }
    }

    /// Evaluates the outcome of a node. If it's a verification node that failed,
    /// this bumps the failure count of the target node.
    pub fn evaluate_transition(&mut self, source_node: &str, target_node: &str, payload: &Value) -> RouterAction {
        // Simple heuristic: If the payload contains a "success": false field, we treat it as a failure.
        let is_failure = payload.get("success").and_then(|v| v.as_bool()) == Some(false);

        if is_failure {
            // We failed. Which node should be penalized? Usually the node we are transitioning to, 
            // assuming we are bouncing back to the Agent node to fix it.
            let count = self.failure_counts.entry(target_node.to_owned()).or_insert(0);
            *count += 1;

            if *count >= self.max_retries {
                return RouterAction::Escalate(target_node.to_owned());
            }
        } else {
            // On success, reset the failure count for the source node, since it successfully passed.
            self.failure_counts.remove(source_node);
        }

        RouterAction::Continue
    }
}

/// The action dictated by the Confidence Router.
#[derive(Debug, Eq, PartialEq)]
pub enum RouterAction {
    /// Continue execution normally using the local model.
    Continue,
    /// Escalate the sub-task to a frontier API (e.g. Claude 3.5 Sonnet).
    Escalate(String),
}
