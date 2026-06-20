//! Pre-delegation token budget gate via agent-heart `/budget/check`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::workflow::NodeKind;

#[derive(Debug, Clone)]
pub struct BudgetGate {
    heart_url: String,
    enabled: bool,
    client: reqwest::Client,
}

#[derive(Debug, Clone, Serialize)]
struct BudgetCheckRequest {
    phase: String,
    estimated_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BudgetDecision {
    pub allowed: bool,
    pub frozen: bool,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum BudgetGateError {
    #[error("token budget frozen: {reason}")]
    Frozen { reason: String },
}

impl BudgetGate {
    #[must_use]
    pub fn from_env() -> Self {
        let enabled = std::env::var("AUTONOMIC_BUDGET_GATE")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        let heart_url = std::env::var("AUTONOMIC_HEART_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3101".into());
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            heart_url: heart_url.trim_end_matches('/').to_string(),
            enabled,
            client,
        }
    }

    #[must_use]
    pub fn disabled() -> Self {
        Self {
            heart_url: String::new(),
            enabled: false,
            client: reqwest::Client::new(),
        }
    }

    pub async fn check_node(
        &self,
        node_kind: &str,
        estimated_tokens: u64,
    ) -> Result<(), BudgetGateError> {
        if !self.enabled {
            return Ok(());
        }

        let req = BudgetCheckRequest {
            phase: phase_for_kind(node_kind).into(),
            estimated_tokens: estimated_tokens.max(1),
            task_kind: Some(node_kind.into()),
        };

        let url = format!("{}/budget/check", self.heart_url);
        let resp = match self.client.post(&url).json(&req).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("budget gate unreachable ({e}); allowing delegation");
                return Ok(());
            }
        };

        if !resp.status().is_success() {
            tracing::warn!(
                "budget gate HTTP {}; allowing delegation",
                resp.status()
            );
            return Ok(());
        }

        let body: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("budget gate parse error ({e}); allowing delegation");
                return Ok(());
            }
        };

        if body.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            tracing::warn!("budget gate error response; allowing delegation");
            return Ok(());
        }

        let Some(decision_val) = body.get("decision") else {
            return Ok(());
        };

        let decision: BudgetDecision = match serde_json::from_value(decision_val.clone()) {
            Ok(dec) => dec,
            Err(e) => {
                tracing::warn!("budget gate decision parse ({e}); allowing delegation");
                return Ok(());
            }
        };

        if !decision.allowed || decision.frozen {
            return Err(BudgetGateError::Frozen {
                reason: decision.reason,
            });
        }

        Ok(())
    }
}

#[must_use]
pub fn phase_for_kind(kind: &str) -> &'static str {
    match kind {
        "approval_gate" => "approval",
        "verify" => "verifying",
        _ => "implementing",
    }
}

#[must_use]
pub fn estimated_tokens_from_payload(payload: &serde_json::Value) -> u64 {
    payload
        .get("_brain_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(2000)
}

#[must_use]
pub fn requires_budget_check(kind: &NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::Agent
            | NodeKind::Checkpoint
            | NodeKind::Router
            | NodeKind::Debate
            | NodeKind::Vote
            | NodeKind::Sandbox
            | NodeKind::ApprovalGate
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn phase_mapping() {
        assert_eq!(phase_for_kind("approval_gate"), "approval");
        assert_eq!(phase_for_kind("agent"), "implementing");
    }

    #[test]
    fn tokens_from_payload() {
        assert_eq!(estimated_tokens_from_payload(&json!({})), 2000);
        assert_eq!(
            estimated_tokens_from_payload(&json!({ "_brain_tokens": 900 })),
            900
        );
    }

    #[test]
    fn requires_check_for_agent_not_fork() {
        assert!(requires_budget_check(&NodeKind::Agent));
        assert!(!requires_budget_check(&NodeKind::Fork));
    }
}
