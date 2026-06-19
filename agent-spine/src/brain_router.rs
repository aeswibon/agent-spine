use std::path::PathBuf;

use serde_json::Value;
use tracing;

use crate::mcp_bridge::{self, McpBridge, RouteLimits, RouteTaskResponse};
use crate::router::{ConfidenceRouter, RouterAction};

/// An optional enhancement over `ConfidenceRouter` that delegates escalation
/// decisions to agent-brain's `route_task` MCP tool.
///
/// If agent-brain is unavailable, falls back to the heuristic `ConfidenceRouter`.
pub struct BrainRouter {
    bridge: Option<McpBridge>,
    fallback: ConfidenceRouter,
    workflow_name: String,
    #[allow(dead_code)]
    cwd: Option<PathBuf>,
    connect_attempted: bool,
}

impl BrainRouter {
    /// Create a new BrainRouter that will lazily connect to agent-brain.
    pub fn new(workflow_name: impl Into<String>, cwd: Option<PathBuf>) -> Self {
        Self {
            bridge: None,
            fallback: ConfidenceRouter::new(3),
            workflow_name: workflow_name.into(),
            cwd,
            connect_attempted: false,
        }
    }

    /// Evaluate a workflow transition, potentially escalating via agent-brain.
    pub async fn evaluate_transition(
        &mut self,
        source_node: &str,
        target_node: &str,
        payload: &Value,
    ) -> RouterAction {
        // Try to connect lazily on first use (only if cwd is configured).
        if self.bridge.is_none() && !self.connect_attempted && self.cwd.is_some() {
            self.connect_attempted = true;
            match McpBridge::connect(self.cwd.as_deref()).await {
                Ok(bridge) => {
                    tracing::info!("connected to agent-brain for workflow routing");
                    self.bridge = Some(bridge);
                }
                Err(e) => {
                    tracing::warn!("agent-brain not available, using fallback router: {e}");
                }
            }
        }

        if let Some(bridge) = self.bridge.as_mut() {
            let message = mcp_bridge::transition_route_message(
                &self.workflow_name,
                source_node,
                target_node,
                payload,
            );

            match Self::call_route_task(bridge, &message).await {
                Ok(resp) => {
                    if resp.escalate_recommended || resp.route_confidence < 0.4 {
                        tracing::info!(
                            "brain recommends escalation (confidence={:.2}, escalate={})",
                            resp.route_confidence,
                            resp.escalate_recommended,
                        );
                        return RouterAction::Escalate(target_node.to_owned());
                    }

                    // Log brain's briefing for observability
                    if !resp.briefing.is_empty() {
                        tracing::debug!("brain route: {}", resp.briefing);
                    }
                    RouterAction::Continue
                }
                Err(e) => {
                    tracing::warn!("brain route_task failed: {e}, using fallback");
                    self.bridge = None; // force reconnect next time
                    self.fallback
                        .evaluate_transition(source_node, target_node, payload)
                }
            }
        } else {
            self.fallback
                .evaluate_transition(source_node, target_node, payload)
        }
    }

    /// Call route_task on the bridge with a timeout and error handling.
    async fn call_route_task(
        bridge: &mut McpBridge,
        message: &str,
    ) -> Result<RouteTaskResponse, mcp_bridge::BridgeError> {
        bridge
            .route_task(
                message,
                None, // cwd — resolved at connect time
                &[],  // open_files
                300,  // max_tokens — tight budget for routing
                RouteLimits {
                    agents: 1,
                    skills: 2,
                    rules: 3,
                    memory: 0,
                },
                Some("implementing"),
                None,
            )
            .await
    }

    /// Store a trajectory record in agent-brain.
    pub async fn store_trajectory(
        &mut self,
        execution_id: &str,
        node_id: &str,
        outcome: &str,
        notes: Option<&str>,
    ) {
        if let Some(bridge) = self.bridge.as_mut()
            && let Err(e) = bridge
                .store_trajectory(execution_id, node_id, outcome, None, None, notes)
                .await
        {
            tracing::debug!("brain store_trajectory skipped: {e}");
        }
    }

    /// Enrich a node payload with brain recommendations before delegation.
    pub async fn enrich_payload(
        &mut self,
        node_name: &str,
        node_kind: &str,
        description: Option<&str>,
        payload: &Value,
    ) -> Option<RouteTaskResponse> {
        let bridge = self.bridge.as_mut()?;

        let message = mcp_bridge::node_route_message(
            &self.workflow_name,
            node_name,
            node_kind,
            description,
            payload,
        );

        match Self::call_route_task(bridge, &message).await {
            Ok(resp) => Some(resp),
            Err(e) => {
                tracing::debug!("brain enrich_payload skipped: {e}");
                None
            }
        }
    }

    /// Check whether the bridge is connected.
    pub fn is_connected(&self) -> bool {
        self.bridge.is_some()
    }
}
