use std::path::PathBuf;

use serde_json::Value;
use tracing;

use crate::mcp_bridge::{self, McpBridge, RouteLimits, RouteTaskResponse};
use crate::router::{ConfidenceRouter, RouterAction};
use crate::wake_on_call;

pub use agent_body_core::BrainProvenance;

/// An optional enhancement over `ConfidenceRouter` that delegates escalation
/// decisions to agent-brain's `route_task` MCP tool.
///
/// If agent-brain is unavailable, falls back to the heuristic `ConfidenceRouter`.
pub struct BrainRouter {
    bridge: Option<McpBridge>,
    fallback: ConfidenceRouter,
    workflow_name: String,
    cwd: Option<PathBuf>,
    connect_attempted: bool,
}

impl BrainRouter {
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
    /// This is best-effort; when brain is unavailable, it falls back immediately.
    pub fn evaluate_transition(
        &mut self,
        source_node: &str,
        target_node: &str,
        payload: &Value,
    ) -> RouterAction {
        if self.bridge.is_none() && !self.connect_attempted && self.cwd.is_some() {
            self.connect_attempted = true;
            let rt = tokio::runtime::Handle::try_current();
            match rt {
                Ok(handle) => {
                    // Try normal connect first
                    match handle.block_on(McpBridge::connect(self.cwd.as_deref())) {
                        Ok(bridge) => {
                            tracing::info!("connected to agent-brain for workflow routing");
                            self.bridge = Some(bridge);
                        }
                        Err(first_err) => {
                            tracing::warn!("agent-brain not reachable, attempting wake-on-call: {first_err}");
                            // Wake-on-call: spawn brain and retry
                            let _child = handle.block_on(wake_on_call::try_spawn_brain());
                            if handle.block_on(wake_on_call::wait_for_brain(30)) {
                                match handle.block_on(McpBridge::connect(self.cwd.as_deref())) {
                                    Ok(bridge) => {
                                        tracing::info!("connected to agent-brain via wake-on-call");
                                        self.bridge = Some(bridge);
                                    }
                                    Err(e) => {
                                        tracing::warn!("wake-on-call connect failed: {e}");
                                    }
                                }
                            } else {
                                tracing::warn!("wake-on-call failed, agent-brain did not start");
                            }
                        }
                    }
                }
                Err(_) => {
                    tracing::warn!("no tokio runtime available, using fallback router");
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

            match Self::call_route_task(bridge, &message) {
                Ok(resp) => {
                    if resp.escalate_recommended || resp.route_confidence < 0.4 {
                        tracing::info!(
                            "brain recommends escalation (confidence={:.2}, escalate={})",
                            resp.route_confidence,
                            resp.escalate_recommended,
                        );
                        return RouterAction::Escalate(target_node.to_owned());
                    }
                    if !resp.briefing.is_empty() {
                        tracing::debug!("brain route: {}", resp.briefing);
                    }
                    RouterAction::Continue
                }
                Err(e) => {
                    tracing::warn!("brain route_task failed: {e}, using fallback");
                    self.bridge = None;
                    self.fallback
                        .evaluate_transition(source_node, target_node, payload)
                }
            }
        } else {
            self.fallback
                .evaluate_transition(source_node, target_node, payload)
        }
    }

    fn call_route_task(
        bridge: &mut McpBridge,
        message: &str,
    ) -> Result<RouteTaskResponse, mcp_bridge::BridgeError> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| mcp_bridge::BridgeError::NotConnected)?;
        rt.block_on(bridge.route_task(
            message,
            None,
            &[],
            300,
            RouteLimits {
                agents: 1,
                skills: 2,
                rules: 3,
                memory: 0,
            },
            Some("implementing"),
            None,
        ))
    }

    /// Store a trajectory record in agent-brain (best-effort).
    pub fn store_trajectory(
        &mut self,
        execution_id: &str,
        node_id: &str,
        outcome: &str,
        notes: Option<&str>,
    ) {
        if let Some(bridge) = self.bridge.as_mut()
            && let rt = tokio::runtime::Handle::try_current()
            && let Ok(handle) = rt
        {
            let _ = handle.block_on(bridge.store_trajectory(
                execution_id,
                node_id,
                outcome,
                None,
                None,
                notes,
            ));
        }
    }

    /// Enrich a node payload with brain recommendations (best-effort).
    pub fn enrich_payload(
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

        match Self::call_route_task(bridge, &message) {
            Ok(resp) => Some(resp),
            Err(e) => {
                tracing::debug!("brain enrich_payload skipped: {e}");
                None
            }
        }
    }

    /// Return structured provenance from a brain routing call for a node.
    ///
    /// This is the v0.4 replacement for `enrich_payload` — it returns
    /// a lighter, structured metadata bundle that gets injected into
    /// snapshot payloads for auditability.
    pub fn get_provenance(
        &mut self,
        node_name: &str,
        node_kind: &str,
        description: Option<&str>,
        payload: &Value,
    ) -> Option<BrainProvenance> {
        self.enrich_payload(node_name, node_kind, description, payload)
            .map(|resp| BrainProvenance {
                context_id: resp.log_id,
                route_confidence: resp.route_confidence,
                skills_used: resp
                    .recommended_skills
                    .into_iter()
                    .map(|s| s.name)
                    .collect(),
                agents_loaded: resp
                    .recommended_agents
                    .into_iter()
                    .map(|a| a.name)
                    .collect(),
            })
    }

    /// Store a trajectory with full metadata (task_kind included).
    pub fn store_trajectory_full(
        &mut self,
        workflow_id: &str,
        node_id: &str,
        outcome: &str,
        task_kind: Option<&str>,
        notes: Option<&str>,
    ) {
        if let Some(bridge) = self.bridge.as_mut()
            && let rt = tokio::runtime::Handle::try_current()
            && let Ok(handle) = rt
        {
            let _ = handle.block_on(bridge.store_trajectory(
                workflow_id,
                node_id,
                outcome,
                None,
                task_kind,
                notes,
            ));
        }
    }

    pub fn is_connected(&self) -> bool {
        self.bridge.is_some()
    }
}
