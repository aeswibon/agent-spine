use agent_body_core::StateTransitionEvent;
use agent_body_core::nats::subjects;
use tracing::{info, warn};

use crate::supervisor::{Supervisor, WorkflowEvent};

fn to_transition(event: WorkflowEvent) -> StateTransitionEvent {
    let (event_type, execution_id, workflow_name, payload) = match event {
        WorkflowEvent::NodeStarted {
            node_name,
            node_kind,
            description,
            workflow_name,
        } => (
            "node_started".to_string(),
            None,
            Some(workflow_name),
            serde_json::json!({
                "node_name": node_name,
                "node_kind": node_kind,
                "description": description,
            }),
        ),
        WorkflowEvent::NodeCompleted {
            node_name,
            node_kind,
        } => (
            "node_completed".to_string(),
            None,
            None,
            serde_json::json!({ "node_name": node_name, "node_kind": node_kind }),
        ),
        WorkflowEvent::NodeFailed {
            node_name,
            node_kind,
            error,
        } => (
            "node_failed".to_string(),
            None,
            None,
            serde_json::json!({ "node_name": node_name, "node_kind": node_kind, "error": error }),
        ),
        WorkflowEvent::PendingApproval {
            node_name,
            description,
            payload,
        } => (
            "pending_approval".to_string(),
            None,
            None,
            serde_json::json!({ "node_name": node_name, "description": description, "payload": payload }),
        ),
        WorkflowEvent::WorkflowCompleted {
            execution_id,
            workflow_name,
        } => (
            "workflow_completed".to_string(),
            Some(execution_id.clone()),
            Some(workflow_name),
            serde_json::json!({}),
        ),
        WorkflowEvent::WorkflowFailed {
            execution_id,
            workflow_name,
            error,
        } => (
            "workflow_failed".to_string(),
            Some(execution_id.clone()),
            Some(workflow_name),
            serde_json::json!({ "error": error }),
        ),
    };

    let msg_id = format!(
        "{execution_id:?}-{event_type}-{}",
        chrono::Utc::now().timestamp_millis()
    );

    StateTransitionEvent {
        msg_id,
        execution_id,
        workflow_name,
        event_type,
        payload,
    }
}

async fn ensure_stream(
    client: &async_nats::Client,
) -> Result<async_nats::jetstream::Context, String> {
    crate::jetstream::ensure_autonomic_stream(client).await
}

/// Forward supervisor workflow events to JetStream for durable event sourcing.
pub fn spawn_state_bridge(supervisor: Supervisor, nats_url: String) {
    tokio::spawn(async move {
        let Ok(Ok(client)) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            async_nats::connect(&nats_url),
        )
        .await
        else {
            warn!("jetstream bridge: failed to connect to {nats_url}");
            return;
        };
        let Ok(js) = ensure_stream(&client).await else {
            warn!("jetstream bridge: failed to ensure AUTONOMIC stream");
            return;
        };

        info!(
            subject = subjects::SPINE_STATE,
            "jetstream state bridge active"
        );

        let mut rx = supervisor.subscribe();
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let transition = to_transition(event);
                    let msg_id = transition.msg_id.clone();
                    let Ok(bytes) = serde_json::to_vec(&transition) else {
                        continue;
                    };
                    if let Err(e) =
                        crate::jetstream::publish_dedup(&js, subjects::SPINE_STATE, &msg_id, &bytes)
                            .await
                    {
                        warn!(error = %e, "jetstream publish failed");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(
                        lagged = n,
                        "jetstream bridge lagged behind supervisor events"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_workflow_completed_event() {
        let t = to_transition(WorkflowEvent::WorkflowCompleted {
            execution_id: "exec-1".into(),
            workflow_name: "demo".into(),
        });
        assert_eq!(t.event_type, "workflow_completed");
        assert_eq!(t.execution_id.as_deref(), Some("exec-1"));
    }

    #[test]
    fn maps_node_started_payload() {
        let t = to_transition(WorkflowEvent::NodeStarted {
            node_name: "lint".into(),
            node_kind: "agent".into(),
            description: None,
            workflow_name: "ci".into(),
        });
        assert_eq!(t.payload["node_name"], json!("lint"));
    }
}
