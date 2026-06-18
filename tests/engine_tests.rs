use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio;

use agent_spine::WorkflowState;
use agent_spine::executor::Executor;
use agent_spine::router::{ConfidenceRouter, RouterAction};
use agent_spine::state::InMemoryStateStore;
use agent_spine::supervisor::Supervisor;
use agent_spine::workflow::{NodeKind, WorkflowDefinition, WorkflowEdge, WorkflowNode};

#[tokio::test]
async fn test_executor_linear_run() {
    let nodes = vec![
        WorkflowNode::new("start_agent", NodeKind::Agent),
        WorkflowNode::new("end_agent", NodeKind::Agent),
    ];
    let edges = vec![WorkflowEdge::new("start_agent", "end_agent")];

    let def = WorkflowDefinition::new("test_linear", 1, "start_agent", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();
    let router = ConfidenceRouter::new(3);

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone(), router);

    // Run the executor in a background task so we can simulate the supervisor responding
    let exec_task = tokio::spawn(async move {
        // This requires mutable executor.
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    // We must wait a tiny bit to let the executor hit the first node and pause in the supervisor
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Check pending tasks
    let pending = supervisor.pending_tasks();
    assert_eq!(pending, vec!["start_agent"]);

    // Resume the first node
    supervisor
        .resume("start_agent", json!({ "step": 1 }))
        .unwrap();

    // Wait for it to hit the second node
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pending = supervisor.pending_tasks();
    assert_eq!(pending, vec!["end_agent"]);

    // Resume the second node
    supervisor
        .resume("end_agent", json!({ "step": 2 }))
        .unwrap();

    // The execution should finish now
    let execution_id = exec_task.await.unwrap().unwrap();

    // Verify state history
    let history = store.lock().unwrap().history(execution_id);
    assert_eq!(history.len(), 3); // Initial -> start_agent -> end_agent
}

#[test]
fn test_confidence_router_escalation() {
    let mut router = ConfidenceRouter::new(3);

    // Fail 1
    assert_eq!(
        router.evaluate_transition("Agent", "Verify", &json!({ "success": false })),
        RouterAction::Continue
    );
    // Fail 2
    assert_eq!(
        router.evaluate_transition("Agent", "Verify", &json!({ "success": false })),
        RouterAction::Continue
    );
    // Fail 3 -> Threshold reached
    assert_eq!(
        router.evaluate_transition("Agent", "Verify", &json!({ "success": false })),
        RouterAction::Escalate("Verify".to_string())
    );
}
