use serde_json::json;
use std::sync::{Arc, Mutex};

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

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pending = supervisor.pending_tasks();
    assert_eq!(pending, vec!["start_agent"]);

    supervisor
        .resume("start_agent", json!({ "step": 1 }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pending = supervisor.pending_tasks();
    assert_eq!(pending, vec!["end_agent"]);

    supervisor
        .resume("end_agent", json!({ "step": 2 }))
        .unwrap();

    let execution_id = exec_task.await.unwrap().unwrap();

    let history = store.lock().unwrap().history(execution_id);
    assert_eq!(history.len(), 3);
}

#[test]
fn test_confidence_router_escalation() {
    let mut router = ConfidenceRouter::new(3);

    assert_eq!(
        router.evaluate_transition("Agent", "Verify", &json!({ "success": false })),
        RouterAction::Continue
    );
    assert_eq!(
        router.evaluate_transition("Agent", "Verify", &json!({ "success": false })),
        RouterAction::Continue
    );
    assert_eq!(
        router.evaluate_transition("Agent", "Verify", &json!({ "success": false })),
        RouterAction::Escalate("Verify".to_string())
    );
}

#[tokio::test]
async fn test_executor_parallel_fan_out() {
    let nodes = vec![
        WorkflowNode::new("start", NodeKind::Agent),
        WorkflowNode::new("branch_b", NodeKind::Agent),
        WorkflowNode::new("branch_c", NodeKind::Agent),
        WorkflowNode::new("end", NodeKind::Agent),
    ];
    let edges = vec![
        WorkflowEdge::new("start", "branch_b"),
        WorkflowEdge::new("start", "branch_c"),
        WorkflowEdge::new("branch_b", "end"),
        WorkflowEdge::new("branch_c", "end"),
    ];

    let def = WorkflowDefinition::new("test_parallel", 1, "start", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["start"]);
    supervisor
        .resume("start", json!({ "start_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut pending = supervisor.pending_tasks();
    pending.sort();
    assert_eq!(pending, vec!["branch_b", "branch_c"]);

    supervisor
        .resume("branch_b", json!({ "b_done": true }))
        .unwrap();
    supervisor
        .resume("branch_c", json!({ "c_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["end"]);
    supervisor.resume("end", json!({ "final": true })).unwrap();

    let execution_id = exec_task.await.unwrap().unwrap();

    let history = store.lock().unwrap().history(execution_id);
    assert_eq!(history.len(), 4);

    let fan_in_payload = history[2].payload();
    assert_eq!(fan_in_payload["b_done"], true);
    assert_eq!(fan_in_payload["c_done"], true);
    assert_eq!(fan_in_payload["start_done"], true);

    let final_payload = history[3].payload();
    assert_eq!(final_payload["final"], true);
    assert_eq!(final_payload["b_done"], true);
    assert_eq!(final_payload["c_done"], true);
}

#[tokio::test]
async fn test_approval_gate_accepts() {
    let nodes = vec![
        WorkflowNode::new("start", NodeKind::Agent),
        WorkflowNode::approval_gate("gate"),
        WorkflowNode::new("end", NodeKind::Agent),
    ];
    let edges = vec![
        WorkflowEdge::new("start", "gate"),
        WorkflowEdge::new("gate", "end"),
    ];

    let def = WorkflowDefinition::new("test_approval_accepts", 1, "start", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    supervisor
        .resume("start", json!({ "start_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["gate"]);

    supervisor
        .resume("gate", json!({ "approved": true, "start_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["end"]);
    supervisor.resume("end", json!({ "final": true })).unwrap();

    let _ = exec_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_approval_gate_rejects() {
    let nodes = vec![
        WorkflowNode::new("start", NodeKind::Agent),
        WorkflowNode::approval_gate("gate"),
        WorkflowNode::new("end", NodeKind::Agent),
    ];
    let edges = vec![
        WorkflowEdge::new("start", "gate"),
        WorkflowEdge::new("gate", "end"),
    ];

    let def = WorkflowDefinition::new("test_approval_rejects", 1, "start", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    supervisor
        .resume("start", json!({ "start_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["gate"]);

    supervisor
        .resume("gate", json!({ "rejected": true }))
        .unwrap();

    let err = exec_task.await.unwrap().unwrap_err();
    assert_eq!(err.to_string(), "execution rejected at approval gate");
}

#[tokio::test]
async fn test_supervisor_timeout_and_retry() {
    let nodes = vec![WorkflowNode::new("start", NodeKind::Agent)];
    let edges = vec![];

    let def = WorkflowDefinition::new("test_timeout", 1, "start", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));

    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["start"]);

    exec_task.abort();
}

#[tokio::test]
async fn test_fork_join_barrier() {
    let nodes = vec![
        WorkflowNode::fork("fork"),
        WorkflowNode::agent("branch_a"),
        WorkflowNode::agent("branch_b"),
        WorkflowNode::join("join"),
        WorkflowNode::agent("end"),
    ];
    let edges = vec![
        WorkflowEdge::new("fork", "branch_a"),
        WorkflowEdge::new("fork", "branch_b"),
        WorkflowEdge::new("branch_a", "join"),
        WorkflowEdge::new("branch_b", "join"),
        WorkflowEdge::new("join", "end"),
    ];

    let def = WorkflowDefinition::new("test_fork_join", 1, "fork", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Fork (pass-through) → branch_a and branch_b run in parallel
    let mut pending = supervisor.pending_tasks();
    pending.sort();
    assert_eq!(pending, vec!["branch_a", "branch_b"]);

    supervisor
        .resume("branch_a", json!({ "a_done": true }))
        .unwrap();
    supervisor
        .resume("branch_b", json!({ "b_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Both branches complete → Join fires → end runs
    assert_eq!(supervisor.pending_tasks(), vec!["end"]);
    supervisor.resume("end", json!({ "final": true })).unwrap();

    let _ = exec_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_conditional_edge_skips_branch() {
    let nodes = vec![
        WorkflowNode::router("router"),
        WorkflowNode::agent("frontend"),
        WorkflowNode::agent("backend"),
        WorkflowNode::agent("end"),
    ];
    let edges = vec![
        WorkflowEdge::conditional("router", "frontend", r#"state.task_type == "frontend""#),
        WorkflowEdge::conditional("router", "backend", r#"state.task_type == "backend""#),
        WorkflowEdge::new("frontend", "end"),
        WorkflowEdge::new("backend", "end"),
    ];

    let def = WorkflowDefinition::new("test_conditional", 1, "router", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "state": { "task_type": "frontend" } }))
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Router executes and delegates to frontend branch
    assert_eq!(supervisor.pending_tasks(), vec!["router"]);

    supervisor
        .resume("router", json!({ "state": { "task_type": "frontend" } }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Only frontend branch runs (backend edge is conditional and was skipped)
    assert_eq!(supervisor.pending_tasks(), vec!["frontend"]);
    supervisor
        .resume("frontend", json!({ "fe_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(supervisor.pending_tasks(), vec!["end"]);
    supervisor.resume("end", json!({ "final": true })).unwrap();

    let _ = exec_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_fork_join_with_multi_level_branches() {
    let nodes = vec![
        WorkflowNode::fork("fork"),
        WorkflowNode::agent("long_branch_a"),
        WorkflowNode::agent("long_branch_b"),
        WorkflowNode::agent("long_branch_c"),
        WorkflowNode::join("join"),
        WorkflowNode::agent("end"),
    ];
    let edges = vec![
        WorkflowEdge::new("fork", "long_branch_a"),
        WorkflowEdge::new("fork", "long_branch_b"),
        WorkflowEdge::new("long_branch_a", "long_branch_c"),
        WorkflowEdge::new("long_branch_b", "join"),
        WorkflowEdge::new("long_branch_c", "join"),
        WorkflowEdge::new("join", "end"),
    ];

    let def = WorkflowDefinition::new("test_multi_level_fork_join", 1, "fork", nodes, edges);
    let validated = def.validate().expect("valid workflow");

    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();

    let executor = Executor::new(validated, Arc::clone(&store), supervisor.clone());

    let exec_task = tokio::spawn(async move {
        let mut exec = executor;
        exec.run(json!({ "init": true })).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Fork passed through → both branches start
    let mut pending = supervisor.pending_tasks();
    pending.sort();
    assert_eq!(pending, vec!["long_branch_a", "long_branch_b"]);

    supervisor
        .resume("long_branch_a", json!({ "a_done": true }))
        .unwrap();
    supervisor
        .resume("long_branch_b", json!({ "b_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Branch B reached Join (1 of 2), Branch A advances to branch C
    assert_eq!(supervisor.pending_tasks(), vec!["long_branch_c"]);

    supervisor
        .resume("long_branch_c", json!({ "c_done": true }))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // All branches complete → Join barrier satisfied → end runs
    assert_eq!(supervisor.pending_tasks(), vec!["end"]);
    supervisor.resume("end", json!({ "final": true })).unwrap();

    let _ = exec_task.await.unwrap().unwrap();
}
