use agent_spine::{NodeKind, WorkflowDefinition, WorkflowEdge, WorkflowNode};

#[test]
fn valid_workflow_is_accepted() {
    let workflow = WorkflowDefinition::new(
        "agent_pipeline",
        1,
        "collect",
        vec![
            WorkflowNode::agent("collect"),
            WorkflowNode::agent("plan"),
            WorkflowNode::new("implement", NodeKind::Verify),
        ],
        vec![
            WorkflowEdge::new("collect", "plan"),
            WorkflowEdge::new("plan", "implement"),
        ],
    );

    let validated = workflow.validate().expect("workflow should validate");
    assert_eq!(validated.definition().start_node(), "collect");
}

#[test]
fn duplicate_nodes_are_rejected() {
    let workflow = WorkflowDefinition::new(
        "duplicate_nodes",
        1,
        "collect",
        vec![
            WorkflowNode::agent("collect"),
            WorkflowNode::agent("collect"),
        ],
        vec![],
    );

    let error = workflow
        .validate()
        .expect_err("duplicate node names must fail");

    assert_eq!(error.to_string(), "duplicate workflow node name: collect");
}

#[test]
fn missing_nodes_are_rejected() {
    let workflow = WorkflowDefinition::new("empty", 1, "start", vec![], vec![]);

    let error = workflow.validate().expect_err("empty workflows must fail");

    assert_eq!(error.to_string(), "workflow must declare at least one node");
}

#[test]
fn cycles_are_allowed_now() {
    let workflow = WorkflowDefinition::new(
        "cycle",
        1,
        "a",
        vec![WorkflowNode::agent("a"), WorkflowNode::agent("b")],
        vec![WorkflowEdge::new("a", "b"), WorkflowEdge::new("b", "a")],
    );

    workflow
        .validate()
        .expect("cycles are now allowed in state machines");
}

#[test]
fn unknown_edges_are_rejected() {
    let workflow = WorkflowDefinition::new(
        "missing_edge",
        1,
        "collect",
        vec![WorkflowNode::agent("collect")],
        vec![WorkflowEdge::new("collect", "plan")],
    );

    let error = workflow
        .validate()
        .expect_err("unknown endpoints must fail");

    assert_eq!(
        error.to_string(),
        "workflow edge references unknown node: plan"
    );
}

#[test]
fn missing_start_node_is_rejected() {
    let workflow = WorkflowDefinition::new(
        "missing_start",
        1,
        "foo",
        vec![WorkflowNode::agent("collect")],
        vec![],
    );

    let error = workflow
        .validate()
        .expect_err("missing start node must fail");

    assert_eq!(error.to_string(), "start_node references unknown node: foo");
}

#[test]
fn conditional_edge_is_accepted() {
    let workflow = WorkflowDefinition::new(
        "conditional_edges",
        1,
        "router",
        vec![
            WorkflowNode::router("router"),
            WorkflowNode::agent("frontend"),
            WorkflowNode::agent("end"),
        ],
        vec![
            WorkflowEdge::conditional("router", "frontend", r#"state.task_type == "frontend""#),
            WorkflowEdge::new("frontend", "end"),
        ],
    );

    let validated = workflow
        .validate()
        .expect("conditional edges must validate");
    let edge = &validated.definition().edges()[0];
    assert_eq!(edge.condition(), Some(r#"state.task_type == "frontend""#));
}

#[test]
fn fork_join_workflow_is_accepted() {
    let workflow = WorkflowDefinition::new(
        "fork_join",
        1,
        "fork",
        vec![
            WorkflowNode::fork("fork"),
            WorkflowNode::agent("a"),
            WorkflowNode::agent("b"),
            WorkflowNode::join("join"),
            WorkflowNode::agent("end"),
        ],
        vec![
            WorkflowEdge::new("fork", "a"),
            WorkflowEdge::new("fork", "b"),
            WorkflowEdge::new("a", "join"),
            WorkflowEdge::new("b", "join"),
            WorkflowEdge::new("join", "end"),
        ],
    );

    workflow
        .validate()
        .expect("fork/join workflow must validate");
}
