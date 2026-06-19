use agent_spine::executor::Executor;
use agent_spine::router::ConfidenceRouter;
use agent_spine::state::InMemoryStateStore;
use agent_spine::supervisor::Supervisor;
use agent_spine::workflow::{NodeKind, WorkflowDefinition, WorkflowEdge, WorkflowNode};
use std::sync::{Arc, Mutex};

#[test]
fn check_send_types() {
    fn assert_send<T: Send>() {}
    assert_send::<ConfidenceRouter>();
    let nodes = vec![WorkflowNode::new("start", NodeKind::Agent)];
    let edges = vec![WorkflowEdge::new("start", "start")];
    let def = WorkflowDefinition::new("test", 1, "start", nodes, edges);
    let validated = def.validate().unwrap();
    let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
    let supervisor = Supervisor::new();
    let router = ConfidenceRouter::new(3);
    let _executor = Executor::new(validated, store, supervisor, router);
    assert_send::<Executor<InMemoryStateStore>>();
}
