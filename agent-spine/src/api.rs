use serde_json::Value;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

use crate::WorkflowState;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};

use crate::supervisor::{Supervisor, WorkflowEvent};

pub mod pb {
    tonic::include_proto!("agent_spine");
}

use pb::dashboard_service_server::DashboardService;
use pb::supervisor_service_server::SupervisorService;
use pb::{
    GetExecutionHistoryRequest, GetExecutionHistoryResponse, GetPendingTaskDetailRequest,
    GetPendingTaskDetailResponse, GetPendingTasksRequest, GetPendingTasksResponse,
    ListExecutionsRequest, ListExecutionsResponse, PendingTask, ResumeRequest, ResumeResponse,
    StateSnapshot as PbStateSnapshot, WatchEventsRequest, WorkflowEvent as PbWorkflowEvent,
};

#[derive(Clone)]
pub struct DashboardApi {
    pub store: Arc<Mutex<dyn WorkflowState>>,
}

#[tonic::async_trait]
impl DashboardService for DashboardApi {
    async fn list_executions(
        &self,
        _request: Request<ListExecutionsRequest>,
    ) -> Result<Response<ListExecutionsResponse>, Status> {
        let store = self.store.lock().unwrap();
        match store.list_executions() {
            Ok(ids) => {
                let execution_ids = ids.iter().map(|id| id.to_string()).collect();
                Ok(Response::new(ListExecutionsResponse { execution_ids }))
            }
            Err(e) => Err(Status::internal(format!(
                "Failed to list executions: {}",
                e
            ))),
        }
    }

    async fn get_execution_history(
        &self,
        request: Request<GetExecutionHistoryRequest>,
    ) -> Result<Response<GetExecutionHistoryResponse>, Status> {
        let req = request.into_inner();
        let execution_id = std::str::FromStr::from_str(&req.execution_id)
            .map_err(|_| Status::invalid_argument("Invalid execution ID format"))?;

        let store = self.store.lock().unwrap();
        let history = store.history(execution_id);

        if history.is_empty() {
            return Err(Status::not_found("Execution not found"));
        }

        let pb_history = history
            .into_iter()
            .map(|snap| PbStateSnapshot {
                execution_id: snap.execution_id().to_string(),
                sequence: snap.sequence(),
                payload_json: serde_json::to_string(snap.payload()).unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(GetExecutionHistoryResponse {
            history: pb_history,
        }))
    }
}

#[derive(Clone)]
pub struct SupervisorApi {
    pub supervisor: Supervisor,
}

#[tonic::async_trait]
impl SupervisorService for SupervisorApi {
    async fn get_pending_tasks(
        &self,
        _request: Request<GetPendingTasksRequest>,
    ) -> Result<Response<GetPendingTasksResponse>, Status> {
        let names = self.supervisor.pending_tasks();
        let tasks = names
            .into_iter()
            .map(|node_name| {
                let info = self.supervisor.pending_task_info(&node_name);
                PendingTask {
                    node_name,
                    node_kind: info.as_ref().map_or(String::new(), |m| m.node_kind.clone()),
                    description: info
                        .as_ref()
                        .map_or(String::new(), |m| m.description.clone().unwrap_or_default()),
                    workflow_name: info
                        .as_ref()
                        .map_or(String::new(), |m| m.workflow_name.clone()),
                }
            })
            .collect();
        Ok(Response::new(GetPendingTasksResponse { tasks }))
    }

    async fn get_pending_task_detail(
        &self,
        request: Request<GetPendingTaskDetailRequest>,
    ) -> Result<Response<GetPendingTaskDetailResponse>, Status> {
        let req = request.into_inner();
        let info = self
            .supervisor
            .pending_task_info(&req.node_name)
            .ok_or_else(|| Status::not_found("no pending task for node"))?;

        Ok(Response::new(GetPendingTaskDetailResponse {
            node_name: info.node_name,
            node_kind: info.node_kind,
            description: info.description.unwrap_or_default(),
            workflow_name: info.workflow_name,
            payload_json: serde_json::to_string(&info.payload).unwrap_or_default(),
        }))
    }

    async fn resume_execution(
        &self,
        request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        let req = request.into_inner();
        let payload: Value = serde_json::from_str(&req.payload_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON payload: {}", e)))?;

        match self.supervisor.resume(&req.node_name, payload) {
            Ok(_) => Ok(Response::new(ResumeResponse {
                success: true,
                error_message: String::new(),
            })),
            Err(e) => Ok(Response::new(ResumeResponse {
                success: false,
                error_message: e.to_string(),
            })),
        }
    }

    type WatchEventsStream = Pin<Box<dyn Stream<Item = Result<PbWorkflowEvent, Status>> + Send>>;

    #[allow(clippy::result_large_err)]
    async fn watch_events(
        &self,
        _request: Request<WatchEventsRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        let rx = self.supervisor.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx);
        let mapped = stream.map(|result| match result {
            Ok(event) => Ok(PbWorkflowEvent::from(event)),
            Err(_) => Err(tonic::Status::internal("event stream lagged behind")),
        });
        Ok(Response::new(Box::pin(mapped)))
    }
}

impl From<WorkflowEvent> for PbWorkflowEvent {
    fn from(event: WorkflowEvent) -> Self {
        let (
            event_type,
            node_name,
            node_kind,
            description,
            workflow_name,
            execution_id,
            error,
            payload_json,
        ) = match event {
            WorkflowEvent::NodeStarted {
                node_name,
                node_kind,
                description,
                workflow_name,
            } => (
                "node_started".to_owned(),
                node_name,
                node_kind,
                description,
                workflow_name,
                String::new(),
                String::new(),
                String::new(),
            ),
            WorkflowEvent::NodeCompleted {
                node_name,
                node_kind,
            } => (
                "node_completed".to_owned(),
                node_name,
                node_kind,
                None,
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ),
            WorkflowEvent::NodeFailed {
                node_name,
                node_kind,
                error,
            } => (
                "node_failed".to_owned(),
                node_name,
                node_kind,
                None,
                String::new(),
                String::new(),
                error,
                String::new(),
            ),
            WorkflowEvent::PendingApproval {
                node_name,
                description,
                payload,
            } => (
                "pending_approval".to_owned(),
                node_name,
                String::new(),
                description,
                String::new(),
                String::new(),
                String::new(),
                serde_json::to_string(&payload).unwrap_or_default(),
            ),
            WorkflowEvent::WorkflowCompleted {
                execution_id,
                workflow_name,
            } => (
                "workflow_completed".to_owned(),
                String::new(),
                String::new(),
                None,
                workflow_name,
                execution_id,
                String::new(),
                String::new(),
            ),
            WorkflowEvent::WorkflowFailed {
                execution_id,
                workflow_name,
                error,
            } => (
                "workflow_failed".to_owned(),
                String::new(),
                String::new(),
                None,
                workflow_name,
                execution_id,
                error,
                String::new(),
            ),
        };

        PbWorkflowEvent {
            event_type,
            node_name,
            node_kind,
            description: description.unwrap_or_default(),
            workflow_name,
            execution_id,
            error,
            payload_json,
        }
    }
}
