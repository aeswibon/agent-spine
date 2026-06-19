use serde_json::Value;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

use crate::WorkflowState;
use crate::supervisor::Supervisor;

pub mod pb {
    tonic::include_proto!("agent_spine");
}

use pb::dashboard_service_server::DashboardService;
use pb::supervisor_service_server::SupervisorService;
use pb::{
    GetExecutionHistoryRequest, GetExecutionHistoryResponse, GetPendingTasksRequest,
    GetPendingTasksResponse, ListExecutionsRequest, ListExecutionsResponse, PendingTask,
    ResumeRequest, ResumeResponse, StateSnapshot as PbStateSnapshot,
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
        let pending = self.supervisor.pending_tasks();
        let tasks = pending
            .into_iter()
            .map(|node_name| PendingTask { node_name })
            .collect();
        Ok(Response::new(GetPendingTasksResponse { tasks }))
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
}
