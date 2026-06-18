use std::net::SocketAddr;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status, transport::Server};

use crate::supervisor::Supervisor;

pub mod proto {
    tonic::include_proto!("agent_spine");
}

use proto::supervisor_service_server::{SupervisorService, SupervisorServiceServer};
use proto::{
    GetPendingTasksRequest, GetPendingTasksResponse, PendingTask, ResumeRequest, ResumeResponse,
};

pub struct GrpcSupervisorService {
    supervisor: Supervisor,
}

impl GrpcSupervisorService {
    pub fn new(supervisor: Supervisor) -> Self {
        Self { supervisor }
    }
}

#[tonic::async_trait]
impl SupervisorService for GrpcSupervisorService {
    async fn resume_execution(
        &self,
        request: Request<ResumeRequest>,
    ) -> Result<Response<ResumeResponse>, Status> {
        let req = request.into_inner();

        let payload = serde_json::from_str(&req.payload_json)
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

    async fn get_pending_tasks(
        &self,
        _request: Request<GetPendingTasksRequest>,
    ) -> Result<Response<GetPendingTasksResponse>, Status> {
        let pending = self.supervisor.pending_tasks();

        let tasks = pending
            .into_iter()
            .map(|name| PendingTask { node_name: name })
            .collect();

        Ok(Response::new(GetPendingTasksResponse { tasks }))
    }
}

/// Start the gRPC server.
pub async fn serve(
    supervisor: Supervisor,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = GrpcSupervisorService::new(supervisor);

    Server::builder()
        .add_service(SupervisorServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
