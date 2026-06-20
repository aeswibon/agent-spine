use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::executor::Executor;
use crate::global_workspace;
use crate::state::SqliteStateStore;
use crate::supervisor::Supervisor;
use crate::{ExecutionId, WorkflowDefinition, WorkflowState};

#[derive(Clone)]
pub struct AutonomicApi {
    pub db_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct WorkflowRunRequest {
    pub workflow: PathBuf,
    #[serde(default = "default_payload")]
    pub payload: Value,
    #[serde(default)]
    pub brain: bool,
    pub meta: Option<String>,
}

fn default_payload() -> Value {
    Value::Object(Default::default())
}

#[derive(Debug, Serialize)]
pub struct WorkflowRunResponse {
    pub execution_id: String,
    pub workflow_name: String,
    pub graph_path: String,
    pub dag_path: String,
}

#[derive(Debug, Serialize)]
pub struct AutonomicHealth {
    pub status: &'static str,
    pub version: &'static str,
    pub state_db: String,
    pub workspace_root: String,
}

impl AutonomicApi {
    pub fn new(db_path: PathBuf) -> Self {
        Self { db_path }
    }

    pub fn router(self) -> Router {
        use axum::routing::{get, post};

        let state = Arc::new(self);
        Router::new()
            .route("/api/v1/health", get(Self::health))
            .route("/api/v1/workflows/run", post(Self::run_workflow))
            .route("/api/v1/executions/{id}", get(Self::get_execution))
            .with_state(state)
    }

    async fn health(State(api): State<Arc<Self>>) -> Json<AutonomicHealth> {
        Json(AutonomicHealth {
            status: "ok",
            version: env!("CARGO_PKG_VERSION"),
            state_db: api.db_path.display().to_string(),
            workspace_root: global_workspace::autonomic_root().display().to_string(),
        })
    }

    async fn run_workflow(
        State(api): State<Arc<Self>>,
        Json(req): Json<WorkflowRunRequest>,
    ) -> Result<Json<WorkflowRunResponse>, (StatusCode, String)> {
        let workflow_path = match req.meta {
            Some(ref prompt) => {
                let workflows_dir = req.workflow.parent().unwrap_or(&req.workflow);
                let router = crate::meta_router::MetaRouter::new(workflows_dir.to_path_buf());
                router
                    .select_workflow(prompt)
                    .unwrap_or_else(|| req.workflow.clone())
            }
            None => req.workflow.clone(),
        };

        let validated = WorkflowDefinition::from_path(&workflow_path)
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
            .validate()
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

        let workflow_name = validated.definition().name().to_string();
        let store = SqliteStateStore::new(&api.db_path)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let store = Arc::new(Mutex::new(store));
        let supervisor = Supervisor::new();
        let mut executor = Executor::new(validated, store.clone(), supervisor);
        if req.brain {
            executor = executor.with_brain(None);
        }

        let execution_id = executor
            .run(req.payload)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let history = {
            let guard = store.lock().map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "state lock poisoned".into(),
                )
            })?;
            guard.history(execution_id)
        };

        let graph_path = {
            let guard = store.lock().map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "state lock poisoned".into(),
                )
            })?;
            global_workspace::export_execution_graph(&*guard, execution_id, &workflow_name)
        }
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let dag_path = global_workspace::export_dag_summary(&history, execution_id)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        Ok(Json(WorkflowRunResponse {
            execution_id: execution_id.to_string(),
            workflow_name,
            graph_path: graph_path.display().to_string(),
            dag_path: dag_path.display().to_string(),
        }))
    }

    async fn get_execution(
        State(api): State<Arc<Self>>,
        Path(id): Path<String>,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        let execution_id: ExecutionId = id
            .parse()
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid execution id".into()))?;

        let store = SqliteStateStore::new(&api.db_path)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let history = store.history(execution_id);
        if history.is_empty() {
            return Err((StatusCode::NOT_FOUND, "execution not found".into()));
        }

        Ok(Json(serde_json::json!({
            "execution_id": execution_id.to_string(),
            "snapshot_count": history.len(),
            "snapshots": history,
        })))
    }
}

pub fn merge_autonomic_routes(router: Router, db_path: PathBuf) -> Router {
    router.merge(AutonomicApi::new(db_path).router())
}
