use rmcp::ServerHandler;
use rmcp::model::{CallToolResult, Content, ErrorData as McpError, ServerInfo};
use rmcp::serve_server;
use rmcp::tool;
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::PathBuf;

use crate::WorkflowManager;

#[derive(Clone)]
pub struct SpineMcp {
    wf_manager: WorkflowManager,
}

impl SpineMcp {
    pub fn new(db_path: PathBuf) -> Self {
        Self {
            wf_manager: WorkflowManager::new(db_path, false),
        }
    }

    pub async fn serve(db_path: PathBuf) -> anyhow::Result<()> {
        let server = Self::new(db_path);
        let service = serve_server(server, rmcp::transport::io::stdio()).await?;
        service.waiting().await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct SubmitWorkflowParams {
    yaml: String,
    name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CheckStatusParams {
    workflow_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ListWorkflowsParams {
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    10
}

#[tool(tool_box)]
impl SpineMcp {
    #[tool(
        description = "Submit a YAML workflow DAG definition for execution and return a workflow ID"
    )]
    async fn spine_submit_workflow(
        &self,
        #[tool(aggr)] params: SubmitWorkflowParams,
    ) -> Result<CallToolResult, McpError> {
        let payload = serde_json::Value::Object(Default::default());
        match self.wf_manager.submit_yaml(&params.yaml, payload) {
            Ok(execution_id) => {
                let result = serde_json::json!({
                    "execution_id": execution_id,
                    "status": "queued",
                });
                let text =
                    serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    #[tool(description = "Check the status of a workflow execution by ID")]
    async fn spine_check_status(
        &self,
        #[tool(aggr)] params: CheckStatusParams,
    ) -> Result<CallToolResult, McpError> {
        match self.wf_manager.execution_status(&params.workflow_id) {
            Some(running) => {
                let result = serde_json::json!({
                    "execution_id": running.execution_id,
                    "workflow_name": running.workflow_name,
                    "status": running.status,
                    "current_nodes": running.current_nodes,
                    "created_at": running.created_at,
                });
                let text =
                    serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            None => Err(McpError::internal_error(
                format!("workflow '{}' not found", params.workflow_id),
                None,
            )),
        }
    }

    #[tool(
        description = "List recent workflow executions with their IDs, names, statuses, and timestamps"
    )]
    async fn spine_list_workflows(
        &self,
        #[tool(aggr)] params: ListWorkflowsParams,
    ) -> Result<CallToolResult, McpError> {
        let workflows = self.wf_manager.list_executions();
        let recent: Vec<serde_json::Value> = workflows
            .into_iter()
            .take(params.limit as usize)
            .map(|wf| {
                serde_json::json!({
                    "execution_id": wf.execution_id,
                    "workflow_name": wf.workflow_name,
                    "status": wf.status,
                    "current_nodes": wf.current_nodes,
                    "created_at": wf.created_at,
                })
            })
            .collect();

        let text = serde_json::to_string_pretty(&recent).unwrap_or_else(|_| "[]".to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }
}

#[tool(tool_box)]
impl ServerHandler for SpineMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Workflow supervision MCP server for agent-spine. Tools: spine_submit_workflow (submit a YAML DAG definition), spine_check_status (poll workflow status by ID), spine_list_workflows (list recent workflows)."
                    .into(),
            ),
            ..Default::default()
        }
    }
}
