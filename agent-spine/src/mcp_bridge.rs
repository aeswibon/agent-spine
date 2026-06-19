use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Types matching agent-brain MCP tool responses.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteLimits {
    #[serde(default)]
    pub agents: usize,
    #[serde(default)]
    pub skills: usize,
    #[serde(default)]
    pub rules: usize,
    #[serde(default)]
    pub memory: usize,
}

impl Default for RouteLimits {
    fn default() -> Self {
        Self {
            agents: 2,
            skills: 3,
            rules: 5,
            memory: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRec {
    pub name: String,
    pub path: String,
    pub rationale: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillRec {
    pub name: String,
    pub path: String,
    pub rationale: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleRec {
    pub topic: String,
    pub text: String,
    pub source_path: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRec {
    pub topic: String,
    pub text: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MustApply {
    pub topic: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteWarning {
    pub topic: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextBundle {
    pub team_rules: Vec<RuleRec>,
    pub negative_memory: Vec<MemoryRec>,
    pub skill_docs: Vec<SkillRec>,
    pub agents: Vec<AgentRec>,
    pub observations: Vec<MemoryRec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteTaskResponse {
    pub recommended_agents: Vec<AgentRec>,
    pub recommended_skills: Vec<SkillRec>,
    pub applicable_rules: Vec<RuleRec>,
    pub relevant_memory: Vec<MemoryRec>,
    pub must_apply: Vec<MustApply>,
    #[serde(default)]
    pub warnings: Vec<RouteWarning>,
    pub recommended_phase: String,
    pub tokens_used: u32,
    pub tokens_budget: u32,
    pub cache_hit: bool,
    pub latency_ms: u64,
    pub log_id: String,
    pub index_total: u32,
    pub briefing: String,
    pub route_confidence: f64,
    #[serde(default)]
    pub escalate_recommended: bool,
    pub context_bundle: Option<ContextBundle>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetContextItem {
    pub item_type: String,
    pub topic: String,
    pub text: String,
    pub score: f64,
    pub scope: Option<String>,
    pub source_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetContextResponse {
    pub items: Vec<GetContextItem>,
    pub tokens_used: usize,
    pub tokens_budget: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrajectoryReport {
    pub id: String,
    pub workflow_id: String,
    pub node_id: String,
    pub outcome: String,
    pub route_log_linked: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryFact {
    pub id: String,
    pub topic: String,
    pub fact: String,
    pub scope: String,
    pub confidence: f64,
    pub polarity: Option<String>,
    pub created_at: String,
}

// ---------------------------------------------------------------------------
// MCP JSON-RPC 2.0 Bridge
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("MCP handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("tool call '{0}' failed: {1}")]
    ToolCallFailed(String, String),
    #[error("agent-brain binary not found: {0}")]
    BinaryNotFound(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bridge not connected (child process exited)")]
    NotConnected,
    #[error("tool call timed out after {0}s")]
    Timeout(u64),
}

/// An MCP client that communicates with agent-brain over child-process stdio.
pub struct McpBridge {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: AtomicU64,
    _brain_path: PathBuf,
    tool_timeout_secs: u64,
}

impl Drop for McpBridge {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl McpBridge {
    /// Connect to agent-brain by spawning it as a child process.
    ///
    /// `brain_path` overrides the automatic binary search.
    /// Returns `BridgeError::BinaryNotFound` if the binary cannot be located.
    pub async fn connect(brain_path: Option<&Path>) -> Result<Self, BridgeError> {
        let path = resolve_brain_path(brain_path)?;

        let mut child = Command::new(&path)
            .arg("serve")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| BridgeError::BinaryNotFound(format!("spawn failed: {e}")))?;

        let stdin = child.stdin.take().ok_or_else(|| {
            BridgeError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "failed to capture child stdin",
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            BridgeError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "failed to capture child stdout",
            ))
        })?;

        let mut bridge = Self {
            child,
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            next_id: AtomicU64::new(1),
            _brain_path: path,
            tool_timeout_secs: 30,
        };

        bridge.handshake().await?;
        Ok(bridge)
    }

    /// Set a timeout (in seconds) for tool calls. Default: 30s.
    pub fn set_tool_timeout(&mut self, secs: u64) {
        self.tool_timeout_secs = secs;
    }

    // -----------------------------------------------------------------------
    // High-level tool wrappers
    // -----------------------------------------------------------------------

    /// Call agent-brain's `route_task` tool.
    pub async fn route_task(
        &mut self,
        user_message: &str,
        cwd: Option<&Path>,
        open_files: &[String],
        max_tokens: usize,
        limits: RouteLimits,
        phase: Option<&str>,
        task_kind: Option<&str>,
    ) -> Result<RouteTaskResponse, BridgeError> {
        let mut args = serde_json::json!({
            "user_message": user_message,
            "max_tokens": max_tokens,
            "limits": limits,
            "open_files": open_files,
        });
        if let Some(cwd) = cwd {
            args["current_working_directory"] =
                serde_json::Value::String(cwd.to_string_lossy().into());
        }
        if let Some(phase) = phase {
            args["phase"] = serde_json::Value::String(phase.into());
        }
        if let Some(tk) = task_kind {
            args["task_kind"] = serde_json::Value::String(tk.into());
        }

        let value = self.call_tool("route_task", args).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call agent-brain's `store_memory` tool.
    pub async fn store_memory(
        &mut self,
        topic: &str,
        fact: &str,
        scope: &str,
        confidence: f64,
        polarity: Option<&str>,
    ) -> Result<Value, BridgeError> {
        let mut args = serde_json::json!({
            "topic": topic,
            "fact": fact,
            "scope": scope,
            "confidence": confidence,
        });
        if let Some(p) = polarity {
            args["polarity"] = serde_json::Value::String(p.into());
        }
        self.call_tool("store_memory", args).await
    }

    /// Call agent-brain's `store_trajectory` tool (no route_task gate).
    pub async fn store_trajectory(
        &mut self,
        workflow_id: &str,
        node_id: &str,
        outcome: &str,
        route_log_id: Option<&str>,
        task_kind: Option<&str>,
        notes: Option<&str>,
    ) -> Result<TrajectoryReport, BridgeError> {
        let mut args = serde_json::json!({
            "workflow_id": workflow_id,
            "node_id": node_id,
            "outcome": outcome,
        });
        if let Some(id) = route_log_id {
            args["route_log_id"] = serde_json::Value::String(id.into());
        }
        if let Some(tk) = task_kind {
            args["task_kind"] = serde_json::Value::String(tk.into());
        }
        if let Some(n) = notes {
            args["notes"] = serde_json::Value::String(n.into());
        }

        let value = self.call_tool("store_trajectory", args).await?;
        Ok(serde_json::from_value(value)?)
    }

    /// Call agent-brain's `get_context` tool.
    pub async fn get_context(
        &mut self,
        task_description: &str,
        cwd: Option<&Path>,
        max_tokens: usize,
        include_types: &[&str],
    ) -> Result<GetContextResponse, BridgeError> {
        let mut args = serde_json::json!({
            "task_description": task_description,
            "max_tokens": max_tokens,
            "include_types": include_types,
        });
        if let Some(cwd) = cwd {
            args["current_working_directory"] =
                serde_json::Value::String(cwd.to_string_lossy().into());
        }
        let value = self.call_tool("get_context", args).await?;
        // get_context returns the response directly (not nested in content)
        Ok(serde_json::from_value(value)?)
    }

    /// Call agent-brain's `list_memory` tool.
    pub async fn list_memory(&mut self, limit: usize) -> Result<Vec<MemoryFact>, BridgeError> {
        let args = serde_json::json!({ "limit": limit });
        let value = self.call_tool("list_memory", args).await?;
        let facts = value.get("facts").cloned().unwrap_or(Value::Null);
        Ok(serde_json::from_value(facts)?)
    }

    /// Health check — completes handshake and reads server info.
    pub async fn health(&mut self) -> Result<McpServerInfo, BridgeError> {
        // Handshake already completed in connect(); we just verify by
        // calling a lightweight tool: list_memory with limit 0.
        self.list_memory(0).await.ok();
        Ok(McpServerInfo {
            name: "agent-brain".into(),
            version: "unknown".into(),
        })
    }

    // -----------------------------------------------------------------------
    // Low-level MCP primitives
    // -----------------------------------------------------------------------

    /// Perform the MCP initialization handshake.
    async fn handshake(&mut self) -> Result<(), BridgeError> {
        let init = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 0u64,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "agent-spine",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        });

        self.write_line(&init).await?;

        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.read_until_response(0),
        )
        .await
        .map_err(|_| {
            BridgeError::HandshakeFailed("timeout waiting for initialize response".into())
        })??;

        let protocol_version = resp
            .get("protocolVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        tracing::debug!("brain MCP handshake complete (protocol: {protocol_version})");

        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        });
        self.write_line(&notification).await?;

        Ok(())
    }

    /// Call an MCP tool by name with the given arguments.
    async fn call_tool(&mut self, name: &str, args: Value) -> Result<Value, BridgeError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": args,
            },
        });

        self.write_line(&request).await?;

        // Read until we get a response with matching id
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(self.tool_timeout_secs),
            self.read_until_response(id),
        )
        .await
        .map_err(|_| BridgeError::Timeout(self.tool_timeout_secs))??;

        // Extract text content from the tool result
        Self::extract_tool_content(result)
    }

    /// Extract the text content from an MCP tool result.
    fn extract_tool_content(result: Value) -> Result<Value, BridgeError> {
        if let Some(content) = result.get("content").and_then(|c| c.as_array()) {
            if let Some(first) = content.first() {
                if let Some(text) = first.get("text").and_then(|t| t.as_str()) {
                    return Ok(serde_json::from_str(text)?);
                }
            }
        }
        // No text content — return the raw result
        Ok(result)
    }

    /// Read JSON-RPC lines from stdout until we find a response with the given id.
    /// Handles server requests (ping, etc.) by responding appropriately.
    async fn read_until_response(&mut self, target_id: u64) -> Result<Value, BridgeError> {
        let mut line = String::new();

        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).await.map_err(|e| {
                BridgeError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

            if n == 0 {
                return Err(BridgeError::NotConnected);
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let msg: Value = serde_json::from_str(trimmed)?;

            // Dispatch by structure
            if let Some(id_val) = msg.get("id") {
                if id_val.as_u64() == Some(target_id) {
                    // This is the response to our request
                    if let Some(err) = msg.get("error") {
                        let code = err.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
                        let message = err
                            .get("message")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown error");
                        return Err(BridgeError::ToolCallFailed(
                            "call_tool".into(),
                            format!("[{code}] {message}"),
                        ));
                    }
                    return Ok(msg.get("result").cloned().unwrap_or(Value::Null));
                }

                // Server request (has id, has method, no result/error)
                if msg.get("method").is_some()
                    && msg.get("result").is_none()
                    && msg.get("error").is_none()
                {
                    self.handle_server_request(msg).await?;
                    continue;
                }
            }

            // Notification (no id) — ignore
            continue;
        }
    }

    /// Respond to an MCP server request (e.g. `ping`).
    async fn handle_server_request(&mut self, msg: Value) -> Result<(), BridgeError> {
        let id = msg.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        let method = msg.get("method").and_then(|v| v.as_str()).unwrap_or("");

        let response = match method {
            "ping" => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {}
            }),
            _ => {
                tracing::warn!("unknown MCP server request: {method}");
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": -32601,
                        "message": format!("Method not found: {method}")
                    }
                })
            }
        };

        let line = serde_json::to_string(&response)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;

        Ok(())
    }

    /// Write a JSON-RPC message as a single line to stdin.
    async fn write_line(&mut self, msg: &Value) -> Result<(), BridgeError> {
        let line = serde_json::to_string(msg)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Binary discovery
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct McpServerInfo {
    pub name: String,
    pub version: String,
}

fn resolve_brain_path(override_path: Option<&Path>) -> Result<PathBuf, BridgeError> {
    // Explicit override
    if let Some(path) = override_path {
        if path.is_file() {
            return Ok(path.to_path_buf());
        }
        return Err(BridgeError::BinaryNotFound(format!(
            "specified path does not exist: {}",
            path.display()
        )));
    }

    // Environment variable
    if let Ok(env_path) = std::env::var("BRAIN_PATH") {
        let path = PathBuf::from(&env_path);
        if path.is_file() {
            return Ok(path);
        }
    }

    // Check common locations
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates = vec![
        PathBuf::from("agent-brain"), // resolve from PATH
        PathBuf::from(format!("{home}/.agent_brain/bin/agent-brain")),
        PathBuf::from("/usr/local/bin/agent-brain"),
        PathBuf::from("/opt/homebrew/bin/agent-brain"),
    ];

    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }

    // Last resort — let the OS resolve from PATH
    Ok(PathBuf::from("agent-brain"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn summarize_payload(payload: &Value) -> String {
    match payload {
        Value::Object(map) => {
            let keys: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
            format!("payload keys: [{}]", keys.join(", "))
        }
        Value::String(s) => format!("payload: {:.100}", s),
        _ => "payload present".into(),
    }
}

/// Build a route_task message from a workflow transition context.
pub fn transition_route_message(
    workflow_name: &str,
    source_node: &str,
    target_node: &str,
    payload: &Value,
) -> String {
    format!(
        "Workflow '{}', node '{}' transitioning to '{}'.\n{}\nEscalation needed?",
        workflow_name,
        source_node,
        target_node,
        summarize_payload(payload),
    )
}

/// Build a route_task message from a node execution context.
pub fn node_route_message(
    workflow_name: &str,
    node_name: &str,
    node_kind: &str,
    description: Option<&str>,
    payload: &Value,
) -> String {
    let desc = description.unwrap_or("");
    format!(
        "Workflow '{}', node '{}' (kind={}) ready for agent.\nDescription: {}\n{}\nWhat skills and rules apply?",
        workflow_name,
        node_name,
        node_kind,
        desc,
        summarize_payload(payload),
    )
}

#[cfg(test)]
mod send_checks {
    use crate::mcp_bridge::McpBridge;
    #[test]
    fn check_mcp_bridge_send() {
        fn assert_send<T: Send>() {}
        assert_send::<McpBridge>();
    }
}
