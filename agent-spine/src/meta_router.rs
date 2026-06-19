use std::path::PathBuf;

use tracing;

use crate::mcp_bridge::{McpBridge, RouteLimits};

/// Select the appropriate workflow YAML for a given task description
/// by querying agent-brain for routing recommendations at run start.
pub struct MetaRouter {
    workflows_dir: PathBuf,
}

impl MetaRouter {
    pub fn new(workflows_dir: PathBuf) -> Self {
        Self { workflows_dir }
    }

    /// Query agent-brain to select a workflow for the given task prompt.
    ///
    /// Returns `Some(workflow_path)` when brain recommends a specific workflow,
    /// or `None` to use the default workflow provided by the caller.
    pub fn select_workflow(&self, task_prompt: &str) -> Option<PathBuf> {
        let rt = tokio::runtime::Handle::try_current().ok()?;

        let mut bridge = match rt.block_on(McpBridge::connect(None)) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("MetaRouter: agent-brain unavailable ({e}), using default workflow");
                return None;
            }
        };

        let message = format!(
            r#"Task: {task_prompt}

Select a workflow YAML file from the workflows directory at `{}`.
Return the filename (without path) of the most appropriate workflow.
Available workflows are determined by scanning the directory."#,
            self.workflows_dir.display()
        );

        let resp = match rt.block_on(bridge.route_task(
            &message,
            Some(&self.workflows_dir),
            &[],
            300,
            RouteLimits {
                agents: 1,
                skills: 0,
                rules: 0,
                memory: 0,
            },
            Some("selecting"),
            Some("planning"),
        )) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("MetaRouter: brain routing failed ({e}), using default workflow");
                return None;
            }
        };

        // Extract workflow name from brain briefing or fallback
        extract_workflow_from_response(&resp.briefing, &self.workflows_dir)
    }
}

fn extract_workflow_from_response(
    briefing: &str,
    workflows_dir: &std::path::Path,
) -> Option<PathBuf> {
    // Try to find a workflow filename mentioned in the briefing
    let words: Vec<&str> = briefing.split_whitespace().collect();
    for word in &words {
        let clean = word.trim_matches(|c: char| c.is_ascii_punctuation());
        if clean.ends_with(".yaml") || clean.ends_with(".yml") {
            let candidate = workflows_dir.join(clean);
            if candidate.exists() {
                tracing::info!("MetaRouter: selected workflow '{}'", candidate.display());
                return Some(candidate);
            }
        }
    }

    // Fallback: scan directory and look for keyword match in briefing
    if let Ok(entries) = std::fs::read_dir(workflows_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_lowercase();
            if !name.ends_with(".yaml") && !name.ends_with(".yml") {
                continue;
            }
            let stem = name.trim_end_matches(".yaml").trim_end_matches(".yml");
            if briefing.to_lowercase().contains(stem) {
                let candidate = entry.path();
                tracing::info!("MetaRouter: matched workflow '{}'", candidate.display());
                return Some(candidate);
            }
        }
    }

    None
}

/// Determine whether agent-brain should be used as a meta-router
/// for a given CLI invocation.
///
/// If a `--meta` flag is present with a prompt value, the meta-router
/// is invoked before execution.
pub fn should_use_meta_router(task_prompt: Option<&str>) -> bool {
    task_prompt.is_some_and(|p| !p.trim().is_empty())
}
