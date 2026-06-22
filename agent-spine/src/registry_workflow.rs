//! Resolve workflow YAML from agent-brain Autonomic Registry cache (`@alias`).

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct WorkflowsRegistryFile {
    workflows: std::collections::BTreeMap<String, WorkflowEntry>,
}

#[derive(Debug, Deserialize)]
struct WorkflowEntry {
    path: String,
    #[serde(default)]
    description: Option<String>,
}

/// Resolve `release-notes`, `@release-notes`, or built-in name to YAML content.
pub fn resolve_workflow_yaml(name: &str) -> Option<(String, String)> {
    let trimmed = name.trim().strip_prefix('@').unwrap_or(name.trim());
    if trimmed.is_empty() {
        return None;
    }
    if let Some(entry) = crate::workflows::find(trimmed) {
        return Some((entry.name.to_string(), entry.yaml.to_string()));
    }
    registry_workflow_yaml(trimmed)
}

fn registry_workflow_yaml(alias: &str) -> Option<(String, String)> {
    let home = brain_home();
    let workflows_json = read_registry_file(&home, "workflows.json")?;
    let reg: WorkflowsRegistryFile = serde_json::from_str(&workflows_json).ok()?;
    let entry = reg.workflows.get(alias)?;
    let rel = entry.path.trim_start_matches('/');
    let path = home.join("registry-cache").join(rel);
    let yaml = fs::read_to_string(&path).ok()?;
    Some((alias.to_string(), yaml))
}

fn brain_home() -> PathBuf {
    std::env::var("AGENT_BRAIN_HOME")
        .map(PathBuf::from)
        .or_else(|_| dirs::home_dir().map(|h| h.join(".agent_brain")).ok_or(()))
        .unwrap_or_else(|_| PathBuf::from(".agent_brain"))
}

fn read_registry_file(home: &Path, name: &str) -> Option<String> {
    let path = home.join("registry-cache").join(name);
    fs::read_to_string(path).ok()
}

/// List `@alias` workflows from agent-brain registry cache.
pub fn list_registry_aliases() -> Vec<(String, String)> {
    let home = brain_home();
    let Some(raw) = read_registry_file(&home, "workflows.json") else {
        return Vec::new();
    };
    let Ok(reg) = serde_json::from_str::<WorkflowsRegistryFile>(&raw) else {
        return Vec::new();
    };
    reg.workflows
        .into_iter()
        .map(|(alias, entry)| {
            let desc = entry
                .description
                .unwrap_or_else(|| "registry workflow".into());
            (alias, desc)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_still_resolves() {
        let (name, yaml) = resolve_workflow_yaml("universal-developer").unwrap();
        assert_eq!(name, "universal-developer");
        assert!(yaml.contains("nodes:"));
    }
}
