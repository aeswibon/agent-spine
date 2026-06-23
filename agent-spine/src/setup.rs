use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use agent_body_core::ui::ProgressRun;

use crate::WorkflowDefinition;

pub fn run_init(
    force: bool,
    dir: Option<PathBuf>,
    with: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = agent_body_core::run_legacy_migrations();
    let config_dir = dir.unwrap_or_else(agent_body_core::spine_config_dir);

    let mut progress = ProgressRun::new("agent-spine init").with_total_hint(4);

    if with.as_deref() == Some("list") {
        let step = progress.step("list workflows");
        println!("Available built-in workflows:");
        for w in crate::workflows::ALL {
            println!("  {:<25} {} — {}", w.name, w.label, w.description);
        }
        let registry_entries = crate::registry_workflow::list_registry_aliases();
        if !registry_entries.is_empty() {
            println!();
            println!("Autonomic Registry workflows (from agent-brain cache):");
            for (alias, desc) in registry_entries {
                println!("  @{:<24} {}", alias, desc);
            }
            println!();
            println!("  Run: agent-brain registry sync --local  (if cache is empty)");
        }
        step.done();
        progress.finish()?;
        return Ok(());
    }

    let prereq = progress.step("prerequisites");
    if !force {
        let mut warnings = Vec::new();
        if Command::new("protoc").arg("--version").output().is_err() {
            warnings.push("protoc — gRPC codegen (https://grpc.io/docs/protoc-installation/)");
        }
        if Command::new("bun").arg("--version").output().is_err() {
            warnings.push("bun — dashboard dev server (https://bun.sh)");
        }
        if Command::new("agent-brain")
            .arg("--version")
            .output()
            .is_err()
            && std::env::var("BRAIN_PATH").is_err()
        {
            warnings.push(
                "agent-brain — MCP routing & memory (install from GitHub releases or set BRAIN_PATH)",
            );
        }
        if warnings.is_empty() {
            prereq.done();
        } else {
            prereq.warn(format!(
                "{} optional dependency(ies) missing",
                warnings.len()
            ));
            for w in &warnings {
                println!("  ⚠  {w}");
            }
        }
    } else {
        prereq.done();
    }

    let dirs = progress.step("config directories");
    std::fs::create_dir_all(&config_dir)
        .map_err(|e| format!("failed to create config dir: {e}"))?;
    let workflows_dir = config_dir.join("workflows");
    std::fs::create_dir_all(&workflows_dir)
        .map_err(|e| format!("failed to create workflows dir: {e}"))?;
    dirs.done();

    let cfg = progress.step("config file");
    let unified = agent_body_core::config_path();
    if !unified.exists() {
        agent_body_core::ensure_default_ecosystem_sections()
            .map_err(|e| format!("init unified config: {e}"))?;
    }
    if agent_body_core::read_organ_section_raw("spine")
        .map_err(|e| format!("read [spine]: {e}"))?
        .is_none()
    {
        let spine_table: toml::Table = toml::from_str(CONFIG_TEMPLATE).unwrap_or_default();
        agent_body_core::write_organ_section_raw("spine", &spine_table)
            .map_err(|e| format!("write [spine]: {e}"))?;
        println!("✓ Created [spine] in {}", unified.display());
        cfg.done();
    } else {
        cfg.cached();
        println!("  [spine] exists in {}", unified.display());
    }

    let wf = progress.step("workflow file");
    let (workflow_name, workflow_yaml) = if let Some(ref kind) = with {
        let (name, yaml) = crate::registry_workflow::resolve_workflow_yaml(kind).ok_or_else(|| {
            format!(
                "Unknown workflow '{kind}'. Use `agent-spine init --with list` to see built-in and @registry workflows."
            )
        })?;
        (name, yaml)
    } else {
        ("example".to_string(), EXAMPLE_WORKFLOW.to_string())
    };

    let workflow_filename = format!("{workflow_name}.yaml");
    let workflow_path = workflows_dir.join(&workflow_filename);
    if !workflow_path.exists() {
        let mut f = std::fs::File::create(&workflow_path)
            .map_err(|e| format!("failed to write workflow: {e}"))?;
        write!(f, "{workflow_yaml}")?;
        println!("✓ Created workflow: {}", workflow_path.display());
        wf.done();
    } else {
        wf.cached();
        println!("  Workflow exists: {}", workflow_path.display());
    }

    progress.finish()?;
    println!();
    println!("Next steps:");
    println!("  Validate your workflow:");
    println!("    agent-spine validate {}", workflow_path.display());
    println!("  List available built-in workflows:");
    println!("    agent-spine init --with list");
    println!("  Start the dashboard server:");
    println!("    agent-spine serve --db state.db --port 3000");
    println!("  Check agent-brain connectivity:");
    println!("    agent-spine brain health");
    Ok(())
}

pub fn run_doctor() -> Result<(), Box<dyn std::error::Error>> {
    let mut progress = ProgressRun::new("agent-spine health check").with_total_hint(7);
    let mut all_ok = true;

    let toolchain = progress.step("rust toolchain");
    if let Ok(output) = Command::new("rustc").arg("--version").output() {
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        println!("✓ rustc: {version}");
        toolchain.done();
    } else {
        toolchain.fail("rustc not found");
        all_ok = false;
    }

    let protoc = progress.step("protoc");
    if let Ok(output) = Command::new("protoc").arg("--version").output() {
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        println!("✓ protoc: {version}");
        protoc.done();
    } else {
        protoc.warn("not found (only needed for source builds)");
    }

    let bun = progress.step("bun");
    if let Ok(output) = Command::new("bun").arg("--version").output() {
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        println!("✓ bun: {version}");
        bun.done();
    } else {
        bun.warn("not found (only needed for dashboard dev)");
    }

    let brain = progress.step("agent-brain");
    if let Ok(output) = Command::new("agent-brain").arg("--version").output() {
        let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        println!("✓ agent-brain: {version}");
        brain.done();
    } else {
        brain.warn("not found (optional — MCP routing & memory)");
    }

    let config_dir = agent_body_core::spine_config_dir();
    let legacy = agent_body_core::legacy_spine_config_dir();
    let cfg = progress.step("config directory");
    if config_dir.exists() {
        println!("✓ spine state: {}", config_dir.display());
        cfg.done();
    } else if legacy.exists() {
        cfg.warn(format!(
            "legacy {} — run `agent-spine init` to migrate",
            legacy.display()
        ));
    } else {
        cfg.warn(format!(
            "{} not yet created — run `agent-spine init`",
            config_dir.display()
        ));
    }

    let unified = agent_body_core::config_path();
    let unified_step = progress.step("unified config");
    if unified.is_file()
        && agent_body_core::read_organ_section_raw("spine")
            .ok()
            .flatten()
            .is_some()
    {
        println!("✓ [spine] in {}", unified.display());
        unified_step.done();
    } else {
        unified_step.warn(format!("missing [spine] in {}", unified.display()));
    }

    let example = config_dir.join("workflows/example.yaml");
    let wf = progress.step("example workflow");
    if example.exists() {
        match WorkflowDefinition::from_path(&example) {
            Ok(def) => match def.validate() {
                Ok(_) => {
                    println!("✓ example workflow: valid");
                    wf.done();
                }
                Err(e) => wf.warn(e.to_string()),
            },
            Err(_) => wf.warn("could not parse"),
        }
    } else {
        wf.cached();
    }

    let summary = progress.finish()?;
    println!();
    println!("agent-spine v{}", env!("CARGO_PKG_VERSION"));
    if all_ok && summary.failed == 0 {
        println!("All checks passed.");
    } else {
        println!("Some checks failed. Run `agent-spine init` for setup help.");
    }
    Ok(())
}

const CONFIG_TEMPLATE: &str = r##"# agent-spine configuration
# See https://github.com/aeswibon/agent-spine for documentation.

[server]
port = 3000
db = "state.db"

[brain]
# Path to agent-brain binary (optional — resolves from PATH or BRAIN_PATH)
# path = "/usr/local/bin/agent-brain"

[routing]
max_failures = 3
"##;

const EXAMPLE_WORKFLOW: &str = r##"name: dev-pipeline
version: 1
start: plan

nodes:
  - name: plan
    kind: Router
    retry:
      max_attempts: 2
      backoff_ms: 500

  - name: fork
    kind: Fork

  - name: implement
    kind: Agent
    retry:
      max_attempts: 2
      backoff_ms: 1000

  - name: test
    kind: Agent
    retry:
      max_attempts: 2
      backoff_ms: 1000

  - name: lint
    kind: Verify

  - name: security-scan
    kind: Agent

  - name: join
    kind: Join

  - name: review-gate
    kind: ApprovalGate

  - name: deploy
    kind: Agent

  - name: verify-deploy
    kind: Verify

edges:
  - from: plan
    to: fork
  - from: fork
    to: implement
  - from: fork
    to: test
  - from: implement
    to: lint
  - from: implement
    to: security-scan
  - from: lint
    to: join
  - from: security-scan
    to: join
  - from: test
    to: join
  - from: join
    to: review-gate
  - from: review-gate
    to: deploy
  - from: deploy
    to: verify-deploy
"##;
