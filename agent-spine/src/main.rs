use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

use agent_spine::WorkflowDefinition;
use agent_spine::mcp_bridge::{McpBridge, RouteLimits};

#[derive(Debug, Parser)]
#[command(
    name = "agent-spine",
    version,
    about = "Stateful workflow supervision for AI coding agents"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize agent-spine: config, example workflow, and prerequisites check.
    Init {
        /// Skip prerequisite checks (protoc, bun, agent-brain).
        #[arg(short, long)]
        force: bool,
        /// Target directory for generated files (default: ~/.config/agent-spine).
        #[arg(short, long)]
        dir: Option<PathBuf>,
        /// Generate a specific built-in workflow instead of the generic example.
        /// Use `--with list` to see available workflows.
        #[arg(short, long)]
        with: Option<String>,
    },
    /// Display the capabilities planned for the current scaffold.
    Status,
    /// Diagnose common setup issues.
    Doctor,
    /// Display daemon logs
    Log {
        /// Daemon name (e.g. spine, nerves, heart) or "all"
        name: Option<String>,
        /// Follow log output (tail -f)
        #[arg(short, long)]
        follow: bool,
        /// List available log files
        #[arg(short, long)]
        list: bool,
    },
    /// Parse and validate a YAML workflow definition.
    Validate {
        /// Path to a workflow definition file.
        workflow: PathBuf,
    },
    /// Execute a YAML workflow locally.
    Run {
        /// Path to a workflow definition file.
        workflow: PathBuf,
        /// Initial JSON payload.
        #[arg(short, long, default_value = "{}")]
        payload: String,
        /// Path to SQLite database (default: ~/.autonomic/logs/spine/state.db)
        #[arg(short, long)]
        db: Option<PathBuf>,
        /// Enable agent-brain routing and enrichment.
        #[arg(short, long)]
        brain: bool,
        /// Enable meta-router: query agent-brain to select the workflow YAML
        /// based on this task prompt before execution.
        #[arg(short, long)]
        meta: Option<String>,
    },
    /// Inspect the history of a specific execution.
    Inspect {
        /// Execution ID to inspect.
        execution_id: String,
        /// Path to SQLite database
        #[arg(short, long)]
        db: Option<PathBuf>,
    },
    /// Replay an execution to recreate its final state.
    Replay {
        /// Execution ID to replay.
        execution_id: String,
        /// Path to SQLite database
        #[arg(short, long)]
        db: Option<PathBuf>,
    },
    /// Interact with the agent-brain MCP server.
    Brain {
        #[command(subcommand)]
        action: BrainCommand,
    },
    /// Event bus operations.
    Event {
        #[command(subcommand)]
        action: EventCommand,
    },
    /// Agent registry operations.
    Agent {
        #[command(subcommand)]
        action: AgentCommand,
    },
    /// Serve the Live Dashboard API.
    Serve {
        /// Path to SQLite database
        #[arg(short, long)]
        db: Option<PathBuf>,
        /// Port to listen on
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
        /// Dashboard HTTP port (defaults to port + 1)
        #[arg(long, default_value_t = 3001)]
        dashboard_port: u16,
        /// NATS URL for JetStream event sourcing (or set AUTONOMIC_NATS_URL)
        #[arg(long)]
        nats_url: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum BrainCommand {
    /// Check if agent-brain is reachable.
    Health,
    /// Send a route_task query and show the response.
    Route {
        /// The message to route through agent-brain.
        message: String,
    },
    /// Show agent-brain index and status info.
    Status,
}

#[derive(Debug, Subcommand)]
enum EventCommand {
    /// Start the event bus server.
    Serve {
        /// Port to listen on.
        #[arg(short, long, default_value_t = 3100)]
        port: u16,
        /// SQLite state db for autonomic workflow API (default: global workspace).
        #[arg(long)]
        db: Option<PathBuf>,
        /// NATS URL for JetStream-backed event bus (or AUTONOMIC_NATS_URL).
        #[arg(long)]
        nats_url: Option<String>,
    },
    /// Publish an event to the bus.
    Pub {
        /// Event subject (e.g. "agent.heart.beat").
        subject: String,
        /// Event payload as JSON string.
        payload: String,
        /// Source agent name.
        #[arg(short, long, default_value = "agent-spine")]
        source: String,
        /// Event bus URL (default: http://localhost:3100).
        #[arg(short, long, default_value = "http://localhost:3100")]
        url: String,
    },
    /// Subscribe to events from the bus (prints to stdout as JSON).
    Sub {
        /// Subject filter (e.g. "agent.heart.>" for wildcard).
        #[arg(short, long, default_value = ">")]
        subject: String,
        /// Event bus URL (default: http://localhost:3100).
        #[arg(short, long, default_value = "http://localhost:3100")]
        url: String,
    },
}

#[derive(Debug, Subcommand)]
enum AgentCommand {
    /// List registered agents.
    List {
        /// Event bus URL (default: http://localhost:3100).
        #[arg(short, long, default_value = "http://localhost:3100")]
        url: String,
    },
    /// Register an agent with the bus.
    Register {
        /// Agent name.
        name: String,
        /// Agent version.
        #[arg(short, long, default_value = "0.1.0")]
        version: String,
        /// Capabilities (comma-separated).
        #[arg(short, long, default_value = "")]
        capabilities: String,
        /// Event bus URL (default: http://localhost:3100).
        #[arg(short, long, default_value = "http://localhost:3100")]
        url: String,
    },
    /// Get info about a specific agent.
    Info {
        /// Agent name.
        name: String,
        /// Event bus URL (default: http://localhost:3100).
        #[arg(short, long, default_value = "http://localhost:3100")]
        url: String,
    },
}

use tracing_subscriber::{Registry, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()
            .expect("Failed to build OTLP exporter");

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();

        use opentelemetry::trace::TracerProvider;
        let tracer = provider.tracer("agent-spine");

        opentelemetry::global::set_tracer_provider(provider);

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        Registry::default()
            .with(EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer())
            .with(telemetry)
            .try_init()
            .ok();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

    if let Err(error) = run(Cli::parse().command).await {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Init { force, dir, with } => {
            use std::io::Write;

            let config_dir = dir.unwrap_or_else(|| {
                let home = std::env::var("HOME").expect("HOME must be set");
                PathBuf::from(home).join(".config/agent-spine")
            });

            println!("agent-spine v{} init", env!("CARGO_PKG_VERSION"));
            println!("  config:  {}", config_dir.display());
            println!();

            // Handle --with list (no-op, just display available workflows)
            if with.as_deref() == Some("list") {
                println!("Available built-in workflows:");
                for w in agent_spine::workflows::ALL {
                    println!("  {:<25} {} — {}", w.name, w.label, w.description);
                }
                return Ok(());
            }

            // Prerequisites
            if !force {
                let mut warnings = Vec::new();
                if std::process::Command::new("protoc")
                    .arg("--version")
                    .output()
                    .is_err()
                {
                    warnings
                        .push("protoc — gRPC codegen (https://grpc.io/docs/protoc-installation/)");
                }
                if std::process::Command::new("bun")
                    .arg("--version")
                    .output()
                    .is_err()
                {
                    warnings.push("bun — dashboard dev server (https://bun.sh)");
                }
                if std::process::Command::new("agent-brain")
                    .arg("--version")
                    .output()
                    .is_err()
                    && std::env::var("BRAIN_PATH").is_err()
                {
                    warnings.push("agent-brain — MCP routing & memory (install from GitHub releases or set BRAIN_PATH)");
                }
                if !warnings.is_empty() {
                    println!("Optional dependencies not found:");
                    for w in &warnings {
                        println!("  ⚠  {w}");
                    }
                    println!();
                } else {
                    println!("✓ All prerequisites satisfied");
                    println!();
                }
            }

            // Create config directory
            std::fs::create_dir_all(&config_dir)
                .map_err(|e| format!("failed to create config dir: {e}"))?;
            let workflows_dir = config_dir.join("workflows");
            std::fs::create_dir_all(&workflows_dir)
                .map_err(|e| format!("failed to create workflows dir: {e}"))?;

            // Write default config
            let config_path = config_dir.join("config.toml");
            if !config_path.exists() {
                let mut f = std::fs::File::create(&config_path)
                    .map_err(|e| format!("failed to write config: {e}"))?;
                write!(f, "{}", CONFIG_TEMPLATE)?;
                println!("✓ Created config: {}", config_path.display());
            } else {
                println!("  Config exists: {}", config_path.display());
            }

            // Determine which workflow to write
            let (workflow_name, workflow_yaml) = if let Some(ref kind) = with {
                let entry = agent_spine::workflows::find(kind).ok_or_else(|| {
                    format!("Unknown workflow '{kind}'. Use `agent-spine init --with list` to see available workflows.")
                })?;
                (entry.name, entry.yaml)
            } else {
                ("example", EXAMPLE_WORKFLOW)
            };

            let workflow_filename = format!("{workflow_name}.yaml");
            let workflow_path = workflows_dir.join(&workflow_filename);
            if !workflow_path.exists() {
                let mut f = std::fs::File::create(&workflow_path)
                    .map_err(|e| format!("failed to write workflow: {e}"))?;
                write!(f, "{workflow_yaml}")?;
                println!("✓ Created workflow: {}", workflow_path.display());
            } else {
                println!("  Workflow exists: {}", workflow_path.display());
            }

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
            println!("  See all commands:");
            println!("    agent-spine --help");

            Ok(())
        }
        Command::Status => {
            info!("agent-spine supervisor initialized");
            println!("agent-spine: skeleton ready; workflow validation is available");
            Ok(())
        }
        Command::Doctor => {
            use std::process::Command;

            println!("agent-spine doctor — diagnostics");
            println!();

            let mut all_ok = true;

            // Rust toolchain
            if let Ok(output) = Command::new("rustc").arg("--version").output() {
                let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                println!("✓ rustc: {version}");
            } else {
                println!("✗ rustc: not found");
                all_ok = false;
            }

            // protoc
            if let Ok(output) = Command::new("protoc").arg("--version").output() {
                let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                println!("✓ protoc: {version}");
            } else {
                println!("⚠ protoc: not found (only needed for source builds)");
            }

            // bun
            if let Ok(output) = Command::new("bun").arg("--version").output() {
                let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                println!("✓ bun: {version}");
            } else {
                println!("⚠ bun: not found (only needed for dashboard dev)");
            }

            // agent-brain
            if let Ok(output) = Command::new("agent-brain").arg("--version").output() {
                let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                println!("✓ agent-brain: {version}");
            } else {
                println!("⚠ agent-brain: not found (optional — MCP routing & memory)");
            }

            // Binary version
            println!();
            println!("agent-spine v{}", env!("CARGO_PKG_VERSION"));

            // Config directory
            let home = std::env::var("HOME").unwrap_or_default();
            let config_dir = std::path::PathBuf::from(&home).join(".config/agent-spine");
            if config_dir.exists() {
                println!("✓ config dir: {}", config_dir.display());
            } else {
                println!(
                    "  config dir: {} (not yet created — run `agent-spine init`)",
                    config_dir.display()
                );
            }

            // Example workflow validation
            let example = config_dir.join("workflows/example.yaml");
            if example.exists() {
                match WorkflowDefinition::from_path(&example) {
                    Ok(def) => match def.validate() {
                        Ok(_) => println!("✓ example workflow: valid"),
                        Err(e) => println!("⚠ example workflow: {e}"),
                    },
                    Err(_) => println!("⚠ example workflow: could not parse"),
                }
            }

            if all_ok {
                println!();
                println!("All checks passed.");
            } else {
                println!();
                println!("Some checks failed. Run `agent-spine init` for setup help.");
            }

            Ok(())
        }
        Command::Log { name, follow, list } => {
            if list {
                let logs = agent_spine::log::list_logs()?;
                if logs.is_empty() {
                    println!("No log files found.");
                } else {
                    println!("Available logs:");
                    for log in &logs {
                        println!("  {log}");
                    }
                }
                return Ok(());
            }
            let name = name.ok_or_else(|| {
                Box::<dyn std::error::Error>::from(
                    "usage: agent-spine log <name> [--follow]  (or --list to see available logs)",
                )
            })?;
            if follow {
                agent_spine::log::follow_log(&name)?;
            } else {
                agent_spine::log::print_log(&name)?;
            }
            Ok(())
        }
        Command::Validate { workflow } => {
            let validated = WorkflowDefinition::from_path(workflow)?.validate()?;
            info!(
                workflow = validated.definition().name(),
                version = validated.definition().version(),
                nodes = validated.definition().nodes().len(),
                edges = validated.definition().edges().len(),
                "workflow validated"
            );
            println!(
                "validated state-machine workflow '{}' starting at node: {}",
                validated.definition().name(),
                validated.definition().start_node()
            );
            Ok(())
        }
        Command::Run {
            workflow,
            payload,
            db,
            brain,
            meta,
        } => {
            // Meta-router: select workflow YAML via agent-brain
            let workflow_path = match meta {
                Some(ref prompt) => {
                    let workflows_dir = workflow.parent().unwrap_or(&workflow);
                    let router =
                        agent_spine::meta_router::MetaRouter::new(workflows_dir.to_path_buf());
                    router
                        .select_workflow(prompt)
                        .unwrap_or_else(|| workflow.clone())
                }
                None => workflow.clone(),
            };

            let validated = WorkflowDefinition::from_path(workflow_path)?.validate()?;
            let initial_payload = serde_json::from_str(&payload)?;
            let db = agent_spine::global_workspace::resolve_state_db(db)?;

            let store = agent_spine::state::SqliteStateStore::new(&db)?;
            let store = std::sync::Arc::new(std::sync::Mutex::new(store));

            let supervisor = agent_spine::supervisor::Supervisor::new();

            let agent = agent_spine::agent::LocalAgent::new(supervisor.clone());
            agent.spawn();

            let cancel = agent_spine::cancellation::CancelToken::new();
            let _signal_task = agent_spine::cancellation::setup_signal_handler(cancel.clone());

            let workflow_name = validated.definition().name().to_string();
            let mut executor =
                agent_spine::executor::Executor::new(validated, store.clone(), supervisor);
            if brain {
                executor = executor.with_brain(None);
            }
            executor = executor.with_cancel_token(cancel);

            let execution_id = executor.run(initial_payload).await?;
            use agent_spine::WorkflowState;
            let history = {
                let guard = store.lock().map_err(|_| "state lock poisoned")?;
                guard.history(execution_id)
            };
            let graph_path = {
                let guard = store.lock().map_err(|_| "state lock poisoned")?;
                agent_spine::global_workspace::export_execution_graph(
                    &*guard,
                    execution_id,
                    &workflow_name,
                )?
            };
            let _dag_path =
                agent_spine::global_workspace::export_dag_summary(&history, execution_id)?;
            println!("Workflow completed. Execution ID: {}", execution_id);
            println!("Execution graph: {}", graph_path.display());
            Ok(())
        }
        Command::Inspect { execution_id, db } => {
            let db = agent_spine::global_workspace::resolve_state_db(db)?;
            let store = agent_spine::state::SqliteStateStore::new(&db)?;
            let id = std::str::FromStr::from_str(&execution_id)?;
            use agent_spine::WorkflowState;
            let history = store.history(id);

            if history.is_empty() {
                println!("No history found for execution ID {}", id);
            } else {
                for snapshot in history {
                    println!("Sequence: {}", snapshot.sequence());
                    println!(
                        "Payload: {}",
                        serde_json::to_string_pretty(snapshot.payload())?
                    );
                    if let Some(trans) = snapshot.transition_edge() {
                        println!("Transition: {} -> {}", trans.from(), trans.to());
                    } else {
                        println!("Transition: Initial");
                    }
                    println!("---");
                }
            }
            Ok(())
        }
        Command::Replay { execution_id, db } => {
            let db = agent_spine::global_workspace::resolve_state_db(db)?;
            let store = agent_spine::state::SqliteStateStore::new(&db)?;
            let id = std::str::FromStr::from_str(&execution_id)?;
            use agent_spine::WorkflowState;
            let history = store.history(id);

            if history.is_empty() {
                println!("No history found for execution ID {}", id);
                return Ok(());
            }

            let mut current_snapshot = history[0].clone();
            println!(
                "Initial Payload: {}",
                serde_json::to_string_pretty(current_snapshot.payload())?
            );

            for snapshot in history.into_iter().skip(1) {
                if let Some(trans) = snapshot.transition_edge() {
                    println!("Replaying transition: {} -> {}", trans.from(), trans.to());
                }
                current_snapshot = snapshot;
                println!(
                    "New Payload: {}",
                    serde_json::to_string_pretty(current_snapshot.payload())?
                );
            }

            println!(
                "Replay complete. Final sequence: {}",
                current_snapshot.sequence()
            );
            Ok(())
        }
        Command::Brain { action } => run_brain(action).await,
        Command::Event { action } => run_event(action).await,
        Command::Agent { action } => run_agent(action).await,
        Command::Serve {
            db,
            port,
            dashboard_port,
            nats_url,
        } => {
            let db = agent_spine::global_workspace::resolve_state_db(db)?;
            info!("Starting agent-spine gRPC server on port {}", port);

            let wf_manager = agent_spine::WorkflowManager::new(db.clone(), false);

            let nats_url = nats_url.or_else(|| std::env::var("AUTONOMIC_NATS_URL").ok());

            #[cfg(feature = "nats")]
            if let Some(url) = nats_url {
                agent_spine::jetstream_bridge::spawn_state_bridge(
                    wf_manager.supervisor.clone(),
                    url,
                );
            }

            let store = agent_spine::state::SqliteStateStore::new(&db)?;
            let store = std::sync::Arc::new(std::sync::Mutex::new(store));

            let supervisor = agent_spine::supervisor::Supervisor::new();

            let dashboard_api = agent_spine::api::DashboardApi {
                store,
                workflow_manager: wf_manager,
            };
            let supervisor_api = agent_spine::api::SupervisorApi { supervisor };

            let dashboard_svc =
                agent_spine::api::pb::dashboard_service_server::DashboardServiceServer::new(
                    dashboard_api,
                );
            let supervisor_svc =
                agent_spine::api::pb::supervisor_service_server::SupervisorServiceServer::new(
                    supervisor_api,
                );

            // Spawn gRPC server
            let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;
            info!("Listening on grpc://{}", grpc_addr);
            let grpc_handle = {
                let dash = dashboard_svc;
                let sup = supervisor_svc;
                tokio::spawn(async move {
                    let cors = tower_http::cors::CorsLayer::new()
                        .allow_origin(tower_http::cors::Any)
                        .allow_headers(tower_http::cors::Any)
                        .allow_methods(tower_http::cors::Any);

                    tonic::transport::Server::builder()
                        .accept_http1(true)
                        .layer(cors)
                        .layer(tonic_web::GrpcWebLayer::new())
                        .add_service(dash)
                        .add_service(sup)
                        .serve(grpc_addr)
                        .await
                })
            };

            // Spawn dashboard HTTP server
            let dash_addr: std::net::SocketAddr = format!("0.0.0.0:{}", dashboard_port).parse()?;
            info!("Dashboard on http://{}", dash_addr);
            let dash_handle = tokio::spawn(async move {
                use axum::response::Html;

                let app = axum::Router::new().route(
                    "/",
                    axum::routing::get(|| async { Html(include_str!("../dashboard/index.html")) }),
                );

                let listener = tokio::net::TcpListener::bind(dash_addr).await.unwrap();
                axum::serve(listener, app).await
            });

            let cancel = agent_spine::cancellation::CancelToken::new();
            let _signal_task = agent_spine::cancellation::setup_signal_handler(cancel.clone());

            tokio::select! {
                r = grpc_handle => r??,
                r = dash_handle => r??,
                _ = async {
                    let mut watcher = cancel.watch();
                    loop {
                        if *watcher.borrow_and_update() { break; }
                        watcher.changed().await.ok();
                    }
                } => {
                    info!("Shutdown requested, waiting for servers to stop...");
                }
            }

            Ok(())
        }
    }
}

async fn run_brain(command: BrainCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        BrainCommand::Health => {
            match McpBridge::connect(None).await {
                Ok(mut bridge) => {
                    bridge.health().await?;
                    println!("✓ agent-brain is reachable");
                    // Drop bridge to kill the child process
                    drop(bridge);
                    Ok(())
                }
                Err(e) => {
                    eprintln!("✗ agent-brain unreachable: {e}");
                    eprintln!();
                    eprintln!("  Make sure agent-brain is installed and in PATH.");
                    eprintln!("  Set BRAIN_PATH env var for a custom location.");
                    eprintln!("  Common locations:");
                    eprintln!("    ~/.agent_brain/bin/agent-brain");
                    eprintln!("    /usr/local/bin/agent-brain");
                    eprintln!("    PATH resolution");
                    std::process::exit(1);
                }
            }
        }
        BrainCommand::Route { message } => {
            let mut bridge = McpBridge::connect(None).await?;
            println!("Routing message to agent-brain...\n");

            let resp = bridge
                .route_task(&message, None, &[], 500, RouteLimits::default(), None, None)
                .await?;

            println!("=== Route Response ===");
            println!("Phase:       {}", resp.recommended_phase);
            println!("Confidence:  {:.3}", resp.route_confidence);
            println!("Escalate:    {}", resp.escalate_recommended);
            println!("Briefing:    {}", resp.briefing);
            println!("Cache hit:   {}", resp.cache_hit);
            println!("Latency:     {}ms", resp.latency_ms);
            println!("Index total: {}", resp.index_total);
            println!();

            if !resp.recommended_agents.is_empty() {
                println!("--- Recommended Agents ---");
                for a in &resp.recommended_agents {
                    println!("  {:<20} score={:.3}  {}", a.name, a.score, a.rationale);
                }
                println!();
            }

            if !resp.recommended_skills.is_empty() {
                println!("--- Recommended Skills ---");
                for s in &resp.recommended_skills {
                    println!("  {:<30} score={:.3}", s.name, s.score);
                }
                println!();
            }

            if !resp.applicable_rules.is_empty() {
                println!("--- Applicable Rules ---");
                for r in &resp.applicable_rules {
                    println!("  {} (score={:.3})", r.topic, r.score);
                }
                println!();
            }

            if !resp.must_apply.is_empty() {
                println!("--- Must Apply ---");
                for m in &resp.must_apply {
                    println!("  {}: {}", m.topic, m.text);
                }
                println!();
            }

            Ok(())
        }
        BrainCommand::Status => {
            let mut bridge = McpBridge::connect(None).await?;
            let info = bridge.health().await?;
            println!("agent-brain status:");
            println!("  Server:  {}", info.name);
            println!("  Version: {}", info.version);

            let facts = bridge.list_memory(5).await.unwrap_or_default();
            println!("  Memory:  {} facts stored (showing up to 5)", facts.len());

            drop(bridge);
            Ok(())
        }
    }
}

async fn run_event(command: EventCommand) -> Result<(), Box<dyn std::error::Error>> {
    use agent_spine::event::{AgentRegistry, connect_event_bus, start_event_server};
    use std::sync::Arc;

    match command {
        EventCommand::Serve { port, db, nats_url } => {
            let db = agent_spine::global_workspace::resolve_state_db(db).ok();
            let nats_url = nats_url.or_else(agent_body_core::default_nats_url);
            let bus = connect_event_bus(nats_url).await?;
            let registry = Arc::new(AgentRegistry::new());
            let handle = start_event_server(bus, registry, port, db);
            handle.await.unwrap();
            Ok(())
        }
        EventCommand::Pub {
            subject,
            payload,
            source,
            url,
        } => {
            let client = reqwest::Client::new();
            let body = serde_json::json!({
                "subject": subject,
                "payload": serde_json::from_str::<serde_json::Value>(&payload)?,
                "source": source,
            });
            let resp = client
                .post(format!("{}/api/v1/events", url))
                .json(&body)
                .send()
                .await?;
            let text = resp.text().await?;
            println!("{}", text);
            Ok(())
        }
        EventCommand::Sub { subject, url } => {
            let client = reqwest::Client::new();
            let resp = client
                .get(format!("{}/api/v1/events/subscribe", url))
                .query(&[("subject", &subject)])
                .send()
                .await?;

            let mut stream = resp.bytes_stream();
            use futures::StreamExt;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                let text = String::from_utf8_lossy(&chunk);
                for line in text.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        println!("{}", data);
                    }
                }
            }
            Ok(())
        }
    }
}

async fn run_agent(command: AgentCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        AgentCommand::List { url } => {
            let client = reqwest::Client::new();
            let resp = client.get(format!("{}/api/v1/agents", url)).send().await?;
            let text = resp.text().await?;
            println!("{}", text);
            Ok(())
        }
        AgentCommand::Register {
            name,
            version,
            capabilities,
            url,
        } => {
            let client = reqwest::Client::new();
            let caps: Vec<String> = if capabilities.is_empty() {
                Vec::new()
            } else {
                capabilities
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            };
            let body = serde_json::json!({
                "name": name,
                "version": version,
                "capabilities": caps,
                "last_seen": chrono::Utc::now().to_rfc3339(),
                "metadata": {},
            });
            let resp = client
                .post(format!("{}/api/v1/agents", url))
                .json(&body)
                .send()
                .await?;
            let text = resp.text().await?;
            println!("{}", text);
            Ok(())
        }
        AgentCommand::Info { name, url } => {
            let client = reqwest::Client::new();
            let resp = client
                .get(format!("{}/api/v1/agents/{}", url, name))
                .send()
                .await?;
            let text = resp.text().await?;
            println!("{}", text);
            Ok(())
        }
    }
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
