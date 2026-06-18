use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

use agent_spine::WorkflowDefinition;

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
    /// Display the capabilities planned for the current scaffold.
    Status,
    /// Parse and validate a YAML workflow definition.
    Validate {
        /// Path to a workflow definition file.
        workflow: PathBuf,
    },
    /// Serve the Live Dashboard API.
    Serve {
        /// Path to SQLite database
        #[arg(short, long, default_value = "state.db")]
        db: PathBuf,
        /// Port to listen on
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .ok();

    if let Err(error) = run(Cli::parse().command).await {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Status => {
            info!("agent-spine supervisor initialized");
            println!("agent-spine: skeleton ready; workflow validation is available");
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
        Command::Serve { db, port } => {
            info!("Starting agent-spine gRPC server on port {}", port);

            let store = agent_spine::state::SqliteStateStore::new(db)?;
            let store = std::sync::Arc::new(std::sync::Mutex::new(store));

            let supervisor = agent_spine::supervisor::Supervisor::new();

            let dashboard_api = agent_spine::api::DashboardApi { store };
            let supervisor_api = agent_spine::api::SupervisorApi { supervisor };

            let dashboard_svc =
                agent_spine::api::pb::dashboard_service_server::DashboardServiceServer::new(
                    dashboard_api,
                );
            let supervisor_svc =
                agent_spine::api::pb::supervisor_service_server::SupervisorServiceServer::new(
                    supervisor_api,
                );

            let addr = format!("0.0.0.0:{}", port).parse()?;
            info!("Listening on grpc://{}", addr);

            // Enable gRPC-Web and CORS
            let cors = tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any);

            tonic::transport::Server::builder()
                .accept_http1(true)
                .layer(cors)
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(dashboard_svc)
                .add_service(supervisor_svc)
                .serve(addr)
                .await?;

            Ok(())
        }
    }
}
