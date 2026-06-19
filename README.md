# Agent Spine

Agent Spine is a local-first, stateful workflow engine for supervising AI coding agents. It provides deterministic graph execution, immutable state history, and native IDE hooks while [`agent-brain`](https://github.com/aeswibon/agent-brain) is intended to supply token-efficient context for each node.

The project is **not** another terminal-bound agent crew. It targets a lightweight Rust supervisor that integrates with tools like Claude Code and Cursor, runs declarative YAML workflows, and records every state transition for inspection, replay, and intervention.

> **Status:** Phase 1–2 in progress. The execution engine, state stores, gRPC supervisor, confidence router, agent-brain MCP bridge, parallel fan-out/fan-in, and a Svelte dashboard are implemented. CLI `run`/`replay`/`inspect` and production persistence adapters are not yet wired end-to-end.

Planning docs and the repository audit live in [`docs/superpowers/`](docs/superpowers/) (local only, not committed).

## Why Agent Spine?

Existing orchestration frameworks are capable, but local production workflows often hit the same operational costs:

- graph definitions accumulate infrastructure boilerplate;
- cyclic failures are difficult to diagnose because state changes are opaque;
- execution is separated from the IDE agent interface where developers already work;
- retries and multi-agent reasoning can become unbounded or impossible to audit.

Agent Spine treats these as execution-engine problems. Workflows should be declarative, transitions immutable, verification mechanical, and escalation explicit.

## Architecture

Agent Spine is organized around four layers:

```mermaid
flowchart TB
    subgraph layers["Execution layers"]
        info["Information<br/>agent-brain context routing<br/>MCP bridge"]
        structure["Structure<br/>typed bounded transitions"]
        reasoning["Reasoning depth<br/>debate · voting · search<br/><i>planned</i>"]
        verify["Mechanical verification<br/>compile · test · lint · schema<br/><i>partial</i>"]
    end

    info --> structure --> reasoning --> verify
```

### System overview

```mermaid
flowchart TB
    brain["agent-brain<br/>context routing · memory<br/>MCP stdio server"]
    dashboard["Dashboard<br/>Svelte · Connect · gRPC-Web"]
    sup_api["Supervisor API<br/>resume · pending tasks"]
    dash_api["Dashboard API<br/>list · history"]
    executor["Executor<br/>state machine · fan-out/fan-in<br/>retries · ApprovalGate · router"]
    brain_router["BrainRouter<br/>MCP bridge · fallback router<br/>enrich · trajectory"]
    supervisor["Supervisor<br/>IDE hooks · pause/resume"]
    state["WorkflowState<br/>InMemory · JSONL · SQLite"]

    dashboard -->|gRPC-Web| sup_api
    dashboard -->|gRPC-Web| dash_api
    sup_api --> executor
    dash_api --> state
    executor --> brain_router
    brain_router <-->|JSON-RPC 2.0| brain
    brain_router --> supervisor
    executor --> supervisor
    executor --> state
```

### Core modules

| Module | Role |
|--------|------|
| `workflow` | YAML workflow definitions, validation, `NodeKind` (`Agent`, `Checkpoint`, `Verify`, `ApprovalGate`) |
| `executor` | Async state-machine traversal with parallel branches, fan-in merge, retries, and routing |
| `supervisor` | Pauses agent nodes and waits for IDE `resume()` via gRPC |
| `brain_router` | MCP bridge to agent-brain for context routing, enrichment, and trajectory logging |
| `mcp_bridge` | JSON-RPC 2.0 client over child-process stdio (MCP protocol) |
| `router` | `ConfidenceRouter` — tracks verification failures and sets `escalation_required` |
| `state` | Append-only `WorkflowState` trait with in-memory, JSONL file, and SQLite adapters |
| `api` | gRPC services generated from `proto/supervisor.proto` |
| `dashboard/` | Svelte frontend for execution monitoring and HITL resume |

Rust is the primary implementation language for predictable resource use, safe concurrency, durable local execution, and a single distributable binary. Model providers, IDE integrations, and persistence backends remain behind adapter traits.

## Current capabilities

```mermaid
flowchart LR
    subgraph done["Implemented"]
        yaml["YAML workflows"]
        snap["Append-only snapshots"]
        parallel["Parallel fan-out/fan-in"]
        hitl["ApprovalGate + resume"]
        retry["Retries + backoff"]
        router_done["Confidence router"]
        stores["InMemory · JSONL · SQLite"]
        grpc["gRPC + gRPC-Web"]
        ui["Svelte dashboard"]
        cicd["pipeline-compose CI"]
    end

    subgraph todo["Not yet"]
        cli_run["CLI run/replay/inspect"]
        serve_exec["Executor in serve"]
        pg["Postgres / Redis stores"]
        otel["OpenTelemetry export"]
        ws["WebSocket live DAG"]
        ide["Cursor / Claude adapters"]
    end
```

### Implemented

- **Declarative YAML workflows** with versioned schemas and validation
- **Immutable append-only snapshots** with parent linkage and monotonic sequence numbers
- **Parallel fan-out / fan-in** via multiple outgoing edges and `tokio::task::JoinSet`
- **Human-in-the-loop gates** via `ApprovalGate` nodes and supervisor resume
- **Built-in retries** with exponential backoff before supervisor failure (hardcoded policy today)
- **Confidence routing** after repeated verification failures
- **agent-brain MCP bridge** — `BrainRouter` with lazy-connect MCP client, payload enrichment, trajectory logging, and graceful fallback to `ConfidenceRouter`
- **`brain` CLI subcommand** — `health`, `route`, `status` for interacting with agent-brain
- **State stores**: `InMemoryStateStore`, `FileStateStore` (JSONL), `SqliteStateStore`
- **gRPC supervisor + dashboard API** with gRPC-Web and CORS for browser clients
- **Live dashboard** (Svelte) — execution list, history viewer, pending-task resume
- **CI/CD** via `pipeline-compose` — Rust check, multi-platform release builds, tag publish

### Not yet implemented

- CLI `run`, `inspect`, and `replay` commands
- Wiring the executor into `agent-spine serve` (server exposes APIs but does not execute workflows today)
- Postgres / Redis state adapters
- OpenTelemetry export (`tracing` spans exist; no OTel pipeline)
- Configurable `RetryPolicy` in workflow YAML
- WebSocket streaming for live DAG updates (dashboard polls today)
- Native Claude Code / Cursor IDE adapters
- Example workflow YAML files in-repo

## Project layout

```mermaid
flowchart TB
    root["agent-spine repo"]
    root --> engine["agent-spine/<br/>Rust engine + CLI"]
    root --> dash["dashboard/<br/>Svelte UI"]
    root --> gh[".github/<br/>CI + release pipelines"]

    engine --> proto["proto/"]
    engine --> src["src/<br/>workflow · executor · supervisor<br/>brain_router · mcp_bridge<br/>router · state · api"]
    engine --> tests["tests/"]
```

## Getting started

### Binary install (recommended)

Download the pre-built binary from [GitHub Releases](https://github.com/aeswibon/agent-spine/releases).

```bash
# macOS / Linux
curl -L https://github.com/aeswibon/agent-spine/releases/download/v0.1.0/agent-spine-{target}.tar.gz | tar xz
sudo mv agent-spine /usr/local/bin/

# One-time setup — creates config, example workflow, checks prerequisites
agent-spine init

# Start the dashboard server
agent-spine serve --db state.db --port 3000
```

### Build from source

```bash
git clone https://github.com/aeswibon/agent-spine.git && cd agent-spine
cargo build --release
./target/release/agent-spine init
```

### Prerequisites

- **protoc** — gRPC codegen (https://grpc.io/docs/protoc-installation/) — only needed when building from source
- **bun** — dashboard dev (https://bun.sh) — optional
- **agent-brain** — MCP routing & memory (set `BRAIN_PATH` or [install from releases](https://github.com/aeswibon/agent-brain/releases)) — optional

## Using v0.1

### Workflow validation

```bash
# Validate a workflow definition
cargo run -p agent-spine -- validate path/to/workflow.yaml
```

### brain CLI (agent-brain integration)

Requires `agent-brain` on your PATH (or set `BRAIN_PATH` env var).

```bash
# Health check — verify agent-brain is reachable
cargo run -p agent-spine -- brain health

# Route a task through agent-brain for context/routing
cargo run -p agent-spine -- brain route "implement user authentication"

# Show brain connection status
cargo run -p agent-spine -- brain status
```

### Programmatic usage (Rust)

```rust
use std::sync::{Arc, Mutex};
use agent_spine::{
    Executor, Supervisor, ConfidenceRouter,
    workflow::{WorkflowDefinition, WorkflowNode, WorkflowEdge, NodeKind},
    state::InMemoryStateStore,
};

// Define a workflow
let workflow = WorkflowDefinition::new("my_pipeline", 1, "start", nodes, edges)
    .validate()?;

// Wire dependencies
let store = Arc::new(Mutex::new(InMemoryStateStore::default()));
let supervisor = Supervisor::new();
let router = ConfidenceRouter::new(3);

// Executor with brain (lazy-connects to agent-brain on first use)
let mut executor = Executor::with_brain(
    validated, store, supervisor, router,
    Some("/path/to/agent-brain".into()),
);

// Run the workflow
let exec_id = executor.run(serde_json::json!({ "input": "data" })).await?;
```

### Dashboard

```bash
# Start the server with gRPC and dashboard API
cargo run -p agent-spine -- serve --db state.db --port 3000

# Open dashboard (separate terminal)
cd dashboard && bun install && bun run dev
```

### Development checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## Key invariants

1. State snapshots are immutable and append-only.
2. Every transition references its parent snapshot.
3. Workflow and state schemas are explicitly versioned.
4. Retries, debates, and searches have hard execution and token limits.
5. External effects use idempotency keys and are recorded before acknowledgement.
6. Human approval is required for configured high-impact transitions.
7. Provider-specific behavior remains behind adapter traits.
8. Replay creates a new execution branch; it does not rewrite history.

## Contributing

Issues that sharpen the workflow schema, snapshot model, IDE hook protocol, or verification boundaries are especially welcome.

Before submitting a change:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
