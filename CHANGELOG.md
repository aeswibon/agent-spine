# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.18.0] - 2026-07-02

### Added

- **Critical-path scheduler (Phase 2 kernel overhaul)** — compute per-node CPP score and dispatch higher-CPP nodes first with a concurrency cap to reduce starvation
- **Scheduler tests** — verifies CPP prefers longer critical paths

## [0.17.2] - 2026-06-27

### Changed

- **agent-body-core dependency** — updated tag to `v0.5.14` for ecosystem alignment

## [0.17.1] - 2026-06-27

### Added

- **MCP server** (`agent-spine mcp-serve`) — starts an MCP stdio server for gateway aggregation. Used by `agent-body serve-mcp`.
- **Organ tool definitions** — `spine_submit_workflow`, `spine_check_status`, `spine_list_workflows` with `#[tool(tool_box)]` naming convention
- **Integration tests** — default parameter validation, `ServerHandler` trait compliance

## [0.17.0] - 2026-06-23

### Added

- ProgressTree on `init` and `doctor` with step-level status (done / warn / cached)
- Global `--progress` CLI flag (also `AGENT_PROGRESS=1`)

### Changed

- Extracted `setup.rs` from `main.rs` for init/doctor orchestration

## [0.16.9] - 2026-06-21

### Added

- **Budget gate grace period** — first freeze offense per phase is warning-only (logged via `tracing::warn!` + allowed). Only subsequent offenses (2+) trigger `BudgetFrozen` error, preventing single-edge-case workflow failures.

## [0.16.8] - 2026-06-21

### Fixed

- Reduce default token budget in `spine` to prevent benchmark false positives

## [0.16.7] - 2026-06-21

### Fixed

- CI builds use git `agent-body-core` dependency instead of a local path
- Removed duplicate `executor.rs` module conflicting with `executor/mod.rs`
- Rustfmt fixes for CI format check

## [0.16.6] - 2026-06-21

### Added

- **V2 workflow resilience** — `executor/resilience.rs` with exponential backoff, per-node circuit breaker, and `system.dlq` dead-letter publishing on final failure
- Per-node `timeout_s` / `timeout_secs` in workflow YAML (default 300s; sandbox uses `sandbox_config.timeout_secs`)

### Changed

- Sandbox nodes route through agent-immune via JetStream when NATS is configured (no Docker fallback)
- Docker fallback hardened with `--network=none`, memory/CPU/pids limits
- NATS clients use `agent_body_core::connect_nats()` for authenticated connections

## [0.16.5] - 2026-06-21

### Added

- `agent-spine update [--force]` — self-update subcommand that checks GitHub releases, compares versions, and downloads the latest binary

## [0.16.4] - 2026-06-21

### Added

- `agent-spine log <name> [--follow] [--list]` — read daemon logs from the supervisor log directory

## [0.16.3] - 2026-06-21

### Fixed

- Workflow manager semaphore overflow — changed `usize::MAX` to `1000` to avoid tokio `MAX_PERMITS` panic at startup (`workflow_manager.rs`)

## [0.16.2] - 2026-06-20

### Fixed

- NATS connect calls use 3s timeout in event bus, async sandbox, and JetStream bridge to avoid hanging when broker is unavailable

## [0.16.1] - 2026-06-20

### Added

- Mermaid architecture charts in README (`38bfc86`)

### Changed

- Ecosystem section in README; install script URL fix (`a2f7120`)

### Fixed

- cargo fmt for budget gate CI (`59c473e`)
- Keep a Changelog header and release section format

## [0.16.0] - 2026-06-20

### Added

- **Predictive token budget gate** — executor calls agent-heart `POST /budget/check` before delegating LLM nodes (`AUTONOMIC_HEART_URL`, `AUTONOMIC_BUDGET_GATE=0` to disable)

## [0.15.1] - 2026-06-20

### Changed

- VS Code DAG extension watches `~/.autonomic/logs/spine/executions/` for live refresh

## [0.15.0] - 2026-06-20

### Added

- **VS Code DAG extension** — `vscode-dag/` explorer + webview for `~/.autonomic/logs/spine/executions/*.dag.json`

## [0.14.1] - 2026-06-20

### Fixed

- Pin `agent-body-core` to `v0.3.1` so CI resolves `default_nats_url()` for JetStream integration

## [0.14.0] - 2026-06-20

### Added

- **JetStream event bus** — `event serve --nats-url` uses durable JetStream (falls back to in-memory)
- **Async sandbox dispatch** — Sandbox nodes publish to `autonomic.execute.sandbox` when `AUTONOMIC_NATS_URL` is set
- **Workflow state bridge** — Supervisor events published to `autonomic.spine.state` via JetStream

### Changed

- **`nats` feature enabled by default** — JetStream integration available out of the box
- Version bumped from `0.13.0` to `0.14.0`

## [0.13.0] - 2026-06-20

### Added

- **Global workspace paths** — default `state.db` and execution JSON graphs under `~/.autonomic/logs/spine/`
- **Autonomic HTTP API** — `/api/v1/health`, `/api/v1/workflows/run`, `/api/v1/executions/{id}` merged into the event server

### Fixed

- **NATS subscribe** — handle `async-nats` `subscribe()` returning `Result` (Clippy/CI)

### Changed

- Version bumped from `0.12.0` to `0.13.0`

## [0.12.0] - 2026-06-20

### Added

- **Omnichannel event bus** — new `event` module with `EventBus` trait, `InMemoryEventBus`, and optional `NatsEventBus` (behind `nats` feature flag) for topic-based pub/sub inter-agent communication
- **Agent registry** — `AgentRegistry` with register, list, get, heartbeat for agent service discovery
- **HTTP event API** — axum router with `POST /api/v1/events` (publish), `GET /api/v1/events/subscribe` (SSE stream), `POST /api/v1/agents` (register), `GET /api/v1/agents` (list), `GET /api/v1/agents/{name}` (get/heartbeat)
- **CLI commands** — `agent-spine event serve` (start bus on port 3100), `event pub` (publish via HTTP), `event sub` (SSE subscribe via HTTP), `agent list`, `agent register`, `agent info`
- **`start_event_server()`** — one-call function to start the event bus HTTP server as a background tokio task
- **New dependencies**: `async-trait`, `chrono`, `futures`, `async-stream`, `reqwest` (CLI), `async-nats` (optional)

### Changed

- Version bumped from `0.11.0` to `0.12.0`

## [0.11.0] - 2026-06-20

### Added

- **Docker sandbox execution** — `NodeKind::Sandbox` now runs commands in Docker containers instead of delegating to AI supervisor; extracts `_sandbox_command` from payload, returns `_sandbox_stdout`/`_sandbox_stderr`/`_sandbox_exit_code`
- **`sandbox::run_sandbox()`** — async Docker execution with configurable image, timeout, and workdir mount; uses `docker run --rm -i`

### Changed

- `NodeTask` gains `sandbox_config` field for per-node image/timeout settings
- Version bumped from `0.10.0` to `0.11.0`

## [0.10.0] - 2026-06-20

### Added

- **agent-body-core dependency** — shared types (ExecutionId, BrainProvenance) migrated from local definitions
- **Wake-on-call** — spawns agent-brain process automatically when unreachable
- **Workflow auto-trigger** — `BrainRouter::auto_submit_triggered_workflows` detects and submits triggered workflows from brain route_task responses
- **get_context_for_node integration** — `BrainRouter::get_context_for_node` calls brain for node-specific rules/skills/agents
- **Enriched trajectory logging** — `store_trajectory` now captures model_used, latency_ms, payload_snapshot, error_message

### Changed

- `ExecutionId` and `BrainProvenance` now re-exported from `agent-body-core`
- `BrainRouter::store_trajectory_full` accepts richer metadata params
- Executor injects `_brain_rules` and `_brain_tokens` into node payloads from brain context
- Version bumped from `0.9.0` to `0.10.0`

## [0.9.0] - 2026-06-19

### Added
- **Debate Node Kind**: `NodeKind::Debate` — alternates coder and critic agents in configurable rounds; accepts `debate_config` with `max_rounds`, `coder_prompt`, `critic_prompt`; injects `_node_role` into payload
- **Vote Node Kind**: `NodeKind::Vote` — runs the same prompt N times (configurable `count`, `temperature`); selects by `majority_vote`; injects `_node_role` into payload
- **Sandbox Node Kind**: `NodeKind::Sandbox` — node for isolated Docker execution; accepts `sandbox_config` with `image` and `timeout_secs`; injects `_node_role` and `_sandbox_image` into payload
- **Hydrate Node Kind**: `NodeKind::Hydrate` — static context-gathering phase before agent execution; brain enrichment runs but node is pass-through with no delegate call
- **Model Escalation**: `escalation_model` field on `WorkflowNode` — after retries exhausted, re-runs with `_escalation_model` injected into payload for frontier model dispatch
- **Config Structs**: `DebateConfig`, `VoteConfig`, `SandboxConfig` with sensible defaults and YAML serialization

### Changed
- `NodeKind` gains 5 new variants: `Debate`, `Vote`, `Sandbox`, `Hydrate`
- `WorkflowNode` gains `escalation_model`, `debate_config`, `vote_config`, `sandbox_config` fields
- Phase 2 executor handles all new kinds with appropriate delegate semantics; Hydrate is pass-through (no delegate call)
- Version bumped from `0.8.0` to `0.9.0`

## [0.8.0] - 2026-06-19

### Added
- **7 Built-in Workflows**: Production-ready YAML pipelines in `workflows/` — universal-developer, database-migration, security-patcher, deep-code-review, brutal-refactor, system-architect, root-cause-analysis
- **`init --with <workflow>`**: Generate a specific built-in workflow instead of the generic example; `init --with list` shows available workflows
- **Workflow Version Pinning**: Optional `min_spine_version` field in YAML frontmatter — validates against running binary version at parse time
- **Embedded Workflow Library**: New `workflows` module compiled into binary via `include_str!` — always available without disk files

### Changed
- `WorkflowDefinition` gains `min_spine_version` field (optional, e.g. `"0.8.0"`) with comparator in `validate()`
- `init` command now takes `--with <name>` flag; writes named workflow instead of generic example

## [0.7.0] - 2026-06-19

### Added
- **PostgresStateStore**: `sqlx::PgPool`-backed state store with JSONB column, `UNIQUE(execution_id, sequence)` constraint, GIN index — behind `postgres` feature gate
- **RedisStateStore**: `redis::aio::MultiplexedConnection`-backed store using sorted sets (`ZADD`/`ZRANGE`/`ZCARD`) — behind `redis` feature gate
- **Execution Cancellation**: `CancelToken` wrapping `tokio::sync::watch<bool>`; `setup_signal_handler()` for SIGINT/SIGTERM; executor checks per-cycle and saves final snapshot with `cancelled: true` marker before graceful exit
- **Idempotency Keys**: `IdempotencyStore` trait with `InMemoryIdempotencyStore` and `SqliteIdempotencyStore`; `idempotency_keys` table with `key` PK; `check_idempotency()` helper for side-effect deduplication
- **Concurrency Limits**: `tokio::sync::Semaphore` in `WorkflowManager` — configurable `--max-concurrency`; new submissions queue when limit reached
- **`--max-concurrency` flag**: Added to `serve` command, propagated to `WorkflowManager`

### Changed
- `WorkflowManager::new()` delegates to `with_concurrency_limit(db_path, brain_enabled, usize::MAX)` for backward compatibility
- Async state backends use `block_in_place` + `block_on` inside tokio multi-threaded runtime to satisfy synchronous `WorkflowState` trait

## [0.6.0] - 2026-06-19

### Added
- **WorkflowManager**: Central module for managing in-process workflow executions in `serve` mode — `submit()`, `list_executions()`, `execution_status()` with background execution via `tokio::spawn`
- **SubmitWorkflow RPC**: gRPC endpoint to submit YAML-based workflows for execution; returns execution ID immediately
- **GetExecutionStatus RPC**: Query the status (`running`/`completed`/`failed`) and current nodes of any submitted execution
- **ListRunningExecutions RPC**: List all in-flight/completed executions managed by the server
- **Tracing Spans**: `#[tracing::instrument]` annotations on `run()`, `prepare_and_append_snapshot()`, `enrich_from_brain()`, `log_trajectory()`; manual `tracing::Span` propagation in `JoinSet` spawned node tasks
- **Live Dashboard**: HTML/JS dashboard page served via axum on configurable `--dashboard-port` (defaults to gRPC port + 1); live event stream via gRPC-Web `WatchEvents`, execution table with auto-refresh, workflow YAML submission form

### Changed
- `Command::Serve` now takes `--dashboard-port` flag for the HTTP dashboard server
- `Supervisor::emit()` visibility changed to `pub(crate)` for WorkflowManager access
- gRPC server and dashboard HTTP server run as separate `tokio::spawn` tasks in a `tokio::select!`

## [0.5.0] - 2026-06-19

### Added
- **Fork/Join Node Kinds**: `Fork` fans out to N parallel paths with explicit barrier at `Join` — true Map-Reduce semantics beyond edge topology
- **Conditional Edges**: Edge `condition` field with expression parser — `state.task_type == "frontend"` evaluated at routing time; false = skipped
- **Meta-Router**: `agent-spine run --meta "task description"` queries agent-brain to select the correct workflow YAML before execution
- **Router Node Kind**: `NodeKind::Router` — delegates to LLM to inject state variables for dynamic branching
- **Join Barrier Tracking**: Pre-computed incoming edge counts; Join only fires when all Fork branches complete
- **Condition module**: Expression evaluator for `state.path.to.field <op> value` with `<`, `>`, `<=`, `>=`, `==`, `!=` support
- **Integration tests**: Fork/Join barrier, multi-level branches, conditional edge skipping

### Changed
- Fork and Join nodes execute as pass-through (no agent delegation)
- Router nodes use supervisor delegation (like Agent)
- Phase 5 routing evaluates condition expressions and Join barriers

## [0.4.0] - 2026-06-19

### Added
- **Context Provenance**: Snapshot metadata records `context_id`, `route_confidence`, `skills_used`, `agents_loaded` from each brain round-trip — injected as `_brain_provenance` in snapshot payloads
- **BrainProvenance**: Structured provenance struct replacing raw response injection
- **Per-Node BrainRouter**: Every agent/checkpoint node calls `agent-brain route_task` before execution, not only at run start
- **Outcome Feedback**: `store_trajectory` called with `task_kind` metadata for all completed nodes
- **Payload Limits**: `SnapshotConfig.max_payload_bytes` — oversize payloads produce `PayloadTooLarge` error before persist
- **Secrets Redaction**: `SnapshotConfig.secrets_redact` — configurable field patterns stripped from snapshots at write time (defaults: api_key, token, password, secret, private_key, authorization)
- **`--brain` flag**: `agent-spine run --brain` enables agent-brain integration from CLI
- **`payload_mut()` accessor**: Mutable payload access on `StateSnapshot` for in-place redaction

### Changed
- **enrich_from_brain**: Now uses `BrainProvenance` struct instead of raw `RouteTaskResponse`
- **log_trajectory**: Passes `task_kind` for richer learning loop metadata
- **Executor::run()**: All snapshot writes go through `prepare_and_append_snapshot` for redaction + size enforcement

## [0.3.0] - 2026-06-19

### Added
- **CLI doctor**: `agent-spine doctor` diagnoses setup issues — checks rustc, protoc, bun, agent-brain, config dir, validates example workflow
- **WorkflowEvent system**: Broadcast-channel event emission for node lifecycle (started, completed, failed, pending_approval, workflow_completed, workflow_failed)
- **gRPC WatchEvents**: Server-streaming RPC on SupervisorService — IDE/UI subscribe to real-time events
- **Enhanced PendingTask**: gRPC response includes `node_kind`, `description`, `workflow_name` for IDE briefing
- **GetPendingTaskDetail**: gRPC RPC returning full metadata + payload for a pending task
- **RetryPolicy validation**: YAML validates `max_attempts > 0` and `backoff_ms > 0` at parse time

### Changed
- **Supervisor::delegate()**: Now accepts `node_kind`, `description`, `workflow_name`; emits events on lifecycle transitions
- **Proto schema**: Added `WatchEvents` RPC, `WorkflowEvent` message, `GetPendingTaskDetail` RPC, enhanced `PendingTask`
- **NodeKind**: Added `Display` impl for string serialization in events and metadata

## [0.2.0] - 2026-06-19

### Added
- **Single-command install**: `install.sh` auto-detects OS/arch, downloads the correct binary from GitHub releases — `curl -fsSL https://raw.githubusercontent.com/aeswibon/agent-spine/main/install.sh | bash`
- **opencode config**: Project-level opencode configuration with agent-brain MCP integration

### Changed
- **README**: Replaced multi-step binary install with a single one-liner at the top

## [0.1.0] - 2026-06-19

### Added
- **Stateful Execution Engine**: A robust graph traversal engine that natively supports cyclic execution loops and state machines without explicitly managing LangChain/CrewAI boilerplates.
- **Time-Travel Debugging**: Immutable, append-only `FileStateStore` that dumps `StateSnapshot` payloads into a local `.jsonl` file to instantly replay loops and identify exact failing iterations.
- **Native IDE Supervisor**: A lightweight, pausing orchestrator that acts as an IDE hook via an embedded gRPC `SupervisorService`.
- **Confidence Router**: Intelligent execution routing that detects iteration ceilings (e.g., 5 continuous failures) and automatically injects an `escalation_required` flag into the state snapshot to escalate local API execution to a frontier model.
- **CI/CD Pipeline**: GitHub Actions orchestrator setup using `pipeline-compose` for linting, testing, building, and releasing.
- **agent-brain MCP Bridge**: `McpBridge` JSON-RPC 2.0 client over child-process stdio — handshake, tool calls, server request handling, with wrappers for `route_task`, `store_memory`, `store_trajectory`, `get_context`, `list_memory`.
- **BrainRouter**: Lazy-connect MCP bridge with graceful fallback to `ConfidenceRouter`, payload enrichment via agent-brain, trajectory logging at execution milestones, and transition routing.
- **Executor brain integration**: Phase 1 payload enrichment, Phase 4 trajectory logging, Phase 5 brain-backed routing with `ConfidenceRouter` fallback.
- **`brain` CLI subcommand**: `health`, `route`, `status` for interacting with agent-brain at runtime.
- **Async Send safety**: Extracted `MutexGuard`-holding operations into non-async helpers to satisfy `tokio::spawn` Send bounds. Added Send compile-time checks for `Executor`, `BrainRouter`, `McpBridge`, `ConfidenceRouter`.

### Changed
- **Executor::run()**: Now enriches agent/checkpoint payloads via BrainRouter, logs trajectories at init/complete/node milestones, and routes transitions through BrainRouter with fallback.
- **README**: Updated architecture diagrams, core modules table, and capability list to reflect MCP bridge integration.

### Fixed
- **Send-bound compilation**: `Executor<InMemoryStateStore>` not `Send` due to `MutexGuard` across async boundary — resolved by moving lock operations into non-async `append_snapshot` helper.
- **Test timing**: Functional tests failing because `McpBridge::connect(None)` would succeed when agent-brain is on PATH, causing slow LLM-based routing. Fixed by skipping MCP connect when `brain_cwd` is `None`.

### Removed
- Debug `#[tracing::instrument]` from `Executor::run()` — propagated Send issues; replaced with manual `tracing` calls.
