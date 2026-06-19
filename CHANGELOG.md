# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
