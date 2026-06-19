# agent-spine Roadmap

## Standalone Features
*Features required to make `agent-spine` a top-tier generic YAML orchestrator, entirely independent of AI.*

- [ ] **DevOps Workflows:** Stabilize the orchestrator as a replacement for `make` or `bash` scripts, allowing parallel execution of complex CI/CD tasks locally.
- [ ] **VS Code Extension:** Build a dedicated IDE extension that visualizes the YAML DAG graphically and shows live execution states.
- [ ] **Robust Retry Policies:** Implement configurable exponential backoff, jitter, and hard-timeout schemas for generic execution nodes.
- [ ] **Dry-Run Mode:** Allow `agent-spine validate --dry-run` to trace the execution path and variable resolutions without executing shell commands.

## Integrated Features
*Features required to make `agent-spine` the central execution engine of the organism.*

- [ ] **Asynchronous Pub/Sub:** Strip out internal channels and wire all state transitions directly to `agent-nerves` JetStream, enabling zero-data-loss crash recovery.
- [ ] **Sandboxed Execution Nodes:** Create native `SandboxNode` types that automatically route their workloads to `agent-immune` Firecracker VMs instead of the local host OS.
- [ ] **Interactive ChatOps Gates:** Wire `ApprovalGate` nodes to publish events to `agent-mouth`, shifting the human approval process from the terminal into Slack.
- [ ] **AST Interception:** Route all generated bash commands through `agent-heart` before OS execution to enforce global safety rules.
