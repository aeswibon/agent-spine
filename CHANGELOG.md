# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Stateful Execution Engine**: A robust graph traversal engine that natively supports cyclic execution loops and state machines without explicitly managing LangChain/CrewAI boilerplates.
- **Time-Travel Debugging**: Immutable, append-only `FileStateStore` that dumps `StateSnapshot` payloads into a local `.jsonl` file to instantly replay loops and identify exact failing iterations.
- **Native IDE Supervisor**: A lightweight, pausing orchestrator that acts as an IDE hook via an embedded gRPC `SupervisorService`.
- **Confidence Router**: Intelligent execution routing that detects iteration ceilings (e.g., 5 continuous failures) and automatically injects an `escalation_required` flag into the state snapshot to escalate local API execution to a frontier model.
- **CI/CD Pipeline**: GitHub Actions orchestrator setup using `pipeline-compose` for linting, testing, building, and releasing.
