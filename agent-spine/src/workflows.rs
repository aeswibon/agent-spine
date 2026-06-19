/// Embedded built-in workflow YAML definitions.
///
/// These are compiled into the binary via `include_str!` so they are always
/// available, even without the `workflows/` directory on disk.
pub const UNIVERSAL_DEVELOPER: &str = include_str!("../../workflows/universal-developer.yaml");
pub const DATABASE_MIGRATION: &str = include_str!("../../workflows/database-migration.yaml");
pub const SECURITY_PATCHER: &str = include_str!("../../workflows/security-patcher.yaml");
pub const DEEP_CODE_REVIEW: &str = include_str!("../../workflows/deep-code-review.yaml");
pub const BRUTAL_REFACTOR: &str = include_str!("../../workflows/brutal-refactor.yaml");
pub const SYSTEM_ARCHITECT: &str = include_str!("../../workflows/system-architect.yaml");
pub const ROOT_CAUSE_ANALYSIS: &str = include_str!("../../workflows/root-cause-analysis.yaml");

/// All built-in workflow entries with their canonical name and YAML content.
pub const ALL: &[WorkflowEntry] = &[
    WorkflowEntry {
        name: "universal-developer",
        label: "Universal Developer",
        description: "General-purpose dev loop — hydrate, plan, write, test, lint, review",
        yaml: UNIVERSAL_DEVELOPER,
    },
    WorkflowEntry {
        name: "database-migration",
        label: "Database Migration",
        description: "Safe schema changes — analyze, write SQL, sandbox, apply, rollback test",
        yaml: DATABASE_MIGRATION,
    },
    WorkflowEntry {
        name: "security-patcher",
        label: "Security Patcher",
        description: "Automated CVE response — audit, bump, test, fix callers, open PR",
        yaml: SECURITY_PATCHER,
    },
    WorkflowEntry {
        name: "deep-code-review",
        label: "Deep Code Review",
        description: "Parallel code review — fan-out Security/Perf/Style critics, fan-in aggregator",
        yaml: DEEP_CODE_REVIEW,
    },
    WorkflowEntry {
        name: "brutal-refactor",
        label: "Brutal Refactor",
        description: "Large-scale refactors — AST search, draft, update callers, typecheck loop",
        yaml: BRUTAL_REFACTOR,
    },
    WorkflowEntry {
        name: "system-architect",
        label: "System Architect",
        description: "System design — gather context, ideate 3 architectures, evaluate, ADR",
        yaml: SYSTEM_ARCHITECT,
    },
    WorkflowEntry {
        name: "root-cause-analysis",
        label: "Root Cause Analysis",
        description: "Incident response — hydrate logs, hypotheses, reproduce, post-mortem",
        yaml: ROOT_CAUSE_ANALYSIS,
    },
];

/// Metadata for a built-in workflow entry.
pub struct WorkflowEntry {
    /// Short kebab-case identifier (used with `init --with`).
    pub name: &'static str,
    /// Human-readable label.
    pub label: &'static str,
    /// One-line description of what the workflow does.
    pub description: &'static str,
    /// The full YAML content.
    pub yaml: &'static str,
}

/// Look up a built-in workflow by name.
///
/// Returns `None` if `name` doesn't match any built-in workflow.
#[must_use]
pub fn find(name: &str) -> Option<&'static WorkflowEntry> {
    ALL.iter().find(|w| w.name == name)
}
