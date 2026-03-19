use libmcp::ReplayContract;
use serde_json::{Value, json};

use crate::mcp::output::with_render_property;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DispatchTarget {
    Host,
    Worker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub dispatch: DispatchTarget,
    pub replay: ReplayContract,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResourceSpec {
    pub uri: &'static str,
    pub dispatch: DispatchTarget,
    pub replay: ReplayContract,
}

impl ToolSpec {
    #[must_use]
    pub fn annotation_json(self) -> Value {
        json!({
            "title": self.name,
            "readOnlyHint": self.replay == ReplayContract::Convergent,
            "destructiveHint": self.replay == ReplayContract::NeverReplay,
            "fidgetSpinner": {
                "dispatch": match self.dispatch {
                    DispatchTarget::Host => "host",
                    DispatchTarget::Worker => "worker",
                },
                "replayContract": match self.replay {
                    ReplayContract::Convergent => "convergent",
                    ReplayContract::ProbeRequired => "probe_required",
                    ReplayContract::NeverReplay => "never_replay",
                },
            }
        })
    }
}

#[must_use]
pub(crate) fn tool_spec(name: &str) -> Option<ToolSpec> {
    match name {
        "project.bind" => Some(ToolSpec {
            name: "project.bind",
            description: "Bind this MCP session to a project root or nested path inside a project store.",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::NeverReplay,
        }),
        "project.status" => Some(ToolSpec {
            name: "project.status",
            description: "Read local project status, store paths, and git availability for the currently bound project.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "project.schema" => Some(ToolSpec {
            name: "project.schema",
            description: "Read the project-local payload schema and field validation tiers.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "frontier.list" => Some(ToolSpec {
            name: "frontier.list",
            description: "List frontiers for the current project.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "frontier.status" => Some(ToolSpec {
            name: "frontier.status",
            description: "Read one frontier projection, including champion and active candidates.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "frontier.init" => Some(ToolSpec {
            name: "frontier.init",
            description: "Create a new frontier rooted in a contract node. If the project is a git repo, the current HEAD becomes the initial champion when possible.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "node.create" => Some(ToolSpec {
            name: "node.create",
            description: "Create a generic DAG node with project payload fields and optional lineage parents.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "change.record" => Some(ToolSpec {
            name: "change.record",
            description: "Record a core-path change hypothesis with low ceremony.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "node.list" => Some(ToolSpec {
            name: "node.list",
            description: "List recent nodes. Archived nodes are hidden unless explicitly requested.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "node.read" => Some(ToolSpec {
            name: "node.read",
            description: "Read one node including payload, diagnostics, and hidden annotations.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "node.annotate" => Some(ToolSpec {
            name: "node.annotate",
            description: "Attach a free-form annotation to any node.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "node.archive" => Some(ToolSpec {
            name: "node.archive",
            description: "Archive a node so it falls out of default enumeration without being deleted.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "note.quick" => Some(ToolSpec {
            name: "note.quick",
            description: "Push a quick off-path note without bureaucratic experiment closure.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "research.record" => Some(ToolSpec {
            name: "research.record",
            description: "Record off-path research or enabling work that should live in the DAG but not on the bureaucratic core path.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "experiment.close" => Some(ToolSpec {
            name: "experiment.close",
            description: "Atomically close a core-path experiment with candidate checkpoint capture, measured result, note, and verdict.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "skill.list" => Some(ToolSpec {
            name: "skill.list",
            description: "List bundled skills shipped with this package.",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::Convergent,
        }),
        "skill.show" => Some(ToolSpec {
            name: "skill.show",
            description: "Return one bundled skill text shipped with this package. Defaults to `fidget-spinner` when name is omitted.",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::Convergent,
        }),
        "system.health" => Some(ToolSpec {
            name: "system.health",
            description: "Read MCP host health, session binding, worker generation, rollout state, and the last fault.",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::Convergent,
        }),
        "system.telemetry" => Some(ToolSpec {
            name: "system.telemetry",
            description: "Read aggregate request, retry, restart, and per-operation telemetry for this MCP session.",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::Convergent,
        }),
        _ => None,
    }
}

#[must_use]
pub(crate) fn resource_spec(uri: &str) -> Option<ResourceSpec> {
    match uri {
        "fidget-spinner://project/config" => Some(ResourceSpec {
            uri: "fidget-spinner://project/config",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "fidget-spinner://project/schema" => Some(ResourceSpec {
            uri: "fidget-spinner://project/schema",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "fidget-spinner://skill/fidget-spinner" => Some(ResourceSpec {
            uri: "fidget-spinner://skill/fidget-spinner",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::Convergent,
        }),
        "fidget-spinner://skill/frontier-loop" => Some(ResourceSpec {
            uri: "fidget-spinner://skill/frontier-loop",
            dispatch: DispatchTarget::Host,
            replay: ReplayContract::Convergent,
        }),
        _ => None,
    }
}

#[must_use]
pub(crate) fn tool_definitions() -> Vec<Value> {
    [
        "project.bind",
        "project.status",
        "project.schema",
        "frontier.list",
        "frontier.status",
        "frontier.init",
        "node.create",
        "change.record",
        "node.list",
        "node.read",
        "node.annotate",
        "node.archive",
        "note.quick",
        "research.record",
        "experiment.close",
        "skill.list",
        "skill.show",
        "system.health",
        "system.telemetry",
    ]
    .into_iter()
    .filter_map(tool_spec)
    .map(|spec| {
        json!({
            "name": spec.name,
            "description": spec.description,
            "inputSchema": with_render_property(input_schema(spec.name)),
            "annotations": spec.annotation_json(),
        })
    })
    .collect()
}

#[must_use]
pub(crate) fn list_resources() -> Vec<Value> {
    vec![
        json!({
            "uri": "fidget-spinner://project/config",
            "name": "project-config",
            "description": "Project-local store configuration",
            "mimeType": "application/json"
        }),
        json!({
            "uri": "fidget-spinner://project/schema",
            "name": "project-schema",
            "description": "Project-local payload schema and validation tiers",
            "mimeType": "application/json"
        }),
        json!({
            "uri": "fidget-spinner://skill/fidget-spinner",
            "name": "fidget-spinner-skill",
            "description": "Bundled base Fidget Spinner skill text for this package",
            "mimeType": "text/markdown"
        }),
        json!({
            "uri": "fidget-spinner://skill/frontier-loop",
            "name": "frontier-loop-skill",
            "description": "Bundled frontier-loop specialization skill text for this package",
            "mimeType": "text/markdown"
        }),
    ]
}

fn input_schema(name: &str) -> Value {
    match name {
        "project.status" | "project.schema" | "skill.list" | "system.health"
        | "system.telemetry" => json!({"type":"object","additionalProperties":false}),
        "project.bind" => json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Project root or any nested path inside a project with .fidget_spinner state." }
            },
            "required": ["path"],
            "additionalProperties": false
        }),
        "skill.show" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Bundled skill name. Defaults to `fidget-spinner`." }
            },
            "additionalProperties": false
        }),
        "frontier.list" => json!({"type":"object","additionalProperties":false}),
        "frontier.status" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string", "description": "Frontier UUID" }
            },
            "required": ["frontier_id"],
            "additionalProperties": false
        }),
        "frontier.init" => json!({
            "type": "object",
            "properties": {
                "label": { "type": "string" },
                "objective": { "type": "string" },
                "contract_title": { "type": "string" },
                "contract_summary": { "type": "string" },
                "benchmark_suites": { "type": "array", "items": { "type": "string" } },
                "promotion_criteria": { "type": "array", "items": { "type": "string" } },
                "primary_metric": metric_spec_schema(),
                "supporting_metrics": { "type": "array", "items": metric_spec_schema() },
                "seed_summary": { "type": "string" }
            },
            "required": ["label", "objective", "contract_title", "benchmark_suites", "promotion_criteria", "primary_metric"],
            "additionalProperties": false
        }),
        "node.create" => json!({
            "type": "object",
            "properties": {
                "class": node_class_schema(),
                "frontier_id": { "type": "string" },
                "title": { "type": "string" },
                "summary": { "type": "string" },
                "payload": { "type": "object" },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["class", "title"],
            "additionalProperties": false
        }),
        "change.record" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "title": { "type": "string" },
                "summary": { "type": "string" },
                "body": { "type": "string" },
                "hypothesis": { "type": "string" },
                "base_checkpoint_id": { "type": "string" },
                "benchmark_suite": { "type": "string" },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["frontier_id", "title", "body"],
            "additionalProperties": false
        }),
        "node.list" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "class": node_class_schema(),
                "include_archived": { "type": "boolean" },
                "limit": { "type": "integer", "minimum": 1, "maximum": 500 }
            },
            "additionalProperties": false
        }),
        "node.read" | "node.archive" => json!({
            "type": "object",
            "properties": {
                "node_id": { "type": "string" }
            },
            "required": ["node_id"],
            "additionalProperties": false
        }),
        "node.annotate" => json!({
            "type": "object",
            "properties": {
                "node_id": { "type": "string" },
                "body": { "type": "string" },
                "label": { "type": "string" },
                "visible": { "type": "boolean" }
            },
            "required": ["node_id", "body"],
            "additionalProperties": false
        }),
        "note.quick" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "title": { "type": "string" },
                "body": { "type": "string" },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["title", "body"],
            "additionalProperties": false
        }),
        "research.record" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "title": { "type": "string" },
                "summary": { "type": "string" },
                "body": { "type": "string" },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["title", "body"],
            "additionalProperties": false
        }),
        "experiment.close" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "base_checkpoint_id": { "type": "string" },
                "change_node_id": { "type": "string" },
                "candidate_summary": { "type": "string" },
                "run": run_schema(),
                "primary_metric": metric_observation_schema(),
                "supporting_metrics": { "type": "array", "items": metric_observation_schema() },
                "note": note_schema(),
                "verdict": verdict_schema(),
                "decision_title": { "type": "string" },
                "decision_rationale": { "type": "string" },
                "analysis_node_id": { "type": "string" }
            },
            "required": [
                "frontier_id",
                "base_checkpoint_id",
                "change_node_id",
                "candidate_summary",
                "run",
                "primary_metric",
                "note",
                "verdict",
                "decision_title",
                "decision_rationale"
            ],
            "additionalProperties": false
        }),
        _ => json!({"type":"object","additionalProperties":false}),
    }
}

fn metric_spec_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "key": { "type": "string" },
            "unit": metric_unit_schema(),
            "objective": optimization_objective_schema()
        },
        "required": ["key", "unit", "objective"],
        "additionalProperties": false
    })
}

fn metric_observation_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "key": { "type": "string" },
            "unit": metric_unit_schema(),
            "objective": optimization_objective_schema(),
            "value": { "type": "number" }
        },
        "required": ["key", "unit", "objective", "value"],
        "additionalProperties": false
    })
}

fn annotation_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "body": { "type": "string" },
            "label": { "type": "string" },
            "visible": { "type": "boolean" }
        },
        "required": ["body"],
        "additionalProperties": false
    })
}

fn node_class_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["contract", "change", "run", "analysis", "decision", "research", "enabling", "note"]
    })
}

fn metric_unit_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["seconds", "bytes", "count", "ratio", "custom"]
    })
}

fn optimization_objective_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["minimize", "maximize", "target"]
    })
}

fn verdict_schema() -> Value {
    json!({
        "type": "string",
        "enum": [
            "promote_to_champion",
            "keep_on_frontier",
            "revert_to_champion",
            "archive_dead_end",
            "needs_more_evidence"
        ]
    })
}

fn run_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "summary": { "type": "string" },
            "backend": {
                "type": "string",
                "enum": ["local_process", "worktree_process", "ssh_process"]
            },
            "benchmark_suite": { "type": "string" },
            "command": {
                "type": "object",
                "properties": {
                    "working_directory": { "type": "string" },
                    "argv": { "type": "array", "items": { "type": "string" } },
                    "env": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "required": ["argv"],
                "additionalProperties": false
            }
        },
        "required": ["title", "backend", "benchmark_suite", "command"],
        "additionalProperties": false
    })
}

fn note_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "summary": { "type": "string" },
            "next_hypotheses": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["summary"],
        "additionalProperties": false
    })
}
