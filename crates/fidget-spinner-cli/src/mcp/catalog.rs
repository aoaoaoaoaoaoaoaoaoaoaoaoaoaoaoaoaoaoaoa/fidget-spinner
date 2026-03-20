use libmcp::ReplayContract;
use serde_json::{Value, json};

use crate::mcp::output::with_common_presentation;

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
        "schema.field.upsert" => Some(ToolSpec {
            name: "schema.field.upsert",
            description: "Add or replace one project-local payload schema field definition.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "schema.field.remove" => Some(ToolSpec {
            name: "schema.field.remove",
            description: "Remove one project-local payload schema field definition, optionally narrowed by node-class set.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "tag.add" => Some(ToolSpec {
            name: "tag.add",
            description: "Register one repo-local tag with a required description. Notes may only reference tags from this registry.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "tag.list" => Some(ToolSpec {
            name: "tag.list",
            description: "List repo-local tags available for note and node tagging.",
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
            description: "Read one frontier projection, including open/completed experiment counts and verdict totals.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "frontier.init" => Some(ToolSpec {
            name: "frontier.init",
            description: "Create a new frontier rooted in a contract node.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "node.create" => Some(ToolSpec {
            name: "node.create",
            description: "Create a generic DAG node with project payload fields and optional lineage parents.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "hypothesis.record" => Some(ToolSpec {
            name: "hypothesis.record",
            description: "Record a core-path hypothesis with low ceremony.",
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
        "source.record" => Some(ToolSpec {
            name: "source.record",
            description: "Record imported sources and documentary context that should live in the DAG without polluting the core path.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "metric.define" => Some(ToolSpec {
            name: "metric.define",
            description: "Register one project-level metric definition so experiment ingestion only has to send key/value observations.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "run.dimension.define" => Some(ToolSpec {
            name: "run.dimension.define",
            description: "Register one project-level run dimension used to slice metrics across scenarios, budgets, and flags.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "run.dimension.list" => Some(ToolSpec {
            name: "run.dimension.list",
            description: "List registered run dimensions together with observed value counts and sample values.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "metric.keys" => Some(ToolSpec {
            name: "metric.keys",
            description: "List rankable metric keys, including registered run metrics and observed payload-derived numeric fields.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "metric.best" => Some(ToolSpec {
            name: "metric.best",
            description: "Rank completed experiments by one numeric key, with optional run-dimension filters.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "metric.migrate" => Some(ToolSpec {
            name: "metric.migrate",
            description: "Re-run the idempotent legacy metric-plane normalization that registers canonical metrics and backfills benchmark_suite dimensions.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "experiment.open" => Some(ToolSpec {
            name: "experiment.open",
            description: "Open a stateful experiment against one hypothesis.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::NeverReplay,
        }),
        "experiment.list" => Some(ToolSpec {
            name: "experiment.list",
            description: "List currently open experiments, optionally narrowed to one frontier.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "experiment.read" => Some(ToolSpec {
            name: "experiment.read",
            description: "Read one currently open experiment by id.",
            dispatch: DispatchTarget::Worker,
            replay: ReplayContract::Convergent,
        }),
        "experiment.close" => Some(ToolSpec {
            name: "experiment.close",
            description: "Close one open experiment with typed run dimensions, preregistered metric observations, optional analysis, note, and verdict.",
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
        "schema.field.upsert",
        "schema.field.remove",
        "tag.add",
        "tag.list",
        "frontier.list",
        "frontier.status",
        "frontier.init",
        "node.create",
        "hypothesis.record",
        "node.list",
        "node.read",
        "node.annotate",
        "node.archive",
        "note.quick",
        "source.record",
        "metric.define",
        "run.dimension.define",
        "run.dimension.list",
        "metric.keys",
        "metric.best",
        "metric.migrate",
        "experiment.open",
        "experiment.list",
        "experiment.read",
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
            "inputSchema": with_common_presentation(input_schema(spec.name)),
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
        "project.status" | "project.schema" | "tag.list" | "skill.list" | "system.health"
        | "system.telemetry" | "run.dimension.list" | "metric.migrate" => {
            json!({"type":"object","additionalProperties":false})
        }
        "schema.field.upsert" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Project payload field name." },
                "node_classes": { "type": "array", "items": node_class_schema(), "description": "Optional node-class scope. Omit or pass [] for all classes." },
                "presence": field_presence_schema(),
                "severity": diagnostic_severity_schema(),
                "role": field_role_schema(),
                "inference_policy": inference_policy_schema(),
                "value_type": field_value_type_schema(),
            },
            "required": ["name", "presence", "severity", "role", "inference_policy"],
            "additionalProperties": false
        }),
        "schema.field.remove" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Project payload field name." },
                "node_classes": { "type": "array", "items": node_class_schema(), "description": "Optional exact node-class scope to remove." }
            },
            "required": ["name"],
            "additionalProperties": false
        }),
        "project.bind" => json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Project root or any nested path inside a project with .fidget_spinner state." }
            },
            "required": ["path"],
            "additionalProperties": false
        }),
        "tag.add" => json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Lowercase repo-local tag name." },
                "description": { "type": "string", "description": "Human-facing tag description." }
            },
            "required": ["name", "description"],
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
                "summary": { "type": "string", "description": "Required for `note` and `source` nodes." },
                "tags": { "type": "array", "items": tag_name_schema(), "description": "Required for `note` nodes; optional for other classes." },
                "payload": { "type": "object", "description": "`note` and `source` nodes require a non-empty string `body` field." },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["class", "title"],
            "additionalProperties": false
        }),
        "hypothesis.record" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "title": { "type": "string" },
                "summary": { "type": "string" },
                "body": { "type": "string" },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["frontier_id", "title", "summary", "body"],
            "additionalProperties": false
        }),
        "node.list" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "class": node_class_schema(),
                "tags": { "type": "array", "items": tag_name_schema() },
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
                "summary": { "type": "string" },
                "body": { "type": "string" },
                "tags": { "type": "array", "items": tag_name_schema() },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["title", "summary", "body", "tags"],
            "additionalProperties": false
        }),
        "source.record" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "title": { "type": "string" },
                "summary": { "type": "string" },
                "body": { "type": "string" },
                "tags": { "type": "array", "items": tag_name_schema() },
                "annotations": { "type": "array", "items": annotation_schema() },
                "parents": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["title", "summary", "body"],
            "additionalProperties": false
        }),
        "metric.define" => json!({
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "unit": metric_unit_schema(),
                "objective": optimization_objective_schema(),
                "description": { "type": "string" }
            },
            "required": ["key", "unit", "objective"],
            "additionalProperties": false
        }),
        "run.dimension.define" => json!({
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "value_type": field_value_type_schema(),
                "description": { "type": "string" }
            },
            "required": ["key", "value_type"],
            "additionalProperties": false
        }),
        "metric.keys" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "source": metric_source_schema(),
                "dimensions": { "type": "object" }
            },
            "additionalProperties": false
        }),
        "metric.best" => json!({
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "frontier_id": { "type": "string" },
                "source": metric_source_schema(),
                "dimensions": { "type": "object" },
                "order": metric_order_schema(),
                "limit": { "type": "integer", "minimum": 1, "maximum": 500 }
            },
            "required": ["key"],
            "additionalProperties": false
        }),
        "experiment.open" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" },
                "hypothesis_node_id": { "type": "string" },
                "title": { "type": "string" },
                "summary": { "type": "string" }
            },
            "required": ["frontier_id", "hypothesis_node_id", "title"],
            "additionalProperties": false
        }),
        "experiment.list" => json!({
            "type": "object",
            "properties": {
                "frontier_id": { "type": "string" }
            },
            "additionalProperties": false
        }),
        "experiment.read" => json!({
            "type": "object",
            "properties": {
                "experiment_id": { "type": "string" }
            },
            "required": ["experiment_id"],
            "additionalProperties": false
        }),
        "experiment.close" => json!({
            "type": "object",
            "properties": {
                "experiment_id": { "type": "string" },
                "run": run_schema(),
                "primary_metric": metric_value_schema(),
                "supporting_metrics": { "type": "array", "items": metric_value_schema() },
                "note": note_schema(),
                "verdict": verdict_schema(),
                "decision_title": { "type": "string" },
                "decision_rationale": { "type": "string" },
                "analysis": analysis_schema()
            },
            "required": [
                "experiment_id",
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

fn metric_value_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "key": { "type": "string" },
            "value": { "type": "number" }
        },
        "required": ["key", "value"],
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

fn analysis_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "title": { "type": "string" },
            "summary": { "type": "string" },
            "body": { "type": "string" }
        },
        "required": ["title", "summary", "body"],
        "additionalProperties": false
    })
}

fn tag_name_schema() -> Value {
    json!({
        "type": "string",
        "pattern": "^[a-z0-9]+(?:[-_/][a-z0-9]+)*$"
    })
}

fn node_class_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["contract", "hypothesis", "run", "analysis", "decision", "source", "note"]
    })
}

fn metric_unit_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["seconds", "bytes", "count", "ratio", "custom"]
    })
}

fn metric_source_schema() -> Value {
    json!({
        "type": "string",
        "enum": [
            "run_metric",
            "hypothesis_payload",
            "run_payload",
            "analysis_payload",
            "decision_payload"
        ]
    })
}

fn metric_order_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["asc", "desc"]
    })
}

fn field_value_type_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["string", "numeric", "boolean", "timestamp"]
    })
}

fn diagnostic_severity_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["error", "warning", "info"]
    })
}

fn field_presence_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["required", "recommended", "optional"]
    })
}

fn field_role_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["index", "projection_gate", "render_only", "opaque"]
    })
}

fn inference_policy_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["manual_only", "model_may_infer"]
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
            "accepted",
            "kept",
            "parked",
            "rejected"
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
            "dimensions": { "type": "object" },
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
        "required": ["title", "backend", "dimensions", "command"],
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
