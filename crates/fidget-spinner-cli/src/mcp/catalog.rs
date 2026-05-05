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

const TOOL_SPECS: &[ToolSpec] = &[
    ToolSpec {
        name: "project.bind",
        description: "Bind this MCP session to a project root or nested path inside a project store.",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "project.status",
        description: "Read coarse project metadata and ledger counts for the bound project.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "tag.add",
        description: "Register one repo-local tag with a required description unless tag definition writes are locked.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "tag.list",
        description: "List active tags plus supervisor tag families, locks, and stale-name guidance.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "frontier.create",
        description: "Create a new frontier scope.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "frontier.list",
        description: "List frontier scopes in the current project.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "frontier.read",
        description: "Read one frontier record, including its brief.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "frontier.open",
        description: "Open the bounded frontier overview: brief, active tags, live metrics, active hypotheses, and open experiments.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "frontier.update",
        description: "Patch frontier objective and grounding state.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "frontier.history",
        description: "Read the frontier revision history.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "frontier.query.schema",
        description: "Read the stable monofrontier SQL query view schema. Query views are q_* surfaces and do not expose frontier columns.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "frontier.query.sql",
        description: "Run one read-only SQL statement against stable q_* views scoped to one frontier.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "hypothesis.record",
        description: "Record an idea eagerly as a cheap hypothesis node, including expected yield and confidence. The body must stay a single paragraph.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "hypothesis.list",
        description: "List hypotheses, optionally narrowed by frontier or tag.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "hypothesis.read",
        description: "Read one hypothesis with its local neighborhood and experiments.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "hypothesis.update",
        description: "Patch hypothesis title, summary, body, expected yield, confidence, tags, or influence parents.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "hypothesis.history",
        description: "Read the revision history for one hypothesis.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "experiment.open",
        description: "Open one experiment anchored to exactly one hypothesis.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "experiment.list",
        description: "List experiments, optionally narrowed by frontier, hypothesis, status, or tags.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "experiment.read",
        description: "Read one experiment with its owning hypothesis, local neighborhood, and outcome.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "experiment.update",
        description: "Patch experiment metadata, influence parents, archive state, or replace the closed outcome wholesale.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "experiment.close",
        description: "Close one open experiment with typed conditions, structured metrics, verdict, rationale, and optional analysis. Requires a clean git worktree and records HEAD automatically from command.working_directory when provided, else from the bound project root.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "experiment.nearest",
        description: "Find the nearest accepted, kept, rejected, and champion comparators for one slice.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "experiment.history",
        description: "Read the revision history for one experiment.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "metric.define",
        description: "Register one project-level metric definition.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "metric.keys",
        description: "List metric keys, defaulting to the live frontier comparison set.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "metric.best",
        description: "Rank closed experiments by one metric key with optional frontier, hypothesis, or condition narrowing.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "kpi.create",
        description: "Promote one existing metric into a KPI metric for one frontier unless KPI creation is locked.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "kpi.list",
        description: "List mandatory KPI metrics for one frontier in supervisor-defined canonical order.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "kpi.reference.set",
        description: "Set one named reference line for one frontier KPI metric. The value is normalized through the metric dimension; omitted unit means the metric display unit.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "kpi.reference.list",
        description: "List named reference lines for frontier KPI metrics.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "kpi.reference.delete",
        description: "Delete one named reference line from one frontier KPI metric.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "kpi.best",
        description: "Rank closed experiments by one frontier KPI metric.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "condition.define",
        description: "Register one typed experimental condition key.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::NeverReplay,
    },
    ToolSpec {
        name: "condition.list",
        description: "List registered experimental condition keys.",
        dispatch: DispatchTarget::Worker,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "skill.list",
        description: "List bundled skills shipped with this package.",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "skill.show",
        description: "Return one bundled skill text shipped with this package. Defaults to `fidget-spinner` when name is omitted.",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "system.health",
        description: "Read MCP host health, session binding, worker generation, and rollout state.",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::Convergent,
    },
    ToolSpec {
        name: "system.telemetry",
        description: "Read aggregate MCP host telemetry for this session.",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::Convergent,
    },
];

const RESOURCE_SPECS: &[ResourceSpec] = &[
    ResourceSpec {
        uri: "fidget-spinner://skill/fidget-spinner",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::Convergent,
    },
    ResourceSpec {
        uri: "fidget-spinner://skill/frontier-loop",
        dispatch: DispatchTarget::Host,
        replay: ReplayContract::Convergent,
    },
];

#[must_use]
pub(crate) fn tool_spec(name: &str) -> Option<ToolSpec> {
    TOOL_SPECS.iter().copied().find(|spec| spec.name == name)
}

#[must_use]
pub(crate) fn resource_spec(uri: &str) -> Option<ResourceSpec> {
    RESOURCE_SPECS.iter().copied().find(|spec| spec.uri == uri)
}

#[must_use]
pub(crate) fn tool_definitions() -> Vec<Value> {
    TOOL_SPECS
        .iter()
        .copied()
        .map(|spec| {
            json!({
                "name": spec.name,
                "description": spec.description,
                "annotations": spec.annotation_json(),
                "inputSchema": tool_input_schema(spec.name),
            })
        })
        .collect()
}

#[must_use]
pub(crate) fn list_resources() -> Vec<Value> {
    RESOURCE_SPECS
        .iter()
        .map(|spec| {
            json!({
                "uri": spec.uri,
                "name": spec.uri.rsplit('/').next().unwrap_or(spec.uri),
                "description": resource_description(spec.uri),
            })
        })
        .collect()
}

fn resource_description(uri: &str) -> &'static str {
    match uri {
        "fidget-spinner://skill/fidget-spinner" => "Bundled Fidget Spinner operating doctrine.",
        "fidget-spinner://skill/frontier-loop" => "Bundled frontier-loop specialization.",
        _ => "Fidget Spinner resource.",
    }
}

fn tool_input_schema(name: &str) -> Value {
    let schema = match name {
        "project.bind" => object_schema(
            &[(
                "path",
                string_schema("Project root or any nested path inside it."),
            )],
            &["path"],
        ),
        "project.status" | "tag.list" | "condition.list" | "skill.list" | "system.health"
        | "system.telemetry" => empty_object_schema(),
        "tag.add" => object_schema(
            &[
                ("name", string_schema("Repo-local tag token.")),
                (
                    "description",
                    string_schema("Human-facing tag description."),
                ),
            ],
            &["name", "description"],
        ),
        "frontier.create" => object_schema(
            &[
                ("label", string_schema("Short frontier label.")),
                ("objective", string_schema("Frontier objective.")),
                ("slug", string_schema("Optional stable frontier slug.")),
            ],
            &["label", "objective"],
        ),
        "frontier.list" => object_schema(&[], &[]),
        "frontier.read" | "frontier.open" | "frontier.history" | "frontier.query.schema" => {
            object_schema(
                &[("frontier", selector_schema("Frontier UUID or slug."))],
                &["frontier"],
            )
        }
        "frontier.query.sql" => object_schema(
            &[
                ("frontier", selector_schema("Frontier UUID or slug.")),
                (
                    "sql",
                    string_schema("One read-only SQL statement against q_* query views."),
                ),
                ("params", sql_params_schema()),
                (
                    "max_rows",
                    integer_schema("Optional row cap, clamped to 1000."),
                ),
                (
                    "timeout_ms",
                    integer_schema("Optional query time budget in milliseconds, clamped to 2000."),
                ),
            ],
            &["frontier", "sql"],
        ),
        "frontier.update" => object_schema(
            &[
                ("frontier", selector_schema("Frontier UUID or slug.")),
                (
                    "expected_revision",
                    integer_schema("Optimistic concurrency guard."),
                ),
                (
                    "label",
                    string_schema("Optional replacement frontier label."),
                ),
                (
                    "objective",
                    string_schema("Optional replacement frontier objective."),
                ),
                (
                    "status",
                    enum_string_schema(
                        &["exploring", "paused"],
                        "Optional replacement frontier status. Archiving is supervisor-only.",
                    ),
                ),
                (
                    "situation",
                    nullable_string_schema("Optional frontier situation text."),
                ),
                ("roadmap", roadmap_schema()),
                (
                    "unknowns",
                    string_array_schema("Ordered frontier unknowns."),
                ),
            ],
            &["frontier"],
        ),
        "hypothesis.record" => object_schema(
            &[
                ("frontier", selector_schema("Owning frontier UUID or slug.")),
                (
                    "title",
                    string_schema(
                        "Terse idea title; hypotheses are cheap and should be opened eagerly.",
                    ),
                ),
                (
                    "summary",
                    string_schema("One-line summary of the idea, branch, suspicion, or mechanism."),
                ),
                (
                    "body",
                    string_schema(
                        "Single-paragraph hypothesis body. Capture the thought now; refine later.",
                    ),
                ),
                (
                    "expected_yield",
                    enum_string_schema(
                        &["low", "medium", "high"],
                        "Crude expected KPI-moving yield vibe check.",
                    ),
                ),
                (
                    "confidence",
                    enum_string_schema(
                        &["low", "medium", "high"],
                        "Crude confidence vibe check for the hypothesis.",
                    ),
                ),
                ("slug", string_schema("Optional stable hypothesis slug.")),
                ("tags", string_array_schema("Tag names.")),
                ("parents", vertex_selector_array_schema()),
            ],
            &[
                "frontier",
                "title",
                "summary",
                "body",
                "expected_yield",
                "confidence",
            ],
        ),
        "hypothesis.list" => object_schema(
            &[
                (
                    "frontier",
                    selector_schema("Optional frontier UUID or slug."),
                ),
                ("tags", string_array_schema("Require all listed tags.")),
                ("limit", integer_schema("Optional row cap.")),
            ],
            &[],
        ),
        "hypothesis.read" | "hypothesis.history" => object_schema(
            &[("hypothesis", selector_schema("Hypothesis UUID or slug."))],
            &["hypothesis"],
        ),
        "hypothesis.update" => object_schema(
            &[
                ("hypothesis", selector_schema("Hypothesis UUID or slug.")),
                (
                    "expected_revision",
                    integer_schema("Optimistic concurrency guard."),
                ),
                ("title", string_schema("Replacement title.")),
                ("summary", string_schema("Replacement summary.")),
                ("body", string_schema("Replacement single-paragraph body.")),
                (
                    "expected_yield",
                    enum_string_schema(
                        &["low", "medium", "high"],
                        "Replacement expected KPI-moving yield vibe check.",
                    ),
                ),
                (
                    "confidence",
                    enum_string_schema(
                        &["low", "medium", "high"],
                        "Replacement confidence vibe check.",
                    ),
                ),
                ("tags", string_array_schema("Replacement tag set.")),
                ("parents", vertex_selector_array_schema()),
            ],
            &["hypothesis"],
        ),
        "experiment.open" => object_schema(
            &[
                (
                    "hypothesis",
                    selector_schema("Owning hypothesis UUID or slug."),
                ),
                ("title", string_schema("Experiment title.")),
                ("summary", string_schema("Optional experiment summary.")),
                ("slug", string_schema("Optional stable experiment slug.")),
                ("tags", string_array_schema("Tag names.")),
                ("parents", vertex_selector_array_schema()),
            ],
            &["hypothesis", "title"],
        ),
        "experiment.list" => object_schema(
            &[
                (
                    "frontier",
                    selector_schema("Optional frontier UUID or slug."),
                ),
                (
                    "hypothesis",
                    selector_schema("Optional hypothesis UUID or slug."),
                ),
                ("tags", string_array_schema("Require all listed tags.")),
                (
                    "status",
                    enum_string_schema(&["open", "closed"], "Optional experiment status filter."),
                ),
                ("limit", integer_schema("Optional row cap.")),
            ],
            &[],
        ),
        "experiment.read" | "experiment.history" => object_schema(
            &[("experiment", selector_schema("Experiment UUID or slug."))],
            &["experiment"],
        ),
        "experiment.update" => object_schema(
            &[
                ("experiment", selector_schema("Experiment UUID or slug.")),
                (
                    "expected_revision",
                    integer_schema("Optimistic concurrency guard."),
                ),
                ("title", string_schema("Replacement title.")),
                (
                    "summary",
                    nullable_string_schema("Replacement summary or explicit null."),
                ),
                ("tags", string_array_schema("Replacement tag set.")),
                ("parents", vertex_selector_array_schema()),
                ("outcome", experiment_outcome_schema()),
            ],
            &["experiment"],
        ),
        "experiment.close" => object_schema(
            &[
                ("experiment", selector_schema("Experiment UUID or slug.")),
                (
                    "expected_revision",
                    integer_schema("Optimistic concurrency guard."),
                ),
                (
                    "backend",
                    enum_string_schema(
                        &["manual", "local_process", "worktree_process", "ssh_process"],
                        "Execution backend.",
                    ),
                ),
                ("command", command_schema()),
                ("conditions", conditions_schema()),
                ("primary_metric", metric_value_schema()),
                ("supporting_metrics", metric_value_array_schema()),
                (
                    "verdict",
                    enum_string_schema(
                        &["accepted", "kept", "parked", "rejected"],
                        "Closed verdict.",
                    ),
                ),
                ("rationale", string_schema("Decision rationale.")),
                ("analysis", experiment_analysis_schema()),
            ],
            &[
                "experiment",
                "backend",
                "command",
                "conditions",
                "primary_metric",
                "verdict",
                "rationale",
            ],
        ),
        "experiment.nearest" => object_schema(
            &[
                (
                    "frontier",
                    selector_schema("Optional frontier UUID or slug."),
                ),
                (
                    "hypothesis",
                    selector_schema("Optional hypothesis UUID or slug."),
                ),
                (
                    "experiment",
                    selector_schema("Optional experiment UUID or slug used as an anchor."),
                ),
                (
                    "metric",
                    string_schema("Optional metric key used to choose the champion."),
                ),
                ("conditions", conditions_schema()),
                ("tags", string_array_schema("Require all listed tags.")),
                (
                    "order",
                    enum_string_schema(
                        &["asc", "desc"],
                        "Optional explicit champion ranking direction.",
                    ),
                ),
            ],
            &[],
        ),
        "metric.define" => object_schema(
            &[
                ("key", string_schema("Metric key.")),
                (
                    "dimension",
                    enum_string_schema(
                        &["time", "count", "bytes", "ratio", "dimensionless"],
                        "Metric dimension.",
                    ),
                ),
                (
                    "display_unit",
                    string_schema(
                        "Optional display unit. Builtins include `scalar`, `count`, `ratio`, `percent`, `bytes`, `kibibytes`, `mebibytes`, `gibibytes`, `nanoseconds`, `microseconds`, `milliseconds`, and `seconds`.",
                    ),
                ),
                (
                    "objective",
                    enum_string_schema(
                        &["minimize", "maximize", "target"],
                        "Optimization objective.",
                    ),
                ),
                (
                    "aggregation",
                    enum_string_schema(
                        &[
                            "point", "mean", "geomean", "median", "p95", "min", "max", "sum",
                        ],
                        "Observation aggregation semantics. Defaults to point.",
                    ),
                ),
                ("description", string_schema("Optional description.")),
            ],
            &["key", "dimension", "objective"],
        ),
        "metric.keys" => object_schema(
            &[
                (
                    "frontier",
                    selector_schema("Optional frontier UUID or slug."),
                ),
                (
                    "scope",
                    enum_string_schema(
                        &["kpi", "live", "default"],
                        "Default-visible registry slice to enumerate. Hidden-by-archive entities are supervisor-only and are not exposed through MCP.",
                    ),
                ),
            ],
            &[],
        ),
        "metric.best" => object_schema(
            &[
                (
                    "frontier",
                    selector_schema("Optional frontier UUID or slug."),
                ),
                (
                    "hypothesis",
                    selector_schema("Optional hypothesis UUID or slug."),
                ),
                ("key", string_schema("Metric key.")),
                ("conditions", conditions_schema()),
                (
                    "include_rejected",
                    boolean_schema("Include rejected experiments."),
                ),
                ("limit", integer_schema("Optional row cap.")),
                (
                    "order",
                    enum_string_schema(&["asc", "desc"], "Optional explicit ranking direction."),
                ),
            ],
            &["key"],
        ),
        "kpi.create" => object_schema(
            &[
                ("frontier", selector_schema("Owning frontier UUID or slug.")),
                (
                    "metric",
                    string_schema("Existing metric key to promote into a one-metric KPI."),
                ),
            ],
            &["frontier", "metric"],
        ),
        "kpi.list" => object_schema(
            &[("frontier", selector_schema("Frontier UUID or slug."))],
            &["frontier"],
        ),
        "kpi.reference.set" => object_schema(
            &[
                ("frontier", selector_schema("Frontier UUID or slug.")),
                ("kpi", string_schema("KPI metric key or KPI id.")),
                ("label", string_schema("Reference line label.")),
                ("value", number_schema("Reference value.")),
                (
                    "unit",
                    string_schema("Optional metric unit. Omit to use the metric display unit."),
                ),
            ],
            &["frontier", "kpi", "label", "value"],
        ),
        "kpi.reference.list" => object_schema(
            &[
                ("frontier", selector_schema("Frontier UUID or slug.")),
                ("kpi", string_schema("Optional KPI metric key or KPI id.")),
            ],
            &["frontier"],
        ),
        "kpi.reference.delete" => object_schema(
            &[
                ("frontier", selector_schema("Frontier UUID or slug.")),
                ("kpi", string_schema("KPI metric key or KPI id.")),
                (
                    "reference",
                    string_schema("Reference label or reference id."),
                ),
            ],
            &["frontier", "kpi", "reference"],
        ),
        "kpi.best" => object_schema(
            &[
                ("frontier", selector_schema("Frontier UUID or slug.")),
                (
                    "kpi",
                    string_schema("Optional KPI metric key. Defaults to the first KPI metric."),
                ),
                ("conditions", conditions_schema()),
                (
                    "include_rejected",
                    boolean_schema("Include rejected experiments."),
                ),
                ("limit", integer_schema("Optional row cap.")),
            ],
            &["frontier"],
        ),
        "condition.define" => object_schema(
            &[
                ("key", string_schema("Condition key.")),
                (
                    "value_type",
                    enum_string_schema(
                        &["string", "numeric", "boolean", "timestamp"],
                        "Condition value type.",
                    ),
                ),
                ("description", string_schema("Optional description.")),
            ],
            &["key", "value_type"],
        ),
        "skill.show" => object_schema(&[("name", string_schema("Bundled skill name."))], &[]),
        _ => empty_object_schema(),
    };
    with_common_presentation(schema)
}

fn empty_object_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false,
    })
}

fn object_schema(properties: &[(&str, Value)], required: &[&str]) -> Value {
    let mut map = serde_json::Map::new();
    for (key, value) in properties {
        let _ = map.insert((*key).to_owned(), value.clone());
    }
    json!({
        "type": "object",
        "properties": Value::Object(map),
        "required": required,
        "additionalProperties": false,
    })
}

fn string_schema(description: &str) -> Value {
    json!({ "type": "string", "description": description })
}

fn nullable_string_schema(description: &str) -> Value {
    json!({
        "description": description,
        "oneOf": [
            { "type": "string" },
            { "type": "null" }
        ]
    })
}

fn integer_schema(description: &str) -> Value {
    json!({ "type": "integer", "minimum": 0, "description": description })
}

fn number_schema(description: &str) -> Value {
    json!({ "type": "number", "description": description })
}

fn boolean_schema(description: &str) -> Value {
    json!({ "type": "boolean", "description": description })
}

fn enum_string_schema(values: &[&str], description: &str) -> Value {
    json!({ "type": "string", "enum": values, "description": description })
}

fn string_array_schema(description: &str) -> Value {
    json!({
        "type": "array",
        "items": { "type": "string" },
        "description": description
    })
}

fn sql_params_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "oneOf": [
                { "type": "string" },
                { "type": "number" },
                { "type": "integer" },
                { "type": "boolean" },
                { "type": "null" }
            ]
        },
        "description": "Positional SQL scalar parameters for ? placeholders."
    })
}

fn selector_schema(description: &str) -> Value {
    string_schema(description)
}

fn vertex_selector_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "kind": { "type": "string", "enum": ["hypothesis", "experiment"] },
            "selector": { "type": "string" }
        },
        "required": ["kind", "selector"],
        "additionalProperties": false
    })
}

fn vertex_selector_array_schema() -> Value {
    json!({ "type": "array", "items": vertex_selector_schema() })
}

fn roadmap_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "rank": { "type": "integer", "minimum": 0 },
                "hypothesis": { "type": "string" },
                "summary": { "type": "string" }
            },
            "required": ["rank", "hypothesis"],
            "additionalProperties": false
        }
    })
}

fn command_schema() -> Value {
    json!({
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
    })
}

fn metric_value_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "key": { "type": "string" },
            "value": { "type": "number" },
            "unit": {
                "type": "string",
                "description": "Optional reported unit. Required when the metric dimension admits more than one unit family member."
            }
        },
        "required": ["key", "value"],
        "additionalProperties": false
    })
}

fn metric_value_array_schema() -> Value {
    json!({ "type": "array", "items": metric_value_schema() })
}

fn conditions_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": true,
        "description": "Exact experimental condition filter or outcome condition map. Conditions describe setup needed for like-for-like comparison, such as instance, profile, implementation, seed, timeout, hardware, or dataset. Measured outcomes belong in metrics, not conditions."
    })
}

fn experiment_analysis_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "summary": { "type": "string" },
            "body": { "type": "string" }
        },
        "required": ["summary", "body"],
        "additionalProperties": false
    })
}

fn experiment_outcome_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "backend": { "type": "string", "enum": ["manual", "local_process", "worktree_process", "ssh_process"] },
            "command": command_schema(),
            "conditions": conditions_schema(),
            "primary_metric": metric_value_schema(),
            "supporting_metrics": metric_value_array_schema(),
            "verdict": { "type": "string", "enum": ["accepted", "kept", "parked", "rejected"] },
            "rationale": { "type": "string" },
            "analysis": experiment_analysis_schema()
        },
        "required": ["backend", "command", "conditions", "primary_metric", "verdict", "rationale"],
        "additionalProperties": false
    })
}
