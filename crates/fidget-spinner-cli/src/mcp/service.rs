use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    AdmissionState, AnnotationVisibility, CodeSnapshotRef, CommandRecipe, ExecutionBackend,
    FrontierContract, FrontierNote, FrontierProjection, FrontierRecord, FrontierVerdict,
    MetricObservation, MetricSpec, MetricUnit, NodeAnnotation, NodeClass, NodePayload,
    NonEmptyText, ProjectSchema, TagName, TagRecord,
};
use fidget_spinner_store_sqlite::{
    CloseExperimentRequest, CreateFrontierRequest, CreateNodeRequest, EdgeAttachment,
    EdgeAttachmentDirection, ExperimentReceipt, ListNodesQuery, NodeSummary, ProjectStore,
    StoreError,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
use crate::mcp::output::{
    ToolOutput, detailed_tool_output, split_presentation, tool_output, tool_success,
};
use crate::mcp::protocol::{TRANSIENT_ONCE_ENV, TRANSIENT_ONCE_MARKER_ENV, WorkerOperation};

pub(crate) struct WorkerService {
    store: ProjectStore,
}

impl WorkerService {
    pub fn new(project: &Utf8Path) -> Result<Self, StoreError> {
        Ok(Self {
            store: crate::open_store(project.as_std_path())?,
        })
    }

    pub fn execute(&mut self, operation: WorkerOperation) -> Result<Value, FaultRecord> {
        let operation_key = match &operation {
            WorkerOperation::CallTool { name, .. } => format!("tools/call:{name}"),
            WorkerOperation::ReadResource { uri } => format!("resources/read:{uri}"),
        };
        Self::maybe_inject_transient(&operation_key)?;

        match operation {
            WorkerOperation::CallTool { name, arguments } => self.call_tool(&name, arguments),
            WorkerOperation::ReadResource { uri } => self.read_resource(&uri),
        }
    }

    fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value, FaultRecord> {
        let operation = format!("tools/call:{name}");
        let (presentation, arguments) =
            split_presentation(arguments, &operation, FaultStage::Worker)?;
        match name {
            "project.status" => {
                let status = json!({
                    "project_root": self.store.project_root(),
                    "state_root": self.store.state_root(),
                    "display_name": self.store.config().display_name,
                    "schema": self.store.schema().schema_ref(),
                    "git_repo_detected": crate::run_git(self.store.project_root(), &["rev-parse", "--show-toplevel"])
                        .map_err(store_fault("tools/call:project.status"))?
                        .is_some(),
                });
                tool_success(
                    project_status_output(&status, self.store.schema()),
                    presentation,
                    FaultStage::Worker,
                    "tools/call:project.status",
                )
            }
            "project.schema" => tool_success(
                project_schema_output(self.store.schema())?,
                presentation,
                FaultStage::Worker,
                "tools/call:project.schema",
            ),
            "tag.add" => {
                let args = deserialize::<TagAddToolArgs>(arguments)?;
                let tag = self
                    .store
                    .add_tag(
                        TagName::new(args.name).map_err(store_fault("tools/call:tag.add"))?,
                        NonEmptyText::new(args.description)
                            .map_err(store_fault("tools/call:tag.add"))?,
                    )
                    .map_err(store_fault("tools/call:tag.add"))?;
                tool_success(
                    tag_add_output(&tag)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:tag.add",
                )
            }
            "tag.list" => {
                let tags = self
                    .store
                    .list_tags()
                    .map_err(store_fault("tools/call:tag.list"))?;
                tool_success(
                    tag_list_output(tags.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:tag.list",
                )
            }
            "frontier.list" => {
                let frontiers = self
                    .store
                    .list_frontiers()
                    .map_err(store_fault("tools/call:frontier.list"))?;
                tool_success(
                    frontier_list_output(frontiers.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:frontier.list",
                )
            }
            "frontier.status" => {
                let args = deserialize::<FrontierStatusToolArgs>(arguments)?;
                let projection = self
                    .store
                    .frontier_projection(
                        crate::parse_frontier_id(&args.frontier_id)
                            .map_err(store_fault("tools/call:frontier.status"))?,
                    )
                    .map_err(store_fault("tools/call:frontier.status"))?;
                tool_success(
                    frontier_status_output(&projection)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:frontier.status",
                )
            }
            "frontier.init" => {
                let args = deserialize::<FrontierInitToolArgs>(arguments)?;
                let initial_checkpoint = self
                    .store
                    .auto_capture_checkpoint(
                        NonEmptyText::new(
                            args.seed_summary
                                .unwrap_or_else(|| "initial champion checkpoint".to_owned()),
                        )
                        .map_err(store_fault("tools/call:frontier.init"))?,
                    )
                    .map_err(store_fault("tools/call:frontier.init"))?;
                let projection = self
                    .store
                    .create_frontier(CreateFrontierRequest {
                        label: NonEmptyText::new(args.label)
                            .map_err(store_fault("tools/call:frontier.init"))?,
                        contract_title: NonEmptyText::new(args.contract_title)
                            .map_err(store_fault("tools/call:frontier.init"))?,
                        contract_summary: args
                            .contract_summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:frontier.init"))?,
                        contract: FrontierContract {
                            objective: NonEmptyText::new(args.objective)
                                .map_err(store_fault("tools/call:frontier.init"))?,
                            evaluation: fidget_spinner_core::EvaluationProtocol {
                                benchmark_suites: crate::to_text_set(args.benchmark_suites)
                                    .map_err(store_fault("tools/call:frontier.init"))?,
                                primary_metric: MetricSpec {
                                    metric_key: NonEmptyText::new(args.primary_metric.key)
                                        .map_err(store_fault("tools/call:frontier.init"))?,
                                    unit: parse_metric_unit_name(&args.primary_metric.unit)
                                        .map_err(store_fault("tools/call:frontier.init"))?,
                                    objective: crate::parse_optimization_objective(
                                        &args.primary_metric.objective,
                                    )
                                    .map_err(store_fault("tools/call:frontier.init"))?,
                                },
                                supporting_metrics: args
                                    .supporting_metrics
                                    .into_iter()
                                    .map(metric_spec_from_wire)
                                    .collect::<Result<_, _>>()
                                    .map_err(store_fault("tools/call:frontier.init"))?,
                            },
                            promotion_criteria: crate::to_text_vec(args.promotion_criteria)
                                .map_err(store_fault("tools/call:frontier.init"))?,
                        },
                        initial_checkpoint,
                    })
                    .map_err(store_fault("tools/call:frontier.init"))?;
                tool_success(
                    frontier_created_output(&projection)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:frontier.init",
                )
            }
            "node.create" => {
                let args = deserialize::<NodeCreateToolArgs>(arguments)?;
                let node = self
                    .store
                    .add_node(CreateNodeRequest {
                        class: parse_node_class_name(&args.class)
                            .map_err(store_fault("tools/call:node.create"))?,
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:node.create"))?,
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:node.create"))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:node.create"))?,
                        tags: args
                            .tags
                            .map(parse_tag_set)
                            .transpose()
                            .map_err(store_fault("tools/call:node.create"))?,
                        payload: NodePayload::with_schema(
                            self.store.schema().schema_ref(),
                            args.payload.unwrap_or_default(),
                        ),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:node.create"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:node.create"))?,
                    })
                    .map_err(store_fault("tools/call:node.create"))?;
                tool_success(
                    created_node_output("created node", &node, "tools/call:node.create")?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:node.create",
                )
            }
            "change.record" => {
                let args = deserialize::<ChangeRecordToolArgs>(arguments)?;
                let mut fields = Map::new();
                let _ = fields.insert("body".to_owned(), Value::String(args.body));
                if let Some(hypothesis) = args.hypothesis {
                    let _ = fields.insert("hypothesis".to_owned(), Value::String(hypothesis));
                }
                if let Some(base_checkpoint_id) = args.base_checkpoint_id {
                    let _ = fields.insert(
                        "base_checkpoint_id".to_owned(),
                        Value::String(base_checkpoint_id),
                    );
                }
                if let Some(benchmark_suite) = args.benchmark_suite {
                    let _ =
                        fields.insert("benchmark_suite".to_owned(), Value::String(benchmark_suite));
                }
                let node = self
                    .store
                    .add_node(CreateNodeRequest {
                        class: NodeClass::Change,
                        frontier_id: Some(
                            crate::parse_frontier_id(&args.frontier_id)
                                .map_err(store_fault("tools/call:change.record"))?,
                        ),
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:change.record"))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:change.record"))?,
                        tags: None,
                        payload: NodePayload::with_schema(self.store.schema().schema_ref(), fields),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:change.record"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:change.record"))?,
                    })
                    .map_err(store_fault("tools/call:change.record"))?;
                tool_success(
                    created_node_output("recorded change", &node, "tools/call:change.record")?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:change.record",
                )
            }
            "node.list" => {
                let args = deserialize::<NodeListToolArgs>(arguments)?;
                let nodes = self
                    .store
                    .list_nodes(ListNodesQuery {
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:node.list"))?,
                        class: args
                            .class
                            .as_deref()
                            .map(parse_node_class_name)
                            .transpose()
                            .map_err(store_fault("tools/call:node.list"))?,
                        tags: parse_tag_set(args.tags)
                            .map_err(store_fault("tools/call:node.list"))?,
                        include_archived: args.include_archived,
                        limit: args.limit.unwrap_or(20),
                    })
                    .map_err(store_fault("tools/call:node.list"))?;
                tool_success(
                    node_list_output(nodes.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:node.list",
                )
            }
            "node.read" => {
                let args = deserialize::<NodeReadToolArgs>(arguments)?;
                let node_id = crate::parse_node_id(&args.node_id)
                    .map_err(store_fault("tools/call:node.read"))?;
                let node = self
                    .store
                    .get_node(node_id)
                    .map_err(store_fault("tools/call:node.read"))?
                    .ok_or_else(|| {
                        FaultRecord::new(
                            FaultKind::InvalidInput,
                            FaultStage::Store,
                            "tools/call:node.read",
                            format!("node {node_id} was not found"),
                        )
                    })?;
                tool_success(
                    node_read_output(&node)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:node.read",
                )
            }
            "node.annotate" => {
                let args = deserialize::<NodeAnnotateToolArgs>(arguments)?;
                let annotation = NodeAnnotation {
                    id: fidget_spinner_core::AnnotationId::fresh(),
                    visibility: if args.visible {
                        AnnotationVisibility::Visible
                    } else {
                        AnnotationVisibility::HiddenByDefault
                    },
                    label: args
                        .label
                        .map(NonEmptyText::new)
                        .transpose()
                        .map_err(store_fault("tools/call:node.annotate"))?,
                    body: NonEmptyText::new(args.body)
                        .map_err(store_fault("tools/call:node.annotate"))?,
                    created_at: time::OffsetDateTime::now_utc(),
                };
                self.store
                    .annotate_node(
                        crate::parse_node_id(&args.node_id)
                            .map_err(store_fault("tools/call:node.annotate"))?,
                        annotation,
                    )
                    .map_err(store_fault("tools/call:node.annotate"))?;
                tool_success(
                    tool_output(
                        &json!({"annotated": args.node_id}),
                        FaultStage::Worker,
                        "tools/call:node.annotate",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:node.annotate",
                )
            }
            "node.archive" => {
                let args = deserialize::<NodeArchiveToolArgs>(arguments)?;
                self.store
                    .archive_node(
                        crate::parse_node_id(&args.node_id)
                            .map_err(store_fault("tools/call:node.archive"))?,
                    )
                    .map_err(store_fault("tools/call:node.archive"))?;
                tool_success(
                    tool_output(
                        &json!({"archived": args.node_id}),
                        FaultStage::Worker,
                        "tools/call:node.archive",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:node.archive",
                )
            }
            "note.quick" => {
                let args = deserialize::<QuickNoteToolArgs>(arguments)?;
                let node = self
                    .store
                    .add_node(CreateNodeRequest {
                        class: NodeClass::Note,
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:note.quick"))?,
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:note.quick"))?,
                        summary: None,
                        tags: Some(
                            parse_tag_set(args.tags)
                                .map_err(store_fault("tools/call:note.quick"))?,
                        ),
                        payload: NodePayload::with_schema(
                            self.store.schema().schema_ref(),
                            crate::json_object(json!({ "body": args.body }))
                                .map_err(store_fault("tools/call:note.quick"))?,
                        ),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:note.quick"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:note.quick"))?,
                    })
                    .map_err(store_fault("tools/call:note.quick"))?;
                tool_success(
                    created_node_output("recorded note", &node, "tools/call:note.quick")?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:note.quick",
                )
            }
            "research.record" => {
                let args = deserialize::<ResearchRecordToolArgs>(arguments)?;
                let node = self
                    .store
                    .add_node(CreateNodeRequest {
                        class: NodeClass::Research,
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:research.record"))?,
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:research.record"))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:research.record"))?,
                        tags: None,
                        payload: NodePayload::with_schema(
                            self.store.schema().schema_ref(),
                            crate::json_object(json!({ "body": args.body }))
                                .map_err(store_fault("tools/call:research.record"))?,
                        ),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:research.record"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:research.record"))?,
                    })
                    .map_err(store_fault("tools/call:research.record"))?;
                tool_success(
                    created_node_output("recorded research", &node, "tools/call:research.record")?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:research.record",
                )
            }
            "experiment.close" => {
                let args = deserialize::<ExperimentCloseToolArgs>(arguments)?;
                let frontier_id = crate::parse_frontier_id(&args.frontier_id)
                    .map_err(store_fault("tools/call:experiment.close"))?;
                let snapshot = self
                    .store
                    .auto_capture_checkpoint(
                        NonEmptyText::new(args.candidate_summary.clone())
                            .map_err(store_fault("tools/call:experiment.close"))?,
                    )
                    .map_err(store_fault("tools/call:experiment.close"))?
                    .map(|seed| seed.snapshot)
                    .ok_or_else(|| {
                        FaultRecord::new(
                            FaultKind::Internal,
                            FaultStage::Store,
                            "tools/call:experiment.close",
                            format!(
                                "git repository inspection failed for {}",
                                self.store.project_root()
                            ),
                        )
                    })?;
                let receipt = self
                    .store
                    .close_experiment(CloseExperimentRequest {
                        frontier_id,
                        base_checkpoint_id: crate::parse_checkpoint_id(&args.base_checkpoint_id)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        change_node_id: crate::parse_node_id(&args.change_node_id)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        candidate_summary: NonEmptyText::new(args.candidate_summary)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        candidate_snapshot: snapshot,
                        run_title: NonEmptyText::new(args.run.title)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        run_summary: args
                            .run
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        backend: parse_backend_name(&args.run.backend)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        benchmark_suite: NonEmptyText::new(args.run.benchmark_suite)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        command: command_recipe_from_wire(
                            args.run.command,
                            self.store.project_root(),
                        )
                        .map_err(store_fault("tools/call:experiment.close"))?,
                        code_snapshot: Some(
                            capture_code_snapshot(self.store.project_root())
                                .map_err(store_fault("tools/call:experiment.close"))?,
                        ),
                        primary_metric: metric_observation_from_wire(args.primary_metric)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        supporting_metrics: args
                            .supporting_metrics
                            .into_iter()
                            .map(metric_observation_from_wire)
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        note: FrontierNote {
                            summary: NonEmptyText::new(args.note.summary)
                                .map_err(store_fault("tools/call:experiment.close"))?,
                            next_hypotheses: crate::to_text_vec(args.note.next_hypotheses)
                                .map_err(store_fault("tools/call:experiment.close"))?,
                        },
                        verdict: parse_verdict_name(&args.verdict)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        decision_title: NonEmptyText::new(args.decision_title)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        decision_rationale: NonEmptyText::new(args.decision_rationale)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        analysis_node_id: args
                            .analysis_node_id
                            .as_deref()
                            .map(crate::parse_node_id)
                            .transpose()
                            .map_err(store_fault("tools/call:experiment.close"))?,
                    })
                    .map_err(store_fault("tools/call:experiment.close"))?;
                tool_success(
                    experiment_close_output(&receipt)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:experiment.close",
                )
            }
            other => Err(FaultRecord::new(
                FaultKind::InvalidInput,
                FaultStage::Worker,
                format!("tools/call:{other}"),
                format!("unknown tool `{other}`"),
            )),
        }
    }

    fn read_resource(&mut self, uri: &str) -> Result<Value, FaultRecord> {
        match uri {
            "fidget-spinner://project/config" => Ok(json!({
                "contents": [{
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": crate::to_pretty_json(self.store.config())
                        .map_err(store_fault("resources/read:fidget-spinner://project/config"))?,
                }]
            })),
            "fidget-spinner://project/schema" => Ok(json!({
                "contents": [{
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": crate::to_pretty_json(self.store.schema())
                        .map_err(store_fault("resources/read:fidget-spinner://project/schema"))?,
                }]
            })),
            _ => Err(FaultRecord::new(
                FaultKind::InvalidInput,
                FaultStage::Worker,
                format!("resources/read:{uri}"),
                format!("unknown resource `{uri}`"),
            )),
        }
    }

    fn maybe_inject_transient(operation: &str) -> Result<(), FaultRecord> {
        let Some(target_operation) = std::env::var_os(TRANSIENT_ONCE_ENV) else {
            return Ok(());
        };
        let target_operation = target_operation.to_string_lossy();
        if target_operation != operation {
            return Ok(());
        }
        let Some(marker_path) = std::env::var_os(TRANSIENT_ONCE_MARKER_ENV) else {
            return Ok(());
        };
        if Utf8PathBuf::from(marker_path.to_string_lossy().into_owned()).exists() {
            return Ok(());
        }
        fs::write(&marker_path, b"triggered").map_err(|error| {
            FaultRecord::new(
                FaultKind::Internal,
                FaultStage::Worker,
                operation,
                format!("failed to write transient marker: {error}"),
            )
        })?;
        Err(FaultRecord::new(
            FaultKind::Transient,
            FaultStage::Worker,
            operation,
            format!("injected transient fault for {operation}"),
        )
        .retryable(None))
    }
}

fn deserialize<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, FaultRecord> {
    serde_json::from_value(value).map_err(|error| {
        FaultRecord::new(
            FaultKind::InvalidInput,
            FaultStage::Protocol,
            "worker.deserialize",
            format!("invalid params: {error}"),
        )
    })
}

fn project_status_output(full: &Value, schema: &ProjectSchema) -> ToolOutput {
    let concise = json!({
        "display_name": full["display_name"],
        "project_root": full["project_root"],
        "state_root": full["state_root"],
        "schema": schema_label(schema),
        "git_repo_detected": full["git_repo_detected"],
    });
    let git = if full["git_repo_detected"].as_bool().unwrap_or(false) {
        "detected"
    } else {
        "not detected"
    };
    ToolOutput::from_values(
        concise,
        full.clone(),
        [
            format!("project {}", value_summary(&full["display_name"])),
            format!("root: {}", value_summary(&full["project_root"])),
            format!("state: {}", value_summary(&full["state_root"])),
            format!("schema: {}", schema_label(schema)),
            format!("git: {git}"),
        ]
        .join("\n"),
        None,
    )
}

fn project_schema_output(schema: &ProjectSchema) -> Result<ToolOutput, FaultRecord> {
    let field_previews = schema
        .fields
        .iter()
        .take(8)
        .map(project_schema_field_value)
        .collect::<Vec<_>>();
    let concise = json!({
        "namespace": schema.namespace,
        "version": schema.version,
        "field_count": schema.fields.len(),
        "fields": field_previews,
        "truncated": schema.fields.len() > 8,
    });
    let mut lines = vec![
        format!("schema {}", schema_label(schema)),
        format!("{} field(s)", schema.fields.len()),
    ];
    for field in schema.fields.iter().take(8) {
        lines.push(format!(
            "{} [{}] {} {}",
            field.name,
            if field.node_classes.is_empty() {
                "any".to_owned()
            } else {
                field
                    .node_classes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            },
            format!("{:?}", field.presence).to_ascii_lowercase(),
            format!("{:?}", field.role).to_ascii_lowercase(),
        ));
    }
    if schema.fields.len() > 8 {
        lines.push(format!("... +{} more field(s)", schema.fields.len() - 8));
    }
    detailed_tool_output(
        &concise,
        schema,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:project.schema",
    )
}

fn tag_add_output(tag: &TagRecord) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "name": tag.name,
        "description": tag.description,
    });
    detailed_tool_output(
        &concise,
        tag,
        format!("registered tag {}\n{}", tag.name, tag.description),
        None,
        FaultStage::Worker,
        "tools/call:tag.add",
    )
}

fn tag_list_output(tags: &[TagRecord]) -> Result<ToolOutput, FaultRecord> {
    let concise = tags
        .iter()
        .map(|tag| {
            json!({
                "name": tag.name,
                "description": tag.description,
            })
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("{} tag(s)", tags.len())];
    lines.extend(
        tags.iter()
            .map(|tag| format!("{}: {}", tag.name, tag.description)),
    );
    detailed_tool_output(
        &concise,
        &tags,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:tag.list",
    )
}

fn frontier_list_output(frontiers: &[FrontierRecord]) -> Result<ToolOutput, FaultRecord> {
    let concise = frontiers
        .iter()
        .map(|frontier| {
            json!({
                "frontier_id": frontier.id,
                "label": frontier.label,
                "status": format!("{:?}", frontier.status).to_ascii_lowercase(),
            })
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("{} frontier(s)", frontiers.len())];
    lines.extend(frontiers.iter().map(|frontier| {
        format!(
            "{} {} {}",
            frontier.id,
            format!("{:?}", frontier.status).to_ascii_lowercase(),
            frontier.label,
        )
    }));
    detailed_tool_output(
        &concise,
        &frontiers,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:frontier.list",
    )
}

fn frontier_status_output(projection: &FrontierProjection) -> Result<ToolOutput, FaultRecord> {
    let concise = frontier_projection_summary_value(projection);
    detailed_tool_output(
        &concise,
        projection,
        frontier_projection_text("frontier", projection),
        None,
        FaultStage::Worker,
        "tools/call:frontier.status",
    )
}

fn frontier_created_output(projection: &FrontierProjection) -> Result<ToolOutput, FaultRecord> {
    let concise = frontier_projection_summary_value(projection);
    detailed_tool_output(
        &concise,
        projection,
        frontier_projection_text("created frontier", projection),
        None,
        FaultStage::Worker,
        "tools/call:frontier.init",
    )
}

fn created_node_output(
    action: &str,
    node: &fidget_spinner_core::DagNode,
    operation: &'static str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = node_brief_value(node);
    let mut lines = vec![format!("{action}: {} {}", node.class, node.id)];
    lines.push(format!("title: {}", node.title));
    if let Some(summary) = node.summary.as_ref() {
        lines.push(format!("summary: {summary}"));
    }
    if !node.tags.is_empty() {
        lines.push(format!("tags: {}", format_tags(&node.tags)));
    }
    if let Some(frontier_id) = node.frontier_id {
        lines.push(format!("frontier: {frontier_id}"));
    }
    if !node.diagnostics.items.is_empty() {
        lines.push(format!(
            "diagnostics: {}",
            diagnostic_summary_text(&node.diagnostics)
        ));
    }
    detailed_tool_output(
        &concise,
        node,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn node_list_output(nodes: &[NodeSummary]) -> Result<ToolOutput, FaultRecord> {
    let concise = nodes.iter().map(node_summary_value).collect::<Vec<_>>();
    let mut lines = vec![format!("{} node(s)", nodes.len())];
    lines.extend(nodes.iter().map(render_node_summary_line));
    detailed_tool_output(
        &concise,
        &nodes,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:node.list",
    )
}

fn node_read_output(node: &fidget_spinner_core::DagNode) -> Result<ToolOutput, FaultRecord> {
    let visible_annotations = node
        .annotations
        .iter()
        .filter(|annotation| annotation.visibility == AnnotationVisibility::Visible)
        .map(|annotation| {
            let mut value = Map::new();
            if let Some(label) = annotation.label.as_ref() {
                let _ = value.insert("label".to_owned(), json!(label));
            }
            let _ = value.insert("body".to_owned(), json!(annotation.body));
            Value::Object(value)
        })
        .collect::<Vec<_>>();
    let visible_annotation_count = visible_annotations.len();
    let hidden_annotation_count = node
        .annotations
        .iter()
        .filter(|annotation| annotation.visibility == AnnotationVisibility::HiddenByDefault)
        .count();
    let mut concise = Map::new();
    let _ = concise.insert("id".to_owned(), json!(node.id));
    let _ = concise.insert("class".to_owned(), json!(node.class.as_str()));
    let _ = concise.insert("title".to_owned(), json!(node.title));
    if let Some(summary) = node.summary.as_ref() {
        let _ = concise.insert("summary".to_owned(), json!(summary));
    }
    if let Some(frontier_id) = node.frontier_id {
        let _ = concise.insert("frontier_id".to_owned(), json!(frontier_id));
    }
    if !node.tags.is_empty() {
        let _ = concise.insert(
            "tags".to_owned(),
            json!(
                node.tags
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
        );
    }
    if !node.payload.fields.is_empty() {
        let _ = concise.insert(
            "payload_field_count".to_owned(),
            json!(node.payload.fields.len()),
        );
        let _ = concise.insert(
            "payload_preview".to_owned(),
            payload_preview_value(&node.payload.fields),
        );
    }
    if !node.diagnostics.items.is_empty() {
        let _ = concise.insert(
            "diagnostics".to_owned(),
            diagnostic_summary_value(&node.diagnostics),
        );
    }
    if visible_annotation_count > 0 {
        let _ = concise.insert(
            "visible_annotations".to_owned(),
            Value::Array(visible_annotations),
        );
    }
    if hidden_annotation_count > 0 {
        let _ = concise.insert(
            "hidden_annotation_count".to_owned(),
            json!(hidden_annotation_count),
        );
    }

    let mut lines = vec![format!("{} {} {}", node.class, node.id, node.title)];
    if let Some(summary) = node.summary.as_ref() {
        lines.push(format!("summary: {summary}"));
    }
    if let Some(frontier_id) = node.frontier_id {
        lines.push(format!("frontier: {frontier_id}"));
    }
    if !node.tags.is_empty() {
        lines.push(format!("tags: {}", format_tags(&node.tags)));
    }
    lines.extend(payload_preview_lines(&node.payload.fields));
    if !node.diagnostics.items.is_empty() {
        lines.push(format!(
            "diagnostics: {}",
            diagnostic_summary_text(&node.diagnostics)
        ));
    }
    if visible_annotation_count > 0 {
        lines.push(format!("visible annotations: {}", visible_annotation_count));
        for annotation in node
            .annotations
            .iter()
            .filter(|annotation| annotation.visibility == AnnotationVisibility::Visible)
            .take(4)
        {
            let label = annotation
                .label
                .as_ref()
                .map(|label| format!("{label}: "))
                .unwrap_or_default();
            lines.push(format!("annotation: {label}{}", annotation.body));
        }
        if visible_annotation_count > 4 {
            lines.push(format!(
                "... +{} more visible annotation(s)",
                visible_annotation_count - 4
            ));
        }
    }
    if hidden_annotation_count > 0 {
        lines.push(format!("hidden annotations: {hidden_annotation_count}"));
    }
    detailed_tool_output(
        &Value::Object(concise),
        node,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:node.read",
    )
}

fn experiment_close_output(receipt: &ExperimentReceipt) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "experiment_id": receipt.experiment.id,
        "frontier_id": receipt.experiment.frontier_id,
        "candidate_checkpoint_id": receipt.experiment.candidate_checkpoint_id,
        "verdict": format!("{:?}", receipt.experiment.verdict).to_ascii_lowercase(),
        "run_id": receipt.run.run_id,
        "decision_node_id": receipt.decision_node.id,
        "primary_metric": metric_value(&receipt.experiment.result.primary_metric),
    });
    detailed_tool_output(
        &concise,
        receipt,
        [
            format!(
                "closed experiment {} on frontier {}",
                receipt.experiment.id, receipt.experiment.frontier_id
            ),
            format!("candidate: {}", receipt.experiment.candidate_checkpoint_id),
            format!(
                "verdict: {}",
                format!("{:?}", receipt.experiment.verdict).to_ascii_lowercase()
            ),
            format!(
                "primary metric: {}",
                metric_text(&receipt.experiment.result.primary_metric)
            ),
            format!("run: {}", receipt.run.run_id),
        ]
        .join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:experiment.close",
    )
}

fn project_schema_field_value(field: &fidget_spinner_core::ProjectFieldSpec) -> Value {
    let mut value = Map::new();
    let _ = value.insert("name".to_owned(), json!(field.name));
    if !field.node_classes.is_empty() {
        let _ = value.insert(
            "node_classes".to_owned(),
            json!(
                field
                    .node_classes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
        );
    }
    let _ = value.insert(
        "presence".to_owned(),
        json!(format!("{:?}", field.presence).to_ascii_lowercase()),
    );
    let _ = value.insert(
        "severity".to_owned(),
        json!(format!("{:?}", field.severity).to_ascii_lowercase()),
    );
    let _ = value.insert(
        "role".to_owned(),
        json!(format!("{:?}", field.role).to_ascii_lowercase()),
    );
    let _ = value.insert(
        "inference_policy".to_owned(),
        json!(format!("{:?}", field.inference_policy).to_ascii_lowercase()),
    );
    if let Some(value_type) = field.value_type {
        let _ = value.insert("value_type".to_owned(), json!(value_type.as_str()));
    }
    Value::Object(value)
}

fn frontier_projection_summary_value(projection: &FrontierProjection) -> Value {
    json!({
        "frontier_id": projection.frontier.id,
        "label": projection.frontier.label,
        "status": format!("{:?}", projection.frontier.status).to_ascii_lowercase(),
        "champion_checkpoint_id": projection.champion_checkpoint_id,
        "candidate_checkpoint_ids": projection.candidate_checkpoint_ids,
        "experiment_count": projection.experiment_count,
    })
}

fn frontier_projection_text(prefix: &str, projection: &FrontierProjection) -> String {
    let champion = projection
        .champion_checkpoint_id
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned());
    [
        format!(
            "{prefix} {} {}",
            projection.frontier.id, projection.frontier.label
        ),
        format!(
            "status: {}",
            format!("{:?}", projection.frontier.status).to_ascii_lowercase()
        ),
        format!("champion: {champion}"),
        format!("candidates: {}", projection.candidate_checkpoint_ids.len()),
        format!("experiments: {}", projection.experiment_count),
    ]
    .join("\n")
}

fn node_summary_value(node: &NodeSummary) -> Value {
    let mut value = Map::new();
    let _ = value.insert("id".to_owned(), json!(node.id));
    let _ = value.insert("class".to_owned(), json!(node.class.as_str()));
    let _ = value.insert("title".to_owned(), json!(node.title));
    if let Some(summary) = node.summary.as_ref() {
        let _ = value.insert("summary".to_owned(), json!(summary));
    }
    if let Some(frontier_id) = node.frontier_id {
        let _ = value.insert("frontier_id".to_owned(), json!(frontier_id));
    }
    if !node.tags.is_empty() {
        let _ = value.insert(
            "tags".to_owned(),
            json!(
                node.tags
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
        );
    }
    if node.archived {
        let _ = value.insert("archived".to_owned(), json!(true));
    }
    if node.diagnostic_count > 0 {
        let _ = value.insert("diagnostic_count".to_owned(), json!(node.diagnostic_count));
    }
    if node.hidden_annotation_count > 0 {
        let _ = value.insert(
            "hidden_annotation_count".to_owned(),
            json!(node.hidden_annotation_count),
        );
    }
    Value::Object(value)
}

fn node_brief_value(node: &fidget_spinner_core::DagNode) -> Value {
    let mut value = Map::new();
    let _ = value.insert("id".to_owned(), json!(node.id));
    let _ = value.insert("class".to_owned(), json!(node.class.as_str()));
    let _ = value.insert("title".to_owned(), json!(node.title));
    if let Some(summary) = node.summary.as_ref() {
        let _ = value.insert("summary".to_owned(), json!(summary));
    }
    if let Some(frontier_id) = node.frontier_id {
        let _ = value.insert("frontier_id".to_owned(), json!(frontier_id));
    }
    if !node.tags.is_empty() {
        let _ = value.insert(
            "tags".to_owned(),
            json!(
                node.tags
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
        );
    }
    if !node.diagnostics.items.is_empty() {
        let _ = value.insert(
            "diagnostics".to_owned(),
            diagnostic_summary_value(&node.diagnostics),
        );
    }
    Value::Object(value)
}

fn render_node_summary_line(node: &NodeSummary) -> String {
    let mut line = format!("{} {} {}", node.class, node.id, node.title);
    if let Some(summary) = node.summary.as_ref() {
        line.push_str(format!(" | {summary}").as_str());
    }
    if let Some(frontier_id) = node.frontier_id {
        line.push_str(format!(" | frontier={frontier_id}").as_str());
    }
    if !node.tags.is_empty() {
        line.push_str(format!(" | tags={}", format_tags(&node.tags)).as_str());
    }
    if node.diagnostic_count > 0 {
        line.push_str(format!(" | diag={}", node.diagnostic_count).as_str());
    }
    if node.hidden_annotation_count > 0 {
        line.push_str(format!(" | hidden-ann={}", node.hidden_annotation_count).as_str());
    }
    if node.archived {
        line.push_str(" | archived");
    }
    line
}

fn diagnostic_summary_value(diagnostics: &fidget_spinner_core::NodeDiagnostics) -> Value {
    let tally = diagnostic_tally(diagnostics);
    json!({
        "admission": match diagnostics.admission {
            AdmissionState::Admitted => "admitted",
            AdmissionState::Rejected => "rejected",
        },
        "count": tally.total,
        "error_count": tally.errors,
        "warning_count": tally.warnings,
        "info_count": tally.infos,
    })
}

fn diagnostic_summary_text(diagnostics: &fidget_spinner_core::NodeDiagnostics) -> String {
    let tally = diagnostic_tally(diagnostics);
    let mut parts = vec![format!("{}", tally.total)];
    if tally.errors > 0 {
        parts.push(format!("{} error", tally.errors));
    }
    if tally.warnings > 0 {
        parts.push(format!("{} warning", tally.warnings));
    }
    if tally.infos > 0 {
        parts.push(format!("{} info", tally.infos));
    }
    format!(
        "{} ({})",
        match diagnostics.admission {
            AdmissionState::Admitted => "admitted",
            AdmissionState::Rejected => "rejected",
        },
        parts.join(", ")
    )
}

fn diagnostic_tally(diagnostics: &fidget_spinner_core::NodeDiagnostics) -> DiagnosticTally {
    diagnostics
        .items
        .iter()
        .fold(DiagnosticTally::default(), |mut tally, item| {
            tally.total += 1;
            match item.severity {
                fidget_spinner_core::DiagnosticSeverity::Error => tally.errors += 1,
                fidget_spinner_core::DiagnosticSeverity::Warning => tally.warnings += 1,
                fidget_spinner_core::DiagnosticSeverity::Info => tally.infos += 1,
            }
            tally
        })
}

fn payload_preview_value(fields: &Map<String, Value>) -> Value {
    let mut preview = Map::new();
    for (index, (name, value)) in fields.iter().enumerate() {
        if index == 6 {
            let _ = preview.insert(
                "...".to_owned(),
                json!(format!("+{} more field(s)", fields.len() - index)),
            );
            break;
        }
        let _ = preview.insert(name.clone(), payload_value_preview(value));
    }
    Value::Object(preview)
}

fn payload_preview_lines(fields: &Map<String, Value>) -> Vec<String> {
    if fields.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![format!("payload fields: {}", fields.len())];
    for (index, (name, value)) in fields.iter().enumerate() {
        if index == 6 {
            lines.push(format!("payload: +{} more field(s)", fields.len() - index));
            break;
        }
        lines.push(format!(
            "payload.{}: {}",
            name,
            value_summary(&payload_value_preview(value))
        ));
    }
    lines
}

fn payload_value_preview(value: &Value) -> Value {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => value.clone(),
        Value::String(text) => Value::String(libmcp::collapse_inline_whitespace(text)),
        Value::Array(items) => {
            let preview = items
                .iter()
                .take(3)
                .map(payload_value_preview)
                .collect::<Vec<_>>();
            if items.len() > 3 {
                json!({
                    "items": preview,
                    "truncated": true,
                    "total_count": items.len(),
                })
            } else {
                Value::Array(preview)
            }
        }
        Value::Object(object) => {
            let mut preview = Map::new();
            for (index, (name, nested)) in object.iter().enumerate() {
                if index == 4 {
                    let _ = preview.insert(
                        "...".to_owned(),
                        json!(format!("+{} more field(s)", object.len() - index)),
                    );
                    break;
                }
                let _ = preview.insert(name.clone(), payload_value_preview(nested));
            }
            Value::Object(preview)
        }
    }
}

fn metric_value(metric: &MetricObservation) -> Value {
    json!({
        "key": metric.metric_key,
        "value": metric.value,
        "unit": format!("{:?}", metric.unit).to_ascii_lowercase(),
        "objective": format!("{:?}", metric.objective).to_ascii_lowercase(),
    })
}

fn metric_text(metric: &MetricObservation) -> String {
    format!(
        "{}={} {} ({})",
        metric.metric_key,
        metric.value,
        format!("{:?}", metric.unit).to_ascii_lowercase(),
        format!("{:?}", metric.objective).to_ascii_lowercase(),
    )
}

fn format_tags(tags: &BTreeSet<TagName>) -> String {
    tags.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn schema_label(schema: &ProjectSchema) -> String {
    format!("{}@{}", schema.namespace, schema.version)
}

fn value_summary(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        Value::Array(items) => format!("{} item(s)", items.len()),
        Value::Object(object) => format!("{} field(s)", object.len()),
    }
}

#[derive(Default)]
struct DiagnosticTally {
    total: usize,
    errors: usize,
    warnings: usize,
    infos: usize,
}

fn store_fault<E>(operation: &'static str) -> impl FnOnce(E) -> FaultRecord
where
    E: std::fmt::Display,
{
    move |error| {
        FaultRecord::new(
            classify_fault_kind(&error.to_string()),
            FaultStage::Store,
            operation,
            error.to_string(),
        )
    }
}

fn classify_fault_kind(message: &str) -> FaultKind {
    if message.contains("was not found")
        || message.contains("invalid")
        || message.contains("unknown")
        || message.contains("empty")
        || message.contains("already exists")
        || message.contains("require an explicit tag list")
    {
        FaultKind::InvalidInput
    } else {
        FaultKind::Internal
    }
}

fn tool_annotations(raw: Vec<WireAnnotation>) -> Result<Vec<NodeAnnotation>, StoreError> {
    raw.into_iter()
        .map(|annotation| {
            Ok(NodeAnnotation {
                id: fidget_spinner_core::AnnotationId::fresh(),
                visibility: if annotation.visible {
                    AnnotationVisibility::Visible
                } else {
                    AnnotationVisibility::HiddenByDefault
                },
                label: annotation.label.map(NonEmptyText::new).transpose()?,
                body: NonEmptyText::new(annotation.body)?,
                created_at: time::OffsetDateTime::now_utc(),
            })
        })
        .collect()
}

fn lineage_attachments(parents: Vec<String>) -> Result<Vec<EdgeAttachment>, StoreError> {
    parents
        .into_iter()
        .map(|parent| {
            Ok(EdgeAttachment {
                node_id: crate::parse_node_id(&parent)?,
                kind: fidget_spinner_core::EdgeKind::Lineage,
                direction: EdgeAttachmentDirection::ExistingToNew,
            })
        })
        .collect()
}

fn parse_tag_set(values: Vec<String>) -> Result<BTreeSet<TagName>, StoreError> {
    values
        .into_iter()
        .map(TagName::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(StoreError::from)
}

fn metric_spec_from_wire(raw: WireMetricSpec) -> Result<MetricSpec, StoreError> {
    Ok(MetricSpec {
        metric_key: NonEmptyText::new(raw.key)?,
        unit: parse_metric_unit_name(&raw.unit)?,
        objective: crate::parse_optimization_objective(&raw.objective)?,
    })
}

fn metric_observation_from_wire(
    raw: WireMetricObservation,
) -> Result<MetricObservation, StoreError> {
    Ok(MetricObservation {
        metric_key: NonEmptyText::new(raw.key)?,
        unit: parse_metric_unit_name(&raw.unit)?,
        objective: crate::parse_optimization_objective(&raw.objective)?,
        value: raw.value,
    })
}

fn command_recipe_from_wire(
    raw: WireRunCommand,
    project_root: &Utf8Path,
) -> Result<CommandRecipe, StoreError> {
    let working_directory = raw
        .working_directory
        .map(Utf8PathBuf::from)
        .unwrap_or_else(|| project_root.to_path_buf());
    CommandRecipe::new(
        working_directory,
        crate::to_text_vec(raw.argv)?,
        raw.env.into_iter().collect::<BTreeMap<_, _>>(),
    )
    .map_err(StoreError::from)
}

fn capture_code_snapshot(project_root: &Utf8Path) -> Result<CodeSnapshotRef, StoreError> {
    crate::capture_code_snapshot(project_root)
}

fn parse_node_class_name(raw: &str) -> Result<NodeClass, StoreError> {
    match raw {
        "contract" => Ok(NodeClass::Contract),
        "change" => Ok(NodeClass::Change),
        "run" => Ok(NodeClass::Run),
        "analysis" => Ok(NodeClass::Analysis),
        "decision" => Ok(NodeClass::Decision),
        "research" => Ok(NodeClass::Research),
        "enabling" => Ok(NodeClass::Enabling),
        "note" => Ok(NodeClass::Note),
        other => Err(crate::invalid_input(format!(
            "unknown node class `{other}`"
        ))),
    }
}

fn parse_metric_unit_name(raw: &str) -> Result<MetricUnit, StoreError> {
    crate::parse_metric_unit(raw)
}

fn parse_backend_name(raw: &str) -> Result<ExecutionBackend, StoreError> {
    match raw {
        "local_process" => Ok(ExecutionBackend::LocalProcess),
        "worktree_process" => Ok(ExecutionBackend::WorktreeProcess),
        "ssh_process" => Ok(ExecutionBackend::SshProcess),
        other => Err(crate::invalid_input(format!("unknown backend `{other}`"))),
    }
}

fn parse_verdict_name(raw: &str) -> Result<FrontierVerdict, StoreError> {
    match raw {
        "promote_to_champion" => Ok(FrontierVerdict::PromoteToChampion),
        "keep_on_frontier" => Ok(FrontierVerdict::KeepOnFrontier),
        "revert_to_champion" => Ok(FrontierVerdict::RevertToChampion),
        "archive_dead_end" => Ok(FrontierVerdict::ArchiveDeadEnd),
        "needs_more_evidence" => Ok(FrontierVerdict::NeedsMoreEvidence),
        other => Err(crate::invalid_input(format!("unknown verdict `{other}`"))),
    }
}

#[derive(Debug, Deserialize)]
struct FrontierStatusToolArgs {
    frontier_id: String,
}

#[derive(Debug, Deserialize)]
struct TagAddToolArgs {
    name: String,
    description: String,
}

#[derive(Debug, Deserialize)]
struct FrontierInitToolArgs {
    label: String,
    objective: String,
    contract_title: String,
    contract_summary: Option<String>,
    benchmark_suites: Vec<String>,
    promotion_criteria: Vec<String>,
    primary_metric: WireMetricSpec,
    #[serde(default)]
    supporting_metrics: Vec<WireMetricSpec>,
    seed_summary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NodeCreateToolArgs {
    class: String,
    frontier_id: Option<String>,
    title: String,
    summary: Option<String>,
    tags: Option<Vec<String>>,
    #[serde(default)]
    payload: Option<Map<String, Value>>,
    #[serde(default)]
    annotations: Vec<WireAnnotation>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ChangeRecordToolArgs {
    frontier_id: String,
    title: String,
    summary: Option<String>,
    body: String,
    hypothesis: Option<String>,
    base_checkpoint_id: Option<String>,
    benchmark_suite: Option<String>,
    #[serde(default)]
    annotations: Vec<WireAnnotation>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct NodeListToolArgs {
    frontier_id: Option<String>,
    class: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    include_archived: bool,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct NodeReadToolArgs {
    node_id: String,
}

#[derive(Debug, Deserialize)]
struct NodeAnnotateToolArgs {
    node_id: String,
    body: String,
    label: Option<String>,
    #[serde(default)]
    visible: bool,
}

#[derive(Debug, Deserialize)]
struct NodeArchiveToolArgs {
    node_id: String,
}

#[derive(Debug, Deserialize)]
struct QuickNoteToolArgs {
    frontier_id: Option<String>,
    title: String,
    body: String,
    tags: Vec<String>,
    #[serde(default)]
    annotations: Vec<WireAnnotation>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ResearchRecordToolArgs {
    frontier_id: Option<String>,
    title: String,
    summary: Option<String>,
    body: String,
    #[serde(default)]
    annotations: Vec<WireAnnotation>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ExperimentCloseToolArgs {
    frontier_id: String,
    base_checkpoint_id: String,
    change_node_id: String,
    candidate_summary: String,
    run: WireRun,
    primary_metric: WireMetricObservation,
    #[serde(default)]
    supporting_metrics: Vec<WireMetricObservation>,
    note: WireFrontierNote,
    verdict: String,
    decision_title: String,
    decision_rationale: String,
    analysis_node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WireAnnotation {
    body: String,
    label: Option<String>,
    #[serde(default)]
    visible: bool,
}

#[derive(Debug, Deserialize)]
struct WireMetricSpec {
    key: String,
    unit: String,
    objective: String,
}

#[derive(Debug, Deserialize)]
struct WireMetricObservation {
    key: String,
    unit: String,
    objective: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct WireRun {
    title: String,
    summary: Option<String>,
    backend: String,
    benchmark_suite: String,
    command: WireRunCommand,
}

#[derive(Debug, Deserialize)]
struct WireRunCommand {
    working_directory: Option<String>,
    argv: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct WireFrontierNote {
    summary: String,
    #[serde(default)]
    next_hypotheses: Vec<String>,
}
