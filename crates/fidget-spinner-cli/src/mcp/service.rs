use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    AdmissionState, AnnotationVisibility, CodeSnapshotRef, CommandRecipe, DiagnosticSeverity,
    ExecutionBackend, FieldPresence, FieldRole, FieldValueType, FrontierContract, FrontierNote,
    FrontierProjection, FrontierRecord, FrontierVerdict, InferencePolicy, MetricSpec, MetricUnit,
    MetricValue, NodeAnnotation, NodeClass, NodePayload, NonEmptyText, ProjectFieldSpec,
    ProjectSchema, RunDimensionValue, TagName, TagRecord,
};
use fidget_spinner_store_sqlite::{
    CloseExperimentRequest, CreateFrontierRequest, CreateNodeRequest, DefineMetricRequest,
    DefineRunDimensionRequest, EdgeAttachment, EdgeAttachmentDirection, ExperimentAnalysisDraft,
    ExperimentReceipt, ListNodesQuery, MetricBestQuery, MetricFieldSource, MetricKeyQuery,
    MetricKeySummary, MetricRankOrder, NodeSummary, OpenExperimentRequest, OpenExperimentSummary,
    ProjectStore, RemoveSchemaFieldRequest, StoreError, UpsertSchemaFieldRequest,
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
            "schema.field.upsert" => {
                let args = deserialize::<SchemaFieldUpsertToolArgs>(arguments)?;
                let field = self
                    .store
                    .upsert_schema_field(UpsertSchemaFieldRequest {
                        name: NonEmptyText::new(args.name)
                            .map_err(store_fault("tools/call:schema.field.upsert"))?,
                        node_classes: args
                            .node_classes
                            .unwrap_or_default()
                            .into_iter()
                            .map(|class| {
                                parse_node_class_name(&class)
                                    .map_err(store_fault("tools/call:schema.field.upsert"))
                            })
                            .collect::<Result<_, _>>()?,
                        presence: parse_field_presence_name(&args.presence)
                            .map_err(store_fault("tools/call:schema.field.upsert"))?,
                        severity: parse_diagnostic_severity_name(&args.severity)
                            .map_err(store_fault("tools/call:schema.field.upsert"))?,
                        role: parse_field_role_name(&args.role)
                            .map_err(store_fault("tools/call:schema.field.upsert"))?,
                        inference_policy: parse_inference_policy_name(&args.inference_policy)
                            .map_err(store_fault("tools/call:schema.field.upsert"))?,
                        value_type: args
                            .value_type
                            .as_deref()
                            .map(parse_field_value_type_name)
                            .transpose()
                            .map_err(store_fault("tools/call:schema.field.upsert"))?,
                    })
                    .map_err(store_fault("tools/call:schema.field.upsert"))?;
                tool_success(
                    schema_field_upsert_output(self.store.schema(), &field)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:schema.field.upsert",
                )
            }
            "schema.field.remove" => {
                let args = deserialize::<SchemaFieldRemoveToolArgs>(arguments)?;
                let removed_count = self
                    .store
                    .remove_schema_field(RemoveSchemaFieldRequest {
                        name: NonEmptyText::new(args.name)
                            .map_err(store_fault("tools/call:schema.field.remove"))?,
                        node_classes: args
                            .node_classes
                            .map(|node_classes| {
                                node_classes
                                    .into_iter()
                                    .map(|class| {
                                        parse_node_class_name(&class)
                                            .map_err(store_fault("tools/call:schema.field.remove"))
                                    })
                                    .collect::<Result<_, _>>()
                            })
                            .transpose()?,
                    })
                    .map_err(store_fault("tools/call:schema.field.remove"))?;
                tool_success(
                    schema_field_remove_output(self.store.schema(), removed_count)?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:schema.field.remove",
                )
            }
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
            "hypothesis.record" => {
                let args = deserialize::<HypothesisRecordToolArgs>(arguments)?;
                let node = self
                    .store
                    .add_node(CreateNodeRequest {
                        class: NodeClass::Hypothesis,
                        frontier_id: Some(
                            crate::parse_frontier_id(&args.frontier_id)
                                .map_err(store_fault("tools/call:hypothesis.record"))?,
                        ),
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:hypothesis.record"))?,
                        summary: Some(
                            NonEmptyText::new(args.summary)
                                .map_err(store_fault("tools/call:hypothesis.record"))?,
                        ),
                        tags: None,
                        payload: NodePayload::with_schema(
                            self.store.schema().schema_ref(),
                            crate::json_object(json!({ "body": args.body }))
                                .map_err(store_fault("tools/call:hypothesis.record"))?,
                        ),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:hypothesis.record"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:hypothesis.record"))?,
                    })
                    .map_err(store_fault("tools/call:hypothesis.record"))?;
                tool_success(
                    created_node_output(
                        "recorded hypothesis",
                        &node,
                        "tools/call:hypothesis.record",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:hypothesis.record",
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
                        summary: Some(
                            NonEmptyText::new(args.summary)
                                .map_err(store_fault("tools/call:note.quick"))?,
                        ),
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
            "source.record" => {
                let args = deserialize::<SourceRecordToolArgs>(arguments)?;
                let node = self
                    .store
                    .add_node(CreateNodeRequest {
                        class: NodeClass::Source,
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:source.record"))?,
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:source.record"))?,
                        summary: Some(
                            NonEmptyText::new(args.summary)
                                .map_err(store_fault("tools/call:source.record"))?,
                        ),
                        tags: args
                            .tags
                            .map(parse_tag_set)
                            .transpose()
                            .map_err(store_fault("tools/call:source.record"))?,
                        payload: NodePayload::with_schema(
                            self.store.schema().schema_ref(),
                            crate::json_object(json!({ "body": args.body }))
                                .map_err(store_fault("tools/call:source.record"))?,
                        ),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:source.record"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:source.record"))?,
                    })
                    .map_err(store_fault("tools/call:source.record"))?;
                tool_success(
                    created_node_output("recorded source", &node, "tools/call:source.record")?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:source.record",
                )
            }
            "metric.define" => {
                let args = deserialize::<MetricDefineToolArgs>(arguments)?;
                let metric = self
                    .store
                    .define_metric(DefineMetricRequest {
                        key: NonEmptyText::new(args.key)
                            .map_err(store_fault("tools/call:metric.define"))?,
                        unit: parse_metric_unit_name(&args.unit)
                            .map_err(store_fault("tools/call:metric.define"))?,
                        objective: crate::parse_optimization_objective(&args.objective)
                            .map_err(store_fault("tools/call:metric.define"))?,
                        description: args
                            .description
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:metric.define"))?,
                    })
                    .map_err(store_fault("tools/call:metric.define"))?;
                tool_success(
                    json_created_output(
                        "registered metric",
                        json!({
                            "key": metric.key,
                            "unit": metric_unit_name(metric.unit),
                            "objective": metric_objective_name(metric.objective),
                            "description": metric.description,
                        }),
                        "tools/call:metric.define",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:metric.define",
                )
            }
            "run.dimension.define" => {
                let args = deserialize::<RunDimensionDefineToolArgs>(arguments)?;
                let dimension = self
                    .store
                    .define_run_dimension(DefineRunDimensionRequest {
                        key: NonEmptyText::new(args.key)
                            .map_err(store_fault("tools/call:run.dimension.define"))?,
                        value_type: parse_field_value_type_name(&args.value_type)
                            .map_err(store_fault("tools/call:run.dimension.define"))?,
                        description: args
                            .description
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:run.dimension.define"))?,
                    })
                    .map_err(store_fault("tools/call:run.dimension.define"))?;
                tool_success(
                    json_created_output(
                        "registered run dimension",
                        json!({
                            "key": dimension.key,
                            "value_type": dimension.value_type.as_str(),
                            "description": dimension.description,
                        }),
                        "tools/call:run.dimension.define",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:run.dimension.define",
                )
            }
            "run.dimension.list" => {
                let items = self
                    .store
                    .list_run_dimensions()
                    .map_err(store_fault("tools/call:run.dimension.list"))?;
                tool_success(
                    run_dimension_list_output(items.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:run.dimension.list",
                )
            }
            "metric.keys" => {
                let args = deserialize::<MetricKeysToolArgs>(arguments)?;
                let keys = self
                    .store
                    .list_metric_keys_filtered(MetricKeyQuery {
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:metric.keys"))?,
                        source: args
                            .source
                            .as_deref()
                            .map(parse_metric_source_name)
                            .transpose()
                            .map_err(store_fault("tools/call:metric.keys"))?,
                        dimensions: coerce_tool_dimensions(
                            &self.store,
                            args.dimensions.unwrap_or_default(),
                            "tools/call:metric.keys",
                        )?,
                    })
                    .map_err(store_fault("tools/call:metric.keys"))?;
                tool_success(
                    metric_keys_output(keys.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:metric.keys",
                )
            }
            "metric.best" => {
                let args = deserialize::<MetricBestToolArgs>(arguments)?;
                let items = self
                    .store
                    .best_metrics(MetricBestQuery {
                        key: NonEmptyText::new(args.key)
                            .map_err(store_fault("tools/call:metric.best"))?,
                        frontier_id: args
                            .frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:metric.best"))?,
                        source: args
                            .source
                            .as_deref()
                            .map(parse_metric_source_name)
                            .transpose()
                            .map_err(store_fault("tools/call:metric.best"))?,
                        dimensions: coerce_tool_dimensions(
                            &self.store,
                            args.dimensions.unwrap_or_default(),
                            "tools/call:metric.best",
                        )?,
                        order: args
                            .order
                            .as_deref()
                            .map(parse_metric_order_name)
                            .transpose()
                            .map_err(store_fault("tools/call:metric.best"))?,
                        limit: args.limit.unwrap_or(10),
                    })
                    .map_err(store_fault("tools/call:metric.best"))?;
                tool_success(
                    metric_best_output(items.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:metric.best",
                )
            }
            "metric.migrate" => {
                let report = self
                    .store
                    .migrate_metric_plane()
                    .map_err(store_fault("tools/call:metric.migrate"))?;
                tool_success(
                    json_created_output(
                        "normalized legacy metric plane",
                        json!(report),
                        "tools/call:metric.migrate",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:metric.migrate",
                )
            }
            "experiment.open" => {
                let args = deserialize::<ExperimentOpenToolArgs>(arguments)?;
                let item = self
                    .store
                    .open_experiment(OpenExperimentRequest {
                        frontier_id: crate::parse_frontier_id(&args.frontier_id)
                            .map_err(store_fault("tools/call:experiment.open"))?,
                        base_checkpoint_id: crate::parse_checkpoint_id(&args.base_checkpoint_id)
                            .map_err(store_fault("tools/call:experiment.open"))?,
                        hypothesis_node_id: crate::parse_node_id(&args.hypothesis_node_id)
                            .map_err(store_fault("tools/call:experiment.open"))?,
                        title: NonEmptyText::new(args.title)
                            .map_err(store_fault("tools/call:experiment.open"))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault("tools/call:experiment.open"))?,
                    })
                    .map_err(store_fault("tools/call:experiment.open"))?;
                tool_success(
                    experiment_open_output(
                        &item,
                        "tools/call:experiment.open",
                        "opened experiment",
                    )?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:experiment.open",
                )
            }
            "experiment.list" => {
                let args = deserialize::<ExperimentListToolArgs>(arguments)?;
                let items = self
                    .store
                    .list_open_experiments(
                        args.frontier_id
                            .as_deref()
                            .map(crate::parse_frontier_id)
                            .transpose()
                            .map_err(store_fault("tools/call:experiment.list"))?,
                    )
                    .map_err(store_fault("tools/call:experiment.list"))?;
                tool_success(
                    experiment_list_output(items.as_slice())?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:experiment.list",
                )
            }
            "experiment.read" => {
                let args = deserialize::<ExperimentReadToolArgs>(arguments)?;
                let item = self
                    .store
                    .read_open_experiment(
                        crate::parse_experiment_id(&args.experiment_id)
                            .map_err(store_fault("tools/call:experiment.read"))?,
                    )
                    .map_err(store_fault("tools/call:experiment.read"))?;
                tool_success(
                    experiment_open_output(&item, "tools/call:experiment.read", "open experiment")?,
                    presentation,
                    FaultStage::Worker,
                    "tools/call:experiment.read",
                )
            }
            "experiment.close" => {
                let args = deserialize::<ExperimentCloseToolArgs>(arguments)?;
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
                        experiment_id: crate::parse_experiment_id(&args.experiment_id)
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
                        dimensions: coerce_tool_dimensions(
                            &self.store,
                            args.run.dimensions,
                            "tools/call:experiment.close",
                        )?,
                        command: command_recipe_from_wire(
                            args.run.command,
                            self.store.project_root(),
                        )
                        .map_err(store_fault("tools/call:experiment.close"))?,
                        code_snapshot: Some(
                            capture_code_snapshot(self.store.project_root())
                                .map_err(store_fault("tools/call:experiment.close"))?,
                        ),
                        primary_metric: metric_value_from_wire(args.primary_metric)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        supporting_metrics: args
                            .supporting_metrics
                            .into_iter()
                            .map(metric_value_from_wire)
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
                        analysis: args
                            .analysis
                            .map(experiment_analysis_from_wire)
                            .transpose()
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        decision_title: NonEmptyText::new(args.decision_title)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                        decision_rationale: NonEmptyText::new(args.decision_rationale)
                            .map_err(store_fault("tools/call:experiment.close"))?,
                    })
                    .map_err(store_fault("tools/call:experiment.close"))?;
                tool_success(
                    experiment_close_output(&self.store, &receipt)?,
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
            field.presence.as_str(),
            field.role.as_str(),
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

fn schema_field_upsert_output(
    schema: &ProjectSchema,
    field: &ProjectFieldSpec,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "schema": schema.schema_ref(),
        "field": project_schema_field_value(field),
    });
    detailed_tool_output(
        &concise,
        &concise,
        format!(
            "upserted schema field {}\nschema: {}\nclasses: {}\npresence: {}\nseverity: {}\nrole: {}\ninference: {}{}",
            field.name,
            schema_label(schema),
            render_schema_node_classes(&field.node_classes),
            field.presence.as_str(),
            field.severity.as_str(),
            field.role.as_str(),
            field.inference_policy.as_str(),
            field
                .value_type
                .map(|value_type| format!("\nvalue_type: {}", value_type.as_str()))
                .unwrap_or_default(),
        ),
        None,
        FaultStage::Worker,
        "tools/call:schema.field.upsert",
    )
}

fn schema_field_remove_output(
    schema: &ProjectSchema,
    removed_count: u64,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "schema": schema.schema_ref(),
        "removed_count": removed_count,
    });
    detailed_tool_output(
        &concise,
        &concise,
        format!(
            "removed {} schema field definition(s)\nschema: {}",
            removed_count,
            schema_label(schema),
        ),
        None,
        FaultStage::Worker,
        "tools/call:schema.field.remove",
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
        let filtered_fields =
            filtered_payload_fields(node.class, &node.payload.fields).collect::<Vec<_>>();
        if !filtered_fields.is_empty() {
            let _ = concise.insert(
                "payload_field_count".to_owned(),
                json!(filtered_fields.len()),
            );
            if is_prose_node(node.class) {
                let _ = concise.insert(
                    "payload_fields".to_owned(),
                    json!(
                        filtered_fields
                            .iter()
                            .take(6)
                            .map(|(name, _)| (*name).clone())
                            .collect::<Vec<_>>()
                    ),
                );
            } else {
                let payload_preview = payload_preview_value(node.class, &node.payload.fields);
                if let Value::Object(object) = &payload_preview
                    && !object.is_empty()
                {
                    let _ = concise.insert("payload_preview".to_owned(), payload_preview);
                }
            }
        }
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
    lines.extend(payload_preview_lines(node.class, &node.payload.fields));
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

fn experiment_close_output(
    store: &ProjectStore,
    receipt: &ExperimentReceipt,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "experiment_id": receipt.experiment.id,
        "frontier_id": receipt.experiment.frontier_id,
        "candidate_checkpoint_id": receipt.experiment.candidate_checkpoint_id,
        "verdict": format!("{:?}", receipt.experiment.verdict).to_ascii_lowercase(),
        "run_id": receipt.run.run_id,
        "hypothesis_node_id": receipt.experiment.hypothesis_node_id,
        "decision_node_id": receipt.decision_node.id,
        "dimensions": run_dimensions_value(&receipt.experiment.result.dimensions),
        "primary_metric": metric_value(store, &receipt.experiment.result.primary_metric)?,
    });
    detailed_tool_output(
        &concise,
        receipt,
        [
            format!(
                "closed experiment {} on frontier {}",
                receipt.experiment.id, receipt.experiment.frontier_id
            ),
            format!("hypothesis: {}", receipt.experiment.hypothesis_node_id),
            format!("candidate: {}", receipt.experiment.candidate_checkpoint_id),
            format!(
                "verdict: {}",
                format!("{:?}", receipt.experiment.verdict).to_ascii_lowercase()
            ),
            format!(
                "primary metric: {}",
                metric_text(store, &receipt.experiment.result.primary_metric)?
            ),
            format!(
                "dimensions: {}",
                render_dimension_kv(&receipt.experiment.result.dimensions)
            ),
            format!("run: {}", receipt.run.run_id),
        ]
        .join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:experiment.close",
    )
}

fn experiment_open_output(
    item: &OpenExperimentSummary,
    operation: &'static str,
    action: &'static str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "experiment_id": item.id,
        "frontier_id": item.frontier_id,
        "base_checkpoint_id": item.base_checkpoint_id,
        "hypothesis_node_id": item.hypothesis_node_id,
        "title": item.title,
        "summary": item.summary,
    });
    detailed_tool_output(
        &concise,
        item,
        [
            format!("{action} {}", item.id),
            format!("frontier: {}", item.frontier_id),
            format!("hypothesis: {}", item.hypothesis_node_id),
            format!("base checkpoint: {}", item.base_checkpoint_id),
            format!("title: {}", item.title),
            item.summary
                .as_ref()
                .map(|summary| format!("summary: {summary}"))
                .unwrap_or_else(|| "summary: <none>".to_owned()),
        ]
        .join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn experiment_list_output(items: &[OpenExperimentSummary]) -> Result<ToolOutput, FaultRecord> {
    let concise = items
        .iter()
        .map(|item| {
            json!({
                "experiment_id": item.id,
                "frontier_id": item.frontier_id,
                "base_checkpoint_id": item.base_checkpoint_id,
                "hypothesis_node_id": item.hypothesis_node_id,
                "title": item.title,
                "summary": item.summary,
            })
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("{} open experiment(s)", items.len())];
    lines.extend(items.iter().map(|item| {
        format!(
            "{} {} | hypothesis={} | checkpoint={}",
            item.id, item.title, item.hypothesis_node_id, item.base_checkpoint_id,
        )
    }));
    detailed_tool_output(
        &concise,
        &items,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:experiment.list",
    )
}

fn metric_keys_output(keys: &[MetricKeySummary]) -> Result<ToolOutput, FaultRecord> {
    let concise = keys
        .iter()
        .map(|key| {
            json!({
                "key": key.key,
                "source": key.source.as_str(),
                "experiment_count": key.experiment_count,
                "unit": key.unit.map(metric_unit_name),
                "objective": key.objective.map(metric_objective_name),
                "description": key.description,
                "requires_order": key.requires_order,
            })
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("{} metric key(s)", keys.len())];
    lines.extend(keys.iter().map(|key| {
        let mut line = format!(
            "{} [{}] experiments={}",
            key.key,
            key.source.as_str(),
            key.experiment_count
        );
        if let Some(unit) = key.unit {
            line.push_str(format!(" unit={}", metric_unit_name(unit)).as_str());
        }
        if let Some(objective) = key.objective {
            line.push_str(format!(" objective={}", metric_objective_name(objective)).as_str());
        }
        if let Some(description) = key.description.as_ref() {
            line.push_str(format!(" | {description}").as_str());
        }
        if key.requires_order {
            line.push_str(" order=required");
        }
        line
    }));
    detailed_tool_output(
        &concise,
        &keys,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:metric.keys",
    )
}

fn metric_best_output(
    items: &[fidget_spinner_store_sqlite::MetricBestEntry],
) -> Result<ToolOutput, FaultRecord> {
    let concise = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            json!({
                "rank": index + 1,
                "key": item.key,
                "source": item.source.as_str(),
                "value": item.value,
                "order": item.order.as_str(),
                "experiment_id": item.experiment_id,
                "frontier_id": item.frontier_id,
                "hypothesis_node_id": item.hypothesis_node_id,
                "hypothesis_title": item.hypothesis_title,
                "verdict": metric_verdict_name(item.verdict),
                "candidate_checkpoint_id": item.candidate_checkpoint_id,
                "candidate_commit_hash": item.candidate_commit_hash,
                "run_id": item.run_id,
                "unit": item.unit.map(metric_unit_name),
                "objective": item.objective.map(metric_objective_name),
                "dimensions": run_dimensions_value(&item.dimensions),
            })
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("{} ranked experiment(s)", items.len())];
    lines.extend(items.iter().enumerate().map(|(index, item)| {
        format!(
            "{}. {}={} [{}] {} | verdict={} | commit={} | checkpoint={}",
            index + 1,
            item.key,
            item.value,
            item.source.as_str(),
            item.hypothesis_title,
            metric_verdict_name(item.verdict),
            item.candidate_commit_hash,
            item.candidate_checkpoint_id,
        )
    }));
    lines.extend(
        items
            .iter()
            .map(|item| format!("   dims: {}", render_dimension_kv(&item.dimensions))),
    );
    detailed_tool_output(
        &concise,
        &items,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:metric.best",
    )
}

fn run_dimension_list_output(
    items: &[fidget_spinner_store_sqlite::RunDimensionSummary],
) -> Result<ToolOutput, FaultRecord> {
    let concise = items
        .iter()
        .map(|item| {
            json!({
                "key": item.key,
                "value_type": item.value_type.as_str(),
                "description": item.description,
                "observed_run_count": item.observed_run_count,
                "distinct_value_count": item.distinct_value_count,
                "sample_values": item.sample_values,
            })
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("{} run dimension(s)", items.len())];
    lines.extend(items.iter().map(|item| {
        let mut line = format!(
            "{} [{}] runs={} distinct={}",
            item.key,
            item.value_type.as_str(),
            item.observed_run_count,
            item.distinct_value_count
        );
        if let Some(description) = item.description.as_ref() {
            line.push_str(format!(" | {description}").as_str());
        }
        if !item.sample_values.is_empty() {
            line.push_str(
                format!(
                    " | samples={}",
                    item.sample_values
                        .iter()
                        .map(value_summary)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .as_str(),
            );
        }
        line
    }));
    detailed_tool_output(
        &concise,
        &items,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        "tools/call:run.dimension.list",
    )
}

fn json_created_output(
    headline: &str,
    structured: Value,
    operation: &'static str,
) -> Result<ToolOutput, FaultRecord> {
    detailed_tool_output(
        &structured,
        &structured,
        format!(
            "{headline}\n{}",
            crate::to_pretty_json(&structured).map_err(store_fault(operation))?
        ),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn project_schema_field_value(field: &ProjectFieldSpec) -> Value {
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
    let _ = value.insert("presence".to_owned(), json!(field.presence.as_str()));
    let _ = value.insert("severity".to_owned(), json!(field.severity.as_str()));
    let _ = value.insert("role".to_owned(), json!(field.role.as_str()));
    let _ = value.insert(
        "inference_policy".to_owned(),
        json!(field.inference_policy.as_str()),
    );
    if let Some(value_type) = field.value_type {
        let _ = value.insert("value_type".to_owned(), json!(value_type.as_str()));
    }
    Value::Object(value)
}

fn render_schema_node_classes(node_classes: &BTreeSet<NodeClass>) -> String {
    if node_classes.is_empty() {
        return "any".to_owned();
    }
    node_classes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
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
                DiagnosticSeverity::Error => tally.errors += 1,
                DiagnosticSeverity::Warning => tally.warnings += 1,
                DiagnosticSeverity::Info => tally.infos += 1,
            }
            tally
        })
}

fn payload_preview_value(class: NodeClass, fields: &Map<String, Value>) -> Value {
    let mut preview = Map::new();
    for (index, (name, value)) in filtered_payload_fields(class, fields).enumerate() {
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

fn payload_preview_lines(class: NodeClass, fields: &Map<String, Value>) -> Vec<String> {
    let filtered = filtered_payload_fields(class, fields).collect::<Vec<_>>();
    if filtered.is_empty() {
        return Vec::new();
    }
    if is_prose_node(class) {
        let preview_names = filtered
            .iter()
            .take(6)
            .map(|(name, _)| (*name).clone())
            .collect::<Vec<_>>();
        let mut lines = vec![format!("payload fields: {}", preview_names.join(", "))];
        if filtered.len() > preview_names.len() {
            lines.push(format!(
                "payload fields: +{} more field(s)",
                filtered.len() - preview_names.len()
            ));
        }
        return lines;
    }
    let mut lines = vec![format!("payload fields: {}", filtered.len())];
    for (index, (name, value)) in filtered.iter().enumerate() {
        if index == 6 {
            lines.push(format!(
                "payload: +{} more field(s)",
                filtered.len() - index
            ));
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

fn filtered_payload_fields(
    class: NodeClass,
    fields: &Map<String, Value>,
) -> impl Iterator<Item = (&String, &Value)> + '_ {
    fields.iter().filter(move |(name, _)| {
        !matches!(class, NodeClass::Note | NodeClass::Source) || name.as_str() != "body"
    })
}

fn payload_value_preview(value: &Value) -> Value {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => value.clone(),
        Value::String(text) => Value::String(truncated_inline_preview(text, 96)),
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

fn is_prose_node(class: NodeClass) -> bool {
    matches!(class, NodeClass::Note | NodeClass::Source)
}

fn truncated_inline_preview(text: &str, limit: usize) -> String {
    let collapsed = libmcp::collapse_inline_whitespace(text);
    let truncated = libmcp::render::truncate_chars(&collapsed, Some(limit));
    if truncated.truncated {
        format!("{}...", truncated.text)
    } else {
        truncated.text
    }
}

fn metric_value(store: &ProjectStore, metric: &MetricValue) -> Result<Value, FaultRecord> {
    let definition = metric_definition(store, &metric.key)?;
    Ok(json!({
        "key": metric.key,
        "value": metric.value,
        "unit": metric_unit_name(definition.unit),
        "objective": metric_objective_name(definition.objective),
    }))
}

fn metric_text(store: &ProjectStore, metric: &MetricValue) -> Result<String, FaultRecord> {
    let definition = metric_definition(store, &metric.key)?;
    Ok(format!(
        "{}={} {} ({})",
        metric.key,
        metric.value,
        metric_unit_name(definition.unit),
        metric_objective_name(definition.objective),
    ))
}

fn metric_unit_name(unit: MetricUnit) -> &'static str {
    match unit {
        MetricUnit::Seconds => "seconds",
        MetricUnit::Bytes => "bytes",
        MetricUnit::Count => "count",
        MetricUnit::Ratio => "ratio",
        MetricUnit::Custom => "custom",
    }
}

fn metric_objective_name(objective: fidget_spinner_core::OptimizationObjective) -> &'static str {
    match objective {
        fidget_spinner_core::OptimizationObjective::Minimize => "minimize",
        fidget_spinner_core::OptimizationObjective::Maximize => "maximize",
        fidget_spinner_core::OptimizationObjective::Target => "target",
    }
}

fn metric_verdict_name(verdict: FrontierVerdict) -> &'static str {
    match verdict {
        FrontierVerdict::PromoteToChampion => "promote_to_champion",
        FrontierVerdict::KeepOnFrontier => "keep_on_frontier",
        FrontierVerdict::RevertToChampion => "revert_to_champion",
        FrontierVerdict::ArchiveDeadEnd => "archive_dead_end",
        FrontierVerdict::NeedsMoreEvidence => "needs_more_evidence",
    }
}

fn run_dimensions_value(dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>) -> Value {
    Value::Object(
        dimensions
            .iter()
            .map(|(key, value)| (key.to_string(), value.as_json()))
            .collect::<Map<String, Value>>(),
    )
}

fn render_dimension_kv(dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>) -> String {
    if dimensions.is_empty() {
        return "none".to_owned();
    }
    dimensions
        .iter()
        .map(|(key, value)| format!("{key}={}", value_summary(&value.as_json())))
        .collect::<Vec<_>>()
        .join(", ")
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
        || message.contains("requires a non-empty summary")
        || message.contains("requires a non-empty string payload field `body`")
        || message.contains("requires an explicit order")
        || message.contains("is ambiguous across sources")
        || message.contains("has conflicting semantics")
        || message.contains("conflicts with existing definition")
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

fn metric_value_from_wire(raw: WireMetricValue) -> Result<MetricValue, StoreError> {
    Ok(MetricValue {
        key: NonEmptyText::new(raw.key)?,
        value: raw.value,
    })
}

fn experiment_analysis_from_wire(raw: WireAnalysis) -> Result<ExperimentAnalysisDraft, StoreError> {
    Ok(ExperimentAnalysisDraft {
        title: NonEmptyText::new(raw.title)?,
        summary: NonEmptyText::new(raw.summary)?,
        body: NonEmptyText::new(raw.body)?,
    })
}

fn metric_definition(store: &ProjectStore, key: &NonEmptyText) -> Result<MetricSpec, FaultRecord> {
    store
        .list_metric_definitions()
        .map_err(store_fault("tools/call:experiment.close"))?
        .into_iter()
        .find(|definition| definition.key == *key)
        .map(|definition| MetricSpec {
            metric_key: definition.key,
            unit: definition.unit,
            objective: definition.objective,
        })
        .ok_or_else(|| {
            FaultRecord::new(
                FaultKind::InvalidInput,
                FaultStage::Store,
                "tools/call:experiment.close",
                format!("metric `{key}` is not registered"),
            )
        })
}

fn coerce_tool_dimensions(
    store: &ProjectStore,
    raw_dimensions: BTreeMap<String, Value>,
    operation: &'static str,
) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, FaultRecord> {
    store
        .coerce_run_dimensions(raw_dimensions)
        .map_err(store_fault(operation))
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
        "hypothesis" => Ok(NodeClass::Hypothesis),
        "run" => Ok(NodeClass::Run),
        "analysis" => Ok(NodeClass::Analysis),
        "decision" => Ok(NodeClass::Decision),
        "source" => Ok(NodeClass::Source),
        "note" => Ok(NodeClass::Note),
        other => Err(crate::invalid_input(format!(
            "unknown node class `{other}`"
        ))),
    }
}

fn parse_metric_unit_name(raw: &str) -> Result<MetricUnit, StoreError> {
    crate::parse_metric_unit(raw)
}

fn parse_metric_source_name(raw: &str) -> Result<MetricFieldSource, StoreError> {
    match raw {
        "run_metric" => Ok(MetricFieldSource::RunMetric),
        "hypothesis_payload" => Ok(MetricFieldSource::HypothesisPayload),
        "run_payload" => Ok(MetricFieldSource::RunPayload),
        "analysis_payload" => Ok(MetricFieldSource::AnalysisPayload),
        "decision_payload" => Ok(MetricFieldSource::DecisionPayload),
        other => Err(StoreError::Json(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown metric source `{other}`"),
            ),
        ))),
    }
}

fn parse_metric_order_name(raw: &str) -> Result<MetricRankOrder, StoreError> {
    match raw {
        "asc" => Ok(MetricRankOrder::Asc),
        "desc" => Ok(MetricRankOrder::Desc),
        other => Err(StoreError::Json(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown metric order `{other}`"),
            ),
        ))),
    }
}

fn parse_field_value_type_name(raw: &str) -> Result<FieldValueType, StoreError> {
    match raw {
        "string" => Ok(FieldValueType::String),
        "numeric" => Ok(FieldValueType::Numeric),
        "boolean" => Ok(FieldValueType::Boolean),
        "timestamp" => Ok(FieldValueType::Timestamp),
        other => Err(crate::invalid_input(format!(
            "unknown field value type `{other}`"
        ))),
    }
}

fn parse_diagnostic_severity_name(raw: &str) -> Result<DiagnosticSeverity, StoreError> {
    match raw {
        "error" => Ok(DiagnosticSeverity::Error),
        "warning" => Ok(DiagnosticSeverity::Warning),
        "info" => Ok(DiagnosticSeverity::Info),
        other => Err(crate::invalid_input(format!(
            "unknown diagnostic severity `{other}`"
        ))),
    }
}

fn parse_field_presence_name(raw: &str) -> Result<FieldPresence, StoreError> {
    match raw {
        "required" => Ok(FieldPresence::Required),
        "recommended" => Ok(FieldPresence::Recommended),
        "optional" => Ok(FieldPresence::Optional),
        other => Err(crate::invalid_input(format!(
            "unknown field presence `{other}`"
        ))),
    }
}

fn parse_field_role_name(raw: &str) -> Result<FieldRole, StoreError> {
    match raw {
        "index" => Ok(FieldRole::Index),
        "projection_gate" => Ok(FieldRole::ProjectionGate),
        "render_only" => Ok(FieldRole::RenderOnly),
        "opaque" => Ok(FieldRole::Opaque),
        other => Err(crate::invalid_input(format!(
            "unknown field role `{other}`"
        ))),
    }
}

fn parse_inference_policy_name(raw: &str) -> Result<InferencePolicy, StoreError> {
    match raw {
        "manual_only" => Ok(InferencePolicy::ManualOnly),
        "model_may_infer" => Ok(InferencePolicy::ModelMayInfer),
        other => Err(crate::invalid_input(format!(
            "unknown inference policy `{other}`"
        ))),
    }
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
struct HypothesisRecordToolArgs {
    frontier_id: String,
    title: String,
    summary: String,
    body: String,
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
    summary: String,
    body: String,
    tags: Vec<String>,
    #[serde(default)]
    annotations: Vec<WireAnnotation>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SourceRecordToolArgs {
    frontier_id: Option<String>,
    title: String,
    summary: String,
    body: String,
    tags: Option<Vec<String>>,
    #[serde(default)]
    annotations: Vec<WireAnnotation>,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SchemaFieldUpsertToolArgs {
    name: String,
    node_classes: Option<Vec<String>>,
    presence: String,
    severity: String,
    role: String,
    inference_policy: String,
    value_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SchemaFieldRemoveToolArgs {
    name: String,
    node_classes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct MetricDefineToolArgs {
    key: String,
    unit: String,
    objective: String,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunDimensionDefineToolArgs {
    key: String,
    value_type: String,
    description: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MetricKeysToolArgs {
    frontier_id: Option<String>,
    source: Option<String>,
    dimensions: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
struct MetricBestToolArgs {
    key: String,
    frontier_id: Option<String>,
    source: Option<String>,
    dimensions: Option<BTreeMap<String, Value>>,
    order: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ExperimentOpenToolArgs {
    frontier_id: String,
    base_checkpoint_id: String,
    hypothesis_node_id: String,
    title: String,
    summary: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ExperimentListToolArgs {
    frontier_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExperimentReadToolArgs {
    experiment_id: String,
}

#[derive(Debug, Deserialize)]
struct ExperimentCloseToolArgs {
    experiment_id: String,
    candidate_summary: String,
    run: WireRun,
    primary_metric: WireMetricValue,
    #[serde(default)]
    supporting_metrics: Vec<WireMetricValue>,
    note: WireFrontierNote,
    verdict: String,
    decision_title: String,
    decision_rationale: String,
    analysis: Option<WireAnalysis>,
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
struct WireMetricValue {
    key: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct WireRun {
    title: String,
    summary: Option<String>,
    backend: String,
    #[serde(default)]
    dimensions: BTreeMap<String, Value>,
    command: WireRunCommand,
}

#[derive(Debug, Deserialize)]
struct WireAnalysis {
    title: String,
    summary: String,
    body: String,
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
