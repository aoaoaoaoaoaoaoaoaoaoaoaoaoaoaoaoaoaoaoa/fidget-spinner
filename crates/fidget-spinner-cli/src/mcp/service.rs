use std::collections::BTreeMap;
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    AnnotationVisibility, CodeSnapshotRef, CommandRecipe, ExecutionBackend, FrontierContract,
    FrontierNote, FrontierVerdict, MetricObservation, MetricSpec, MetricUnit, NodeAnnotation,
    NodeClass, NodePayload, NonEmptyText,
};
use fidget_spinner_store_sqlite::{
    CloseExperimentRequest, CreateFrontierRequest, CreateNodeRequest, EdgeAttachment,
    EdgeAttachmentDirection, ListNodesQuery, ProjectStore, StoreError,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
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
        match name {
            "project.status" => tool_success(&json!({
                "project_root": self.store.project_root(),
                "state_root": self.store.state_root(),
                "display_name": self.store.config().display_name,
                "schema": self.store.schema().schema_ref(),
                "git_repo_detected": crate::run_git(self.store.project_root(), &["rev-parse", "--show-toplevel"])
                    .map_err(store_fault("tools/call:project.status"))?
                    .is_some(),
            })),
            "project.schema" => tool_success(self.store.schema()),
            "frontier.list" => tool_success(
                &self
                    .store
                    .list_frontiers()
                    .map_err(store_fault("tools/call:frontier.list"))?,
            ),
            "frontier.status" => {
                let args = deserialize::<FrontierStatusToolArgs>(arguments)?;
                tool_success(
                    &self
                        .store
                        .frontier_projection(
                            crate::parse_frontier_id(&args.frontier_id)
                                .map_err(store_fault("tools/call:frontier.status"))?,
                        )
                        .map_err(store_fault("tools/call:frontier.status"))?,
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
                tool_success(&projection)
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
                tool_success(&node)
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
                        payload: NodePayload::with_schema(self.store.schema().schema_ref(), fields),
                        annotations: tool_annotations(args.annotations)
                            .map_err(store_fault("tools/call:change.record"))?,
                        attachments: lineage_attachments(args.parents)
                            .map_err(store_fault("tools/call:change.record"))?,
                    })
                    .map_err(store_fault("tools/call:change.record"))?;
                tool_success(&node)
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
                        include_archived: args.include_archived,
                        limit: args.limit.unwrap_or(20),
                    })
                    .map_err(store_fault("tools/call:node.list"))?;
                tool_success(&nodes)
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
                tool_success(&node)
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
                tool_success(&json!({"annotated": args.node_id}))
            }
            "node.archive" => {
                let args = deserialize::<NodeArchiveToolArgs>(arguments)?;
                self.store
                    .archive_node(
                        crate::parse_node_id(&args.node_id)
                            .map_err(store_fault("tools/call:node.archive"))?,
                    )
                    .map_err(store_fault("tools/call:node.archive"))?;
                tool_success(&json!({"archived": args.node_id}))
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
                tool_success(&node)
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
                tool_success(&node)
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
                tool_success(&receipt)
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

fn tool_success(value: &impl serde::Serialize) -> Result<Value, FaultRecord> {
    Ok(json!({
        "content": [{
            "type": "text",
            "text": crate::to_pretty_json(value).map_err(store_fault("worker.tool_success"))?,
        }],
        "structuredContent": serde_json::to_value(value)
            .map_err(store_fault("worker.tool_success"))?,
        "isError": false,
    }))
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
