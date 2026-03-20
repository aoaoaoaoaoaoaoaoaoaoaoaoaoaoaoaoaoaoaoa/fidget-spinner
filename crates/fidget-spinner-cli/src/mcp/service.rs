use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    ArtifactKind, CommandRecipe, ExecutionBackend, ExperimentAnalysis, ExperimentStatus,
    FieldValueType, FrontierVerdict, MetricUnit, MetricVisibility, NonEmptyText,
    OptimizationObjective, RunDimensionValue, Slug, TagName,
};
use fidget_spinner_store_sqlite::{
    AttachmentSelector, CloseExperimentRequest, CreateArtifactRequest, CreateFrontierRequest,
    CreateHypothesisRequest, DefineMetricRequest, DefineRunDimensionRequest, EntityHistoryEntry,
    ExperimentOutcomePatch, FrontierOpenProjection, FrontierRoadmapItemDraft, FrontierSummary,
    ListArtifactsQuery, ListExperimentsQuery, ListHypothesesQuery, MetricBestEntry,
    MetricBestQuery, MetricKeySummary, MetricKeysQuery, MetricRankOrder, MetricScope,
    OpenExperimentRequest, ProjectStatus, ProjectStore, StoreError, TextPatch,
    UpdateArtifactRequest, UpdateExperimentRequest, UpdateFrontierBriefRequest,
    UpdateHypothesisRequest, VertexSelector,
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
            WorkerOperation::ReadResource { uri } => Self::read_resource(&uri),
        }
    }

    fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value, FaultRecord> {
        let operation = format!("tools/call:{name}");
        let (presentation, arguments) =
            split_presentation(arguments, &operation, FaultStage::Worker)?;
        macro_rules! lift {
            ($expr:expr) => {
                with_fault($expr, &operation)?
            };
        }
        let output = match name {
            "project.status" => project_status_output(&lift!(self.store.status()), &operation)?,
            "tag.add" => {
                let args = deserialize::<TagAddArgs>(arguments)?;
                let tag = lift!(self.store.register_tag(
                    TagName::new(args.name).map_err(store_fault(&operation))?,
                    NonEmptyText::new(args.description).map_err(store_fault(&operation))?,
                ));
                tool_output(&tag, FaultStage::Worker, &operation)?
            }
            "tag.list" => tag_list_output(&lift!(self.store.list_tags()), &operation)?,
            "frontier.create" => {
                let args = deserialize::<FrontierCreateArgs>(arguments)?;
                let frontier = lift!(
                    self.store.create_frontier(CreateFrontierRequest {
                        label: NonEmptyText::new(args.label).map_err(store_fault(&operation))?,
                        objective: NonEmptyText::new(args.objective)
                            .map_err(store_fault(&operation))?,
                        slug: args
                            .slug
                            .map(Slug::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                    })
                );
                frontier_record_output(&frontier, &operation)?
            }
            "frontier.list" => {
                frontier_list_output(&lift!(self.store.list_frontiers()), &operation)?
            }
            "frontier.read" => {
                let args = deserialize::<FrontierSelectorArgs>(arguments)?;
                frontier_record_output(
                    &lift!(self.store.read_frontier(&args.frontier)),
                    &operation,
                )?
            }
            "frontier.open" => {
                let args = deserialize::<FrontierSelectorArgs>(arguments)?;
                frontier_open_output(&lift!(self.store.frontier_open(&args.frontier)), &operation)?
            }
            "frontier.brief.update" => {
                let args = deserialize::<FrontierBriefUpdateArgs>(arguments)?;
                let frontier = lift!(
                    self.store
                        .update_frontier_brief(UpdateFrontierBriefRequest {
                            frontier: args.frontier,
                            expected_revision: args.expected_revision,
                            situation: nullable_text_patch_from_wire(args.situation, &operation)?,
                            roadmap: args
                                .roadmap
                                .map(|items| {
                                    items
                                        .into_iter()
                                        .map(|item| {
                                            Ok(FrontierRoadmapItemDraft {
                                                rank: item.rank,
                                                hypothesis: item.hypothesis,
                                                summary: item
                                                    .summary
                                                    .map(NonEmptyText::new)
                                                    .transpose()
                                                    .map_err(store_fault(&operation))?,
                                            })
                                        })
                                        .collect::<Result<Vec<_>, FaultRecord>>()
                                })
                                .transpose()?,
                            unknowns: args
                                .unknowns
                                .map(|items| {
                                    items
                                        .into_iter()
                                        .map(NonEmptyText::new)
                                        .collect::<Result<Vec<_>, _>>()
                                        .map_err(store_fault(&operation))
                                })
                                .transpose()?,
                        })
                );
                frontier_record_output(&frontier, &operation)?
            }
            "frontier.history" => {
                let args = deserialize::<FrontierSelectorArgs>(arguments)?;
                history_output(
                    &lift!(self.store.frontier_history(&args.frontier)),
                    &operation,
                )?
            }
            "hypothesis.record" => {
                let args = deserialize::<HypothesisRecordArgs>(arguments)?;
                let hypothesis = lift!(
                    self.store.create_hypothesis(CreateHypothesisRequest {
                        frontier: args.frontier,
                        slug: args
                            .slug
                            .map(Slug::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        title: NonEmptyText::new(args.title).map_err(store_fault(&operation))?,
                        summary: NonEmptyText::new(args.summary)
                            .map_err(store_fault(&operation))?,
                        body: NonEmptyText::new(args.body).map_err(store_fault(&operation))?,
                        tags: tags_to_set(args.tags.unwrap_or_default())
                            .map_err(store_fault(&operation))?,
                        parents: args.parents.unwrap_or_default(),
                    })
                );
                hypothesis_record_output(&hypothesis, &operation)?
            }
            "hypothesis.list" => {
                let args = deserialize::<HypothesisListArgs>(arguments)?;
                let hypotheses = lift!(
                    self.store.list_hypotheses(ListHypothesesQuery {
                        frontier: args.frontier,
                        tags: tags_to_set(args.tags.unwrap_or_default())
                            .map_err(store_fault(&operation))?,
                        include_archived: args.include_archived.unwrap_or(false),
                        limit: args.limit,
                    })
                );
                hypothesis_list_output(&hypotheses, &operation)?
            }
            "hypothesis.read" => {
                let args = deserialize::<HypothesisSelectorArgs>(arguments)?;
                hypothesis_detail_output(
                    &lift!(self.store.read_hypothesis(&args.hypothesis)),
                    &operation,
                )?
            }
            "hypothesis.update" => {
                let args = deserialize::<HypothesisUpdateArgs>(arguments)?;
                let hypothesis = lift!(
                    self.store.update_hypothesis(UpdateHypothesisRequest {
                        hypothesis: args.hypothesis,
                        expected_revision: args.expected_revision,
                        title: args
                            .title
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        body: args
                            .body
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        tags: args
                            .tags
                            .map(tags_to_set)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        parents: args.parents,
                        archived: args.archived,
                    })
                );
                hypothesis_record_output(&hypothesis, &operation)?
            }
            "hypothesis.history" => {
                let args = deserialize::<HypothesisSelectorArgs>(arguments)?;
                history_output(
                    &lift!(self.store.hypothesis_history(&args.hypothesis)),
                    &operation,
                )?
            }
            "experiment.open" => {
                let args = deserialize::<ExperimentOpenArgs>(arguments)?;
                let experiment = lift!(
                    self.store.open_experiment(OpenExperimentRequest {
                        hypothesis: args.hypothesis,
                        slug: args
                            .slug
                            .map(Slug::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        title: NonEmptyText::new(args.title).map_err(store_fault(&operation))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        tags: tags_to_set(args.tags.unwrap_or_default())
                            .map_err(store_fault(&operation))?,
                        parents: args.parents.unwrap_or_default(),
                    })
                );
                experiment_record_output(&experiment, &operation)?
            }
            "experiment.list" => {
                let args = deserialize::<ExperimentListArgs>(arguments)?;
                let experiments = lift!(
                    self.store.list_experiments(ListExperimentsQuery {
                        frontier: args.frontier,
                        hypothesis: args.hypothesis,
                        tags: tags_to_set(args.tags.unwrap_or_default())
                            .map_err(store_fault(&operation))?,
                        include_archived: args.include_archived.unwrap_or(false),
                        status: args.status,
                        limit: args.limit,
                    })
                );
                experiment_list_output(&experiments, &operation)?
            }
            "experiment.read" => {
                let args = deserialize::<ExperimentSelectorArgs>(arguments)?;
                experiment_detail_output(
                    &lift!(self.store.read_experiment(&args.experiment)),
                    &operation,
                )?
            }
            "experiment.update" => {
                let args = deserialize::<ExperimentUpdateArgs>(arguments)?;
                let experiment = lift!(
                    self.store.update_experiment(UpdateExperimentRequest {
                        experiment: args.experiment,
                        expected_revision: args.expected_revision,
                        title: args
                            .title
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        summary: nullable_text_patch_from_wire(args.summary, &operation)?,
                        tags: args
                            .tags
                            .map(tags_to_set)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        parents: args.parents,
                        archived: args.archived,
                        outcome: args
                            .outcome
                            .map(|wire| experiment_outcome_patch_from_wire(wire, &operation))
                            .transpose()?,
                    })
                );
                experiment_record_output(&experiment, &operation)?
            }
            "experiment.close" => {
                let args = deserialize::<ExperimentCloseArgs>(arguments)?;
                let experiment = lift!(
                    self.store.close_experiment(CloseExperimentRequest {
                        experiment: args.experiment,
                        expected_revision: args.expected_revision,
                        backend: args.backend,
                        command: args.command,
                        dimensions: dimension_map_from_wire(args.dimensions)?,
                        primary_metric: metric_value_from_wire(args.primary_metric, &operation)?,
                        supporting_metrics: args
                            .supporting_metrics
                            .unwrap_or_default()
                            .into_iter()
                            .map(|metric| metric_value_from_wire(metric, &operation))
                            .collect::<Result<Vec<_>, _>>()?,
                        verdict: args.verdict,
                        rationale: NonEmptyText::new(args.rationale)
                            .map_err(store_fault(&operation))?,
                        analysis: args
                            .analysis
                            .map(|analysis| experiment_analysis_from_wire(analysis, &operation))
                            .transpose()?,
                    })
                );
                experiment_record_output(&experiment, &operation)?
            }
            "experiment.history" => {
                let args = deserialize::<ExperimentSelectorArgs>(arguments)?;
                history_output(
                    &lift!(self.store.experiment_history(&args.experiment)),
                    &operation,
                )?
            }
            "artifact.record" => {
                let args = deserialize::<ArtifactRecordArgs>(arguments)?;
                let artifact = lift!(
                    self.store.create_artifact(CreateArtifactRequest {
                        slug: args
                            .slug
                            .map(Slug::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        kind: args.kind,
                        label: NonEmptyText::new(args.label).map_err(store_fault(&operation))?,
                        summary: args
                            .summary
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        locator: NonEmptyText::new(args.locator)
                            .map_err(store_fault(&operation))?,
                        media_type: args
                            .media_type
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        attachments: args.attachments.unwrap_or_default(),
                    })
                );
                artifact_record_output(&artifact, &operation)?
            }
            "artifact.list" => {
                let args = deserialize::<ArtifactListArgs>(arguments)?;
                let artifacts = lift!(self.store.list_artifacts(ListArtifactsQuery {
                    frontier: args.frontier,
                    kind: args.kind,
                    attached_to: args.attached_to,
                    limit: args.limit,
                }));
                artifact_list_output(&artifacts, &operation)?
            }
            "artifact.read" => {
                let args = deserialize::<ArtifactSelectorArgs>(arguments)?;
                artifact_detail_output(
                    &lift!(self.store.read_artifact(&args.artifact)),
                    &operation,
                )?
            }
            "artifact.update" => {
                let args = deserialize::<ArtifactUpdateArgs>(arguments)?;
                let artifact = lift!(
                    self.store.update_artifact(UpdateArtifactRequest {
                        artifact: args.artifact,
                        expected_revision: args.expected_revision,
                        kind: args.kind,
                        label: args
                            .label
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        summary: nullable_text_patch_from_wire(args.summary, &operation)?,
                        locator: args
                            .locator
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        media_type: nullable_text_patch_from_wire(args.media_type, &operation)?,
                        attachments: args.attachments,
                    })
                );
                artifact_record_output(&artifact, &operation)?
            }
            "artifact.history" => {
                let args = deserialize::<ArtifactSelectorArgs>(arguments)?;
                history_output(
                    &lift!(self.store.artifact_history(&args.artifact)),
                    &operation,
                )?
            }
            "metric.define" => {
                let args = deserialize::<MetricDefineArgs>(arguments)?;
                tool_output(
                    &lift!(
                        self.store.define_metric(DefineMetricRequest {
                            key: NonEmptyText::new(args.key).map_err(store_fault(&operation))?,
                            unit: args.unit,
                            objective: args.objective,
                            visibility: args.visibility.unwrap_or(MetricVisibility::Canonical),
                            description: args
                                .description
                                .map(NonEmptyText::new)
                                .transpose()
                                .map_err(store_fault(&operation))?,
                        })
                    ),
                    FaultStage::Worker,
                    &operation,
                )?
            }
            "metric.keys" => {
                let args = deserialize::<MetricKeysArgs>(arguments)?;
                metric_keys_output(
                    &lift!(self.store.metric_keys(MetricKeysQuery {
                        frontier: args.frontier,
                        scope: args.scope.unwrap_or(MetricScope::Live),
                    })),
                    &operation,
                )?
            }
            "metric.best" => {
                let args = deserialize::<MetricBestArgs>(arguments)?;
                metric_best_output(
                    &lift!(self.store.metric_best(MetricBestQuery {
                        frontier: args.frontier,
                        hypothesis: args.hypothesis,
                        key: NonEmptyText::new(args.key).map_err(store_fault(&operation))?,
                        dimensions: dimension_map_from_wire(args.dimensions)?,
                        include_rejected: args.include_rejected.unwrap_or(false),
                        limit: args.limit,
                        order: args.order,
                    })),
                    &operation,
                )?
            }
            "run.dimension.define" => {
                let args = deserialize::<DimensionDefineArgs>(arguments)?;
                tool_output(
                    &lift!(
                        self.store.define_run_dimension(DefineRunDimensionRequest {
                            key: NonEmptyText::new(args.key).map_err(store_fault(&operation))?,
                            value_type: args.value_type,
                            description: args
                                .description
                                .map(NonEmptyText::new)
                                .transpose()
                                .map_err(store_fault(&operation))?,
                        })
                    ),
                    FaultStage::Worker,
                    &operation,
                )?
            }
            "run.dimension.list" => tool_output(
                &lift!(self.store.list_run_dimensions()),
                FaultStage::Worker,
                &operation,
            )?,
            other => {
                return Err(FaultRecord::new(
                    FaultKind::InvalidInput,
                    FaultStage::Worker,
                    &operation,
                    format!("unknown worker tool `{other}`"),
                ));
            }
        };
        tool_success(output, presentation, FaultStage::Worker, &operation)
    }

    fn read_resource(uri: &str) -> Result<Value, FaultRecord> {
        Err(FaultRecord::new(
            FaultKind::InvalidInput,
            FaultStage::Worker,
            format!("resources/read:{uri}"),
            format!("unknown worker resource `{uri}`"),
        ))
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

#[derive(Debug, Deserialize)]
struct TagAddArgs {
    name: String,
    description: String,
}

#[derive(Debug, Deserialize)]
struct FrontierCreateArgs {
    label: String,
    objective: String,
    slug: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FrontierSelectorArgs {
    frontier: String,
}

#[derive(Debug, Deserialize)]
struct FrontierBriefUpdateArgs {
    frontier: String,
    expected_revision: Option<u64>,
    situation: Option<NullableStringArg>,
    roadmap: Option<Vec<FrontierRoadmapItemWire>>,
    unknowns: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct FrontierRoadmapItemWire {
    rank: u32,
    hypothesis: String,
    summary: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HypothesisRecordArgs {
    frontier: String,
    title: String,
    summary: String,
    body: String,
    slug: Option<String>,
    tags: Option<Vec<String>>,
    parents: Option<Vec<VertexSelector>>,
}

#[derive(Debug, Deserialize)]
struct HypothesisListArgs {
    frontier: Option<String>,
    tags: Option<Vec<String>>,
    include_archived: Option<bool>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct HypothesisSelectorArgs {
    hypothesis: String,
}

#[derive(Debug, Deserialize)]
struct HypothesisUpdateArgs {
    hypothesis: String,
    expected_revision: Option<u64>,
    title: Option<String>,
    summary: Option<String>,
    body: Option<String>,
    tags: Option<Vec<String>>,
    parents: Option<Vec<VertexSelector>>,
    archived: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ExperimentOpenArgs {
    hypothesis: String,
    title: String,
    summary: Option<String>,
    slug: Option<String>,
    tags: Option<Vec<String>>,
    parents: Option<Vec<VertexSelector>>,
}

#[derive(Debug, Deserialize)]
struct ExperimentListArgs {
    frontier: Option<String>,
    hypothesis: Option<String>,
    tags: Option<Vec<String>>,
    include_archived: Option<bool>,
    status: Option<ExperimentStatus>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ExperimentSelectorArgs {
    experiment: String,
}

#[derive(Debug, Deserialize)]
struct ExperimentUpdateArgs {
    experiment: String,
    expected_revision: Option<u64>,
    title: Option<String>,
    summary: Option<NullableStringArg>,
    tags: Option<Vec<String>>,
    parents: Option<Vec<VertexSelector>>,
    archived: Option<bool>,
    outcome: Option<ExperimentOutcomeWire>,
}

#[derive(Debug, Deserialize)]
struct ExperimentCloseArgs {
    experiment: String,
    expected_revision: Option<u64>,
    backend: ExecutionBackend,
    command: CommandRecipe,
    dimensions: Option<Map<String, Value>>,
    primary_metric: MetricValueWire,
    supporting_metrics: Option<Vec<MetricValueWire>>,
    verdict: FrontierVerdict,
    rationale: String,
    analysis: Option<ExperimentAnalysisWire>,
}

#[derive(Debug, Deserialize)]
struct ExperimentOutcomeWire {
    backend: ExecutionBackend,
    command: CommandRecipe,
    dimensions: Option<Map<String, Value>>,
    primary_metric: MetricValueWire,
    supporting_metrics: Option<Vec<MetricValueWire>>,
    verdict: FrontierVerdict,
    rationale: String,
    analysis: Option<ExperimentAnalysisWire>,
}

#[derive(Debug, Deserialize)]
struct ExperimentAnalysisWire {
    summary: String,
    body: String,
}

#[derive(Debug, Deserialize)]
struct MetricValueWire {
    key: String,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct ArtifactRecordArgs {
    kind: ArtifactKind,
    label: String,
    summary: Option<String>,
    locator: String,
    media_type: Option<String>,
    slug: Option<String>,
    attachments: Option<Vec<AttachmentSelector>>,
}

#[derive(Debug, Deserialize)]
struct ArtifactListArgs {
    frontier: Option<String>,
    kind: Option<ArtifactKind>,
    attached_to: Option<AttachmentSelector>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ArtifactSelectorArgs {
    artifact: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactUpdateArgs {
    artifact: String,
    expected_revision: Option<u64>,
    kind: Option<ArtifactKind>,
    label: Option<String>,
    summary: Option<NullableStringArg>,
    locator: Option<String>,
    media_type: Option<NullableStringArg>,
    attachments: Option<Vec<AttachmentSelector>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum NullableStringArg {
    Set(String),
    Clear(()),
}

#[derive(Debug, Deserialize)]
struct MetricDefineArgs {
    key: String,
    unit: MetricUnit,
    objective: OptimizationObjective,
    visibility: Option<MetricVisibility>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MetricKeysArgs {
    frontier: Option<String>,
    scope: Option<MetricScope>,
}

#[derive(Debug, Deserialize)]
struct MetricBestArgs {
    frontier: Option<String>,
    hypothesis: Option<String>,
    key: String,
    dimensions: Option<Map<String, Value>>,
    include_rejected: Option<bool>,
    limit: Option<u32>,
    order: Option<MetricRankOrder>,
}

#[derive(Debug, Deserialize)]
struct DimensionDefineArgs {
    key: String,
    value_type: FieldValueType,
    description: Option<String>,
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

fn store_fault<E>(operation: &str) -> impl FnOnce(E) -> FaultRecord + '_
where
    E: Into<StoreError>,
{
    move |error| {
        let error: StoreError = error.into();
        let kind = match error {
            StoreError::MissingProjectStore(_)
            | StoreError::AmbiguousProjectStoreDiscovery { .. }
            | StoreError::UnknownTag(_)
            | StoreError::UnknownMetricDefinition(_)
            | StoreError::UnknownRunDimension(_)
            | StoreError::UnknownFrontierSelector(_)
            | StoreError::UnknownHypothesisSelector(_)
            | StoreError::UnknownExperimentSelector(_)
            | StoreError::UnknownArtifactSelector(_)
            | StoreError::RevisionMismatch { .. }
            | StoreError::HypothesisBodyMustBeSingleParagraph
            | StoreError::ExperimentHypothesisRequired
            | StoreError::ExperimentAlreadyClosed(_)
            | StoreError::ExperimentStillOpen(_)
            | StoreError::CrossFrontierInfluence
            | StoreError::SelfEdge
            | StoreError::UnknownRoadmapHypothesis(_)
            | StoreError::ManualExperimentRequiresCommand
            | StoreError::MetricOrderRequired { .. }
            | StoreError::UnknownDimensionFilter(_)
            | StoreError::DuplicateTag(_)
            | StoreError::DuplicateMetricDefinition(_)
            | StoreError::DuplicateRunDimension(_)
            | StoreError::InvalidInput(_) => FaultKind::InvalidInput,
            StoreError::IncompatibleStoreFormatVersion { .. } => FaultKind::Unavailable,
            StoreError::Io(_)
            | StoreError::Sql(_)
            | StoreError::Json(_)
            | StoreError::TimeParse(_)
            | StoreError::TimeFormat(_)
            | StoreError::Core(_)
            | StoreError::Uuid(_) => FaultKind::Internal,
        };
        FaultRecord::new(kind, FaultStage::Store, operation, error.to_string())
    }
}

fn with_fault<T, E>(result: Result<T, E>, operation: &str) -> Result<T, FaultRecord>
where
    E: Into<StoreError>,
{
    result.map_err(store_fault(operation))
}

fn tags_to_set(tags: Vec<String>) -> Result<BTreeSet<TagName>, StoreError> {
    tags.into_iter()
        .map(TagName::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(StoreError::from)
}

fn metric_value_from_wire(
    wire: MetricValueWire,
    operation: &str,
) -> Result<fidget_spinner_core::MetricValue, FaultRecord> {
    Ok(fidget_spinner_core::MetricValue {
        key: NonEmptyText::new(wire.key).map_err(store_fault(operation))?,
        value: wire.value,
    })
}

fn experiment_analysis_from_wire(
    wire: ExperimentAnalysisWire,
    operation: &str,
) -> Result<ExperimentAnalysis, FaultRecord> {
    Ok(ExperimentAnalysis {
        summary: NonEmptyText::new(wire.summary).map_err(store_fault(operation))?,
        body: NonEmptyText::new(wire.body).map_err(store_fault(operation))?,
    })
}

fn experiment_outcome_patch_from_wire(
    wire: ExperimentOutcomeWire,
    operation: &str,
) -> Result<ExperimentOutcomePatch, FaultRecord> {
    Ok(ExperimentOutcomePatch {
        backend: wire.backend,
        command: wire.command,
        dimensions: dimension_map_from_wire(wire.dimensions)?,
        primary_metric: metric_value_from_wire(wire.primary_metric, operation)?,
        supporting_metrics: wire
            .supporting_metrics
            .unwrap_or_default()
            .into_iter()
            .map(|metric| metric_value_from_wire(metric, operation))
            .collect::<Result<Vec<_>, _>>()?,
        verdict: wire.verdict,
        rationale: NonEmptyText::new(wire.rationale).map_err(store_fault(operation))?,
        analysis: wire
            .analysis
            .map(|analysis| experiment_analysis_from_wire(analysis, operation))
            .transpose()?,
    })
}

fn nullable_text_patch_from_wire(
    patch: Option<NullableStringArg>,
    operation: &str,
) -> Result<Option<TextPatch<NonEmptyText>>, FaultRecord> {
    match patch {
        None => Ok(None),
        Some(NullableStringArg::Clear(())) => Ok(Some(TextPatch::Clear)),
        Some(NullableStringArg::Set(value)) => Ok(Some(TextPatch::Set(
            NonEmptyText::new(value).map_err(store_fault(operation))?,
        ))),
    }
}

fn dimension_map_from_wire(
    dimensions: Option<Map<String, Value>>,
) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, FaultRecord> {
    dimensions
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| {
            Ok((
                NonEmptyText::new(key).map_err(store_fault("dimension-map"))?,
                json_value_to_dimension(value)?,
            ))
        })
        .collect()
}

fn json_value_to_dimension(value: Value) -> Result<RunDimensionValue, FaultRecord> {
    match value {
        Value::String(raw) => {
            if time::OffsetDateTime::parse(&raw, &time::format_description::well_known::Rfc3339)
                .is_ok()
            {
                NonEmptyText::new(raw)
                    .map(RunDimensionValue::Timestamp)
                    .map_err(store_fault("dimension-map"))
            } else {
                NonEmptyText::new(raw)
                    .map(RunDimensionValue::String)
                    .map_err(store_fault("dimension-map"))
            }
        }
        Value::Number(number) => number
            .as_f64()
            .map(RunDimensionValue::Numeric)
            .ok_or_else(|| {
                FaultRecord::new(
                    FaultKind::InvalidInput,
                    FaultStage::Protocol,
                    "dimension-map",
                    "numeric dimension values must fit into f64",
                )
            }),
        Value::Bool(value) => Ok(RunDimensionValue::Boolean(value)),
        _ => Err(FaultRecord::new(
            FaultKind::InvalidInput,
            FaultStage::Protocol,
            "dimension-map",
            "dimension values must be string, number, boolean, or RFC3339 timestamp",
        )),
    }
}

fn project_status_output(
    status: &ProjectStatus,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "display_name": status.display_name,
        "project_root": status.project_root,
        "frontier_count": status.frontier_count,
        "hypothesis_count": status.hypothesis_count,
        "experiment_count": status.experiment_count,
        "open_experiment_count": status.open_experiment_count,
        "artifact_count": status.artifact_count,
    });
    detailed_tool_output(
        &concise,
        status,
        [
            format!("project {}", status.display_name),
            format!("root: {}", status.project_root),
            format!("frontiers: {}", status.frontier_count),
            format!("hypotheses: {}", status.hypothesis_count),
            format!(
                "experiments: {} (open {})",
                status.experiment_count, status.open_experiment_count
            ),
            format!("artifacts: {}", status.artifact_count),
        ]
        .join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn tag_list_output(
    tags: &[fidget_spinner_core::TagRecord],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "count": tags.len(),
        "tags": tags,
    });
    detailed_tool_output(
        &concise,
        &concise,
        if tags.is_empty() {
            "no tags".to_owned()
        } else {
            tags.iter()
                .map(|tag| format!("{} — {}", tag.name, tag.description))
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn frontier_list_output(
    frontiers: &[FrontierSummary],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": frontiers.len(), "frontiers": frontiers });
    detailed_tool_output(
        &concise,
        &concise,
        if frontiers.is_empty() {
            "no frontiers".to_owned()
        } else {
            frontiers
                .iter()
                .map(|frontier| {
                    format!(
                        "{} — {} | active hypotheses {} | open experiments {}",
                        frontier.slug,
                        frontier.objective,
                        frontier.active_hypothesis_count,
                        frontier.open_experiment_count
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn frontier_record_output(
    frontier: &fidget_spinner_core::FrontierRecord,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let mut lines = vec![format!(
        "frontier {} — {}",
        frontier.slug, frontier.objective
    )];
    lines.push(format!("status: {}", frontier.status.as_str()));
    if let Some(situation) = frontier.brief.situation.as_ref() {
        lines.push(format!("situation: {}", situation));
    }
    if !frontier.brief.roadmap.is_empty() {
        lines.push("roadmap:".to_owned());
        for item in &frontier.brief.roadmap {
            lines.push(format!(
                "  {}. {}{}",
                item.rank,
                item.hypothesis_id,
                item.summary
                    .as_ref()
                    .map_or_else(String::new, |summary| format!(" — {summary}"))
            ));
        }
    }
    if !frontier.brief.unknowns.is_empty() {
        lines.push(format!(
            "unknowns: {}",
            frontier
                .brief
                .unknowns
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    detailed_tool_output(
        &frontier,
        frontier,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn frontier_open_output(
    projection: &FrontierOpenProjection,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let mut lines = vec![format!(
        "frontier {} — {}",
        projection.frontier.slug, projection.frontier.objective
    )];
    if let Some(situation) = projection.frontier.brief.situation.as_ref() {
        lines.push(format!("situation: {}", situation));
    }
    if !projection.active_tags.is_empty() {
        lines.push(format!(
            "active tags: {}",
            projection
                .active_tags
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !projection.active_metric_keys.is_empty() {
        lines.push(format!(
            "live metrics: {}",
            projection
                .active_metric_keys
                .iter()
                .map(|metric| metric.key.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !projection.active_hypotheses.is_empty() {
        lines.push("active hypotheses:".to_owned());
        for state in &projection.active_hypotheses {
            let status = state
                .latest_closed_experiment
                .as_ref()
                .and_then(|experiment| experiment.verdict)
                .map_or_else(
                    || "unjudged".to_owned(),
                    |verdict| verdict.as_str().to_owned(),
                );
            lines.push(format!(
                "  {} — {} | open {} | latest {}",
                state.hypothesis.slug,
                state.hypothesis.summary,
                state.open_experiments.len(),
                status
            ));
        }
    }
    if !projection.open_experiments.is_empty() {
        lines.push("open experiments:".to_owned());
        for experiment in &projection.open_experiments {
            lines.push(format!(
                "  {} — {}",
                experiment.slug,
                experiment
                    .summary
                    .as_ref()
                    .map_or_else(|| experiment.title.to_string(), ToString::to_string)
            ));
        }
    }
    detailed_tool_output(
        projection,
        projection,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn hypothesis_record_output(
    hypothesis: &fidget_spinner_core::HypothesisRecord,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    detailed_tool_output(
        hypothesis,
        hypothesis,
        format!("hypothesis {} — {}", hypothesis.slug, hypothesis.summary),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn hypothesis_list_output(
    hypotheses: &[fidget_spinner_store_sqlite::HypothesisSummary],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": hypotheses.len(), "hypotheses": hypotheses });
    detailed_tool_output(
        &concise,
        &concise,
        if hypotheses.is_empty() {
            "no hypotheses".to_owned()
        } else {
            hypotheses
                .iter()
                .map(|hypothesis| {
                    let verdict = hypothesis.latest_verdict.map_or_else(
                        || "unjudged".to_owned(),
                        |verdict| verdict.as_str().to_owned(),
                    );
                    format!(
                        "{} — {} | open {} | latest {}",
                        hypothesis.slug,
                        hypothesis.summary,
                        hypothesis.open_experiment_count,
                        verdict
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn hypothesis_detail_output(
    detail: &fidget_spinner_store_sqlite::HypothesisDetail,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let mut lines = vec![
        format!(
            "hypothesis {} — {}",
            detail.record.slug, detail.record.summary
        ),
        detail.record.body.to_string(),
    ];
    if !detail.record.tags.is_empty() {
        lines.push(format!(
            "tags: {}",
            detail
                .record
                .tags
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.push(format!(
        "parents: {} | children: {} | open experiments: {} | closed experiments: {} | artifacts: {}",
        detail.parents.len(),
        detail.children.len(),
        detail.open_experiments.len(),
        detail.closed_experiments.len(),
        detail.artifacts.len()
    ));
    detailed_tool_output(
        detail,
        detail,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn experiment_record_output(
    experiment: &fidget_spinner_core::ExperimentRecord,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let mut line = format!("experiment {} — {}", experiment.slug, experiment.title);
    if let Some(outcome) = experiment.outcome.as_ref() {
        let _ = write!(
            line,
            " | {} {}={}",
            outcome.verdict.as_str(),
            outcome.primary_metric.key,
            outcome.primary_metric.value
        );
    } else {
        let _ = write!(line, " | open");
    }
    detailed_tool_output(
        experiment,
        experiment,
        line,
        None,
        FaultStage::Worker,
        operation,
    )
}

fn experiment_list_output(
    experiments: &[fidget_spinner_store_sqlite::ExperimentSummary],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": experiments.len(), "experiments": experiments });
    detailed_tool_output(
        &concise,
        &concise,
        if experiments.is_empty() {
            "no experiments".to_owned()
        } else {
            experiments
                .iter()
                .map(|experiment| {
                    let status = experiment.verdict.map_or_else(
                        || experiment.status.as_str().to_owned(),
                        |verdict| verdict.as_str().to_owned(),
                    );
                    let metric = experiment
                        .primary_metric
                        .as_ref()
                        .map_or_else(String::new, |metric| {
                            format!(" | {}={}", metric.key, metric.value)
                        });
                    format!(
                        "{} — {} | {}{}",
                        experiment.slug, experiment.title, status, metric
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn experiment_detail_output(
    detail: &fidget_spinner_store_sqlite::ExperimentDetail,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let mut lines = vec![format!(
        "experiment {} — {}",
        detail.record.slug, detail.record.title
    )];
    lines.push(format!("hypothesis: {}", detail.owning_hypothesis.slug));
    lines.push(format!(
        "status: {}",
        detail.record.outcome.as_ref().map_or_else(
            || "open".to_owned(),
            |outcome| outcome.verdict.as_str().to_owned()
        )
    ));
    if let Some(outcome) = detail.record.outcome.as_ref() {
        lines.push(format!(
            "primary metric: {}={}",
            outcome.primary_metric.key, outcome.primary_metric.value
        ));
        lines.push(format!("rationale: {}", outcome.rationale));
    }
    lines.push(format!(
        "parents: {} | children: {} | artifacts: {}",
        detail.parents.len(),
        detail.children.len(),
        detail.artifacts.len()
    ));
    detailed_tool_output(
        detail,
        detail,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn artifact_record_output(
    artifact: &fidget_spinner_core::ArtifactRecord,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    detailed_tool_output(
        artifact,
        artifact,
        format!(
            "artifact {} — {} -> {}",
            artifact.slug, artifact.label, artifact.locator
        ),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn artifact_list_output(
    artifacts: &[fidget_spinner_store_sqlite::ArtifactSummary],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": artifacts.len(), "artifacts": artifacts });
    detailed_tool_output(
        &concise,
        &concise,
        if artifacts.is_empty() {
            "no artifacts".to_owned()
        } else {
            artifacts
                .iter()
                .map(|artifact| {
                    format!(
                        "{} — {} -> {}",
                        artifact.slug, artifact.label, artifact.locator
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn artifact_detail_output(
    detail: &fidget_spinner_store_sqlite::ArtifactDetail,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let mut lines = vec![format!(
        "artifact {} — {} -> {}",
        detail.record.slug, detail.record.label, detail.record.locator
    )];
    if !detail.attachments.is_empty() {
        lines.push(format!("attachments: {}", detail.attachments.len()));
    }
    detailed_tool_output(
        detail,
        detail,
        lines.join("\n"),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn metric_keys_output(
    keys: &[MetricKeySummary],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": keys.len(), "metrics": keys });
    detailed_tool_output(
        &concise,
        &concise,
        if keys.is_empty() {
            "no metrics".to_owned()
        } else {
            keys.iter()
                .map(|metric| {
                    format!(
                        "{} [{} {} {}] refs={}",
                        metric.key,
                        metric.unit.as_str(),
                        metric.objective.as_str(),
                        metric.visibility.as_str(),
                        metric.reference_count
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn metric_best_output(
    entries: &[MetricBestEntry],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": entries.len(), "entries": entries });
    detailed_tool_output(
        &concise,
        &concise,
        if entries.is_empty() {
            "no matching experiments".to_owned()
        } else {
            entries
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    format!(
                        "{}. {} / {} = {} ({})",
                        index + 1,
                        entry.experiment.slug,
                        entry.hypothesis.slug,
                        entry.value,
                        entry.experiment.verdict.map_or_else(
                            || entry.experiment.status.as_str().to_owned(),
                            |verdict| verdict.as_str().to_owned()
                        )
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn history_output(
    history: &[EntityHistoryEntry],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({ "count": history.len(), "history": history });
    detailed_tool_output(
        &concise,
        &concise,
        if history.is_empty() {
            "no history".to_owned()
        } else {
            history
                .iter()
                .map(|entry| {
                    format!(
                        "rev {} {} @ {}",
                        entry.revision, entry.event_kind, entry.occurred_at
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}
