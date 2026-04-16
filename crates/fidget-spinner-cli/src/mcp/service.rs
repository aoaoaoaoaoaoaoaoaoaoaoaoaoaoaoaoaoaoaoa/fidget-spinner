use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::time::UNIX_EPOCH;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    ArtifactKind, AttachmentTargetRef, CommandRecipe, ExecutionBackend, ExperimentAnalysis,
    ExperimentStatus, FieldValueType, FrontierStatus, FrontierVerdict, MetricUnit,
    MetricVisibility, NonEmptyText, OptimizationObjective, RunDimensionValue, Slug, TagName,
};
use fidget_spinner_store_sqlite::{
    AttachmentSelector, CloseExperimentRequest, CreateArtifactRequest, CreateFrontierRequest,
    CreateHypothesisRequest, DefineMetricRequest, DefineRunDimensionRequest, EntityHistoryEntry,
    ExperimentNearestQuery, ExperimentOutcomePatch, FrontierOpenProjection,
    FrontierRoadmapItemDraft, FrontierSummary, ListArtifactsQuery, ListExperimentsQuery,
    ListFrontiersQuery, ListHypothesesQuery, MetricBestEntry, MetricBestQuery, MetricKeySummary,
    MetricKeysQuery, MetricRankOrder, MetricScope, OpenExperimentRequest, ProjectStatus,
    ProjectStore, StoreError, TextPatch, UpdateArtifactRequest, UpdateExperimentRequest,
    UpdateFrontierRequest, UpdateHypothesisRequest, VertexSelector, VertexSummary,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
use crate::mcp::output::{
    ToolOutput, fallback_detailed_tool_output, projected_tool_output, split_presentation,
    tool_success,
};
use crate::mcp::projection;
use crate::mcp::protocol::{TRANSIENT_ONCE_ENV, TRANSIENT_ONCE_MARKER_ENV, WorkerOperation};

pub(crate) struct WorkerService {
    project_root: Utf8PathBuf,
    store: ProjectStore,
    store_identity: StoreIdentity,
}

impl WorkerService {
    pub fn new(project: &Utf8Path) -> Result<Self, StoreError> {
        let project_root = project.to_path_buf();
        let store = crate::open_store(project_root.as_std_path())?;
        let store_identity = read_store_identity(&project_root)?;
        Ok(Self {
            project_root,
            store,
            store_identity,
        })
    }

    pub fn execute(&mut self, operation: WorkerOperation) -> Result<Value, FaultRecord> {
        let operation_key = match &operation {
            WorkerOperation::CallTool { name, .. } => format!("tools/call:{name}"),
            WorkerOperation::ReadResource { uri } => format!("resources/read:{uri}"),
        };
        self.refresh_store_if_replaced(&operation_key)?;
        Self::maybe_inject_transient(&operation_key)?;
        let result = match operation {
            WorkerOperation::CallTool { name, arguments } => self.call_tool(&name, arguments),
            WorkerOperation::ReadResource { uri } => Self::read_resource(&uri),
        };
        if result.is_ok() {
            self.refresh_store_identity_snapshot();
        }
        result
    }

    fn refresh_store_if_replaced(&mut self, operation: &str) -> Result<(), FaultRecord> {
        let live_identity = with_fault(read_store_identity(&self.project_root), operation)?;
        if live_identity == self.store_identity {
            return Ok(());
        }
        self.store = with_fault(
            crate::open_store(self.project_root.as_std_path()),
            operation,
        )?;
        self.store_identity = live_identity;
        Ok(())
    }

    fn refresh_store_identity_snapshot(&mut self) {
        if let Ok(identity) = read_store_identity(&self.project_root) {
            self.store_identity = identity;
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
                let tag = lift!(self.store.register_tag_from_mcp(
                    TagName::new(args.name).map_err(store_fault(&operation))?,
                    NonEmptyText::new(args.description).map_err(store_fault(&operation))?,
                ));
                tag_record_output(&tag, &operation)?
            }
            "tag.list" => tag_registry_output(&lift!(self.store.tag_registry()), &operation)?,
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
                frontier_record_output(&self.store, &frontier, &operation)?
            }
            "frontier.list" => {
                let args = deserialize::<FrontierListArgs>(arguments)?;
                frontier_list_output(
                    &lift!(self.store.list_frontiers(ListFrontiersQuery {
                        include_archived: args.include_archived.unwrap_or(false),
                    })),
                    &operation,
                )?
            }
            "frontier.read" => {
                let args = deserialize::<FrontierSelectorArgs>(arguments)?;
                frontier_record_output(
                    &self.store,
                    &lift!(self.store.read_frontier(&args.frontier)),
                    &operation,
                )?
            }
            "frontier.open" => {
                let args = deserialize::<FrontierSelectorArgs>(arguments)?;
                frontier_open_output(&lift!(self.store.frontier_open(&args.frontier)), &operation)?
            }
            "frontier.update" => {
                let args = deserialize::<FrontierUpdateArgs>(arguments)?;
                let frontier = lift!(
                    self.store.update_frontier(UpdateFrontierRequest {
                        frontier: args.frontier,
                        expected_revision: args.expected_revision,
                        objective: args
                            .objective
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                        status: args.status,
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
                        scoreboard_metric_keys: args
                            .scoreboard_metric_keys
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
                frontier_record_output(&self.store, &frontier, &operation)?
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
                    self.store
                        .create_hypothesis_from_mcp(CreateHypothesisRequest {
                            frontier: args.frontier,
                            slug: args
                                .slug
                                .map(Slug::new)
                                .transpose()
                                .map_err(store_fault(&operation))?,
                            title: NonEmptyText::new(args.title)
                                .map_err(store_fault(&operation))?,
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
                    &self.store,
                    &lift!(self.store.read_hypothesis(&args.hypothesis)),
                    &operation,
                )?
            }
            "hypothesis.update" => {
                let args = deserialize::<HypothesisUpdateArgs>(arguments)?;
                let hypothesis = lift!(
                    self.store
                        .update_hypothesis_from_mcp(UpdateHypothesisRequest {
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
                    self.store.open_experiment_from_mcp(OpenExperimentRequest {
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
                    &self.store,
                    &lift!(self.store.read_experiment(&args.experiment)),
                    &operation,
                )?
            }
            "experiment.update" => {
                let args = deserialize::<ExperimentUpdateArgs>(arguments)?;
                let experiment = lift!(
                    self.store
                        .update_experiment_from_mcp(UpdateExperimentRequest {
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
            "experiment.nearest" => {
                let args = deserialize::<ExperimentNearestArgs>(arguments)?;
                experiment_nearest_output(
                    &lift!(
                        self.store.experiment_nearest(ExperimentNearestQuery {
                            frontier: args.frontier,
                            hypothesis: args.hypothesis,
                            experiment: args.experiment,
                            metric: args
                                .metric
                                .map(NonEmptyText::new)
                                .transpose()
                                .map_err(store_fault(&operation))?,
                            dimensions: dimension_map_from_wire(args.dimensions)?,
                            tags: args
                                .tags
                                .map(tags_to_set)
                                .transpose()
                                .map_err(store_fault(&operation))?
                                .unwrap_or_default(),
                            order: args.order,
                        })
                    ),
                    &operation,
                )?
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
                    &self.store,
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
                let metric = lift!(
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
                );
                metric_definition_output(&metric, &operation)?
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
                let dimension = lift!(
                    self.store.define_run_dimension(DefineRunDimensionRequest {
                        key: NonEmptyText::new(args.key).map_err(store_fault(&operation))?,
                        value_type: args.value_type,
                        description: args
                            .description
                            .map(NonEmptyText::new)
                            .transpose()
                            .map_err(store_fault(&operation))?,
                    })
                );
                run_dimension_definition_output(&dimension, &operation)?
            }
            "run.dimension.list" => {
                let dimensions = lift!(self.store.list_run_dimensions());
                run_dimension_list_output(&dimensions, &operation)?
            }
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
struct FrontierListArgs {
    include_archived: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct FrontierSelectorArgs {
    frontier: String,
}

#[derive(Debug, Deserialize)]
struct FrontierUpdateArgs {
    frontier: String,
    expected_revision: Option<u64>,
    objective: Option<String>,
    status: Option<FrontierStatus>,
    situation: Option<NullableStringArg>,
    roadmap: Option<Vec<FrontierRoadmapItemWire>>,
    unknowns: Option<Vec<String>>,
    scoreboard_metric_keys: Option<Vec<String>>,
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
struct ExperimentNearestArgs {
    frontier: Option<String>,
    hypothesis: Option<String>,
    experiment: Option<String>,
    metric: Option<String>,
    dimensions: Option<Map<String, Value>>,
    tags: Option<Vec<String>>,
    order: Option<MetricRankOrder>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoreIdentity {
    config: FileIdentity,
    database: FileIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    len_bytes: u64,
    modified_unix_nanos: u128,
    unique_key: u128,
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
            | StoreError::UnknownTagFamily(_)
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
            | StoreError::MetricScopeRequiresFrontier { .. }
            | StoreError::UnknownDimensionFilter(_)
            | StoreError::DuplicateTag(_)
            | StoreError::DuplicateTagFamily(_)
            | StoreError::DuplicateMetricDefinition(_)
            | StoreError::DuplicateRunDimension(_)
            | StoreError::GitWorktreeRequired(_)
            | StoreError::GitHeadRequired(_)
            | StoreError::DirtyGitWorktree { .. }
            | StoreError::InvalidInput(_) => FaultKind::InvalidInput,
            StoreError::PolicyViolation(_) => FaultKind::PolicyViolation,
            StoreError::IncompatibleStoreFormatVersion { .. } => FaultKind::Unavailable,
            StoreError::Io(_)
            | StoreError::Sql(_)
            | StoreError::Json(_)
            | StoreError::TimeParse(_)
            | StoreError::TimeFormat(_)
            | StoreError::Core(_)
            | StoreError::GitSpawn { .. }
            | StoreError::GitCommandFailed { .. }
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

fn read_store_identity(project_root: &Utf8Path) -> Result<StoreIdentity, StoreError> {
    let state_root = fidget_spinner_store_sqlite::state_root_for_project_root(project_root)?;
    let config_path = state_root.join(fidget_spinner_store_sqlite::PROJECT_CONFIG_NAME);
    let database_path = state_root.join(fidget_spinner_store_sqlite::STATE_DB_NAME);
    if !config_path.exists() || !database_path.exists() {
        return Err(StoreError::MissingProjectStore(project_root.to_path_buf()));
    }
    Ok(StoreIdentity {
        config: read_file_identity(&config_path)?,
        database: read_file_identity(&database_path)?,
    })
}

fn read_file_identity(path: &Utf8Path) -> Result<FileIdentity, StoreError> {
    let metadata = fs::metadata(path.as_std_path())?;
    let modified_unix_nanos = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    #[cfg(unix)]
    let unique_key = (u128::from(metadata.dev()) << 64) | u128::from(metadata.ino());
    #[cfg(not(unix))]
    let unique_key = 0;
    Ok(FileIdentity {
        len_bytes: metadata.len(),
        modified_unix_nanos,
        unique_key,
    })
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

fn run_dimension_value_text(value: &RunDimensionValue) -> String {
    match value {
        RunDimensionValue::String(value) | RunDimensionValue::Timestamp(value) => value.to_string(),
        RunDimensionValue::Numeric(value) => value.to_string(),
        RunDimensionValue::Boolean(value) => value.to_string(),
    }
}

fn project_status_output(
    status: &ProjectStatus,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = json!({
        "display_name": status.display_name,
        "project_root": status.project_root,
        "state_root": status.state_root,
        "frontier_count": status.frontier_count,
        "hypothesis_count": status.hypothesis_count,
        "experiment_count": status.experiment_count,
        "open_experiment_count": status.open_experiment_count,
        "artifact_count": status.artifact_count,
    });
    fallback_detailed_tool_output(
        &concise,
        status,
        [
            format!("project {}", status.display_name),
            format!("root: {}", status.project_root),
            format!("state: {}", status.state_root),
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
        libmcp::SurfaceKind::Overview,
        FaultStage::Worker,
        operation,
    )
}

fn tag_record_output(
    tag: &fidget_spinner_core::TagRecord,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::tag_record(tag);
    projected_tool_output(
        &projection,
        format!("tag {} — {}", tag.name, tag.description),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn tag_registry_output(
    registry: &fidget_spinner_core::TagRegistrySnapshot,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::tag_registry(registry);
    projected_tool_output(
        &projection,
        if registry.tags.is_empty() {
            "no tags".to_owned()
        } else {
            let mut lines = registry
                .tags
                .iter()
                .map(|tag| {
                    let family = tag
                        .family
                        .as_ref()
                        .map_or(String::new(), |family| format!(" [{family}]"));
                    format!("{}{} — {}", tag.name, family, tag.description)
                })
                .collect::<Vec<_>>();
            for lock in &registry.locks {
                lines.push(format!(
                    "LOCKED {}:{} — {}",
                    lock.registry,
                    lock.mode.as_str(),
                    lock.reason
                ));
            }
            for family in registry.families.iter().filter(|family| family.mandatory) {
                lines.push(format!("mandatory family {} is active", family.name));
            }
            lines.join("\n")
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
    let projection = projection::frontier_list(frontiers);
    projected_tool_output(
        &projection,
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
    store: &ProjectStore,
    frontier: &fidget_spinner_core::FrontierRecord,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::frontier_record(store, frontier, operation)?;
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
    if !frontier.brief.scoreboard_metric_keys.is_empty() {
        lines.push(format!(
            "scoreboard metrics: {}",
            frontier
                .brief
                .scoreboard_metric_keys
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    projected_tool_output(
        &projection,
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
    let output_projection = projection::frontier_open(projection);
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
    if !projection.scoreboard_metric_keys.is_empty() {
        lines.push(format!(
            "scoreboard metrics: {}",
            projection
                .scoreboard_metric_keys
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
    projected_tool_output(
        &output_projection,
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
    let projection = projection::hypothesis_record(hypothesis);
    projected_tool_output(
        &projection,
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
    let projection = projection::hypothesis_list(hypotheses);
    projected_tool_output(
        &projection,
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
    store: &ProjectStore,
    detail: &fidget_spinner_store_sqlite::HypothesisDetail,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::hypothesis_detail(store, detail, operation)?;
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
    projected_tool_output(
        &projection,
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
    let projection = projection::experiment_record(experiment);
    let mut line = format!("experiment {} — {}", experiment.slug, experiment.title);
    if let Some(outcome) = experiment.outcome.as_ref() {
        let _ = write!(
            line,
            " | {} {}={}",
            outcome.verdict.as_str(),
            outcome.primary_metric.key,
            outcome.primary_metric.value
        );
        if let Some(commit_hash) = outcome.commit_hash.as_ref() {
            let _ = write!(
                line,
                " @{}",
                &commit_hash.as_str()[..commit_hash.as_str().len().min(12)]
            );
        }
    } else {
        let _ = write!(line, " | open");
    }
    projected_tool_output(&projection, line, None, FaultStage::Worker, operation)
}

fn experiment_list_output(
    experiments: &[fidget_spinner_store_sqlite::ExperimentSummary],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::experiment_list(experiments);
    projected_tool_output(
        &projection,
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
    store: &ProjectStore,
    detail: &fidget_spinner_store_sqlite::ExperimentDetail,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::experiment_detail(store, detail, operation)?;
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
        if let Some(commit_hash) = outcome.commit_hash.as_ref() {
            lines.push(format!("commit: {commit_hash}"));
        }
        lines.push(format!("rationale: {}", outcome.rationale));
    }
    lines.push(format!(
        "parents: {} | children: {} | artifacts: {}",
        detail.parents.len(),
        detail.children.len(),
        detail.artifacts.len()
    ));
    projected_tool_output(
        &projection,
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
    let projection = projection::artifact_record(artifact);
    projected_tool_output(
        &projection,
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
    let projection = projection::artifact_list(artifacts);
    projected_tool_output(
        &projection,
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
    store: &ProjectStore,
    detail: &fidget_spinner_store_sqlite::ArtifactDetail,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::artifact_detail(store, detail, operation)?;
    let mut lines = vec![format!(
        "artifact {} — {} -> {}",
        detail.record.slug, detail.record.label, detail.record.locator
    )];
    if !detail.attachments.is_empty() {
        lines.push(format!("attachments: {}", detail.attachments.len()));
    }
    projected_tool_output(
        &projection,
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
    let projection = projection::metric_keys(keys);
    projected_tool_output(
        &projection,
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

fn metric_definition_output(
    metric: &fidget_spinner_core::MetricDefinition,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::metric_definition(metric);
    projected_tool_output(
        &projection,
        format!(
            "metric {} [{} {} {}]",
            metric.key,
            metric.unit.as_str(),
            metric.objective.as_str(),
            metric.visibility.as_str()
        ),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn metric_best_output(
    entries: &[MetricBestEntry],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::metric_best(entries);
    projected_tool_output(
        &projection,
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

fn experiment_nearest_output(
    result: &fidget_spinner_store_sqlite::ExperimentNearestResult,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::experiment_nearest(result);
    let mut lines = Vec::new();
    if !result.target_dimensions.is_empty() {
        lines.push(format!(
            "target slice: {}",
            result
                .target_dimensions
                .iter()
                .map(|(key, value)| format!("{key}={}", run_dimension_value_text(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(metric) = result.metric.as_ref() {
        lines.push(format!(
            "champion metric: {} [{} {}]",
            metric.key,
            metric.unit.as_str(),
            metric.objective.as_str()
        ));
    }
    for (label, hit) in [
        ("accepted", result.accepted.as_ref()),
        ("kept", result.kept.as_ref()),
        ("rejected", result.rejected.as_ref()),
        ("champion", result.champion.as_ref()),
    ] {
        if let Some(hit) = hit {
            let suffix = hit
                .metric_value
                .as_ref()
                .map_or_else(String::new, |metric| {
                    format!(" | {}={}", metric.key, metric.value)
                });
            lines.push(format!(
                "{}: {} / {}{}",
                label, hit.experiment.slug, hit.hypothesis.slug, suffix
            ));
            lines.push(format!(
                "  why: {}",
                hit.reasons
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
    }
    projected_tool_output(
        &projection,
        if lines.is_empty() {
            "no comparator candidates".to_owned()
        } else {
            lines.join("\n")
        },
        None,
        FaultStage::Worker,
        operation,
    )
}

fn run_dimension_definition_output(
    dimension: &fidget_spinner_core::RunDimensionDefinition,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::run_dimension_definition(dimension);
    projected_tool_output(
        &projection,
        format!(
            "dimension {} [{}]",
            dimension.key,
            dimension.value_type.as_str()
        ),
        None,
        FaultStage::Worker,
        operation,
    )
}

fn run_dimension_list_output(
    dimensions: &[fidget_spinner_core::RunDimensionDefinition],
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = projection::run_dimension_list(dimensions);
    projected_tool_output(
        &projection,
        if dimensions.is_empty() {
            "no run dimensions".to_owned()
        } else {
            dimensions
                .iter()
                .map(|dimension| {
                    format!(
                        "{} [{}]{}",
                        dimension.key,
                        dimension.value_type.as_str(),
                        dimension
                            .description
                            .as_ref()
                            .map_or_else(String::new, |description| format!(" — {description}"))
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
    let projection = projection::history(history);
    projected_tool_output(
        &projection,
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

#[allow(
    dead_code,
    reason = "replaced by typed projection structs in crate::mcp::projection"
)]
#[allow(
    clippy::wildcard_imports,
    reason = "legacy helpers are quarantined pending full purge"
)]
mod legacy_projection_values {
    use super::*;

    fn frontier_summary_value(frontier: &FrontierSummary) -> Value {
        json!({
            "slug": frontier.slug,
            "label": frontier.label,
            "objective": frontier.objective,
            "status": frontier.status,
            "active_hypothesis_count": frontier.active_hypothesis_count,
            "open_experiment_count": frontier.open_experiment_count,
            "updated_at": timestamp_value(frontier.updated_at),
        })
    }

    fn frontier_record_value(
        store: &ProjectStore,
        frontier: &fidget_spinner_core::FrontierRecord,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        let roadmap = frontier
            .brief
            .roadmap
            .iter()
            .map(|item| {
                let hypothesis = store
                    .read_hypothesis(&item.hypothesis_id.to_string())
                    .map_err(store_fault(operation))?;
                Ok(json!({
                    "rank": item.rank,
                    "hypothesis": {
                        "slug": hypothesis.record.slug,
                        "title": hypothesis.record.title,
                        "summary": hypothesis.record.summary,
                    },
                    "summary": item.summary,
                }))
            })
            .collect::<Result<Vec<_>, FaultRecord>>()?;
        Ok(json!({
            "record": {
                "slug": frontier.slug,
                "label": frontier.label,
                "objective": frontier.objective,
                "status": frontier.status,
                "revision": frontier.revision,
                "created_at": timestamp_value(frontier.created_at),
                "updated_at": timestamp_value(frontier.updated_at),
                "brief": {
                    "situation": frontier.brief.situation,
                    "roadmap": roadmap,
                    "unknowns": frontier.brief.unknowns,
                    "scoreboard_metric_keys": frontier.brief.scoreboard_metric_keys,
                    "revision": frontier.brief.revision,
                    "updated_at": frontier.brief.updated_at.map(timestamp_value),
                },
            }
        }))
    }

    fn frontier_open_value(projection: &FrontierOpenProjection) -> Value {
        let roadmap = projection
            .frontier
            .brief
            .roadmap
            .iter()
            .map(|item| {
                let hypothesis = projection
                    .active_hypotheses
                    .iter()
                    .find(|state| state.hypothesis.id == item.hypothesis_id)
                    .map(|state| {
                        json!({
                            "slug": state.hypothesis.slug,
                            "title": state.hypothesis.title,
                            "summary": state.hypothesis.summary,
                        })
                    });
                json!({
                    "rank": item.rank,
                    "hypothesis": hypothesis,
                    "summary": item.summary,
                })
            })
            .collect::<Vec<_>>();
        json!({
            "frontier": {
                "slug": projection.frontier.slug,
                "label": projection.frontier.label,
                "objective": projection.frontier.objective,
                "status": projection.frontier.status,
                "revision": projection.frontier.revision,
                "created_at": timestamp_value(projection.frontier.created_at),
                "updated_at": timestamp_value(projection.frontier.updated_at),
                "brief": {
                    "situation": projection.frontier.brief.situation,
                    "roadmap": roadmap,
                    "unknowns": projection.frontier.brief.unknowns,
                    "scoreboard_metric_keys": projection.frontier.brief.scoreboard_metric_keys,
                    "revision": projection.frontier.brief.revision,
                    "updated_at": projection.frontier.brief.updated_at.map(timestamp_value),
                },
            },
            "active_tags": projection.active_tags,
            "scoreboard_metrics": projection
                .scoreboard_metric_keys
                .iter()
                .map(metric_key_summary_value)
                .collect::<Vec<_>>(),
            "active_metric_keys": projection
                .active_metric_keys
                .iter()
                .map(metric_key_summary_value)
                .collect::<Vec<_>>(),
            "active_hypotheses": projection
                .active_hypotheses
                .iter()
                .map(hypothesis_current_state_value)
                .collect::<Vec<_>>(),
            "open_experiments": projection
                .open_experiments
                .iter()
                .map(experiment_summary_value)
                .collect::<Vec<_>>(),
        })
    }

    fn hypothesis_summary_value(
        hypothesis: &fidget_spinner_store_sqlite::HypothesisSummary,
    ) -> Value {
        json!({
            "slug": hypothesis.slug,
            "archived": hypothesis.archived,
            "title": hypothesis.title,
            "summary": hypothesis.summary,
            "tags": hypothesis.tags,
            "open_experiment_count": hypothesis.open_experiment_count,
            "latest_verdict": hypothesis.latest_verdict,
            "updated_at": timestamp_value(hypothesis.updated_at),
        })
    }

    fn hypothesis_record_value(hypothesis: &fidget_spinner_core::HypothesisRecord) -> Value {
        json!({
            "slug": hypothesis.slug,
            "archived": hypothesis.archived,
            "title": hypothesis.title,
            "summary": hypothesis.summary,
            "body": hypothesis.body,
            "tags": hypothesis.tags,
            "revision": hypothesis.revision,
            "created_at": timestamp_value(hypothesis.created_at),
            "updated_at": timestamp_value(hypothesis.updated_at),
        })
    }

    fn hypothesis_detail_concise_value(
        store: &ProjectStore,
        detail: &fidget_spinner_store_sqlite::HypothesisDetail,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        let frontier = store
            .read_frontier(&detail.record.frontier_id.to_string())
            .map_err(store_fault(operation))?;
        Ok(json!({
            "record": {
                "slug": detail.record.slug,
                "archived": detail.record.archived,
                "title": detail.record.title,
                "summary": detail.record.summary,
                "tags": detail.record.tags,
                "revision": detail.record.revision,
                "updated_at": timestamp_value(detail.record.updated_at),
            },
            "frontier": {
                "slug": frontier.slug,
                "label": frontier.label,
                "status": frontier.status,
            },
            "parents": detail.parents.len(),
            "children": detail.children.len(),
            "open_experiments": detail
                .open_experiments
                .iter()
                .map(experiment_summary_value)
                .collect::<Vec<_>>(),
            "latest_closed_experiment": detail
                .closed_experiments
                .first()
                .map(experiment_summary_value),
            "artifact_count": detail.artifacts.len(),
        }))
    }

    fn hypothesis_detail_full_value(
        store: &ProjectStore,
        detail: &fidget_spinner_store_sqlite::HypothesisDetail,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        let frontier = store
            .read_frontier(&detail.record.frontier_id.to_string())
            .map_err(store_fault(operation))?;
        Ok(json!({
            "record": hypothesis_record_value(&detail.record),
            "frontier": {
                "slug": frontier.slug,
                "label": frontier.label,
                "status": frontier.status,
            },
            "parents": detail.parents.iter().map(vertex_summary_value).collect::<Vec<_>>(),
            "children": detail.children.iter().map(vertex_summary_value).collect::<Vec<_>>(),
            "open_experiments": detail
                .open_experiments
                .iter()
                .map(experiment_summary_value)
                .collect::<Vec<_>>(),
            "closed_experiments": detail
                .closed_experiments
                .iter()
                .map(experiment_summary_value)
                .collect::<Vec<_>>(),
            "artifacts": detail.artifacts.iter().map(artifact_summary_value).collect::<Vec<_>>(),
        }))
    }

    fn experiment_summary_value(
        experiment: &fidget_spinner_store_sqlite::ExperimentSummary,
    ) -> Value {
        json!({
            "slug": experiment.slug,
            "archived": experiment.archived,
            "title": experiment.title,
            "summary": experiment.summary,
            "tags": experiment.tags,
            "status": experiment.status,
            "verdict": experiment.verdict,
            "primary_metric": experiment
                .primary_metric
                .as_ref()
                .map(metric_observation_summary_value),
            "updated_at": timestamp_value(experiment.updated_at),
            "closed_at": experiment.closed_at.map(timestamp_value),
        })
    }

    fn experiment_record_value(experiment: &fidget_spinner_core::ExperimentRecord) -> Value {
        json!({
            "slug": experiment.slug,
            "archived": experiment.archived,
            "title": experiment.title,
            "summary": experiment.summary,
            "tags": experiment.tags,
            "status": experiment.status,
            "outcome": experiment.outcome.as_ref().map(experiment_outcome_value),
            "revision": experiment.revision,
            "created_at": timestamp_value(experiment.created_at),
            "updated_at": timestamp_value(experiment.updated_at),
        })
    }

    fn experiment_detail_concise_value(
        store: &ProjectStore,
        detail: &fidget_spinner_store_sqlite::ExperimentDetail,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        let frontier = store
            .read_frontier(&detail.record.frontier_id.to_string())
            .map_err(store_fault(operation))?;
        Ok(json!({
            "record": {
                "slug": detail.record.slug,
                "archived": detail.record.archived,
                "title": detail.record.title,
                "summary": detail.record.summary,
                "tags": detail.record.tags,
                "status": detail.record.status,
                "verdict": detail.record.outcome.as_ref().map(|outcome| outcome.verdict),
                "revision": detail.record.revision,
                "updated_at": timestamp_value(detail.record.updated_at),
            },
            "frontier": {
                "slug": frontier.slug,
                "label": frontier.label,
                "status": frontier.status,
            },
            "owning_hypothesis": hypothesis_summary_value(&detail.owning_hypothesis),
            "parents": detail.parents.len(),
            "children": detail.children.len(),
            "artifact_count": detail.artifacts.len(),
            "outcome": detail.record.outcome.as_ref().map(experiment_outcome_value),
        }))
    }

    fn experiment_detail_full_value(
        store: &ProjectStore,
        detail: &fidget_spinner_store_sqlite::ExperimentDetail,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        let frontier = store
            .read_frontier(&detail.record.frontier_id.to_string())
            .map_err(store_fault(operation))?;
        Ok(json!({
            "record": experiment_record_value(&detail.record),
            "frontier": {
                "slug": frontier.slug,
                "label": frontier.label,
                "status": frontier.status,
            },
            "owning_hypothesis": hypothesis_summary_value(&detail.owning_hypothesis),
            "parents": detail.parents.iter().map(vertex_summary_value).collect::<Vec<_>>(),
            "children": detail.children.iter().map(vertex_summary_value).collect::<Vec<_>>(),
            "artifacts": detail.artifacts.iter().map(artifact_summary_value).collect::<Vec<_>>(),
        }))
    }

    fn artifact_summary_value(artifact: &fidget_spinner_store_sqlite::ArtifactSummary) -> Value {
        json!({
            "slug": artifact.slug,
            "kind": artifact.kind,
            "label": artifact.label,
            "summary": artifact.summary,
            "locator": artifact.locator,
            "media_type": artifact.media_type,
            "updated_at": timestamp_value(artifact.updated_at),
        })
    }

    fn artifact_record_value(artifact: &fidget_spinner_core::ArtifactRecord) -> Value {
        json!({
            "slug": artifact.slug,
            "kind": artifact.kind,
            "label": artifact.label,
            "summary": artifact.summary,
            "locator": artifact.locator,
            "media_type": artifact.media_type,
            "revision": artifact.revision,
            "created_at": timestamp_value(artifact.created_at),
            "updated_at": timestamp_value(artifact.updated_at),
        })
    }

    fn artifact_detail_concise_value(
        detail: &fidget_spinner_store_sqlite::ArtifactDetail,
    ) -> Value {
        json!({
            "record": {
                "slug": detail.record.slug,
                "kind": detail.record.kind,
                "label": detail.record.label,
                "summary": detail.record.summary,
                "locator": detail.record.locator,
                "media_type": detail.record.media_type,
                "revision": detail.record.revision,
                "updated_at": timestamp_value(detail.record.updated_at),
            },
            "attachment_count": detail.attachments.len(),
        })
    }

    fn artifact_detail_full_value(
        store: &ProjectStore,
        detail: &fidget_spinner_store_sqlite::ArtifactDetail,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        let attachments = detail
            .attachments
            .iter()
            .copied()
            .map(|attachment| attachment_target_value(store, attachment, operation))
            .collect::<Result<Vec<_>, FaultRecord>>()?;
        Ok(json!({
            "record": artifact_record_value(&detail.record),
            "attachments": attachments,
        }))
    }

    fn hypothesis_current_state_value(
        state: &fidget_spinner_store_sqlite::HypothesisCurrentState,
    ) -> Value {
        json!({
            "hypothesis": hypothesis_summary_value(&state.hypothesis),
            "open_experiments": state
                .open_experiments
                .iter()
                .map(experiment_summary_value)
                .collect::<Vec<_>>(),
            "latest_closed_experiment": state
                .latest_closed_experiment
                .as_ref()
                .map(experiment_summary_value),
        })
    }

    fn metric_key_summary_value(metric: &MetricKeySummary) -> Value {
        json!({
            "key": metric.key,
            "unit": metric.unit,
            "objective": metric.objective,
            "visibility": metric.visibility,
            "description": metric.description,
            "reference_count": metric.reference_count,
        })
    }

    fn metric_best_entry_value(entry: &MetricBestEntry) -> Value {
        json!({
            "experiment": experiment_summary_value(&entry.experiment),
            "hypothesis": hypothesis_summary_value(&entry.hypothesis),
            "value": entry.value,
            "dimensions": dimension_map_value(&entry.dimensions),
        })
    }

    fn metric_observation_summary_value(
        metric: &fidget_spinner_store_sqlite::MetricObservationSummary,
    ) -> Value {
        json!({
            "key": metric.key,
            "value": metric.value,
            "unit": metric.unit,
            "objective": metric.objective,
        })
    }

    fn experiment_outcome_value(outcome: &fidget_spinner_core::ExperimentOutcome) -> Value {
        json!({
            "backend": outcome.backend,
            "command": command_recipe_value(&outcome.command),
            "dimensions": dimension_map_value(&outcome.dimensions),
            "primary_metric": metric_value_value(&outcome.primary_metric),
            "supporting_metrics": outcome
                .supporting_metrics
                .iter()
                .map(metric_value_value)
                .collect::<Vec<_>>(),
            "verdict": outcome.verdict,
            "rationale": outcome.rationale,
            "analysis": outcome.analysis.as_ref().map(experiment_analysis_value),
            "commit_hash": outcome.commit_hash.as_ref().map(ToString::to_string),
            "closed_at": timestamp_value(outcome.closed_at),
        })
    }

    fn experiment_analysis_value(analysis: &ExperimentAnalysis) -> Value {
        json!({
            "summary": analysis.summary,
            "body": analysis.body,
        })
    }

    fn metric_value_value(metric: &fidget_spinner_core::MetricValue) -> Value {
        json!({
            "key": metric.key,
            "value": metric.value,
        })
    }

    fn command_recipe_value(command: &CommandRecipe) -> Value {
        json!({
            "argv": command.argv,
            "working_directory": command.working_directory,
            "env": command.env,
        })
    }

    fn dimension_map_value(dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>) -> Value {
        let mut object = Map::new();
        for (key, value) in dimensions {
            let _ = object.insert(key.to_string(), run_dimension_value(value));
        }
        Value::Object(object)
    }

    fn run_dimension_value(value: &RunDimensionValue) -> Value {
        match value {
            RunDimensionValue::String(value) => Value::String(value.to_string()),
            RunDimensionValue::Numeric(value) => json!(value),
            RunDimensionValue::Boolean(value) => json!(value),
            RunDimensionValue::Timestamp(value) => Value::String(value.to_string()),
        }
    }

    fn vertex_summary_value(vertex: &VertexSummary) -> Value {
        json!({
            "kind": vertex.vertex.kind().as_str(),
            "slug": vertex.slug,
            "archived": vertex.archived,
            "title": vertex.title,
            "summary": vertex.summary,
            "updated_at": timestamp_value(vertex.updated_at),
        })
    }

    fn attachment_target_value(
        store: &ProjectStore,
        attachment: AttachmentTargetRef,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        match attachment {
            AttachmentTargetRef::Frontier(id) => {
                let frontier = store
                    .read_frontier(&id.to_string())
                    .map_err(store_fault(operation))?;
                Ok(json!({
                    "kind": "frontier",
                    "slug": frontier.slug,
                    "label": frontier.label,
                    "status": frontier.status,
                }))
            }
            AttachmentTargetRef::Hypothesis(id) => {
                let hypothesis = store
                    .read_hypothesis(&id.to_string())
                    .map_err(store_fault(operation))?;
                Ok(json!({
                    "kind": "hypothesis",
                    "slug": hypothesis.record.slug,
                    "title": hypothesis.record.title,
                    "summary": hypothesis.record.summary,
                }))
            }
            AttachmentTargetRef::Experiment(id) => {
                let experiment = store
                    .read_experiment(&id.to_string())
                    .map_err(store_fault(operation))?;
                Ok(json!({
                    "kind": "experiment",
                    "slug": experiment.record.slug,
                    "title": experiment.record.title,
                    "summary": experiment.record.summary,
                }))
            }
        }
    }

    fn timestamp_value(timestamp: time::OffsetDateTime) -> String {
        timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| timestamp.unix_timestamp().to_string())
    }
}
