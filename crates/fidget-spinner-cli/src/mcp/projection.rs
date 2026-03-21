use std::collections::BTreeMap;

use fidget_spinner_core::{
    AttachmentTargetRef, CommandRecipe, ExperimentAnalysis, ExperimentOutcome, FrontierBrief,
    FrontierRecord, MetricValue, NonEmptyText, RunDimensionValue,
};
use fidget_spinner_store_sqlite::{
    ArtifactDetail, ArtifactSummary, ExperimentDetail, ExperimentSummary, FrontierOpenProjection,
    FrontierSummary, HypothesisCurrentState, HypothesisDetail, MetricBestEntry, MetricKeySummary,
    MetricObservationSummary, ProjectStore, StoreError, VertexSummary,
};
use libmcp::{
    ProjectionError, SelectorProjection, StructuredProjection, SurfaceKind, SurfacePolicy,
};
use serde::Serialize;
use serde_json::Value;

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};

#[derive(Clone, Serialize, libmcp::SelectorProjection)]
pub(crate) struct HypothesisSelector {
    pub(crate) slug: String,
    pub(crate) title: String,
}

#[derive(Clone, Serialize, libmcp::SelectorProjection)]
pub(crate) struct ExperimentSelector {
    pub(crate) slug: String,
    pub(crate) title: String,
}

#[derive(Clone, Serialize, libmcp::SelectorProjection)]
pub(crate) struct FrontierSelector {
    pub(crate) slug: String,
    #[libmcp(title)]
    pub(crate) label: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct FrontierSummaryProjection {
    pub(crate) slug: String,
    pub(crate) label: String,
    pub(crate) objective: String,
    pub(crate) status: String,
    pub(crate) active_hypothesis_count: u64,
    pub(crate) open_experiment_count: u64,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct FrontierBriefProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) situation: Option<String>,
    pub(crate) roadmap: Vec<RoadmapItemProjection>,
    pub(crate) unknowns: Vec<String>,
    pub(crate) revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at: Option<String>,
}

#[derive(Clone, Serialize)]
pub(crate) struct RoadmapItemProjection {
    pub(crate) rank: u32,
    pub(crate) hypothesis: Option<HypothesisRoadmapProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisRoadmapProjection {
    pub(crate) slug: String,
    pub(crate) title: String,
    pub(crate) summary: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct FrontierRecordProjection {
    pub(crate) slug: String,
    pub(crate) label: String,
    pub(crate) objective: String,
    pub(crate) status: String,
    pub(crate) revision: u64,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) brief: FrontierBriefProjection,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "read")]
pub(crate) struct FrontierReadOutput {
    pub(crate) record: FrontierRecordProjection,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "list")]
pub(crate) struct FrontierListOutput {
    pub(crate) count: usize,
    pub(crate) frontiers: Vec<FrontierSummaryProjection>,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "overview")]
pub(crate) struct FrontierOpenOutput {
    pub(crate) frontier: FrontierOpenFrontierProjection,
    pub(crate) active_tags: Vec<String>,
    pub(crate) active_metric_keys: Vec<MetricKeySummaryProjection>,
    pub(crate) active_hypotheses: Vec<HypothesisCurrentStateProjection>,
    pub(crate) open_experiments: Vec<ExperimentSummaryProjection>,
}

#[derive(Clone, Serialize)]
pub(crate) struct FrontierOpenFrontierProjection {
    pub(crate) slug: String,
    pub(crate) label: String,
    pub(crate) objective: String,
    pub(crate) status: String,
    pub(crate) brief: FrontierBriefProjection,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisSummaryProjection {
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    pub(crate) summary: String,
    pub(crate) tags: Vec<String>,
    pub(crate) open_experiment_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) latest_verdict: Option<String>,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisRecordProjection {
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    pub(crate) summary: String,
    pub(crate) body: String,
    pub(crate) tags: Vec<String>,
    pub(crate) revision: u64,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisReadRecordProjection {
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    pub(crate) summary: String,
    pub(crate) tags: Vec<String>,
    pub(crate) revision: u64,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct FrontierLinkProjection {
    pub(crate) slug: String,
    pub(crate) label: String,
    pub(crate) status: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisDetailConcise {
    pub(crate) record: HypothesisReadRecordProjection,
    pub(crate) frontier: FrontierLinkProjection,
    pub(crate) parents: usize,
    pub(crate) children: usize,
    pub(crate) open_experiments: Vec<ExperimentSummaryProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) latest_closed_experiment: Option<ExperimentSummaryProjection>,
    pub(crate) artifact_count: usize,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisDetailFull {
    pub(crate) record: HypothesisRecordProjection,
    pub(crate) frontier: FrontierLinkProjection,
    pub(crate) parents: Vec<VertexSummaryProjection>,
    pub(crate) children: Vec<VertexSummaryProjection>,
    pub(crate) open_experiments: Vec<ExperimentSummaryProjection>,
    pub(crate) closed_experiments: Vec<ExperimentSummaryProjection>,
    pub(crate) artifacts: Vec<ArtifactSummaryProjection>,
}

pub(crate) struct HypothesisDetailOutput {
    concise: HypothesisDetailConcise,
    full: HypothesisDetailFull,
}

impl StructuredProjection for HypothesisDetailOutput {
    fn concise_projection(&self) -> Result<Value, ProjectionError> {
        Ok(serde_json::to_value(&self.concise)?)
    }

    fn full_projection(&self) -> Result<Value, ProjectionError> {
        Ok(serde_json::to_value(&self.full)?)
    }
}

impl SurfacePolicy for HypothesisDetailOutput {
    const KIND: SurfaceKind = SurfaceKind::Read;
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "mutation")]
pub(crate) struct HypothesisRecordOutput {
    pub(crate) record: HypothesisRecordProjection,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "list")]
pub(crate) struct HypothesisListOutput {
    pub(crate) count: usize,
    pub(crate) hypotheses: Vec<HypothesisSummaryProjection>,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentSummaryProjection {
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) verdict: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_metric: Option<MetricObservationSummaryProjection>,
    pub(crate) updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) closed_at: Option<String>,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentRecordProjection {
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) outcome: Option<ExperimentOutcomeProjection>,
    pub(crate) revision: u64,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentReadRecordProjection {
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) verdict: Option<String>,
    pub(crate) revision: u64,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentDetailConcise {
    pub(crate) record: ExperimentReadRecordProjection,
    pub(crate) frontier: FrontierLinkProjection,
    pub(crate) owning_hypothesis: HypothesisSummaryProjection,
    pub(crate) parents: usize,
    pub(crate) children: usize,
    pub(crate) artifact_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) outcome: Option<ExperimentOutcomeProjection>,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentDetailFull {
    pub(crate) record: ExperimentRecordProjection,
    pub(crate) frontier: FrontierLinkProjection,
    pub(crate) owning_hypothesis: HypothesisSummaryProjection,
    pub(crate) parents: Vec<VertexSummaryProjection>,
    pub(crate) children: Vec<VertexSummaryProjection>,
    pub(crate) artifacts: Vec<ArtifactSummaryProjection>,
}

pub(crate) struct ExperimentDetailOutput {
    concise: ExperimentDetailConcise,
    full: ExperimentDetailFull,
}

impl StructuredProjection for ExperimentDetailOutput {
    fn concise_projection(&self) -> Result<Value, ProjectionError> {
        Ok(serde_json::to_value(&self.concise)?)
    }

    fn full_projection(&self) -> Result<Value, ProjectionError> {
        Ok(serde_json::to_value(&self.full)?)
    }
}

impl SurfacePolicy for ExperimentDetailOutput {
    const KIND: SurfaceKind = SurfaceKind::Read;
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "mutation")]
pub(crate) struct ExperimentRecordOutput {
    pub(crate) record: ExperimentRecordProjection,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "list")]
pub(crate) struct ExperimentListOutput {
    pub(crate) count: usize,
    pub(crate) experiments: Vec<ExperimentSummaryProjection>,
}

#[derive(Clone, Serialize)]
pub(crate) struct ArtifactSummaryProjection {
    pub(crate) slug: String,
    pub(crate) kind: String,
    pub(crate) label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) locator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) media_type: Option<String>,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ArtifactRecordProjection {
    pub(crate) slug: String,
    pub(crate) kind: String,
    pub(crate) label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) locator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) media_type: Option<String>,
    pub(crate) revision: u64,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ArtifactReadRecordProjection {
    pub(crate) slug: String,
    pub(crate) kind: String,
    pub(crate) label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) locator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) media_type: Option<String>,
    pub(crate) revision: u64,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ArtifactDetailConcise {
    pub(crate) record: ArtifactReadRecordProjection,
    pub(crate) attachment_count: usize,
}

#[derive(Clone, Serialize)]
pub(crate) struct ArtifactDetailFull {
    pub(crate) record: ArtifactRecordProjection,
    pub(crate) attachments: Vec<AttachmentTargetProjection>,
}

pub(crate) struct ArtifactDetailOutput {
    concise: ArtifactDetailConcise,
    full: ArtifactDetailFull,
}

impl StructuredProjection for ArtifactDetailOutput {
    fn concise_projection(&self) -> Result<Value, ProjectionError> {
        Ok(serde_json::to_value(&self.concise)?)
    }

    fn full_projection(&self) -> Result<Value, ProjectionError> {
        Ok(serde_json::to_value(&self.full)?)
    }
}

impl SurfacePolicy for ArtifactDetailOutput {
    const KIND: SurfaceKind = SurfaceKind::Read;
    const REFERENCE_ONLY: bool = true;
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "mutation", reference_only)]
pub(crate) struct ArtifactRecordOutput {
    pub(crate) record: ArtifactRecordProjection,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "list", reference_only)]
pub(crate) struct ArtifactListOutput {
    pub(crate) count: usize,
    pub(crate) artifacts: Vec<ArtifactSummaryProjection>,
}

#[derive(Clone, Serialize)]
pub(crate) struct HypothesisCurrentStateProjection {
    pub(crate) hypothesis: HypothesisSummaryProjection,
    pub(crate) open_experiments: Vec<ExperimentSummaryProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) latest_closed_experiment: Option<ExperimentSummaryProjection>,
}

#[derive(Clone, Serialize)]
pub(crate) struct MetricKeySummaryProjection {
    pub(crate) key: String,
    pub(crate) unit: String,
    pub(crate) objective: String,
    pub(crate) visibility: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    pub(crate) reference_count: u64,
}

#[derive(Clone, Serialize)]
pub(crate) struct MetricBestEntryProjection {
    pub(crate) experiment: ExperimentSummaryProjection,
    pub(crate) hypothesis: HypothesisSummaryProjection,
    pub(crate) value: f64,
    pub(crate) dimensions: BTreeMap<String, Value>,
}

#[derive(Clone, Serialize)]
pub(crate) struct MetricObservationSummaryProjection {
    pub(crate) key: String,
    pub(crate) value: f64,
    pub(crate) unit: String,
    pub(crate) objective: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentOutcomeProjection {
    pub(crate) backend: String,
    pub(crate) command: CommandRecipeProjection,
    pub(crate) dimensions: BTreeMap<String, Value>,
    pub(crate) primary_metric: MetricValueProjection,
    pub(crate) supporting_metrics: Vec<MetricValueProjection>,
    pub(crate) verdict: String,
    pub(crate) rationale: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) analysis: Option<ExperimentAnalysisProjection>,
    pub(crate) closed_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct ExperimentAnalysisProjection {
    pub(crate) summary: String,
    pub(crate) body: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct MetricValueProjection {
    pub(crate) key: String,
    pub(crate) value: f64,
}

#[derive(Clone, Serialize)]
pub(crate) struct CommandRecipeProjection {
    pub(crate) argv: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) working_directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) env: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Serialize)]
pub(crate) struct VertexSummaryProjection {
    pub(crate) kind: String,
    pub(crate) slug: String,
    pub(crate) archived: bool,
    pub(crate) title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    pub(crate) updated_at: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct AttachmentTargetProjection {
    pub(crate) kind: String,
    pub(crate) slug: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<String>,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "list")]
pub(crate) struct MetricKeysOutput {
    pub(crate) count: usize,
    pub(crate) metrics: Vec<MetricKeySummaryProjection>,
}

#[derive(Clone, Serialize, libmcp::ToolProjection)]
#[libmcp(kind = "list")]
pub(crate) struct MetricBestOutput {
    pub(crate) count: usize,
    pub(crate) entries: Vec<MetricBestEntryProjection>,
}

pub(crate) fn frontier_list(frontiers: &[FrontierSummary]) -> FrontierListOutput {
    FrontierListOutput {
        count: frontiers.len(),
        frontiers: frontiers.iter().map(frontier_summary).collect(),
    }
}

pub(crate) fn frontier_record(
    store: &ProjectStore,
    frontier: &FrontierRecord,
    operation: &str,
) -> Result<FrontierReadOutput, FaultRecord> {
    Ok(FrontierReadOutput {
        record: frontier_record_projection(store, frontier, operation)?,
    })
}

pub(crate) fn frontier_open(projection: &FrontierOpenProjection) -> FrontierOpenOutput {
    FrontierOpenOutput {
        frontier: FrontierOpenFrontierProjection {
            slug: projection.frontier.slug.to_string(),
            label: projection.frontier.label.to_string(),
            objective: projection.frontier.objective.to_string(),
            status: projection.frontier.status.as_str().to_owned(),
            brief: frontier_brief_projection(
                &projection.frontier.brief,
                projection
                    .frontier
                    .brief
                    .roadmap
                    .iter()
                    .map(|item| {
                        let hypothesis = projection
                            .active_hypotheses
                            .iter()
                            .find(|state| state.hypothesis.id == item.hypothesis_id)
                            .map(|state| HypothesisRoadmapProjection {
                                slug: state.hypothesis.slug.to_string(),
                                title: state.hypothesis.title.to_string(),
                                summary: state.hypothesis.summary.to_string(),
                            });
                        RoadmapItemProjection {
                            rank: item.rank,
                            hypothesis,
                            summary: item.summary.as_ref().map(ToString::to_string),
                        }
                    })
                    .collect(),
            ),
        },
        active_tags: projection
            .active_tags
            .iter()
            .map(ToString::to_string)
            .collect(),
        active_metric_keys: projection
            .active_metric_keys
            .iter()
            .map(metric_key_summary)
            .collect(),
        active_hypotheses: projection
            .active_hypotheses
            .iter()
            .map(hypothesis_current_state)
            .collect(),
        open_experiments: projection
            .open_experiments
            .iter()
            .map(experiment_summary)
            .collect(),
    }
}

pub(crate) fn hypothesis_record(
    hypothesis: &fidget_spinner_core::HypothesisRecord,
) -> HypothesisRecordOutput {
    HypothesisRecordOutput {
        record: hypothesis_record_projection(hypothesis),
    }
}

pub(crate) fn hypothesis_list(
    hypotheses: &[fidget_spinner_store_sqlite::HypothesisSummary],
) -> HypothesisListOutput {
    HypothesisListOutput {
        count: hypotheses.len(),
        hypotheses: hypotheses.iter().map(hypothesis_summary).collect(),
    }
}

pub(crate) fn hypothesis_detail(
    store: &ProjectStore,
    detail: &HypothesisDetail,
    operation: &str,
) -> Result<HypothesisDetailOutput, FaultRecord> {
    let frontier = store
        .read_frontier(&detail.record.frontier_id.to_string())
        .map_err(store_fault(operation))?;
    let frontier = FrontierLinkProjection {
        slug: frontier.slug.to_string(),
        label: frontier.label.to_string(),
        status: frontier.status.as_str().to_owned(),
    };
    Ok(HypothesisDetailOutput {
        concise: HypothesisDetailConcise {
            record: HypothesisReadRecordProjection {
                slug: detail.record.slug.to_string(),
                archived: detail.record.archived,
                title: detail.record.title.to_string(),
                summary: detail.record.summary.to_string(),
                tags: detail.record.tags.iter().map(ToString::to_string).collect(),
                revision: detail.record.revision,
                updated_at: timestamp_value(detail.record.updated_at),
            },
            frontier: frontier.clone(),
            parents: detail.parents.len(),
            children: detail.children.len(),
            open_experiments: detail
                .open_experiments
                .iter()
                .map(experiment_summary)
                .collect(),
            latest_closed_experiment: detail.closed_experiments.first().map(experiment_summary),
            artifact_count: detail.artifacts.len(),
        },
        full: HypothesisDetailFull {
            record: hypothesis_record_projection(&detail.record),
            frontier,
            parents: detail.parents.iter().map(vertex_summary).collect(),
            children: detail.children.iter().map(vertex_summary).collect(),
            open_experiments: detail
                .open_experiments
                .iter()
                .map(experiment_summary)
                .collect(),
            closed_experiments: detail
                .closed_experiments
                .iter()
                .map(experiment_summary)
                .collect(),
            artifacts: detail.artifacts.iter().map(artifact_summary).collect(),
        },
    })
}

pub(crate) fn experiment_record(
    experiment: &fidget_spinner_core::ExperimentRecord,
) -> ExperimentRecordOutput {
    ExperimentRecordOutput {
        record: experiment_record_projection(experiment),
    }
}

pub(crate) fn experiment_list(experiments: &[ExperimentSummary]) -> ExperimentListOutput {
    ExperimentListOutput {
        count: experiments.len(),
        experiments: experiments.iter().map(experiment_summary).collect(),
    }
}

pub(crate) fn experiment_detail(
    store: &ProjectStore,
    detail: &ExperimentDetail,
    operation: &str,
) -> Result<ExperimentDetailOutput, FaultRecord> {
    let frontier = store
        .read_frontier(&detail.record.frontier_id.to_string())
        .map_err(store_fault(operation))?;
    let frontier = FrontierLinkProjection {
        slug: frontier.slug.to_string(),
        label: frontier.label.to_string(),
        status: frontier.status.as_str().to_owned(),
    };
    Ok(ExperimentDetailOutput {
        concise: ExperimentDetailConcise {
            record: ExperimentReadRecordProjection {
                slug: detail.record.slug.to_string(),
                archived: detail.record.archived,
                title: detail.record.title.to_string(),
                summary: detail.record.summary.as_ref().map(ToString::to_string),
                tags: detail.record.tags.iter().map(ToString::to_string).collect(),
                status: detail.record.status.as_str().to_owned(),
                verdict: detail
                    .record
                    .outcome
                    .as_ref()
                    .map(|outcome| outcome.verdict.as_str().to_owned()),
                revision: detail.record.revision,
                updated_at: timestamp_value(detail.record.updated_at),
            },
            frontier: frontier.clone(),
            owning_hypothesis: hypothesis_summary(&detail.owning_hypothesis),
            parents: detail.parents.len(),
            children: detail.children.len(),
            artifact_count: detail.artifacts.len(),
            outcome: detail.record.outcome.as_ref().map(experiment_outcome),
        },
        full: ExperimentDetailFull {
            record: experiment_record_projection(&detail.record),
            frontier,
            owning_hypothesis: hypothesis_summary(&detail.owning_hypothesis),
            parents: detail.parents.iter().map(vertex_summary).collect(),
            children: detail.children.iter().map(vertex_summary).collect(),
            artifacts: detail.artifacts.iter().map(artifact_summary).collect(),
        },
    })
}

pub(crate) fn artifact_record(
    artifact: &fidget_spinner_core::ArtifactRecord,
) -> ArtifactRecordOutput {
    ArtifactRecordOutput {
        record: artifact_record_projection(artifact),
    }
}

pub(crate) fn artifact_list(artifacts: &[ArtifactSummary]) -> ArtifactListOutput {
    ArtifactListOutput {
        count: artifacts.len(),
        artifacts: artifacts.iter().map(artifact_summary).collect(),
    }
}

pub(crate) fn artifact_detail(
    store: &ProjectStore,
    detail: &ArtifactDetail,
    operation: &str,
) -> Result<ArtifactDetailOutput, FaultRecord> {
    let attachments = detail
        .attachments
        .iter()
        .copied()
        .map(|attachment| attachment_target(store, attachment, operation))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ArtifactDetailOutput {
        concise: ArtifactDetailConcise {
            record: ArtifactReadRecordProjection {
                slug: detail.record.slug.to_string(),
                kind: detail.record.kind.as_str().to_owned(),
                label: detail.record.label.to_string(),
                summary: detail.record.summary.as_ref().map(ToString::to_string),
                locator: detail.record.locator.to_string(),
                media_type: detail.record.media_type.as_ref().map(ToString::to_string),
                revision: detail.record.revision,
                updated_at: timestamp_value(detail.record.updated_at),
            },
            attachment_count: detail.attachments.len(),
        },
        full: ArtifactDetailFull {
            record: artifact_record_projection(&detail.record),
            attachments,
        },
    })
}

pub(crate) fn metric_keys(keys: &[MetricKeySummary]) -> MetricKeysOutput {
    MetricKeysOutput {
        count: keys.len(),
        metrics: keys.iter().map(metric_key_summary).collect(),
    }
}

pub(crate) fn metric_best(entries: &[MetricBestEntry]) -> MetricBestOutput {
    MetricBestOutput {
        count: entries.len(),
        entries: entries.iter().map(metric_best_entry).collect(),
    }
}

fn frontier_summary(frontier: &FrontierSummary) -> FrontierSummaryProjection {
    FrontierSummaryProjection {
        slug: frontier.slug.to_string(),
        label: frontier.label.to_string(),
        objective: frontier.objective.to_string(),
        status: frontier.status.as_str().to_owned(),
        active_hypothesis_count: frontier.active_hypothesis_count,
        open_experiment_count: frontier.open_experiment_count,
        updated_at: timestamp_value(frontier.updated_at),
    }
}

fn frontier_record_projection(
    store: &ProjectStore,
    frontier: &FrontierRecord,
    operation: &str,
) -> Result<FrontierRecordProjection, FaultRecord> {
    let roadmap = frontier
        .brief
        .roadmap
        .iter()
        .map(|item| {
            let hypothesis = store
                .read_hypothesis(&item.hypothesis_id.to_string())
                .map_err(store_fault(operation))?;
            Ok(RoadmapItemProjection {
                rank: item.rank,
                hypothesis: Some(HypothesisRoadmapProjection {
                    slug: hypothesis.record.slug.to_string(),
                    title: hypothesis.record.title.to_string(),
                    summary: hypothesis.record.summary.to_string(),
                }),
                summary: item.summary.as_ref().map(ToString::to_string),
            })
        })
        .collect::<Result<Vec<_>, FaultRecord>>()?;
    Ok(FrontierRecordProjection {
        slug: frontier.slug.to_string(),
        label: frontier.label.to_string(),
        objective: frontier.objective.to_string(),
        status: frontier.status.as_str().to_owned(),
        revision: frontier.revision,
        created_at: timestamp_value(frontier.created_at),
        updated_at: timestamp_value(frontier.updated_at),
        brief: frontier_brief_projection(&frontier.brief, roadmap),
    })
}

fn frontier_brief_projection(
    brief: &FrontierBrief,
    roadmap: Vec<RoadmapItemProjection>,
) -> FrontierBriefProjection {
    FrontierBriefProjection {
        situation: brief.situation.as_ref().map(ToString::to_string),
        roadmap,
        unknowns: brief.unknowns.iter().map(ToString::to_string).collect(),
        revision: brief.revision,
        updated_at: brief.updated_at.map(timestamp_value),
    }
}

fn hypothesis_summary(
    hypothesis: &fidget_spinner_store_sqlite::HypothesisSummary,
) -> HypothesisSummaryProjection {
    HypothesisSummaryProjection {
        slug: hypothesis.slug.to_string(),
        archived: hypothesis.archived,
        title: hypothesis.title.to_string(),
        summary: hypothesis.summary.to_string(),
        tags: hypothesis.tags.iter().map(ToString::to_string).collect(),
        open_experiment_count: hypothesis.open_experiment_count,
        latest_verdict: hypothesis
            .latest_verdict
            .map(|verdict| verdict.as_str().to_owned()),
        updated_at: timestamp_value(hypothesis.updated_at),
    }
}

fn hypothesis_record_projection(
    hypothesis: &fidget_spinner_core::HypothesisRecord,
) -> HypothesisRecordProjection {
    HypothesisRecordProjection {
        slug: hypothesis.slug.to_string(),
        archived: hypothesis.archived,
        title: hypothesis.title.to_string(),
        summary: hypothesis.summary.to_string(),
        body: hypothesis.body.to_string(),
        tags: hypothesis.tags.iter().map(ToString::to_string).collect(),
        revision: hypothesis.revision,
        created_at: timestamp_value(hypothesis.created_at),
        updated_at: timestamp_value(hypothesis.updated_at),
    }
}

fn experiment_summary(experiment: &ExperimentSummary) -> ExperimentSummaryProjection {
    ExperimentSummaryProjection {
        slug: experiment.slug.to_string(),
        archived: experiment.archived,
        title: experiment.title.to_string(),
        summary: experiment.summary.as_ref().map(ToString::to_string),
        tags: experiment.tags.iter().map(ToString::to_string).collect(),
        status: experiment.status.as_str().to_owned(),
        verdict: experiment
            .verdict
            .map(|verdict| verdict.as_str().to_owned()),
        primary_metric: experiment
            .primary_metric
            .as_ref()
            .map(metric_observation_summary),
        updated_at: timestamp_value(experiment.updated_at),
        closed_at: experiment.closed_at.map(timestamp_value),
    }
}

fn experiment_record_projection(
    experiment: &fidget_spinner_core::ExperimentRecord,
) -> ExperimentRecordProjection {
    ExperimentRecordProjection {
        slug: experiment.slug.to_string(),
        archived: experiment.archived,
        title: experiment.title.to_string(),
        summary: experiment.summary.as_ref().map(ToString::to_string),
        tags: experiment.tags.iter().map(ToString::to_string).collect(),
        status: experiment.status.as_str().to_owned(),
        outcome: experiment.outcome.as_ref().map(experiment_outcome),
        revision: experiment.revision,
        created_at: timestamp_value(experiment.created_at),
        updated_at: timestamp_value(experiment.updated_at),
    }
}

fn artifact_summary(artifact: &ArtifactSummary) -> ArtifactSummaryProjection {
    ArtifactSummaryProjection {
        slug: artifact.slug.to_string(),
        kind: artifact.kind.as_str().to_owned(),
        label: artifact.label.to_string(),
        summary: artifact.summary.as_ref().map(ToString::to_string),
        locator: artifact.locator.to_string(),
        media_type: artifact.media_type.as_ref().map(ToString::to_string),
        updated_at: timestamp_value(artifact.updated_at),
    }
}

fn artifact_record_projection(
    artifact: &fidget_spinner_core::ArtifactRecord,
) -> ArtifactRecordProjection {
    ArtifactRecordProjection {
        slug: artifact.slug.to_string(),
        kind: artifact.kind.as_str().to_owned(),
        label: artifact.label.to_string(),
        summary: artifact.summary.as_ref().map(ToString::to_string),
        locator: artifact.locator.to_string(),
        media_type: artifact.media_type.as_ref().map(ToString::to_string),
        revision: artifact.revision,
        created_at: timestamp_value(artifact.created_at),
        updated_at: timestamp_value(artifact.updated_at),
    }
}

fn hypothesis_current_state(state: &HypothesisCurrentState) -> HypothesisCurrentStateProjection {
    HypothesisCurrentStateProjection {
        hypothesis: hypothesis_summary(&state.hypothesis),
        open_experiments: state
            .open_experiments
            .iter()
            .map(experiment_summary)
            .collect(),
        latest_closed_experiment: state
            .latest_closed_experiment
            .as_ref()
            .map(experiment_summary),
    }
}

fn metric_key_summary(metric: &MetricKeySummary) -> MetricKeySummaryProjection {
    MetricKeySummaryProjection {
        key: metric.key.to_string(),
        unit: metric.unit.as_str().to_owned(),
        objective: metric.objective.as_str().to_owned(),
        visibility: metric.visibility.as_str().to_owned(),
        description: metric.description.as_ref().map(ToString::to_string),
        reference_count: metric.reference_count,
    }
}

fn metric_best_entry(entry: &MetricBestEntry) -> MetricBestEntryProjection {
    MetricBestEntryProjection {
        experiment: experiment_summary(&entry.experiment),
        hypothesis: hypothesis_summary(&entry.hypothesis),
        value: entry.value,
        dimensions: dimension_map(&entry.dimensions),
    }
}

fn metric_observation_summary(
    metric: &MetricObservationSummary,
) -> MetricObservationSummaryProjection {
    MetricObservationSummaryProjection {
        key: metric.key.to_string(),
        value: metric.value,
        unit: metric.unit.as_str().to_owned(),
        objective: metric.objective.as_str().to_owned(),
    }
}

fn experiment_outcome(outcome: &ExperimentOutcome) -> ExperimentOutcomeProjection {
    ExperimentOutcomeProjection {
        backend: outcome.backend.as_str().to_owned(),
        command: command_recipe(&outcome.command),
        dimensions: dimension_map(&outcome.dimensions),
        primary_metric: metric_value(&outcome.primary_metric),
        supporting_metrics: outcome
            .supporting_metrics
            .iter()
            .map(metric_value)
            .collect(),
        verdict: outcome.verdict.as_str().to_owned(),
        rationale: outcome.rationale.to_string(),
        analysis: outcome.analysis.as_ref().map(experiment_analysis),
        closed_at: timestamp_value(outcome.closed_at),
    }
}

fn experiment_analysis(analysis: &ExperimentAnalysis) -> ExperimentAnalysisProjection {
    ExperimentAnalysisProjection {
        summary: analysis.summary.to_string(),
        body: analysis.body.to_string(),
    }
}

fn metric_value(metric: &MetricValue) -> MetricValueProjection {
    MetricValueProjection {
        key: metric.key.to_string(),
        value: metric.value,
    }
}

fn command_recipe(command: &CommandRecipe) -> CommandRecipeProjection {
    CommandRecipeProjection {
        argv: command.argv.iter().map(ToString::to_string).collect(),
        working_directory: command.working_directory.as_ref().map(ToString::to_string),
        env: (!command.env.is_empty()).then(|| {
            command
                .env
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        }),
    }
}

fn dimension_map(
    dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> BTreeMap<String, Value> {
    dimensions
        .iter()
        .map(|(key, value)| (key.to_string(), run_dimension_value(value)))
        .collect()
}

fn run_dimension_value(value: &RunDimensionValue) -> Value {
    match value {
        RunDimensionValue::String(value) => Value::String(value.to_string()),
        RunDimensionValue::Numeric(value) => serde_json::json!(value),
        RunDimensionValue::Boolean(value) => serde_json::json!(value),
        RunDimensionValue::Timestamp(value) => Value::String(value.to_string()),
    }
}

fn vertex_summary(vertex: &VertexSummary) -> VertexSummaryProjection {
    VertexSummaryProjection {
        kind: vertex.vertex.kind().as_str().to_owned(),
        slug: vertex.slug.to_string(),
        archived: vertex.archived,
        title: vertex.title.to_string(),
        summary: vertex.summary.as_ref().map(ToString::to_string),
        updated_at: timestamp_value(vertex.updated_at),
    }
}

fn attachment_target(
    store: &ProjectStore,
    attachment: AttachmentTargetRef,
    operation: &str,
) -> Result<AttachmentTargetProjection, FaultRecord> {
    match attachment {
        AttachmentTargetRef::Frontier(id) => {
            let frontier = store
                .read_frontier(&id.to_string())
                .map_err(store_fault(operation))?;
            let reference = FrontierSelector {
                slug: frontier.slug.to_string(),
                label: frontier.label.to_string(),
            };
            let selector = reference.selector_ref();
            Ok(AttachmentTargetProjection {
                kind: "frontier".to_owned(),
                slug: selector.slug,
                title: None,
                label: selector.title,
                summary: None,
                status: Some(frontier.status.as_str().to_owned()),
            })
        }
        AttachmentTargetRef::Hypothesis(id) => {
            let hypothesis = store
                .read_hypothesis(&id.to_string())
                .map_err(store_fault(operation))?;
            let reference = HypothesisSelector {
                slug: hypothesis.record.slug.to_string(),
                title: hypothesis.record.title.to_string(),
            };
            let selector = reference.selector_ref();
            Ok(AttachmentTargetProjection {
                kind: "hypothesis".to_owned(),
                slug: selector.slug,
                title: selector.title,
                label: None,
                summary: Some(hypothesis.record.summary.to_string()),
                status: None,
            })
        }
        AttachmentTargetRef::Experiment(id) => {
            let experiment = store
                .read_experiment(&id.to_string())
                .map_err(store_fault(operation))?;
            let reference = ExperimentSelector {
                slug: experiment.record.slug.to_string(),
                title: experiment.record.title.to_string(),
            };
            let selector = reference.selector_ref();
            Ok(AttachmentTargetProjection {
                kind: "experiment".to_owned(),
                slug: selector.slug,
                title: selector.title,
                label: None,
                summary: experiment.record.summary.as_ref().map(ToString::to_string),
                status: None,
            })
        }
    }
}

fn timestamp_value(timestamp: time::OffsetDateTime) -> String {
    timestamp
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| timestamp.unix_timestamp().to_string())
}

fn store_fault(operation: &str) -> impl Fn(StoreError) -> FaultRecord + '_ {
    move |error| {
        FaultRecord::new(
            FaultKind::Internal,
            FaultStage::Store,
            operation,
            error.to_string(),
        )
    }
}
