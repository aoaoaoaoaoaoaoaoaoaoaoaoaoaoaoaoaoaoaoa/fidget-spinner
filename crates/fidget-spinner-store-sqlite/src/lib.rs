use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::process::Command;
use std::sync::OnceLock;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    ArtifactId, ArtifactKind, ArtifactRecord, AttachmentTargetRef, CommandRecipe, CoreError,
    DefaultVisibility, ExecutionBackend, ExperimentAnalysis, ExperimentId, ExperimentOutcome,
    ExperimentRecord, ExperimentStatus, FieldValueType, FrontierBrief, FrontierId,
    FrontierKpiRecord, FrontierRecord, FrontierRoadmapItem, FrontierStatus, FrontierVerdict,
    GitCommitHash, HiddenByDefaultReason, HypothesisId, HypothesisRecord, KpiId,
    KpiMetricAlternativeRecord, MetricAggregation, MetricDefinition, MetricDimension, MetricId,
    MetricUnit, MetricValue, NonEmptyText, OptimizationObjective, RegistryLockId, RegistryLockMode,
    RegistryLockRecord, RegistryName, RunDimensionDefinition, RunDimensionValue, Slug, TagFamilyId,
    TagFamilyName, TagFamilyRecord, TagId, TagName, TagNameDisposition, TagNameHistoryRecord,
    TagRecord, TagRegistrySnapshot, TagStatus, VertexRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

pub const STORE_DIR_NAME: &str = ".fidget_spinner";
pub const GIT_DIR_NAME: &str = ".git";
pub const STATE_DB_NAME: &str = "state.sqlite";
pub const PROJECT_CONFIG_NAME: &str = "project.json";
pub const CURRENT_STORE_FORMAT_VERSION: u32 = 8;
pub const STATE_HOME_DIR_NAME: &str = "fidget-spinner";
pub const PROJECT_STATE_DIR_NAME: &str = "projects";
const PROJECT_ROOT_NAMESPACE: Uuid = Uuid::from_u128(0x0df3_58f4_3649_44f1_8f05_0bb2_4ebd_8d31);
static STATE_HOME_OVERRIDE: OnceLock<Utf8PathBuf> = OnceLock::new();

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("project store is not initialized at {0}")]
    MissingProjectStore(Utf8PathBuf),
    #[error("path `{path}` contains multiple descendant project stores: {candidates}")]
    AmbiguousProjectStoreDiscovery {
        path: Utf8PathBuf,
        candidates: String,
    },
    #[error("I/O failure")]
    Io(#[from] io::Error),
    #[error("SQLite failure")]
    Sql(#[from] rusqlite::Error),
    #[error("JSON failure")]
    Json(#[from] serde_json::Error),
    #[error("time parse failure")]
    TimeParse(#[from] time::error::Parse),
    #[error("time format failure")]
    TimeFormat(#[from] time::error::Format),
    #[error("core domain failure")]
    Core(#[from] CoreError),
    #[error("UUID parse failure")]
    Uuid(#[from] uuid::Error),
    #[error("{0}")]
    InvalidInput(String),
    #[error(
        "project store format {observed} is incompatible with this binary (expected {expected}); restart/upgrade the stale MCP binary if the store is newer, or run the manual store migration if the store is older"
    )]
    IncompatibleStoreFormatVersion { observed: u32, expected: u32 },
    #[error("unknown tag `{0}`")]
    UnknownTag(TagName),
    #[error("tag `{0}` already exists")]
    DuplicateTag(TagName),
    #[error("unknown tag family `{0}`")]
    UnknownTagFamily(TagFamilyName),
    #[error("tag family `{0}` already exists")]
    DuplicateTagFamily(TagFamilyName),
    #[error("{0}")]
    PolicyViolation(String),
    #[error("metric `{0}` is not registered")]
    UnknownMetricDefinition(NonEmptyText),
    #[error("KPI `{0}` is not registered")]
    UnknownKpi(String),
    #[error("metric `{0}` already exists")]
    DuplicateMetricDefinition(NonEmptyText),
    #[error("KPI `{0}` already exists")]
    DuplicateKpi(NonEmptyText),
    #[error("KPI `{kpi}` must reference at least one metric")]
    EmptyKpi { kpi: NonEmptyText },
    #[error("KPI `{kpi}` objective does not match metric `{metric}`")]
    KpiMetricObjectiveMismatch {
        kpi: NonEmptyText,
        metric: NonEmptyText,
    },
    #[error("KPI `{kpi}` dimension does not match metric `{metric}`")]
    KpiMetricDimensionMismatch {
        kpi: NonEmptyText,
        metric: NonEmptyText,
    },
    #[error("mandatory KPI `{kpi}` is missing; report one of: {metrics}")]
    MissingMandatoryKpi { kpi: NonEmptyText, metrics: String },
    #[error(
        "frontier `{frontier}` has no KPI contract; create at least one KPI before model enumeration or MCP experiment close"
    )]
    MissingFrontierKpiContract { frontier: String },
    #[error("run dimension `{0}` is not registered")]
    UnknownRunDimension(NonEmptyText),
    #[error("run dimension `{0}` already exists")]
    DuplicateRunDimension(NonEmptyText),
    #[error("frontier selector `{0}` did not resolve")]
    UnknownFrontierSelector(String),
    #[error("hypothesis selector `{0}` did not resolve")]
    UnknownHypothesisSelector(String),
    #[error("experiment selector `{0}` did not resolve")]
    UnknownExperimentSelector(String),
    #[error("artifact selector `{0}` did not resolve")]
    UnknownArtifactSelector(String),
    #[error(
        "entity revision mismatch for {kind} `{selector}`: expected {expected}, observed {observed}"
    )]
    RevisionMismatch {
        kind: &'static str,
        selector: String,
        expected: u64,
        observed: u64,
    },
    #[error("hypothesis body must be exactly one paragraph")]
    HypothesisBodyMustBeSingleParagraph,
    #[error("experiments must hang off exactly one hypothesis")]
    ExperimentHypothesisRequired,
    #[error("experiment `{0}` is already closed")]
    ExperimentAlreadyClosed(ExperimentId),
    #[error("experiment `{0}` is still open")]
    ExperimentStillOpen(ExperimentId),
    #[error(
        "closing an experiment requires a git worktree at `{0}` so Spinner can record a commit hash"
    )]
    GitWorktreeRequired(Utf8PathBuf),
    #[error(
        "closing an experiment requires a committed HEAD at `{0}`; make a fast commit before closing"
    )]
    GitHeadRequired(Utf8PathBuf),
    #[error(
        "closing an experiment requires a clean git worktree at `{project_root}`; make a fast commit before closing. Dirty entries:\n{status}"
    )]
    DirtyGitWorktree {
        project_root: Utf8PathBuf,
        status: String,
    },
    #[error("failed to spawn `{command}` while inspecting `{project_root}`: {source}")]
    GitSpawn {
        project_root: Utf8PathBuf,
        command: String,
        #[source]
        source: io::Error,
    },
    #[error("git inspection failed at `{project_root}` while running `{command}`: {stderr}")]
    GitCommandFailed {
        project_root: Utf8PathBuf,
        command: String,
        stderr: String,
    },
    #[error("influence edge crosses frontier scope")]
    CrossFrontierInfluence,
    #[error("self edges are not allowed")]
    SelfEdge,
    #[error("unknown roadmap hypothesis `{0}`")]
    UnknownRoadmapHypothesis(String),
    #[error(
        "manual experiments may omit command context only by using an empty argv surrogate explicitly"
    )]
    ManualExperimentRequiresCommand,
    #[error("metric key `{key}` requires an explicit ranking order")]
    MetricOrderRequired { key: String },
    #[error("dimension filter references unknown run dimension `{0}`")]
    UnknownDimensionFilter(String),
    #[error("metric scope `{scope}` requires a frontier selector")]
    MetricScopeRequiresFrontier { scope: &'static str },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectConfig {
    pub display_name: NonEmptyText,
    pub created_at: OffsetDateTime,
    pub store_format_version: u32,
}

impl ProjectConfig {
    #[must_use]
    pub fn new(display_name: NonEmptyText) -> Self {
        Self {
            display_name,
            created_at: OffsetDateTime::now_utc(),
            store_format_version: CURRENT_STORE_FORMAT_VERSION,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectStatus {
    pub project_root: Utf8PathBuf,
    pub state_root: Utf8PathBuf,
    pub display_name: NonEmptyText,
    pub store_format_version: u32,
    pub frontier_count: u64,
    pub hypothesis_count: u64,
    pub experiment_count: u64,
    pub open_experiment_count: u64,
    pub artifact_count: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricScope {
    Kpi,
    Live,
    Default,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationOrigin {
    Mcp,
    Supervisor,
}

impl MutationOrigin {
    #[must_use]
    pub const fn is_mcp(self) -> bool {
        matches!(self, Self::Mcp)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateTagFamilyRequest {
    pub name: TagFamilyName,
    pub description: NonEmptyText,
    pub mandatory: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameTagRequest {
    pub tag: TagName,
    pub expected_revision: Option<u64>,
    pub new_name: TagName,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MergeTagRequest {
    pub source: TagName,
    pub expected_revision: Option<u64>,
    pub target: TagName,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeleteTagRequest {
    pub tag: TagName,
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AssignTagFamilyRequest {
    pub tag: TagName,
    pub expected_revision: Option<u64>,
    pub family: Option<TagFamilyName>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SetTagFamilyMandatoryRequest {
    pub family: TagFamilyName,
    pub expected_revision: Option<u64>,
    pub mandatory: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SetRegistryLockRequest {
    pub registry: RegistryName,
    pub mode: RegistryLockMode,
    pub locked: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SetFrontierRegistryLockRequest {
    pub registry: RegistryName,
    pub mode: RegistryLockMode,
    pub frontier: String,
    pub locked: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TagRegistryQuery {
    pub include_hidden: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricRankOrder {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "selector", rename_all = "snake_case")]
pub enum VertexSelector {
    Hypothesis(String),
    Experiment(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "selector", rename_all = "snake_case")]
pub enum AttachmentSelector {
    Frontier(String),
    Hypothesis(String),
    Experiment(String),
}

#[derive(Clone, Debug, Default)]
pub struct ListFrontiersQuery {
    pub include_archived: bool,
}

#[derive(Clone, Debug)]
pub struct CreateFrontierRequest {
    pub label: NonEmptyText,
    pub objective: NonEmptyText,
    pub slug: Option<Slug>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierSummary {
    pub id: FrontierId,
    pub slug: Slug,
    pub label: NonEmptyText,
    pub objective: NonEmptyText,
    pub status: FrontierStatus,
    pub active_hypothesis_count: u64,
    pub open_experiment_count: u64,
    pub revision: u64,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierRoadmapItemDraft {
    pub rank: u32,
    pub hypothesis: String,
    pub summary: Option<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub enum TextPatch<T> {
    Set(T),
    Clear,
}

#[derive(Clone, Debug)]
pub struct UpdateFrontierRequest {
    pub frontier: String,
    pub expected_revision: Option<u64>,
    pub objective: Option<NonEmptyText>,
    pub status: Option<FrontierStatus>,
    pub situation: Option<TextPatch<NonEmptyText>>,
    pub roadmap: Option<Vec<FrontierRoadmapItemDraft>>,
    pub unknowns: Option<Vec<NonEmptyText>>,
}

#[derive(Clone, Debug)]
pub struct CreateHypothesisRequest {
    pub frontier: String,
    pub slug: Option<Slug>,
    pub title: NonEmptyText,
    pub summary: NonEmptyText,
    pub body: NonEmptyText,
    pub tags: BTreeSet<TagName>,
    pub parents: Vec<VertexSelector>,
}

#[derive(Clone, Debug)]
pub struct UpdateHypothesisRequest {
    pub hypothesis: String,
    pub expected_revision: Option<u64>,
    pub title: Option<NonEmptyText>,
    pub summary: Option<NonEmptyText>,
    pub body: Option<NonEmptyText>,
    pub tags: Option<BTreeSet<TagName>>,
    pub parents: Option<Vec<VertexSelector>>,
    pub archived: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct ListHypothesesQuery {
    pub frontier: Option<String>,
    pub tags: BTreeSet<TagName>,
    pub include_archived: bool,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VertexSummary {
    pub vertex: VertexRef,
    pub frontier_id: FrontierId,
    pub slug: Slug,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HypothesisSummary {
    pub id: HypothesisId,
    pub slug: Slug,
    pub frontier_id: FrontierId,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: NonEmptyText,
    pub tags: Vec<TagName>,
    pub open_experiment_count: u64,
    pub latest_verdict: Option<FrontierVerdict>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct HypothesisDetail {
    pub record: HypothesisRecord,
    pub parents: Vec<VertexSummary>,
    pub children: Vec<VertexSummary>,
    pub open_experiments: Vec<ExperimentSummary>,
    pub closed_experiments: Vec<ExperimentSummary>,
    pub artifacts: Vec<ArtifactSummary>,
}

#[derive(Clone, Debug)]
pub struct OpenExperimentRequest {
    pub hypothesis: String,
    pub slug: Option<Slug>,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: BTreeSet<TagName>,
    pub parents: Vec<VertexSelector>,
}

#[derive(Clone, Debug)]
pub struct UpdateExperimentRequest {
    pub experiment: String,
    pub expected_revision: Option<u64>,
    pub title: Option<NonEmptyText>,
    pub summary: Option<TextPatch<NonEmptyText>>,
    pub tags: Option<BTreeSet<TagName>>,
    pub parents: Option<Vec<VertexSelector>>,
    pub archived: Option<bool>,
    pub outcome: Option<ExperimentOutcomePatch>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentOutcomePatch {
    pub backend: ExecutionBackend,
    pub command: CommandRecipe,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub primary_metric: MetricValue,
    pub supporting_metrics: Vec<MetricValue>,
    pub verdict: FrontierVerdict,
    pub rationale: NonEmptyText,
    pub analysis: Option<ExperimentAnalysis>,
}

#[derive(Clone, Debug)]
pub struct CloseExperimentRequest {
    pub experiment: String,
    pub expected_revision: Option<u64>,
    pub backend: ExecutionBackend,
    pub command: CommandRecipe,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub primary_metric: MetricValue,
    pub supporting_metrics: Vec<MetricValue>,
    pub verdict: FrontierVerdict,
    pub rationale: NonEmptyText,
    pub analysis: Option<ExperimentAnalysis>,
}

#[derive(Clone, Debug, Default)]
pub struct ListExperimentsQuery {
    pub frontier: Option<String>,
    pub hypothesis: Option<String>,
    pub tags: BTreeSet<TagName>,
    pub include_archived: bool,
    pub status: Option<ExperimentStatus>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricObservationSummary {
    pub key: NonEmptyText,
    pub value: f64,
    pub unit: MetricUnit,
    pub dimension: MetricDimension,
    pub objective: OptimizationObjective,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KpiMetricSummary {
    pub key: NonEmptyText,
    pub precedence: u32,
    pub unit: MetricUnit,
    pub dimension: MetricDimension,
    pub aggregation: MetricAggregation,
    pub objective: OptimizationObjective,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KpiSummary {
    pub id: KpiId,
    pub name: NonEmptyText,
    pub objective: OptimizationObjective,
    pub description: Option<NonEmptyText>,
    pub metrics: Vec<KpiMetricSummary>,
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentSummary {
    pub id: ExperimentId,
    pub slug: Slug,
    pub frontier_id: FrontierId,
    pub hypothesis_id: HypothesisId,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: Vec<TagName>,
    pub status: ExperimentStatus,
    pub verdict: Option<FrontierVerdict>,
    pub primary_metric: Option<MetricObservationSummary>,
    pub updated_at: OffsetDateTime,
    pub closed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentDetail {
    pub record: ExperimentRecord,
    pub owning_hypothesis: HypothesisSummary,
    pub parents: Vec<VertexSummary>,
    pub children: Vec<VertexSummary>,
    pub artifacts: Vec<ArtifactSummary>,
}

#[derive(Clone, Debug)]
pub struct CreateArtifactRequest {
    pub slug: Option<Slug>,
    pub kind: ArtifactKind,
    pub label: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub locator: NonEmptyText,
    pub media_type: Option<NonEmptyText>,
    pub attachments: Vec<AttachmentSelector>,
}

#[derive(Clone, Debug)]
pub struct UpdateArtifactRequest {
    pub artifact: String,
    pub expected_revision: Option<u64>,
    pub kind: Option<ArtifactKind>,
    pub label: Option<NonEmptyText>,
    pub summary: Option<TextPatch<NonEmptyText>>,
    pub locator: Option<NonEmptyText>,
    pub media_type: Option<TextPatch<NonEmptyText>>,
    pub attachments: Option<Vec<AttachmentSelector>>,
}

#[derive(Clone, Debug, Default)]
pub struct ListArtifactsQuery {
    pub frontier: Option<String>,
    pub kind: Option<ArtifactKind>,
    pub attached_to: Option<AttachmentSelector>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactSummary {
    pub id: ArtifactId,
    pub slug: Slug,
    pub kind: ArtifactKind,
    pub label: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub locator: NonEmptyText,
    pub media_type: Option<NonEmptyText>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactDetail {
    pub record: ArtifactRecord,
    pub attachments: Vec<AttachmentTargetRef>,
}

#[derive(Clone, Debug)]
pub struct DefineMetricRequest {
    pub key: NonEmptyText,
    pub unit: MetricUnit,
    pub aggregation: MetricAggregation,
    pub objective: OptimizationObjective,
    pub description: Option<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub struct RenameMetricRequest {
    pub metric: NonEmptyText,
    pub new_key: NonEmptyText,
}

#[derive(Clone, Debug)]
pub struct MergeMetricRequest {
    pub source: NonEmptyText,
    pub target: NonEmptyText,
}

#[derive(Clone, Debug)]
pub struct DeleteMetricRequest {
    pub metric: NonEmptyText,
}

#[derive(Clone, Debug)]
pub struct CreateKpiRequest {
    pub frontier: String,
    pub name: NonEmptyText,
    pub objective: OptimizationObjective,
    pub description: Option<NonEmptyText>,
    pub metric_keys: Vec<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub struct PromoteMetricToKpiRequest {
    pub frontier: String,
    pub metric: NonEmptyText,
}

#[derive(Clone, Debug)]
pub struct UpdateKpiRequest {
    pub frontier: String,
    pub kpi: String,
    pub name: NonEmptyText,
    pub objective: OptimizationObjective,
    pub description: Option<NonEmptyText>,
    pub metric_keys: Vec<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub struct DeleteKpiRequest {
    pub frontier: String,
    pub kpi: String,
}

#[derive(Clone, Debug)]
pub struct KpiListQuery {
    pub frontier: String,
}

#[derive(Clone, Debug)]
pub struct KpiBestQuery {
    pub frontier: String,
    pub kpi: Option<String>,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub include_rejected: bool,
    pub limit: Option<u32>,
    pub strict: bool,
}

#[derive(Clone, Debug)]
pub struct DefineRunDimensionRequest {
    pub key: NonEmptyText,
    pub value_type: FieldValueType,
    pub description: Option<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub struct MetricKeysQuery {
    pub frontier: Option<String>,
    pub scope: MetricScope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricKeySummary {
    pub key: NonEmptyText,
    pub unit: MetricUnit,
    pub dimension: MetricDimension,
    pub aggregation: MetricAggregation,
    pub objective: OptimizationObjective,
    pub default_visibility: DefaultVisibility,
    pub description: Option<NonEmptyText>,
    pub reference_count: u64,
}

#[derive(Clone, Debug)]
pub struct MetricBestQuery {
    pub frontier: Option<String>,
    pub hypothesis: Option<String>,
    pub key: NonEmptyText,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub include_rejected: bool,
    pub limit: Option<u32>,
    pub order: Option<MetricRankOrder>,
}

#[derive(Clone, Debug)]
pub struct ExperimentNearestQuery {
    pub frontier: Option<String>,
    pub hypothesis: Option<String>,
    pub experiment: Option<String>,
    pub metric: Option<NonEmptyText>,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub tags: BTreeSet<TagName>,
    pub order: Option<MetricRankOrder>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentNearestHit {
    pub experiment: ExperimentSummary,
    pub hypothesis: HypothesisSummary,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub reasons: Vec<NonEmptyText>,
    pub metric_value: Option<MetricObservationSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentNearestResult {
    pub metric: Option<MetricKeySummary>,
    pub target_dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub accepted: Option<ExperimentNearestHit>,
    pub kept: Option<ExperimentNearestHit>,
    pub rejected: Option<ExperimentNearestHit>,
    pub champion: Option<ExperimentNearestHit>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricBestEntry {
    pub experiment: ExperimentSummary,
    pub hypothesis: HypothesisSummary,
    pub value: f64,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityHistoryEntry {
    pub revision: u64,
    pub event_kind: NonEmptyText,
    pub occurred_at: OffsetDateTime,
    pub snapshot: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct HypothesisCurrentState {
    pub hypothesis: HypothesisSummary,
    pub open_experiments: Vec<ExperimentSummary>,
    pub latest_closed_experiment: Option<ExperimentSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontierOpenProjection {
    pub frontier: FrontierRecord,
    pub active_tags: Vec<TagName>,
    pub kpis: Vec<KpiSummary>,
    pub active_metric_keys: Vec<MetricKeySummary>,
    pub active_hypotheses: Vec<HypothesisCurrentState>,
    pub open_experiments: Vec<ExperimentSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontierMetricPoint {
    pub experiment: ExperimentSummary,
    pub hypothesis: HypothesisSummary,
    pub value: f64,
    pub metric_key: NonEmptyText,
    pub verdict: FrontierVerdict,
    pub closed_at: OffsetDateTime,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontierMetricSeries {
    pub frontier: FrontierRecord,
    pub metric: MetricKeySummary,
    pub kpi: Option<KpiSummary>,
    pub points: Vec<FrontierMetricPoint>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct KpiBestEntry {
    pub experiment: ExperimentSummary,
    pub hypothesis: HypothesisSummary,
    pub value: f64,
    pub metric_key: NonEmptyText,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

pub struct ProjectStore {
    project_root: Utf8PathBuf,
    state_root: Utf8PathBuf,
    config: ProjectConfig,
    connection: Connection,
}

impl ProjectStore {
    pub fn init(
        project_root: impl AsRef<Utf8Path>,
        display_name: NonEmptyText,
    ) -> Result<Self, StoreError> {
        let project_root = canonical_project_root(project_root.as_ref())?;
        fs::create_dir_all(project_root.as_std_path())?;
        let state_root = state_root_for_project_root(&project_root)?;
        fs::create_dir_all(state_root.as_std_path())?;
        let config = ProjectConfig::new(display_name);
        write_json_file(&state_root.join(PROJECT_CONFIG_NAME), &config)?;

        let database_path = state_root.join(STATE_DB_NAME);
        let connection = Connection::open(database_path.as_std_path())?;
        connection.pragma_update(None, "foreign_keys", 1_i64)?;
        connection.pragma_update(
            None,
            "user_version",
            i64::from(CURRENT_STORE_FORMAT_VERSION),
        )?;
        install_schema(&connection)?;

        Ok(Self {
            project_root,
            state_root,
            config,
            connection,
        })
    }

    pub fn open(project_root: impl AsRef<Utf8Path>) -> Result<Self, StoreError> {
        let project_root = canonical_project_root(project_root.as_ref())?;
        let state_root = state_root_for_project_root(&project_root)?;
        if !state_root.exists() {
            return Err(StoreError::MissingProjectStore(project_root));
        }
        let config: ProjectConfig = read_json_file(&state_root.join(PROJECT_CONFIG_NAME))?;
        if config.store_format_version != CURRENT_STORE_FORMAT_VERSION {
            return Err(StoreError::IncompatibleStoreFormatVersion {
                observed: config.store_format_version,
                expected: CURRENT_STORE_FORMAT_VERSION,
            });
        }
        let database_path = state_root.join(STATE_DB_NAME);
        let connection = Connection::open(database_path.as_std_path())?;
        connection.pragma_update(None, "foreign_keys", 1_i64)?;
        let observed_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if u32::try_from(observed_version).ok() != Some(CURRENT_STORE_FORMAT_VERSION) {
            return Err(StoreError::IncompatibleStoreFormatVersion {
                observed: u32::try_from(observed_version).unwrap_or(0),
                expected: CURRENT_STORE_FORMAT_VERSION,
            });
        }

        Ok(Self {
            project_root,
            state_root,
            config,
            connection,
        })
    }

    #[must_use]
    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }

    #[must_use]
    pub fn project_root(&self) -> &Utf8Path {
        &self.project_root
    }

    #[must_use]
    pub fn state_root(&self) -> &Utf8Path {
        &self.state_root
    }

    pub fn status(&self) -> Result<ProjectStatus, StoreError> {
        Ok(ProjectStatus {
            project_root: self.project_root.clone(),
            state_root: self.state_root.clone(),
            display_name: self.config.display_name.clone(),
            store_format_version: self.config.store_format_version,
            frontier_count: count_rows(&self.connection, "frontiers")?,
            hypothesis_count: count_rows(&self.connection, "hypotheses")?,
            experiment_count: count_rows(&self.connection, "experiments")?,
            open_experiment_count: count_rows_where(
                &self.connection,
                "experiments",
                "status = 'open'",
            )?,
            artifact_count: count_rows(&self.connection, "artifacts")?,
        })
    }

    pub fn register_tag(
        &mut self,
        name: TagName,
        description: NonEmptyText,
    ) -> Result<TagRecord, StoreError> {
        self.register_tag_with_origin(name, description, None, MutationOrigin::Supervisor)
    }

    pub fn register_tag_in_family(
        &mut self,
        name: TagName,
        description: NonEmptyText,
        family: Option<TagFamilyName>,
    ) -> Result<TagRecord, StoreError> {
        self.register_tag_with_origin(name, description, family, MutationOrigin::Supervisor)
    }

    pub fn register_tag_from_mcp(
        &mut self,
        name: TagName,
        description: NonEmptyText,
    ) -> Result<TagRecord, StoreError> {
        self.register_tag_with_origin(name, description, None, MutationOrigin::Mcp)
    }

    fn register_tag_with_origin(
        &mut self,
        name: TagName,
        description: NonEmptyText,
        family: Option<TagFamilyName>,
        origin: MutationOrigin,
    ) -> Result<TagRecord, StoreError> {
        if origin.is_mcp() {
            self.assert_tag_add_open()?;
            self.assert_no_stale_tag_name(&name)?;
        }
        if self.tag_record_by_name(&name)?.is_some() {
            return Err(StoreError::DuplicateTag(name));
        }
        let family = family
            .as_ref()
            .map(|name| {
                self.tag_family_by_name(name)?
                    .ok_or_else(|| StoreError::UnknownTagFamily(name.clone()))
            })
            .transpose()?;
        let now = OffsetDateTime::now_utc();
        let record = TagRecord {
            id: TagId::fresh(),
            name,
            description,
            family_id: family.as_ref().map(|family| family.id),
            family: family.as_ref().map(|family| family.name.clone()),
            status: TagStatus::Active,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let transaction = self.connection.transaction()?;
        insert_tag(&transaction, &record)?;
        let _ = transaction.execute(
            "DELETE FROM tag_name_history WHERE name = ?1",
            params![record.name.as_str()],
        )?;
        record_event(
            &transaction,
            "tag",
            &record.id.to_string(),
            record.revision,
            "created",
            &record,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn list_tags(&self) -> Result<Vec<TagRecord>, StoreError> {
        self.default_visible_tag_records()
    }

    pub fn tag_registry(&self, query: TagRegistryQuery) -> Result<TagRegistrySnapshot, StoreError> {
        let tags = if query.include_hidden {
            self.load_tag_records()?
        } else {
            self.default_visible_tag_records()?
        };
        Ok(TagRegistrySnapshot {
            tags,
            families: self.load_tag_family_records()?,
            locks: self.load_registry_locks(&RegistryName::tags())?,
            name_history: self.load_tag_name_history()?,
        })
    }

    pub fn create_tag_family(
        &mut self,
        request: CreateTagFamilyRequest,
    ) -> Result<TagFamilyRecord, StoreError> {
        if self.tag_family_by_name(&request.name)?.is_some() {
            return Err(StoreError::DuplicateTagFamily(request.name));
        }
        let now = OffsetDateTime::now_utc();
        let record = TagFamilyRecord {
            id: TagFamilyId::fresh(),
            name: request.name,
            description: request.description,
            mandatory: request.mandatory,
            status: TagStatus::Active,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let transaction = self.connection.transaction()?;
        insert_tag_family(&transaction, &record)?;
        record_event(
            &transaction,
            "tag_family",
            &record.id.to_string(),
            record.revision,
            "created",
            &record,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn rename_tag(&mut self, request: RenameTagRequest) -> Result<TagRecord, StoreError> {
        let record = self
            .tag_record_by_name(&request.tag)?
            .ok_or_else(|| StoreError::UnknownTag(request.tag.clone()))?;
        enforce_revision(
            "tag",
            &request.tag.to_string(),
            request.expected_revision,
            record.revision,
        )?;
        if self.tag_record_by_name(&request.new_name)?.is_some() {
            return Err(StoreError::DuplicateTag(request.new_name));
        }
        let now = OffsetDateTime::now_utc();
        let updated = TagRecord {
            name: request.new_name,
            revision: record.revision.saturating_add(1),
            updated_at: now,
            ..record
        };
        let message = NonEmptyText::new(format!(
            "tag `{}` was renamed to `{}`; use `{}`",
            request.tag, updated.name, updated.name
        ))?;
        let history = TagNameHistoryRecord {
            name: request.tag,
            target_tag_id: Some(updated.id),
            target_tag_name: Some(updated.name.clone()),
            disposition: TagNameDisposition::Renamed,
            message,
            created_at: now,
        };
        let transaction = self.connection.transaction()?;
        update_tag(&transaction, &updated)?;
        upsert_tag_name_history(&transaction, &history)?;
        record_event(
            &transaction,
            "tag",
            &updated.id.to_string(),
            updated.revision,
            "renamed",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn merge_tag(&mut self, request: MergeTagRequest) -> Result<TagRecord, StoreError> {
        if request.source == request.target {
            return Err(StoreError::InvalidInput(
                "cannot merge a tag into itself".to_owned(),
            ));
        }
        let source = self
            .tag_record_by_name(&request.source)?
            .ok_or_else(|| StoreError::UnknownTag(request.source.clone()))?;
        let target = self
            .tag_record_by_name(&request.target)?
            .ok_or_else(|| StoreError::UnknownTag(request.target.clone()))?;
        enforce_revision(
            "tag",
            &request.source.to_string(),
            request.expected_revision,
            source.revision,
        )?;
        let now = OffsetDateTime::now_utc();
        let message = NonEmptyText::new(format!(
            "tag `{}` was merged into `{}`; use `{}`",
            source.name, target.name, target.name
        ))?;
        let history = TagNameHistoryRecord {
            name: source.name.clone(),
            target_tag_id: Some(target.id),
            target_tag_name: Some(target.name.clone()),
            disposition: TagNameDisposition::Merged,
            message,
            created_at: now,
        };
        let transaction = self.connection.transaction()?;
        merge_tag_edges(&transaction, source.id, target.id)?;
        delete_tag_row(&transaction, source.id)?;
        upsert_tag_name_history(&transaction, &history)?;
        record_event(
            &transaction,
            "tag",
            &source.id.to_string(),
            source.revision.saturating_add(1),
            "merged",
            &history,
        )?;
        transaction.commit()?;
        Ok(target)
    }

    pub fn delete_tag(&mut self, request: DeleteTagRequest) -> Result<(), StoreError> {
        let record = self
            .tag_record_by_name(&request.tag)?
            .ok_or_else(|| StoreError::UnknownTag(request.tag.clone()))?;
        enforce_revision(
            "tag",
            &request.tag.to_string(),
            request.expected_revision,
            record.revision,
        )?;
        let now = OffsetDateTime::now_utc();
        let message = NonEmptyText::new(format!(
            "tag `{}` was deleted by the supervisor; choose an active tag from tag.list",
            record.name
        ))?;
        let history = TagNameHistoryRecord {
            name: record.name.clone(),
            target_tag_id: None,
            target_tag_name: None,
            disposition: TagNameDisposition::Deleted,
            message,
            created_at: now,
        };
        let transaction = self.connection.transaction()?;
        delete_tag_row(&transaction, record.id)?;
        upsert_tag_name_history(&transaction, &history)?;
        record_event(
            &transaction,
            "tag",
            &record.id.to_string(),
            record.revision.saturating_add(1),
            "deleted",
            &history,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn assign_tag_family(
        &mut self,
        request: AssignTagFamilyRequest,
    ) -> Result<TagRecord, StoreError> {
        let record = self
            .tag_record_by_name(&request.tag)?
            .ok_or_else(|| StoreError::UnknownTag(request.tag.clone()))?;
        enforce_revision(
            "tag",
            &request.tag.to_string(),
            request.expected_revision,
            record.revision,
        )?;
        let family = request
            .family
            .as_ref()
            .map(|name| {
                self.tag_family_by_name(name)?
                    .ok_or_else(|| StoreError::UnknownTagFamily(name.clone()))
            })
            .transpose()?;
        let updated = TagRecord {
            family_id: family.as_ref().map(|family| family.id),
            family: family.as_ref().map(|family| family.name.clone()),
            revision: record.revision.saturating_add(1),
            updated_at: OffsetDateTime::now_utc(),
            ..record
        };
        let transaction = self.connection.transaction()?;
        update_tag(&transaction, &updated)?;
        record_event(
            &transaction,
            "tag",
            &updated.id.to_string(),
            updated.revision,
            "family_assigned",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn set_tag_family_mandatory(
        &mut self,
        request: SetTagFamilyMandatoryRequest,
    ) -> Result<TagFamilyRecord, StoreError> {
        let record = self
            .tag_family_by_name(&request.family)?
            .ok_or_else(|| StoreError::UnknownTagFamily(request.family.clone()))?;
        enforce_revision(
            "tag_family",
            &request.family.to_string(),
            request.expected_revision,
            record.revision,
        )?;
        let updated = TagFamilyRecord {
            mandatory: request.mandatory,
            revision: record.revision.saturating_add(1),
            updated_at: OffsetDateTime::now_utc(),
            ..record
        };
        let transaction = self.connection.transaction()?;
        update_tag_family(&transaction, &updated)?;
        record_event(
            &transaction,
            "tag_family",
            &updated.id.to_string(),
            updated.revision,
            "mandatory_updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn set_registry_lock(
        &mut self,
        request: SetRegistryLockRequest,
    ) -> Result<Option<RegistryLockRecord>, StoreError> {
        if !request.locked {
            let transaction = self.connection.transaction()?;
            let _ = transaction.execute(
                "DELETE FROM registry_locks
                 WHERE registry = ?1 AND mode = ?2 AND scope_kind = 'project' AND scope_id = 'project'",
                params![request.registry.as_str(), request.mode.as_str()],
            )?;
            transaction.commit()?;
            return Ok(None);
        }
        let now = OffsetDateTime::now_utc();
        let existing = self.registry_lock(&request.registry, request.mode)?;
        let reason = registry_lock_reason(&request.registry, request.mode)?;
        let record = RegistryLockRecord {
            id: existing
                .as_ref()
                .map_or_else(RegistryLockId::fresh, |lock| lock.id),
            registry: request.registry,
            mode: request.mode,
            scope_kind: NonEmptyText::new("project")?,
            scope_id: NonEmptyText::new("project")?,
            reason,
            revision: existing
                .as_ref()
                .map_or(1, |lock| lock.revision.saturating_add(1)),
            locked_at: existing.as_ref().map_or(now, |lock| lock.locked_at),
            updated_at: now,
        };
        let transaction = self.connection.transaction()?;
        upsert_registry_lock(&transaction, &record)?;
        record_event(
            &transaction,
            "registry_lock",
            &record.id.to_string(),
            record.revision,
            "updated",
            &record,
        )?;
        transaction.commit()?;
        Ok(Some(record))
    }

    pub fn set_frontier_registry_lock(
        &mut self,
        request: SetFrontierRegistryLockRequest,
    ) -> Result<Option<RegistryLockRecord>, StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        if !request.locked {
            let transaction = self.connection.transaction()?;
            let _ = transaction.execute(
                "DELETE FROM registry_locks
                 WHERE registry = ?1 AND mode = ?2 AND scope_kind = 'frontier' AND scope_id = ?3",
                params![
                    request.registry.as_str(),
                    request.mode.as_str(),
                    frontier.id.to_string()
                ],
            )?;
            transaction.commit()?;
            return Ok(None);
        }
        let now = OffsetDateTime::now_utc();
        let existing =
            self.frontier_registry_lock_by_id(&request.registry, request.mode, frontier.id)?;
        let reason = frontier_registry_lock_reason(&request.registry, request.mode, &frontier)?;
        let record = RegistryLockRecord {
            id: existing
                .as_ref()
                .map_or_else(RegistryLockId::fresh, |lock| lock.id),
            registry: request.registry,
            mode: request.mode,
            scope_kind: NonEmptyText::new("frontier")?,
            scope_id: NonEmptyText::new(frontier.id.to_string())?,
            reason,
            revision: existing
                .as_ref()
                .map_or(1, |lock| lock.revision.saturating_add(1)),
            locked_at: existing.as_ref().map_or(now, |lock| lock.locked_at),
            updated_at: now,
        };
        let transaction = self.connection.transaction()?;
        upsert_registry_lock(&transaction, &record)?;
        record_event(
            &transaction,
            "registry_lock",
            &record.id.to_string(),
            record.revision,
            "updated",
            &record,
        )?;
        transaction.commit()?;
        Ok(Some(record))
    }

    pub fn frontier_registry_lock(
        &self,
        registry: &RegistryName,
        mode: RegistryLockMode,
        frontier: &str,
    ) -> Result<Option<RegistryLockRecord>, StoreError> {
        let frontier = self.resolve_frontier(frontier)?;
        self.frontier_registry_lock_by_id(registry, mode, frontier.id)
    }

    pub fn define_metric(
        &mut self,
        request: DefineMetricRequest,
    ) -> Result<MetricDefinition, StoreError> {
        if self.metric_definition(&request.key)?.is_some() {
            return Err(StoreError::DuplicateMetricDefinition(request.key));
        }
        let record = MetricDefinition::new(
            request.key,
            request.unit,
            request.aggregation,
            request.objective,
            request.description,
        );
        let _ = self.connection.execute(
            "INSERT INTO metric_definitions (id, key, dimension, display_unit, aggregation, objective, description, revision, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.id.to_string(),
                record.key.as_str(),
                record.dimension.as_str(),
                record.unit.as_str(),
                record.aggregation.as_str(),
                record.objective.as_str(),
                record.description.as_ref().map(NonEmptyText::as_str),
                record.revision,
                encode_timestamp(record.created_at)?,
                encode_timestamp(record.updated_at)?,
            ],
        )?;
        Ok(record)
    }

    pub fn list_metric_definitions(&self) -> Result<Vec<MetricDefinition>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, key, dimension, display_unit, aggregation, objective, description, revision, created_at, updated_at
             FROM metric_definitions
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map([], decode_metric_definition_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn rename_metric(
        &mut self,
        request: RenameMetricRequest,
    ) -> Result<MetricDefinition, StoreError> {
        if self.metric_definition(&request.new_key)?.is_some() {
            return Err(StoreError::DuplicateMetricDefinition(request.new_key));
        }
        let metric = self
            .metric_definition(&request.metric)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.metric.clone()))?;
        let mut renamed = metric.clone();
        renamed.key = request.new_key.clone();
        renamed.revision = renamed.revision.saturating_add(1);
        renamed.updated_at = OffsetDateTime::now_utc();
        let transaction = self.connection.transaction()?;
        rewrite_outcome_metric_key(&transaction, &metric.key, &request.new_key)?;
        update_metric_definition_key(&transaction, &renamed)?;
        insert_metric_name_history(
            &transaction,
            metric.key.as_str(),
            Some(metric.id),
            Some(request.new_key.as_str()),
            TagNameDisposition::Renamed,
            &format!(
                "metric `{}` was renamed to `{}`",
                metric.key, request.new_key
            ),
        )?;
        record_event(
            &transaction,
            "metric",
            &metric.id.to_string(),
            renamed.revision,
            "renamed",
            &renamed,
        )?;
        transaction.commit()?;
        Ok(renamed)
    }

    pub fn merge_metric(&mut self, request: MergeMetricRequest) -> Result<(), StoreError> {
        if request.source == request.target {
            return Err(StoreError::InvalidInput(
                "metric merge source and target must differ".to_owned(),
            ));
        }
        let source = self
            .metric_definition(&request.source)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.source.clone()))?;
        let target = self
            .metric_definition(&request.target)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.target.clone()))?;
        if source.dimension != target.dimension || source.objective != target.objective {
            return Err(StoreError::PolicyViolation(format!(
                "metric `{}` cannot merge into `{}` because dimension/objective differ",
                source.key, target.key
            )));
        }
        let transaction = self.connection.transaction()?;
        rewrite_outcome_metric_key(&transaction, &source.key, &target.key)?;
        merge_experiment_metric_rows(&transaction, source.id, target.id)?;
        merge_kpi_metric_alternatives(&transaction, source.id, target.id)?;
        delete_metric_definition_row(&transaction, source.id)?;
        insert_metric_name_history(
            &transaction,
            source.key.as_str(),
            Some(target.id),
            Some(target.key.as_str()),
            TagNameDisposition::Merged,
            &format!("metric `{}` was merged into `{}`", source.key, target.key),
        )?;
        record_event(
            &transaction,
            "metric",
            &source.id.to_string(),
            source.revision.saturating_add(1),
            "merged",
            &serde_json::json!({
                "source": source.key,
                "target": target.key,
            }),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn delete_metric(&mut self, request: DeleteMetricRequest) -> Result<(), StoreError> {
        let metric = self
            .metric_definition(&request.metric)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.metric.clone()))?;
        let reference_count = self.metric_reference_count(None, metric.id)?;
        let kpi_count = self.kpi_reference_count(metric.id)?;
        if reference_count != 0 || kpi_count != 0 {
            return Err(StoreError::PolicyViolation(format!(
                "metric `{}` is still referenced by {} observations and {} KPI alternatives; merge or remove those references before deletion",
                metric.key, reference_count, kpi_count
            )));
        }
        let transaction = self.connection.transaction()?;
        delete_metric_definition_row(&transaction, metric.id)?;
        insert_metric_name_history(
            &transaction,
            metric.key.as_str(),
            None,
            None,
            TagNameDisposition::Deleted,
            &format!("metric `{}` was deleted", metric.key),
        )?;
        record_event(
            &transaction,
            "metric",
            &metric.id.to_string(),
            metric.revision.saturating_add(1),
            "deleted",
            &metric,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn create_kpi(&mut self, request: CreateKpiRequest) -> Result<KpiSummary, StoreError> {
        if request.metric_keys.is_empty() {
            return Err(StoreError::EmptyKpi { kpi: request.name });
        }
        let frontier = self.resolve_frontier(&request.frontier)?;
        if self
            .kpi_by_name(frontier.id, request.name.as_str())?
            .is_some()
        {
            return Err(StoreError::DuplicateKpi(request.name));
        }
        let metrics = self.resolve_kpi_metric_definitions(
            &request.name,
            request.objective,
            &request.metric_keys,
        )?;
        let now = OffsetDateTime::now_utc();
        let record = FrontierKpiRecord {
            id: KpiId::fresh(),
            frontier_id: frontier.id,
            name: request.name,
            objective: request.objective,
            description: request.description,
            status: TagStatus::Active,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let alternatives = metrics
            .iter()
            .enumerate()
            .map(|(index, metric)| KpiMetricAlternativeRecord {
                kpi_id: record.id,
                metric_id: metric.id,
                metric_key: metric.key.clone(),
                precedence: u32::try_from(index).unwrap_or(u32::MAX),
            })
            .collect::<Vec<_>>();
        let transaction = self.connection.transaction()?;
        insert_kpi(&transaction, &record)?;
        replace_kpi_alternatives(&transaction, record.id, &alternatives)?;
        record_event(
            &transaction,
            "kpi",
            &record.id.to_string(),
            record.revision,
            "created",
            &record,
        )?;
        transaction.commit()?;
        self.kpi_summary(record)
    }

    pub fn promote_metric_to_kpi_from_mcp(
        &mut self,
        request: PromoteMetricToKpiRequest,
    ) -> Result<KpiSummary, StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        if let Some(lock) = self.frontier_registry_lock_by_id(
            &RegistryName::kpis(),
            RegistryLockMode::Assignment,
            frontier.id,
        )? {
            return Err(StoreError::PolicyViolation(format!(
                "MCP KPI creation is locked for frontier `{}`; ask the supervisor to unlock KPI creation on the Metrics page. Reason: {}",
                frontier.slug, lock.reason
            )));
        }
        let metric = self
            .metric_definition(&request.metric)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.metric.clone()))?;
        if let Some(kpi) = self.frontier_kpis(frontier.id)?.into_iter().find(|kpi| {
            kpi.metrics
                .iter()
                .any(|candidate| candidate.key == metric.key)
        }) {
            return Err(StoreError::PolicyViolation(format!(
                "metric `{}` is already a KPI metric for frontier `{}` via KPI `{}`",
                metric.key, frontier.slug, kpi.name
            )));
        }
        self.create_kpi(CreateKpiRequest {
            frontier: request.frontier,
            name: metric.key.clone(),
            objective: metric.objective,
            description: metric.description,
            metric_keys: vec![metric.key],
        })
    }

    pub fn update_kpi(&mut self, request: UpdateKpiRequest) -> Result<KpiSummary, StoreError> {
        if request.metric_keys.is_empty() {
            return Err(StoreError::EmptyKpi { kpi: request.name });
        }
        let frontier = self.resolve_frontier(&request.frontier)?;
        let mut record = self
            .kpi_by_selector(frontier.id, &request.kpi)?
            .ok_or_else(|| StoreError::UnknownKpi(request.kpi.clone()))?;
        if record.name != request.name
            && self
                .kpi_by_name(frontier.id, request.name.as_str())?
                .is_some()
        {
            return Err(StoreError::DuplicateKpi(request.name));
        }
        let metrics = self.resolve_kpi_metric_definitions(
            &request.name,
            request.objective,
            &request.metric_keys,
        )?;
        let now = OffsetDateTime::now_utc();
        record.name = request.name;
        record.objective = request.objective;
        record.description = request.description;
        record.revision = record.revision.saturating_add(1);
        record.updated_at = now;
        let alternatives = metrics
            .iter()
            .enumerate()
            .map(|(index, metric)| KpiMetricAlternativeRecord {
                kpi_id: record.id,
                metric_id: metric.id,
                metric_key: metric.key.clone(),
                precedence: u32::try_from(index).unwrap_or(u32::MAX),
            })
            .collect::<Vec<_>>();
        let transaction = self.connection.transaction()?;
        update_kpi(&transaction, &record)?;
        replace_kpi_alternatives(&transaction, record.id, &alternatives)?;
        record_event(
            &transaction,
            "kpi",
            &record.id.to_string(),
            record.revision,
            "updated",
            &record,
        )?;
        transaction.commit()?;
        self.kpi_summary(record)
    }

    pub fn delete_kpi(&mut self, request: DeleteKpiRequest) -> Result<(), StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        let mut record = self
            .kpi_by_selector(frontier.id, &request.kpi)?
            .ok_or_else(|| StoreError::UnknownKpi(request.kpi.clone()))?;
        record.revision = record.revision.saturating_add(1);
        record.updated_at = OffsetDateTime::now_utc();
        let transaction = self.connection.transaction()?;
        let _ = transaction.execute(
            "DELETE FROM frontier_kpis WHERE id = ?1",
            params![record.id.to_string()],
        )?;
        record_event(
            &transaction,
            "kpi",
            &record.id.to_string(),
            record.revision,
            "deleted",
            &record,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn list_kpis(&self, query: KpiListQuery) -> Result<Vec<KpiSummary>, StoreError> {
        let frontier = self.resolve_frontier(&query.frontier)?;
        self.frontier_kpis(frontier.id)
    }

    pub fn kpi_best(&self, query: KpiBestQuery) -> Result<Vec<KpiBestEntry>, StoreError> {
        let frontier = self.resolve_frontier(&query.frontier)?;
        let kpi = self.resolve_kpi_for_query(frontier.id, query.kpi.as_deref())?;
        let order = match kpi.objective {
            OptimizationObjective::Minimize => MetricRankOrder::Asc,
            OptimizationObjective::Maximize => MetricRankOrder::Desc,
            OptimizationObjective::Target => {
                return Err(StoreError::MetricOrderRequired {
                    key: kpi.name.to_string(),
                });
            }
        };
        let preferred_key = kpi.metrics.first().map(|metric| metric.key.clone());
        let experiments = self
            .load_experiment_records(Some(frontier.id), None, true)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Closed)
            .filter(|record| {
                query.include_rejected
                    || record
                        .outcome
                        .as_ref()
                        .is_some_and(|outcome| outcome.verdict != FrontierVerdict::Rejected)
            })
            .collect::<Vec<_>>();
        let mut entries = experiments
            .into_iter()
            .filter_map(|record| {
                let outcome = record.outcome.clone()?;
                if !dimension_subset_matches(&query.dimensions, &outcome.dimensions) {
                    return None;
                }
                let resolved =
                    resolve_kpi_metric(&kpi, &outcome, query.strict, preferred_key.as_ref())?;
                Some((record, outcome.dimensions.clone(), resolved))
            })
            .map(|(record, dimensions, metric)| {
                let definition = self
                    .metric_definition(&metric.key)?
                    .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))?;
                let sort_value = definition.unit.canonical_value(metric.value);
                Ok((
                    KpiBestEntry {
                        experiment: self.experiment_summary_from_record(record.clone())?,
                        hypothesis: self.hypothesis_summary_from_record(
                            self.hypothesis_by_id(record.hypothesis_id)?,
                        )?,
                        value: metric.value,
                        metric_key: metric.key,
                        dimensions,
                    },
                    sort_value,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        entries.sort_by(|left, right| compare_metric_values(left.1, right.1, order));
        Ok(apply_limit(
            entries
                .into_iter()
                .map(|(entry, _sort_value)| entry)
                .collect::<Vec<_>>(),
            query.limit,
        ))
    }

    pub fn define_run_dimension(
        &mut self,
        request: DefineRunDimensionRequest,
    ) -> Result<RunDimensionDefinition, StoreError> {
        if self.run_dimension_definition(&request.key)?.is_some() {
            return Err(StoreError::DuplicateRunDimension(request.key));
        }
        let record =
            RunDimensionDefinition::new(request.key, request.value_type, request.description);
        let _ = self.connection.execute(
            "INSERT INTO run_dimension_definitions (key, value_type, description, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.key.as_str(),
                record.value_type.as_str(),
                record.description.as_ref().map(NonEmptyText::as_str),
                encode_timestamp(record.created_at)?,
                encode_timestamp(record.updated_at)?,
            ],
        )?;
        Ok(record)
    }

    pub fn list_run_dimensions(&self) -> Result<Vec<RunDimensionDefinition>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, value_type, description, created_at, updated_at
             FROM run_dimension_definitions
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map([], decode_run_dimension_definition_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn create_frontier(
        &mut self,
        request: CreateFrontierRequest,
    ) -> Result<FrontierRecord, StoreError> {
        let id = FrontierId::fresh();
        let slug = self.unique_frontier_slug(request.slug, &request.label)?;
        let now = OffsetDateTime::now_utc();
        let record = FrontierRecord {
            id,
            slug,
            label: request.label,
            objective: request.objective,
            status: FrontierStatus::Exploring,
            brief: FrontierBrief::default(),
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let transaction = self.connection.transaction()?;
        insert_frontier(&transaction, &record)?;
        record_event(
            &transaction,
            "frontier",
            &record.id.to_string(),
            1,
            "created",
            &record,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn list_frontiers(
        &self,
        query: ListFrontiersQuery,
    ) -> Result<Vec<FrontierSummary>, StoreError> {
        self.frontier_records()?
            .into_iter()
            .filter(|record| query.include_archived || record.status != FrontierStatus::Archived)
            .map(|record| self.frontier_summary_from_record(record))
            .collect()
    }

    pub fn list_frontiers_from_mcp(
        &self,
        query: ListFrontiersQuery,
    ) -> Result<Vec<FrontierSummary>, StoreError> {
        self.frontier_records()?
            .into_iter()
            .filter(|record| query.include_archived || record.status != FrontierStatus::Archived)
            .map(|record| {
                let has_kpi = !self.frontier_kpis(record.id)?.is_empty();
                Ok((record, has_kpi))
            })
            .collect::<Result<Vec<_>, StoreError>>()?
            .into_iter()
            .filter(|(record, has_kpi)| record.status == FrontierStatus::Archived || *has_kpi)
            .map(|(record, _has_kpi)| self.frontier_summary_from_record(record))
            .collect()
    }

    fn frontier_records(&self) -> Result<Vec<FrontierRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, slug, label, objective, status, brief_json, revision, created_at, updated_at
             FROM frontiers
             ORDER BY updated_at DESC, created_at DESC",
        )?;
        let rows = statement.query_map([], decode_frontier_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn frontier_summary_from_record(
        &self,
        record: FrontierRecord,
    ) -> Result<FrontierSummary, StoreError> {
        Ok(FrontierSummary {
            active_hypothesis_count: self.active_hypothesis_count(record.id)?,
            open_experiment_count: self.open_experiment_count(Some(record.id))?,
            id: record.id,
            slug: record.slug,
            label: record.label,
            objective: record.objective,
            status: record.status,
            revision: record.revision,
            updated_at: record.updated_at,
        })
    }

    fn frontier_slug_by_id(&self, frontier_id: FrontierId) -> Result<String, StoreError> {
        self.connection
            .query_row(
                "SELECT slug FROM frontiers WHERE id = ?1",
                params![frontier_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .map_err(StoreError::from)
    }

    pub fn read_frontier(&self, selector: &str) -> Result<FrontierRecord, StoreError> {
        self.resolve_frontier(selector)
    }

    pub fn update_frontier(
        &mut self,
        request: UpdateFrontierRequest,
    ) -> Result<FrontierRecord, StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        enforce_revision(
            "frontier",
            &request.frontier,
            request.expected_revision,
            frontier.revision,
        )?;
        let now = OffsetDateTime::now_utc();
        let brief_changed =
            request.situation.is_some() || request.roadmap.is_some() || request.unknowns.is_some();
        let brief = FrontierBrief {
            situation: apply_optional_text_patch(
                request.situation,
                frontier.brief.situation.clone(),
            ),
            roadmap: match request.roadmap {
                Some(items) => items
                    .into_iter()
                    .map(|item| {
                        Ok(FrontierRoadmapItem {
                            rank: item.rank,
                            hypothesis_id: self.resolve_hypothesis(&item.hypothesis)?.id,
                            summary: item.summary,
                        })
                    })
                    .collect::<Result<Vec<_>, StoreError>>()?,
                None => frontier.brief.roadmap.clone(),
            },
            unknowns: request.unknowns.unwrap_or(frontier.brief.unknowns.clone()),
            revision: if brief_changed {
                frontier.brief.revision.saturating_add(1)
            } else {
                frontier.brief.revision
            },
            updated_at: if brief_changed {
                Some(now)
            } else {
                frontier.brief.updated_at
            },
        };
        let updated = FrontierRecord {
            objective: request.objective.unwrap_or(frontier.objective.clone()),
            status: request.status.unwrap_or(frontier.status),
            brief,
            revision: frontier.revision.saturating_add(1),
            updated_at: now,
            ..frontier
        };
        let transaction = self.connection.transaction()?;
        update_frontier_row(&transaction, &updated)?;
        record_event(
            &transaction,
            "frontier",
            &updated.id.to_string(),
            updated.revision,
            "updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn create_hypothesis(
        &mut self,
        request: CreateHypothesisRequest,
    ) -> Result<HypothesisRecord, StoreError> {
        self.create_hypothesis_with_origin(request, MutationOrigin::Supervisor)
    }

    pub fn create_hypothesis_from_mcp(
        &mut self,
        request: CreateHypothesisRequest,
    ) -> Result<HypothesisRecord, StoreError> {
        self.create_hypothesis_with_origin(request, MutationOrigin::Mcp)
    }

    fn create_hypothesis_with_origin(
        &mut self,
        request: CreateHypothesisRequest,
        origin: MutationOrigin,
    ) -> Result<HypothesisRecord, StoreError> {
        validate_hypothesis_body(&request.body)?;
        let tag_ids = self.resolve_tag_set(&request.tags, origin)?;
        self.assert_tag_policy_for_assignment(&request.tags, origin)?;
        let frontier = self.resolve_frontier(&request.frontier)?;
        let id = HypothesisId::fresh();
        let slug = self.unique_hypothesis_slug(request.slug, &request.title)?;
        let now = OffsetDateTime::now_utc();
        let record = HypothesisRecord {
            id,
            slug,
            frontier_id: frontier.id,
            archived: false,
            title: request.title,
            summary: request.summary,
            body: request.body,
            tags: request.tags.iter().cloned().collect(),
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let parents = self.resolve_vertex_parents(
            frontier.id,
            &request.parents,
            Some(VertexRef::Hypothesis(id)),
        )?;
        let transaction = self.connection.transaction()?;
        insert_hypothesis(&transaction, &record)?;
        replace_hypothesis_tags(&transaction, record.id, &tag_ids)?;
        replace_influence_parents(&transaction, VertexRef::Hypothesis(id), &parents)?;
        record_event(
            &transaction,
            "hypothesis",
            &record.id.to_string(),
            1,
            "created",
            &record,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn list_hypotheses(
        &self,
        query: ListHypothesesQuery,
    ) -> Result<Vec<HypothesisSummary>, StoreError> {
        let frontier_id = query
            .frontier
            .as_deref()
            .map(|selector| self.resolve_frontier(selector).map(|frontier| frontier.id))
            .transpose()?;
        let records = self.load_hypothesis_records(frontier_id, query.include_archived)?;
        let filtered = records
            .into_iter()
            .filter(|record| {
                query.tags.is_empty() || query.tags.iter().all(|tag| record.tags.contains(tag))
            })
            .map(|record| self.hypothesis_summary_from_record(record))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(apply_limit(filtered, query.limit))
    }

    pub fn read_hypothesis(&self, selector: &str) -> Result<HypothesisDetail, StoreError> {
        let record = self.resolve_hypothesis(selector)?;
        let parents = self.load_vertex_parents(VertexRef::Hypothesis(record.id))?;
        let children = self.load_vertex_children(VertexRef::Hypothesis(record.id))?;
        let experiments = self.list_experiments(ListExperimentsQuery {
            hypothesis: Some(record.id.to_string()),
            include_archived: true,
            limit: None,
            ..ListExperimentsQuery::default()
        })?;
        let (open_experiments, closed_experiments): (Vec<_>, Vec<_>) = experiments
            .into_iter()
            .partition(|experiment| experiment.status == ExperimentStatus::Open);
        Ok(HypothesisDetail {
            artifacts: self.list_artifacts(ListArtifactsQuery {
                attached_to: Some(AttachmentSelector::Hypothesis(record.id.to_string())),
                limit: None,
                ..ListArtifactsQuery::default()
            })?,
            children,
            closed_experiments,
            open_experiments,
            parents,
            record,
        })
    }

    pub fn update_hypothesis(
        &mut self,
        request: UpdateHypothesisRequest,
    ) -> Result<HypothesisRecord, StoreError> {
        self.update_hypothesis_with_origin(request, MutationOrigin::Supervisor)
    }

    pub fn update_hypothesis_from_mcp(
        &mut self,
        request: UpdateHypothesisRequest,
    ) -> Result<HypothesisRecord, StoreError> {
        self.update_hypothesis_with_origin(request, MutationOrigin::Mcp)
    }

    fn update_hypothesis_with_origin(
        &mut self,
        request: UpdateHypothesisRequest,
        origin: MutationOrigin,
    ) -> Result<HypothesisRecord, StoreError> {
        let record = self.resolve_hypothesis(&request.hypothesis)?;
        enforce_revision(
            "hypothesis",
            &request.hypothesis,
            request.expected_revision,
            record.revision,
        )?;
        if let Some(body) = request.body.as_ref() {
            validate_hypothesis_body(body)?;
        }
        let tag_ids = request
            .tags
            .as_ref()
            .map(|tags| {
                self.assert_tag_policy_for_assignment(tags, origin)?;
                self.resolve_tag_set(tags, origin)
            })
            .transpose()?;
        let updated = HypothesisRecord {
            title: request.title.unwrap_or(record.title.clone()),
            summary: request.summary.unwrap_or(record.summary.clone()),
            body: request.body.unwrap_or(record.body.clone()),
            tags: request
                .tags
                .clone()
                .map_or_else(|| record.tags.clone(), |tags| tags.into_iter().collect()),
            archived: request.archived.unwrap_or(record.archived),
            revision: record.revision.saturating_add(1),
            updated_at: OffsetDateTime::now_utc(),
            ..record
        };
        let parents = request
            .parents
            .as_ref()
            .map(|selectors| {
                self.resolve_vertex_parents(
                    updated.frontier_id,
                    selectors,
                    Some(VertexRef::Hypothesis(updated.id)),
                )
            })
            .transpose()?;
        let final_tag_ids = match tag_ids {
            Some(tag_ids) => tag_ids,
            None => self.resolve_existing_tag_names(&updated.tags)?,
        };
        let transaction = self.connection.transaction()?;
        update_hypothesis_row(&transaction, &updated)?;
        replace_hypothesis_tags(&transaction, updated.id, &final_tag_ids)?;
        if let Some(parents) = parents.as_ref() {
            replace_influence_parents(&transaction, VertexRef::Hypothesis(updated.id), parents)?;
        }
        record_event(
            &transaction,
            "hypothesis",
            &updated.id.to_string(),
            updated.revision,
            "updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn open_experiment(
        &mut self,
        request: OpenExperimentRequest,
    ) -> Result<ExperimentRecord, StoreError> {
        self.open_experiment_with_origin(request, MutationOrigin::Supervisor)
    }

    pub fn open_experiment_from_mcp(
        &mut self,
        request: OpenExperimentRequest,
    ) -> Result<ExperimentRecord, StoreError> {
        self.open_experiment_with_origin(request, MutationOrigin::Mcp)
    }

    fn open_experiment_with_origin(
        &mut self,
        request: OpenExperimentRequest,
        origin: MutationOrigin,
    ) -> Result<ExperimentRecord, StoreError> {
        let tag_ids = self.resolve_tag_set(&request.tags, origin)?;
        self.assert_tag_policy_for_assignment(&request.tags, origin)?;
        let hypothesis = self.resolve_hypothesis(&request.hypothesis)?;
        let id = ExperimentId::fresh();
        let slug = self.unique_experiment_slug(request.slug, &request.title)?;
        let now = OffsetDateTime::now_utc();
        let record = ExperimentRecord {
            id,
            slug,
            frontier_id: hypothesis.frontier_id,
            hypothesis_id: hypothesis.id,
            archived: false,
            title: request.title,
            summary: request.summary,
            tags: request.tags.iter().cloned().collect(),
            status: ExperimentStatus::Open,
            outcome: None,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let parents = self.resolve_vertex_parents(
            hypothesis.frontier_id,
            &request.parents,
            Some(VertexRef::Experiment(id)),
        )?;
        let transaction = self.connection.transaction()?;
        insert_experiment(&transaction, &record)?;
        replace_experiment_tags(&transaction, record.id, &tag_ids)?;
        replace_influence_parents(&transaction, VertexRef::Experiment(id), &parents)?;
        record_event(
            &transaction,
            "experiment",
            &record.id.to_string(),
            1,
            "opened",
            &record,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn list_experiments(
        &self,
        query: ListExperimentsQuery,
    ) -> Result<Vec<ExperimentSummary>, StoreError> {
        let frontier_id = query
            .frontier
            .as_deref()
            .map(|selector| self.resolve_frontier(selector).map(|frontier| frontier.id))
            .transpose()?;
        let hypothesis_id = query
            .hypothesis
            .as_deref()
            .map(|selector| {
                self.resolve_hypothesis(selector)
                    .map(|hypothesis| hypothesis.id)
            })
            .transpose()?;
        let records =
            self.load_experiment_records(frontier_id, hypothesis_id, query.include_archived)?;
        let filtered = records
            .into_iter()
            .filter(|record| query.status.is_none_or(|status| record.status == status))
            .filter(|record| {
                query.tags.is_empty() || query.tags.iter().all(|tag| record.tags.contains(tag))
            })
            .map(|record| self.experiment_summary_from_record(record))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(apply_limit(filtered, query.limit))
    }

    pub fn read_experiment(&self, selector: &str) -> Result<ExperimentDetail, StoreError> {
        let record = self.resolve_experiment(selector)?;
        Ok(ExperimentDetail {
            artifacts: self.list_artifacts(ListArtifactsQuery {
                attached_to: Some(AttachmentSelector::Experiment(record.id.to_string())),
                limit: None,
                ..ListArtifactsQuery::default()
            })?,
            children: self.load_vertex_children(VertexRef::Experiment(record.id))?,
            owning_hypothesis: self
                .hypothesis_summary_from_record(self.hypothesis_by_id(record.hypothesis_id)?)?,
            parents: self.load_vertex_parents(VertexRef::Experiment(record.id))?,
            record,
        })
    }

    pub fn update_experiment(
        &mut self,
        request: UpdateExperimentRequest,
    ) -> Result<ExperimentRecord, StoreError> {
        self.update_experiment_with_origin(request, MutationOrigin::Supervisor)
    }

    pub fn update_experiment_from_mcp(
        &mut self,
        request: UpdateExperimentRequest,
    ) -> Result<ExperimentRecord, StoreError> {
        self.update_experiment_with_origin(request, MutationOrigin::Mcp)
    }

    fn update_experiment_with_origin(
        &mut self,
        request: UpdateExperimentRequest,
        origin: MutationOrigin,
    ) -> Result<ExperimentRecord, StoreError> {
        let record = self.resolve_experiment(&request.experiment)?;
        enforce_revision(
            "experiment",
            &request.experiment,
            request.expected_revision,
            record.revision,
        )?;
        let tag_ids = request
            .tags
            .as_ref()
            .map(|tags| {
                self.assert_tag_policy_for_assignment(tags, origin)?;
                self.resolve_tag_set(tags, origin)
            })
            .transpose()?;
        let outcome = match request.outcome {
            Some(patch) => Some(self.materialize_outcome(
                &patch,
                record.outcome.as_ref(),
                record.frontier_id,
                origin,
            )?),
            None => record.outcome.clone(),
        };
        let updated = ExperimentRecord {
            title: request.title.unwrap_or(record.title.clone()),
            summary: apply_optional_text_patch(request.summary, record.summary.clone()),
            tags: request
                .tags
                .clone()
                .map_or_else(|| record.tags.clone(), |tags| tags.into_iter().collect()),
            archived: request.archived.unwrap_or(record.archived),
            status: if outcome.is_some() {
                ExperimentStatus::Closed
            } else {
                record.status
            },
            outcome,
            revision: record.revision.saturating_add(1),
            updated_at: OffsetDateTime::now_utc(),
            ..record
        };
        let parents = request
            .parents
            .as_ref()
            .map(|selectors| {
                self.resolve_vertex_parents(
                    updated.frontier_id,
                    selectors,
                    Some(VertexRef::Experiment(updated.id)),
                )
            })
            .transpose()?;
        let final_tag_ids = match tag_ids {
            Some(tag_ids) => tag_ids,
            None => self.resolve_existing_tag_names(&updated.tags)?,
        };
        let transaction = self.connection.transaction()?;
        update_experiment_row(&transaction, &updated)?;
        replace_experiment_tags(&transaction, updated.id, &final_tag_ids)?;
        replace_experiment_dimensions(&transaction, updated.id, updated.outcome.as_ref())?;
        replace_experiment_metrics(&transaction, updated.id, updated.outcome.as_ref())?;
        if let Some(parents) = parents.as_ref() {
            replace_influence_parents(&transaction, VertexRef::Experiment(updated.id), parents)?;
        }
        record_event(
            &transaction,
            "experiment",
            &updated.id.to_string(),
            updated.revision,
            "updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn close_experiment(
        &mut self,
        request: CloseExperimentRequest,
    ) -> Result<ExperimentRecord, StoreError> {
        self.close_experiment_with_origin(request, MutationOrigin::Supervisor)
    }

    pub fn close_experiment_from_mcp(
        &mut self,
        request: CloseExperimentRequest,
    ) -> Result<ExperimentRecord, StoreError> {
        self.close_experiment_with_origin(request, MutationOrigin::Mcp)
    }

    fn close_experiment_with_origin(
        &mut self,
        request: CloseExperimentRequest,
        origin: MutationOrigin,
    ) -> Result<ExperimentRecord, StoreError> {
        let record = self.resolve_experiment(&request.experiment)?;
        if record.status == ExperimentStatus::Closed {
            return Err(StoreError::ExperimentAlreadyClosed(record.id));
        }
        enforce_revision(
            "experiment",
            &request.experiment,
            request.expected_revision,
            record.revision,
        )?;
        let outcome = self.materialize_outcome(
            &ExperimentOutcomePatch {
                backend: request.backend,
                command: request.command,
                dimensions: request.dimensions,
                primary_metric: request.primary_metric,
                supporting_metrics: request.supporting_metrics,
                verdict: request.verdict,
                rationale: request.rationale,
                analysis: request.analysis,
            },
            None,
            record.frontier_id,
            origin,
        )?;
        let updated = ExperimentRecord {
            status: ExperimentStatus::Closed,
            outcome: Some(outcome),
            revision: record.revision.saturating_add(1),
            updated_at: OffsetDateTime::now_utc(),
            ..record
        };
        let transaction = self.connection.transaction()?;
        update_experiment_row(&transaction, &updated)?;
        replace_experiment_dimensions(&transaction, updated.id, updated.outcome.as_ref())?;
        replace_experiment_metrics(&transaction, updated.id, updated.outcome.as_ref())?;
        record_event(
            &transaction,
            "experiment",
            &updated.id.to_string(),
            updated.revision,
            "closed",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn create_artifact(
        &mut self,
        request: CreateArtifactRequest,
    ) -> Result<ArtifactRecord, StoreError> {
        let id = ArtifactId::fresh();
        let slug = self.unique_artifact_slug(request.slug, &request.label)?;
        let now = OffsetDateTime::now_utc();
        let record = ArtifactRecord {
            id,
            slug,
            kind: request.kind,
            label: request.label,
            summary: request.summary,
            locator: request.locator,
            media_type: request.media_type,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let attachments = self.resolve_attachment_targets(&request.attachments)?;
        let transaction = self.connection.transaction()?;
        insert_artifact(&transaction, &record)?;
        replace_artifact_attachments(&transaction, record.id, &attachments)?;
        record_event(
            &transaction,
            "artifact",
            &record.id.to_string(),
            1,
            "created",
            &record,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn list_artifacts(
        &self,
        query: ListArtifactsQuery,
    ) -> Result<Vec<ArtifactSummary>, StoreError> {
        let records = self.load_artifact_records()?;
        let frontier_id = query
            .frontier
            .as_deref()
            .map(|selector| self.resolve_frontier(selector).map(|frontier| frontier.id))
            .transpose()?;
        let mut filtered = Vec::new();
        for record in records {
            if query.kind.is_some_and(|kind| record.kind != kind) {
                continue;
            }
            if let Some(frontier_id) = frontier_id
                && !self.artifact_attached_to_frontier(record.id, frontier_id)?
            {
                continue;
            }
            filtered.push(record);
        }
        let attached_filtered = match query.attached_to {
            Some(selector) => {
                let target = self.resolve_attachment_target(&selector)?;
                filtered
                    .into_iter()
                    .filter(|record| {
                        self.artifact_attachment_targets(record.id)
                            .map(|targets| targets.contains(&target))
                            .unwrap_or(false)
                    })
                    .collect()
            }
            None => filtered,
        };
        Ok(apply_limit(
            attached_filtered
                .into_iter()
                .map(|record| ArtifactSummary {
                    id: record.id,
                    slug: record.slug,
                    kind: record.kind,
                    label: record.label,
                    summary: record.summary,
                    locator: record.locator,
                    media_type: record.media_type,
                    updated_at: record.updated_at,
                })
                .collect(),
            query.limit,
        ))
    }

    pub fn read_artifact(&self, selector: &str) -> Result<ArtifactDetail, StoreError> {
        let record = self.resolve_artifact(selector)?;
        Ok(ArtifactDetail {
            attachments: self.artifact_attachment_targets(record.id)?,
            record,
        })
    }

    pub fn update_artifact(
        &mut self,
        request: UpdateArtifactRequest,
    ) -> Result<ArtifactRecord, StoreError> {
        let record = self.resolve_artifact(&request.artifact)?;
        enforce_revision(
            "artifact",
            &request.artifact,
            request.expected_revision,
            record.revision,
        )?;
        let updated = ArtifactRecord {
            kind: request.kind.unwrap_or(record.kind),
            label: request.label.unwrap_or(record.label.clone()),
            summary: apply_optional_text_patch(request.summary, record.summary.clone()),
            locator: request.locator.unwrap_or(record.locator.clone()),
            media_type: apply_optional_text_patch(request.media_type, record.media_type.clone()),
            revision: record.revision.saturating_add(1),
            updated_at: OffsetDateTime::now_utc(),
            ..record
        };
        let attachments = request
            .attachments
            .as_ref()
            .map(|selectors| self.resolve_attachment_targets(selectors))
            .transpose()?;
        let transaction = self.connection.transaction()?;
        update_artifact_row(&transaction, &updated)?;
        if let Some(attachments) = attachments.as_ref() {
            replace_artifact_attachments(&transaction, updated.id, attachments)?;
        }
        record_event(
            &transaction,
            "artifact",
            &updated.id.to_string(),
            updated.revision,
            "updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn frontier_open(&self, selector: &str) -> Result<FrontierOpenProjection, StoreError> {
        let frontier = self.resolve_frontier(selector)?;
        let active_hypothesis_ids = self.active_hypothesis_ids(frontier.id, &frontier.brief)?;
        let active_hypotheses = active_hypothesis_ids
            .into_iter()
            .map(|hypothesis_id| {
                let summary =
                    self.hypothesis_summary_from_record(self.hypothesis_by_id(hypothesis_id)?)?;
                let open_experiments = self.list_experiments(ListExperimentsQuery {
                    hypothesis: Some(hypothesis_id.to_string()),
                    status: Some(ExperimentStatus::Open),
                    limit: None,
                    ..ListExperimentsQuery::default()
                })?;
                let latest_closed_experiment = self
                    .list_experiments(ListExperimentsQuery {
                        hypothesis: Some(hypothesis_id.to_string()),
                        status: Some(ExperimentStatus::Closed),
                        limit: Some(1),
                        ..ListExperimentsQuery::default()
                    })?
                    .into_iter()
                    .next();
                Ok(HypothesisCurrentState {
                    hypothesis: summary,
                    open_experiments,
                    latest_closed_experiment,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let open_experiments = self.list_experiments(ListExperimentsQuery {
            frontier: Some(frontier.id.to_string()),
            status: Some(ExperimentStatus::Open),
            limit: None,
            ..ListExperimentsQuery::default()
        })?;
        let active_tags = derive_active_tags(&active_hypotheses, &open_experiments);
        let active_metric_keys =
            self.live_metric_keys(frontier.id, &active_hypotheses, &open_experiments)?;
        let kpis = self.frontier_kpis(frontier.id)?;
        Ok(FrontierOpenProjection {
            frontier,
            active_tags,
            kpis,
            active_metric_keys,
            active_hypotheses,
            open_experiments,
        })
    }

    pub fn frontier_metric_series(
        &self,
        frontier: &str,
        key: &NonEmptyText,
        include_rejected: bool,
    ) -> Result<FrontierMetricSeries, StoreError> {
        let frontier = self.resolve_frontier(frontier)?;
        let definition = self
            .metric_definition(key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(key.clone()))?;
        let mut points = self
            .load_experiment_records(Some(frontier.id), None, true)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Closed)
            .filter_map(|record| {
                let outcome = record.outcome.clone()?;
                if !include_rejected && outcome.verdict == FrontierVerdict::Rejected {
                    return None;
                }
                let metric = all_metrics(&outcome)
                    .into_iter()
                    .find(|metric| metric.key == *key)?;
                Some((record, outcome, metric.value))
            })
            .map(|(record, outcome, value)| {
                Ok(FrontierMetricPoint {
                    closed_at: outcome.closed_at,
                    dimensions: outcome.dimensions.clone(),
                    experiment: self.experiment_summary_from_record(record.clone())?,
                    hypothesis: self.hypothesis_summary_from_record(
                        self.hypothesis_by_id(record.hypothesis_id)?,
                    )?,
                    value,
                    metric_key: definition.key.clone(),
                    verdict: outcome.verdict,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        points.sort_by_key(|point| point.closed_at);
        Ok(FrontierMetricSeries {
            metric: self.metric_key_summary_from_definition(definition, Some(frontier.id))?,
            kpi: None,
            frontier,
            points,
        })
    }

    pub fn metric_keys(&self, query: MetricKeysQuery) -> Result<Vec<MetricKeySummary>, StoreError> {
        let frontier = query
            .frontier
            .as_deref()
            .map(|selector| self.resolve_frontier(selector))
            .transpose()?;
        let frontier_id = frontier.as_ref().map(|frontier| frontier.id);
        if query.scope == MetricScope::Kpi && frontier.is_none() {
            return Err(StoreError::MetricScopeRequiresFrontier { scope: "kpi" });
        }
        if query.scope == MetricScope::Kpi {
            return match frontier.as_ref() {
                Some(frontier) => self.frontier_kpi_metric_keys(frontier.id),
                None => Err(StoreError::MetricScopeRequiresFrontier { scope: "kpi" }),
            };
        }
        let definitions = self.list_metric_definitions()?;
        let live_keys = frontier_id
            .map(|frontier_id| self.live_metric_key_names(frontier_id))
            .transpose()?
            .unwrap_or_default();
        let mut keys = definitions
            .into_iter()
            .map(|definition| self.metric_key_summary_from_definition(definition, frontier_id))
            .filter_map(|summary| match summary {
                Ok(summary) => {
                    let keep = match query.scope {
                        MetricScope::Kpi => unreachable!("handled above"),
                        MetricScope::Live => live_keys.contains(summary.key.as_str()),
                        MetricScope::Default => summary.default_visibility.is_default_visible(),
                        MetricScope::All => true,
                    };
                    keep.then_some(Ok(summary))
                }
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        keys.sort_by(|left, right| left.key.as_str().cmp(right.key.as_str()));
        Ok(keys)
    }

    pub fn metric_best(&self, query: MetricBestQuery) -> Result<Vec<MetricBestEntry>, StoreError> {
        let definition = self
            .metric_definition(&query.key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(query.key.clone()))?;
        let frontier_id = query
            .frontier
            .as_deref()
            .map(|selector| self.resolve_frontier(selector).map(|frontier| frontier.id))
            .transpose()?;
        let hypothesis_id = query
            .hypothesis
            .as_deref()
            .map(|selector| {
                self.resolve_hypothesis(selector)
                    .map(|hypothesis| hypothesis.id)
            })
            .transpose()?;
        let order = query.order.unwrap_or(match definition.objective {
            OptimizationObjective::Minimize => MetricRankOrder::Asc,
            OptimizationObjective::Maximize => MetricRankOrder::Desc,
            OptimizationObjective::Target => {
                return Err(StoreError::MetricOrderRequired {
                    key: query.key.to_string(),
                });
            }
        });
        let experiments = self
            .load_experiment_records(frontier_id, hypothesis_id, true)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Closed)
            .filter(|record| {
                query.include_rejected
                    || record
                        .outcome
                        .as_ref()
                        .is_some_and(|outcome| outcome.verdict != FrontierVerdict::Rejected)
            })
            .collect::<Vec<_>>();
        let mut entries = experiments
            .into_iter()
            .filter_map(|record| {
                let outcome = record.outcome.clone()?;
                if !dimension_subset_matches(&query.dimensions, &outcome.dimensions) {
                    return None;
                }
                let metric = all_metrics(&outcome)
                    .into_iter()
                    .find(|metric| metric.key == query.key)?;
                Some((record, outcome.dimensions.clone(), metric.value))
            })
            .map(|(record, dimensions, display_value)| {
                Ok(MetricBestEntry {
                    experiment: self.experiment_summary_from_record(record.clone())?,
                    hypothesis: self.hypothesis_summary_from_record(
                        self.hypothesis_by_id(record.hypothesis_id)?,
                    )?,
                    value: display_value,
                    dimensions,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        entries.sort_by(|left, right| compare_metric_values(left.value, right.value, order));
        Ok(apply_limit(entries, query.limit))
    }

    pub fn experiment_nearest(
        &self,
        query: ExperimentNearestQuery,
    ) -> Result<ExperimentNearestResult, StoreError> {
        let anchor_experiment = query
            .experiment
            .as_deref()
            .map(|selector| self.resolve_experiment(selector))
            .transpose()?;
        let anchor_hypothesis = query
            .hypothesis
            .as_deref()
            .map(|selector| self.resolve_hypothesis(selector))
            .transpose()?;
        let frontier = match query.frontier.as_deref() {
            Some(selector) => Some(self.resolve_frontier(selector)?),
            None => anchor_experiment
                .as_ref()
                .map(|experiment| self.resolve_frontier(&experiment.frontier_id.to_string()))
                .transpose()?
                .or(anchor_hypothesis
                    .as_ref()
                    .map(|hypothesis| self.resolve_frontier(&hypothesis.frontier_id.to_string()))
                    .transpose()?),
        };
        let frontier_id = frontier.as_ref().map(|frontier| frontier.id);
        let anchor_hypothesis_id = anchor_hypothesis
            .as_ref()
            .map(|hypothesis| hypothesis.id)
            .or_else(|| {
                anchor_experiment
                    .as_ref()
                    .map(|experiment| experiment.hypothesis_id)
            });
        let target_dimensions = if query.dimensions.is_empty() {
            anchor_experiment
                .as_ref()
                .and_then(|experiment| {
                    experiment
                        .outcome
                        .as_ref()
                        .map(|outcome| outcome.dimensions.clone())
                })
                .unwrap_or_default()
        } else {
            query.dimensions
        };
        let metric_definition = match query.metric.as_ref() {
            Some(key) => Some(
                self.metric_definition(key)?
                    .ok_or_else(|| StoreError::UnknownMetricDefinition(key.clone()))?,
            ),
            None => frontier
                .as_ref()
                .map(|frontier| self.frontier_kpis(frontier.id))
                .transpose()?
                .and_then(|kpis| {
                    kpis.into_iter()
                        .next()
                        .and_then(|kpi| kpi.metrics.into_iter().next())
                        .map(|metric| metric.key)
                })
                .map(|key| {
                    self.metric_definition(&key)?
                        .ok_or_else(|| StoreError::UnknownMetricDefinition(key))
                })
                .transpose()?,
        };
        let champion_order = metric_definition.as_ref().and_then(|definition| {
            query.order.or(match definition.objective {
                OptimizationObjective::Minimize => Some(MetricRankOrder::Asc),
                OptimizationObjective::Maximize => Some(MetricRankOrder::Desc),
                OptimizationObjective::Target => None,
            })
        });
        let influence_neighborhood =
            self.influence_neighborhood(anchor_experiment.as_ref(), anchor_hypothesis_id)?;
        let candidates = self
            .load_experiment_records(frontier_id, None, false)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Closed)
            .filter(|record| {
                anchor_experiment
                    .as_ref()
                    .is_none_or(|anchor| record.id != anchor.id)
            })
            .filter(|record| {
                anchor_hypothesis_id.is_none_or(|hypothesis_id| {
                    anchor_hypothesis.is_none() || record.hypothesis_id == hypothesis_id
                })
            })
            .map(|record| {
                let Some(outcome) = record.outcome.clone() else {
                    return Ok(None);
                };
                let hypothesis_record = self.hypothesis_by_id(record.hypothesis_id)?;
                if !query.tags.is_empty() {
                    let candidate_tags = record
                        .tags
                        .iter()
                        .cloned()
                        .chain(hypothesis_record.tags.iter().cloned())
                        .collect::<BTreeSet<_>>();
                    if !query.tags.iter().all(|tag| candidate_tags.contains(tag)) {
                        return Ok(None);
                    }
                }
                let structural_rank = comparator_rank(
                    &target_dimensions,
                    &outcome.dimensions,
                    anchor_hypothesis_id,
                    hypothesis_record.id,
                    record.id,
                    &influence_neighborhood,
                );
                let metric_value = metric_definition.as_ref().and_then(|definition| {
                    all_metrics(&outcome)
                        .into_iter()
                        .find(|metric| metric.key == definition.key)
                        .map(|metric| MetricObservationSummary {
                            key: metric.key.clone(),
                            value: metric.value,
                            unit: definition.unit.clone(),
                            dimension: definition.dimension,
                            objective: definition.objective,
                        })
                });
                Ok(Some(NearestComparatorCandidate {
                    closed_at: outcome.closed_at,
                    verdict: outcome.verdict,
                    experiment: self.experiment_summary_from_record(record)?,
                    hypothesis: self.hypothesis_summary_from_record(hypothesis_record)?,
                    dimensions: outcome.dimensions,
                    structural_rank,
                    metric_value,
                }))
            })
            .collect::<Result<Vec<_>, StoreError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let metric = metric_definition
            .clone()
            .map(|definition| self.metric_key_summary_from_definition(definition, frontier_id))
            .transpose()?;
        Ok(ExperimentNearestResult {
            metric,
            target_dimensions,
            accepted: pick_nearest_bucket(
                &candidates,
                FrontierVerdict::Accepted,
                metric_definition
                    .as_ref()
                    .map(|definition| definition.key.as_str()),
            ),
            kept: pick_nearest_bucket(
                &candidates,
                FrontierVerdict::Kept,
                metric_definition
                    .as_ref()
                    .map(|definition| definition.key.as_str()),
            ),
            rejected: pick_nearest_bucket(
                &candidates,
                FrontierVerdict::Rejected,
                metric_definition
                    .as_ref()
                    .map(|definition| definition.key.as_str()),
            ),
            champion: champion_order.and_then(|order| {
                pick_champion_candidate(
                    &candidates,
                    order,
                    metric_definition
                        .as_ref()
                        .map(|definition| definition.key.as_str()),
                )
            }),
        })
    }

    pub fn frontier_history(&self, selector: &str) -> Result<Vec<EntityHistoryEntry>, StoreError> {
        let frontier = self.resolve_frontier(selector)?;
        self.entity_history("frontier", &frontier.id.to_string())
    }

    pub fn hypothesis_history(
        &self,
        selector: &str,
    ) -> Result<Vec<EntityHistoryEntry>, StoreError> {
        let hypothesis = self.resolve_hypothesis(selector)?;
        self.entity_history("hypothesis", &hypothesis.id.to_string())
    }

    pub fn experiment_history(
        &self,
        selector: &str,
    ) -> Result<Vec<EntityHistoryEntry>, StoreError> {
        let experiment = self.resolve_experiment(selector)?;
        self.entity_history("experiment", &experiment.id.to_string())
    }

    pub fn artifact_history(&self, selector: &str) -> Result<Vec<EntityHistoryEntry>, StoreError> {
        let artifact = self.resolve_artifact(selector)?;
        self.entity_history("artifact", &artifact.id.to_string())
    }

    fn metric_definition(
        &self,
        key: &NonEmptyText,
    ) -> Result<Option<MetricDefinition>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, key, dimension, display_unit, aggregation, objective, description, revision, created_at, updated_at
                 FROM metric_definitions
                 WHERE key = ?1",
                params![key.as_str()],
                decode_metric_definition_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn metric_definition_by_id(&self, id: MetricId) -> Result<MetricDefinition, StoreError> {
        self.connection
            .query_row(
                "SELECT id, key, dimension, display_unit, aggregation, objective, description, revision, created_at, updated_at
                 FROM metric_definitions
                 WHERE id = ?1",
                params![id.to_string()],
                decode_metric_definition_row,
            )
            .map_err(StoreError::from)
    }

    fn resolve_kpi_metric_definitions(
        &self,
        kpi_name: &NonEmptyText,
        objective: OptimizationObjective,
        metric_keys: &[NonEmptyText],
    ) -> Result<Vec<MetricDefinition>, StoreError> {
        let mut definitions = Vec::with_capacity(metric_keys.len());
        let mut dimension = None;
        for key in metric_keys {
            let definition = self
                .metric_definition(key)?
                .ok_or_else(|| StoreError::UnknownMetricDefinition(key.clone()))?;
            if definition.objective != objective {
                return Err(StoreError::KpiMetricObjectiveMismatch {
                    kpi: kpi_name.clone(),
                    metric: key.clone(),
                });
            }
            if let Some(expected) = dimension {
                if definition.dimension != expected {
                    return Err(StoreError::KpiMetricDimensionMismatch {
                        kpi: kpi_name.clone(),
                        metric: key.clone(),
                    });
                }
            } else {
                dimension = Some(definition.dimension);
            }
            definitions.push(definition);
        }
        Ok(definitions)
    }

    fn kpi_by_name(
        &self,
        frontier_id: FrontierId,
        name: &str,
    ) -> Result<Option<FrontierKpiRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, frontier_id, name, objective, description, status, revision, created_at, updated_at
                 FROM frontier_kpis
                 WHERE frontier_id = ?1 AND name = ?2",
                params![frontier_id.to_string(), name],
                decode_kpi_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn kpi_by_selector(
        &self,
        frontier_id: FrontierId,
        selector: &str,
    ) -> Result<Option<FrontierKpiRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, frontier_id, name, objective, description, status, revision, created_at, updated_at
                 FROM frontier_kpis
                 WHERE frontier_id = ?1 AND (name = ?2 OR id = ?2)",
                params![frontier_id.to_string(), selector],
                decode_kpi_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn frontier_kpi_records(
        &self,
        frontier_id: FrontierId,
    ) -> Result<Vec<FrontierKpiRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, frontier_id, name, objective, description, status, revision, created_at, updated_at
             FROM frontier_kpis
             WHERE frontier_id = ?1 AND status = 'active'
             ORDER BY created_at ASC",
        )?;
        let rows = statement.query_map(params![frontier_id.to_string()], decode_kpi_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn kpi_alternatives(
        &self,
        kpi_id: KpiId,
    ) -> Result<Vec<KpiMetricAlternativeRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT alternatives.kpi_id, alternatives.metric_id, definitions.key, alternatives.precedence
             FROM kpi_metric_alternatives alternatives
             JOIN metric_definitions definitions ON definitions.id = alternatives.metric_id
             WHERE alternatives.kpi_id = ?1
             ORDER BY alternatives.precedence ASC",
        )?;
        let rows = statement.query_map(params![kpi_id.to_string()], decode_kpi_alternative_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn kpi_summary(&self, record: FrontierKpiRecord) -> Result<KpiSummary, StoreError> {
        let metrics = self
            .kpi_alternatives(record.id)?
            .into_iter()
            .map(|alternative| {
                let definition = self.metric_definition_by_id(alternative.metric_id)?;
                Ok(KpiMetricSummary {
                    key: definition.key,
                    precedence: alternative.precedence,
                    unit: definition.unit,
                    dimension: definition.dimension,
                    aggregation: definition.aggregation,
                    objective: definition.objective,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(KpiSummary {
            id: record.id,
            name: record.name,
            objective: record.objective,
            description: record.description,
            metrics,
            revision: record.revision,
        })
    }

    fn frontier_kpis(&self, frontier_id: FrontierId) -> Result<Vec<KpiSummary>, StoreError> {
        self.frontier_kpi_records(frontier_id)?
            .into_iter()
            .map(|record| self.kpi_summary(record))
            .collect()
    }

    fn resolve_kpi_for_query(
        &self,
        frontier_id: FrontierId,
        selector: Option<&str>,
    ) -> Result<KpiSummary, StoreError> {
        let kpis = self.frontier_kpis(frontier_id)?;
        match selector {
            Some(selector) => kpis
                .into_iter()
                .find(|kpi| kpi.name.as_str() == selector || kpi.id.to_string() == selector)
                .ok_or_else(|| StoreError::UnknownKpi(selector.to_owned())),
            None => kpis
                .into_iter()
                .next()
                .ok_or_else(|| StoreError::UnknownKpi("frontier has no KPIs".to_owned())),
        }
    }

    fn run_dimension_definition(
        &self,
        key: &NonEmptyText,
    ) -> Result<Option<RunDimensionDefinition>, StoreError> {
        self.connection
            .query_row(
                "SELECT key, value_type, description, created_at, updated_at
                 FROM run_dimension_definitions
                 WHERE key = ?1",
                params![key.as_str()],
                decode_run_dimension_definition_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn hypothesis_by_id(&self, id: HypothesisId) -> Result<HypothesisRecord, StoreError> {
        self.connection
            .query_row(
                "SELECT id, slug, frontier_id, archived, title, summary, body, revision, created_at, updated_at
                 FROM hypotheses WHERE id = ?1",
                params![id.to_string()],
                |row| self.decode_hypothesis_row(row),
            )
            .map_err(StoreError::from)
    }

    fn resolve_frontier(&self, selector: &str) -> Result<FrontierRecord, StoreError> {
        let record = match resolve_selector(selector)? {
            Selector::Id(uuid) => self
                .connection
                .query_row(
                    "SELECT id, slug, label, objective, status, brief_json, revision, created_at, updated_at
                     FROM frontiers WHERE id = ?1",
                    params![uuid.to_string()],
                    decode_frontier_row,
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, label, objective, status, brief_json, revision, created_at, updated_at
                     FROM frontiers WHERE slug = ?1",
                    params![slug.as_str()],
                    decode_frontier_row,
                )
                .optional()?,
        };
        record.ok_or_else(|| StoreError::UnknownFrontierSelector(selector.to_owned()))
    }

    fn resolve_hypothesis(&self, selector: &str) -> Result<HypothesisRecord, StoreError> {
        let record = match resolve_selector(selector)? {
            Selector::Id(uuid) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, archived, title, summary, body, revision, created_at, updated_at
                     FROM hypotheses WHERE id = ?1",
                    params![uuid.to_string()],
                    |row| self.decode_hypothesis_row(row),
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, archived, title, summary, body, revision, created_at, updated_at
                     FROM hypotheses WHERE slug = ?1",
                    params![slug.as_str()],
                    |row| self.decode_hypothesis_row(row),
                )
                .optional()?,
        };
        record.ok_or_else(|| StoreError::UnknownHypothesisSelector(selector.to_owned()))
    }

    fn resolve_experiment(&self, selector: &str) -> Result<ExperimentRecord, StoreError> {
        let record = match resolve_selector(selector)? {
            Selector::Id(uuid) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, hypothesis_id, archived, title, summary, status, outcome_json, revision, created_at, updated_at
                     FROM experiments WHERE id = ?1",
                    params![uuid.to_string()],
                    decode_experiment_row,
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, hypothesis_id, archived, title, summary, status, outcome_json, revision, created_at, updated_at
                     FROM experiments WHERE slug = ?1",
                    params![slug.as_str()],
                    decode_experiment_row,
                )
                .optional()?,
        };
        record
            .ok_or_else(|| StoreError::UnknownExperimentSelector(selector.to_owned()))
            .and_then(|record| self.hydrate_experiment_tags(record))
    }

    fn resolve_artifact(&self, selector: &str) -> Result<ArtifactRecord, StoreError> {
        let record = match resolve_selector(selector)? {
            Selector::Id(uuid) => self
                .connection
                .query_row(
                    "SELECT id, slug, kind, label, summary, locator, media_type, revision, created_at, updated_at
                     FROM artifacts WHERE id = ?1",
                    params![uuid.to_string()],
                    decode_artifact_row,
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, kind, label, summary, locator, media_type, revision, created_at, updated_at
                     FROM artifacts WHERE slug = ?1",
                    params![slug.as_str()],
                    decode_artifact_row,
                )
                .optional()?,
        };
        record.ok_or_else(|| StoreError::UnknownArtifactSelector(selector.to_owned()))
    }

    fn resolve_vertex_parents(
        &self,
        frontier_id: FrontierId,
        selectors: &[VertexSelector],
        child: Option<VertexRef>,
    ) -> Result<Vec<VertexRef>, StoreError> {
        selectors
            .iter()
            .map(|selector| {
                let vertex = match selector {
                    VertexSelector::Hypothesis(selector) => {
                        VertexRef::Hypothesis(self.resolve_hypothesis(selector)?.id)
                    }
                    VertexSelector::Experiment(selector) => {
                        VertexRef::Experiment(self.resolve_experiment(selector)?.id)
                    }
                };
                let parent_frontier_id = match vertex {
                    VertexRef::Hypothesis(id) => self.hypothesis_by_id(id)?.frontier_id,
                    VertexRef::Experiment(id) => {
                        self.resolve_experiment(&id.to_string())?.frontier_id
                    }
                };
                if parent_frontier_id != frontier_id {
                    return Err(StoreError::CrossFrontierInfluence);
                }
                if child.is_some_and(|child| child == vertex) {
                    return Err(StoreError::SelfEdge);
                }
                Ok(vertex)
            })
            .collect()
    }

    fn resolve_attachment_targets(
        &self,
        selectors: &[AttachmentSelector],
    ) -> Result<Vec<AttachmentTargetRef>, StoreError> {
        selectors
            .iter()
            .map(|selector| match selector {
                AttachmentSelector::Frontier(selector) => Ok(AttachmentTargetRef::Frontier(
                    self.resolve_frontier(selector)?.id,
                )),
                AttachmentSelector::Hypothesis(selector) => Ok(AttachmentTargetRef::Hypothesis(
                    self.resolve_hypothesis(selector)?.id,
                )),
                AttachmentSelector::Experiment(selector) => Ok(AttachmentTargetRef::Experiment(
                    self.resolve_experiment(selector)?.id,
                )),
            })
            .collect()
    }

    fn resolve_attachment_target(
        &self,
        selector: &AttachmentSelector,
    ) -> Result<AttachmentTargetRef, StoreError> {
        match selector {
            AttachmentSelector::Frontier(selector) => Ok(AttachmentTargetRef::Frontier(
                self.resolve_frontier(selector)?.id,
            )),
            AttachmentSelector::Hypothesis(selector) => Ok(AttachmentTargetRef::Hypothesis(
                self.resolve_hypothesis(selector)?.id,
            )),
            AttachmentSelector::Experiment(selector) => Ok(AttachmentTargetRef::Experiment(
                self.resolve_experiment(selector)?.id,
            )),
        }
    }

    fn influence_neighborhood(
        &self,
        anchor_experiment: Option<&ExperimentRecord>,
        anchor_hypothesis_id: Option<HypothesisId>,
    ) -> Result<Vec<VertexRef>, StoreError> {
        let mut neighborhood = Vec::new();
        if let Some(hypothesis_id) = anchor_hypothesis_id {
            let anchor = VertexRef::Hypothesis(hypothesis_id);
            neighborhood.extend(
                self.load_vertex_parents(anchor)?
                    .into_iter()
                    .map(|summary| summary.vertex),
            );
            neighborhood.extend(
                self.load_vertex_children(anchor)?
                    .into_iter()
                    .map(|summary| summary.vertex),
            );
        }
        if let Some(experiment) = anchor_experiment {
            let anchor = VertexRef::Experiment(experiment.id);
            neighborhood.extend(
                self.load_vertex_parents(anchor)?
                    .into_iter()
                    .map(|summary| summary.vertex),
            );
            neighborhood.extend(
                self.load_vertex_children(anchor)?
                    .into_iter()
                    .map(|summary| summary.vertex),
            );
        }
        Ok(neighborhood)
    }

    fn load_hypothesis_records(
        &self,
        frontier_id: Option<FrontierId>,
        include_archived: bool,
    ) -> Result<Vec<HypothesisRecord>, StoreError> {
        let mut records = if let Some(frontier_id) = frontier_id {
            let mut statement = self.connection.prepare(
                "SELECT id, slug, frontier_id, archived, title, summary, body, revision, created_at, updated_at
                 FROM hypotheses
                 WHERE frontier_id = ?1
                 ORDER BY updated_at DESC, created_at DESC",
            )?;
            let rows = statement.query_map(params![frontier_id.to_string()], |row| {
                self.decode_hypothesis_row(row)
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut statement = self.connection.prepare(
                "SELECT id, slug, frontier_id, archived, title, summary, body, revision, created_at, updated_at
                 FROM hypotheses
                 ORDER BY updated_at DESC, created_at DESC",
            )?;
            let rows = statement.query_map([], |row| self.decode_hypothesis_row(row))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        if !include_archived {
            records.retain(|record| !record.archived);
        }
        Ok(records)
    }

    fn load_experiment_records(
        &self,
        frontier_id: Option<FrontierId>,
        hypothesis_id: Option<HypothesisId>,
        include_archived: bool,
    ) -> Result<Vec<ExperimentRecord>, StoreError> {
        let base_sql = "SELECT id, slug, frontier_id, hypothesis_id, archived, title, summary, status, outcome_json, revision, created_at, updated_at FROM experiments";
        let records = match (frontier_id, hypothesis_id) {
            (Some(frontier_id), Some(hypothesis_id)) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} WHERE frontier_id = ?1 AND hypothesis_id = ?2 ORDER BY updated_at DESC, created_at DESC"
                ))?;
                let rows = statement.query_map(
                    params![frontier_id.to_string(), hypothesis_id.to_string()],
                    decode_experiment_row,
                )?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (Some(frontier_id), None) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} WHERE frontier_id = ?1 ORDER BY updated_at DESC, created_at DESC"
                ))?;
                let rows =
                    statement.query_map(params![frontier_id.to_string()], decode_experiment_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (None, Some(hypothesis_id)) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} WHERE hypothesis_id = ?1 ORDER BY updated_at DESC, created_at DESC"
                ))?;
                let rows = statement
                    .query_map(params![hypothesis_id.to_string()], decode_experiment_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (None, None) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} ORDER BY updated_at DESC, created_at DESC"
                ))?;
                let rows = statement.query_map([], decode_experiment_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
        };
        let records = records
            .into_iter()
            .map(|record| self.hydrate_experiment_tags(record))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(if include_archived {
            records
        } else {
            records
                .into_iter()
                .filter(|record| !record.archived)
                .collect()
        })
    }

    fn load_artifact_records(&self) -> Result<Vec<ArtifactRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, slug, kind, label, summary, locator, media_type, revision, created_at, updated_at
             FROM artifacts
             ORDER BY updated_at DESC, created_at DESC",
        )?;
        let rows = statement.query_map([], decode_artifact_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn decode_hypothesis_row(
        &self,
        row: &rusqlite::Row<'_>,
    ) -> Result<HypothesisRecord, rusqlite::Error> {
        let id = HypothesisId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?);
        Ok(HypothesisRecord {
            id,
            slug: parse_slug(&row.get::<_, String>(1)?)?,
            frontier_id: FrontierId::from_uuid(parse_uuid_sql(&row.get::<_, String>(2)?)?),
            archived: row.get::<_, i64>(3)? != 0,
            title: parse_non_empty_text(&row.get::<_, String>(4)?)?,
            summary: parse_non_empty_text(&row.get::<_, String>(5)?)?,
            body: parse_non_empty_text(&row.get::<_, String>(6)?)?,
            tags: self.hypothesis_tags(id)?,
            revision: row.get::<_, u64>(7)?,
            created_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
            updated_at: parse_timestamp_sql(&row.get::<_, String>(9)?)?,
        })
    }

    fn hypothesis_tags(&self, id: HypothesisId) -> Result<Vec<TagName>, rusqlite::Error> {
        let mut statement = self.connection.prepare(
            "SELECT tags.name
             FROM hypothesis_tags
             JOIN tags ON tags.id = hypothesis_tags.tag_id
             WHERE hypothesis_tags.hypothesis_id = ?1
             ORDER BY tags.name ASC",
        )?;
        let rows = statement.query_map(params![id.to_string()], |row| {
            parse_tag_name(&row.get::<_, String>(0)?)
        })?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    fn experiment_tags(&self, id: ExperimentId) -> Result<Vec<TagName>, rusqlite::Error> {
        let mut statement = self.connection.prepare(
            "SELECT tags.name
             FROM experiment_tags
             JOIN tags ON tags.id = experiment_tags.tag_id
             WHERE experiment_tags.experiment_id = ?1
             ORDER BY tags.name ASC",
        )?;
        let rows = statement.query_map(params![id.to_string()], |row| {
            parse_tag_name(&row.get::<_, String>(0)?)
        })?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    fn hydrate_experiment_tags(
        &self,
        mut record: ExperimentRecord,
    ) -> Result<ExperimentRecord, StoreError> {
        record.tags = self.experiment_tags(record.id)?;
        Ok(record)
    }

    fn hypothesis_summary_from_record(
        &self,
        record: HypothesisRecord,
    ) -> Result<HypothesisSummary, StoreError> {
        let latest_verdict = self
            .latest_closed_experiment(record.id)?
            .and_then(|experiment| experiment.outcome.map(|outcome| outcome.verdict));
        Ok(HypothesisSummary {
            id: record.id,
            slug: record.slug,
            frontier_id: record.frontier_id,
            archived: record.archived,
            title: record.title,
            summary: record.summary,
            tags: record.tags,
            open_experiment_count: self
                .list_experiments(ListExperimentsQuery {
                    hypothesis: Some(record.id.to_string()),
                    status: Some(ExperimentStatus::Open),
                    limit: None,
                    ..ListExperimentsQuery::default()
                })?
                .len() as u64,
            latest_verdict,
            updated_at: record.updated_at,
        })
    }

    fn experiment_summary_from_record(
        &self,
        record: ExperimentRecord,
    ) -> Result<ExperimentSummary, StoreError> {
        Ok(ExperimentSummary {
            id: record.id,
            slug: record.slug,
            frontier_id: record.frontier_id,
            hypothesis_id: record.hypothesis_id,
            archived: record.archived,
            title: record.title,
            summary: record.summary,
            tags: record.tags,
            status: record.status,
            verdict: record.outcome.as_ref().map(|outcome| outcome.verdict),
            primary_metric: record
                .outcome
                .as_ref()
                .map(|outcome| self.metric_observation_summary(&outcome.primary_metric))
                .transpose()?,
            updated_at: record.updated_at,
            closed_at: record.outcome.as_ref().map(|outcome| outcome.closed_at),
        })
    }

    fn metric_observation_summary(
        &self,
        metric: &MetricValue,
    ) -> Result<MetricObservationSummary, StoreError> {
        let definition = self
            .metric_definition(&metric.key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))?;
        Ok(MetricObservationSummary {
            key: metric.key.clone(),
            value: metric.value,
            unit: definition.unit,
            dimension: definition.dimension,
            objective: definition.objective,
        })
    }

    fn latest_closed_experiment(
        &self,
        hypothesis_id: HypothesisId,
    ) -> Result<Option<ExperimentRecord>, StoreError> {
        self.load_experiment_records(None, Some(hypothesis_id), true)
            .map(|records| {
                records
                    .into_iter()
                    .filter(|record| record.status == ExperimentStatus::Closed)
                    .max_by_key(|record| {
                        record
                            .outcome
                            .as_ref()
                            .map(|outcome| outcome.closed_at)
                            .unwrap_or(record.updated_at)
                    })
            })
    }

    fn load_vertex_parents(&self, child: VertexRef) -> Result<Vec<VertexSummary>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT parent_kind, parent_id
             FROM influence_edges
             WHERE child_kind = ?1 AND child_id = ?2
             ORDER BY ordinal ASC, parent_kind ASC, parent_id ASC",
        )?;
        let rows = statement.query_map(
            params![vertex_kind_name(child), child.opaque_id()],
            |row| -> Result<VertexRef, rusqlite::Error> {
                decode_vertex_ref(&row.get::<_, String>(0)?, &row.get::<_, String>(1)?)
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|parent| self.vertex_summary(parent))
            .collect()
    }

    fn load_vertex_children(&self, parent: VertexRef) -> Result<Vec<VertexSummary>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT child_kind, child_id
             FROM influence_edges
             WHERE parent_kind = ?1 AND parent_id = ?2
             ORDER BY ordinal ASC, child_kind ASC, child_id ASC",
        )?;
        let rows = statement.query_map(
            params![vertex_kind_name(parent), parent.opaque_id()],
            |row| -> Result<VertexRef, rusqlite::Error> {
                decode_vertex_ref(&row.get::<_, String>(0)?, &row.get::<_, String>(1)?)
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|child| self.vertex_summary(child))
            .collect()
    }

    fn vertex_summary(&self, vertex: VertexRef) -> Result<VertexSummary, StoreError> {
        match vertex {
            VertexRef::Hypothesis(id) => {
                let record = self.hypothesis_by_id(id)?;
                Ok(VertexSummary {
                    vertex,
                    frontier_id: record.frontier_id,
                    slug: record.slug,
                    archived: record.archived,
                    title: record.title,
                    summary: Some(record.summary),
                    updated_at: record.updated_at,
                })
            }
            VertexRef::Experiment(id) => {
                let record = self.resolve_experiment(&id.to_string())?;
                Ok(VertexSummary {
                    vertex,
                    frontier_id: record.frontier_id,
                    slug: record.slug,
                    archived: record.archived,
                    title: record.title,
                    summary: record.summary,
                    updated_at: record.updated_at,
                })
            }
        }
    }

    fn artifact_attachment_targets(
        &self,
        artifact_id: ArtifactId,
    ) -> Result<Vec<AttachmentTargetRef>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT target_kind, target_id
             FROM artifact_attachments
             WHERE artifact_id = ?1
             ORDER BY ordinal ASC, target_kind ASC, target_id ASC",
        )?;
        let rows = statement.query_map(params![artifact_id.to_string()], |row| {
            decode_attachment_target(&row.get::<_, String>(0)?, &row.get::<_, String>(1)?)
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn artifact_attached_to_frontier(
        &self,
        artifact_id: ArtifactId,
        frontier_id: FrontierId,
    ) -> Result<bool, StoreError> {
        let targets = self.artifact_attachment_targets(artifact_id)?;
        if targets.contains(&AttachmentTargetRef::Frontier(frontier_id)) {
            return Ok(true);
        }
        for target in targets {
            match target {
                AttachmentTargetRef::Hypothesis(hypothesis_id) => {
                    if self.hypothesis_by_id(hypothesis_id)?.frontier_id == frontier_id {
                        return Ok(true);
                    }
                }
                AttachmentTargetRef::Experiment(experiment_id) => {
                    if self
                        .resolve_experiment(&experiment_id.to_string())?
                        .frontier_id
                        == frontier_id
                    {
                        return Ok(true);
                    }
                }
                AttachmentTargetRef::Frontier(_) => {}
            }
        }
        Ok(false)
    }

    fn active_hypothesis_ids(
        &self,
        frontier_id: FrontierId,
        brief: &FrontierBrief,
    ) -> Result<BTreeSet<HypothesisId>, StoreError> {
        let mut ids = brief
            .roadmap
            .iter()
            .map(|item| item.hypothesis_id)
            .collect::<BTreeSet<_>>();
        for experiment in self.list_experiments(ListExperimentsQuery {
            frontier: Some(frontier_id.to_string()),
            status: Some(ExperimentStatus::Open),
            limit: None,
            ..ListExperimentsQuery::default()
        })? {
            let _ = ids.insert(experiment.hypothesis_id);
        }
        Ok(ids)
    }

    fn active_hypothesis_count(&self, frontier_id: FrontierId) -> Result<u64, StoreError> {
        let frontier = self.read_frontier(&frontier_id.to_string())?;
        Ok(self
            .active_hypothesis_ids(frontier_id, &frontier.brief)?
            .len() as u64)
    }

    fn open_experiment_count(&self, frontier_id: Option<FrontierId>) -> Result<u64, StoreError> {
        Ok(self
            .load_experiment_records(frontier_id, None, false)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Open)
            .count() as u64)
    }

    fn metric_key_summary_from_definition(
        &self,
        definition: MetricDefinition,
        frontier_id: Option<FrontierId>,
    ) -> Result<MetricKeySummary, StoreError> {
        Ok(MetricKeySummary {
            reference_count: self.metric_reference_count(frontier_id, definition.id)?,
            key: definition.key,
            unit: definition.unit,
            dimension: definition.dimension,
            aggregation: definition.aggregation,
            objective: definition.objective,
            default_visibility: self.default_visibility_for_metric(definition.id)?,
            description: definition.description,
        })
    }

    fn live_metric_keys(
        &self,
        frontier_id: FrontierId,
        active_hypotheses: &[HypothesisCurrentState],
        open_experiments: &[ExperimentSummary],
    ) -> Result<Vec<MetricKeySummary>, StoreError> {
        let live_names = self.live_metric_key_names_with_context(
            frontier_id,
            active_hypotheses,
            open_experiments,
        )?;
        let mut keys = self
            .list_metric_definitions()?
            .into_iter()
            .filter(|definition| live_names.contains(definition.key.as_str()))
            .map(|definition| {
                self.metric_key_summary_from_definition(definition, Some(frontier_id))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        keys.sort_by(|left, right| left.key.as_str().cmp(right.key.as_str()));
        Ok(keys)
    }

    fn frontier_kpi_metric_keys(
        &self,
        frontier_id: FrontierId,
    ) -> Result<Vec<MetricKeySummary>, StoreError> {
        let mut seen = BTreeSet::new();
        self.frontier_kpis(frontier_id)?
            .into_iter()
            .flat_map(|kpi| kpi.metrics)
            .filter_map(|metric| {
                if seen.insert(metric.key.clone()) {
                    Some(metric)
                } else {
                    None
                }
            })
            .map(|metric| {
                let definition = self
                    .metric_definition(&metric.key)?
                    .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))?;
                self.metric_key_summary_from_definition(definition, Some(frontier_id))
            })
            .collect()
    }

    fn live_metric_key_names(
        &self,
        frontier_id: FrontierId,
    ) -> Result<BTreeSet<String>, StoreError> {
        let frontier = self.read_frontier(&frontier_id.to_string())?;
        let active_hypotheses = self
            .active_hypothesis_ids(frontier_id, &frontier.brief)?
            .into_iter()
            .map(|hypothesis_id| {
                let summary =
                    self.hypothesis_summary_from_record(self.hypothesis_by_id(hypothesis_id)?)?;
                let open_experiments = self.list_experiments(ListExperimentsQuery {
                    hypothesis: Some(hypothesis_id.to_string()),
                    status: Some(ExperimentStatus::Open),
                    limit: None,
                    ..ListExperimentsQuery::default()
                })?;
                let latest_closed_experiment = self
                    .list_experiments(ListExperimentsQuery {
                        hypothesis: Some(hypothesis_id.to_string()),
                        status: Some(ExperimentStatus::Closed),
                        limit: Some(1),
                        ..ListExperimentsQuery::default()
                    })?
                    .into_iter()
                    .next();
                Ok(HypothesisCurrentState {
                    hypothesis: summary,
                    open_experiments,
                    latest_closed_experiment,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let open_experiments = self.list_experiments(ListExperimentsQuery {
            frontier: Some(frontier_id.to_string()),
            status: Some(ExperimentStatus::Open),
            limit: None,
            ..ListExperimentsQuery::default()
        })?;
        self.live_metric_key_names_with_context(frontier_id, &active_hypotheses, &open_experiments)
    }

    fn live_metric_key_names_with_context(
        &self,
        _frontier_id: FrontierId,
        active_hypotheses: &[HypothesisCurrentState],
        open_experiments: &[ExperimentSummary],
    ) -> Result<BTreeSet<String>, StoreError> {
        let mut keys = BTreeSet::new();
        for state in active_hypotheses {
            if let Some(experiment) = state.latest_closed_experiment.as_ref() {
                keys.extend(self.experiment_metric_key_names(experiment.id)?);
            }
        }
        for experiment in open_experiments {
            for parent in self.load_vertex_parents(VertexRef::Experiment(experiment.id))? {
                if let VertexRef::Experiment(parent_id) = parent.vertex {
                    keys.extend(self.experiment_metric_key_names(parent_id)?);
                }
            }
        }
        Ok(keys)
    }

    fn experiment_metric_key_names(
        &self,
        experiment_id: ExperimentId,
    ) -> Result<BTreeSet<String>, StoreError> {
        let record = self.resolve_experiment(&experiment_id.to_string())?;
        Ok(record
            .outcome
            .as_ref()
            .map(all_metrics)
            .unwrap_or_default()
            .into_iter()
            .map(|metric| metric.key.to_string())
            .collect())
    }

    fn metric_reference_count(
        &self,
        frontier_id: Option<FrontierId>,
        metric_id: MetricId,
    ) -> Result<u64, StoreError> {
        let base_sql = "SELECT COUNT(*)
                        FROM experiment_metrics metrics
                        JOIN experiments experiments ON experiments.id = metrics.experiment_id";
        let count = if let Some(frontier_id) = frontier_id {
            self.connection.query_row(
                &format!(
                    "{base_sql} WHERE metrics.metric_id = ?1 AND experiments.frontier_id = ?2"
                ),
                params![metric_id.to_string(), frontier_id.to_string()],
                |row| row.get::<_, u64>(0),
            )?
        } else {
            self.connection.query_row(
                &format!("{base_sql} WHERE metrics.metric_id = ?1"),
                params![metric_id.to_string()],
                |row| row.get::<_, u64>(0),
            )?
        };
        Ok(count)
    }

    fn default_visibility_for_metric(
        &self,
        metric_id: MetricId,
    ) -> Result<DefaultVisibility, StoreError> {
        self.default_visibility_for_frontier_appearances(
            self.metric_frontier_appearances(metric_id)?,
        )
    }

    fn default_visibility_for_tag(&self, tag_id: TagId) -> Result<DefaultVisibility, StoreError> {
        self.default_visibility_for_frontier_appearances(self.tag_frontier_appearances(tag_id)?)
    }

    fn default_visibility_for_frontier_appearances(
        &self,
        frontier_ids: BTreeSet<FrontierId>,
    ) -> Result<DefaultVisibility, StoreError> {
        if frontier_ids.is_empty() {
            return Ok(DefaultVisibility::visible());
        }
        for frontier_id in frontier_ids {
            let status = self.connection.query_row(
                "SELECT status FROM frontiers WHERE id = ?1",
                params![frontier_id.to_string()],
                |row| parse_frontier_status(&row.get::<_, String>(0)?),
            )?;
            if status != FrontierStatus::Archived {
                return Ok(DefaultVisibility::visible());
            }
        }
        Ok(DefaultVisibility::hidden(
            HiddenByDefaultReason::InArchivedFrontiersOnly,
        ))
    }

    fn metric_frontier_appearances(
        &self,
        metric_id: MetricId,
    ) -> Result<BTreeSet<FrontierId>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT experiments.frontier_id
             FROM experiment_metrics metrics
             JOIN experiments ON experiments.id = metrics.experiment_id
             WHERE metrics.metric_id = ?1
             UNION
             SELECT DISTINCT frontier_kpis.frontier_id
             FROM kpi_metric_alternatives alternatives
             JOIN frontier_kpis ON frontier_kpis.id = alternatives.kpi_id
             WHERE alternatives.metric_id = ?1",
        )?;
        let rows = statement.query_map(params![metric_id.to_string()], |row| {
            parse_frontier_id_sql(&row.get::<_, String>(0)?)
        })?;
        rows.collect::<Result<BTreeSet<_>, _>>()
            .map_err(StoreError::from)
    }

    fn tag_frontier_appearances(&self, tag_id: TagId) -> Result<BTreeSet<FrontierId>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT hypotheses.frontier_id
             FROM hypothesis_tags tags
             JOIN hypotheses ON hypotheses.id = tags.hypothesis_id
             WHERE tags.tag_id = ?1
             UNION
             SELECT DISTINCT experiments.frontier_id
             FROM experiment_tags tags
             JOIN experiments ON experiments.id = tags.experiment_id
             WHERE tags.tag_id = ?1",
        )?;
        let rows = statement.query_map(params![tag_id.to_string()], |row| {
            parse_frontier_id_sql(&row.get::<_, String>(0)?)
        })?;
        rows.collect::<Result<BTreeSet<_>, _>>()
            .map_err(StoreError::from)
    }

    fn kpi_reference_count(&self, metric_id: MetricId) -> Result<u64, StoreError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM kpi_metric_alternatives WHERE metric_id = ?1",
                params![metric_id.to_string()],
                |row| row.get::<_, u64>(0),
            )
            .map_err(StoreError::from)
    }

    fn materialize_outcome(
        &self,
        patch: &ExperimentOutcomePatch,
        existing: Option<&ExperimentOutcome>,
        frontier_id: FrontierId,
        origin: MutationOrigin,
    ) -> Result<ExperimentOutcome, StoreError> {
        if patch.backend == ExecutionBackend::Manual && patch.command.argv.is_empty() {
            return Err(StoreError::ManualExperimentRequiresCommand);
        }
        for key in patch.dimensions.keys() {
            let definition = self
                .run_dimension_definition(key)?
                .ok_or_else(|| StoreError::UnknownRunDimension(key.clone()))?;
            let observed = patch
                .dimensions
                .get(key)
                .map(RunDimensionValue::value_type)
                .ok_or_else(|| StoreError::UnknownRunDimension(key.clone()))?;
            if definition.value_type != observed {
                return Err(StoreError::UnknownDimensionFilter(key.to_string()));
            }
        }
        let _primary_definition = self
            .metric_definition(&patch.primary_metric.key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(patch.primary_metric.key.clone()))?;
        let primary_metric = patch.primary_metric.clone();
        let mut supporting_metrics = Vec::with_capacity(patch.supporting_metrics.len());
        for metric in &patch.supporting_metrics {
            let _definition = self
                .metric_definition(&metric.key)?
                .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))?;
            supporting_metrics.push(metric.clone());
        }
        if origin.is_mcp() {
            self.assert_frontier_kpis_satisfied(frontier_id, &primary_metric, &supporting_metrics)?;
        }
        let (commit_hash, closed_at) = match existing {
            Some(outcome) => (outcome.commit_hash.clone(), outcome.closed_at),
            None => (
                Some(capture_clean_git_commit(&self.project_root)?),
                OffsetDateTime::now_utc(),
            ),
        };
        Ok(ExperimentOutcome {
            backend: patch.backend,
            command: patch.command.clone(),
            dimensions: patch.dimensions.clone(),
            primary_metric,
            supporting_metrics,
            verdict: patch.verdict,
            rationale: patch.rationale.clone(),
            analysis: patch.analysis.clone(),
            commit_hash,
            closed_at,
        })
    }

    fn resolve_tag_set(
        &self,
        tags: &BTreeSet<TagName>,
        origin: MutationOrigin,
    ) -> Result<BTreeSet<TagId>, StoreError> {
        let tag_ids = self.resolve_existing_tag_names(&tags.iter().cloned().collect::<Vec<_>>())?;
        if origin.is_mcp() {
            self.assert_mandatory_tag_families(tags)?;
        }
        Ok(tag_ids)
    }

    fn resolve_existing_tag_names(&self, tags: &[TagName]) -> Result<BTreeSet<TagId>, StoreError> {
        tags.iter()
            .map(|tag| self.tag_id_by_name(tag))
            .collect::<Result<BTreeSet<_>, _>>()
    }

    fn tag_id_by_name(&self, tag: &TagName) -> Result<TagId, StoreError> {
        if let Some(record) = self.tag_record_by_name(tag)? {
            return Ok(record.id);
        }
        if let Some(history) = self.tag_name_history_by_name(tag)? {
            return Err(StoreError::PolicyViolation(history.message.to_string()));
        }
        Err(StoreError::UnknownTag(tag.clone()))
    }

    fn assert_no_stale_tag_name(&self, tag: &TagName) -> Result<(), StoreError> {
        if let Some(history) = self.tag_name_history_by_name(tag)? {
            return Err(StoreError::PolicyViolation(history.message.to_string()));
        }
        Ok(())
    }

    fn assert_tag_policy_for_assignment(
        &self,
        tags: &BTreeSet<TagName>,
        origin: MutationOrigin,
    ) -> Result<(), StoreError> {
        if !origin.is_mcp() {
            return Ok(());
        }
        self.assert_mandatory_tag_families(tags)
    }

    fn assert_tag_add_open(&self) -> Result<(), StoreError> {
        if let Some(lock) =
            self.registry_lock(&RegistryName::tags(), RegistryLockMode::Definition)?
        {
            return Err(StoreError::PolicyViolation(format!(
                "new tag creation is locked; use an existing tag from tag.list or ask the supervisor to unlock additions. Reason: {}",
                lock.reason
            )));
        }
        Ok(())
    }

    fn assert_mandatory_tag_families(&self, tags: &BTreeSet<TagName>) -> Result<(), StoreError> {
        let selected_ids =
            self.resolve_existing_tag_names(&tags.iter().cloned().collect::<Vec<_>>())?;
        for family in self.load_tag_family_records()? {
            if !family.mandatory {
                continue;
            }
            let members = self.tag_names_for_family(family.id)?;
            let satisfied = selected_ids.iter().any(|tag_id| {
                self.tag_family_id_for_tag_id(*tag_id)
                    .ok()
                    .flatten()
                    .is_some_and(|family_id| family_id == family.id)
            });
            if !satisfied {
                if members.is_empty() {
                    return Err(StoreError::PolicyViolation(format!(
                        "mandatory tag family `{}` is missing, but it has no active tags; ask the supervisor to add a tag or make the family optional",
                        family.name
                    )));
                }
                let choices = members
                    .iter()
                    .take(8)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let suffix = if members.len() > 8 {
                    "; call tag.list for the complete family"
                } else {
                    ""
                };
                return Err(StoreError::PolicyViolation(format!(
                    "mandatory tag family `{}` is missing; include at least one of: {}{}",
                    family.name, choices, suffix
                )));
            }
        }
        Ok(())
    }

    fn assert_frontier_kpis_satisfied(
        &self,
        frontier_id: FrontierId,
        primary_metric: &MetricValue,
        supporting_metrics: &[MetricValue],
    ) -> Result<(), StoreError> {
        let reported = std::iter::once(primary_metric)
            .chain(supporting_metrics.iter())
            .map(|metric| metric.key.clone())
            .collect::<BTreeSet<_>>();
        let kpis = self.frontier_kpis(frontier_id)?;
        if kpis.is_empty() {
            return Err(StoreError::MissingFrontierKpiContract {
                frontier: self.frontier_slug_by_id(frontier_id)?,
            });
        }
        for kpi in kpis {
            let alternatives = kpi
                .metrics
                .iter()
                .map(|metric| metric.key.clone())
                .collect::<Vec<_>>();
            if alternatives.iter().any(|key| reported.contains(key)) {
                continue;
            }
            return Err(StoreError::MissingMandatoryKpi {
                kpi: kpi.name,
                metrics: alternatives
                    .into_iter()
                    .map(|key| key.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        Ok(())
    }

    fn tag_record_by_name(&self, name: &TagName) -> Result<Option<TagRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT tags.id, tags.name, tags.description, tags.family_id, tag_families.name,
                        tags.status, tags.revision, tags.created_at, tags.updated_at
                 FROM tags
                 LEFT JOIN tag_families ON tag_families.id = tags.family_id
                 WHERE tags.name = ?1",
                params![name.as_str()],
                decode_tag_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn tag_family_by_name(
        &self,
        name: &TagFamilyName,
    ) -> Result<Option<TagFamilyRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, name, description, mandatory, status, revision, created_at, updated_at
                 FROM tag_families
                 WHERE name = ?1",
                params![name.as_str()],
                decode_tag_family_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn tag_name_history_by_name(
        &self,
        name: &TagName,
    ) -> Result<Option<TagNameHistoryRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT tag_name_history.name, tag_name_history.target_tag_id, tags.name,
                        tag_name_history.disposition, tag_name_history.message,
                        tag_name_history.created_at
                 FROM tag_name_history
                 LEFT JOIN tags ON tags.id = tag_name_history.target_tag_id
                 WHERE tag_name_history.name = ?1",
                params![name.as_str()],
                decode_tag_name_history_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn registry_lock(
        &self,
        registry: &RegistryName,
        mode: RegistryLockMode,
    ) -> Result<Option<RegistryLockRecord>, StoreError> {
        self.registry_lock_in_scope(registry, mode, "project", "project")
    }

    fn frontier_registry_lock_by_id(
        &self,
        registry: &RegistryName,
        mode: RegistryLockMode,
        frontier_id: FrontierId,
    ) -> Result<Option<RegistryLockRecord>, StoreError> {
        self.registry_lock_in_scope(registry, mode, "frontier", &frontier_id.to_string())
    }

    fn registry_lock_in_scope(
        &self,
        registry: &RegistryName,
        mode: RegistryLockMode,
        scope_kind: &str,
        scope_id: &str,
    ) -> Result<Option<RegistryLockRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, registry, mode, scope_kind, scope_id, reason, revision, locked_at, updated_at
                 FROM registry_locks
                 WHERE registry = ?1 AND mode = ?2 AND scope_kind = ?3 AND scope_id = ?4",
                params![registry.as_str(), mode.as_str(), scope_kind, scope_id],
                decode_registry_lock_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn load_tag_records(&self) -> Result<Vec<TagRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT tags.id, tags.name, tags.description, tags.family_id, tag_families.name,
                    tags.status, tags.revision, tags.created_at, tags.updated_at
             FROM tags
             LEFT JOIN tag_families ON tag_families.id = tags.family_id
             ORDER BY tags.name ASC",
        )?;
        let rows = statement.query_map([], decode_tag_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn default_visible_tag_records(&self) -> Result<Vec<TagRecord>, StoreError> {
        self.load_tag_records()?
            .into_iter()
            .filter_map(|tag| match self.default_visibility_for_tag(tag.id) {
                Ok(visibility) if visibility.is_default_visible() => Some(Ok(tag)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    fn load_tag_family_records(&self) -> Result<Vec<TagFamilyRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, name, description, mandatory, status, revision, created_at, updated_at
             FROM tag_families
             ORDER BY name ASC",
        )?;
        let rows = statement.query_map([], decode_tag_family_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn load_registry_locks(
        &self,
        registry: &RegistryName,
    ) -> Result<Vec<RegistryLockRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, registry, mode, scope_kind, scope_id, reason, revision, locked_at, updated_at
             FROM registry_locks
             WHERE registry = ?1
             ORDER BY mode ASC, scope_kind ASC, scope_id ASC",
        )?;
        let rows = statement.query_map(params![registry.as_str()], decode_registry_lock_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn load_tag_name_history(&self) -> Result<Vec<TagNameHistoryRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT tag_name_history.name, tag_name_history.target_tag_id, tags.name,
                    tag_name_history.disposition, tag_name_history.message,
                    tag_name_history.created_at
             FROM tag_name_history
             LEFT JOIN tags ON tags.id = tag_name_history.target_tag_id
             ORDER BY tag_name_history.created_at DESC, tag_name_history.name ASC",
        )?;
        let rows = statement.query_map([], decode_tag_name_history_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn tag_names_for_family(&self, family_id: TagFamilyId) -> Result<Vec<TagName>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT name FROM tags WHERE family_id = ?1 ORDER BY name ASC")?;
        let rows = statement.query_map(params![family_id.to_string()], |row| {
            parse_tag_name(&row.get::<_, String>(0)?)
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn tag_family_id_for_tag_id(&self, tag_id: TagId) -> Result<Option<TagFamilyId>, StoreError> {
        self.connection
            .query_row(
                "SELECT family_id FROM tags WHERE id = ?1",
                params![tag_id.to_string()],
                |row| {
                    row.get::<_, Option<String>>(0)?
                        .map(|raw| parse_tag_family_id(&raw))
                        .transpose()
                },
            )
            .map_err(StoreError::from)
    }

    fn unique_frontier_slug(
        &self,
        explicit: Option<Slug>,
        label: &NonEmptyText,
    ) -> Result<Slug, StoreError> {
        self.unique_slug("frontiers", "slug", explicit, label)
    }

    fn unique_hypothesis_slug(
        &self,
        explicit: Option<Slug>,
        title: &NonEmptyText,
    ) -> Result<Slug, StoreError> {
        self.unique_slug("hypotheses", "slug", explicit, title)
    }

    fn unique_experiment_slug(
        &self,
        explicit: Option<Slug>,
        title: &NonEmptyText,
    ) -> Result<Slug, StoreError> {
        self.unique_slug("experiments", "slug", explicit, title)
    }

    fn unique_artifact_slug(
        &self,
        explicit: Option<Slug>,
        label: &NonEmptyText,
    ) -> Result<Slug, StoreError> {
        self.unique_slug("artifacts", "slug", explicit, label)
    }

    fn unique_slug(
        &self,
        table: &str,
        column: &str,
        explicit: Option<Slug>,
        seed: &NonEmptyText,
    ) -> Result<Slug, StoreError> {
        if let Some(explicit) = explicit {
            return Ok(explicit);
        }
        let base = slugify(seed.as_str())?;
        if !self.slug_exists(table, column, &base)? {
            return Ok(base);
        }
        for ordinal in 2..10_000 {
            let candidate = Slug::new(format!("{}-{ordinal}", base.as_str()))?;
            if !self.slug_exists(table, column, &candidate)? {
                return Ok(candidate);
            }
        }
        Slug::new(format!("{}-{}", base.as_str(), Uuid::now_v7().simple()))
            .map_err(StoreError::from)
    }

    fn slug_exists(&self, table: &str, column: &str, slug: &Slug) -> Result<bool, StoreError> {
        let sql = format!("SELECT 1 FROM {table} WHERE {column} = ?1");
        self.connection
            .query_row(&sql, params![slug.as_str()], |_| Ok(()))
            .optional()
            .map(|value| value.is_some())
            .map_err(StoreError::from)
    }

    fn entity_history(
        &self,
        entity_kind: &str,
        entity_id: &str,
    ) -> Result<Vec<EntityHistoryEntry>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT revision, event_kind, occurred_at, snapshot_json
             FROM events
             WHERE entity_kind = ?1 AND entity_id = ?2
             ORDER BY revision DESC, occurred_at DESC",
        )?;
        let rows = statement.query_map(params![entity_kind, entity_id], |row| {
            Ok(EntityHistoryEntry {
                revision: row.get(0)?,
                event_kind: parse_non_empty_text(&row.get::<_, String>(1)?)?,
                occurred_at: parse_timestamp_sql(&row.get::<_, String>(2)?)?,
                snapshot: decode_json(&row.get::<_, String>(3)?)
                    .map_err(to_sql_conversion_error)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }
}

fn install_schema(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS tags (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL,
            family_id TEXT REFERENCES tag_families(id) ON DELETE SET NULL,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tag_families (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL,
            mandatory INTEGER NOT NULL,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tag_name_history (
            name TEXT PRIMARY KEY NOT NULL,
            target_tag_id TEXT REFERENCES tags(id) ON DELETE SET NULL,
            disposition TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS registry_locks (
            id TEXT PRIMARY KEY NOT NULL,
            registry TEXT NOT NULL,
            mode TEXT NOT NULL,
            scope_kind TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            revision INTEGER NOT NULL,
            locked_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (registry, mode, scope_kind, scope_id)
        );

        CREATE TABLE IF NOT EXISTS frontiers (
            id TEXT PRIMARY KEY NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL,
            objective TEXT NOT NULL,
            status TEXT NOT NULL,
            brief_json TEXT NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hypotheses (
            id TEXT PRIMARY KEY NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            archived INTEGER NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            body TEXT NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hypothesis_tags (
            hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
            tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (hypothesis_id, tag_id)
        );

        CREATE TABLE IF NOT EXISTS experiments (
            id TEXT PRIMARY KEY NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
            archived INTEGER NOT NULL,
            title TEXT NOT NULL,
            summary TEXT,
            status TEXT NOT NULL,
            outcome_json TEXT,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_tags (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (experiment_id, tag_id)
        );

        CREATE TABLE IF NOT EXISTS influence_edges (
            parent_kind TEXT NOT NULL,
            parent_id TEXT NOT NULL,
            child_kind TEXT NOT NULL,
            child_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY (parent_kind, parent_id, child_kind, child_id)
        );

        CREATE TABLE IF NOT EXISTS artifacts (
            id TEXT PRIMARY KEY NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL,
            label TEXT NOT NULL,
            summary TEXT,
            locator TEXT NOT NULL,
            media_type TEXT,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS artifact_attachments (
            artifact_id TEXT NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
            target_kind TEXT NOT NULL,
            target_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY (artifact_id, target_kind, target_id)
        );

        CREATE TABLE IF NOT EXISTS metric_definitions (
            id TEXT PRIMARY KEY NOT NULL,
            key TEXT NOT NULL UNIQUE,
            dimension TEXT NOT NULL,
            display_unit TEXT NOT NULL,
            aggregation TEXT NOT NULL,
            objective TEXT NOT NULL,
            description TEXT,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metric_name_history (
            name TEXT PRIMARY KEY NOT NULL,
            target_metric_id TEXT REFERENCES metric_definitions(id) ON DELETE SET NULL,
            target_metric_key TEXT,
            disposition TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS frontier_kpis (
            id TEXT PRIMARY KEY NOT NULL,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            objective TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (frontier_id, name)
        );

        CREATE TABLE IF NOT EXISTS kpi_metric_alternatives (
            kpi_id TEXT NOT NULL REFERENCES frontier_kpis(id) ON DELETE CASCADE,
            metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
            precedence INTEGER NOT NULL,
            PRIMARY KEY (kpi_id, metric_id),
            UNIQUE (kpi_id, precedence)
        );

        CREATE TABLE IF NOT EXISTS run_dimension_definitions (
            key TEXT PRIMARY KEY NOT NULL,
            value_type TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_dimensions (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value_json TEXT NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_metrics (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            is_primary INTEGER NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (experiment_id, metric_id)
        );

        CREATE TABLE IF NOT EXISTS events (
            entity_kind TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            event_kind TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            PRIMARY KEY (entity_kind, entity_id, revision)
        );
        ",
    )?;
    Ok(())
}

fn insert_tag(transaction: &Transaction<'_>, tag: &TagRecord) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO tags (id, name, description, family_id, status, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            tag.id.to_string(),
            tag.name.as_str(),
            tag.description.as_str(),
            tag.family_id.map(|id| id.to_string()),
            tag.status.as_str(),
            tag.revision,
            encode_timestamp(tag.created_at)?,
            encode_timestamp(tag.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_tag(transaction: &Transaction<'_>, tag: &TagRecord) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE tags
         SET name = ?2, description = ?3, family_id = ?4, status = ?5, revision = ?6, updated_at = ?7
         WHERE id = ?1",
        params![
            tag.id.to_string(),
            tag.name.as_str(),
            tag.description.as_str(),
            tag.family_id.map(|id| id.to_string()),
            tag.status.as_str(),
            tag.revision,
            encode_timestamp(tag.updated_at)?,
        ],
    )?;
    Ok(())
}

fn delete_tag_row(transaction: &Transaction<'_>, tag_id: TagId) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM tags WHERE id = ?1",
        params![tag_id.to_string()],
    )?;
    Ok(())
}

fn insert_tag_family(
    transaction: &Transaction<'_>,
    family: &TagFamilyRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO tag_families (id, name, description, mandatory, status, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            family.id.to_string(),
            family.name.as_str(),
            family.description.as_str(),
            bool_to_sql(family.mandatory),
            family.status.as_str(),
            family.revision,
            encode_timestamp(family.created_at)?,
            encode_timestamp(family.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_tag_family(
    transaction: &Transaction<'_>,
    family: &TagFamilyRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE tag_families
         SET name = ?2, description = ?3, mandatory = ?4, status = ?5, revision = ?6, updated_at = ?7
         WHERE id = ?1",
        params![
            family.id.to_string(),
            family.name.as_str(),
            family.description.as_str(),
            bool_to_sql(family.mandatory),
            family.status.as_str(),
            family.revision,
            encode_timestamp(family.updated_at)?,
        ],
    )?;
    Ok(())
}

fn insert_kpi(transaction: &Transaction<'_>, record: &FrontierKpiRecord) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO frontier_kpis (id, frontier_id, name, objective, description, status, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            record.id.to_string(),
            record.frontier_id.to_string(),
            record.name.as_str(),
            record.objective.as_str(),
            record.description.as_ref().map(NonEmptyText::as_str),
            record.status.as_str(),
            record.revision,
            encode_timestamp(record.created_at)?,
            encode_timestamp(record.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_kpi(transaction: &Transaction<'_>, record: &FrontierKpiRecord) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE frontier_kpis
         SET name = ?2, objective = ?3, description = ?4, status = ?5, revision = ?6, updated_at = ?7
         WHERE id = ?1",
        params![
            record.id.to_string(),
            record.name.as_str(),
            record.objective.as_str(),
            record.description.as_ref().map(NonEmptyText::as_str),
            record.status.as_str(),
            record.revision,
            encode_timestamp(record.updated_at)?,
        ],
    )?;
    Ok(())
}

fn replace_kpi_alternatives(
    transaction: &Transaction<'_>,
    kpi_id: KpiId,
    alternatives: &[KpiMetricAlternativeRecord],
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM kpi_metric_alternatives WHERE kpi_id = ?1",
        params![kpi_id.to_string()],
    )?;
    for alternative in alternatives {
        let _ = transaction.execute(
            "INSERT INTO kpi_metric_alternatives (kpi_id, metric_id, precedence)
             VALUES (?1, ?2, ?3)",
            params![
                alternative.kpi_id.to_string(),
                alternative.metric_id.to_string(),
                alternative.precedence,
            ],
        )?;
    }
    Ok(())
}

fn upsert_tag_name_history(
    transaction: &Transaction<'_>,
    history: &TagNameHistoryRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO tag_name_history (name, target_tag_id, disposition, message, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(name) DO UPDATE SET
             target_tag_id = excluded.target_tag_id,
             disposition = excluded.disposition,
             message = excluded.message,
             created_at = excluded.created_at",
        params![
            history.name.as_str(),
            history.target_tag_id.map(|id| id.to_string()),
            history.disposition.as_str(),
            history.message.as_str(),
            encode_timestamp(history.created_at)?,
        ],
    )?;
    Ok(())
}

fn upsert_registry_lock(
    transaction: &Transaction<'_>,
    lock: &RegistryLockRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO registry_locks (id, registry, mode, scope_kind, scope_id, reason, revision, locked_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(registry, mode, scope_kind, scope_id) DO UPDATE SET
             reason = excluded.reason,
             revision = excluded.revision,
             updated_at = excluded.updated_at",
        params![
            lock.id.to_string(),
            lock.registry.as_str(),
            lock.mode.as_str(),
            lock.scope_kind.as_str(),
            lock.scope_id.as_str(),
            lock.reason.as_str(),
            lock.revision,
            encode_timestamp(lock.locked_at)?,
            encode_timestamp(lock.updated_at)?,
        ],
    )?;
    Ok(())
}

fn merge_tag_edges(
    transaction: &Transaction<'_>,
    source: TagId,
    target: TagId,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT OR IGNORE INTO hypothesis_tags (hypothesis_id, tag_id)
         SELECT hypothesis_id, ?2 FROM hypothesis_tags WHERE tag_id = ?1",
        params![source.to_string(), target.to_string()],
    )?;
    let _ = transaction.execute(
        "DELETE FROM hypothesis_tags WHERE tag_id = ?1",
        params![source.to_string()],
    )?;
    let _ = transaction.execute(
        "INSERT OR IGNORE INTO experiment_tags (experiment_id, tag_id)
         SELECT experiment_id, ?2 FROM experiment_tags WHERE tag_id = ?1",
        params![source.to_string(), target.to_string()],
    )?;
    let _ = transaction.execute(
        "DELETE FROM experiment_tags WHERE tag_id = ?1",
        params![source.to_string()],
    )?;
    Ok(())
}

fn insert_frontier(
    transaction: &Transaction<'_>,
    frontier: &FrontierRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO frontiers (id, slug, label, objective, status, brief_json, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            frontier.id.to_string(),
            frontier.slug.as_str(),
            frontier.label.as_str(),
            frontier.objective.as_str(),
            frontier.status.as_str(),
            encode_json(&frontier.brief)?,
            frontier.revision,
            encode_timestamp(frontier.created_at)?,
            encode_timestamp(frontier.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_frontier_row(
    transaction: &Transaction<'_>,
    frontier: &FrontierRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE frontiers
         SET slug = ?2, label = ?3, objective = ?4, status = ?5, brief_json = ?6, revision = ?7, updated_at = ?8
         WHERE id = ?1",
        params![
            frontier.id.to_string(),
            frontier.slug.as_str(),
            frontier.label.as_str(),
            frontier.objective.as_str(),
            frontier.status.as_str(),
            encode_json(&frontier.brief)?,
            frontier.revision,
            encode_timestamp(frontier.updated_at)?,
        ],
    )?;
    Ok(())
}

fn insert_hypothesis(
    transaction: &Transaction<'_>,
    hypothesis: &HypothesisRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO hypotheses (id, slug, frontier_id, archived, title, summary, body, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            hypothesis.id.to_string(),
            hypothesis.slug.as_str(),
            hypothesis.frontier_id.to_string(),
            bool_to_sql(hypothesis.archived),
            hypothesis.title.as_str(),
            hypothesis.summary.as_str(),
            hypothesis.body.as_str(),
            hypothesis.revision,
            encode_timestamp(hypothesis.created_at)?,
            encode_timestamp(hypothesis.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_hypothesis_row(
    transaction: &Transaction<'_>,
    hypothesis: &HypothesisRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE hypotheses
         SET slug = ?2, archived = ?3, title = ?4, summary = ?5, body = ?6, revision = ?7, updated_at = ?8
         WHERE id = ?1",
        params![
            hypothesis.id.to_string(),
            hypothesis.slug.as_str(),
            bool_to_sql(hypothesis.archived),
            hypothesis.title.as_str(),
            hypothesis.summary.as_str(),
            hypothesis.body.as_str(),
            hypothesis.revision,
            encode_timestamp(hypothesis.updated_at)?,
        ],
    )?;
    Ok(())
}

fn replace_hypothesis_tags(
    transaction: &Transaction<'_>,
    hypothesis_id: HypothesisId,
    tags: &BTreeSet<TagId>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM hypothesis_tags WHERE hypothesis_id = ?1",
        params![hypothesis_id.to_string()],
    )?;
    for tag in tags {
        let _ = transaction.execute(
            "INSERT INTO hypothesis_tags (hypothesis_id, tag_id) VALUES (?1, ?2)",
            params![hypothesis_id.to_string(), tag.to_string()],
        )?;
    }
    Ok(())
}

fn insert_experiment(
    transaction: &Transaction<'_>,
    experiment: &ExperimentRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO experiments (id, slug, frontier_id, hypothesis_id, archived, title, summary, status, outcome_json, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            experiment.id.to_string(),
            experiment.slug.as_str(),
            experiment.frontier_id.to_string(),
            experiment.hypothesis_id.to_string(),
            bool_to_sql(experiment.archived),
            experiment.title.as_str(),
            experiment.summary.as_ref().map(NonEmptyText::as_str),
            experiment.status.as_str(),
            experiment.outcome.as_ref().map(encode_json).transpose()?,
            experiment.revision,
            encode_timestamp(experiment.created_at)?,
            encode_timestamp(experiment.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_experiment_row(
    transaction: &Transaction<'_>,
    experiment: &ExperimentRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE experiments
         SET slug = ?2, archived = ?3, title = ?4, summary = ?5, status = ?6, outcome_json = ?7, revision = ?8, updated_at = ?9
         WHERE id = ?1",
        params![
            experiment.id.to_string(),
            experiment.slug.as_str(),
            bool_to_sql(experiment.archived),
            experiment.title.as_str(),
            experiment.summary.as_ref().map(NonEmptyText::as_str),
            experiment.status.as_str(),
            experiment.outcome.as_ref().map(encode_json).transpose()?,
            experiment.revision,
            encode_timestamp(experiment.updated_at)?,
        ],
    )?;
    Ok(())
}

fn replace_experiment_tags(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
    tags: &BTreeSet<TagId>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM experiment_tags WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    for tag in tags {
        let _ = transaction.execute(
            "INSERT INTO experiment_tags (experiment_id, tag_id) VALUES (?1, ?2)",
            params![experiment_id.to_string(), tag.to_string()],
        )?;
    }
    Ok(())
}

fn replace_influence_parents(
    transaction: &Transaction<'_>,
    child: VertexRef,
    parents: &[VertexRef],
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM influence_edges WHERE child_kind = ?1 AND child_id = ?2",
        params![vertex_kind_name(child), child.opaque_id()],
    )?;
    for (ordinal, parent) in parents.iter().enumerate() {
        let _ = transaction.execute(
            "INSERT INTO influence_edges (parent_kind, parent_id, child_kind, child_id, ordinal)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                vertex_kind_name(*parent),
                parent.opaque_id(),
                vertex_kind_name(child),
                child.opaque_id(),
                i64::try_from(ordinal).unwrap_or(i64::MAX),
            ],
        )?;
    }
    Ok(())
}

fn insert_artifact(
    transaction: &Transaction<'_>,
    artifact: &ArtifactRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO artifacts (id, slug, kind, label, summary, locator, media_type, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            artifact.id.to_string(),
            artifact.slug.as_str(),
            artifact.kind.as_str(),
            artifact.label.as_str(),
            artifact.summary.as_ref().map(NonEmptyText::as_str),
            artifact.locator.as_str(),
            artifact.media_type.as_ref().map(NonEmptyText::as_str),
            artifact.revision,
            encode_timestamp(artifact.created_at)?,
            encode_timestamp(artifact.updated_at)?,
        ],
    )?;
    Ok(())
}

fn update_artifact_row(
    transaction: &Transaction<'_>,
    artifact: &ArtifactRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE artifacts
         SET slug = ?2, kind = ?3, label = ?4, summary = ?5, locator = ?6, media_type = ?7, revision = ?8, updated_at = ?9
         WHERE id = ?1",
        params![
            artifact.id.to_string(),
            artifact.slug.as_str(),
            artifact.kind.as_str(),
            artifact.label.as_str(),
            artifact.summary.as_ref().map(NonEmptyText::as_str),
            artifact.locator.as_str(),
            artifact.media_type.as_ref().map(NonEmptyText::as_str),
            artifact.revision,
            encode_timestamp(artifact.updated_at)?,
        ],
    )?;
    Ok(())
}

fn replace_artifact_attachments(
    transaction: &Transaction<'_>,
    artifact_id: ArtifactId,
    attachments: &[AttachmentTargetRef],
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM artifact_attachments WHERE artifact_id = ?1",
        params![artifact_id.to_string()],
    )?;
    for (ordinal, attachment) in attachments.iter().enumerate() {
        let _ = transaction.execute(
            "INSERT INTO artifact_attachments (artifact_id, target_kind, target_id, ordinal)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                artifact_id.to_string(),
                attachment_target_kind_name(*attachment),
                attachment.opaque_id(),
                i64::try_from(ordinal).unwrap_or(i64::MAX),
            ],
        )?;
    }
    Ok(())
}

fn replace_experiment_dimensions(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
    outcome: Option<&ExperimentOutcome>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM experiment_dimensions WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    if let Some(outcome) = outcome {
        for (key, value) in &outcome.dimensions {
            let _ = transaction.execute(
                "INSERT INTO experiment_dimensions (experiment_id, key, value_json) VALUES (?1, ?2, ?3)",
                params![experiment_id.to_string(), key.as_str(), encode_json(value)?],
            )?;
        }
    }
    Ok(())
}

fn replace_experiment_metrics(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
    outcome: Option<&ExperimentOutcome>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM experiment_metrics WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    if let Some(outcome) = outcome {
        for (ordinal, metric) in all_metrics(outcome).into_iter().enumerate() {
            let (metric_id, display_unit) = transaction.query_row(
                "SELECT id, display_unit FROM metric_definitions WHERE key = ?1",
                params![metric.key.as_str()],
                |row| {
                    Ok((
                        parse_metric_id_sql(&row.get::<_, String>(0)?)?,
                        parse_metric_unit(&row.get::<_, String>(1)?)?,
                    ))
                },
            )?;
            let value = display_unit.canonical_value(metric.value);
            let _ = transaction.execute(
                "INSERT INTO experiment_metrics (experiment_id, metric_id, ordinal, is_primary, value)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    experiment_id.to_string(),
                    metric_id.to_string(),
                    i64::try_from(ordinal).unwrap_or(i64::MAX),
                    bool_to_sql(ordinal == 0),
                    value,
                ],
            )?;
        }
    }
    Ok(())
}

fn update_metric_definition_key(
    transaction: &Transaction<'_>,
    metric: &MetricDefinition,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE metric_definitions SET key = ?2, revision = ?3, updated_at = ?4 WHERE id = ?1",
        params![
            metric.id.to_string(),
            metric.key.as_str(),
            metric.revision,
            encode_timestamp(metric.updated_at)?,
        ],
    )?;
    Ok(())
}

fn delete_metric_definition_row(
    transaction: &Transaction<'_>,
    metric_id: MetricId,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM metric_definitions WHERE id = ?1",
        params![metric_id.to_string()],
    )?;
    Ok(())
}

fn insert_metric_name_history(
    transaction: &Transaction<'_>,
    name: &str,
    target_metric_id: Option<MetricId>,
    target_metric_key: Option<&str>,
    disposition: TagNameDisposition,
    message: &str,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT OR REPLACE INTO metric_name_history
         (name, target_metric_id, target_metric_key, disposition, message, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            name,
            target_metric_id.map(|id| id.to_string()),
            target_metric_key,
            disposition.as_str(),
            message,
            encode_timestamp(OffsetDateTime::now_utc())?,
        ],
    )?;
    Ok(())
}

fn rewrite_outcome_metric_key(
    transaction: &Transaction<'_>,
    source: &NonEmptyText,
    target: &NonEmptyText,
) -> Result<(), StoreError> {
    let rows = {
        let mut statement = transaction
            .prepare("SELECT id, outcome_json FROM experiments WHERE outcome_json IS NOT NULL")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (experiment_id, raw_outcome) in rows {
        let mut outcome = decode_json::<ExperimentOutcome>(&raw_outcome)?;
        let mut changed = false;
        if outcome.primary_metric.key == *source {
            outcome.primary_metric.key = target.clone();
            changed = true;
        }
        for metric in &mut outcome.supporting_metrics {
            if metric.key == *source {
                metric.key = target.clone();
                changed = true;
            }
        }
        if changed {
            let _ = transaction.execute(
                "UPDATE experiments SET outcome_json = ?2 WHERE id = ?1",
                params![experiment_id, encode_json(&outcome)?],
            )?;
        }
    }
    Ok(())
}

fn merge_experiment_metric_rows(
    transaction: &Transaction<'_>,
    source: MetricId,
    target: MetricId,
) -> Result<(), StoreError> {
    let rows = {
        let mut statement = transaction.prepare(
            "SELECT experiment_id FROM experiment_metrics WHERE metric_id = ?1 ORDER BY experiment_id",
        )?;
        statement
            .query_map(params![source.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for experiment_id in rows {
        let target_exists = transaction
            .query_row(
                "SELECT 1 FROM experiment_metrics WHERE experiment_id = ?1 AND metric_id = ?2",
                params![experiment_id, target.to_string()],
                |_row| Ok(()),
            )
            .optional()?
            .is_some();
        if target_exists {
            let _ = transaction.execute(
                "DELETE FROM experiment_metrics WHERE experiment_id = ?1 AND metric_id = ?2",
                params![experiment_id, source.to_string()],
            )?;
        } else {
            let _ = transaction.execute(
                "UPDATE experiment_metrics SET metric_id = ?2 WHERE experiment_id = ?1 AND metric_id = ?3",
                params![experiment_id, target.to_string(), source.to_string()],
            )?;
        }
    }
    Ok(())
}

fn merge_kpi_metric_alternatives(
    transaction: &Transaction<'_>,
    source: MetricId,
    target: MetricId,
) -> Result<(), StoreError> {
    let rows = {
        let mut statement = transaction
            .prepare("SELECT kpi_id FROM kpi_metric_alternatives WHERE metric_id = ?1")?;
        statement
            .query_map(params![source.to_string()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for kpi_id in rows {
        let target_exists = transaction
            .query_row(
                "SELECT 1 FROM kpi_metric_alternatives WHERE kpi_id = ?1 AND metric_id = ?2",
                params![kpi_id, target.to_string()],
                |_row| Ok(()),
            )
            .optional()?
            .is_some();
        if target_exists {
            let _ = transaction.execute(
                "DELETE FROM kpi_metric_alternatives WHERE kpi_id = ?1 AND metric_id = ?2",
                params![kpi_id, source.to_string()],
            )?;
        } else {
            let _ = transaction.execute(
                "UPDATE kpi_metric_alternatives SET metric_id = ?2 WHERE kpi_id = ?1 AND metric_id = ?3",
                params![kpi_id, target.to_string(), source.to_string()],
            )?;
        }
    }
    Ok(())
}

fn record_event(
    transaction: &Transaction<'_>,
    entity_kind: &str,
    entity_id: &str,
    revision: u64,
    event_kind: &str,
    snapshot: &impl Serialize,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO events (entity_kind, entity_id, revision, event_kind, occurred_at, snapshot_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            entity_kind,
            entity_id,
            revision,
            event_kind,
            encode_timestamp(OffsetDateTime::now_utc())?,
            encode_json(snapshot)?,
        ],
    )?;
    Ok(())
}

fn decode_tag_row(row: &rusqlite::Row<'_>) -> Result<TagRecord, rusqlite::Error> {
    Ok(TagRecord {
        id: TagId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        name: parse_tag_name(&row.get::<_, String>(1)?)?,
        description: parse_non_empty_text(&row.get::<_, String>(2)?)?,
        family_id: row
            .get::<_, Option<String>>(3)?
            .map(|raw| parse_tag_family_id(&raw))
            .transpose()?,
        family: row
            .get::<_, Option<String>>(4)?
            .map(TagFamilyName::new)
            .transpose()
            .map_err(core_to_sql_conversion_error)?,
        status: parse_tag_status(&row.get::<_, String>(5)?)?,
        revision: row.get(6)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
    })
}

fn decode_tag_family_row(row: &rusqlite::Row<'_>) -> Result<TagFamilyRecord, rusqlite::Error> {
    Ok(TagFamilyRecord {
        id: TagFamilyId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        name: TagFamilyName::new(row.get::<_, String>(1)?).map_err(core_to_sql_conversion_error)?,
        description: parse_non_empty_text(&row.get::<_, String>(2)?)?,
        mandatory: row.get::<_, i64>(3)? != 0,
        status: parse_tag_status(&row.get::<_, String>(4)?)?,
        revision: row.get(5)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(6)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
    })
}

fn decode_tag_name_history_row(
    row: &rusqlite::Row<'_>,
) -> Result<TagNameHistoryRecord, rusqlite::Error> {
    Ok(TagNameHistoryRecord {
        name: parse_tag_name(&row.get::<_, String>(0)?)?,
        target_tag_id: row
            .get::<_, Option<String>>(1)?
            .map(|raw| parse_tag_id(&raw))
            .transpose()?,
        target_tag_name: row
            .get::<_, Option<String>>(2)?
            .map(|raw| parse_tag_name(&raw))
            .transpose()?,
        disposition: parse_tag_name_disposition(&row.get::<_, String>(3)?)?,
        message: parse_non_empty_text(&row.get::<_, String>(4)?)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(5)?)?,
    })
}

fn decode_registry_lock_row(
    row: &rusqlite::Row<'_>,
) -> Result<RegistryLockRecord, rusqlite::Error> {
    Ok(RegistryLockRecord {
        id: RegistryLockId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        registry: RegistryName::new(row.get::<_, String>(1)?)
            .map_err(core_to_sql_conversion_error)?,
        mode: parse_registry_lock_mode(&row.get::<_, String>(2)?)?,
        scope_kind: parse_non_empty_text(&row.get::<_, String>(3)?)?,
        scope_id: parse_non_empty_text(&row.get::<_, String>(4)?)?,
        reason: parse_non_empty_text(&row.get::<_, String>(5)?)?,
        revision: row.get(6)?,
        locked_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
    })
}

fn registry_lock_reason(
    registry: &RegistryName,
    mode: RegistryLockMode,
) -> Result<NonEmptyText, StoreError> {
    let reason = match (registry.as_str(), mode) {
        ("tags", RegistryLockMode::Definition) => "new tag creation is locked from the Tags page",
        ("tags", RegistryLockMode::Assignment) => {
            "model tag assignment locks are retired; use mandatory families to shape tag use"
        }
        ("tags", RegistryLockMode::Family) => {
            "MCP tag registry editing is locked from the Tags page"
        }
        _ => {
            return Ok(NonEmptyText::new(format!(
                "{} {} writes are locked from the supervisor UI",
                registry.as_str(),
                mode.as_str()
            ))?);
        }
    };
    Ok(NonEmptyText::new(reason)?)
}

fn frontier_registry_lock_reason(
    registry: &RegistryName,
    mode: RegistryLockMode,
    frontier: &FrontierRecord,
) -> Result<NonEmptyText, StoreError> {
    if registry.as_str() == "kpis" && mode == RegistryLockMode::Assignment {
        return Ok(NonEmptyText::new(format!(
            "MCP KPI creation is locked for frontier `{}` from the Metrics page",
            frontier.slug
        ))?);
    }
    Ok(NonEmptyText::new(format!(
        "{} {} writes are locked for frontier `{}` from the supervisor UI",
        registry.as_str(),
        mode.as_str(),
        frontier.slug
    ))?)
}

fn decode_frontier_row(row: &rusqlite::Row<'_>) -> Result<FrontierRecord, rusqlite::Error> {
    Ok(FrontierRecord {
        id: FrontierId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        slug: parse_slug(&row.get::<_, String>(1)?)?,
        label: parse_non_empty_text(&row.get::<_, String>(2)?)?,
        objective: parse_non_empty_text(&row.get::<_, String>(3)?)?,
        status: parse_frontier_status(&row.get::<_, String>(4)?)?,
        brief: decode_json(&row.get::<_, String>(5)?).map_err(to_sql_conversion_error)?,
        revision: row.get(6)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
    })
}

fn decode_experiment_row(row: &rusqlite::Row<'_>) -> Result<ExperimentRecord, rusqlite::Error> {
    Ok(ExperimentRecord {
        id: ExperimentId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        slug: parse_slug(&row.get::<_, String>(1)?)?,
        frontier_id: FrontierId::from_uuid(parse_uuid_sql(&row.get::<_, String>(2)?)?),
        hypothesis_id: HypothesisId::from_uuid(parse_uuid_sql(&row.get::<_, String>(3)?)?),
        archived: row.get::<_, i64>(4)? != 0,
        title: parse_non_empty_text(&row.get::<_, String>(5)?)?,
        summary: parse_optional_non_empty_text(row.get::<_, Option<String>>(6)?)?,
        tags: Vec::new(),
        status: parse_experiment_status(&row.get::<_, String>(7)?)?,
        outcome: row
            .get::<_, Option<String>>(8)?
            .map(|raw| decode_json(&raw).map_err(to_sql_conversion_error))
            .transpose()?,
        revision: row.get(9)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(10)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(11)?)?,
    })
}

fn decode_artifact_row(row: &rusqlite::Row<'_>) -> Result<ArtifactRecord, rusqlite::Error> {
    Ok(ArtifactRecord {
        id: ArtifactId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        slug: parse_slug(&row.get::<_, String>(1)?)?,
        kind: parse_artifact_kind(&row.get::<_, String>(2)?)?,
        label: parse_non_empty_text(&row.get::<_, String>(3)?)?,
        summary: parse_optional_non_empty_text(row.get::<_, Option<String>>(4)?)?,
        locator: parse_non_empty_text(&row.get::<_, String>(5)?)?,
        media_type: parse_optional_non_empty_text(row.get::<_, Option<String>>(6)?)?,
        revision: row.get(7)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(9)?)?,
    })
}

fn decode_metric_definition_row(
    row: &rusqlite::Row<'_>,
) -> Result<MetricDefinition, rusqlite::Error> {
    Ok(MetricDefinition {
        id: parse_metric_id_sql(&row.get::<_, String>(0)?)?,
        key: parse_non_empty_text(&row.get::<_, String>(1)?)?,
        dimension: parse_metric_dimension(&row.get::<_, String>(2)?)?,
        unit: parse_metric_unit(&row.get::<_, String>(3)?)?,
        aggregation: parse_metric_aggregation(&row.get::<_, String>(4)?)?,
        objective: parse_optimization_objective(&row.get::<_, String>(5)?)?,
        description: parse_optional_non_empty_text(row.get::<_, Option<String>>(6)?)?,
        revision: row.get::<_, u64>(7)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(9)?)?,
    })
}

fn decode_kpi_row(row: &rusqlite::Row<'_>) -> Result<FrontierKpiRecord, rusqlite::Error> {
    Ok(FrontierKpiRecord {
        id: parse_kpi_id_sql(&row.get::<_, String>(0)?)?,
        frontier_id: FrontierId::from_uuid(parse_uuid_sql(&row.get::<_, String>(1)?)?),
        name: parse_non_empty_text(&row.get::<_, String>(2)?)?,
        objective: parse_optimization_objective(&row.get::<_, String>(3)?)?,
        description: parse_optional_non_empty_text(row.get::<_, Option<String>>(4)?)?,
        status: parse_tag_status(&row.get::<_, String>(5)?)?,
        revision: row.get(6)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
    })
}

fn decode_kpi_alternative_row(
    row: &rusqlite::Row<'_>,
) -> Result<KpiMetricAlternativeRecord, rusqlite::Error> {
    Ok(KpiMetricAlternativeRecord {
        kpi_id: parse_kpi_id_sql(&row.get::<_, String>(0)?)?,
        metric_id: parse_metric_id_sql(&row.get::<_, String>(1)?)?,
        metric_key: parse_non_empty_text(&row.get::<_, String>(2)?)?,
        precedence: row.get(3)?,
    })
}

fn decode_run_dimension_definition_row(
    row: &rusqlite::Row<'_>,
) -> Result<RunDimensionDefinition, rusqlite::Error> {
    Ok(RunDimensionDefinition {
        key: parse_non_empty_text(&row.get::<_, String>(0)?)?,
        value_type: parse_field_value_type(&row.get::<_, String>(1)?)?,
        description: parse_optional_non_empty_text(row.get::<_, Option<String>>(2)?)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(3)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(4)?)?,
    })
}

fn enforce_revision(
    kind: &'static str,
    selector: &str,
    expected: Option<u64>,
    observed: u64,
) -> Result<(), StoreError> {
    if let Some(expected) = expected
        && expected != observed
    {
        return Err(StoreError::RevisionMismatch {
            kind,
            selector: selector.to_owned(),
            expected,
            observed,
        });
    }
    Ok(())
}

fn validate_hypothesis_body(body: &NonEmptyText) -> Result<(), StoreError> {
    let raw = body.as_str().trim();
    if raw.contains("\n\n")
        || raw.lines().any(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with('-') || trimmed.starts_with('*') || trimmed.starts_with('#')
        })
    {
        return Err(StoreError::HypothesisBodyMustBeSingleParagraph);
    }
    Ok(())
}

fn capture_clean_git_commit(project_root: &Utf8Path) -> Result<GitCommitHash, StoreError> {
    assert_git_worktree(project_root)?;

    let head_output = run_git(project_root, &["rev-parse", "--verify", "HEAD"])?;
    if !head_output.success {
        return Err(StoreError::GitHeadRequired(project_root.to_path_buf()));
    }
    let commit_hash = GitCommitHash::new(head_output.stdout.trim().to_owned())?;

    let status_output = run_git(
        project_root,
        &["status", "--porcelain=v1", "--untracked-files=all"],
    )?;
    if !status_output.success {
        return Err(StoreError::GitCommandFailed {
            project_root: project_root.to_path_buf(),
            command: format_git_command(
                project_root,
                &["status", "--porcelain=v1", "--untracked-files=all"],
            ),
            stderr: status_output.stderr,
        });
    }
    let status = status_output.stdout.trim();
    if !status.is_empty() {
        return Err(StoreError::DirtyGitWorktree {
            project_root: project_root.to_path_buf(),
            status: status.to_owned(),
        });
    }
    Ok(commit_hash)
}

fn assert_git_worktree(project_root: &Utf8Path) -> Result<(), StoreError> {
    let args = ["rev-parse", "--is-inside-work-tree"];
    let output = run_git(project_root, &args)?;
    if output.success && output.stdout.trim() == "true" {
        return Ok(());
    }
    if output.stderr.contains("not a git repository") {
        return Err(StoreError::GitWorktreeRequired(project_root.to_path_buf()));
    }
    Err(StoreError::GitCommandFailed {
        project_root: project_root.to_path_buf(),
        command: format_git_command(project_root, &args),
        stderr: output.stderr,
    })
}

fn run_git(project_root: &Utf8Path, args: &[&str]) -> Result<GitCommandOutput, StoreError> {
    let command = format_git_command(project_root, args);
    let output = Command::new("git")
        .arg("-C")
        .arg(project_root.as_str())
        .args(args)
        .output()
        .map_err(|source| StoreError::GitSpawn {
            project_root: project_root.to_path_buf(),
            command: command.clone(),
            source,
        })?;
    Ok(GitCommandOutput {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

fn format_git_command(project_root: &Utf8Path, args: &[&str]) -> String {
    format!("git -C {} {}", project_root, args.join(" "))
}

struct GitCommandOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

fn parse_frontier_status(raw: &str) -> Result<FrontierStatus, rusqlite::Error> {
    match raw {
        "exploring" => Ok(FrontierStatus::Exploring),
        "paused" => Ok(FrontierStatus::Paused),
        "archived" => Ok(FrontierStatus::Archived),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid frontier status `{raw}`"),
            )),
        ))),
    }
}

fn parse_tag_status(raw: &str) -> Result<TagStatus, rusqlite::Error> {
    match raw {
        "active" => Ok(TagStatus::Active),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid tag status `{raw}`"),
            )),
        ))),
    }
}

fn parse_tag_name_disposition(raw: &str) -> Result<TagNameDisposition, rusqlite::Error> {
    match raw {
        "renamed" => Ok(TagNameDisposition::Renamed),
        "merged" => Ok(TagNameDisposition::Merged),
        "deleted" => Ok(TagNameDisposition::Deleted),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid tag name disposition `{raw}`"),
            )),
        ))),
    }
}

fn parse_registry_lock_mode(raw: &str) -> Result<RegistryLockMode, rusqlite::Error> {
    match raw {
        "definition" => Ok(RegistryLockMode::Definition),
        "assignment" => Ok(RegistryLockMode::Assignment),
        "family" => Ok(RegistryLockMode::Family),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid registry lock mode `{raw}`"),
            )),
        ))),
    }
}

fn parse_metric_unit(raw: &str) -> Result<MetricUnit, rusqlite::Error> {
    MetricUnit::new(raw).map_err(|error| {
        to_sql_conversion_error(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            error.to_string(),
        ))))
    })
}

fn parse_metric_dimension(raw: &str) -> Result<MetricDimension, rusqlite::Error> {
    match raw {
        "time" => Ok(MetricDimension::Time),
        "count" => Ok(MetricDimension::Count),
        "bytes" => Ok(MetricDimension::Bytes),
        "ratio" => Ok(MetricDimension::Ratio),
        "dimensionless" => Ok(MetricDimension::Dimensionless),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid metric dimension `{raw}`"),
            )),
        ))),
    }
}

fn parse_metric_aggregation(raw: &str) -> Result<MetricAggregation, rusqlite::Error> {
    match raw {
        "point" => Ok(MetricAggregation::Point),
        "mean" => Ok(MetricAggregation::Mean),
        "geomean" => Ok(MetricAggregation::Geomean),
        "median" => Ok(MetricAggregation::Median),
        "p95" => Ok(MetricAggregation::P95),
        "min" => Ok(MetricAggregation::Min),
        "max" => Ok(MetricAggregation::Max),
        "sum" => Ok(MetricAggregation::Sum),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid metric aggregation `{raw}`"),
            )),
        ))),
    }
}

fn parse_optimization_objective(raw: &str) -> Result<OptimizationObjective, rusqlite::Error> {
    match raw {
        "minimize" => Ok(OptimizationObjective::Minimize),
        "maximize" => Ok(OptimizationObjective::Maximize),
        "target" => Ok(OptimizationObjective::Target),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid objective `{raw}`"),
            )),
        ))),
    }
}

fn parse_field_value_type(raw: &str) -> Result<FieldValueType, rusqlite::Error> {
    match raw {
        "string" => Ok(FieldValueType::String),
        "numeric" => Ok(FieldValueType::Numeric),
        "boolean" => Ok(FieldValueType::Boolean),
        "timestamp" => Ok(FieldValueType::Timestamp),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid field type `{raw}`"),
            )),
        ))),
    }
}

fn parse_experiment_status(raw: &str) -> Result<ExperimentStatus, rusqlite::Error> {
    match raw {
        "open" => Ok(ExperimentStatus::Open),
        "closed" => Ok(ExperimentStatus::Closed),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid experiment status `{raw}`"),
            )),
        ))),
    }
}

fn parse_artifact_kind(raw: &str) -> Result<ArtifactKind, rusqlite::Error> {
    match raw {
        "document" => Ok(ArtifactKind::Document),
        "link" => Ok(ArtifactKind::Link),
        "log" => Ok(ArtifactKind::Log),
        "table" => Ok(ArtifactKind::Table),
        "plot" => Ok(ArtifactKind::Plot),
        "dump" => Ok(ArtifactKind::Dump),
        "binary" => Ok(ArtifactKind::Binary),
        "other" => Ok(ArtifactKind::Other),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid artifact kind `{raw}`"),
            )),
        ))),
    }
}

fn resolve_selector(raw: &str) -> Result<Selector, StoreError> {
    if let Ok(uuid) = Uuid::parse_str(raw) {
        Ok(Selector::Id(uuid))
    } else {
        Ok(Selector::Slug(Slug::new(raw.to_owned())?))
    }
}

enum Selector {
    Id(Uuid),
    Slug(Slug),
}

fn slugify(raw: &str) -> Result<Slug, CoreError> {
    let mut slug = String::with_capacity(raw.len());
    let mut last_was_separator = true;
    for character in raw.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            slug.push(character);
            last_was_separator = false;
            continue;
        }
        if matches!(character, ' ' | '-' | '_' | '/' | ':') && !last_was_separator {
            slug.push('-');
            last_was_separator = true;
        }
    }
    if slug.ends_with('-') {
        let _ = slug.pop();
    }
    if slug.is_empty() {
        slug.push_str("untitled");
    }
    Slug::new(slug)
}

fn vertex_kind_name(vertex: VertexRef) -> &'static str {
    match vertex {
        VertexRef::Hypothesis(_) => "hypothesis",
        VertexRef::Experiment(_) => "experiment",
    }
}

fn attachment_target_kind_name(target: AttachmentTargetRef) -> &'static str {
    match target {
        AttachmentTargetRef::Frontier(_) => "frontier",
        AttachmentTargetRef::Hypothesis(_) => "hypothesis",
        AttachmentTargetRef::Experiment(_) => "experiment",
    }
}

fn decode_vertex_ref(kind: &str, raw_id: &str) -> Result<VertexRef, rusqlite::Error> {
    let uuid = parse_uuid_sql(raw_id)?;
    match kind {
        "hypothesis" => Ok(VertexRef::Hypothesis(HypothesisId::from_uuid(uuid))),
        "experiment" => Ok(VertexRef::Experiment(ExperimentId::from_uuid(uuid))),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid vertex kind `{kind}`"),
            )),
        ))),
    }
}

fn decode_attachment_target(
    kind: &str,
    raw_id: &str,
) -> Result<AttachmentTargetRef, rusqlite::Error> {
    let uuid = parse_uuid_sql(raw_id)?;
    match kind {
        "frontier" => Ok(AttachmentTargetRef::Frontier(FrontierId::from_uuid(uuid))),
        "hypothesis" => Ok(AttachmentTargetRef::Hypothesis(HypothesisId::from_uuid(
            uuid,
        ))),
        "experiment" => Ok(AttachmentTargetRef::Experiment(ExperimentId::from_uuid(
            uuid,
        ))),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid attachment target kind `{kind}`"),
            )),
        ))),
    }
}

fn derive_active_tags(
    active_hypotheses: &[HypothesisCurrentState],
    open_experiments: &[ExperimentSummary],
) -> Vec<TagName> {
    let mut tags = BTreeSet::new();
    for state in active_hypotheses {
        tags.extend(state.hypothesis.tags.iter().cloned());
        for experiment in &state.open_experiments {
            tags.extend(experiment.tags.iter().cloned());
        }
    }
    for experiment in open_experiments {
        tags.extend(experiment.tags.iter().cloned());
    }
    tags.into_iter().collect()
}

fn dimension_subset_matches(
    expected: &BTreeMap<NonEmptyText, RunDimensionValue>,
    observed: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> bool {
    expected.iter().all(|(key, value)| {
        observed
            .get(key)
            .is_some_and(|candidate| candidate == value)
    })
}

fn compare_metric_values(left: f64, right: f64, order: MetricRankOrder) -> std::cmp::Ordering {
    let ordering = left
        .partial_cmp(&right)
        .unwrap_or(std::cmp::Ordering::Equal);
    match order {
        MetricRankOrder::Asc => ordering,
        MetricRankOrder::Desc => ordering.reverse(),
    }
}

fn all_metrics(outcome: &ExperimentOutcome) -> Vec<MetricValue> {
    std::iter::once(outcome.primary_metric.clone())
        .chain(outcome.supporting_metrics.clone())
        .collect()
}

fn resolve_kpi_metric(
    kpi: &KpiSummary,
    outcome: &ExperimentOutcome,
    strict: bool,
    preferred_key: Option<&NonEmptyText>,
) -> Option<MetricValue> {
    let reported = all_metrics(outcome);
    if strict {
        let preferred_key = preferred_key?;
        return reported
            .into_iter()
            .find(|metric| &metric.key == preferred_key);
    }
    kpi.metrics.iter().find_map(|alternative| {
        reported
            .iter()
            .find(|metric| metric.key == alternative.key)
            .cloned()
    })
}

#[derive(Clone)]
struct ComparatorRank {
    exact_dimension_match: bool,
    core_dimension_matches: usize,
    matched_dimension_count: usize,
    same_hypothesis: bool,
    neighborhood_match: bool,
}

#[derive(Clone)]
struct NearestComparatorCandidate {
    experiment: ExperimentSummary,
    hypothesis: HypothesisSummary,
    dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    verdict: FrontierVerdict,
    closed_at: OffsetDateTime,
    structural_rank: ComparatorRank,
    metric_value: Option<MetricObservationSummary>,
}

fn comparator_rank(
    target_dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
    candidate_dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
    anchor_hypothesis_id: Option<HypothesisId>,
    candidate_hypothesis_id: HypothesisId,
    candidate_experiment_id: ExperimentId,
    influence_neighborhood: &[VertexRef],
) -> ComparatorRank {
    let matched_dimension_keys = target_dimensions
        .iter()
        .filter(|(key, value)| {
            candidate_dimensions
                .get(*key)
                .is_some_and(|candidate| candidate == *value)
        })
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>();
    let core_dimension_matches = matched_dimension_keys
        .iter()
        .filter(|key| {
            matches!(
                **key,
                "instance" | "profile" | "family" | "duration_s" | "budget_s"
            )
        })
        .count();
    let exact_dimension_match = !target_dimensions.is_empty()
        && target_dimensions.len() == candidate_dimensions.len()
        && dimension_subset_matches(target_dimensions, candidate_dimensions);
    let same_hypothesis = anchor_hypothesis_id == Some(candidate_hypothesis_id);
    let neighborhood_match = influence_neighborhood.iter().any(|vertex| {
        *vertex == VertexRef::Hypothesis(candidate_hypothesis_id)
            || *vertex == VertexRef::Experiment(candidate_experiment_id)
    });
    ComparatorRank {
        exact_dimension_match,
        core_dimension_matches,
        matched_dimension_count: matched_dimension_keys.len(),
        same_hypothesis,
        neighborhood_match,
    }
}

fn compare_structural_rank(left: &ComparatorRank, right: &ComparatorRank) -> std::cmp::Ordering {
    (
        left.exact_dimension_match,
        left.core_dimension_matches,
        left.matched_dimension_count,
        left.same_hypothesis,
        left.neighborhood_match,
    )
        .cmp(&(
            right.exact_dimension_match,
            right.core_dimension_matches,
            right.matched_dimension_count,
            right.same_hypothesis,
            right.neighborhood_match,
        ))
}

fn preferred_metric_ordering(left: f64, right: f64, order: MetricRankOrder) -> std::cmp::Ordering {
    compare_metric_values(left, right, order).reverse()
}

fn pick_nearest_bucket(
    candidates: &[NearestComparatorCandidate],
    verdict: FrontierVerdict,
    metric_key: Option<&str>,
) -> Option<ExperimentNearestHit> {
    candidates
        .iter()
        .filter(|candidate| candidate.verdict == verdict)
        .max_by(|left, right| {
            compare_structural_rank(&left.structural_rank, &right.structural_rank)
                .then_with(|| left.closed_at.cmp(&right.closed_at))
        })
        .map(|candidate| nearest_hit(candidate, metric_key, false))
}

fn pick_champion_candidate(
    candidates: &[NearestComparatorCandidate],
    order: MetricRankOrder,
    metric_key: Option<&str>,
) -> Option<ExperimentNearestHit> {
    candidates
        .iter()
        .filter(|candidate| {
            matches!(
                candidate.verdict,
                FrontierVerdict::Accepted | FrontierVerdict::Kept
            ) && candidate.metric_value.is_some()
        })
        .max_by(|left, right| {
            compare_structural_rank(&left.structural_rank, &right.structural_rank)
                .then_with(|| match (&left.metric_value, &right.metric_value) {
                    (Some(left_metric), Some(right_metric)) => {
                        preferred_metric_ordering(left_metric.value, right_metric.value, order)
                    }
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                })
                .then_with(|| left.closed_at.cmp(&right.closed_at))
        })
        .map(|candidate| nearest_hit(candidate, metric_key, true))
}

fn nearest_hit(
    candidate: &NearestComparatorCandidate,
    metric_key: Option<&str>,
    is_champion: bool,
) -> ExperimentNearestHit {
    let mut reasons = Vec::new();
    if candidate.structural_rank.exact_dimension_match {
        reasons.push(must_non_empty_reason("exact dimension match"));
    } else if candidate.structural_rank.core_dimension_matches > 0 {
        reasons.push(must_non_empty_reason(format!(
            "matched {} core slice keys",
            candidate.structural_rank.core_dimension_matches
        )));
    } else if candidate.structural_rank.matched_dimension_count > 0 {
        reasons.push(must_non_empty_reason(format!(
            "matched {} requested dimensions",
            candidate.structural_rank.matched_dimension_count
        )));
    }
    if candidate.structural_rank.same_hypothesis {
        reasons.push(must_non_empty_reason("same owning hypothesis"));
    } else if candidate.structural_rank.neighborhood_match {
        reasons.push(must_non_empty_reason("same influence neighborhood"));
    }
    if is_champion {
        reasons.push(must_non_empty_reason(format!(
            "best closed non-rejected result{}",
            metric_key.map_or_else(String::new, |key| format!(" for {key}"))
        )));
    } else {
        reasons.push(must_non_empty_reason(format!(
            "nearest {} comparator",
            candidate.verdict.as_str()
        )));
    }
    ExperimentNearestHit {
        experiment: candidate.experiment.clone(),
        hypothesis: candidate.hypothesis.clone(),
        dimensions: candidate.dimensions.clone(),
        reasons,
        metric_value: candidate.metric_value.clone(),
    }
}

fn must_non_empty_reason(text: impl Into<String>) -> NonEmptyText {
    match NonEmptyText::new(text) {
        Ok(text) => text,
        Err(_) => unreachable!("comparator reasons must never be empty"),
    }
}

fn bool_to_sql(value: bool) -> i64 {
    i64::from(value)
}

fn count_rows(connection: &Connection, table: &str) -> Result<u64, StoreError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection
        .query_row(&sql, [], |row| row.get::<_, u64>(0))
        .map_err(StoreError::from)
}

fn count_rows_where(
    connection: &Connection,
    table: &str,
    predicate: &str,
) -> Result<u64, StoreError> {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
    connection
        .query_row(&sql, [], |row| row.get::<_, u64>(0))
        .map_err(StoreError::from)
}

fn apply_limit<T>(items: Vec<T>, limit: Option<u32>) -> Vec<T> {
    if let Some(limit) = limit {
        items.into_iter().take(limit as usize).collect()
    } else {
        items
    }
}

fn apply_optional_text_patch<T>(patch: Option<TextPatch<T>>, current: Option<T>) -> Option<T> {
    match patch {
        None => current,
        Some(TextPatch::Set(value)) => Some(value),
        Some(TextPatch::Clear) => None,
    }
}

fn write_json_file<T: Serialize>(path: &Utf8Path, value: &T) -> Result<(), StoreError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path.as_std_path(), bytes)?;
    Ok(())
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Utf8Path) -> Result<T, StoreError> {
    let bytes = fs::read(path.as_std_path())?;
    serde_json::from_slice(&bytes).map_err(StoreError::from)
}

fn encode_json<T: Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(StoreError::from)
}

fn decode_json<T: for<'de> Deserialize<'de>>(raw: &str) -> Result<T, StoreError> {
    serde_json::from_str(raw).map_err(StoreError::from)
}

fn encode_timestamp(timestamp: OffsetDateTime) -> Result<String, StoreError> {
    timestamp.format(&Rfc3339).map_err(StoreError::from)
}

fn decode_timestamp(raw: &str) -> Result<OffsetDateTime, time::error::Parse> {
    OffsetDateTime::parse(raw, &Rfc3339)
}

#[must_use]
pub fn legacy_state_root(project_root: &Utf8Path) -> Utf8PathBuf {
    project_root.join(STORE_DIR_NAME)
}

pub fn state_root_for_project_root(project_root: &Utf8Path) -> Result<Utf8PathBuf, StoreError> {
    let project_root = canonical_project_root(project_root)?;
    Ok(spinner_state_home()?.join(project_store_dir_name(&project_root)))
}

pub fn install_state_home_override(path: impl AsRef<Utf8Path>) -> Result<(), StoreError> {
    let state_home = canonicalize_utf8_path(path.as_ref())?;
    STATE_HOME_OVERRIDE
        .set(state_home)
        .map_err(|_| StoreError::InvalidInput("state home override already installed".to_owned()))
}

pub fn discover_project_root(
    path: impl AsRef<Utf8Path>,
) -> Result<Option<Utf8PathBuf>, StoreError> {
    let mut cursor = discovery_start(path.as_ref());
    loop {
        if state_root_for_project_root(&cursor)?.exists() {
            return Ok(Some(canonical_project_root(&cursor)?));
        }
        let Some(parent) = cursor.parent() else {
            return Ok(None);
        };
        cursor = parent.to_path_buf();
    }
}

pub fn preferred_project_root(path: impl AsRef<Utf8Path>) -> Result<Utf8PathBuf, StoreError> {
    let start = discovery_start(path.as_ref());
    let mut cursor = start.clone();
    loop {
        if has_git_marker(&cursor)? {
            return canonical_project_root(&cursor);
        }
        let Some(parent) = cursor.parent() else {
            return canonical_project_root(&start);
        };
        cursor = parent.to_path_buf();
    }
}

fn discovery_start(path: &Utf8Path) -> Utf8PathBuf {
    if matches!(path.file_name(), Some(STORE_DIR_NAME) | Some(GIT_DIR_NAME)) {
        return path
            .parent()
            .map_or_else(|| path.to_path_buf(), Utf8Path::to_path_buf);
    }
    match fs::metadata(path.as_std_path()) {
        Ok(metadata) if metadata.is_file() => path
            .parent()
            .map_or_else(|| path.to_path_buf(), Utf8Path::to_path_buf),
        _ => path.to_path_buf(),
    }
}

fn spinner_state_home() -> Result<Utf8PathBuf, StoreError> {
    if let Some(path) = STATE_HOME_OVERRIDE.get() {
        return Ok(path.join(STATE_HOME_DIR_NAME).join(PROJECT_STATE_DIR_NAME));
    }
    if let Some(path) = std::env::var_os("FIDGET_SPINNER_STATE_HOME") {
        return Ok(utf8_path(std::path::PathBuf::from(path))
            .join(STATE_HOME_DIR_NAME)
            .join(PROJECT_STATE_DIR_NAME));
    }
    let state_root = dirs::state_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join(".local/state")))
        .ok_or_else(|| StoreError::InvalidInput("state directory not found".to_owned()))?;
    Ok(utf8_path(state_root)
        .join(STATE_HOME_DIR_NAME)
        .join(PROJECT_STATE_DIR_NAME))
}

fn project_store_dir_name(project_root: &Utf8Path) -> String {
    let stem = project_root
        .file_name()
        .map_or_else(|| "project".to_owned(), sanitize_project_stem);
    let identity = Uuid::new_v5(&PROJECT_ROOT_NAMESPACE, project_root.as_str().as_bytes());
    format!("{stem}-{identity}")
}

fn sanitize_project_stem(raw: &str) -> String {
    let sanitized = raw
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let trimmed = sanitized.trim_matches('-');
    if trimmed.is_empty() {
        "project".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn canonical_project_root(project_root: &Utf8Path) -> Result<Utf8PathBuf, StoreError> {
    canonicalize_utf8_path(project_root)
}

fn canonicalize_utf8_path(path: &Utf8Path) -> Result<Utf8PathBuf, StoreError> {
    match fs::canonicalize(path.as_std_path()) {
        Ok(canonical) => Ok(utf8_path(canonical)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let Some(parent) = path.parent() else {
                return Err(StoreError::from(error));
            };
            let canonical_parent = fs::canonicalize(parent.as_std_path())?;
            Ok(utf8_path(canonical_parent).join(path.file_name().unwrap_or_default()))
        }
        Err(error) => Err(StoreError::from(error)),
    }
}

fn has_git_marker(path: &Utf8Path) -> Result<bool, StoreError> {
    let git_marker = path.join(".git");
    match fs::metadata(git_marker.as_std_path()) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StoreError::from(error)),
    }
}

fn utf8_path(path: impl Into<std::path::PathBuf>) -> Utf8PathBuf {
    Utf8PathBuf::from(path.into().to_string_lossy().into_owned())
}

fn to_sql_conversion_error(error: StoreError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn core_to_sql_conversion_error(error: CoreError) -> rusqlite::Error {
    to_sql_conversion_error(StoreError::from(error))
}

fn uuid_to_sql_conversion_error(error: uuid::Error) -> rusqlite::Error {
    to_sql_conversion_error(StoreError::from(error))
}

fn time_to_sql_conversion_error(error: time::error::Parse) -> rusqlite::Error {
    to_sql_conversion_error(StoreError::from(error))
}

fn parse_non_empty_text(raw: &str) -> Result<NonEmptyText, rusqlite::Error> {
    NonEmptyText::new(raw.to_owned()).map_err(core_to_sql_conversion_error)
}

fn parse_optional_non_empty_text(
    raw: Option<String>,
) -> Result<Option<NonEmptyText>, rusqlite::Error> {
    raw.map(|value| parse_non_empty_text(&value)).transpose()
}

fn parse_slug(raw: &str) -> Result<Slug, rusqlite::Error> {
    Slug::new(raw.to_owned()).map_err(core_to_sql_conversion_error)
}

fn parse_tag_name(raw: &str) -> Result<TagName, rusqlite::Error> {
    TagName::new(raw.to_owned()).map_err(core_to_sql_conversion_error)
}

fn parse_tag_id(raw: &str) -> Result<TagId, rusqlite::Error> {
    parse_uuid_sql(raw).map(TagId::from_uuid)
}

fn parse_tag_family_id(raw: &str) -> Result<TagFamilyId, rusqlite::Error> {
    parse_uuid_sql(raw).map(TagFamilyId::from_uuid)
}

fn parse_frontier_id_sql(raw: &str) -> Result<FrontierId, rusqlite::Error> {
    parse_uuid_sql(raw).map(FrontierId::from_uuid)
}

fn parse_metric_id_sql(raw: &str) -> Result<MetricId, rusqlite::Error> {
    parse_uuid_sql(raw).map(MetricId::from_uuid)
}

fn parse_kpi_id_sql(raw: &str) -> Result<KpiId, rusqlite::Error> {
    parse_uuid_sql(raw).map(KpiId::from_uuid)
}

fn parse_uuid_sql(raw: &str) -> Result<Uuid, rusqlite::Error> {
    Uuid::parse_str(raw).map_err(uuid_to_sql_conversion_error)
}

fn parse_timestamp_sql(raw: &str) -> Result<OffsetDateTime, rusqlite::Error> {
    decode_timestamp(raw).map_err(time_to_sql_conversion_error)
}
