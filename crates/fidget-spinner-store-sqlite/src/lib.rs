use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::process::Command;
use std::sync::OnceLock;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    CommandRecipe, CoreError, DefaultVisibility, ExecutionBackend, ExperimentAnalysis,
    ExperimentId, ExperimentOutcome, ExperimentRecord, ExperimentStatus, FieldValueType,
    FrontierBrief, FrontierId, FrontierKpiRecord, FrontierRecord, FrontierRoadmapItem,
    FrontierStatus, FrontierVerdict, GitCommitHash, HiddenByDefaultReason, HypothesisId,
    HypothesisRecord, KpiId, KpiOrdinal, MetricAggregation, MetricDefinition, MetricDimension,
    MetricId, MetricUnit, MetricValue, NonEmptyText, OptimizationObjective, RegistryLockId,
    RegistryLockMode, RegistryLockRecord, RegistryName, ReportedMetricValue,
    RunDimensionDefinition, RunDimensionValue, Slug, TagFamilyId, TagFamilyName, TagFamilyRecord,
    TagId, TagName, TagNameDisposition, TagNameHistoryRecord, TagRecord, TagRegistrySnapshot,
    VertexRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

mod query;

pub use query::{
    FrontierSqlColumn, FrontierSqlQuery, FrontierSqlQueryResult, FrontierSqlSchema, FrontierSqlView,
};

pub const STORE_DIR_NAME: &str = ".fidget_spinner";
pub const GIT_DIR_NAME: &str = ".git";
pub const STATE_DB_NAME: &str = "state.sqlite";
pub const CURRENT_STORE_FORMAT_VERSION: u32 = 13;
pub const STATE_HOME_DIR_NAME: &str = "fidget-spinner";
pub const PROJECT_STATE_DIR_NAME: &str = "projects";
const LEGACY_PROJECT_CONFIG_NAME: &str = "project.json";
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
    #[error("KPI metric `{0}` is not registered")]
    UnknownKpi(String),
    #[error("metric `{0}` already exists")]
    DuplicateMetricDefinition(NonEmptyText),
    #[error("metric `{0}` is already a KPI metric")]
    DuplicateKpi(NonEmptyText),
    #[error("mandatory KPI metric `{kpi}` is missing; report `{metrics}`")]
    MissingMandatoryKpi { kpi: NonEmptyText, metrics: String },
    #[error(
        "frontier `{frontier}` has no KPI metrics; promote at least one metric before MCP frontier work such as hypothesis.record, experiment.open, or experiment.close"
    )]
    MissingFrontierKpiContract { frontier: String },
    #[error("condition `{0}` is not registered")]
    UnknownRunDimension(NonEmptyText),
    #[error("condition `{0}` already exists")]
    DuplicateRunDimension(NonEmptyText),
    #[error("frontier selector `{0}` did not resolve")]
    UnknownFrontierSelector(String),
    #[error("hypothesis selector `{0}` did not resolve")]
    UnknownHypothesisSelector(String),
    #[error("experiment selector `{0}` did not resolve")]
    UnknownExperimentSelector(String),
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
    #[error("condition filter references unknown condition `{0}`")]
    UnknownDimensionFilter(String),
    #[error("metric scope `{scope}` requires a frontier selector")]
    MetricScopeRequiresFrontier { scope: &'static str },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectConfig {
    pub display_name: NonEmptyText,
    pub description: Option<NonEmptyText>,
    pub created_at: OffsetDateTime,
}

impl ProjectConfig {
    #[must_use]
    pub fn new(display_name: NonEmptyText) -> Self {
        Self {
            display_name,
            description: None,
            created_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectStatus {
    pub project_root: Utf8PathBuf,
    pub state_root: Utf8PathBuf,
    pub display_name: NonEmptyText,
    pub description: Option<NonEmptyText>,
    pub store_format_version: u32,
    pub frontier_count: u64,
    pub hypothesis_count: u64,
    pub experiment_count: u64,
    pub open_experiment_count: u64,
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
    pub label: Option<NonEmptyText>,
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
}

#[derive(Clone, Debug, Default)]
pub struct ListHypothesesQuery {
    pub frontier: Option<String>,
    pub tags: BTreeSet<TagName>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VertexSummary {
    pub vertex: VertexRef,
    pub frontier_id: FrontierId,
    pub slug: Slug,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HypothesisSummary {
    pub id: HypothesisId,
    pub slug: Slug,
    pub frontier_id: FrontierId,
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
    pub outcome: Option<ExperimentOutcomePatch>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentOutcomePatch {
    pub backend: ExecutionBackend,
    pub command: CommandRecipe,
    #[serde(rename = "conditions")]
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub primary_metric: ReportedMetricValue,
    pub supporting_metrics: Vec<ReportedMetricValue>,
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
    pub primary_metric: ReportedMetricValue,
    pub supporting_metrics: Vec<ReportedMetricValue>,
    pub verdict: FrontierVerdict,
    pub rationale: NonEmptyText,
    pub analysis: Option<ExperimentAnalysis>,
}

#[derive(Clone, Debug, Default)]
pub struct ListExperimentsQuery {
    pub frontier: Option<String>,
    pub hypothesis: Option<String>,
    pub tags: BTreeSet<TagName>,
    pub status: Option<ExperimentStatus>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricObservationSummary {
    pub key: NonEmptyText,
    pub value: f64,
    pub display_unit: MetricUnit,
    pub dimension: MetricDimension,
    pub objective: OptimizationObjective,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KpiSummary {
    pub id: KpiId,
    pub ordinal: KpiOrdinal,
    pub metric: MetricKeySummary,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentSummary {
    pub id: ExperimentId,
    pub slug: Slug,
    pub frontier_id: FrontierId,
    pub hypothesis_id: HypothesisId,
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
}

#[derive(Clone, Debug)]
pub struct DefineMetricRequest {
    pub key: NonEmptyText,
    pub dimension: MetricDimension,
    pub display_unit: Option<MetricUnit>,
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
pub struct UpdateMetricRequest {
    pub metric: NonEmptyText,
    pub description: TextPatch<NonEmptyText>,
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
    pub metric: NonEmptyText,
}

#[derive(Clone, Debug)]
pub struct DeleteKpiRequest {
    pub frontier: String,
    pub kpi: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveKpiDirection {
    Up,
    Down,
}

#[derive(Clone, Debug)]
pub struct MoveKpiRequest {
    pub frontier: String,
    pub kpi: String,
    pub direction: MoveKpiDirection,
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
    pub display_unit: MetricUnit,
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
    #[serde(rename = "conditions")]
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub reasons: Vec<NonEmptyText>,
    pub metric_value: Option<MetricObservationSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentNearestResult {
    pub metric: Option<MetricKeySummary>,
    #[serde(rename = "target_conditions")]
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
    #[serde(rename = "conditions")]
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
    #[serde(rename = "conditions")]
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
    #[serde(rename = "conditions")]
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

pub struct ProjectStore {
    project_root: Utf8PathBuf,
    state_root: Utf8PathBuf,
    config: ProjectConfig,
    connection: Connection,
}

#[derive(Clone, Debug)]
pub struct UpdateProjectRequest {
    pub description: TextPatch<NonEmptyText>,
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

        let database_path = state_root.join(STATE_DB_NAME);
        let connection = Connection::open(database_path.as_std_path())?;
        connection.pragma_update(None, "foreign_keys", 1_i64)?;
        connection.pragma_update(
            None,
            "user_version",
            i64::from(CURRENT_STORE_FORMAT_VERSION),
        )?;
        install_schema(&connection)?;
        replace_project_metadata(&connection, &config)?;

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
        let database_path = state_root.join(STATE_DB_NAME);
        let mut connection = Connection::open(database_path.as_std_path())?;
        connection.pragma_update(None, "foreign_keys", 1_i64)?;
        let observed_version = load_store_user_version(&connection)?;
        if observed_version != CURRENT_STORE_FORMAT_VERSION {
            migrate_store_to_current(&state_root, &mut connection, observed_version)?;
        }
        let observed_version = load_store_user_version(&connection)?;
        if observed_version != CURRENT_STORE_FORMAT_VERSION {
            return Err(StoreError::IncompatibleStoreFormatVersion {
                observed: observed_version,
                expected: CURRENT_STORE_FORMAT_VERSION,
            });
        }
        if legacy_artifact_schema_present(&connection)? {
            purge_legacy_artifact_schema(&connection)?;
        }
        let config = load_project_metadata(&connection)?;

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
            description: self.config.description.clone(),
            store_format_version: load_store_user_version(&self.connection)?,
            frontier_count: count_rows(&self.connection, "frontiers")?,
            hypothesis_count: count_rows(&self.connection, "hypotheses")?,
            experiment_count: count_rows(&self.connection, "experiments")?,
            open_experiment_count: count_rows_where(
                &self.connection,
                "experiments",
                "NOT EXISTS (
                    SELECT 1
                    FROM experiment_outcomes
                    WHERE experiment_outcomes.experiment_id = experiments.id
                )",
            )?,
        })
    }

    pub fn update_project(
        &mut self,
        request: UpdateProjectRequest,
    ) -> Result<ProjectStatus, StoreError> {
        self.config.description =
            apply_optional_text_patch(Some(request.description), self.config.description.clone());
        replace_project_metadata(&self.connection, &self.config)?;
        self.status()
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
        let display_unit = request
            .display_unit
            .unwrap_or_else(|| request.dimension.default_display_unit());
        if !request.dimension.supports(display_unit) {
            return Err(StoreError::InvalidInput(format!(
                "metric `{}` has dimension `{}`; display unit `{}` belongs to `{}`",
                request.key,
                request.dimension.as_str(),
                display_unit.as_str(),
                display_unit.dimension().as_str()
            )));
        }
        let record = MetricDefinition::new(
            request.key,
            request.dimension,
            display_unit,
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
                record.display_unit.as_str(),
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
        update_metric_definition_row(&transaction, &renamed)?;
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

    pub fn update_metric(
        &mut self,
        request: UpdateMetricRequest,
    ) -> Result<MetricDefinition, StoreError> {
        let metric = self
            .metric_definition(&request.metric)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.metric.clone()))?;
        let description =
            apply_optional_text_patch(Some(request.description), metric.description.clone());
        if description == metric.description {
            return Ok(metric);
        }
        let mut updated = metric.clone();
        updated.description = description;
        updated.revision = updated.revision.saturating_add(1);
        updated.updated_at = OffsetDateTime::now_utc();
        let transaction = self.connection.transaction()?;
        update_metric_definition_row(&transaction, &updated)?;
        record_event(
            &transaction,
            "metric",
            &metric.id.to_string(),
            updated.revision,
            "updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
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
        merge_kpi_metric_edges(&transaction, source.id, target.id)?;
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
                "metric `{}` is still referenced by {} observations and {} KPI edges; merge or remove those references before deletion",
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
        let frontier = self.resolve_frontier(&request.frontier)?;
        let metric = self
            .metric_definition(&request.metric)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(request.metric.clone()))?;
        if self.kpi_by_metric(frontier.id, metric.id)?.is_some() {
            return Err(StoreError::DuplicateKpi(metric.key));
        }
        let now = OffsetDateTime::now_utc();
        let record = FrontierKpiRecord {
            id: KpiId::fresh(),
            frontier_id: frontier.id,
            metric_id: metric.id,
            ordinal: self.next_kpi_ordinal(frontier.id)?,
            created_at: now,
        };
        let transaction = self.connection.transaction()?;
        insert_kpi(&transaction, &record)?;
        record_event(
            &transaction,
            "kpi",
            &record.id.to_string(),
            1,
            "created",
            &record,
        )?;
        transaction.commit()?;
        self.kpi_summary(record)
    }

    pub fn create_kpi_from_mcp(
        &mut self,
        request: CreateKpiRequest,
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
        self.create_kpi(request)
    }

    pub fn delete_kpi(&mut self, request: DeleteKpiRequest) -> Result<(), StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        let record = self
            .kpi_by_selector(frontier.id, &request.kpi)?
            .ok_or_else(|| StoreError::UnknownKpi(request.kpi.clone()))?;
        let transaction = self.connection.transaction()?;
        let _ = transaction.execute(
            "DELETE FROM frontier_kpis WHERE id = ?1",
            params![record.id.to_string()],
        )?;
        let revision = next_event_revision(&transaction, "kpi", &record.id.to_string())?;
        record_event(
            &transaction,
            "kpi",
            &record.id.to_string(),
            revision,
            "deleted",
            &record,
        )?;
        compact_frontier_kpi_ordinals(&transaction, frontier.id)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn move_kpi(&mut self, request: MoveKpiRequest) -> Result<(), StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        let record = self
            .kpi_by_selector(frontier.id, &request.kpi)?
            .ok_or_else(|| StoreError::UnknownKpi(request.kpi.clone()))?;
        let records = self.frontier_kpi_records(frontier.id)?;
        let index = records
            .iter()
            .position(|candidate| candidate.id == record.id)
            .ok_or_else(|| StoreError::UnknownKpi(request.kpi.clone()))?;
        let Some(neighbor_index) = (match request.direction {
            MoveKpiDirection::Up => index.checked_sub(1),
            MoveKpiDirection::Down => (index + 1 < records.len()).then_some(index + 1),
        }) else {
            return Ok(());
        };
        let neighbor = records[neighbor_index].clone();
        let moved = FrontierKpiRecord {
            ordinal: neighbor.ordinal,
            ..record.clone()
        };
        let transaction = self.connection.transaction()?;
        swap_kpi_ordinals(&transaction, &record, &neighbor)?;
        let revision = next_event_revision(&transaction, "kpi", &record.id.to_string())?;
        record_event(
            &transaction,
            "kpi",
            &record.id.to_string(),
            revision,
            "moved",
            &moved,
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
        let definition = self
            .metric_definition(&kpi.metric.key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(kpi.metric.key.clone()))?;
        let order = match kpi.metric.objective {
            OptimizationObjective::Minimize => MetricRankOrder::Asc,
            OptimizationObjective::Maximize => MetricRankOrder::Desc,
            OptimizationObjective::Target => {
                return Err(StoreError::MetricOrderRequired {
                    key: kpi.metric.key.to_string(),
                });
            }
        };
        let experiments = self
            .load_experiment_records(Some(frontier.id), None)?
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
            .map(|record| {
                let Some(outcome) = record.outcome.clone() else {
                    return Ok(None);
                };
                if !dimension_subset_matches(&query.dimensions, &outcome.dimensions) {
                    return Ok(None);
                }
                let Some(canonical_value) =
                    self.experiment_metric_canonical_value(record.id, definition.id)?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    KpiBestEntry {
                        experiment: self.experiment_summary_from_record(record.clone())?,
                        hypothesis: self.hypothesis_summary_from_record(
                            self.hypothesis_by_id(record.hypothesis_id)?,
                        )?,
                        value: definition.display_unit.display_value(canonical_value),
                        metric_key: definition.key.clone(),
                        dimensions: outcome.dimensions.clone(),
                    },
                    canonical_value,
                )))
            })
            .collect::<Result<Vec<_>, StoreError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
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
            "INSERT INTO run_dimension_definitions (key, value_type, description, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                record.key.as_str(),
                record.value_type.as_str(),
                record.description.as_ref().map(NonEmptyText::as_str),
                encode_timestamp(record.created_at)?,
            ],
        )?;
        Ok(record)
    }

    pub fn list_run_dimensions(&self) -> Result<Vec<RunDimensionDefinition>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, value_type, description, created_at
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
            "SELECT id, slug, label, objective, status, revision, created_at, updated_at
             FROM frontiers
             ORDER BY updated_at DESC, created_at DESC",
        )?;
        let rows = statement.query_map([], decode_frontier_row)?;
        rows.collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|record| self.hydrate_frontier_brief(record))
            .collect()
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

    fn hydrate_frontier_brief(
        &self,
        mut record: FrontierRecord,
    ) -> Result<FrontierRecord, StoreError> {
        record.brief = self.frontier_brief(record.id)?;
        Ok(record)
    }

    fn frontier_brief(&self, frontier_id: FrontierId) -> Result<FrontierBrief, StoreError> {
        let situation = self
            .connection
            .query_row(
                "SELECT situation FROM frontier_briefs WHERE frontier_id = ?1",
                params![frontier_id.to_string()],
                |row| parse_optional_non_empty_text(row.get::<_, Option<String>>(0)?),
            )
            .optional()?
            .flatten();
        let roadmap = {
            let mut statement = self.connection.prepare(
                "SELECT ordinal, hypothesis_id, summary
                 FROM frontier_roadmap_items
                 WHERE frontier_id = ?1
                 ORDER BY ordinal ASC",
            )?;
            let rows = statement.query_map(params![frontier_id.to_string()], |row| {
                let ordinal = row.get::<_, u32>(0)?;
                Ok(FrontierRoadmapItem {
                    rank: ordinal.saturating_add(1),
                    hypothesis_id: HypothesisId::from_uuid(parse_uuid_sql(
                        &row.get::<_, String>(1)?,
                    )?),
                    summary: parse_optional_non_empty_text(row.get::<_, Option<String>>(2)?)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let unknowns = {
            let mut statement = self.connection.prepare(
                "SELECT body
                 FROM frontier_unknowns
                 WHERE frontier_id = ?1
                 ORDER BY ordinal ASC",
            )?;
            let rows = statement.query_map(params![frontier_id.to_string()], |row| {
                parse_non_empty_text(&row.get::<_, String>(0)?)
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        Ok(FrontierBrief {
            situation,
            roadmap,
            unknowns,
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
        };
        let updated = FrontierRecord {
            label: request.label.unwrap_or(frontier.label.clone()),
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
        if origin == MutationOrigin::Mcp {
            self.assert_frontier_has_kpis(frontier.id)?;
        }
        let id = HypothesisId::fresh();
        let slug = self.unique_hypothesis_slug(request.slug, &request.title)?;
        let now = OffsetDateTime::now_utc();
        let record = HypothesisRecord {
            id,
            slug,
            frontier_id: frontier.id,
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
        let records = self.load_hypothesis_records(frontier_id)?;
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
            limit: None,
            ..ListExperimentsQuery::default()
        })?;
        let (open_experiments, closed_experiments): (Vec<_>, Vec<_>) = experiments
            .into_iter()
            .partition(|experiment| experiment.status == ExperimentStatus::Open);
        Ok(HypothesisDetail {
            record,
            parents,
            children,
            open_experiments,
            closed_experiments,
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
        if origin == MutationOrigin::Mcp {
            self.assert_frontier_has_kpis(hypothesis.frontier_id)?;
        }
        let id = ExperimentId::fresh();
        let slug = self.unique_experiment_slug(request.slug, &request.title)?;
        let now = OffsetDateTime::now_utc();
        let record = ExperimentRecord {
            id,
            slug,
            frontier_id: hypothesis.frontier_id,
            hypothesis_id: hypothesis.id,
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
        let records = self.load_experiment_records(frontier_id, hypothesis_id)?;
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
            .load_experiment_records(Some(frontier.id), None)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Closed)
            .map(|record| {
                let Some(outcome) = record.outcome.clone() else {
                    return Ok(None);
                };
                if !include_rejected && outcome.verdict == FrontierVerdict::Rejected {
                    return Ok(None);
                }
                let Some(canonical_value) =
                    self.experiment_metric_canonical_value(record.id, definition.id)?
                else {
                    return Ok(None);
                };
                Ok(Some(FrontierMetricPoint {
                    closed_at: outcome.closed_at,
                    dimensions: outcome.dimensions.clone(),
                    experiment: self.experiment_summary_from_record(record.clone())?,
                    hypothesis: self.hypothesis_summary_from_record(
                        self.hypothesis_by_id(record.hypothesis_id)?,
                    )?,
                    value: definition.display_unit.display_value(canonical_value),
                    metric_key: definition.key.clone(),
                    verdict: outcome.verdict,
                }))
            })
            .collect::<Result<Vec<_>, StoreError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        points.sort_by_key(|point| point.closed_at);
        let kpi = self
            .kpi_by_metric(frontier.id, definition.id)?
            .map(|record| self.kpi_summary(record))
            .transpose()?;
        Ok(FrontierMetricSeries {
            metric: self.metric_key_summary_from_definition(definition, Some(frontier.id))?,
            kpi,
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
            .load_experiment_records(frontier_id, hypothesis_id)?
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
            .map(|record| {
                let Some(outcome) = record.outcome.clone() else {
                    return Ok(None);
                };
                if !dimension_subset_matches(&query.dimensions, &outcome.dimensions) {
                    return Ok(None);
                }
                let Some(canonical_value) =
                    self.experiment_metric_canonical_value(record.id, definition.id)?
                else {
                    return Ok(None);
                };
                Ok(Some(MetricBestEntry {
                    experiment: self.experiment_summary_from_record(record.clone())?,
                    hypothesis: self.hypothesis_summary_from_record(
                        self.hypothesis_by_id(record.hypothesis_id)?,
                    )?,
                    value: definition.display_unit.display_value(canonical_value),
                    dimensions: outcome.dimensions.clone(),
                }))
            })
            .collect::<Result<Vec<_>, StoreError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            compare_metric_values(
                definition.display_unit.canonical_value(left.value),
                definition.display_unit.canonical_value(right.value),
                order,
            )
        });
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
                .and_then(|kpis| kpis.into_iter().next().map(|kpi| kpi.metric.key))
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
            .load_experiment_records(frontier_id, None)?
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
                let metric_value = metric_definition
                    .as_ref()
                    .map(|definition| {
                        self.experiment_metric_canonical_value(record.id, definition.id)
                            .map(|maybe_value| {
                                maybe_value.map(|canonical_value| MetricObservationSummary {
                                    key: definition.key.clone(),
                                    value: definition.display_unit.display_value(canonical_value),
                                    display_unit: definition.display_unit,
                                    dimension: definition.dimension,
                                    objective: definition.objective,
                                })
                            })
                    })
                    .transpose()?
                    .flatten();
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

    fn kpi_by_metric(
        &self,
        frontier_id: FrontierId,
        metric_id: MetricId,
    ) -> Result<Option<FrontierKpiRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, frontier_id, metric_id, ordinal, created_at
                 FROM frontier_kpis
                 WHERE frontier_id = ?1 AND metric_id = ?2",
                params![frontier_id.to_string(), metric_id.to_string()],
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
                "SELECT frontier_kpis.id, frontier_kpis.frontier_id, frontier_kpis.metric_id,
                        frontier_kpis.ordinal, frontier_kpis.created_at
                 FROM frontier_kpis
                 JOIN metric_definitions ON metric_definitions.id = frontier_kpis.metric_id
                 WHERE frontier_kpis.frontier_id = ?1
                   AND (frontier_kpis.id = ?2 OR metric_definitions.key = ?2)",
                params![frontier_id.to_string(), selector],
                decode_kpi_row,
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn next_kpi_ordinal(&self, frontier_id: FrontierId) -> Result<KpiOrdinal, StoreError> {
        self.connection
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0)
                 FROM frontier_kpis
                 WHERE frontier_id = ?1",
                params![frontier_id.to_string()],
                |row| parse_kpi_ordinal_sql(row.get::<_, i64>(0)?),
            )
            .map_err(StoreError::from)
    }

    fn frontier_kpi_records(
        &self,
        frontier_id: FrontierId,
    ) -> Result<Vec<FrontierKpiRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT frontier_kpis.id, frontier_kpis.frontier_id, frontier_kpis.metric_id,
                    frontier_kpis.ordinal, frontier_kpis.created_at
             FROM frontier_kpis
             JOIN metric_definitions ON metric_definitions.id = frontier_kpis.metric_id
             WHERE frontier_kpis.frontier_id = ?1
             ORDER BY frontier_kpis.ordinal ASC, metric_definitions.key ASC, frontier_kpis.id ASC",
        )?;
        let rows = statement.query_map(params![frontier_id.to_string()], decode_kpi_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn kpi_summary(&self, record: FrontierKpiRecord) -> Result<KpiSummary, StoreError> {
        let metric = self.metric_key_summary_from_definition(
            self.metric_definition_by_id(record.metric_id)?,
            Some(record.frontier_id),
        )?;
        Ok(KpiSummary {
            id: record.id,
            ordinal: record.ordinal,
            metric,
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
                .find(|kpi| kpi.metric.key.as_str() == selector || kpi.id.to_string() == selector)
                .ok_or_else(|| StoreError::UnknownKpi(selector.to_owned())),
            None => kpis
                .into_iter()
                .next()
                .ok_or_else(|| StoreError::UnknownKpi("frontier has no KPI metrics".to_owned())),
        }
    }

    fn run_dimension_definition(
        &self,
        key: &NonEmptyText,
    ) -> Result<Option<RunDimensionDefinition>, StoreError> {
        self.connection
            .query_row(
                "SELECT key, value_type, description, created_at
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
                "SELECT id, slug, frontier_id, title, summary, body, revision, created_at, updated_at
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
                    "SELECT id, slug, label, objective, status, revision, created_at, updated_at
                     FROM frontiers WHERE id = ?1",
                    params![uuid.to_string()],
                    decode_frontier_row,
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, label, objective, status, revision, created_at, updated_at
                     FROM frontiers WHERE slug = ?1",
                    params![slug.as_str()],
                    decode_frontier_row,
                )
                .optional()?,
        };
        record
            .map(|record| self.hydrate_frontier_brief(record))
            .transpose()?
            .ok_or_else(|| StoreError::UnknownFrontierSelector(selector.to_owned()))
    }

    fn resolve_hypothesis(&self, selector: &str) -> Result<HypothesisRecord, StoreError> {
        let record = match resolve_selector(selector)? {
            Selector::Id(uuid) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, title, summary, body, revision, created_at, updated_at
                     FROM hypotheses WHERE id = ?1",
                    params![uuid.to_string()],
                    |row| self.decode_hypothesis_row(row),
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, title, summary, body, revision, created_at, updated_at
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
                    "SELECT experiments.id, experiments.slug, hypotheses.frontier_id,
                            experiments.hypothesis_id, experiments.title, experiments.summary,
                            experiments.revision, experiments.created_at, experiments.updated_at
                     FROM experiments
                     JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
                     WHERE experiments.id = ?1",
                    params![uuid.to_string()],
                    decode_experiment_row,
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT experiments.id, experiments.slug, hypotheses.frontier_id,
                            experiments.hypothesis_id, experiments.title, experiments.summary,
                            experiments.revision, experiments.created_at, experiments.updated_at
                     FROM experiments
                     JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
                     WHERE experiments.slug = ?1",
                    params![slug.as_str()],
                    decode_experiment_row,
                )
                .optional()?,
        };
        record
            .ok_or_else(|| StoreError::UnknownExperimentSelector(selector.to_owned()))
            .and_then(|record| self.hydrate_experiment(record))
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
    ) -> Result<Vec<HypothesisRecord>, StoreError> {
        let records = if let Some(frontier_id) = frontier_id {
            let mut statement = self.connection.prepare(
                "SELECT id, slug, frontier_id, title, summary, body, revision, created_at, updated_at
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
                "SELECT id, slug, frontier_id, title, summary, body, revision, created_at, updated_at
                 FROM hypotheses
                 ORDER BY updated_at DESC, created_at DESC",
            )?;
            let rows = statement.query_map([], |row| self.decode_hypothesis_row(row))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        Ok(records)
    }

    fn load_experiment_records(
        &self,
        frontier_id: Option<FrontierId>,
        hypothesis_id: Option<HypothesisId>,
    ) -> Result<Vec<ExperimentRecord>, StoreError> {
        let base_sql = "SELECT experiments.id, experiments.slug, hypotheses.frontier_id,
                               experiments.hypothesis_id, experiments.title, experiments.summary,
                               experiments.revision, experiments.created_at, experiments.updated_at
                        FROM experiments
                        JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id";
        let records = match (frontier_id, hypothesis_id) {
            (Some(frontier_id), Some(hypothesis_id)) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} WHERE hypotheses.frontier_id = ?1 AND experiments.hypothesis_id = ?2 ORDER BY experiments.updated_at DESC, experiments.created_at DESC"
                ))?;
                let rows = statement.query_map(
                    params![frontier_id.to_string(), hypothesis_id.to_string()],
                    decode_experiment_row,
                )?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (Some(frontier_id), None) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} WHERE hypotheses.frontier_id = ?1 ORDER BY experiments.updated_at DESC, experiments.created_at DESC"
                ))?;
                let rows =
                    statement.query_map(params![frontier_id.to_string()], decode_experiment_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (None, Some(hypothesis_id)) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} WHERE experiments.hypothesis_id = ?1 ORDER BY experiments.updated_at DESC, experiments.created_at DESC"
                ))?;
                let rows = statement
                    .query_map(params![hypothesis_id.to_string()], decode_experiment_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            (None, None) => {
                let mut statement = self.connection.prepare(&format!(
                    "{base_sql} ORDER BY experiments.updated_at DESC, experiments.created_at DESC"
                ))?;
                let rows = statement.query_map([], decode_experiment_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
        };
        let records = records
            .into_iter()
            .map(|record| self.hydrate_experiment(record))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
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
            title: parse_non_empty_text(&row.get::<_, String>(3)?)?,
            summary: parse_non_empty_text(&row.get::<_, String>(4)?)?,
            body: parse_non_empty_text(&row.get::<_, String>(5)?)?,
            tags: self.hypothesis_tags(id)?,
            revision: row.get::<_, u64>(6)?,
            created_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
            updated_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
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

    fn hydrate_experiment(
        &self,
        mut record: ExperimentRecord,
    ) -> Result<ExperimentRecord, StoreError> {
        record.tags = self.experiment_tags(record.id)?;
        record.outcome = self.experiment_outcome(record.id)?;
        record.status = if record.outcome.is_some() {
            ExperimentStatus::Closed
        } else {
            ExperimentStatus::Open
        };
        Ok(record)
    }

    fn experiment_outcome(
        &self,
        experiment_id: ExperimentId,
    ) -> Result<Option<ExperimentOutcome>, StoreError> {
        let outcome = self
            .connection
            .query_row(
                "SELECT backend, verdict, rationale, analysis_summary, analysis_body,
                        working_directory, commit_hash, closed_at
                 FROM experiment_outcomes
                 WHERE experiment_id = ?1",
                params![experiment_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            backend,
            verdict,
            rationale,
            analysis_summary,
            analysis_body,
            working_directory,
            commit_hash,
            closed_at,
        )) = outcome
        else {
            return Ok(None);
        };
        let analysis = match (analysis_summary, analysis_body) {
            (Some(summary), Some(body)) => Some(ExperimentAnalysis {
                summary: NonEmptyText::new(summary)?,
                body: NonEmptyText::new(body)?,
            }),
            (None, None) => None,
            _ => {
                return Err(StoreError::InvalidInput(
                    "experiment outcome analysis is partially populated".to_owned(),
                ));
            }
        };
        let (primary_metric, supporting_metrics) =
            self.experiment_outcome_metrics(experiment_id)?;
        Ok(Some(ExperimentOutcome {
            backend: parse_execution_backend(&backend).map_err(StoreError::from)?,
            command: self.experiment_command(experiment_id, working_directory)?,
            dimensions: self.experiment_dimensions(experiment_id)?,
            primary_metric,
            supporting_metrics,
            verdict: parse_frontier_verdict(&verdict).map_err(StoreError::from)?,
            rationale: NonEmptyText::new(rationale)?,
            analysis,
            commit_hash: commit_hash.map(GitCommitHash::new).transpose()?,
            closed_at: OffsetDateTime::parse(&closed_at, &Rfc3339)?,
        }))
    }

    fn experiment_command(
        &self,
        experiment_id: ExperimentId,
        working_directory: Option<String>,
    ) -> Result<CommandRecipe, StoreError> {
        let mut argv_statement = self.connection.prepare(
            "SELECT arg
             FROM experiment_command_argv
             WHERE experiment_id = ?1
             ORDER BY ordinal ASC",
        )?;
        let argv = argv_statement
            .query_map(params![experiment_id.to_string()], |row| {
                parse_non_empty_text(&row.get::<_, String>(0)?)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut env_statement = self.connection.prepare(
            "SELECT key, value
             FROM experiment_command_env
             WHERE experiment_id = ?1
             ORDER BY key ASC",
        )?;
        let env = env_statement
            .query_map(params![experiment_id.to_string()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(CommandRecipe {
            working_directory: working_directory.map(Utf8PathBuf::from),
            argv,
            env,
        })
    }

    fn experiment_dimensions(
        &self,
        experiment_id: ExperimentId,
    ) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, StoreError> {
        let mut dimensions = BTreeMap::new();
        self.load_string_dimensions(experiment_id, &mut dimensions)?;
        self.load_numeric_dimensions(experiment_id, &mut dimensions)?;
        self.load_boolean_dimensions(experiment_id, &mut dimensions)?;
        self.load_timestamp_dimensions(experiment_id, &mut dimensions)?;
        Ok(dimensions)
    }

    fn load_string_dimensions(
        &self,
        experiment_id: ExperimentId,
        dimensions: &mut BTreeMap<NonEmptyText, RunDimensionValue>,
    ) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, value
             FROM experiment_dimension_strings
             WHERE experiment_id = ?1
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map(params![experiment_id.to_string()], |row| {
            Ok((
                parse_non_empty_text(&row.get::<_, String>(0)?)?,
                RunDimensionValue::String(parse_non_empty_text(&row.get::<_, String>(1)?)?),
            ))
        })?;
        for row in rows {
            let (key, value) = row?;
            reject_duplicate_dimension(dimensions, key, value)?;
        }
        Ok(())
    }

    fn load_numeric_dimensions(
        &self,
        experiment_id: ExperimentId,
        dimensions: &mut BTreeMap<NonEmptyText, RunDimensionValue>,
    ) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, value
             FROM experiment_dimension_numbers
             WHERE experiment_id = ?1
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map(params![experiment_id.to_string()], |row| {
            Ok((
                parse_non_empty_text(&row.get::<_, String>(0)?)?,
                RunDimensionValue::Numeric(row.get::<_, f64>(1)?),
            ))
        })?;
        for row in rows {
            let (key, value) = row?;
            reject_duplicate_dimension(dimensions, key, value)?;
        }
        Ok(())
    }

    fn load_boolean_dimensions(
        &self,
        experiment_id: ExperimentId,
        dimensions: &mut BTreeMap<NonEmptyText, RunDimensionValue>,
    ) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, value
             FROM experiment_dimension_booleans
             WHERE experiment_id = ?1
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map(params![experiment_id.to_string()], |row| {
            Ok((
                parse_non_empty_text(&row.get::<_, String>(0)?)?,
                RunDimensionValue::Boolean(row.get::<_, i64>(1)? != 0),
            ))
        })?;
        for row in rows {
            let (key, value) = row?;
            reject_duplicate_dimension(dimensions, key, value)?;
        }
        Ok(())
    }

    fn load_timestamp_dimensions(
        &self,
        experiment_id: ExperimentId,
        dimensions: &mut BTreeMap<NonEmptyText, RunDimensionValue>,
    ) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, value
             FROM experiment_dimension_timestamps
             WHERE experiment_id = ?1
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map(params![experiment_id.to_string()], |row| {
            Ok((
                parse_non_empty_text(&row.get::<_, String>(0)?)?,
                RunDimensionValue::Timestamp(parse_non_empty_text(&row.get::<_, String>(1)?)?),
            ))
        })?;
        for row in rows {
            let (key, value) = row?;
            reject_duplicate_dimension(dimensions, key, value)?;
        }
        Ok(())
    }

    fn experiment_outcome_metrics(
        &self,
        experiment_id: ExperimentId,
    ) -> Result<(MetricValue, Vec<MetricValue>), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT experiment_metrics.is_primary, experiment_metrics.value,
                    metric_definitions.key, metric_definitions.display_unit
             FROM experiment_metrics
             JOIN metric_definitions ON metric_definitions.id = experiment_metrics.metric_id
             WHERE experiment_metrics.experiment_id = ?1
             ORDER BY experiment_metrics.ordinal ASC",
        )?;
        let rows = statement.query_map(params![experiment_id.to_string()], |row| {
            let unit = parse_metric_unit(&row.get::<_, String>(3)?)?;
            Ok((
                row.get::<_, i64>(0)? != 0,
                MetricValue {
                    key: parse_non_empty_text(&row.get::<_, String>(2)?)?,
                    value: unit.display_value(row.get::<_, f64>(1)?),
                    unit,
                },
            ))
        })?;
        let mut primary_metric = None;
        let mut supporting_metrics = Vec::new();
        for row in rows {
            let (is_primary, metric) = row?;
            if is_primary {
                if primary_metric.replace(metric).is_some() {
                    return Err(StoreError::InvalidInput(format!(
                        "experiment `{experiment_id}` has multiple primary metric rows"
                    )));
                }
            } else {
                supporting_metrics.push(metric);
            }
        }
        let Some(primary_metric) = primary_metric else {
            return Err(StoreError::InvalidInput(format!(
                "experiment `{experiment_id}` has an outcome without a primary metric row"
            )));
        };
        Ok((primary_metric, supporting_metrics))
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
        let canonical_value = metric.unit.canonical_value(metric.value);
        Ok(MetricObservationSummary {
            key: metric.key.clone(),
            value: definition.display_unit.display_value(canonical_value),
            display_unit: definition.display_unit,
            dimension: definition.dimension,
            objective: definition.objective,
        })
    }

    fn latest_closed_experiment(
        &self,
        hypothesis_id: HypothesisId,
    ) -> Result<Option<ExperimentRecord>, StoreError> {
        self.load_experiment_records(None, Some(hypothesis_id))
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
                    title: record.title,
                    summary: record.summary,
                    updated_at: record.updated_at,
                })
            }
        }
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
            .load_experiment_records(frontier_id, None)?
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
            display_unit: definition.display_unit,
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
        Ok(self
            .frontier_kpis(frontier_id)?
            .into_iter()
            .filter_map(|kpi| {
                if seen.insert(kpi.metric.key.clone()) {
                    Some(kpi.metric)
                } else {
                    None
                }
            })
            .collect())
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
                        JOIN experiments experiments ON experiments.id = metrics.experiment_id
                        JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id";
        let count = if let Some(frontier_id) = frontier_id {
            self.connection.query_row(
                &format!("{base_sql} WHERE metrics.metric_id = ?1 AND hypotheses.frontier_id = ?2"),
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
            "SELECT DISTINCT hypotheses.frontier_id
             FROM experiment_metrics metrics
             JOIN experiments ON experiments.id = metrics.experiment_id
             JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
             WHERE metrics.metric_id = ?1
             UNION
             SELECT DISTINCT frontier_id
             FROM frontier_kpis
             WHERE metric_id = ?1",
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
             SELECT DISTINCT hypotheses.frontier_id
             FROM experiment_tags tags
             JOIN experiments ON experiments.id = tags.experiment_id
             JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
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
                "SELECT COUNT(*) FROM frontier_kpis WHERE metric_id = ?1",
                params![metric_id.to_string()],
                |row| row.get::<_, u64>(0),
            )
            .map_err(StoreError::from)
    }

    fn experiment_metric_canonical_value(
        &self,
        experiment_id: ExperimentId,
        metric_id: MetricId,
    ) -> Result<Option<f64>, StoreError> {
        self.connection
            .query_row(
                "SELECT value FROM experiment_metrics WHERE experiment_id = ?1 AND metric_id = ?2",
                params![experiment_id.to_string(), metric_id.to_string()],
                |row| row.get::<_, f64>(0),
            )
            .optional()
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
        let primary_metric = self.resolve_reported_metric_value(&patch.primary_metric)?;
        let mut supporting_metrics = Vec::with_capacity(patch.supporting_metrics.len());
        for metric in &patch.supporting_metrics {
            supporting_metrics.push(self.resolve_reported_metric_value(metric)?);
        }
        if origin.is_mcp() {
            self.assert_frontier_kpis_satisfied(frontier_id, &primary_metric, &supporting_metrics)?;
        }
        let git_capture_root = experiment_git_capture_root(&self.project_root, &patch.command);
        let (commit_hash, closed_at) = match existing {
            Some(outcome) => (outcome.commit_hash.clone(), outcome.closed_at),
            None => (
                Some(capture_clean_git_commit(&git_capture_root)?),
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

    fn resolve_reported_metric_value(
        &self,
        metric: &ReportedMetricValue,
    ) -> Result<MetricValue, StoreError> {
        let definition = self
            .metric_definition(&metric.key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))?;
        let unit = match metric.unit {
            Some(unit) if definition.dimension.supports(unit) => unit,
            Some(unit) => {
                return Err(StoreError::InvalidInput(format!(
                    "metric `{}` has dimension `{}`; unit `{}` belongs to `{}`",
                    metric.key,
                    definition.dimension.as_str(),
                    unit.as_str(),
                    unit.dimension().as_str()
                )));
            }
            None => definition.dimension.implicit_unit().ok_or_else(|| {
                StoreError::InvalidInput(format!(
                    "metric `{}` has dimension `{}`; report a unit: {}",
                    metric.key,
                    definition.dimension.as_str(),
                    definition.dimension.unit_catalog()
                ))
            })?,
        };
        Ok(MetricValue {
            key: metric.key.clone(),
            value: metric.value,
            unit,
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
        let kpis = self.require_frontier_kpis(frontier_id)?;
        for kpi in kpis {
            if reported.contains(&kpi.metric.key) {
                continue;
            }
            return Err(StoreError::MissingMandatoryKpi {
                kpi: kpi.metric.key.clone(),
                metrics: kpi.metric.key.to_string(),
            });
        }
        Ok(())
    }

    fn assert_frontier_has_kpis(&self, frontier_id: FrontierId) -> Result<(), StoreError> {
        self.require_frontier_kpis(frontier_id).map(|_| ())
    }

    fn require_frontier_kpis(
        &self,
        frontier_id: FrontierId,
    ) -> Result<Vec<KpiSummary>, StoreError> {
        let kpis = self.frontier_kpis(frontier_id)?;
        if kpis.is_empty() {
            return Err(StoreError::MissingFrontierKpiContract {
                frontier: self.frontier_slug_by_id(frontier_id)?,
            });
        }
        Ok(kpis)
    }

    fn tag_record_by_name(&self, name: &TagName) -> Result<Option<TagRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT tags.id, tags.name, tags.description, tags.family_id, tag_families.name,
                        tags.revision, tags.created_at, tags.updated_at
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
                "SELECT id, name, description, mandatory, revision, created_at, updated_at
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
                    tags.revision, tags.created_at, tags.updated_at
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
            "SELECT id, name, description, mandatory, revision, created_at, updated_at
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
        CREATE TABLE IF NOT EXISTS project_metadata (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            display_name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tags (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL,
            family_id TEXT REFERENCES tag_families(id) ON DELETE SET NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tag_families (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL,
            mandatory INTEGER NOT NULL,
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
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS frontier_briefs (
            frontier_id TEXT PRIMARY KEY NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            situation TEXT
        );

        CREATE TABLE IF NOT EXISTS frontier_roadmap_items (
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
            summary TEXT,
            PRIMARY KEY (frontier_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS frontier_unknowns (
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            body TEXT NOT NULL,
            PRIMARY KEY (frontier_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS hypotheses (
            id TEXT PRIMARY KEY NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
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
            hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            summary TEXT,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_outcomes (
            experiment_id TEXT PRIMARY KEY NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            backend TEXT NOT NULL,
            verdict TEXT NOT NULL,
            rationale TEXT NOT NULL,
            analysis_summary TEXT,
            analysis_body TEXT,
            working_directory TEXT,
            commit_hash TEXT,
            closed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_command_argv (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            arg TEXT NOT NULL,
            PRIMARY KEY (experiment_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS experiment_command_env (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (experiment_id, key)
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
            metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (frontier_id, metric_id),
            UNIQUE (frontier_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS run_dimension_definitions (
            key TEXT PRIMARY KEY NOT NULL,
            value_type TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_strings (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value TEXT NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_numbers (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value REAL NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_booleans (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value INTEGER NOT NULL CHECK (value IN (0, 1)),
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_timestamps (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value TEXT NOT NULL,
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

fn load_store_user_version(connection: &Connection) -> Result<u32, StoreError> {
    let observed: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    Ok(u32::try_from(observed).unwrap_or(0))
}

fn replace_project_metadata(
    connection: &Connection,
    config: &ProjectConfig,
) -> Result<(), StoreError> {
    let _ = connection.execute(
        "INSERT OR REPLACE INTO project_metadata (id, display_name, description, created_at)
         VALUES (1, ?1, ?2, ?3)",
        params![
            config.display_name.as_str(),
            config.description.as_ref().map(NonEmptyText::as_str),
            encode_timestamp(config.created_at)?,
        ],
    )?;
    Ok(())
}

fn load_project_metadata(connection: &Connection) -> Result<ProjectConfig, StoreError> {
    connection
        .query_row(
            "SELECT display_name, description, created_at FROM project_metadata WHERE id = 1",
            [],
            |row| {
                Ok(ProjectConfig {
                    display_name: parse_non_empty_text(&row.get::<_, String>(0)?)?,
                    description: parse_optional_non_empty_text(row.get::<_, Option<String>>(1)?)?,
                    created_at: parse_timestamp_sql(&row.get::<_, String>(2)?)?,
                })
            },
        )
        .map_err(StoreError::from)
}

fn migrate_store_to_current(
    state_root: &Utf8Path,
    connection: &mut Connection,
    observed_version: u32,
) -> Result<(), StoreError> {
    let mut version = observed_version;
    if version == 9 {
        migrate_store_v9_to_v10(connection)?;
        version = 10;
    }
    if version == 10 {
        migrate_store_v10_to_v11(connection)?;
        version = 11;
    }
    if version == 11 {
        migrate_store_v11_to_v12(state_root, connection)?;
        version = 12;
    }
    if version == 12 {
        migrate_store_v12_to_v13(connection)?;
        version = 13;
    }
    if version == CURRENT_STORE_FORMAT_VERSION {
        return Ok(());
    }
    Err(StoreError::IncompatibleStoreFormatVersion {
        observed: observed_version,
        expected: CURRENT_STORE_FORMAT_VERSION,
    })
}

fn migrate_store_v9_to_v10(connection: &mut Connection) -> Result<(), StoreError> {
    purge_legacy_artifact_schema(connection)?;
    connection.pragma_update(None, "user_version", 10_i64)?;
    Ok(())
}

fn migrate_store_v10_to_v11(connection: &mut Connection) -> Result<(), StoreError> {
    let transaction = connection.transaction()?;
    let mut definitions = {
        let mut statement = transaction.prepare(
            "SELECT id, key, dimension, display_unit, aggregation, objective, description, revision, created_at, updated_at
             FROM metric_definitions
             ORDER BY key ASC",
        )?;
        statement
            .query_map([], decode_metric_definition_row)?
            .collect::<Result<Vec<_>, _>>()?
    };
    inject_metric_units_into_legacy_outcomes(&transaction, &definitions)?;
    normalize_legacy_time_metric_keys(&transaction, &mut definitions)?;
    refresh_experiment_metric_index(&transaction)?;
    transaction.commit()?;
    connection.pragma_update(None, "user_version", 11_i64)?;
    Ok(())
}

fn migrate_store_v11_to_v12(
    state_root: &Utf8Path,
    connection: &mut Connection,
) -> Result<(), StoreError> {
    let config = read_legacy_project_metadata(state_root)?;
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS project_metadata (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            display_name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS frontier_briefs (
            frontier_id TEXT PRIMARY KEY NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            situation TEXT
        );

        CREATE TABLE IF NOT EXISTS frontier_roadmap_items (
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
            summary TEXT,
            PRIMARY KEY (frontier_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS frontier_unknowns (
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            body TEXT NOT NULL,
            PRIMARY KEY (frontier_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS experiment_outcomes (
            experiment_id TEXT PRIMARY KEY NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            backend TEXT NOT NULL,
            verdict TEXT NOT NULL,
            rationale TEXT NOT NULL,
            analysis_summary TEXT,
            analysis_body TEXT,
            working_directory TEXT,
            commit_hash TEXT,
            closed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_command_argv (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            arg TEXT NOT NULL,
            PRIMARY KEY (experiment_id, ordinal)
        );

        CREATE TABLE IF NOT EXISTS experiment_command_env (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_strings (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value TEXT NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_numbers (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value REAL NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_booleans (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value INTEGER NOT NULL CHECK (value IN (0, 1)),
            PRIMARY KEY (experiment_id, key)
        );

        CREATE TABLE IF NOT EXISTS experiment_dimension_timestamps (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            key TEXT NOT NULL REFERENCES run_dimension_definitions(key) ON DELETE CASCADE,
            value TEXT NOT NULL,
            PRIMARY KEY (experiment_id, key)
        );
        ",
    )?;
    let _ = transaction.execute(
        "INSERT OR REPLACE INTO project_metadata (id, display_name, description, created_at)
         VALUES (1, ?1, ?2, ?3)",
        params![
            config.display_name.as_str(),
            config.description.as_ref().map(NonEmptyText::as_str),
            encode_timestamp(config.created_at)?,
        ],
    )?;
    let frontier_briefs = {
        let mut statement = transaction.prepare("SELECT id, brief_json FROM frontiers")?;
        statement
            .query_map([], |row| {
                Ok((
                    FrontierId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
                    row.get::<_, String>(1)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (frontier_id, raw_brief) in frontier_briefs {
        let brief = decode_json::<FrontierBrief>(&raw_brief)?;
        replace_frontier_brief(&transaction, frontier_id, &brief)?;
    }
    let experiment_outcomes = {
        let mut statement = transaction
            .prepare("SELECT id, outcome_json FROM experiments WHERE outcome_json IS NOT NULL")?;
        statement
            .query_map([], |row| {
                Ok((
                    ExperimentId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
                    row.get::<_, String>(1)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (experiment_id, raw_outcome) in experiment_outcomes {
        let outcome = decode_json::<ExperimentOutcome>(&raw_outcome)?;
        replace_experiment_outcome(&transaction, experiment_id, Some(&outcome))?;
        replace_experiment_dimensions(&transaction, experiment_id, Some(&outcome))?;
        replace_experiment_metrics(&transaction, experiment_id, Some(&outcome))?;
    }
    drop_column_if_exists(&transaction, "tags", "status")?;
    drop_column_if_exists(&transaction, "tag_families", "status")?;
    drop_column_if_exists(&transaction, "frontiers", "brief_json")?;
    drop_column_if_exists(&transaction, "hypotheses", "archived")?;
    drop_column_if_exists(&transaction, "experiments", "frontier_id")?;
    drop_column_if_exists(&transaction, "experiments", "archived")?;
    drop_column_if_exists(&transaction, "experiments", "status")?;
    drop_column_if_exists(&transaction, "experiments", "outcome_json")?;
    drop_column_if_exists(&transaction, "frontier_kpis", "revision")?;
    drop_column_if_exists(&transaction, "frontier_kpis", "updated_at")?;
    drop_column_if_exists(&transaction, "run_dimension_definitions", "updated_at")?;
    let _ = transaction.execute("DROP TABLE IF EXISTS experiment_dimensions", [])?;
    transaction.commit()?;
    connection.pragma_update(None, "user_version", 12_i64)?;
    let legacy_config_path = state_root.join(LEGACY_PROJECT_CONFIG_NAME);
    match fs::remove_file(legacy_config_path.as_std_path()) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(StoreError::Io(error)),
    }
    Ok(())
}

fn migrate_store_v12_to_v13(connection: &mut Connection) -> Result<(), StoreError> {
    struct LegacyKpiEdge {
        id: String,
        frontier_id: String,
        metric_id: String,
        created_at: String,
    }

    let transaction = connection.transaction()?;
    let rows = {
        let mut statement = transaction.prepare(
            "SELECT frontier_kpis.id, frontier_kpis.frontier_id, frontier_kpis.metric_id, frontier_kpis.created_at
             FROM frontier_kpis
             JOIN metric_definitions ON metric_definitions.id = frontier_kpis.metric_id
             ORDER BY frontier_kpis.frontier_id ASC,
                      frontier_kpis.created_at ASC,
                      metric_definitions.key ASC,
                      frontier_kpis.id ASC",
        )?;
        statement
            .query_map([], |row| {
                Ok(LegacyKpiEdge {
                    id: row.get(0)?,
                    frontier_id: row.get(1)?,
                    metric_id: row.get(2)?,
                    created_at: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    transaction.execute_batch(
        "
        CREATE TABLE frontier_kpis_v13 (
            id TEXT PRIMARY KEY NOT NULL,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (frontier_id, metric_id),
            UNIQUE (frontier_id, ordinal)
        );
        ",
    )?;
    let mut next_by_frontier = BTreeMap::<String, u32>::new();
    for row in rows {
        let next_ordinal = next_by_frontier.entry(row.frontier_id.clone()).or_insert(0);
        let ordinal = *next_ordinal;
        *next_ordinal = (*next_ordinal).saturating_add(1);
        let _ = transaction.execute(
            "INSERT INTO frontier_kpis_v13 (id, frontier_id, metric_id, ordinal, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                row.id,
                row.frontier_id,
                row.metric_id,
                i64::from(ordinal),
                row.created_at,
            ],
        )?;
    }
    transaction.execute_batch(
        "
        DROP TABLE frontier_kpis;
        ALTER TABLE frontier_kpis_v13 RENAME TO frontier_kpis;
        ",
    )?;
    transaction.commit()?;
    connection.pragma_update(None, "user_version", 13_i64)?;
    Ok(())
}

fn read_legacy_project_metadata(state_root: &Utf8Path) -> Result<ProjectConfig, StoreError> {
    let path = state_root.join(LEGACY_PROJECT_CONFIG_NAME);
    match read_json_file(&path) {
        Ok(config) => Ok(config),
        Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(
            ProjectConfig::new(NonEmptyText::new("Fidget Spinner Project")?),
        ),
        Err(error) => Err(error),
    }
}

fn drop_column_if_exists(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<(), StoreError> {
    if !table_has_column(connection, table, column)? {
        return Ok(());
    }
    let _ = connection.execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"), [])?;
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, StoreError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|candidate| candidate == column))
}

fn inject_metric_units_into_legacy_outcomes(
    transaction: &Transaction<'_>,
    definitions: &[MetricDefinition],
) -> Result<(), StoreError> {
    let unit_by_key = definitions
        .iter()
        .map(|definition| (definition.key.to_string(), definition.display_unit))
        .collect::<BTreeMap<_, _>>();
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
        let mut outcome = decode_json::<Value>(&raw_outcome)?;
        let mut changed = false;
        inject_metric_unit_in_value(
            outcome.get_mut("primary_metric"),
            &unit_by_key,
            &mut changed,
        )?;
        if let Some(metrics) = outcome
            .get_mut("supporting_metrics")
            .and_then(Value::as_array_mut)
        {
            for metric in metrics {
                inject_metric_unit_in_value(Some(metric), &unit_by_key, &mut changed)?;
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

fn inject_metric_unit_in_value(
    metric: Option<&mut Value>,
    unit_by_key: &BTreeMap<String, MetricUnit>,
    changed: &mut bool,
) -> Result<(), StoreError> {
    let Some(metric) = metric else {
        return Ok(());
    };
    let Some(object) = metric.as_object_mut() else {
        return Ok(());
    };
    if object.contains_key("unit") {
        return Ok(());
    }
    let key = object.get("key").and_then(Value::as_str).ok_or_else(|| {
        StoreError::InvalidInput("experiment outcome metric is missing its key".to_owned())
    })?;
    let key_text = NonEmptyText::new(key.to_owned())?;
    let unit = unit_by_key
        .get(key)
        .ok_or_else(|| StoreError::UnknownMetricDefinition(key_text))?;
    let _ = object.insert("unit".to_owned(), Value::String(unit.as_str().to_owned()));
    *changed = true;
    Ok(())
}

fn normalize_legacy_time_metric_keys(
    transaction: &Transaction<'_>,
    definitions: &mut Vec<MetricDefinition>,
) -> Result<(), StoreError> {
    let mut by_key = definitions
        .iter()
        .cloned()
        .map(|definition| (definition.key.to_string(), definition))
        .collect::<BTreeMap<_, _>>();
    let originals = definitions.clone();
    for definition in originals {
        if definition.dimension != MetricDimension::Time {
            continue;
        }
        let Some(normalized_key) = normalize_legacy_time_metric_key(definition.key.as_str()) else {
            continue;
        };
        if normalized_key == definition.key.as_str() {
            continue;
        }
        let normalized_key = NonEmptyText::new(normalized_key)?;
        if let Some(target) = by_key.get(normalized_key.as_str()).cloned() {
            if target.id == definition.id {
                continue;
            }
            if target.dimension != definition.dimension
                || target.aggregation != definition.aggregation
                || target.objective != definition.objective
            {
                return Err(StoreError::InvalidInput(format!(
                    "cannot normalize legacy metric `{}` into `{}` because their dimension/aggregation/objective differ",
                    definition.key, normalized_key
                )));
            }
            rewrite_outcome_metric_key(transaction, &definition.key, &normalized_key)?;
            merge_experiment_metric_rows(transaction, definition.id, target.id)?;
            merge_kpi_metric_edges(transaction, definition.id, target.id)?;
            delete_metric_definition_row(transaction, definition.id)?;
            insert_metric_name_history(
                transaction,
                definition.key.as_str(),
                Some(target.id),
                Some(normalized_key.as_str()),
                TagNameDisposition::Merged,
                &format!(
                    "metric `{}` was merged into `{}` during the v11 unit normalization",
                    definition.key, normalized_key
                ),
            )?;
            record_event(
                transaction,
                "metric",
                &definition.id.to_string(),
                definition.revision.saturating_add(1),
                "merged",
                &serde_json::json!({
                    "source": definition.key,
                    "target": normalized_key,
                    "reason": "legacy_millisecond_suffix_cleanup",
                }),
            )?;
            let _ = by_key.remove(definition.key.as_str());
            continue;
        }
        let mut renamed = definition.clone();
        renamed.key = normalized_key.clone();
        renamed.revision = renamed.revision.saturating_add(1);
        renamed.updated_at = OffsetDateTime::now_utc();
        rewrite_outcome_metric_key(transaction, &definition.key, &normalized_key)?;
        update_metric_definition_row(transaction, &renamed)?;
        insert_metric_name_history(
            transaction,
            definition.key.as_str(),
            Some(renamed.id),
            Some(normalized_key.as_str()),
            TagNameDisposition::Renamed,
            &format!(
                "metric `{}` was renamed to `{}` during the v11 unit normalization",
                definition.key, normalized_key
            ),
        )?;
        record_event(
            transaction,
            "metric",
            &definition.id.to_string(),
            renamed.revision,
            "renamed",
            &renamed,
        )?;
        let _ = by_key.remove(definition.key.as_str());
        let _ = by_key.insert(normalized_key.to_string(), renamed);
    }
    *definitions = by_key.into_values().collect();
    Ok(())
}

fn normalize_legacy_time_metric_key(raw: &str) -> Option<String> {
    let tokens = raw.split('_').collect::<Vec<_>>();
    if !tokens.contains(&"ms") {
        return None;
    }
    let filtered = tokens
        .into_iter()
        .filter(|token| *token != "ms")
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return None;
    }
    Some(filtered.join("_"))
}

fn refresh_experiment_metric_index(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let rows = {
        let mut statement = transaction
            .prepare("SELECT id, outcome_json FROM experiments ORDER BY created_at ASC")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (experiment_id, raw_outcome) in rows {
        let outcome = raw_outcome
            .map(|raw| decode_json::<ExperimentOutcome>(&raw))
            .transpose()?;
        replace_experiment_metrics(
            transaction,
            ExperimentId::from_uuid(parse_uuid_sql(&experiment_id)?),
            outcome.as_ref(),
        )?;
    }
    Ok(())
}

fn legacy_artifact_schema_present(connection: &Connection) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table'
                  AND name IN ('artifacts', 'artifact_attachments')
            )",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)
        .map_err(StoreError::from)
}

fn purge_legacy_artifact_schema(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch(
        "
        BEGIN IMMEDIATE;
        DELETE FROM events WHERE entity_kind = 'artifact';
        DROP TABLE IF EXISTS artifact_attachments;
        DROP TABLE IF EXISTS artifacts;
        COMMIT;
        ",
    )?;
    let _ = connection.execute_batch("VACUUM");
    Ok(())
}

fn insert_tag(transaction: &Transaction<'_>, tag: &TagRecord) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO tags (id, name, description, family_id, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            tag.id.to_string(),
            tag.name.as_str(),
            tag.description.as_str(),
            tag.family_id.map(|id| id.to_string()),
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
         SET name = ?2, description = ?3, family_id = ?4, revision = ?5, updated_at = ?6
         WHERE id = ?1",
        params![
            tag.id.to_string(),
            tag.name.as_str(),
            tag.description.as_str(),
            tag.family_id.map(|id| id.to_string()),
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
        "INSERT INTO tag_families (id, name, description, mandatory, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            family.id.to_string(),
            family.name.as_str(),
            family.description.as_str(),
            bool_to_sql(family.mandatory),
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
         SET name = ?2, description = ?3, mandatory = ?4, revision = ?5, updated_at = ?6
         WHERE id = ?1",
        params![
            family.id.to_string(),
            family.name.as_str(),
            family.description.as_str(),
            bool_to_sql(family.mandatory),
            family.revision,
            encode_timestamp(family.updated_at)?,
        ],
    )?;
    Ok(())
}

fn insert_kpi(transaction: &Transaction<'_>, record: &FrontierKpiRecord) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO frontier_kpis (id, frontier_id, metric_id, ordinal, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            record.id.to_string(),
            record.frontier_id.to_string(),
            record.metric_id.to_string(),
            i64::from(record.ordinal.value()),
            encode_timestamp(record.created_at)?,
        ],
    )?;
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
        "INSERT INTO frontiers (id, slug, label, objective, status, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            frontier.id.to_string(),
            frontier.slug.as_str(),
            frontier.label.as_str(),
            frontier.objective.as_str(),
            frontier.status.as_str(),
            frontier.revision,
            encode_timestamp(frontier.created_at)?,
            encode_timestamp(frontier.updated_at)?,
        ],
    )?;
    replace_frontier_brief(transaction, frontier.id, &frontier.brief)?;
    Ok(())
}

fn replace_frontier_brief(
    transaction: &Transaction<'_>,
    frontier_id: FrontierId,
    brief: &FrontierBrief,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM frontier_briefs WHERE frontier_id = ?1",
        params![frontier_id.to_string()],
    )?;
    let _ = transaction.execute(
        "DELETE FROM frontier_roadmap_items WHERE frontier_id = ?1",
        params![frontier_id.to_string()],
    )?;
    let _ = transaction.execute(
        "DELETE FROM frontier_unknowns WHERE frontier_id = ?1",
        params![frontier_id.to_string()],
    )?;
    let _ = transaction.execute(
        "INSERT INTO frontier_briefs (frontier_id, situation) VALUES (?1, ?2)",
        params![
            frontier_id.to_string(),
            brief.situation.as_ref().map(NonEmptyText::as_str),
        ],
    )?;
    for (ordinal, item) in brief.roadmap.iter().enumerate() {
        let _ = transaction.execute(
            "INSERT INTO frontier_roadmap_items (frontier_id, ordinal, hypothesis_id, summary)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                frontier_id.to_string(),
                i64::try_from(ordinal).unwrap_or(i64::MAX),
                item.hypothesis_id.to_string(),
                item.summary.as_ref().map(NonEmptyText::as_str),
            ],
        )?;
    }
    for (ordinal, unknown) in brief.unknowns.iter().enumerate() {
        let _ = transaction.execute(
            "INSERT INTO frontier_unknowns (frontier_id, ordinal, body)
             VALUES (?1, ?2, ?3)",
            params![
                frontier_id.to_string(),
                i64::try_from(ordinal).unwrap_or(i64::MAX),
                unknown.as_str(),
            ],
        )?;
    }
    Ok(())
}

fn update_frontier_row(
    transaction: &Transaction<'_>,
    frontier: &FrontierRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE frontiers
         SET slug = ?2, label = ?3, objective = ?4, status = ?5, revision = ?6, updated_at = ?7
         WHERE id = ?1",
        params![
            frontier.id.to_string(),
            frontier.slug.as_str(),
            frontier.label.as_str(),
            frontier.objective.as_str(),
            frontier.status.as_str(),
            frontier.revision,
            encode_timestamp(frontier.updated_at)?,
        ],
    )?;
    replace_frontier_brief(transaction, frontier.id, &frontier.brief)?;
    Ok(())
}

fn insert_hypothesis(
    transaction: &Transaction<'_>,
    hypothesis: &HypothesisRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO hypotheses (id, slug, frontier_id, title, summary, body, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            hypothesis.id.to_string(),
            hypothesis.slug.as_str(),
            hypothesis.frontier_id.to_string(),
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
         SET slug = ?2, title = ?3, summary = ?4, body = ?5, revision = ?6, updated_at = ?7
         WHERE id = ?1",
        params![
            hypothesis.id.to_string(),
            hypothesis.slug.as_str(),
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
        "INSERT INTO experiments (id, slug, hypothesis_id, title, summary, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            experiment.id.to_string(),
            experiment.slug.as_str(),
            experiment.hypothesis_id.to_string(),
            experiment.title.as_str(),
            experiment.summary.as_ref().map(NonEmptyText::as_str),
            experiment.revision,
            encode_timestamp(experiment.created_at)?,
            encode_timestamp(experiment.updated_at)?,
        ],
    )?;
    replace_experiment_outcome(transaction, experiment.id, experiment.outcome.as_ref())?;
    replace_experiment_dimensions(transaction, experiment.id, experiment.outcome.as_ref())?;
    replace_experiment_metrics(transaction, experiment.id, experiment.outcome.as_ref())?;
    Ok(())
}

fn update_experiment_row(
    transaction: &Transaction<'_>,
    experiment: &ExperimentRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE experiments
         SET slug = ?2, title = ?3, summary = ?4, revision = ?5, updated_at = ?6
         WHERE id = ?1",
        params![
            experiment.id.to_string(),
            experiment.slug.as_str(),
            experiment.title.as_str(),
            experiment.summary.as_ref().map(NonEmptyText::as_str),
            experiment.revision,
            encode_timestamp(experiment.updated_at)?,
        ],
    )?;
    replace_experiment_outcome(transaction, experiment.id, experiment.outcome.as_ref())?;
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

fn replace_experiment_dimensions(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
    outcome: Option<&ExperimentOutcome>,
) -> Result<(), StoreError> {
    delete_experiment_dimension_rows(transaction, experiment_id)?;
    if let Some(outcome) = outcome {
        for (key, value) in &outcome.dimensions {
            insert_experiment_dimension(transaction, experiment_id, key, value)?;
        }
    }
    Ok(())
}

fn delete_experiment_dimension_rows(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
) -> Result<(), StoreError> {
    for table in [
        "experiment_dimension_strings",
        "experiment_dimension_numbers",
        "experiment_dimension_booleans",
        "experiment_dimension_timestamps",
    ] {
        let _ = transaction.execute(
            &format!("DELETE FROM {table} WHERE experiment_id = ?1"),
            params![experiment_id.to_string()],
        )?;
    }
    Ok(())
}

fn insert_experiment_dimension(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
    key: &NonEmptyText,
    value: &RunDimensionValue,
) -> Result<(), StoreError> {
    match value {
        RunDimensionValue::String(value) => {
            let _ = transaction.execute(
                "INSERT INTO experiment_dimension_strings (experiment_id, key, value)
                 VALUES (?1, ?2, ?3)",
                params![experiment_id.to_string(), key.as_str(), value.as_str()],
            )?;
        }
        RunDimensionValue::Numeric(value) => {
            let _ = transaction.execute(
                "INSERT INTO experiment_dimension_numbers (experiment_id, key, value)
                 VALUES (?1, ?2, ?3)",
                params![experiment_id.to_string(), key.as_str(), value],
            )?;
        }
        RunDimensionValue::Boolean(value) => {
            let _ = transaction.execute(
                "INSERT INTO experiment_dimension_booleans (experiment_id, key, value)
                 VALUES (?1, ?2, ?3)",
                params![experiment_id.to_string(), key.as_str(), bool_to_sql(*value)],
            )?;
        }
        RunDimensionValue::Timestamp(value) => {
            let _ = transaction.execute(
                "INSERT INTO experiment_dimension_timestamps (experiment_id, key, value)
                 VALUES (?1, ?2, ?3)",
                params![experiment_id.to_string(), key.as_str(), value.as_str()],
            )?;
        }
    }
    Ok(())
}

fn replace_experiment_outcome(
    transaction: &Transaction<'_>,
    experiment_id: ExperimentId,
    outcome: Option<&ExperimentOutcome>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM experiment_outcomes WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    let _ = transaction.execute(
        "DELETE FROM experiment_command_argv WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    let _ = transaction.execute(
        "DELETE FROM experiment_command_env WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    if let Some(outcome) = outcome {
        let _ = transaction.execute(
            "INSERT INTO experiment_outcomes (
                experiment_id, backend, verdict, rationale, analysis_summary, analysis_body,
                working_directory, commit_hash, closed_at
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                experiment_id.to_string(),
                outcome.backend.as_str(),
                outcome.verdict.as_str(),
                outcome.rationale.as_str(),
                outcome
                    .analysis
                    .as_ref()
                    .map(|analysis| analysis.summary.as_str()),
                outcome
                    .analysis
                    .as_ref()
                    .map(|analysis| analysis.body.as_str()),
                outcome
                    .command
                    .working_directory
                    .as_ref()
                    .map(|path| path.as_str()),
                outcome.commit_hash.as_ref().map(GitCommitHash::as_str),
                encode_timestamp(outcome.closed_at)?,
            ],
        )?;
        for (ordinal, arg) in outcome.command.argv.iter().enumerate() {
            let _ = transaction.execute(
                "INSERT INTO experiment_command_argv (experiment_id, ordinal, arg)
                 VALUES (?1, ?2, ?3)",
                params![
                    experiment_id.to_string(),
                    i64::try_from(ordinal).unwrap_or(i64::MAX),
                    arg.as_str(),
                ],
            )?;
        }
        for (key, value) in &outcome.command.env {
            let _ = transaction.execute(
                "INSERT INTO experiment_command_env (experiment_id, key, value)
                 VALUES (?1, ?2, ?3)",
                params![experiment_id.to_string(), key, value],
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
            let metric_id = transaction.query_row(
                "SELECT id FROM metric_definitions WHERE key = ?1",
                params![metric.key.as_str()],
                |row| parse_metric_id_sql(&row.get::<_, String>(0)?),
            )?;
            let value = metric.unit.canonical_value(metric.value);
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

fn update_metric_definition_row(
    transaction: &Transaction<'_>,
    metric: &MetricDefinition,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE metric_definitions
         SET key = ?2,
             dimension = ?3,
             display_unit = ?4,
             aggregation = ?5,
             objective = ?6,
             description = ?7,
             revision = ?8,
             updated_at = ?9
         WHERE id = ?1",
        params![
            metric.id.to_string(),
            metric.key.as_str(),
            metric.dimension.as_str(),
            metric.display_unit.as_str(),
            metric.aggregation.as_str(),
            metric.objective.as_str(),
            metric.description.as_ref().map(NonEmptyText::as_str),
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

fn merge_kpi_metric_edges(
    transaction: &Transaction<'_>,
    source: MetricId,
    target: MetricId,
) -> Result<(), StoreError> {
    let affected_frontiers = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT frontier_id
             FROM frontier_kpis
             WHERE metric_id IN (?1, ?2)
             ORDER BY frontier_id ASC",
        )?;
        statement
            .query_map(params![source.to_string(), target.to_string()], |row| {
                parse_frontier_id_sql(&row.get::<_, String>(0)?)
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let _ = transaction.execute(
        "DELETE FROM frontier_kpis
         WHERE metric_id = ?1
           AND EXISTS (
             SELECT 1 FROM frontier_kpis target
             WHERE target.frontier_id = frontier_kpis.frontier_id
               AND target.metric_id = ?2
           )",
        params![source.to_string(), target.to_string()],
    )?;
    let _ = transaction.execute(
        "UPDATE frontier_kpis SET metric_id = ?2 WHERE metric_id = ?1",
        params![source.to_string(), target.to_string()],
    )?;
    for frontier_id in affected_frontiers {
        compact_frontier_kpi_ordinals(transaction, frontier_id)?;
    }
    Ok(())
}

fn swap_kpi_ordinals(
    transaction: &Transaction<'_>,
    lhs: &FrontierKpiRecord,
    rhs: &FrontierKpiRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "UPDATE frontier_kpis SET ordinal = -1 WHERE id = ?1",
        params![lhs.id.to_string()],
    )?;
    let _ = transaction.execute(
        "UPDATE frontier_kpis SET ordinal = ?2 WHERE id = ?1",
        params![rhs.id.to_string(), i64::from(lhs.ordinal.value())],
    )?;
    let _ = transaction.execute(
        "UPDATE frontier_kpis SET ordinal = ?2 WHERE id = ?1",
        params![lhs.id.to_string(), i64::from(rhs.ordinal.value())],
    )?;
    Ok(())
}

fn compact_frontier_kpi_ordinals(
    transaction: &Transaction<'_>,
    frontier_id: FrontierId,
) -> Result<(), StoreError> {
    let kpi_ids = {
        let mut statement = transaction.prepare(
            "SELECT frontier_kpis.id
             FROM frontier_kpis
             JOIN metric_definitions ON metric_definitions.id = frontier_kpis.metric_id
             WHERE frontier_kpis.frontier_id = ?1
             ORDER BY frontier_kpis.ordinal ASC, metric_definitions.key ASC, frontier_kpis.id ASC",
        )?;
        statement
            .query_map(params![frontier_id.to_string()], |row| {
                parse_kpi_id_sql(&row.get::<_, String>(0)?)
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (index, kpi_id) in kpi_ids.iter().enumerate() {
        let offset = i64::try_from(index).map_err(|error| {
            StoreError::InvalidInput(format!("too many KPI edges to compact: {error}"))
        })?;
        let _ = transaction.execute(
            "UPDATE frontier_kpis SET ordinal = ?2 WHERE id = ?1",
            params![kpi_id.to_string(), -1_i64 - offset],
        )?;
    }
    for (index, kpi_id) in kpi_ids.iter().enumerate() {
        let ordinal = i64::try_from(index).map_err(|error| {
            StoreError::InvalidInput(format!("too many KPI edges to compact: {error}"))
        })?;
        let _ = transaction.execute(
            "UPDATE frontier_kpis SET ordinal = ?2 WHERE id = ?1",
            params![kpi_id.to_string(), ordinal],
        )?;
    }
    Ok(())
}

fn next_event_revision(
    transaction: &Transaction<'_>,
    entity_kind: &str,
    entity_id: &str,
) -> Result<u64, StoreError> {
    let raw = transaction.query_row(
        "SELECT COALESCE(MAX(revision) + 1, 1)
         FROM events
         WHERE entity_kind = ?1 AND entity_id = ?2",
        params![entity_kind, entity_id],
        |row| row.get::<_, i64>(0),
    )?;
    u64::try_from(raw).map_err(|error| {
        StoreError::InvalidInput(format!(
            "invalid next event revision `{raw}` for {entity_kind} `{entity_id}`: {error}"
        ))
    })
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
        revision: row.get(5)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(6)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
    })
}

fn decode_tag_family_row(row: &rusqlite::Row<'_>) -> Result<TagFamilyRecord, rusqlite::Error> {
    Ok(TagFamilyRecord {
        id: TagFamilyId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        name: TagFamilyName::new(row.get::<_, String>(1)?).map_err(core_to_sql_conversion_error)?,
        description: parse_non_empty_text(&row.get::<_, String>(2)?)?,
        mandatory: row.get::<_, i64>(3)? != 0,
        revision: row.get(4)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(5)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(6)?)?,
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
        brief: FrontierBrief::default(),
        revision: row.get(5)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(6)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
    })
}

fn decode_experiment_row(row: &rusqlite::Row<'_>) -> Result<ExperimentRecord, rusqlite::Error> {
    Ok(ExperimentRecord {
        id: ExperimentId::from_uuid(parse_uuid_sql(&row.get::<_, String>(0)?)?),
        slug: parse_slug(&row.get::<_, String>(1)?)?,
        frontier_id: FrontierId::from_uuid(parse_uuid_sql(&row.get::<_, String>(2)?)?),
        hypothesis_id: HypothesisId::from_uuid(parse_uuid_sql(&row.get::<_, String>(3)?)?),
        title: parse_non_empty_text(&row.get::<_, String>(4)?)?,
        summary: parse_optional_non_empty_text(row.get::<_, Option<String>>(5)?)?,
        tags: Vec::new(),
        status: ExperimentStatus::Open,
        outcome: None,
        revision: row.get(6)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(7)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(8)?)?,
    })
}

fn decode_metric_definition_row(
    row: &rusqlite::Row<'_>,
) -> Result<MetricDefinition, rusqlite::Error> {
    Ok(MetricDefinition {
        id: parse_metric_id_sql(&row.get::<_, String>(0)?)?,
        key: parse_non_empty_text(&row.get::<_, String>(1)?)?,
        dimension: parse_metric_dimension(&row.get::<_, String>(2)?)?,
        display_unit: parse_metric_unit(&row.get::<_, String>(3)?)?,
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
        metric_id: parse_metric_id_sql(&row.get::<_, String>(2)?)?,
        ordinal: parse_kpi_ordinal_sql(row.get::<_, i64>(3)?)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(4)?)?,
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

fn experiment_git_capture_root(project_root: &Utf8Path, command: &CommandRecipe) -> Utf8PathBuf {
    command.working_directory.as_ref().map_or_else(
        || project_root.to_path_buf(),
        |working_directory| {
            if working_directory.is_absolute() {
                working_directory.clone()
            } else {
                project_root.join(working_directory)
            }
        },
    )
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

fn parse_execution_backend(raw: &str) -> Result<ExecutionBackend, rusqlite::Error> {
    match raw {
        "manual" => Ok(ExecutionBackend::Manual),
        "local_process" => Ok(ExecutionBackend::LocalProcess),
        "worktree_process" => Ok(ExecutionBackend::WorktreeProcess),
        "ssh_process" => Ok(ExecutionBackend::SshProcess),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid execution backend `{raw}`"),
            )),
        ))),
    }
}

fn parse_frontier_verdict(raw: &str) -> Result<FrontierVerdict, rusqlite::Error> {
    match raw {
        "accepted" => Ok(FrontierVerdict::Accepted),
        "kept" => Ok(FrontierVerdict::Kept),
        "parked" => Ok(FrontierVerdict::Parked),
        "rejected" => Ok(FrontierVerdict::Rejected),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid frontier verdict `{raw}`"),
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

fn reject_duplicate_dimension(
    dimensions: &mut BTreeMap<NonEmptyText, RunDimensionValue>,
    key: NonEmptyText,
    value: RunDimensionValue,
) -> Result<(), StoreError> {
    if dimensions.insert(key.clone(), value).is_some() {
        return Err(StoreError::InvalidInput(format!(
            "experiment dimension `{key}` is stored in multiple typed dimension tables"
        )));
    }
    Ok(())
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
            "matched {} requested conditions",
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

fn parse_kpi_ordinal_sql(raw: i64) -> Result<KpiOrdinal, rusqlite::Error> {
    let value = u32::try_from(raw).map_err(|error| {
        to_sql_conversion_error(StoreError::InvalidInput(format!(
            "invalid KPI ordinal `{raw}`: {error}"
        )))
    })?;
    Ok(KpiOrdinal::new(value))
}

fn parse_uuid_sql(raw: &str) -> Result<Uuid, rusqlite::Error> {
    Uuid::parse_str(raw).map_err(uuid_to_sql_conversion_error)
}

fn parse_timestamp_sql(raw: &str) -> Result<OffsetDateTime, rusqlite::Error> {
    decode_timestamp(raw).map_err(time_to_sql_conversion_error)
}
