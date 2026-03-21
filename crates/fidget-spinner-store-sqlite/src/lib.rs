use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    ArtifactId, ArtifactKind, ArtifactRecord, AttachmentTargetRef, CommandRecipe, CoreError,
    ExecutionBackend, ExperimentAnalysis, ExperimentId, ExperimentOutcome, ExperimentRecord,
    ExperimentStatus, FieldValueType, FrontierBrief, FrontierId, FrontierRecord,
    FrontierRoadmapItem, FrontierStatus, FrontierVerdict, HypothesisId, HypothesisRecord,
    MetricDefinition, MetricUnit, MetricValue, MetricVisibility, NonEmptyText,
    OptimizationObjective, RunDimensionDefinition, RunDimensionValue, Slug, TagName, TagRecord,
    VertexRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

pub const STORE_DIR_NAME: &str = ".fidget_spinner";
pub const STATE_DB_NAME: &str = "state.sqlite";
pub const PROJECT_CONFIG_NAME: &str = "project.json";
pub const CURRENT_STORE_FORMAT_VERSION: u32 = 4;

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
        "project store format {observed} is incompatible with this binary (expected {expected}); reinitialize the store"
    )]
    IncompatibleStoreFormatVersion { observed: u32, expected: u32 },
    #[error("unknown tag `{0}`")]
    UnknownTag(TagName),
    #[error("tag `{0}` already exists")]
    DuplicateTag(TagName),
    #[error("metric `{0}` is not registered")]
    UnknownMetricDefinition(NonEmptyText),
    #[error("metric `{0}` already exists")]
    DuplicateMetricDefinition(NonEmptyText),
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
    Live,
    Visible,
    All,
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
pub struct UpdateFrontierBriefRequest {
    pub frontier: String,
    pub expected_revision: Option<u64>,
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
    pub objective: OptimizationObjective,
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
    pub objective: OptimizationObjective,
    pub visibility: MetricVisibility,
    pub description: Option<NonEmptyText>,
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
    pub objective: OptimizationObjective,
    pub visibility: MetricVisibility,
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
    pub active_metric_keys: Vec<MetricKeySummary>,
    pub active_hypotheses: Vec<HypothesisCurrentState>,
    pub open_experiments: Vec<ExperimentSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontierMetricPoint {
    pub experiment: ExperimentSummary,
    pub hypothesis: HypothesisSummary,
    pub value: f64,
    pub verdict: FrontierVerdict,
    pub closed_at: OffsetDateTime,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontierMetricSeries {
    pub frontier: FrontierRecord,
    pub metric: MetricKeySummary,
    pub points: Vec<FrontierMetricPoint>,
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
        let project_root = project_root.as_ref().to_path_buf();
        fs::create_dir_all(project_root.as_std_path())?;
        let state_root = state_root(&project_root);
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
        let project_root = project_root.as_ref().to_path_buf();
        let state_root = state_root(&project_root);
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
        if self
            .connection
            .query_row(
                "SELECT 1 FROM tags WHERE name = ?1",
                params![name.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(StoreError::DuplicateTag(name));
        }
        let created_at = OffsetDateTime::now_utc();
        let _ = self.connection.execute(
            "INSERT INTO tags (name, description, created_at) VALUES (?1, ?2, ?3)",
            params![
                name.as_str(),
                description.as_str(),
                encode_timestamp(created_at)?
            ],
        )?;
        Ok(TagRecord {
            name,
            description,
            created_at,
        })
    }

    pub fn list_tags(&self) -> Result<Vec<TagRecord>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT name, description, created_at FROM tags ORDER BY name ASC")?;
        let rows = statement.query_map([], |row| {
            Ok(TagRecord {
                name: parse_tag_name(&row.get::<_, String>(0)?)?,
                description: parse_non_empty_text(&row.get::<_, String>(1)?)?,
                created_at: parse_timestamp_sql(&row.get::<_, String>(2)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
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
            request.objective,
            request.visibility,
            request.description,
        );
        let _ = self.connection.execute(
            "INSERT INTO metric_definitions (key, unit, objective, visibility, description, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                record.key.as_str(),
                record.unit.as_str(),
                record.objective.as_str(),
                record.visibility.as_str(),
                record.description.as_ref().map(NonEmptyText::as_str),
                encode_timestamp(record.created_at)?,
                encode_timestamp(record.updated_at)?,
            ],
        )?;
        Ok(record)
    }

    pub fn list_metric_definitions(&self) -> Result<Vec<MetricDefinition>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT key, unit, objective, visibility, description, created_at, updated_at
             FROM metric_definitions
             ORDER BY key ASC",
        )?;
        let rows = statement.query_map([], decode_metric_definition_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
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

    pub fn list_frontiers(&self) -> Result<Vec<FrontierSummary>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, slug, label, objective, status, brief_json, revision, created_at, updated_at
             FROM frontiers
             ORDER BY updated_at DESC, created_at DESC",
        )?;
        let rows = statement.query_map([], decode_frontier_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)?
            .into_iter()
            .map(|record| {
                Ok(FrontierSummary {
                    active_hypothesis_count: self.active_hypothesis_count(record.id)?,
                    open_experiment_count: self.open_experiment_count(Some(record.id))?,
                    id: record.id,
                    slug: record.slug,
                    label: record.label,
                    objective: record.objective,
                    status: record.status,
                    updated_at: record.updated_at,
                })
            })
            .collect()
    }

    pub fn read_frontier(&self, selector: &str) -> Result<FrontierRecord, StoreError> {
        self.resolve_frontier(selector)
    }

    pub fn update_frontier_brief(
        &mut self,
        request: UpdateFrontierBriefRequest,
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
            revision: frontier.brief.revision.saturating_add(1),
            updated_at: Some(now),
        };
        let updated = FrontierRecord {
            brief,
            revision: frontier.revision.saturating_add(1),
            updated_at: now,
            ..frontier
        };
        let transaction = self.connection.transaction()?;
        update_frontier(&transaction, &updated)?;
        record_event(
            &transaction,
            "frontier",
            &updated.id.to_string(),
            updated.revision,
            "brief_updated",
            &updated,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn create_hypothesis(
        &mut self,
        request: CreateHypothesisRequest,
    ) -> Result<HypothesisRecord, StoreError> {
        validate_hypothesis_body(&request.body)?;
        self.assert_known_tags(&request.tags)?;
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
        replace_hypothesis_tags(&transaction, record.id, &request.tags)?;
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
        if let Some(tags) = request.tags.as_ref() {
            self.assert_known_tags(tags)?;
        }
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
        let transaction = self.connection.transaction()?;
        update_hypothesis_row(&transaction, &updated)?;
        replace_hypothesis_tags(
            &transaction,
            updated.id,
            &updated.tags.iter().cloned().collect::<BTreeSet<_>>(),
        )?;
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
        self.assert_known_tags(&request.tags)?;
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
        replace_experiment_tags(&transaction, record.id, &request.tags)?;
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
        let record = self.resolve_experiment(&request.experiment)?;
        enforce_revision(
            "experiment",
            &request.experiment,
            request.expected_revision,
            record.revision,
        )?;
        if let Some(tags) = request.tags.as_ref() {
            self.assert_known_tags(tags)?;
        }
        let outcome = match request.outcome {
            Some(patch) => Some(self.materialize_outcome(&patch)?),
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
        let transaction = self.connection.transaction()?;
        update_experiment_row(&transaction, &updated)?;
        replace_experiment_tags(
            &transaction,
            updated.id,
            &updated.tags.iter().cloned().collect::<BTreeSet<_>>(),
        )?;
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
        let outcome = self.materialize_outcome(&ExperimentOutcomePatch {
            backend: request.backend,
            command: request.command,
            dimensions: request.dimensions,
            primary_metric: request.primary_metric,
            supporting_metrics: request.supporting_metrics,
            verdict: request.verdict,
            rationale: request.rationale,
            analysis: request.analysis,
        })?;
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
        Ok(FrontierOpenProjection {
            frontier,
            active_tags,
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
                    verdict: outcome.verdict,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        points.sort_by_key(|point| point.closed_at);
        Ok(FrontierMetricSeries {
            metric: MetricKeySummary {
                key: definition.key.clone(),
                unit: definition.unit,
                objective: definition.objective,
                visibility: definition.visibility,
                description: definition.description,
                reference_count: self.metric_reference_count(Some(frontier.id), key)?,
            },
            frontier,
            points,
        })
    }

    pub fn metric_keys(&self, query: MetricKeysQuery) -> Result<Vec<MetricKeySummary>, StoreError> {
        let frontier_id = query
            .frontier
            .as_deref()
            .map(|selector| self.resolve_frontier(selector).map(|frontier| frontier.id))
            .transpose()?;
        let definitions = self.list_metric_definitions()?;
        let live_keys = frontier_id
            .map(|frontier_id| self.live_metric_key_names(frontier_id))
            .transpose()?
            .unwrap_or_default();
        let mut keys = definitions
            .into_iter()
            .filter(|definition| match query.scope {
                MetricScope::Live => live_keys.contains(definition.key.as_str()),
                MetricScope::Visible => definition.visibility.is_default_visible(),
                MetricScope::All => true,
            })
            .map(|definition| {
                Ok(MetricKeySummary {
                    reference_count: self.metric_reference_count(frontier_id, &definition.key)?,
                    key: definition.key,
                    unit: definition.unit,
                    objective: definition.objective,
                    visibility: definition.visibility,
                    description: definition.description,
                })
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
            .map(|(record, dimensions, value)| {
                Ok(MetricBestEntry {
                    experiment: self.experiment_summary_from_record(record.clone())?,
                    hypothesis: self.hypothesis_summary_from_record(
                        self.hypothesis_by_id(record.hypothesis_id)?,
                    )?,
                    value,
                    dimensions,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        entries.sort_by(|left, right| compare_metric_values(left.value, right.value, order));
        Ok(apply_limit(entries, query.limit))
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
                "SELECT key, unit, objective, visibility, description, created_at, updated_at
                 FROM metric_definitions
                 WHERE key = ?1",
                params![key.as_str()],
                decode_metric_definition_row,
            )
            .optional()
            .map_err(StoreError::from)
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
                    "SELECT id, slug, frontier_id, hypothesis_id, archived, title, summary, tags_json, status, outcome_json, revision, created_at, updated_at
                     FROM experiments WHERE id = ?1",
                    params![uuid.to_string()],
                    decode_experiment_row,
                )
                .optional()?,
            Selector::Slug(slug) => self
                .connection
                .query_row(
                    "SELECT id, slug, frontier_id, hypothesis_id, archived, title, summary, tags_json, status, outcome_json, revision, created_at, updated_at
                     FROM experiments WHERE slug = ?1",
                    params![slug.as_str()],
                    decode_experiment_row,
                )
                .optional()?,
        };
        record.ok_or_else(|| StoreError::UnknownExperimentSelector(selector.to_owned()))
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
        let base_sql = "SELECT id, slug, frontier_id, hypothesis_id, archived, title, summary, tags_json, status, outcome_json, revision, created_at, updated_at FROM experiments";
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
            "SELECT tag_name FROM hypothesis_tags WHERE hypothesis_id = ?1 ORDER BY tag_name ASC",
        )?;
        let rows = statement.query_map(params![id.to_string()], |row| {
            parse_tag_name(&row.get::<_, String>(0)?)
        })?;
        rows.collect::<Result<Vec<_>, _>>()
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
            .filter(|definition| definition.visibility.is_default_visible())
            .map(|definition| {
                Ok(MetricKeySummary {
                    reference_count: self
                        .metric_reference_count(Some(frontier_id), &definition.key)?,
                    key: definition.key,
                    unit: definition.unit,
                    objective: definition.objective,
                    visibility: definition.visibility,
                    description: definition.description,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        keys.sort_by(|left, right| left.key.as_str().cmp(right.key.as_str()));
        Ok(keys)
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
        key: &NonEmptyText,
    ) -> Result<u64, StoreError> {
        let base_sql = "SELECT COUNT(*)
                        FROM experiment_metrics metrics
                        JOIN experiments experiments ON experiments.id = metrics.experiment_id";
        let count = if let Some(frontier_id) = frontier_id {
            self.connection.query_row(
                &format!("{base_sql} WHERE metrics.key = ?1 AND experiments.frontier_id = ?2"),
                params![key.as_str(), frontier_id.to_string()],
                |row| row.get::<_, u64>(0),
            )?
        } else {
            self.connection.query_row(
                &format!("{base_sql} WHERE metrics.key = ?1"),
                params![key.as_str()],
                |row| row.get::<_, u64>(0),
            )?
        };
        Ok(count)
    }

    fn materialize_outcome(
        &self,
        patch: &ExperimentOutcomePatch,
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
        let _ = self
            .metric_definition(&patch.primary_metric.key)?
            .ok_or_else(|| StoreError::UnknownMetricDefinition(patch.primary_metric.key.clone()))?;
        for metric in &patch.supporting_metrics {
            let _ = self
                .metric_definition(&metric.key)?
                .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))?;
        }
        Ok(ExperimentOutcome {
            backend: patch.backend,
            command: patch.command.clone(),
            dimensions: patch.dimensions.clone(),
            primary_metric: patch.primary_metric.clone(),
            supporting_metrics: patch.supporting_metrics.clone(),
            verdict: patch.verdict,
            rationale: patch.rationale.clone(),
            analysis: patch.analysis.clone(),
            closed_at: OffsetDateTime::now_utc(),
        })
    }

    fn assert_known_tags(&self, tags: &BTreeSet<TagName>) -> Result<(), StoreError> {
        for tag in tags {
            if self
                .connection
                .query_row(
                    "SELECT 1 FROM tags WHERE name = ?1",
                    params![tag.as_str()],
                    |_| Ok(()),
                )
                .optional()?
                .is_none()
            {
                return Err(StoreError::UnknownTag(tag.clone()));
            }
        }
        Ok(())
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
            name TEXT PRIMARY KEY NOT NULL,
            description TEXT NOT NULL,
            created_at TEXT NOT NULL
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
            tag_name TEXT NOT NULL REFERENCES tags(name) ON DELETE CASCADE,
            PRIMARY KEY (hypothesis_id, tag_name)
        );

        CREATE TABLE IF NOT EXISTS experiments (
            id TEXT PRIMARY KEY NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
            archived INTEGER NOT NULL,
            title TEXT NOT NULL,
            summary TEXT,
            tags_json TEXT NOT NULL,
            status TEXT NOT NULL,
            outcome_json TEXT,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS experiment_tags (
            experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
            tag_name TEXT NOT NULL REFERENCES tags(name) ON DELETE CASCADE,
            PRIMARY KEY (experiment_id, tag_name)
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
            key TEXT PRIMARY KEY NOT NULL,
            unit TEXT NOT NULL,
            objective TEXT NOT NULL,
            visibility TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
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
            key TEXT NOT NULL REFERENCES metric_definitions(key) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            is_primary INTEGER NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (experiment_id, key, ordinal)
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

fn update_frontier(
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
    tags: &BTreeSet<TagName>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM hypothesis_tags WHERE hypothesis_id = ?1",
        params![hypothesis_id.to_string()],
    )?;
    for tag in tags {
        let _ = transaction.execute(
            "INSERT INTO hypothesis_tags (hypothesis_id, tag_name) VALUES (?1, ?2)",
            params![hypothesis_id.to_string(), tag.as_str()],
        )?;
    }
    Ok(())
}

fn insert_experiment(
    transaction: &Transaction<'_>,
    experiment: &ExperimentRecord,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "INSERT INTO experiments (id, slug, frontier_id, hypothesis_id, archived, title, summary, tags_json, status, outcome_json, revision, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            experiment.id.to_string(),
            experiment.slug.as_str(),
            experiment.frontier_id.to_string(),
            experiment.hypothesis_id.to_string(),
            bool_to_sql(experiment.archived),
            experiment.title.as_str(),
            experiment.summary.as_ref().map(NonEmptyText::as_str),
            encode_json(&experiment.tags)?,
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
         SET slug = ?2, archived = ?3, title = ?4, summary = ?5, tags_json = ?6, status = ?7, outcome_json = ?8, revision = ?9, updated_at = ?10
         WHERE id = ?1",
        params![
            experiment.id.to_string(),
            experiment.slug.as_str(),
            bool_to_sql(experiment.archived),
            experiment.title.as_str(),
            experiment.summary.as_ref().map(NonEmptyText::as_str),
            encode_json(&experiment.tags)?,
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
    tags: &BTreeSet<TagName>,
) -> Result<(), StoreError> {
    let _ = transaction.execute(
        "DELETE FROM experiment_tags WHERE experiment_id = ?1",
        params![experiment_id.to_string()],
    )?;
    for tag in tags {
        let _ = transaction.execute(
            "INSERT INTO experiment_tags (experiment_id, tag_name) VALUES (?1, ?2)",
            params![experiment_id.to_string(), tag.as_str()],
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
            let _ = transaction.execute(
                "INSERT INTO experiment_metrics (experiment_id, key, ordinal, is_primary, value)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    experiment_id.to_string(),
                    metric.key.as_str(),
                    i64::try_from(ordinal).unwrap_or(i64::MAX),
                    bool_to_sql(ordinal == 0),
                    metric.value,
                ],
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
        tags: decode_json(&row.get::<_, String>(7)?).map_err(to_sql_conversion_error)?,
        status: parse_experiment_status(&row.get::<_, String>(8)?)?,
        outcome: row
            .get::<_, Option<String>>(9)?
            .map(|raw| decode_json(&raw).map_err(to_sql_conversion_error))
            .transpose()?,
        revision: row.get(10)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(11)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(12)?)?,
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
        key: parse_non_empty_text(&row.get::<_, String>(0)?)?,
        unit: parse_metric_unit(&row.get::<_, String>(1)?)?,
        objective: parse_optimization_objective(&row.get::<_, String>(2)?)?,
        visibility: parse_metric_visibility(&row.get::<_, String>(3)?)?,
        description: parse_optional_non_empty_text(row.get::<_, Option<String>>(4)?)?,
        created_at: parse_timestamp_sql(&row.get::<_, String>(5)?)?,
        updated_at: parse_timestamp_sql(&row.get::<_, String>(6)?)?,
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

fn parse_metric_unit(raw: &str) -> Result<MetricUnit, rusqlite::Error> {
    MetricUnit::new(raw).map_err(|error| {
        to_sql_conversion_error(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            error.to_string(),
        ))))
    })
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

fn parse_metric_visibility(raw: &str) -> Result<MetricVisibility, rusqlite::Error> {
    match raw {
        "canonical" => Ok(MetricVisibility::Canonical),
        "minor" => Ok(MetricVisibility::Minor),
        "hidden" => Ok(MetricVisibility::Hidden),
        "archived" => Ok(MetricVisibility::Archived),
        _ => Err(to_sql_conversion_error(StoreError::Json(
            serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid metric visibility `{raw}`"),
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

fn state_root(project_root: &Utf8Path) -> Utf8PathBuf {
    project_root.join(STORE_DIR_NAME)
}

#[must_use]
pub fn discover_project_root(path: impl AsRef<Utf8Path>) -> Option<Utf8PathBuf> {
    let mut cursor = discovery_start(path.as_ref());
    loop {
        if state_root(&cursor).exists() {
            return Some(cursor);
        }
        let parent = cursor.parent()?;
        cursor = parent.to_path_buf();
    }
}

fn discovery_start(path: &Utf8Path) -> Utf8PathBuf {
    match fs::metadata(path.as_std_path()) {
        Ok(metadata) if metadata.is_file() => path
            .parent()
            .map_or_else(|| path.to_path_buf(), Utf8Path::to_path_buf),
        _ => path.to_path_buf(),
    }
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

fn parse_uuid_sql(raw: &str) -> Result<Uuid, rusqlite::Error> {
    Uuid::parse_str(raw).map_err(uuid_to_sql_conversion_error)
}

fn parse_timestamp_sql(raw: &str) -> Result<OffsetDateTime, rusqlite::Error> {
    decode_timestamp(raw).map_err(time_to_sql_conversion_error)
}
