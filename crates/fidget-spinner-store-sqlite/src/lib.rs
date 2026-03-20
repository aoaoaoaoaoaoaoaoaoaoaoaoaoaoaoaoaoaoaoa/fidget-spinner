use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::process::Command;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    AnnotationVisibility, CheckpointDisposition, CheckpointRecord, CheckpointSnapshotRef,
    CodeSnapshotRef, CommandRecipe, CompletedExperiment, DagEdge, DagNode, DiagnosticSeverity,
    EdgeKind, ExecutionBackend, ExperimentResult, FieldPresence, FieldRole, FieldValueType,
    FrontierContract, FrontierNote, FrontierProjection, FrontierRecord, FrontierStatus,
    FrontierVerdict, GitCommitHash, InferencePolicy, JsonObject, MetricDefinition, MetricSpec,
    MetricUnit, MetricValue, NodeAnnotation, NodeClass, NodeDiagnostics, NodePayload, NonEmptyText,
    OptimizationObjective, ProjectFieldSpec, ProjectSchema, RunDimensionDefinition,
    RunDimensionValue, RunRecord, RunStatus, TagName, TagRecord,
};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

pub const STORE_DIR_NAME: &str = ".fidget_spinner";
pub const STATE_DB_NAME: &str = "state.sqlite";
pub const PROJECT_CONFIG_NAME: &str = "project.json";
pub const PROJECT_SCHEMA_NAME: &str = "schema.json";

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("project store is not initialized at {0}")]
    MissingProjectStore(Utf8PathBuf),
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
    Core(#[from] fidget_spinner_core::CoreError),
    #[error("UUID parse failure")]
    Uuid(#[from] uuid::Error),
    #[error("node {0} was not found")]
    NodeNotFound(fidget_spinner_core::NodeId),
    #[error("frontier {0} was not found")]
    FrontierNotFound(fidget_spinner_core::FrontierId),
    #[error("checkpoint {0} was not found")]
    CheckpointNotFound(fidget_spinner_core::CheckpointId),
    #[error("node {0} is not a change node")]
    NodeNotChange(fidget_spinner_core::NodeId),
    #[error("frontier {frontier_id} has no champion checkpoint")]
    MissingChampionCheckpoint {
        frontier_id: fidget_spinner_core::FrontierId,
    },
    #[error("unknown tag `{0}`")]
    UnknownTag(TagName),
    #[error("tag `{0}` already exists")]
    DuplicateTag(TagName),
    #[error("note nodes require an explicit tag list; use an empty list if no tags apply")]
    NoteTagsRequired,
    #[error("{0} nodes require a non-empty summary")]
    ProseSummaryRequired(NodeClass),
    #[error("{0} nodes require a non-empty string payload field `body`")]
    ProseBodyRequired(NodeClass),
    #[error("git repository inspection failed for {0}")]
    GitInspectionFailed(Utf8PathBuf),
    #[error("metric `{0}` is not registered")]
    UnknownMetricDefinition(NonEmptyText),
    #[error(
        "metric `{key}` conflicts with existing definition ({existing_unit}/{existing_objective} vs {new_unit}/{new_objective})"
    )]
    ConflictingMetricDefinition {
        key: String,
        existing_unit: String,
        existing_objective: String,
        new_unit: String,
        new_objective: String,
    },
    #[error("run dimension `{0}` is not registered")]
    UnknownRunDimension(NonEmptyText),
    #[error("run dimension `{0}` already exists")]
    DuplicateRunDimension(NonEmptyText),
    #[error(
        "run dimension `{key}` conflicts with existing definition ({existing_type} vs {new_type})"
    )]
    ConflictingRunDimensionDefinition {
        key: String,
        existing_type: String,
        new_type: String,
    },
    #[error("run dimension `{key}` expects {expected} values, got {observed}")]
    InvalidRunDimensionValue {
        key: String,
        expected: String,
        observed: String,
    },
    #[error("schema field `{0}` was not found")]
    SchemaFieldNotFound(String),
    #[error("metric key `{key}` is ambiguous across sources: {sources}")]
    AmbiguousMetricKey { key: String, sources: String },
    #[error("metric key `{key}` for source `{metric_source}` requires an explicit order")]
    MetricOrderRequired { key: String, metric_source: String },
    #[error("metric key `{key}` for source `{metric_source}` has conflicting semantics")]
    MetricSemanticsAmbiguous { key: String, metric_source: String },
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
            store_format_version: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateNodeRequest {
    pub class: NodeClass,
    pub frontier_id: Option<fidget_spinner_core::FrontierId>,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: Option<BTreeSet<TagName>>,
    pub payload: NodePayload,
    pub annotations: Vec<NodeAnnotation>,
    pub attachments: Vec<EdgeAttachment>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EdgeAttachmentDirection {
    ExistingToNew,
    NewToExisting,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EdgeAttachment {
    pub node_id: fidget_spinner_core::NodeId,
    pub kind: EdgeKind,
    pub direction: EdgeAttachmentDirection,
}

impl EdgeAttachment {
    #[must_use]
    pub fn materialize(&self, new_node_id: fidget_spinner_core::NodeId) -> DagEdge {
        match self.direction {
            EdgeAttachmentDirection::ExistingToNew => DagEdge {
                source_id: self.node_id,
                target_id: new_node_id,
                kind: self.kind,
            },
            EdgeAttachmentDirection::NewToExisting => DagEdge {
                source_id: new_node_id,
                target_id: self.node_id,
                kind: self.kind,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ListNodesQuery {
    pub frontier_id: Option<fidget_spinner_core::FrontierId>,
    pub class: Option<NodeClass>,
    pub tags: BTreeSet<TagName>,
    pub include_archived: bool,
    pub limit: u32,
}

impl Default for ListNodesQuery {
    fn default() -> Self {
        Self {
            frontier_id: None,
            class: None,
            tags: BTreeSet::new(),
            include_archived: false,
            limit: 20,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeSummary {
    pub id: fidget_spinner_core::NodeId,
    pub class: NodeClass,
    pub track: fidget_spinner_core::NodeTrack,
    pub frontier_id: Option<fidget_spinner_core::FrontierId>,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: BTreeSet<TagName>,
    pub diagnostic_count: u64,
    pub hidden_annotation_count: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricFieldSource {
    RunMetric,
    ChangePayload,
    RunPayload,
    AnalysisPayload,
    DecisionPayload,
}

impl MetricFieldSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunMetric => "run_metric",
            Self::ChangePayload => "change_payload",
            Self::RunPayload => "run_payload",
            Self::AnalysisPayload => "analysis_payload",
            Self::DecisionPayload => "decision_payload",
        }
    }

    #[must_use]
    pub const fn from_payload_class(class: NodeClass) -> Option<Self> {
        match class {
            NodeClass::Change => Some(Self::ChangePayload),
            NodeClass::Run => Some(Self::RunPayload),
            NodeClass::Analysis => Some(Self::AnalysisPayload),
            NodeClass::Decision => Some(Self::DecisionPayload),
            NodeClass::Contract | NodeClass::Research | NodeClass::Enabling | NodeClass::Note => {
                None
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricRankOrder {
    Asc,
    Desc,
}

impl MetricRankOrder {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricBestQuery {
    pub key: NonEmptyText,
    pub frontier_id: Option<fidget_spinner_core::FrontierId>,
    pub source: Option<MetricFieldSource>,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub order: Option<MetricRankOrder>,
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricKeySummary {
    pub key: NonEmptyText,
    pub source: MetricFieldSource,
    pub experiment_count: u64,
    pub unit: Option<MetricUnit>,
    pub objective: Option<OptimizationObjective>,
    pub description: Option<NonEmptyText>,
    pub requires_order: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricBestEntry {
    pub key: NonEmptyText,
    pub source: MetricFieldSource,
    pub value: f64,
    pub order: MetricRankOrder,
    pub experiment_id: fidget_spinner_core::ExperimentId,
    pub frontier_id: fidget_spinner_core::FrontierId,
    pub change_node_id: fidget_spinner_core::NodeId,
    pub change_title: NonEmptyText,
    pub run_id: fidget_spinner_core::RunId,
    pub verdict: FrontierVerdict,
    pub candidate_checkpoint_id: fidget_spinner_core::CheckpointId,
    pub candidate_commit_hash: GitCommitHash,
    pub unit: Option<MetricUnit>,
    pub objective: Option<OptimizationObjective>,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricKeyQuery {
    pub frontier_id: Option<fidget_spinner_core::FrontierId>,
    pub source: Option<MetricFieldSource>,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

#[derive(Clone, Debug)]
pub struct DefineMetricRequest {
    pub key: NonEmptyText,
    pub unit: MetricUnit,
    pub objective: OptimizationObjective,
    pub description: Option<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub struct DefineRunDimensionRequest {
    pub key: NonEmptyText,
    pub value_type: FieldValueType,
    pub description: Option<NonEmptyText>,
}

#[derive(Clone, Debug)]
pub struct UpsertSchemaFieldRequest {
    pub name: NonEmptyText,
    pub node_classes: BTreeSet<NodeClass>,
    pub presence: FieldPresence,
    pub severity: DiagnosticSeverity,
    pub role: FieldRole,
    pub inference_policy: InferencePolicy,
    pub value_type: Option<FieldValueType>,
}

#[derive(Clone, Debug)]
pub struct RemoveSchemaFieldRequest {
    pub name: NonEmptyText,
    pub node_classes: Option<BTreeSet<NodeClass>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunDimensionSummary {
    pub key: NonEmptyText,
    pub value_type: FieldValueType,
    pub description: Option<NonEmptyText>,
    pub observed_run_count: u64,
    pub distinct_value_count: u64,
    pub sample_values: Vec<Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricPlaneMigrationReport {
    pub inserted_metric_definitions: u64,
    pub inserted_dimension_definitions: u64,
    pub inserted_dimension_values: u64,
}

#[derive(Clone, Debug)]
pub struct CreateFrontierRequest {
    pub label: NonEmptyText,
    pub contract_title: NonEmptyText,
    pub contract_summary: Option<NonEmptyText>,
    pub contract: FrontierContract,
    pub initial_checkpoint: Option<CheckpointSeed>,
}

#[derive(Clone, Debug)]
pub struct CheckpointSeed {
    pub summary: NonEmptyText,
    pub snapshot: CheckpointSnapshotRef,
}

#[derive(Clone, Debug)]
pub struct CloseExperimentRequest {
    pub frontier_id: fidget_spinner_core::FrontierId,
    pub base_checkpoint_id: fidget_spinner_core::CheckpointId,
    pub change_node_id: fidget_spinner_core::NodeId,
    pub candidate_summary: NonEmptyText,
    pub candidate_snapshot: CheckpointSnapshotRef,
    pub run_title: NonEmptyText,
    pub run_summary: Option<NonEmptyText>,
    pub backend: ExecutionBackend,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub command: CommandRecipe,
    pub code_snapshot: Option<CodeSnapshotRef>,
    pub primary_metric: MetricValue,
    pub supporting_metrics: Vec<MetricValue>,
    pub note: FrontierNote,
    pub verdict: FrontierVerdict,
    pub decision_title: NonEmptyText,
    pub decision_rationale: NonEmptyText,
    pub analysis_node_id: Option<fidget_spinner_core::NodeId>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentReceipt {
    pub checkpoint: CheckpointRecord,
    pub run_node: DagNode,
    pub run: RunRecord,
    pub decision_node: DagNode,
    pub experiment: CompletedExperiment,
}

pub struct ProjectStore {
    project_root: Utf8PathBuf,
    state_root: Utf8PathBuf,
    connection: Connection,
    config: ProjectConfig,
    schema: ProjectSchema,
}

impl ProjectStore {
    pub fn init(
        project_root: impl AsRef<Utf8Path>,
        display_name: NonEmptyText,
        schema_namespace: NonEmptyText,
    ) -> Result<Self, StoreError> {
        let project_root = project_root.as_ref().to_path_buf();
        let state_root = state_root(&project_root);
        fs::create_dir_all(state_root.join("blobs"))?;
        let config = ProjectConfig::new(display_name);
        write_json_file(&state_root.join(PROJECT_CONFIG_NAME), &config)?;
        let schema = ProjectSchema::default_with_namespace(schema_namespace);
        write_json_file(&state_root.join(PROJECT_SCHEMA_NAME), &schema)?;

        let mut connection = Connection::open(state_root.join(STATE_DB_NAME).as_std_path())?;
        upgrade_store(&mut connection)?;

        Ok(Self {
            project_root,
            state_root,
            connection,
            config,
            schema,
        })
    }

    pub fn open(project_root: impl AsRef<Utf8Path>) -> Result<Self, StoreError> {
        let requested_root = project_root.as_ref().to_path_buf();
        let project_root = discover_project_root(&requested_root)
            .ok_or(StoreError::MissingProjectStore(requested_root))?;
        let state_root = state_root(&project_root);
        let config = read_json_file::<ProjectConfig>(&state_root.join(PROJECT_CONFIG_NAME))?;
        let schema = read_json_file::<ProjectSchema>(&state_root.join(PROJECT_SCHEMA_NAME))?;
        let mut connection = Connection::open(state_root.join(STATE_DB_NAME).as_std_path())?;
        upgrade_store(&mut connection)?;
        Ok(Self {
            project_root,
            state_root,
            connection,
            config,
            schema,
        })
    }

    #[must_use]
    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }

    #[must_use]
    pub fn schema(&self) -> &ProjectSchema {
        &self.schema
    }

    pub fn upsert_schema_field(
        &mut self,
        request: UpsertSchemaFieldRequest,
    ) -> Result<ProjectFieldSpec, StoreError> {
        let field = ProjectFieldSpec {
            name: request.name,
            node_classes: request.node_classes,
            presence: request.presence,
            severity: request.severity,
            role: request.role,
            inference_policy: request.inference_policy,
            value_type: request.value_type,
        };
        if let Some(existing) = self.schema.fields.iter_mut().find(|existing| {
            existing.name == field.name && existing.node_classes == field.node_classes
        }) {
            if *existing == field {
                return Ok(field);
            }
            *existing = field.clone();
        } else {
            self.schema.fields.push(field.clone());
        }
        sort_schema_fields(&mut self.schema.fields);
        self.bump_schema_version();
        self.save_schema()?;
        Ok(field)
    }

    pub fn remove_schema_field(
        &mut self,
        request: RemoveSchemaFieldRequest,
    ) -> Result<u64, StoreError> {
        let before = self.schema.fields.len();
        self.schema.fields.retain(|field| {
            field.name != request.name
                || request
                    .node_classes
                    .as_ref()
                    .is_some_and(|node_classes| field.node_classes != *node_classes)
        });
        let removed = before.saturating_sub(self.schema.fields.len()) as u64;
        if removed == 0 {
            return Err(StoreError::SchemaFieldNotFound(
                request.name.as_str().to_owned(),
            ));
        }
        sort_schema_fields(&mut self.schema.fields);
        self.bump_schema_version();
        self.save_schema()?;
        Ok(removed)
    }

    #[must_use]
    pub fn project_root(&self) -> &Utf8Path {
        &self.project_root
    }

    #[must_use]
    pub fn state_root(&self) -> &Utf8Path {
        &self.state_root
    }

    fn bump_schema_version(&mut self) {
        self.schema.version = self.schema.version.saturating_add(1);
    }

    fn save_schema(&self) -> Result<(), StoreError> {
        write_json_file(&self.state_root.join(PROJECT_SCHEMA_NAME), &self.schema)
    }

    pub fn create_frontier(
        &mut self,
        request: CreateFrontierRequest,
    ) -> Result<FrontierProjection, StoreError> {
        let frontier_id = fidget_spinner_core::FrontierId::fresh();
        let payload = NodePayload::with_schema(
            self.schema.schema_ref(),
            frontier_contract_payload(&request.contract)?,
        );
        let diagnostics = self.schema.validate_node(NodeClass::Contract, &payload);
        let contract_node = DagNode::new(
            NodeClass::Contract,
            Some(frontier_id),
            request.contract_title,
            request.contract_summary,
            payload,
            diagnostics,
        );
        let frontier = FrontierRecord::with_id(frontier_id, request.label, contract_node.id);

        let tx = self.connection.transaction()?;
        let _ = upsert_metric_definition_tx(
            &tx,
            &MetricDefinition::new(
                request
                    .contract
                    .evaluation
                    .primary_metric
                    .metric_key
                    .clone(),
                request.contract.evaluation.primary_metric.unit,
                request.contract.evaluation.primary_metric.objective,
                None,
            ),
        )?;
        for metric in &request.contract.evaluation.supporting_metrics {
            let _ = upsert_metric_definition_tx(
                &tx,
                &MetricDefinition::new(
                    metric.metric_key.clone(),
                    metric.unit,
                    metric.objective,
                    None,
                ),
            )?;
        }
        insert_node(&tx, &contract_node)?;
        insert_frontier(&tx, &frontier)?;
        if let Some(seed) = request.initial_checkpoint {
            let checkpoint = CheckpointRecord {
                id: fidget_spinner_core::CheckpointId::fresh(),
                frontier_id: frontier.id,
                node_id: contract_node.id,
                snapshot: seed.snapshot,
                disposition: CheckpointDisposition::Champion,
                summary: seed.summary,
                created_at: OffsetDateTime::now_utc(),
            };
            insert_checkpoint(&tx, &checkpoint)?;
        }
        insert_event(
            &tx,
            "frontier",
            &frontier.id.to_string(),
            "frontier.created",
            json!({"root_contract_node_id": contract_node.id}),
        )?;
        tx.commit()?;

        self.frontier_projection(frontier.id)
    }

    pub fn define_metric(
        &mut self,
        request: DefineMetricRequest,
    ) -> Result<MetricDefinition, StoreError> {
        let record = MetricDefinition::new(
            request.key,
            request.unit,
            request.objective,
            request.description,
        );
        let tx = self.connection.transaction()?;
        let _ = upsert_metric_definition_tx(&tx, &record)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn list_metric_definitions(&self) -> Result<Vec<MetricDefinition>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT metric_key, unit, objective, description, created_at
             FROM metric_definitions
             ORDER BY metric_key ASC",
        )?;
        let mut rows = statement.query([])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(MetricDefinition {
                key: NonEmptyText::new(row.get::<_, String>(0)?)?,
                unit: decode_metric_unit(&row.get::<_, String>(1)?)?,
                objective: decode_optimization_objective(&row.get::<_, String>(2)?)?,
                description: row
                    .get::<_, Option<String>>(3)?
                    .map(NonEmptyText::new)
                    .transpose()?,
                created_at: decode_timestamp(&row.get::<_, String>(4)?)?,
            });
        }
        Ok(items)
    }

    pub fn define_run_dimension(
        &mut self,
        request: DefineRunDimensionRequest,
    ) -> Result<RunDimensionDefinition, StoreError> {
        let record =
            RunDimensionDefinition::new(request.key, request.value_type, request.description);
        let tx = self.connection.transaction()?;
        let _ = insert_run_dimension_definition_tx(&tx, &record)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn list_run_dimensions(&self) -> Result<Vec<RunDimensionSummary>, StoreError> {
        load_run_dimension_summaries(self)
    }

    pub fn coerce_run_dimensions(
        &self,
        raw_dimensions: BTreeMap<String, Value>,
    ) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, StoreError> {
        coerce_run_dimension_map(&run_dimension_definitions_by_key(self)?, raw_dimensions)
    }

    pub fn migrate_metric_plane(&mut self) -> Result<MetricPlaneMigrationReport, StoreError> {
        let tx = self.connection.transaction()?;
        let report = normalize_metric_plane_tx(&tx)?;
        tx.commit()?;
        Ok(report)
    }

    pub fn add_tag(
        &mut self,
        name: TagName,
        description: NonEmptyText,
    ) -> Result<TagRecord, StoreError> {
        let record = TagRecord {
            name,
            description,
            created_at: OffsetDateTime::now_utc(),
        };
        let tx = self.connection.transaction()?;
        insert_tag(&tx, &record)?;
        insert_event(
            &tx,
            "tag",
            record.name.as_str(),
            "tag.created",
            json!({"description": record.description.as_str()}),
        )?;
        tx.commit()?;
        Ok(record)
    }

    pub fn list_tags(&self) -> Result<Vec<TagRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT name, description, created_at
             FROM tags
             ORDER BY name ASC",
        )?;
        let mut rows = statement.query([])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(TagRecord {
                name: TagName::new(row.get::<_, String>(0)?)?,
                description: NonEmptyText::new(row.get::<_, String>(1)?)?,
                created_at: decode_timestamp(&row.get::<_, String>(2)?)?,
            });
        }
        Ok(items)
    }

    pub fn add_node(&mut self, request: CreateNodeRequest) -> Result<DagNode, StoreError> {
        validate_prose_node_request(&request)?;
        let diagnostics = self.schema.validate_node(request.class, &request.payload);
        let mut node = DagNode::new(
            request.class,
            request.frontier_id,
            request.title,
            request.summary,
            request.payload,
            diagnostics,
        );
        node.tags = match (request.class, request.tags) {
            (NodeClass::Note, Some(tags)) => tags,
            (NodeClass::Note, None) => return Err(StoreError::NoteTagsRequired),
            (_, Some(tags)) => tags,
            (_, None) => BTreeSet::new(),
        };
        node.annotations = request.annotations;

        let tx = self.connection.transaction()?;
        ensure_known_tags(&tx, &node.tags)?;
        insert_node(&tx, &node)?;
        for attachment in &request.attachments {
            insert_edge(&tx, &attachment.materialize(node.id))?;
        }
        insert_event(
            &tx,
            "node",
            &node.id.to_string(),
            "node.created",
            json!({"class": node.class.as_str(), "frontier_id": node.frontier_id}),
        )?;
        tx.commit()?;
        Ok(node)
    }

    pub fn list_metric_keys(&self) -> Result<Vec<MetricKeySummary>, StoreError> {
        self.list_metric_keys_filtered(MetricKeyQuery::default())
    }

    pub fn list_metric_keys_filtered(
        &self,
        query: MetricKeyQuery,
    ) -> Result<Vec<MetricKeySummary>, StoreError> {
        let mut summaries = collect_metric_samples(self, &query)?
            .into_iter()
            .fold(
                BTreeMap::<(MetricFieldSource, String), MetricKeyAccumulator>::new(),
                |mut accumulators, sample| {
                    let key = (sample.source, sample.key.as_str().to_owned());
                    let _ = accumulators
                        .entry(key)
                        .and_modify(|entry| entry.observe(&sample))
                        .or_insert_with(|| MetricKeyAccumulator::from_sample(&sample));
                    accumulators
                },
            )
            .into_values()
            .map(MetricKeyAccumulator::finish)
            .collect::<Vec<_>>();
        if query
            .source
            .is_none_or(|source| source == MetricFieldSource::RunMetric)
        {
            merge_registered_run_metric_summaries(self, &mut summaries)?;
        }
        summaries.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.source.cmp(&right.source))
        });
        Ok(summaries)
    }

    pub fn best_metrics(&self, query: MetricBestQuery) -> Result<Vec<MetricBestEntry>, StoreError> {
        let matching = collect_metric_samples(
            self,
            &MetricKeyQuery {
                frontier_id: query.frontier_id,
                source: query.source,
                dimensions: query.dimensions.clone(),
            },
        )?
        .into_iter()
        .filter(|sample| sample.key == query.key)
        .collect::<Vec<_>>();
        if matching.is_empty() {
            return Ok(Vec::new());
        }

        let source = if let Some(source) = query.source {
            source
        } else {
            let sources = matching
                .iter()
                .map(|sample| sample.source)
                .collect::<BTreeSet<_>>();
            if sources.len() != 1 {
                return Err(StoreError::AmbiguousMetricKey {
                    key: query.key.as_str().to_owned(),
                    sources: sources
                        .into_iter()
                        .map(MetricFieldSource::as_str)
                        .collect::<Vec<_>>()
                        .join(", "),
                });
            }
            let Some(source) = sources.iter().copied().next() else {
                return Ok(Vec::new());
            };
            source
        };

        let mut matching = matching
            .into_iter()
            .filter(|sample| sample.source == source)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Ok(Vec::new());
        }

        let order = resolve_metric_order(&matching, &query, source)?;
        matching.sort_by(|left, right| compare_metric_samples(left, right, order));
        matching.truncate(query.limit as usize);
        Ok(matching
            .into_iter()
            .map(|sample| sample.into_entry(order))
            .collect())
    }

    pub fn archive_node(&mut self, node_id: fidget_spinner_core::NodeId) -> Result<(), StoreError> {
        let updated_at = encode_timestamp(OffsetDateTime::now_utc())?;
        let changed = self.connection.execute(
            "UPDATE nodes SET archived = 1, updated_at = ?1 WHERE id = ?2",
            params![updated_at, node_id.to_string()],
        )?;
        if changed == 0 {
            return Err(StoreError::NodeNotFound(node_id));
        }
        Ok(())
    }

    pub fn annotate_node(
        &mut self,
        node_id: fidget_spinner_core::NodeId,
        annotation: NodeAnnotation,
    ) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        let exists = tx
            .query_row(
                "SELECT 1 FROM nodes WHERE id = ?1",
                params![node_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::NodeNotFound(node_id));
        }
        insert_annotation(&tx, node_id, &annotation)?;
        let _ = tx.execute(
            "UPDATE nodes SET updated_at = ?1 WHERE id = ?2",
            params![
                encode_timestamp(OffsetDateTime::now_utc())?,
                node_id.to_string()
            ],
        )?;
        insert_event(
            &tx,
            "node",
            &node_id.to_string(),
            "node.annotated",
            json!({"visibility": format!("{:?}", annotation.visibility)}),
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_node(
        &self,
        node_id: fidget_spinner_core::NodeId,
    ) -> Result<Option<DagNode>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT
                id,
                class,
                track,
                frontier_id,
                archived,
                title,
                summary,
                payload_schema_namespace,
                payload_schema_version,
                payload_json,
                diagnostics_json,
                agent_session_id,
                created_at,
                updated_at
             FROM nodes
             WHERE id = ?1",
        )?;
        let node = statement
            .query_row(params![node_id.to_string()], read_node_row)
            .optional()?;
        node.map(|mut item| {
            item.tags = self.load_tags(item.id)?;
            item.annotations = self.load_annotations(item.id)?;
            Ok(item)
        })
        .transpose()
    }

    pub fn list_nodes(&self, query: ListNodesQuery) -> Result<Vec<NodeSummary>, StoreError> {
        let frontier_id = query.frontier_id.map(|id| id.to_string());
        let class = query.class.map(|item| item.as_str().to_owned());
        let mut sql = String::from(
            "SELECT
                n.id,
                n.class,
                n.track,
                n.frontier_id,
                n.archived,
                n.title,
                n.summary,
                n.diagnostics_json,
                n.created_at,
                n.updated_at,
                (
                    SELECT COUNT(*)
                    FROM node_annotations AS a
                    WHERE a.node_id = n.id AND a.visibility = 'hidden'
                ) AS hidden_annotation_count
             FROM nodes AS n
             WHERE (?1 IS NULL OR n.frontier_id = ?1)
               AND (?2 IS NULL OR n.class = ?2)
               AND (?3 = 1 OR n.archived = 0)",
        );
        let mut parameters = vec![
            frontier_id.map_or(SqlValue::Null, SqlValue::Text),
            class.map_or(SqlValue::Null, SqlValue::Text),
            SqlValue::Integer(i64::from(query.include_archived)),
        ];
        for (index, tag) in query.tags.iter().enumerate() {
            let placeholder = parameters.len() + 1;
            let _ = write!(
                sql,
                "
               AND EXISTS (
                    SELECT 1
                    FROM node_tags AS nt{index}
                    WHERE nt{index}.node_id = n.id AND nt{index}.tag_name = ?{placeholder}
               )"
            );
            parameters.push(SqlValue::Text(tag.as_str().to_owned()));
        }
        let limit_placeholder = parameters.len() + 1;
        let _ = write!(
            sql,
            "
             ORDER BY n.updated_at DESC
             LIMIT ?{limit_placeholder}"
        );
        parameters.push(SqlValue::Integer(i64::from(query.limit)));
        let mut statement = self.connection.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(parameters.iter()))?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            let diagnostics = decode_json::<NodeDiagnostics>(&row.get::<_, String>(7)?)?;
            let node_id = parse_node_id(&row.get::<_, String>(0)?)?;
            items.push(NodeSummary {
                id: node_id,
                class: parse_node_class(&row.get::<_, String>(1)?)?,
                track: parse_node_track(&row.get::<_, String>(2)?)?,
                frontier_id: row
                    .get::<_, Option<String>>(3)?
                    .map(|raw| parse_frontier_id(&raw))
                    .transpose()?,
                archived: row.get::<_, i64>(4)? != 0,
                title: NonEmptyText::new(row.get::<_, String>(5)?)?,
                summary: row
                    .get::<_, Option<String>>(6)?
                    .map(NonEmptyText::new)
                    .transpose()?,
                tags: self.load_tags(node_id)?,
                diagnostic_count: diagnostics.items.len() as u64,
                hidden_annotation_count: row.get::<_, i64>(10)? as u64,
                created_at: decode_timestamp(&row.get::<_, String>(8)?)?,
                updated_at: decode_timestamp(&row.get::<_, String>(9)?)?,
            });
        }
        Ok(items)
    }

    pub fn list_frontiers(&self) -> Result<Vec<FrontierRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, label, root_contract_node_id, status, created_at, updated_at
             FROM frontiers
             ORDER BY updated_at DESC",
        )?;
        let mut rows = statement.query([])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(read_frontier_row(row)?);
        }
        Ok(items)
    }

    pub fn frontier_projection(
        &self,
        frontier_id: fidget_spinner_core::FrontierId,
    ) -> Result<FrontierProjection, StoreError> {
        let frontier = self.load_frontier(frontier_id)?;
        let mut champion_checkpoint_id = None;
        let mut candidate_checkpoint_ids = BTreeSet::new();

        let mut statement = self.connection.prepare(
            "SELECT id, disposition
             FROM checkpoints
             WHERE frontier_id = ?1",
        )?;
        let mut rows = statement.query(params![frontier_id.to_string()])?;
        while let Some(row) = rows.next()? {
            let checkpoint_id = parse_checkpoint_id(&row.get::<_, String>(0)?)?;
            match parse_checkpoint_disposition(&row.get::<_, String>(1)?)? {
                CheckpointDisposition::Champion => champion_checkpoint_id = Some(checkpoint_id),
                CheckpointDisposition::FrontierCandidate => {
                    let _ = candidate_checkpoint_ids.insert(checkpoint_id);
                }
                CheckpointDisposition::Baseline
                | CheckpointDisposition::DeadEnd
                | CheckpointDisposition::Archived => {}
            }
        }
        let experiment_count = self.connection.query_row(
            "SELECT COUNT(*) FROM experiments WHERE frontier_id = ?1",
            params![frontier_id.to_string()],
            |row| row.get::<_, i64>(0),
        )? as u64;

        Ok(FrontierProjection {
            frontier,
            champion_checkpoint_id,
            candidate_checkpoint_ids,
            experiment_count,
        })
    }

    pub fn load_checkpoint(
        &self,
        checkpoint_id: fidget_spinner_core::CheckpointId,
    ) -> Result<Option<CheckpointRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT
                id,
                frontier_id,
                node_id,
                repo_root,
                worktree_root,
                worktree_name,
                commit_hash,
                disposition,
                summary,
                created_at
             FROM checkpoints
             WHERE id = ?1",
        )?;
        statement
            .query_row(params![checkpoint_id.to_string()], |row| {
                read_checkpoint_row(row)
            })
            .optional()
            .map_err(StoreError::from)
    }

    pub fn close_experiment(
        &mut self,
        request: CloseExperimentRequest,
    ) -> Result<ExperimentReceipt, StoreError> {
        let change_node = self
            .get_node(request.change_node_id)?
            .ok_or(StoreError::NodeNotFound(request.change_node_id))?;
        if change_node.class != NodeClass::Change {
            return Err(StoreError::NodeNotChange(request.change_node_id));
        }
        if change_node.frontier_id != Some(request.frontier_id) {
            return Err(StoreError::FrontierNotFound(request.frontier_id));
        }
        let base_checkpoint = self
            .load_checkpoint(request.base_checkpoint_id)?
            .ok_or(StoreError::CheckpointNotFound(request.base_checkpoint_id))?;
        if base_checkpoint.frontier_id != request.frontier_id {
            return Err(StoreError::CheckpointNotFound(request.base_checkpoint_id));
        }
        let tx = self.connection.transaction()?;
        let dimensions = validate_run_dimensions_tx(&tx, &request.dimensions)?;
        let primary_metric_definition =
            load_metric_definition_tx(&tx, &request.primary_metric.key)?.ok_or_else(|| {
                StoreError::UnknownMetricDefinition(request.primary_metric.key.clone())
            })?;
        let supporting_metric_definitions = request
            .supporting_metrics
            .iter()
            .map(|metric| {
                load_metric_definition_tx(&tx, &metric.key)?
                    .ok_or_else(|| StoreError::UnknownMetricDefinition(metric.key.clone()))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let benchmark_suite = benchmark_suite_label(&dimensions);

        let run_payload = NodePayload::with_schema(
            self.schema.schema_ref(),
            json_object(json!({
                "dimensions": run_dimensions_json(&dimensions),
                "backend": format!("{:?}", request.backend),
                "command": request.command.argv.iter().map(NonEmptyText::as_str).collect::<Vec<_>>(),
            }))?,
        );
        let run_diagnostics = self.schema.validate_node(NodeClass::Run, &run_payload);
        let run_node = DagNode::new(
            NodeClass::Run,
            Some(request.frontier_id),
            request.run_title,
            request.run_summary,
            run_payload,
            run_diagnostics,
        );
        let run_id = fidget_spinner_core::RunId::fresh();
        let now = OffsetDateTime::now_utc();
        let run = RunRecord {
            node_id: run_node.id,
            run_id,
            frontier_id: Some(request.frontier_id),
            status: RunStatus::Succeeded,
            backend: request.backend,
            code_snapshot: request.code_snapshot,
            dimensions: dimensions.clone(),
            command: request.command,
            started_at: Some(now),
            finished_at: Some(now),
        };

        let decision_payload = NodePayload::with_schema(
            self.schema.schema_ref(),
            json_object(json!({
                "verdict": format!("{:?}", request.verdict),
                "rationale": request.decision_rationale.as_str(),
            }))?,
        );
        let decision_diagnostics = self
            .schema
            .validate_node(NodeClass::Decision, &decision_payload);
        let decision_node = DagNode::new(
            NodeClass::Decision,
            Some(request.frontier_id),
            request.decision_title,
            Some(request.decision_rationale.clone()),
            decision_payload,
            decision_diagnostics,
        );

        let checkpoint = CheckpointRecord {
            id: fidget_spinner_core::CheckpointId::fresh(),
            frontier_id: request.frontier_id,
            node_id: run_node.id,
            snapshot: request.candidate_snapshot,
            disposition: match request.verdict {
                FrontierVerdict::PromoteToChampion => CheckpointDisposition::Champion,
                FrontierVerdict::KeepOnFrontier | FrontierVerdict::NeedsMoreEvidence => {
                    CheckpointDisposition::FrontierCandidate
                }
                FrontierVerdict::RevertToChampion => CheckpointDisposition::DeadEnd,
                FrontierVerdict::ArchiveDeadEnd => CheckpointDisposition::Archived,
            },
            summary: request.candidate_summary,
            created_at: now,
        };

        let experiment = CompletedExperiment {
            id: fidget_spinner_core::ExperimentId::fresh(),
            frontier_id: request.frontier_id,
            base_checkpoint_id: request.base_checkpoint_id,
            candidate_checkpoint_id: checkpoint.id,
            change_node_id: request.change_node_id,
            run_node_id: run_node.id,
            run_id,
            analysis_node_id: request.analysis_node_id,
            decision_node_id: decision_node.id,
            result: ExperimentResult {
                dimensions: dimensions.clone(),
                primary_metric: request.primary_metric,
                supporting_metrics: request.supporting_metrics,
                benchmark_bundle: None,
            },
            note: request.note,
            verdict: request.verdict,
            created_at: now,
        };
        insert_node(&tx, &run_node)?;
        insert_node(&tx, &decision_node)?;
        insert_edge(
            &tx,
            &DagEdge {
                source_id: request.change_node_id,
                target_id: run_node.id,
                kind: EdgeKind::Lineage,
            },
        )?;
        insert_edge(
            &tx,
            &DagEdge {
                source_id: run_node.id,
                target_id: decision_node.id,
                kind: EdgeKind::Evidence,
            },
        )?;
        insert_run(
            &tx,
            &run,
            benchmark_suite.as_deref(),
            &experiment.result.primary_metric,
            &primary_metric_definition,
            &experiment.result.supporting_metrics,
            supporting_metric_definitions.as_slice(),
        )?;
        insert_run_dimensions(&tx, run.run_id, &dimensions)?;
        match request.verdict {
            FrontierVerdict::PromoteToChampion => {
                demote_previous_champion(&tx, request.frontier_id)?;
            }
            FrontierVerdict::KeepOnFrontier
            | FrontierVerdict::NeedsMoreEvidence
            | FrontierVerdict::RevertToChampion
            | FrontierVerdict::ArchiveDeadEnd => {}
        }
        insert_checkpoint(&tx, &checkpoint)?;
        insert_experiment(&tx, &experiment)?;
        touch_frontier(&tx, request.frontier_id)?;
        insert_event(
            &tx,
            "experiment",
            &experiment.id.to_string(),
            "experiment.closed",
            json!({
                "frontier_id": request.frontier_id,
                "verdict": format!("{:?}", request.verdict),
                "candidate_checkpoint_id": checkpoint.id,
            }),
        )?;
        tx.commit()?;

        Ok(ExperimentReceipt {
            checkpoint,
            run_node,
            run,
            decision_node,
            experiment,
        })
    }

    pub fn auto_capture_checkpoint(
        &self,
        summary: NonEmptyText,
    ) -> Result<Option<CheckpointSeed>, StoreError> {
        auto_capture_checkpoint_seed(&self.project_root, summary)
    }

    fn load_annotations(
        &self,
        node_id: fidget_spinner_core::NodeId,
    ) -> Result<Vec<NodeAnnotation>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, visibility, label, body, created_at
             FROM node_annotations
             WHERE node_id = ?1
             ORDER BY created_at ASC",
        )?;
        let mut rows = statement.query(params![node_id.to_string()])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(NodeAnnotation {
                id: parse_annotation_id(&row.get::<_, String>(0)?)?,
                visibility: parse_annotation_visibility(&row.get::<_, String>(1)?)?,
                label: row
                    .get::<_, Option<String>>(2)?
                    .map(NonEmptyText::new)
                    .transpose()?,
                body: NonEmptyText::new(row.get::<_, String>(3)?)?,
                created_at: decode_timestamp(&row.get::<_, String>(4)?)?,
            });
        }
        Ok(items)
    }

    fn load_tags(
        &self,
        node_id: fidget_spinner_core::NodeId,
    ) -> Result<BTreeSet<TagName>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT tag_name
             FROM node_tags
             WHERE node_id = ?1
             ORDER BY tag_name ASC",
        )?;
        let mut rows = statement.query(params![node_id.to_string()])?;
        let mut items = BTreeSet::new();
        while let Some(row) = rows.next()? {
            let _ = items.insert(TagName::new(row.get::<_, String>(0)?)?);
        }
        Ok(items)
    }

    fn load_frontier(
        &self,
        frontier_id: fidget_spinner_core::FrontierId,
    ) -> Result<FrontierRecord, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, label, root_contract_node_id, status, created_at, updated_at
             FROM frontiers
             WHERE id = ?1",
        )?;
        let frontier = statement
            .query_row(params![frontier_id.to_string()], |row| {
                read_frontier_row(row).map_err(to_sql_conversion_error)
            })
            .optional()?;
        frontier.ok_or(StoreError::FrontierNotFound(frontier_id))
    }
}

fn upgrade_store(connection: &mut Connection) -> Result<(), StoreError> {
    migrate(connection)?;
    backfill_prose_summaries(connection)?;
    let tx = connection.transaction()?;
    let _ = normalize_metric_plane_tx(&tx)?;
    tx.commit()?;
    Ok(())
}

fn validate_prose_node_request(request: &CreateNodeRequest) -> Result<(), StoreError> {
    if !matches!(request.class, NodeClass::Note | NodeClass::Research) {
        return Ok(());
    }
    if request.summary.is_none() {
        return Err(StoreError::ProseSummaryRequired(request.class));
    }
    match request.payload.field("body") {
        Some(Value::String(body)) if !body.trim().is_empty() => Ok(()),
        _ => Err(StoreError::ProseBodyRequired(request.class)),
    }
}

#[derive(Clone, Debug)]
struct MetricSample {
    key: NonEmptyText,
    source: MetricFieldSource,
    value: f64,
    frontier_id: fidget_spinner_core::FrontierId,
    experiment_id: fidget_spinner_core::ExperimentId,
    change_node_id: fidget_spinner_core::NodeId,
    change_title: NonEmptyText,
    run_id: fidget_spinner_core::RunId,
    verdict: FrontierVerdict,
    candidate_checkpoint_id: fidget_spinner_core::CheckpointId,
    candidate_commit_hash: GitCommitHash,
    unit: Option<MetricUnit>,
    objective: Option<OptimizationObjective>,
    dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

impl MetricSample {
    fn into_entry(self, order: MetricRankOrder) -> MetricBestEntry {
        MetricBestEntry {
            key: self.key,
            source: self.source,
            value: self.value,
            order,
            experiment_id: self.experiment_id,
            frontier_id: self.frontier_id,
            change_node_id: self.change_node_id,
            change_title: self.change_title,
            run_id: self.run_id,
            verdict: self.verdict,
            candidate_checkpoint_id: self.candidate_checkpoint_id,
            candidate_commit_hash: self.candidate_commit_hash,
            unit: self.unit,
            objective: self.objective,
            dimensions: self.dimensions,
        }
    }
}

#[derive(Clone, Debug)]
struct MetricKeyAccumulator {
    key: NonEmptyText,
    source: MetricFieldSource,
    experiment_ids: BTreeSet<fidget_spinner_core::ExperimentId>,
    unit: Option<MetricUnit>,
    objective: Option<OptimizationObjective>,
    ambiguous_semantics: bool,
}

impl MetricKeyAccumulator {
    fn from_sample(sample: &MetricSample) -> Self {
        Self {
            key: sample.key.clone(),
            source: sample.source,
            experiment_ids: BTreeSet::from([sample.experiment_id]),
            unit: sample.unit,
            objective: sample.objective,
            ambiguous_semantics: false,
        }
    }

    fn observe(&mut self, sample: &MetricSample) {
        let _ = self.experiment_ids.insert(sample.experiment_id);
        if self.unit != sample.unit || self.objective != sample.objective {
            self.ambiguous_semantics = true;
            self.unit = None;
            self.objective = None;
        }
    }

    fn finish(self) -> MetricKeySummary {
        MetricKeySummary {
            key: self.key,
            source: self.source,
            experiment_count: self.experiment_ids.len() as u64,
            unit: self.unit,
            objective: self.objective,
            description: None,
            requires_order: self.source != MetricFieldSource::RunMetric
                || self.ambiguous_semantics
                || !matches!(
                    self.objective,
                    Some(OptimizationObjective::Minimize | OptimizationObjective::Maximize)
                ),
        }
    }
}

fn collect_metric_samples(
    store: &ProjectStore,
    query: &MetricKeyQuery,
) -> Result<Vec<MetricSample>, StoreError> {
    let rows = load_experiment_rows(store)?;
    let metric_definitions = metric_definitions_by_key(store)?;
    let mut samples = Vec::new();
    for row in rows {
        if query
            .frontier_id
            .is_some_and(|frontier_id| row.frontier_id != frontier_id)
        {
            continue;
        }
        if !dimensions_match(&row.dimensions, &query.dimensions) {
            continue;
        }
        samples.extend(metric_samples_for_row(
            store.schema(),
            &row,
            &metric_definitions,
        ));
    }
    Ok(if let Some(source) = query.source {
        samples
            .into_iter()
            .filter(|sample| sample.source == source)
            .collect()
    } else {
        samples
    })
}

fn resolve_metric_order(
    matching: &[MetricSample],
    query: &MetricBestQuery,
    source: MetricFieldSource,
) -> Result<MetricRankOrder, StoreError> {
    if let Some(order) = query.order {
        return Ok(order);
    }
    if source != MetricFieldSource::RunMetric {
        return Err(StoreError::MetricOrderRequired {
            key: query.key.as_str().to_owned(),
            metric_source: source.as_str().to_owned(),
        });
    }
    let objectives = matching
        .iter()
        .map(|sample| sample.objective)
        .collect::<BTreeSet<_>>();
    match objectives.len() {
        1 => match objectives.into_iter().next().flatten() {
            Some(OptimizationObjective::Minimize) => Ok(MetricRankOrder::Asc),
            Some(OptimizationObjective::Maximize) => Ok(MetricRankOrder::Desc),
            Some(OptimizationObjective::Target) | None => Err(StoreError::MetricOrderRequired {
                key: query.key.as_str().to_owned(),
                metric_source: source.as_str().to_owned(),
            }),
        },
        _ => Err(StoreError::MetricSemanticsAmbiguous {
            key: query.key.as_str().to_owned(),
            metric_source: source.as_str().to_owned(),
        }),
    }
}

fn compare_metric_samples(
    left: &MetricSample,
    right: &MetricSample,
    order: MetricRankOrder,
) -> Ordering {
    let metric_order = match order {
        MetricRankOrder::Asc => left
            .value
            .partial_cmp(&right.value)
            .unwrap_or(Ordering::Equal),
        MetricRankOrder::Desc => right
            .value
            .partial_cmp(&left.value)
            .unwrap_or(Ordering::Equal),
    };
    metric_order
        .then_with(|| right.experiment_id.cmp(&left.experiment_id))
        .then_with(|| left.key.cmp(&right.key))
}

#[derive(Clone, Debug)]
struct ExperimentMetricRow {
    experiment_id: fidget_spinner_core::ExperimentId,
    frontier_id: fidget_spinner_core::FrontierId,
    run_id: fidget_spinner_core::RunId,
    verdict: FrontierVerdict,
    candidate_checkpoint: CheckpointRecord,
    change_node: DagNode,
    run_node: DagNode,
    analysis_node: Option<DagNode>,
    decision_node: DagNode,
    primary_metric: MetricValue,
    supporting_metrics: Vec<MetricValue>,
    dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
}

fn load_experiment_rows(store: &ProjectStore) -> Result<Vec<ExperimentMetricRow>, StoreError> {
    let run_dimensions = load_run_dimensions_by_run_id(store)?;
    let mut statement = store.connection.prepare(
        "SELECT
            id,
            frontier_id,
            run_id,
            change_node_id,
            run_node_id,
            analysis_node_id,
            decision_node_id,
            candidate_checkpoint_id,
            primary_metric_json,
            supporting_metrics_json,
            verdict
         FROM experiments",
    )?;
    let mut rows = statement.query([])?;
    let mut items = Vec::new();
    while let Some(row) = rows.next()? {
        let change_node_id = parse_node_id(&row.get::<_, String>(3)?)?;
        let run_id = parse_run_id(&row.get::<_, String>(2)?)?;
        let run_node_id = parse_node_id(&row.get::<_, String>(4)?)?;
        let analysis_node_id = row
            .get::<_, Option<String>>(5)?
            .map(|raw| parse_node_id(&raw))
            .transpose()?;
        let decision_node_id = parse_node_id(&row.get::<_, String>(6)?)?;
        let candidate_checkpoint_id = parse_checkpoint_id(&row.get::<_, String>(7)?)?;
        items.push(ExperimentMetricRow {
            experiment_id: parse_experiment_id(&row.get::<_, String>(0)?)?,
            frontier_id: parse_frontier_id(&row.get::<_, String>(1)?)?,
            run_id,
            verdict: parse_frontier_verdict(&row.get::<_, String>(10)?)?,
            candidate_checkpoint: store
                .load_checkpoint(candidate_checkpoint_id)?
                .ok_or(StoreError::CheckpointNotFound(candidate_checkpoint_id))?,
            change_node: store
                .get_node(change_node_id)?
                .ok_or(StoreError::NodeNotFound(change_node_id))?,
            run_node: store
                .get_node(run_node_id)?
                .ok_or(StoreError::NodeNotFound(run_node_id))?,
            analysis_node: analysis_node_id
                .map(|node_id| {
                    store
                        .get_node(node_id)?
                        .ok_or(StoreError::NodeNotFound(node_id))
                })
                .transpose()?,
            decision_node: store
                .get_node(decision_node_id)?
                .ok_or(StoreError::NodeNotFound(decision_node_id))?,
            primary_metric: decode_json(&row.get::<_, String>(8)?)?,
            supporting_metrics: decode_json(&row.get::<_, String>(9)?)?,
            dimensions: run_dimensions.get(&run_id).cloned().unwrap_or_default(),
        });
    }
    Ok(items)
}

fn metric_samples_for_row(
    schema: &ProjectSchema,
    row: &ExperimentMetricRow,
    metric_definitions: &BTreeMap<String, MetricDefinition>,
) -> Vec<MetricSample> {
    let mut samples = vec![metric_sample_from_observation(
        row,
        &row.primary_metric,
        metric_definitions,
        MetricFieldSource::RunMetric,
    )];
    samples.extend(row.supporting_metrics.iter().map(|metric| {
        metric_sample_from_observation(
            row,
            metric,
            metric_definitions,
            MetricFieldSource::RunMetric,
        )
    }));
    samples.extend(metric_samples_from_payload(schema, row, &row.change_node));
    samples.extend(metric_samples_from_payload(schema, row, &row.run_node));
    if let Some(node) = row.analysis_node.as_ref() {
        samples.extend(metric_samples_from_payload(schema, row, node));
    }
    samples.extend(metric_samples_from_payload(schema, row, &row.decision_node));
    samples
}

fn metric_sample_from_observation(
    row: &ExperimentMetricRow,
    metric: &MetricValue,
    metric_definitions: &BTreeMap<String, MetricDefinition>,
    source: MetricFieldSource,
) -> MetricSample {
    let registry = metric_definitions.get(metric.key.as_str());
    MetricSample {
        key: metric.key.clone(),
        source,
        value: metric.value,
        frontier_id: row.frontier_id,
        experiment_id: row.experiment_id,
        change_node_id: row.change_node.id,
        change_title: row.change_node.title.clone(),
        run_id: row.run_id,
        verdict: row.verdict,
        candidate_checkpoint_id: row.candidate_checkpoint.id,
        candidate_commit_hash: row.candidate_checkpoint.snapshot.commit_hash.clone(),
        unit: registry.map(|definition| definition.unit),
        objective: registry.map(|definition| definition.objective),
        dimensions: row.dimensions.clone(),
    }
}

fn metric_samples_from_payload(
    schema: &ProjectSchema,
    row: &ExperimentMetricRow,
    node: &DagNode,
) -> Vec<MetricSample> {
    let Some(source) = MetricFieldSource::from_payload_class(node.class) else {
        return Vec::new();
    };
    node.payload
        .fields
        .iter()
        .filter_map(|(key, value)| {
            let value = value.as_f64()?;
            let spec = schema.field_spec(node.class, key);
            if spec.is_some_and(|field| {
                field
                    .value_type
                    .is_some_and(|kind| kind != FieldValueType::Numeric)
            }) {
                return None;
            }
            Some(MetricSample {
                key: NonEmptyText::new(key.clone()).ok()?,
                source,
                value,
                frontier_id: row.frontier_id,
                experiment_id: row.experiment_id,
                change_node_id: row.change_node.id,
                change_title: row.change_node.title.clone(),
                run_id: row.run_id,
                verdict: row.verdict,
                candidate_checkpoint_id: row.candidate_checkpoint.id,
                candidate_commit_hash: row.candidate_checkpoint.snapshot.commit_hash.clone(),
                unit: None,
                objective: None,
                dimensions: row.dimensions.clone(),
            })
        })
        .collect()
}

fn migrate(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch(
        "
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            class TEXT NOT NULL,
            track TEXT NOT NULL,
            frontier_id TEXT,
            archived INTEGER NOT NULL,
            title TEXT NOT NULL,
            summary TEXT,
            payload_schema_namespace TEXT,
            payload_schema_version INTEGER,
            payload_json TEXT NOT NULL,
            diagnostics_json TEXT NOT NULL,
            agent_session_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS node_annotations (
            id TEXT PRIMARY KEY,
            node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            visibility TEXT NOT NULL,
            label TEXT,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tags (
            name TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS node_tags (
            node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            tag_name TEXT NOT NULL REFERENCES tags(name) ON DELETE RESTRICT,
            PRIMARY KEY (node_id, tag_name)
        );

        CREATE TABLE IF NOT EXISTS node_edges (
            source_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            target_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            PRIMARY KEY (source_id, target_id, kind)
        );

        CREATE TABLE IF NOT EXISTS frontiers (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            root_contract_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS checkpoints (
            id TEXT PRIMARY KEY,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
            repo_root TEXT NOT NULL,
            worktree_root TEXT NOT NULL,
            worktree_name TEXT,
            commit_hash TEXT NOT NULL,
            disposition TEXT NOT NULL,
            summary TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            frontier_id TEXT REFERENCES frontiers(id) ON DELETE SET NULL,
            status TEXT NOT NULL,
            backend TEXT NOT NULL,
            repo_root TEXT,
            worktree_root TEXT,
            worktree_name TEXT,
            head_commit TEXT,
            dirty_paths_json TEXT,
            benchmark_suite TEXT,
            working_directory TEXT NOT NULL,
            argv_json TEXT NOT NULL,
            env_json TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        );

        CREATE TABLE IF NOT EXISTS metrics (
            run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
            metric_key TEXT NOT NULL,
            unit TEXT NOT NULL,
            objective TEXT NOT NULL,
            value REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metric_definitions (
            metric_key TEXT PRIMARY KEY,
            unit TEXT NOT NULL,
            objective TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_dimension_definitions (
            dimension_key TEXT PRIMARY KEY,
            value_type TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_dimensions (
            run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
            dimension_key TEXT NOT NULL REFERENCES run_dimension_definitions(dimension_key) ON DELETE RESTRICT,
            value_type TEXT NOT NULL,
            value_text TEXT,
            value_numeric REAL,
            value_boolean INTEGER,
            value_timestamp TEXT,
            PRIMARY KEY (run_id, dimension_key)
        );

        CREATE TABLE IF NOT EXISTS experiments (
            id TEXT PRIMARY KEY,
            frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
            base_checkpoint_id TEXT NOT NULL REFERENCES checkpoints(id) ON DELETE RESTRICT,
            candidate_checkpoint_id TEXT NOT NULL REFERENCES checkpoints(id) ON DELETE RESTRICT,
            change_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
            run_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
            run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
            analysis_node_id TEXT REFERENCES nodes(id) ON DELETE RESTRICT,
            decision_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
            benchmark_suite TEXT NOT NULL,
            primary_metric_json TEXT NOT NULL,
            supporting_metrics_json TEXT NOT NULL,
            note_summary TEXT NOT NULL,
            note_next_json TEXT NOT NULL,
            verdict TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS metrics_by_key ON metrics(metric_key);
        CREATE INDEX IF NOT EXISTS run_dimensions_by_key_text ON run_dimensions(dimension_key, value_text);
        CREATE INDEX IF NOT EXISTS run_dimensions_by_key_numeric ON run_dimensions(dimension_key, value_numeric);
        CREATE INDEX IF NOT EXISTS run_dimensions_by_run ON run_dimensions(run_id, dimension_key);
        CREATE INDEX IF NOT EXISTS experiments_by_frontier ON experiments(frontier_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_kind TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            event_kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        ",
    )?;
    Ok(())
}

fn backfill_prose_summaries(connection: &Connection) -> Result<(), StoreError> {
    let mut statement = connection.prepare(
        "SELECT id, payload_json
         FROM nodes
         WHERE class IN ('note', 'research')
           AND (summary IS NULL OR trim(summary) = '')",
    )?;
    let mut rows = statement.query([])?;
    let mut updates = Vec::new();
    while let Some(row) = rows.next()? {
        let node_id = row.get::<_, String>(0)?;
        let payload = decode_json::<NodePayload>(&row.get::<_, String>(1)?)?;
        let Some(Value::String(body)) = payload.field("body") else {
            continue;
        };
        let Some(summary) = derive_summary_from_body(body) else {
            continue;
        };
        updates.push((node_id, summary));
    }
    for (node_id, summary) in updates {
        let _ = connection.execute(
            "UPDATE nodes SET summary = ?1 WHERE id = ?2",
            params![summary.as_str(), node_id],
        )?;
    }
    Ok(())
}

fn sort_schema_fields(fields: &mut [ProjectFieldSpec]) {
    fields.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.node_classes.iter().cmp(right.node_classes.iter()))
    });
}

fn normalize_metric_plane_tx(
    tx: &Transaction<'_>,
) -> Result<MetricPlaneMigrationReport, StoreError> {
    let mut report = MetricPlaneMigrationReport::default();

    if insert_run_dimension_definition_tx(
        tx,
        &RunDimensionDefinition::new(
            NonEmptyText::new("benchmark_suite")?,
            FieldValueType::String,
            Some(NonEmptyText::new("Legacy coarse benchmark label")?),
        ),
    )? {
        report.inserted_dimension_definitions += 1;
    }

    {
        let mut statement = tx.prepare(
            "SELECT DISTINCT metric_key, unit, objective
             FROM metrics
             ORDER BY metric_key ASC",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let definition = MetricDefinition::new(
                NonEmptyText::new(row.get::<_, String>(0)?)?,
                decode_metric_unit(&row.get::<_, String>(1)?)?,
                decode_optimization_objective(&row.get::<_, String>(2)?)?,
                None,
            );
            if upsert_metric_definition_tx(tx, &definition)? {
                report.inserted_metric_definitions += 1;
            }
        }
    }

    {
        let mut statement = tx.prepare(
            "SELECT payload_json
             FROM nodes
             WHERE class = 'contract'",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let payload = decode_json::<NodePayload>(&row.get::<_, String>(0)?)?;
            for definition in contract_metric_definitions(&payload)? {
                if upsert_metric_definition_tx(tx, &definition)? {
                    report.inserted_metric_definitions += 1;
                }
            }
        }
    }

    {
        let mut statement = tx.prepare(
            "SELECT run_id, benchmark_suite
             FROM runs
             WHERE benchmark_suite IS NOT NULL
               AND trim(benchmark_suite) != ''",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let run_id = parse_run_id(&row.get::<_, String>(0)?)?;
            let value = RunDimensionValue::String(NonEmptyText::new(row.get::<_, String>(1)?)?);
            if insert_run_dimension_value_tx(
                tx,
                run_id,
                &NonEmptyText::new("benchmark_suite")?,
                &value,
            )? {
                report.inserted_dimension_values += 1;
            }
        }
    }

    Ok(report)
}

fn contract_metric_definitions(payload: &NodePayload) -> Result<Vec<MetricDefinition>, StoreError> {
    let mut definitions = Vec::new();
    if let Some(primary) = payload.field("primary_metric") {
        definitions.push(metric_definition_from_json(primary, None)?);
    }
    if let Some(Value::Array(items)) = payload.field("supporting_metrics") {
        for item in items {
            definitions.push(metric_definition_from_json(item, None)?);
        }
    }
    Ok(definitions)
}

fn metric_definition_from_json(
    value: &Value,
    description: Option<NonEmptyText>,
) -> Result<MetricDefinition, StoreError> {
    let Some(object) = value.as_object() else {
        return Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric definition payload must be an object",
        ))));
    };
    let key = object
        .get("metric_key")
        .or_else(|| object.get("key"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            StoreError::Json(serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric definition missing key",
            )))
        })?;
    let unit = object.get("unit").and_then(Value::as_str).ok_or_else(|| {
        StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric definition missing unit",
        )))
    })?;
    let objective = object
        .get("objective")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            StoreError::Json(serde_json::Error::io(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric definition missing objective",
            )))
        })?;
    Ok(MetricDefinition::new(
        NonEmptyText::new(key)?,
        decode_metric_unit(unit)?,
        decode_optimization_objective(objective)?,
        description,
    ))
}

fn upsert_metric_definition_tx(
    tx: &Transaction<'_>,
    definition: &MetricDefinition,
) -> Result<bool, StoreError> {
    let existing = tx
        .query_row(
            "SELECT unit, objective, description
             FROM metric_definitions
             WHERE metric_key = ?1",
            params![definition.key.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    if let Some((existing_unit, existing_objective, existing_description)) = existing {
        let new_unit = encode_metric_unit(definition.unit).to_owned();
        let new_objective = encode_optimization_objective(definition.objective).to_owned();
        if existing_unit != new_unit || existing_objective != new_objective {
            return Err(StoreError::ConflictingMetricDefinition {
                key: definition.key.as_str().to_owned(),
                existing_unit,
                existing_objective,
                new_unit,
                new_objective,
            });
        }
        if existing_description.is_none() && definition.description.is_some() {
            let _ = tx.execute(
                "UPDATE metric_definitions SET description = ?2 WHERE metric_key = ?1",
                params![
                    definition.key.as_str(),
                    definition.description.as_ref().map(NonEmptyText::as_str)
                ],
            )?;
        }
        Ok(false)
    } else {
        let _ = tx.execute(
            "INSERT INTO metric_definitions (metric_key, unit, objective, description, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                definition.key.as_str(),
                encode_metric_unit(definition.unit),
                encode_optimization_objective(definition.objective),
                definition.description.as_ref().map(NonEmptyText::as_str),
                encode_timestamp(definition.created_at)?,
            ],
        )?;
        Ok(true)
    }
}

fn insert_run_dimension_definition_tx(
    tx: &Transaction<'_>,
    definition: &RunDimensionDefinition,
) -> Result<bool, StoreError> {
    let existing = tx
        .query_row(
            "SELECT value_type, description
             FROM run_dimension_definitions
             WHERE dimension_key = ?1",
            params![definition.key.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    if let Some((existing_type, existing_description)) = existing {
        let new_type = encode_field_value_type(definition.value_type).to_owned();
        if existing_type != new_type {
            return Err(StoreError::ConflictingRunDimensionDefinition {
                key: definition.key.as_str().to_owned(),
                existing_type,
                new_type,
            });
        }
        if existing_description.is_none() && definition.description.is_some() {
            let _ = tx.execute(
                "UPDATE run_dimension_definitions SET description = ?2 WHERE dimension_key = ?1",
                params![
                    definition.key.as_str(),
                    definition.description.as_ref().map(NonEmptyText::as_str)
                ],
            )?;
        }
        Ok(false)
    } else {
        let _ = tx.execute(
            "INSERT INTO run_dimension_definitions (dimension_key, value_type, description, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                definition.key.as_str(),
                encode_field_value_type(definition.value_type),
                definition.description.as_ref().map(NonEmptyText::as_str),
                encode_timestamp(definition.created_at)?,
            ],
        )?;
        Ok(true)
    }
}

fn load_metric_definition_tx(
    tx: &Transaction<'_>,
    key: &NonEmptyText,
) -> Result<Option<MetricDefinition>, StoreError> {
    tx.query_row(
        "SELECT metric_key, unit, objective, description, created_at
         FROM metric_definitions
         WHERE metric_key = ?1",
        params![key.as_str()],
        |row| {
            Ok(MetricDefinition {
                key: NonEmptyText::new(row.get::<_, String>(0)?)
                    .map_err(core_to_sql_conversion_error)?,
                unit: decode_metric_unit(&row.get::<_, String>(1)?)
                    .map_err(to_sql_conversion_error)?,
                objective: decode_optimization_objective(&row.get::<_, String>(2)?)
                    .map_err(to_sql_conversion_error)?,
                description: row
                    .get::<_, Option<String>>(3)?
                    .map(NonEmptyText::new)
                    .transpose()
                    .map_err(core_to_sql_conversion_error)?,
                created_at: decode_timestamp(&row.get::<_, String>(4)?)
                    .map_err(to_sql_conversion_error)?,
            })
        },
    )
    .optional()
    .map_err(StoreError::from)
}

fn metric_definitions_by_key(
    store: &ProjectStore,
) -> Result<BTreeMap<String, MetricDefinition>, StoreError> {
    Ok(store
        .list_metric_definitions()?
        .into_iter()
        .map(|definition| (definition.key.as_str().to_owned(), definition))
        .collect())
}

fn run_dimension_definitions_by_key(
    store: &ProjectStore,
) -> Result<BTreeMap<String, RunDimensionDefinition>, StoreError> {
    let mut statement = store.connection.prepare(
        "SELECT dimension_key, value_type, description, created_at
         FROM run_dimension_definitions",
    )?;
    let mut rows = statement.query([])?;
    let mut items = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let definition = RunDimensionDefinition {
            key: NonEmptyText::new(row.get::<_, String>(0)?)?,
            value_type: decode_field_value_type(&row.get::<_, String>(1)?)?,
            description: row
                .get::<_, Option<String>>(2)?
                .map(NonEmptyText::new)
                .transpose()?,
            created_at: decode_timestamp(&row.get::<_, String>(3)?)?,
        };
        let _ = items.insert(definition.key.as_str().to_owned(), definition);
    }
    Ok(items)
}

fn coerce_run_dimension_map(
    definitions: &BTreeMap<String, RunDimensionDefinition>,
    raw_dimensions: BTreeMap<String, Value>,
) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, StoreError> {
    let mut dimensions = BTreeMap::new();
    for (raw_key, raw_value) in raw_dimensions {
        let key = NonEmptyText::new(raw_key)?;
        let Some(definition) = definitions.get(key.as_str()) else {
            return Err(StoreError::UnknownRunDimension(key));
        };
        let value = coerce_run_dimension_value(definition, raw_value)?;
        let _ = dimensions.insert(key, value);
    }
    Ok(dimensions)
}

fn coerce_run_dimension_value(
    definition: &RunDimensionDefinition,
    raw_value: Value,
) -> Result<RunDimensionValue, StoreError> {
    match definition.value_type {
        FieldValueType::String => match raw_value {
            Value::String(value) => Ok(RunDimensionValue::String(NonEmptyText::new(value)?)),
            other => Err(StoreError::InvalidRunDimensionValue {
                key: definition.key.as_str().to_owned(),
                expected: definition.value_type.as_str().to_owned(),
                observed: value_kind_name(&other).to_owned(),
            }),
        },
        FieldValueType::Numeric => match raw_value.as_f64() {
            Some(value) => Ok(RunDimensionValue::Numeric(value)),
            None => Err(StoreError::InvalidRunDimensionValue {
                key: definition.key.as_str().to_owned(),
                expected: definition.value_type.as_str().to_owned(),
                observed: value_kind_name(&raw_value).to_owned(),
            }),
        },
        FieldValueType::Boolean => match raw_value {
            Value::Bool(value) => Ok(RunDimensionValue::Boolean(value)),
            other => Err(StoreError::InvalidRunDimensionValue {
                key: definition.key.as_str().to_owned(),
                expected: definition.value_type.as_str().to_owned(),
                observed: value_kind_name(&other).to_owned(),
            }),
        },
        FieldValueType::Timestamp => match raw_value {
            Value::String(value) => {
                let _ = OffsetDateTime::parse(&value, &Rfc3339)?;
                Ok(RunDimensionValue::Timestamp(NonEmptyText::new(value)?))
            }
            other => Err(StoreError::InvalidRunDimensionValue {
                key: definition.key.as_str().to_owned(),
                expected: definition.value_type.as_str().to_owned(),
                observed: value_kind_name(&other).to_owned(),
            }),
        },
    }
}

fn insert_run_dimension_value_tx(
    tx: &Transaction<'_>,
    run_id: fidget_spinner_core::RunId,
    key: &NonEmptyText,
    value: &RunDimensionValue,
) -> Result<bool, StoreError> {
    let (value_text, value_numeric, value_boolean, value_timestamp) =
        encode_run_dimension_columns(value)?;
    let changed = tx.execute(
        "INSERT OR IGNORE INTO run_dimensions (
            run_id,
            dimension_key,
            value_type,
            value_text,
            value_numeric,
            value_boolean,
            value_timestamp
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run_id.to_string(),
            key.as_str(),
            encode_field_value_type(value.value_type()),
            value_text,
            value_numeric,
            value_boolean,
            value_timestamp,
        ],
    )?;
    Ok(changed > 0)
}

fn insert_run_dimensions(
    tx: &Transaction<'_>,
    run_id: fidget_spinner_core::RunId,
    dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> Result<(), StoreError> {
    for (key, value) in dimensions {
        let _ = insert_run_dimension_value_tx(tx, run_id, key, value)?;
    }
    Ok(())
}

fn validate_run_dimensions_tx(
    tx: &Transaction<'_>,
    dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, StoreError> {
    for (key, value) in dimensions {
        let Some(expected_type) = tx
            .query_row(
                "SELECT value_type
                 FROM run_dimension_definitions
                 WHERE dimension_key = ?1",
                params![key.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            return Err(StoreError::UnknownRunDimension(key.clone()));
        };
        let expected_type = decode_field_value_type(&expected_type)?;
        let observed_type = value.value_type();
        if expected_type != observed_type {
            return Err(StoreError::InvalidRunDimensionValue {
                key: key.as_str().to_owned(),
                expected: expected_type.as_str().to_owned(),
                observed: observed_type.as_str().to_owned(),
            });
        }
        if matches!(value, RunDimensionValue::Timestamp(raw) if OffsetDateTime::parse(raw.as_str(), &Rfc3339).is_err())
        {
            return Err(StoreError::InvalidRunDimensionValue {
                key: key.as_str().to_owned(),
                expected: FieldValueType::Timestamp.as_str().to_owned(),
                observed: "string".to_owned(),
            });
        }
    }
    Ok(dimensions.clone())
}

fn load_run_dimensions_by_run_id(
    store: &ProjectStore,
) -> Result<
    BTreeMap<fidget_spinner_core::RunId, BTreeMap<NonEmptyText, RunDimensionValue>>,
    StoreError,
> {
    let mut statement = store.connection.prepare(
        "SELECT run_id, dimension_key, value_type, value_text, value_numeric, value_boolean, value_timestamp
         FROM run_dimensions
         ORDER BY dimension_key ASC",
    )?;
    let mut rows = statement.query([])?;
    let mut values =
        BTreeMap::<fidget_spinner_core::RunId, BTreeMap<NonEmptyText, RunDimensionValue>>::new();
    while let Some(row) = rows.next()? {
        let run_id = parse_run_id(&row.get::<_, String>(0)?)?;
        let key = NonEmptyText::new(row.get::<_, String>(1)?)?;
        let value_type = decode_field_value_type(&row.get::<_, String>(2)?)?;
        let value = decode_run_dimension_value(
            value_type,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<f64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, Option<String>>(6)?,
        )?;
        let _ = values.entry(run_id).or_default().insert(key, value);
    }
    Ok(values)
}

fn load_run_dimension_summaries(
    store: &ProjectStore,
) -> Result<Vec<RunDimensionSummary>, StoreError> {
    let definitions = {
        let mut statement = store.connection.prepare(
            "SELECT dimension_key, value_type, description, created_at
             FROM run_dimension_definitions
             ORDER BY dimension_key ASC",
        )?;
        let mut rows = statement.query([])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(RunDimensionDefinition {
                key: NonEmptyText::new(row.get::<_, String>(0)?)?,
                value_type: decode_field_value_type(&row.get::<_, String>(1)?)?,
                description: row
                    .get::<_, Option<String>>(2)?
                    .map(NonEmptyText::new)
                    .transpose()?,
                created_at: decode_timestamp(&row.get::<_, String>(3)?)?,
            });
        }
        items
    };

    let mut summaries = Vec::new();
    for definition in definitions {
        let mut statement = store.connection.prepare(
            "SELECT value_text, value_numeric, value_boolean, value_timestamp
             FROM run_dimensions
             WHERE dimension_key = ?1",
        )?;
        let mut rows = statement.query(params![definition.key.as_str()])?;
        let mut observed_run_count = 0_u64;
        let mut distinct = BTreeSet::new();
        let mut sample_values = Vec::new();
        while let Some(row) = rows.next()? {
            observed_run_count += 1;
            let value = decode_run_dimension_value(
                definition.value_type,
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            )?;
            let serialized = encode_json(&value.as_json())?;
            if distinct.insert(serialized) && sample_values.len() < 5 {
                sample_values.push(value.as_json());
            }
        }
        summaries.push(RunDimensionSummary {
            key: definition.key,
            value_type: definition.value_type,
            description: definition.description,
            observed_run_count,
            distinct_value_count: distinct.len() as u64,
            sample_values,
        });
    }
    Ok(summaries)
}

fn merge_registered_run_metric_summaries(
    store: &ProjectStore,
    summaries: &mut Vec<MetricKeySummary>,
) -> Result<(), StoreError> {
    let definitions = store.list_metric_definitions()?;
    for definition in definitions {
        if let Some(summary) = summaries.iter_mut().find(|summary| {
            summary.source == MetricFieldSource::RunMetric && summary.key == definition.key
        }) {
            summary.unit = Some(definition.unit);
            summary.objective = Some(definition.objective);
            summary.description.clone_from(&definition.description);
            summary.requires_order = matches!(definition.objective, OptimizationObjective::Target);
            continue;
        }
        summaries.push(MetricKeySummary {
            key: definition.key,
            source: MetricFieldSource::RunMetric,
            experiment_count: 0,
            unit: Some(definition.unit),
            objective: Some(definition.objective),
            description: definition.description,
            requires_order: matches!(definition.objective, OptimizationObjective::Target),
        });
    }
    Ok(())
}

fn dimensions_match(
    haystack: &BTreeMap<NonEmptyText, RunDimensionValue>,
    needle: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> bool {
    needle
        .iter()
        .all(|(key, value)| haystack.get(key) == Some(value))
}

fn run_dimensions_json(dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>) -> Value {
    Value::Object(
        dimensions
            .iter()
            .map(|(key, value)| (key.to_string(), value.as_json()))
            .collect::<serde_json::Map<String, Value>>(),
    )
}

fn benchmark_suite_label(dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>) -> Option<String> {
    dimensions
        .get(&NonEmptyText::new("benchmark_suite").ok()?)
        .and_then(|value| match value {
            RunDimensionValue::String(item) => Some(item.to_string()),
            _ => None,
        })
        .or_else(|| {
            if dimensions.is_empty() {
                None
            } else {
                Some(
                    dimensions
                        .iter()
                        .map(|(key, value)| format!("{key}={}", dimension_value_text(value)))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            }
        })
}

fn derive_summary_from_body(body: &str) -> Option<NonEmptyText> {
    const MAX_SUMMARY_CHARS: usize = 240;

    let paragraph = body
        .split("\n\n")
        .map(collapse_inline_whitespace)
        .map(|text| text.trim().to_owned())
        .find(|text| !text.is_empty())?;
    let summary = truncate_chars(&paragraph, MAX_SUMMARY_CHARS);
    NonEmptyText::new(summary).ok()
}

fn collapse_inline_whitespace(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut truncated = value.chars().take(max_chars).collect::<String>();
    if let Some(index) = truncated.rfind(char::is_whitespace) {
        truncated.truncate(index);
    }
    format!("{}…", truncated.trim_end())
}

fn insert_node(tx: &Transaction<'_>, node: &DagNode) -> Result<(), StoreError> {
    let schema_namespace = node
        .payload
        .schema
        .as_ref()
        .map(|schema| schema.namespace.as_str());
    let schema_version = node
        .payload
        .schema
        .as_ref()
        .map(|schema| i64::from(schema.version));
    let _ = tx.execute(
        "INSERT INTO nodes (
            id,
            class,
            track,
            frontier_id,
            archived,
            title,
            summary,
            payload_schema_namespace,
            payload_schema_version,
            payload_json,
            diagnostics_json,
            agent_session_id,
            created_at,
            updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            node.id.to_string(),
            node.class.as_str(),
            encode_node_track(node.track),
            node.frontier_id.map(|id| id.to_string()),
            i64::from(node.archived),
            node.title.as_str(),
            node.summary.as_ref().map(NonEmptyText::as_str),
            schema_namespace,
            schema_version,
            encode_json(&node.payload)?,
            encode_json(&node.diagnostics)?,
            node.agent_session_id.map(|id| id.to_string()),
            encode_timestamp(node.created_at)?,
            encode_timestamp(node.updated_at)?,
        ],
    )?;
    for annotation in &node.annotations {
        insert_annotation(tx, node.id, annotation)?;
    }
    for tag in &node.tags {
        insert_node_tag(tx, node.id, tag)?;
    }
    Ok(())
}

fn insert_tag(tx: &Transaction<'_>, tag: &TagRecord) -> Result<(), StoreError> {
    let existing = tx
        .query_row(
            "SELECT 1 FROM tags WHERE name = ?1",
            params![tag.name.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(StoreError::DuplicateTag(tag.name.clone()));
    }
    let _ = tx.execute(
        "INSERT INTO tags (name, description, created_at)
         VALUES (?1, ?2, ?3)",
        params![
            tag.name.as_str(),
            tag.description.as_str(),
            encode_timestamp(tag.created_at)?,
        ],
    )?;
    Ok(())
}

fn insert_annotation(
    tx: &Transaction<'_>,
    node_id: fidget_spinner_core::NodeId,
    annotation: &NodeAnnotation,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT INTO node_annotations (id, node_id, visibility, label, body, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            annotation.id.to_string(),
            node_id.to_string(),
            encode_annotation_visibility(annotation.visibility),
            annotation.label.as_ref().map(NonEmptyText::as_str),
            annotation.body.as_str(),
            encode_timestamp(annotation.created_at)?,
        ],
    )?;
    Ok(())
}

fn insert_node_tag(
    tx: &Transaction<'_>,
    node_id: fidget_spinner_core::NodeId,
    tag: &TagName,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT INTO node_tags (node_id, tag_name)
         VALUES (?1, ?2)",
        params![node_id.to_string(), tag.as_str()],
    )?;
    Ok(())
}

fn ensure_known_tags(tx: &Transaction<'_>, tags: &BTreeSet<TagName>) -> Result<(), StoreError> {
    let mut statement = tx.prepare("SELECT 1 FROM tags WHERE name = ?1")?;
    for tag in tags {
        let exists = statement
            .query_row(params![tag.as_str()], |row| row.get::<_, i64>(0))
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::UnknownTag(tag.clone()));
        }
    }
    Ok(())
}

fn insert_edge(tx: &Transaction<'_>, edge: &DagEdge) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT OR IGNORE INTO node_edges (source_id, target_id, kind)
         VALUES (?1, ?2, ?3)",
        params![
            edge.source_id.to_string(),
            edge.target_id.to_string(),
            encode_edge_kind(edge.kind),
        ],
    )?;
    Ok(())
}

fn insert_frontier(tx: &Transaction<'_>, frontier: &FrontierRecord) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT INTO frontiers (id, label, root_contract_node_id, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            frontier.id.to_string(),
            frontier.label.as_str(),
            frontier.root_contract_node_id.to_string(),
            encode_frontier_status(frontier.status),
            encode_timestamp(frontier.created_at)?,
            encode_timestamp(frontier.updated_at)?,
        ],
    )?;
    Ok(())
}

fn insert_checkpoint(
    tx: &Transaction<'_>,
    checkpoint: &CheckpointRecord,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT INTO checkpoints (
            id,
            frontier_id,
            node_id,
            repo_root,
            worktree_root,
            worktree_name,
            commit_hash,
            disposition,
            summary,
            created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            checkpoint.id.to_string(),
            checkpoint.frontier_id.to_string(),
            checkpoint.node_id.to_string(),
            checkpoint.snapshot.repo_root.as_str(),
            checkpoint.snapshot.worktree_root.as_str(),
            checkpoint
                .snapshot
                .worktree_name
                .as_ref()
                .map(NonEmptyText::as_str),
            checkpoint.snapshot.commit_hash.as_str(),
            encode_checkpoint_disposition(checkpoint.disposition),
            checkpoint.summary.as_str(),
            encode_timestamp(checkpoint.created_at)?,
        ],
    )?;
    Ok(())
}

fn insert_run(
    tx: &Transaction<'_>,
    run: &RunRecord,
    benchmark_suite: Option<&str>,
    primary_metric: &MetricValue,
    primary_metric_definition: &MetricDefinition,
    supporting_metrics: &[MetricValue],
    supporting_metric_definitions: &[MetricDefinition],
) -> Result<(), StoreError> {
    let (repo_root, worktree_root, worktree_name, head_commit, dirty_paths) = run
        .code_snapshot
        .as_ref()
        .map_or((None, None, None, None, None), |snapshot| {
            (
                Some(snapshot.repo_root.as_str().to_owned()),
                Some(snapshot.worktree_root.as_str().to_owned()),
                snapshot.worktree_name.as_ref().map(ToOwned::to_owned),
                snapshot.head_commit.as_ref().map(ToOwned::to_owned),
                Some(
                    snapshot
                        .dirty_paths
                        .iter()
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>(),
                ),
            )
        });
    let dirty_paths_json = match dirty_paths.as_ref() {
        Some(paths) => Some(encode_json(paths)?),
        None => None,
    };
    let started_at = match run.started_at {
        Some(timestamp) => Some(encode_timestamp(timestamp)?),
        None => None,
    };
    let finished_at = match run.finished_at {
        Some(timestamp) => Some(encode_timestamp(timestamp)?),
        None => None,
    };
    let _ = tx.execute(
        "INSERT INTO runs (
            run_id,
            node_id,
            frontier_id,
            status,
            backend,
            repo_root,
            worktree_root,
            worktree_name,
            head_commit,
            dirty_paths_json,
            benchmark_suite,
            working_directory,
            argv_json,
            env_json,
            started_at,
            finished_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            run.run_id.to_string(),
            run.node_id.to_string(),
            run.frontier_id.map(|id| id.to_string()),
            encode_run_status(run.status),
            encode_backend(run.backend),
            repo_root,
            worktree_root,
            worktree_name.map(|item| item.to_string()),
            head_commit.map(|item| item.to_string()),
            dirty_paths_json,
            benchmark_suite,
            run.command.working_directory.as_str(),
            encode_json(&run.command.argv)?,
            encode_json(&run.command.env)?,
            started_at,
            finished_at,
        ],
    )?;

    for (metric, definition) in std::iter::once((primary_metric, primary_metric_definition)).chain(
        supporting_metrics
            .iter()
            .zip(supporting_metric_definitions.iter()),
    ) {
        let _ = tx.execute(
            "INSERT INTO metrics (run_id, metric_key, unit, objective, value)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                run.run_id.to_string(),
                metric.key.as_str(),
                encode_metric_unit(definition.unit),
                encode_optimization_objective(definition.objective),
                metric.value,
            ],
        )?;
    }
    Ok(())
}

fn insert_experiment(
    tx: &Transaction<'_>,
    experiment: &CompletedExperiment,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT INTO experiments (
            id,
            frontier_id,
            base_checkpoint_id,
            candidate_checkpoint_id,
            change_node_id,
            run_node_id,
            run_id,
            analysis_node_id,
            decision_node_id,
            benchmark_suite,
            primary_metric_json,
            supporting_metrics_json,
            note_summary,
            note_next_json,
            verdict,
            created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            experiment.id.to_string(),
            experiment.frontier_id.to_string(),
            experiment.base_checkpoint_id.to_string(),
            experiment.candidate_checkpoint_id.to_string(),
            experiment.change_node_id.to_string(),
            experiment.run_node_id.to_string(),
            experiment.run_id.to_string(),
            experiment.analysis_node_id.map(|id| id.to_string()),
            experiment.decision_node_id.to_string(),
            benchmark_suite_label(&experiment.result.dimensions),
            encode_json(&experiment.result.primary_metric)?,
            encode_json(&experiment.result.supporting_metrics)?,
            experiment.note.summary.as_str(),
            encode_json(&experiment.note.next_hypotheses)?,
            encode_frontier_verdict(experiment.verdict),
            encode_timestamp(experiment.created_at)?,
        ],
    )?;
    Ok(())
}

fn insert_event(
    tx: &Transaction<'_>,
    entity_kind: &str,
    entity_id: &str,
    event_kind: &str,
    payload: Value,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "INSERT INTO events (entity_kind, entity_id, event_kind, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            entity_kind,
            entity_id,
            event_kind,
            payload.to_string(),
            encode_timestamp(OffsetDateTime::now_utc())?,
        ],
    )?;
    Ok(())
}

fn touch_frontier(
    tx: &Transaction<'_>,
    frontier_id: fidget_spinner_core::FrontierId,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "UPDATE frontiers SET updated_at = ?1 WHERE id = ?2",
        params![
            encode_timestamp(OffsetDateTime::now_utc())?,
            frontier_id.to_string()
        ],
    )?;
    Ok(())
}

fn demote_previous_champion(
    tx: &Transaction<'_>,
    frontier_id: fidget_spinner_core::FrontierId,
) -> Result<(), StoreError> {
    let _ = tx.execute(
        "UPDATE checkpoints
         SET disposition = 'baseline'
         WHERE frontier_id = ?1 AND disposition = 'champion'",
        params![frontier_id.to_string()],
    )?;
    Ok(())
}

fn read_node_row(row: &rusqlite::Row<'_>) -> Result<DagNode, rusqlite::Error> {
    let payload_json = row.get::<_, String>(9)?;
    let diagnostics_json = row.get::<_, String>(10)?;
    let payload = decode_json::<NodePayload>(&payload_json).map_err(to_sql_conversion_error)?;
    let diagnostics =
        decode_json::<NodeDiagnostics>(&diagnostics_json).map_err(to_sql_conversion_error)?;
    Ok(DagNode {
        id: parse_node_id(&row.get::<_, String>(0)?).map_err(to_sql_conversion_error)?,
        class: parse_node_class(&row.get::<_, String>(1)?).map_err(to_sql_conversion_error)?,
        track: parse_node_track(&row.get::<_, String>(2)?).map_err(to_sql_conversion_error)?,
        frontier_id: row
            .get::<_, Option<String>>(3)?
            .map(|raw| parse_frontier_id(&raw))
            .transpose()
            .map_err(to_sql_conversion_error)?,
        archived: row.get::<_, i64>(4)? != 0,
        title: NonEmptyText::new(row.get::<_, String>(5)?).map_err(core_to_sql_conversion_error)?,
        summary: row
            .get::<_, Option<String>>(6)?
            .map(NonEmptyText::new)
            .transpose()
            .map_err(core_to_sql_conversion_error)?,
        tags: BTreeSet::new(),
        payload,
        annotations: Vec::new(),
        diagnostics,
        agent_session_id: row
            .get::<_, Option<String>>(11)?
            .map(|raw| parse_agent_session_id(&raw))
            .transpose()
            .map_err(to_sql_conversion_error)?,
        created_at: decode_timestamp(&row.get::<_, String>(12)?)
            .map_err(to_sql_conversion_error)?,
        updated_at: decode_timestamp(&row.get::<_, String>(13)?)
            .map_err(to_sql_conversion_error)?,
    })
}

fn read_frontier_row(row: &rusqlite::Row<'_>) -> Result<FrontierRecord, StoreError> {
    Ok(FrontierRecord {
        id: parse_frontier_id(&row.get::<_, String>(0)?)?,
        label: NonEmptyText::new(row.get::<_, String>(1)?)?,
        root_contract_node_id: parse_node_id(&row.get::<_, String>(2)?)?,
        status: parse_frontier_status(&row.get::<_, String>(3)?)?,
        created_at: decode_timestamp(&row.get::<_, String>(4)?)?,
        updated_at: decode_timestamp(&row.get::<_, String>(5)?)?,
    })
}

fn read_checkpoint_row(row: &rusqlite::Row<'_>) -> Result<CheckpointRecord, rusqlite::Error> {
    Ok(CheckpointRecord {
        id: parse_checkpoint_id(&row.get::<_, String>(0)?).map_err(to_sql_conversion_error)?,
        frontier_id: parse_frontier_id(&row.get::<_, String>(1)?)
            .map_err(to_sql_conversion_error)?,
        node_id: parse_node_id(&row.get::<_, String>(2)?).map_err(to_sql_conversion_error)?,
        snapshot: CheckpointSnapshotRef {
            repo_root: Utf8PathBuf::from(row.get::<_, String>(3)?),
            worktree_root: Utf8PathBuf::from(row.get::<_, String>(4)?),
            worktree_name: row
                .get::<_, Option<String>>(5)?
                .map(NonEmptyText::new)
                .transpose()
                .map_err(core_to_sql_conversion_error)?,
            commit_hash: GitCommitHash::new(row.get::<_, String>(6)?)
                .map_err(core_to_sql_conversion_error)?,
        },
        disposition: parse_checkpoint_disposition(&row.get::<_, String>(7)?)
            .map_err(to_sql_conversion_error)?,
        summary: NonEmptyText::new(row.get::<_, String>(8)?)
            .map_err(core_to_sql_conversion_error)?,
        created_at: decode_timestamp(&row.get::<_, String>(9)?).map_err(to_sql_conversion_error)?,
    })
}

fn frontier_contract_payload(contract: &FrontierContract) -> Result<JsonObject, StoreError> {
    json_object(json!({
        "objective": contract.objective.as_str(),
        "benchmark_suites": contract
            .evaluation
            .benchmark_suites
            .iter()
            .map(NonEmptyText::as_str)
            .collect::<Vec<_>>(),
        "primary_metric": metric_spec_json(&contract.evaluation.primary_metric),
        "supporting_metrics": contract
            .evaluation
            .supporting_metrics
            .iter()
            .map(metric_spec_json)
            .collect::<Vec<_>>(),
        "promotion_criteria": contract
            .promotion_criteria
            .iter()
            .map(NonEmptyText::as_str)
            .collect::<Vec<_>>(),
    }))
}

fn metric_spec_json(metric: &MetricSpec) -> Value {
    json!({
        "metric_key": metric.metric_key.as_str(),
        "unit": encode_metric_unit(metric.unit),
        "objective": encode_optimization_objective(metric.objective),
    })
}

fn json_object(value: Value) -> Result<JsonObject, StoreError> {
    match value {
        Value::Object(map) => Ok(map),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected JSON object, got {other:?}"),
        )))),
    }
}

fn write_json_file<T: Serialize>(path: &Utf8Path, value: &T) -> Result<(), StoreError> {
    let serialized = serde_json::to_string_pretty(value)?;
    fs::write(path.as_std_path(), serialized)?;
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

fn decode_timestamp(raw: &str) -> Result<OffsetDateTime, StoreError> {
    OffsetDateTime::parse(raw, &Rfc3339).map_err(StoreError::from)
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

fn auto_capture_checkpoint_seed(
    project_root: &Utf8Path,
    summary: NonEmptyText,
) -> Result<Option<CheckpointSeed>, StoreError> {
    let top_level = git_output(project_root, &["rev-parse", "--show-toplevel"])?;
    let Some(repo_root) = top_level else {
        return Ok(None);
    };
    let commit_hash = git_output(project_root, &["rev-parse", "HEAD"])?
        .ok_or_else(|| StoreError::GitInspectionFailed(project_root.to_path_buf()))?;
    let worktree_name = git_output(project_root, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    Ok(Some(CheckpointSeed {
        summary,
        snapshot: CheckpointSnapshotRef {
            repo_root: Utf8PathBuf::from(repo_root),
            worktree_root: project_root.to_path_buf(),
            worktree_name: worktree_name.map(NonEmptyText::new).transpose()?,
            commit_hash: GitCommitHash::new(commit_hash)?,
        },
    }))
}

fn git_output(project_root: &Utf8Path, args: &[&str]) -> Result<Option<String>, StoreError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(project_root.as_str())
        .args(args)
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if text.is_empty() {
        return Ok(None);
    }
    Ok(Some(text))
}

fn to_sql_conversion_error(error: StoreError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn core_to_sql_conversion_error(error: fidget_spinner_core::CoreError) -> rusqlite::Error {
    to_sql_conversion_error(StoreError::from(error))
}

fn parse_uuid(raw: &str) -> Result<Uuid, StoreError> {
    Uuid::parse_str(raw).map_err(StoreError::from)
}

fn parse_node_id(raw: &str) -> Result<fidget_spinner_core::NodeId, StoreError> {
    Ok(fidget_spinner_core::NodeId::from_uuid(parse_uuid(raw)?))
}

fn parse_frontier_id(raw: &str) -> Result<fidget_spinner_core::FrontierId, StoreError> {
    Ok(fidget_spinner_core::FrontierId::from_uuid(parse_uuid(raw)?))
}

fn parse_checkpoint_id(raw: &str) -> Result<fidget_spinner_core::CheckpointId, StoreError> {
    Ok(fidget_spinner_core::CheckpointId::from_uuid(parse_uuid(
        raw,
    )?))
}

fn parse_experiment_id(raw: &str) -> Result<fidget_spinner_core::ExperimentId, StoreError> {
    Ok(fidget_spinner_core::ExperimentId::from_uuid(parse_uuid(
        raw,
    )?))
}

fn parse_run_id(raw: &str) -> Result<fidget_spinner_core::RunId, StoreError> {
    Ok(fidget_spinner_core::RunId::from_uuid(parse_uuid(raw)?))
}

fn parse_agent_session_id(raw: &str) -> Result<fidget_spinner_core::AgentSessionId, StoreError> {
    Ok(fidget_spinner_core::AgentSessionId::from_uuid(parse_uuid(
        raw,
    )?))
}

fn parse_annotation_id(raw: &str) -> Result<fidget_spinner_core::AnnotationId, StoreError> {
    Ok(fidget_spinner_core::AnnotationId::from_uuid(parse_uuid(
        raw,
    )?))
}

fn parse_node_class(raw: &str) -> Result<NodeClass, StoreError> {
    match raw {
        "contract" => Ok(NodeClass::Contract),
        "change" => Ok(NodeClass::Change),
        "run" => Ok(NodeClass::Run),
        "analysis" => Ok(NodeClass::Analysis),
        "decision" => Ok(NodeClass::Decision),
        "research" => Ok(NodeClass::Research),
        "enabling" => Ok(NodeClass::Enabling),
        "note" => Ok(NodeClass::Note),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown node class `{other}`"),
        )))),
    }
}

fn encode_node_track(track: fidget_spinner_core::NodeTrack) -> &'static str {
    match track {
        fidget_spinner_core::NodeTrack::CorePath => "core-path",
        fidget_spinner_core::NodeTrack::OffPath => "off-path",
    }
}

fn parse_node_track(raw: &str) -> Result<fidget_spinner_core::NodeTrack, StoreError> {
    match raw {
        "core-path" => Ok(fidget_spinner_core::NodeTrack::CorePath),
        "off-path" => Ok(fidget_spinner_core::NodeTrack::OffPath),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown node track `{other}`"),
        )))),
    }
}

fn encode_annotation_visibility(visibility: AnnotationVisibility) -> &'static str {
    match visibility {
        AnnotationVisibility::HiddenByDefault => "hidden",
        AnnotationVisibility::Visible => "visible",
    }
}

fn parse_annotation_visibility(raw: &str) -> Result<AnnotationVisibility, StoreError> {
    match raw {
        "hidden" => Ok(AnnotationVisibility::HiddenByDefault),
        "visible" => Ok(AnnotationVisibility::Visible),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown annotation visibility `{other}`"),
        )))),
    }
}

fn encode_edge_kind(kind: EdgeKind) -> &'static str {
    match kind {
        EdgeKind::Lineage => "lineage",
        EdgeKind::Evidence => "evidence",
        EdgeKind::Comparison => "comparison",
        EdgeKind::Supersedes => "supersedes",
        EdgeKind::Annotation => "annotation",
    }
}

fn encode_frontier_status(status: FrontierStatus) -> &'static str {
    match status {
        FrontierStatus::Exploring => "exploring",
        FrontierStatus::Paused => "paused",
        FrontierStatus::Saturated => "saturated",
        FrontierStatus::Archived => "archived",
    }
}

fn parse_frontier_status(raw: &str) -> Result<FrontierStatus, StoreError> {
    match raw {
        "exploring" => Ok(FrontierStatus::Exploring),
        "paused" => Ok(FrontierStatus::Paused),
        "saturated" => Ok(FrontierStatus::Saturated),
        "archived" => Ok(FrontierStatus::Archived),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown frontier status `{other}`"),
        )))),
    }
}

fn encode_checkpoint_disposition(disposition: CheckpointDisposition) -> &'static str {
    match disposition {
        CheckpointDisposition::Champion => "champion",
        CheckpointDisposition::FrontierCandidate => "frontier-candidate",
        CheckpointDisposition::Baseline => "baseline",
        CheckpointDisposition::DeadEnd => "dead-end",
        CheckpointDisposition::Archived => "archived",
    }
}

fn parse_checkpoint_disposition(raw: &str) -> Result<CheckpointDisposition, StoreError> {
    match raw {
        "champion" => Ok(CheckpointDisposition::Champion),
        "frontier-candidate" => Ok(CheckpointDisposition::FrontierCandidate),
        "baseline" => Ok(CheckpointDisposition::Baseline),
        "dead-end" => Ok(CheckpointDisposition::DeadEnd),
        "archived" => Ok(CheckpointDisposition::Archived),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown checkpoint disposition `{other}`"),
        )))),
    }
}

fn encode_run_status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn encode_backend(backend: ExecutionBackend) -> &'static str {
    match backend {
        ExecutionBackend::LocalProcess => "local-process",
        ExecutionBackend::WorktreeProcess => "worktree-process",
        ExecutionBackend::SshProcess => "ssh-process",
    }
}

fn encode_field_value_type(value_type: FieldValueType) -> &'static str {
    value_type.as_str()
}

fn decode_field_value_type(raw: &str) -> Result<FieldValueType, StoreError> {
    match raw {
        "string" => Ok(FieldValueType::String),
        "numeric" => Ok(FieldValueType::Numeric),
        "boolean" => Ok(FieldValueType::Boolean),
        "timestamp" => Ok(FieldValueType::Timestamp),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown field value type `{other}`"),
        )))),
    }
}

fn encode_metric_unit(unit: MetricUnit) -> &'static str {
    match unit {
        MetricUnit::Seconds => "seconds",
        MetricUnit::Bytes => "bytes",
        MetricUnit::Count => "count",
        MetricUnit::Ratio => "ratio",
        MetricUnit::Custom => "custom",
    }
}

fn decode_metric_unit(raw: &str) -> Result<MetricUnit, StoreError> {
    match raw {
        "seconds" => Ok(MetricUnit::Seconds),
        "bytes" => Ok(MetricUnit::Bytes),
        "count" => Ok(MetricUnit::Count),
        "ratio" => Ok(MetricUnit::Ratio),
        "custom" => Ok(MetricUnit::Custom),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown metric unit `{other}`"),
        )))),
    }
}

fn encode_optimization_objective(objective: OptimizationObjective) -> &'static str {
    match objective {
        OptimizationObjective::Minimize => "minimize",
        OptimizationObjective::Maximize => "maximize",
        OptimizationObjective::Target => "target",
    }
}

fn decode_optimization_objective(raw: &str) -> Result<OptimizationObjective, StoreError> {
    match raw {
        "minimize" => Ok(OptimizationObjective::Minimize),
        "maximize" => Ok(OptimizationObjective::Maximize),
        "target" => Ok(OptimizationObjective::Target),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown optimization objective `{other}`"),
        )))),
    }
}

fn encode_frontier_verdict(verdict: FrontierVerdict) -> &'static str {
    match verdict {
        FrontierVerdict::PromoteToChampion => "promote-to-champion",
        FrontierVerdict::KeepOnFrontier => "keep-on-frontier",
        FrontierVerdict::RevertToChampion => "revert-to-champion",
        FrontierVerdict::ArchiveDeadEnd => "archive-dead-end",
        FrontierVerdict::NeedsMoreEvidence => "needs-more-evidence",
    }
}

fn parse_frontier_verdict(raw: &str) -> Result<FrontierVerdict, StoreError> {
    match raw {
        "promote-to-champion" => Ok(FrontierVerdict::PromoteToChampion),
        "keep-on-frontier" => Ok(FrontierVerdict::KeepOnFrontier),
        "revert-to-champion" => Ok(FrontierVerdict::RevertToChampion),
        "archive-dead-end" => Ok(FrontierVerdict::ArchiveDeadEnd),
        "needs-more-evidence" => Ok(FrontierVerdict::NeedsMoreEvidence),
        other => Err(StoreError::Json(serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown frontier verdict `{other}`"),
        )))),
    }
}

type RunDimensionColumns = (Option<String>, Option<f64>, Option<i64>, Option<String>);

fn encode_run_dimension_columns(
    value: &RunDimensionValue,
) -> Result<RunDimensionColumns, StoreError> {
    match value {
        RunDimensionValue::String(item) => Ok((Some(item.to_string()), None, None, None)),
        RunDimensionValue::Numeric(item) => Ok((None, Some(*item), None, None)),
        RunDimensionValue::Boolean(item) => Ok((None, None, Some(i64::from(*item)), None)),
        RunDimensionValue::Timestamp(item) => {
            let _ = OffsetDateTime::parse(item.as_str(), &Rfc3339)?;
            Ok((None, None, None, Some(item.to_string())))
        }
    }
}

fn decode_run_dimension_value(
    value_type: FieldValueType,
    value_text: Option<String>,
    value_numeric: Option<f64>,
    value_boolean: Option<i64>,
    value_timestamp: Option<String>,
) -> Result<RunDimensionValue, StoreError> {
    match value_type {
        FieldValueType::String => Ok(RunDimensionValue::String(NonEmptyText::new(
            value_text.ok_or_else(|| {
                StoreError::Json(serde_json::Error::io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing string dimension value",
                )))
            })?,
        )?)),
        FieldValueType::Numeric => Ok(RunDimensionValue::Numeric(value_numeric.ok_or_else(
            || {
                StoreError::Json(serde_json::Error::io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing numeric dimension value",
                )))
            },
        )?)),
        FieldValueType::Boolean => Ok(RunDimensionValue::Boolean(
            value_boolean.ok_or_else(|| {
                StoreError::Json(serde_json::Error::io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing boolean dimension value",
                )))
            })? != 0,
        )),
        FieldValueType::Timestamp => {
            let value = value_timestamp.ok_or_else(|| {
                StoreError::Json(serde_json::Error::io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing timestamp dimension value",
                )))
            })?;
            let _ = OffsetDateTime::parse(&value, &Rfc3339)?;
            Ok(RunDimensionValue::Timestamp(NonEmptyText::new(value)?))
        }
    }
}

fn dimension_value_text(value: &RunDimensionValue) -> String {
    match value {
        RunDimensionValue::String(item) | RunDimensionValue::Timestamp(item) => item.to_string(),
        RunDimensionValue::Numeric(item) => item.to_string(),
        RunDimensionValue::Boolean(item) => item.to_string(),
    }
}

fn value_kind_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "numeric",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::json;

    use super::{
        CloseExperimentRequest, CreateFrontierRequest, CreateNodeRequest, DefineMetricRequest,
        DefineRunDimensionRequest, ListNodesQuery, MetricBestQuery, MetricFieldSource,
        MetricKeyQuery, MetricRankOrder, PROJECT_SCHEMA_NAME, ProjectStore,
        RemoveSchemaFieldRequest, UpsertSchemaFieldRequest,
    };
    use fidget_spinner_core::{
        CheckpointSnapshotRef, CommandRecipe, DiagnosticSeverity, EvaluationProtocol,
        FieldPresence, FieldRole, FieldValueType, FrontierContract, FrontierNote, FrontierVerdict,
        GitCommitHash, InferencePolicy, MetricSpec, MetricUnit, MetricValue, NodeAnnotation,
        NodeClass, NodePayload, NonEmptyText, OptimizationObjective, RunDimensionValue, TagName,
    };

    fn temp_project_root(label: &str) -> camino::Utf8PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "fidget_spinner_store_test_{}_{}",
            label,
            uuid::Uuid::now_v7()
        ));
        camino::Utf8PathBuf::from(path.to_string_lossy().into_owned())
    }

    #[test]
    fn init_writes_model_facing_schema_file() -> Result<(), super::StoreError> {
        let root = temp_project_root("schema");
        let store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;

        assert!(store.state_root().join(PROJECT_SCHEMA_NAME).exists());
        Ok(())
    }

    #[test]
    fn add_node_persists_hidden_annotations() -> Result<(), super::StoreError> {
        let root = temp_project_root("notes");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let node = store.add_node(CreateNodeRequest {
            class: NodeClass::Research,
            frontier_id: None,
            title: NonEmptyText::new("feature sketch")?,
            summary: Some(NonEmptyText::new("research note")?),
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "freeform"}))?,
            ),
            annotations: vec![NodeAnnotation::hidden(NonEmptyText::new(
                "private scratch",
            )?)],
            attachments: Vec::new(),
        })?;
        let loaded = store
            .get_node(node.id)?
            .ok_or(super::StoreError::NodeNotFound(node.id))?;

        assert_eq!(loaded.annotations.len(), 1);
        assert_eq!(
            loaded.annotations[0].visibility,
            fidget_spinner_core::AnnotationVisibility::HiddenByDefault
        );
        Ok(())
    }

    #[test]
    fn frontier_projection_tracks_initial_champion() -> Result<(), super::StoreError> {
        let root = temp_project_root("frontier");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let projection = store.create_frontier(CreateFrontierRequest {
            label: NonEmptyText::new("optimization frontier")?,
            contract_title: NonEmptyText::new("contract root")?,
            contract_summary: None,
            contract: FrontierContract {
                objective: NonEmptyText::new("improve wall time")?,
                evaluation: EvaluationProtocol {
                    benchmark_suites: BTreeSet::from([NonEmptyText::new("smoke")?]),
                    primary_metric: MetricSpec {
                        metric_key: NonEmptyText::new("wall_clock_s")?,
                        unit: MetricUnit::Seconds,
                        objective: OptimizationObjective::Minimize,
                    },
                    supporting_metrics: BTreeSet::new(),
                },
                promotion_criteria: vec![NonEmptyText::new("strict speedup")?],
            },
            initial_checkpoint: Some(super::CheckpointSeed {
                summary: NonEmptyText::new("seed")?,
                snapshot: CheckpointSnapshotRef {
                    repo_root: root.clone(),
                    worktree_root: root,
                    worktree_name: Some(NonEmptyText::new("main")?),
                    commit_hash: GitCommitHash::new("0123456789abcdef")?,
                },
            }),
        })?;

        assert!(projection.champion_checkpoint_id.is_some());
        Ok(())
    }

    #[test]
    fn list_nodes_hides_archived_by_default() -> Result<(), super::StoreError> {
        let root = temp_project_root("archive");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let node = store.add_node(CreateNodeRequest {
            class: NodeClass::Note,
            frontier_id: None,
            title: NonEmptyText::new("quick note")?,
            summary: Some(NonEmptyText::new("quick note summary")?),
            tags: Some(BTreeSet::new()),
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "hello"}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        })?;
        store.archive_node(node.id)?;

        let visible = store.list_nodes(ListNodesQuery::default())?;
        let hidden = store.list_nodes(ListNodesQuery {
            include_archived: true,
            ..ListNodesQuery::default()
        })?;

        assert!(visible.is_empty());
        assert_eq!(hidden.len(), 1);
        Ok(())
    }

    #[test]
    fn frontier_filter_includes_root_contract_node() -> Result<(), super::StoreError> {
        let root = temp_project_root("contract-filter");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let projection = store.create_frontier(CreateFrontierRequest {
            label: NonEmptyText::new("frontier")?,
            contract_title: NonEmptyText::new("root contract")?,
            contract_summary: None,
            contract: FrontierContract {
                objective: NonEmptyText::new("optimize")?,
                evaluation: EvaluationProtocol {
                    benchmark_suites: BTreeSet::from([NonEmptyText::new("smoke")?]),
                    primary_metric: MetricSpec {
                        metric_key: NonEmptyText::new("wall_clock_s")?,
                        unit: MetricUnit::Seconds,
                        objective: OptimizationObjective::Minimize,
                    },
                    supporting_metrics: BTreeSet::new(),
                },
                promotion_criteria: vec![NonEmptyText::new("faster")?],
            },
            initial_checkpoint: None,
        })?;

        let nodes = store.list_nodes(ListNodesQuery {
            frontier_id: Some(projection.frontier.id),
            ..ListNodesQuery::default()
        })?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].class, NodeClass::Contract);
        Ok(())
    }

    #[test]
    fn notes_require_explicit_tags_even_when_empty() -> Result<(), super::StoreError> {
        let root = temp_project_root("note-tags-required");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;

        let result = store.add_node(CreateNodeRequest {
            class: NodeClass::Note,
            frontier_id: None,
            title: NonEmptyText::new("quick note")?,
            summary: Some(NonEmptyText::new("quick note summary")?),
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "hello"}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        });

        assert!(matches!(result, Err(super::StoreError::NoteTagsRequired)));
        Ok(())
    }

    #[test]
    fn tags_round_trip_and_filter_node_list() -> Result<(), super::StoreError> {
        let root = temp_project_root("tag-roundtrip");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let cuts = store.add_tag(
            TagName::new("cuts/core")?,
            NonEmptyText::new("Core cutset work")?,
        )?;
        let heuristics = store.add_tag(
            TagName::new("heuristic")?,
            NonEmptyText::new("Heuristic tuning")?,
        )?;
        let note = store.add_node(CreateNodeRequest {
            class: NodeClass::Note,
            frontier_id: None,
            title: NonEmptyText::new("tagged note")?,
            summary: Some(NonEmptyText::new("tagged note summary")?),
            tags: Some(BTreeSet::from([cuts.name.clone(), heuristics.name.clone()])),
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "tagged"}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        })?;

        let loaded = store
            .get_node(note.id)?
            .ok_or(super::StoreError::NodeNotFound(note.id))?;
        assert_eq!(loaded.tags.len(), 2);

        let filtered = store.list_nodes(ListNodesQuery {
            tags: BTreeSet::from([cuts.name]),
            ..ListNodesQuery::default()
        })?;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].tags.len(), 2);
        Ok(())
    }

    #[test]
    fn prose_nodes_require_summary_and_body() -> Result<(), super::StoreError> {
        let root = temp_project_root("prose-summary");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;

        let missing_summary = store.add_node(CreateNodeRequest {
            class: NodeClass::Research,
            frontier_id: None,
            title: NonEmptyText::new("research note")?,
            summary: None,
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "research body"}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        });
        assert!(matches!(
            missing_summary,
            Err(super::StoreError::ProseSummaryRequired(NodeClass::Research))
        ));

        let missing_body = store.add_node(CreateNodeRequest {
            class: NodeClass::Note,
            frontier_id: None,
            title: NonEmptyText::new("quick note")?,
            summary: Some(NonEmptyText::new("quick note summary")?),
            tags: Some(BTreeSet::new()),
            payload: NodePayload::with_schema(store.schema().schema_ref(), serde_json::Map::new()),
            annotations: Vec::new(),
            attachments: Vec::new(),
        });
        assert!(matches!(
            missing_body,
            Err(super::StoreError::ProseBodyRequired(NodeClass::Note))
        ));
        Ok(())
    }

    #[test]
    fn opening_store_backfills_missing_prose_summaries() -> Result<(), super::StoreError> {
        let root = temp_project_root("summary-backfill");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let node = store.add_node(CreateNodeRequest {
            class: NodeClass::Research,
            frontier_id: None,
            title: NonEmptyText::new("research note")?,
            summary: Some(NonEmptyText::new("temporary summary")?),
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "First paragraph.\n\nSecond paragraph."}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        })?;
        drop(store);

        let connection = rusqlite::Connection::open(
            root.join(super::STORE_DIR_NAME)
                .join(super::STATE_DB_NAME)
                .as_std_path(),
        )?;
        let _ = connection.execute(
            "UPDATE nodes SET summary = NULL WHERE id = ?1",
            rusqlite::params![node.id.to_string()],
        )?;
        drop(connection);

        let reopened = ProjectStore::open(&root)?;
        let loaded = reopened
            .get_node(node.id)?
            .ok_or(super::StoreError::NodeNotFound(node.id))?;
        assert_eq!(
            loaded.summary.as_ref().map(NonEmptyText::as_str),
            Some("First paragraph.")
        );
        Ok(())
    }

    #[test]
    fn schema_field_upsert_remove_persists_and_bumps_version() -> Result<(), super::StoreError> {
        let root = temp_project_root("schema-upsert-remove");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let initial_version = store.schema().version;

        let field = store.upsert_schema_field(UpsertSchemaFieldRequest {
            name: NonEmptyText::new("scenario")?,
            node_classes: BTreeSet::from([NodeClass::Change, NodeClass::Analysis]),
            presence: FieldPresence::Recommended,
            severity: DiagnosticSeverity::Warning,
            role: FieldRole::ProjectionGate,
            inference_policy: InferencePolicy::ManualOnly,
            value_type: Some(FieldValueType::String),
        })?;
        assert_eq!(field.name.as_str(), "scenario");
        assert_eq!(store.schema().version, initial_version + 1);
        assert!(
            store
                .schema()
                .fields
                .iter()
                .any(|item| item.name.as_str() == "scenario")
        );
        drop(store);

        let mut reopened = ProjectStore::open(&root)?;
        assert_eq!(reopened.schema().version, initial_version + 1);
        assert!(
            reopened
                .schema()
                .fields
                .iter()
                .any(|item| item.name.as_str() == "scenario")
        );

        let removed = reopened.remove_schema_field(RemoveSchemaFieldRequest {
            name: NonEmptyText::new("scenario")?,
            node_classes: Some(BTreeSet::from([NodeClass::Change, NodeClass::Analysis])),
        })?;
        assert_eq!(removed, 1);
        assert_eq!(reopened.schema().version, initial_version + 2);
        assert!(
            !reopened
                .schema()
                .fields
                .iter()
                .any(|item| item.name.as_str() == "scenario")
        );
        Ok(())
    }

    #[test]
    fn metric_queries_surface_canonical_and_payload_numeric_fields() -> Result<(), super::StoreError>
    {
        let root = temp_project_root("metric-best");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let projection = store.create_frontier(CreateFrontierRequest {
            label: NonEmptyText::new("optimization frontier")?,
            contract_title: NonEmptyText::new("contract root")?,
            contract_summary: None,
            contract: FrontierContract {
                objective: NonEmptyText::new("improve wall time")?,
                evaluation: EvaluationProtocol {
                    benchmark_suites: BTreeSet::from([NonEmptyText::new("smoke")?]),
                    primary_metric: MetricSpec {
                        metric_key: NonEmptyText::new("wall_clock_s")?,
                        unit: MetricUnit::Seconds,
                        objective: OptimizationObjective::Minimize,
                    },
                    supporting_metrics: BTreeSet::new(),
                },
                promotion_criteria: vec![NonEmptyText::new("strict speedup")?],
            },
            initial_checkpoint: Some(super::CheckpointSeed {
                summary: NonEmptyText::new("seed")?,
                snapshot: checkpoint_snapshot(&root, "aaaaaaaaaaaaaaaa")?,
            }),
        })?;
        let frontier_id = projection.frontier.id;
        let base_checkpoint_id = projection
            .champion_checkpoint_id
            .ok_or_else(|| super::StoreError::MissingChampionCheckpoint { frontier_id })?;
        let _ = store.define_metric(DefineMetricRequest {
            key: NonEmptyText::new("wall_clock_s")?,
            unit: MetricUnit::Seconds,
            objective: OptimizationObjective::Minimize,
            description: Some(NonEmptyText::new("elapsed wall time")?),
        })?;
        let _ = store.define_run_dimension(DefineRunDimensionRequest {
            key: NonEmptyText::new("scenario")?,
            value_type: FieldValueType::String,
            description: Some(NonEmptyText::new("workload family")?),
        })?;
        let _ = store.define_run_dimension(DefineRunDimensionRequest {
            key: NonEmptyText::new("duration_s")?,
            value_type: FieldValueType::Numeric,
            description: Some(NonEmptyText::new("time budget in seconds")?),
        })?;

        let first_change = store.add_node(CreateNodeRequest {
            class: NodeClass::Change,
            frontier_id: Some(frontier_id),
            title: NonEmptyText::new("first change")?,
            summary: Some(NonEmptyText::new("first change summary")?),
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "first body", "latency_hint": 14.0}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        })?;
        let second_change = store.add_node(CreateNodeRequest {
            class: NodeClass::Change,
            frontier_id: Some(frontier_id),
            title: NonEmptyText::new("second change")?,
            summary: Some(NonEmptyText::new("second change summary")?),
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"body": "second body", "latency_hint": 7.0}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        })?;

        let first_receipt = store.close_experiment(experiment_request(
            &root,
            frontier_id,
            base_checkpoint_id,
            first_change.id,
            "bbbbbbbbbbbbbbbb",
            "first run",
            10.0,
            run_dimensions("belt_4x5", 20.0)?,
        )?)?;
        let second_receipt = store.close_experiment(experiment_request(
            &root,
            frontier_id,
            base_checkpoint_id,
            second_change.id,
            "cccccccccccccccc",
            "second run",
            5.0,
            run_dimensions("belt_4x5", 60.0)?,
        )?)?;

        let keys = store.list_metric_keys()?;
        assert!(keys.iter().any(|key| {
            key.key.as_str() == "wall_clock_s" && key.source == MetricFieldSource::RunMetric
        }));
        assert!(keys.iter().any(|key| {
            key.key.as_str() == "latency_hint" && key.source == MetricFieldSource::ChangePayload
        }));
        assert!(keys.iter().any(|key| {
            key.key.as_str() == "wall_clock_s"
                && key.source == MetricFieldSource::RunMetric
                && key.description.as_ref().map(NonEmptyText::as_str) == Some("elapsed wall time")
        }));

        let filtered_keys = store.list_metric_keys_filtered(MetricKeyQuery {
            frontier_id: Some(frontier_id),
            source: Some(MetricFieldSource::RunMetric),
            dimensions: run_dimensions("belt_4x5", 60.0)?,
        })?;
        assert_eq!(filtered_keys.len(), 1);
        assert_eq!(filtered_keys[0].experiment_count, 1);

        let dimension_summaries = store.list_run_dimensions()?;
        assert!(dimension_summaries.iter().any(|dimension| {
            dimension.key.as_str() == "benchmark_suite"
                && dimension.value_type == FieldValueType::String
                && dimension.observed_run_count == 2
        }));
        assert!(dimension_summaries.iter().any(|dimension| {
            dimension.key.as_str() == "scenario"
                && dimension.description.as_ref().map(NonEmptyText::as_str)
                    == Some("workload family")
        }));
        assert!(dimension_summaries.iter().any(|dimension| {
            dimension.key.as_str() == "duration_s"
                && dimension.value_type == FieldValueType::Numeric
                && dimension.distinct_value_count == 2
        }));

        let canonical_best = store.best_metrics(MetricBestQuery {
            key: NonEmptyText::new("wall_clock_s")?,
            frontier_id: Some(frontier_id),
            source: Some(MetricFieldSource::RunMetric),
            dimensions: run_dimensions("belt_4x5", 60.0)?,
            order: None,
            limit: 5,
        })?;
        assert_eq!(canonical_best.len(), 1);
        assert_eq!(canonical_best[0].value, 5.0);
        assert_eq!(
            canonical_best[0].candidate_checkpoint_id,
            second_receipt.checkpoint.id
        );
        assert_eq!(
            canonical_best[0]
                .dimensions
                .get(&NonEmptyText::new("duration_s")?),
            Some(&RunDimensionValue::Numeric(60.0))
        );

        let payload_best = store.best_metrics(MetricBestQuery {
            key: NonEmptyText::new("latency_hint")?,
            frontier_id: Some(frontier_id),
            source: Some(MetricFieldSource::ChangePayload),
            dimensions: run_dimensions("belt_4x5", 60.0)?,
            order: Some(MetricRankOrder::Asc),
            limit: 5,
        })?;
        assert_eq!(payload_best.len(), 1);
        assert_eq!(payload_best[0].value, 7.0);
        assert_eq!(payload_best[0].change_node_id, second_change.id);

        let missing_order = store.best_metrics(MetricBestQuery {
            key: NonEmptyText::new("latency_hint")?,
            frontier_id: Some(frontier_id),
            source: Some(MetricFieldSource::ChangePayload),
            dimensions: BTreeMap::new(),
            order: None,
            limit: 5,
        });
        assert!(matches!(
            missing_order,
            Err(super::StoreError::MetricOrderRequired { .. })
        ));
        assert_eq!(
            first_receipt.checkpoint.snapshot.commit_hash.as_str(),
            "bbbbbbbbbbbbbbbb"
        );
        Ok(())
    }

    #[test]
    fn opening_store_backfills_legacy_benchmark_suite_dimensions() -> Result<(), super::StoreError>
    {
        let root = temp_project_root("metric-plane-backfill");
        let mut store = ProjectStore::init(
            &root,
            NonEmptyText::new("test project")?,
            NonEmptyText::new("local.test")?,
        )?;
        let projection = store.create_frontier(CreateFrontierRequest {
            label: NonEmptyText::new("migration frontier")?,
            contract_title: NonEmptyText::new("migration contract")?,
            contract_summary: None,
            contract: FrontierContract {
                objective: NonEmptyText::new("exercise metric migration")?,
                evaluation: EvaluationProtocol {
                    benchmark_suites: BTreeSet::from([NonEmptyText::new("smoke")?]),
                    primary_metric: MetricSpec {
                        metric_key: NonEmptyText::new("wall_clock_s")?,
                        unit: MetricUnit::Seconds,
                        objective: OptimizationObjective::Minimize,
                    },
                    supporting_metrics: BTreeSet::new(),
                },
                promotion_criteria: vec![NonEmptyText::new("keep the metric plane queryable")?],
            },
            initial_checkpoint: Some(super::CheckpointSeed {
                summary: NonEmptyText::new("seed")?,
                snapshot: checkpoint_snapshot(&root, "aaaaaaaaaaaaaaaa")?,
            }),
        })?;
        let frontier_id = projection.frontier.id;
        let base_checkpoint_id = projection
            .champion_checkpoint_id
            .ok_or_else(|| super::StoreError::MissingChampionCheckpoint { frontier_id })?;
        let change = store.add_node(CreateNodeRequest {
            class: NodeClass::Change,
            frontier_id: Some(frontier_id),
            title: NonEmptyText::new("candidate change")?,
            summary: Some(NonEmptyText::new("candidate change summary")?),
            tags: None,
            payload: NodePayload::with_schema(
                store.schema().schema_ref(),
                super::json_object(json!({"latency_hint": 9.0}))?,
            ),
            annotations: Vec::new(),
            attachments: Vec::new(),
        })?;
        let _ = store.close_experiment(experiment_request(
            &root,
            frontier_id,
            base_checkpoint_id,
            change.id,
            "bbbbbbbbbbbbbbbb",
            "migration run",
            11.0,
            BTreeMap::from([(
                NonEmptyText::new("benchmark_suite")?,
                RunDimensionValue::String(NonEmptyText::new("smoke")?),
            )]),
        )?)?;
        drop(store);

        let connection = rusqlite::Connection::open(
            root.join(super::STORE_DIR_NAME)
                .join(super::STATE_DB_NAME)
                .as_std_path(),
        )?;
        let _ = connection.execute("DELETE FROM run_dimensions", [])?;
        drop(connection);

        let reopened = ProjectStore::open(&root)?;
        let dimensions = reopened.list_run_dimensions()?;
        assert!(dimensions.iter().any(|dimension| {
            dimension.key.as_str() == "benchmark_suite" && dimension.observed_run_count == 1
        }));

        let best = reopened.best_metrics(MetricBestQuery {
            key: NonEmptyText::new("wall_clock_s")?,
            frontier_id: Some(frontier_id),
            source: Some(MetricFieldSource::RunMetric),
            dimensions: BTreeMap::from([(
                NonEmptyText::new("benchmark_suite")?,
                RunDimensionValue::String(NonEmptyText::new("smoke")?),
            )]),
            order: None,
            limit: 5,
        })?;
        assert_eq!(best.len(), 1);
        assert_eq!(best[0].value, 11.0);
        Ok(())
    }

    fn checkpoint_snapshot(
        root: &camino::Utf8Path,
        commit: &str,
    ) -> Result<CheckpointSnapshotRef, super::StoreError> {
        Ok(CheckpointSnapshotRef {
            repo_root: root.to_path_buf(),
            worktree_root: root.to_path_buf(),
            worktree_name: Some(NonEmptyText::new("main")?),
            commit_hash: GitCommitHash::new(commit)?,
        })
    }

    fn experiment_request(
        root: &camino::Utf8Path,
        frontier_id: fidget_spinner_core::FrontierId,
        base_checkpoint_id: fidget_spinner_core::CheckpointId,
        change_node_id: fidget_spinner_core::NodeId,
        candidate_commit: &str,
        run_title: &str,
        wall_clock_s: f64,
        dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    ) -> Result<CloseExperimentRequest, super::StoreError> {
        Ok(CloseExperimentRequest {
            frontier_id,
            base_checkpoint_id,
            change_node_id,
            candidate_summary: NonEmptyText::new(format!("candidate {candidate_commit}"))?,
            candidate_snapshot: checkpoint_snapshot(root, candidate_commit)?,
            run_title: NonEmptyText::new(run_title)?,
            run_summary: Some(NonEmptyText::new("run summary")?),
            backend: fidget_spinner_core::ExecutionBackend::WorktreeProcess,
            dimensions,
            command: CommandRecipe::new(
                root.to_path_buf(),
                vec![NonEmptyText::new("true")?],
                BTreeMap::new(),
            )?,
            code_snapshot: None,
            primary_metric: MetricValue {
                key: NonEmptyText::new("wall_clock_s")?,
                value: wall_clock_s,
            },
            supporting_metrics: Vec::new(),
            note: FrontierNote {
                summary: NonEmptyText::new("note summary")?,
                next_hypotheses: Vec::new(),
            },
            verdict: FrontierVerdict::KeepOnFrontier,
            decision_title: NonEmptyText::new("decision")?,
            decision_rationale: NonEmptyText::new("decision rationale")?,
            analysis_node_id: None,
        })
    }

    fn run_dimensions(
        scenario: &str,
        duration_s: f64,
    ) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, super::StoreError> {
        Ok(BTreeMap::from([
            (
                NonEmptyText::new("benchmark_suite")?,
                RunDimensionValue::String(NonEmptyText::new("smoke")?),
            ),
            (
                NonEmptyText::new("scenario")?,
                RunDimensionValue::String(NonEmptyText::new(scenario)?),
            ),
            (
                NonEmptyText::new("duration_s")?,
                RunDimensionValue::Numeric(duration_s),
            ),
        ]))
    }
}
