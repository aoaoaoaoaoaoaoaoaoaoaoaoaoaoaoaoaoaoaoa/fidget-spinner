use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::process::Command;

use camino::{Utf8Path, Utf8PathBuf};
use fidget_spinner_core::{
    AnnotationVisibility, CheckpointDisposition, CheckpointRecord, CheckpointSnapshotRef,
    CodeSnapshotRef, CommandRecipe, CompletedExperiment, DagEdge, DagNode, EdgeKind,
    ExecutionBackend, ExperimentResult, FrontierContract, FrontierNote, FrontierProjection,
    FrontierRecord, FrontierStatus, FrontierVerdict, GitCommitHash, JsonObject, MetricObservation,
    MetricSpec, MetricUnit, NodeAnnotation, NodeClass, NodeDiagnostics, NodePayload, NonEmptyText,
    OptimizationObjective, ProjectSchema, RunRecord, RunStatus, TagName, TagRecord,
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
    #[error("git repository inspection failed for {0}")]
    GitInspectionFailed(Utf8PathBuf),
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
    pub benchmark_suite: NonEmptyText,
    pub command: CommandRecipe,
    pub code_snapshot: Option<CodeSnapshotRef>,
    pub primary_metric: MetricObservation,
    pub supporting_metrics: Vec<MetricObservation>,
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

        let connection = Connection::open(state_root.join(STATE_DB_NAME).as_std_path())?;
        migrate(&connection)?;

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
        let connection = Connection::open(state_root.join(STATE_DB_NAME).as_std_path())?;
        migrate(&connection)?;
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

    #[must_use]
    pub fn project_root(&self) -> &Utf8Path {
        &self.project_root
    }

    #[must_use]
    pub fn state_root(&self) -> &Utf8Path {
        &self.state_root
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

        let run_payload = NodePayload::with_schema(
            self.schema.schema_ref(),
            json_object(json!({
                "benchmark_suite": request.benchmark_suite.as_str(),
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
            benchmark_suite: Some(request.benchmark_suite.clone()),
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
                benchmark_suite: request.benchmark_suite,
                primary_metric: request.primary_metric,
                supporting_metrics: request.supporting_metrics,
                benchmark_bundle: None,
            },
            note: request.note,
            verdict: request.verdict,
            created_at: now,
        };

        let tx = self.connection.transaction()?;
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
            &experiment.result.primary_metric,
            &experiment.result.supporting_metrics,
        )?;
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
    primary_metric: &MetricObservation,
    supporting_metrics: &[MetricObservation],
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
            run.benchmark_suite.as_ref().map(NonEmptyText::as_str),
            run.command.working_directory.as_str(),
            encode_json(&run.command.argv)?,
            encode_json(&run.command.env)?,
            started_at,
            finished_at,
        ],
    )?;

    for metric in std::iter::once(primary_metric).chain(supporting_metrics.iter()) {
        let _ = tx.execute(
            "INSERT INTO metrics (run_id, metric_key, unit, objective, value)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                run.run_id.to_string(),
                metric.metric_key.as_str(),
                encode_metric_unit(metric.unit),
                encode_optimization_objective(metric.objective),
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
            experiment.result.benchmark_suite.as_str(),
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

fn encode_metric_unit(unit: MetricUnit) -> &'static str {
    match unit {
        MetricUnit::Seconds => "seconds",
        MetricUnit::Bytes => "bytes",
        MetricUnit::Count => "count",
        MetricUnit::Ratio => "ratio",
        MetricUnit::Custom => "custom",
    }
}

fn encode_optimization_objective(objective: OptimizationObjective) -> &'static str {
    match objective {
        OptimizationObjective::Minimize => "minimize",
        OptimizationObjective::Maximize => "maximize",
        OptimizationObjective::Target => "target",
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::{
        CreateFrontierRequest, CreateNodeRequest, ListNodesQuery, PROJECT_SCHEMA_NAME, ProjectStore,
    };
    use fidget_spinner_core::{
        CheckpointSnapshotRef, EvaluationProtocol, FrontierContract, MetricSpec, MetricUnit,
        NodeAnnotation, NodeClass, NodePayload, NonEmptyText, OptimizationObjective, TagName,
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
                    commit_hash: fidget_spinner_core::GitCommitHash::new("0123456789abcdef")?,
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
            summary: None,
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
            summary: None,
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
            summary: None,
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
}
