use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{
    AgentSessionId, AnnotationId, ArtifactId, CheckpointId, CoreError, ExperimentId, FrontierId,
    NodeId, RunId,
};

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct NonEmptyText(String);

impl NonEmptyText {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(CoreError::EmptyText);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for NonEmptyText {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct GitCommitHash(NonEmptyText);

impl GitCommitHash {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        NonEmptyText::new(value).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for GitCommitHash {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TagName(String);

impl TagName {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let normalized = value.into().trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(CoreError::EmptyTagName);
        }
        let mut previous_was_separator = true;
        for character in normalized.chars() {
            if character.is_ascii_lowercase() || character.is_ascii_digit() {
                previous_was_separator = false;
                continue;
            }
            if matches!(character, '-' | '_' | '/') && !previous_was_separator {
                previous_was_separator = true;
                continue;
            }
            return Err(CoreError::InvalidTagName(normalized));
        }
        if previous_was_separator {
            return Err(CoreError::InvalidTagName(normalized));
        }
        Ok(Self(normalized))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for TagName {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TagName> for String {
    fn from(value: TagName) -> Self {
        value.0
    }
}

impl Display for TagName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub type JsonObject = Map<String, Value>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum NodeClass {
    Contract,
    Change,
    Run,
    Analysis,
    Decision,
    Research,
    Enabling,
    Note,
}

impl NodeClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Change => "change",
            Self::Run => "run",
            Self::Analysis => "analysis",
            Self::Decision => "decision",
            Self::Research => "research",
            Self::Enabling => "enabling",
            Self::Note => "note",
        }
    }

    #[must_use]
    pub const fn default_track(self) -> NodeTrack {
        match self {
            Self::Contract | Self::Change | Self::Run | Self::Analysis | Self::Decision => {
                NodeTrack::CorePath
            }
            Self::Research | Self::Enabling | Self::Note => NodeTrack::OffPath,
        }
    }
}

impl Display for NodeClass {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum NodeTrack {
    CorePath,
    OffPath,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum AnnotationVisibility {
    HiddenByDefault,
    Visible,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum FieldPresence {
    Required,
    Recommended,
    Optional,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum FieldRole {
    Index,
    ProjectionGate,
    RenderOnly,
    Opaque,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum InferencePolicy {
    ManualOnly,
    ModelMayInfer,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldValueType {
    String,
    Numeric,
    Boolean,
    Timestamp,
}

impl FieldValueType {
    #[must_use]
    pub const fn is_plottable(self) -> bool {
        matches!(self, Self::Numeric | Self::Timestamp)
    }

    #[must_use]
    pub fn accepts(self, value: &Value) -> bool {
        match self {
            Self::String => value.is_string(),
            Self::Numeric => value.is_number(),
            Self::Boolean => value.is_boolean(),
            Self::Timestamp => value
                .as_str()
                .is_some_and(|raw| OffsetDateTime::parse(raw, &Rfc3339).is_ok()),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Numeric => "numeric",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FrontierStatus {
    Exploring,
    Paused,
    Saturated,
    Archived,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CheckpointDisposition {
    Champion,
    FrontierCandidate,
    Baseline,
    DeadEnd,
    Archived,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum MetricUnit {
    Seconds,
    Bytes,
    Count,
    Ratio,
    Custom,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum OptimizationObjective {
    Minimize,
    Maximize,
    Target,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ExecutionBackend {
    LocalProcess,
    WorktreeProcess,
    SshProcess,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FrontierVerdict {
    PromoteToChampion,
    KeepOnFrontier,
    RevertToChampion,
    ArchiveDeadEnd,
    NeedsMoreEvidence,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AdmissionState {
    Admitted,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PayloadSchemaRef {
    pub namespace: NonEmptyText,
    pub version: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NodePayload {
    pub schema: Option<PayloadSchemaRef>,
    pub fields: JsonObject,
}

impl NodePayload {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            schema: None,
            fields: JsonObject::new(),
        }
    }

    #[must_use]
    pub fn with_schema(schema: PayloadSchemaRef, fields: JsonObject) -> Self {
        Self {
            schema: Some(schema),
            fields,
        }
    }

    #[must_use]
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeAnnotation {
    pub id: AnnotationId,
    pub visibility: AnnotationVisibility,
    pub label: Option<NonEmptyText>,
    pub body: NonEmptyText,
    pub created_at: OffsetDateTime,
}

impl NodeAnnotation {
    #[must_use]
    pub fn hidden(body: NonEmptyText) -> Self {
        Self {
            id: AnnotationId::fresh(),
            visibility: AnnotationVisibility::HiddenByDefault,
            label: None,
            body,
            created_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagRecord {
    pub name: TagName,
    pub description: NonEmptyText,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidationDiagnostic {
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: NonEmptyText,
    pub field_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeDiagnostics {
    pub admission: AdmissionState,
    pub items: Vec<ValidationDiagnostic>,
}

impl NodeDiagnostics {
    #[must_use]
    pub const fn admitted() -> Self {
        Self {
            admission: AdmissionState::Admitted,
            items: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectFieldSpec {
    pub name: NonEmptyText,
    pub node_classes: BTreeSet<NodeClass>,
    pub presence: FieldPresence,
    pub severity: DiagnosticSeverity,
    pub role: FieldRole,
    pub inference_policy: InferencePolicy,
    #[serde(default)]
    pub value_type: Option<FieldValueType>,
}

impl ProjectFieldSpec {
    #[must_use]
    pub fn applies_to(&self, class: NodeClass) -> bool {
        self.node_classes.is_empty() || self.node_classes.contains(&class)
    }

    #[must_use]
    pub fn is_plottable(&self) -> bool {
        self.value_type.is_some_and(FieldValueType::is_plottable)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectSchema {
    pub namespace: NonEmptyText,
    pub version: u32,
    pub fields: Vec<ProjectFieldSpec>,
}

impl ProjectSchema {
    #[must_use]
    pub fn default_with_namespace(namespace: NonEmptyText) -> Self {
        Self {
            namespace,
            version: 1,
            fields: Vec::new(),
        }
    }

    #[must_use]
    pub fn schema_ref(&self) -> PayloadSchemaRef {
        PayloadSchemaRef {
            namespace: self.namespace.clone(),
            version: self.version,
        }
    }

    #[must_use]
    pub fn field_spec(&self, class: NodeClass, name: &str) -> Option<&ProjectFieldSpec> {
        self.fields
            .iter()
            .find(|field| field.applies_to(class) && field.name.as_str() == name)
    }

    #[must_use]
    pub fn validate_node(&self, class: NodeClass, payload: &NodePayload) -> NodeDiagnostics {
        let items = self
            .fields
            .iter()
            .filter(|field| field.applies_to(class))
            .filter_map(|field| {
                let value = payload.field(field.name.as_str());
                let is_missing = value.is_none();
                if !is_missing || field.presence == FieldPresence::Optional {
                    if let (Some(value), Some(value_type)) = (value, field.value_type)
                        && !value_type.accepts(value)
                    {
                        return Some(ValidationDiagnostic {
                            severity: field.severity,
                            code: format!("type.{}", field.name.as_str()),
                            message: validation_message(format!(
                                "project payload field `{}` expected {}, found {}",
                                field.name.as_str(),
                                value_type.as_str(),
                                json_value_kind(value)
                            )),
                            field_name: Some(field.name.as_str().to_owned()),
                        });
                    }
                    return None;
                }
                Some(ValidationDiagnostic {
                    severity: field.severity,
                    code: format!("missing.{}", field.name.as_str()),
                    message: validation_message(format!(
                        "missing project payload field `{}`",
                        field.name.as_str()
                    )),
                    field_name: Some(field.name.as_str().to_owned()),
                })
            })
            .collect();
        NodeDiagnostics {
            admission: AdmissionState::Admitted,
            items,
        }
    }
}

fn validation_message(value: String) -> NonEmptyText {
    match NonEmptyText::new(value) {
        Ok(message) => message,
        Err(_) => unreachable!("validation diagnostics are never empty"),
    }
}

fn json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "numeric",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DagNode {
    pub id: NodeId,
    pub class: NodeClass,
    pub track: NodeTrack,
    pub frontier_id: Option<FrontierId>,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: BTreeSet<TagName>,
    pub payload: NodePayload,
    pub annotations: Vec<NodeAnnotation>,
    pub diagnostics: NodeDiagnostics,
    pub agent_session_id: Option<AgentSessionId>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl DagNode {
    #[must_use]
    pub fn new(
        class: NodeClass,
        frontier_id: Option<FrontierId>,
        title: NonEmptyText,
        summary: Option<NonEmptyText>,
        payload: NodePayload,
        diagnostics: NodeDiagnostics,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id: NodeId::fresh(),
            class,
            track: class.default_track(),
            frontier_id,
            archived: false,
            title,
            summary,
            tags: BTreeSet::new(),
            payload,
            annotations: Vec::new(),
            diagnostics,
            agent_session_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[must_use]
    pub fn is_core_path(&self) -> bool {
        self.track == NodeTrack::CorePath
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum EdgeKind {
    Lineage,
    Evidence,
    Comparison,
    Supersedes,
    Annotation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DagEdge {
    pub source_id: NodeId,
    pub target_id: NodeId,
    pub kind: EdgeKind,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ArtifactKind {
    Note,
    Patch,
    BenchmarkBundle,
    MetricSeries,
    Table,
    Plot,
    Log,
    Binary,
    Checkpoint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactRef {
    pub id: ArtifactId,
    pub kind: ArtifactKind,
    pub label: NonEmptyText,
    pub path: Utf8PathBuf,
    pub media_type: Option<NonEmptyText>,
    pub produced_by_run: Option<RunId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CodeSnapshotRef {
    pub repo_root: Utf8PathBuf,
    pub worktree_root: Utf8PathBuf,
    pub worktree_name: Option<NonEmptyText>,
    pub head_commit: Option<GitCommitHash>,
    pub dirty_paths: BTreeSet<Utf8PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CheckpointSnapshotRef {
    pub repo_root: Utf8PathBuf,
    pub worktree_root: Utf8PathBuf,
    pub worktree_name: Option<NonEmptyText>,
    pub commit_hash: GitCommitHash,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandRecipe {
    pub working_directory: Utf8PathBuf,
    pub argv: Vec<NonEmptyText>,
    pub env: BTreeMap<String, String>,
}

impl CommandRecipe {
    pub fn new(
        working_directory: Utf8PathBuf,
        argv: Vec<NonEmptyText>,
        env: BTreeMap<String, String>,
    ) -> Result<Self, CoreError> {
        if argv.is_empty() {
            return Err(CoreError::EmptyCommand);
        }
        Ok(Self {
            working_directory,
            argv,
            env,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MetricSpec {
    pub metric_key: NonEmptyText,
    pub unit: MetricUnit,
    pub objective: OptimizationObjective,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EvaluationProtocol {
    pub benchmark_suites: BTreeSet<NonEmptyText>,
    pub primary_metric: MetricSpec,
    pub supporting_metrics: BTreeSet<MetricSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierContract {
    pub objective: NonEmptyText,
    pub evaluation: EvaluationProtocol,
    pub promotion_criteria: Vec<NonEmptyText>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricObservation {
    pub metric_key: NonEmptyText,
    pub unit: MetricUnit,
    pub objective: OptimizationObjective,
    pub value: f64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierRecord {
    pub id: FrontierId,
    pub label: NonEmptyText,
    pub root_contract_node_id: NodeId,
    pub status: FrontierStatus,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl FrontierRecord {
    #[must_use]
    pub fn new(label: NonEmptyText, root_contract_node_id: NodeId) -> Self {
        Self::with_id(FrontierId::fresh(), label, root_contract_node_id)
    }

    #[must_use]
    pub fn with_id(id: FrontierId, label: NonEmptyText, root_contract_node_id: NodeId) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id,
            label,
            root_contract_node_id,
            status: FrontierStatus::Exploring,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CheckpointRecord {
    pub id: CheckpointId,
    pub frontier_id: FrontierId,
    pub node_id: NodeId,
    pub snapshot: CheckpointSnapshotRef,
    pub disposition: CheckpointDisposition,
    pub summary: NonEmptyText,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunRecord {
    pub node_id: NodeId,
    pub run_id: RunId,
    pub frontier_id: Option<FrontierId>,
    pub status: RunStatus,
    pub backend: ExecutionBackend,
    pub code_snapshot: Option<CodeSnapshotRef>,
    pub benchmark_suite: Option<NonEmptyText>,
    pub command: CommandRecipe,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentResult {
    pub benchmark_suite: NonEmptyText,
    pub primary_metric: MetricObservation,
    pub supporting_metrics: Vec<MetricObservation>,
    pub benchmark_bundle: Option<ArtifactId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierNote {
    pub summary: NonEmptyText,
    pub next_hypotheses: Vec<NonEmptyText>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CompletedExperiment {
    pub id: ExperimentId,
    pub frontier_id: FrontierId,
    pub base_checkpoint_id: CheckpointId,
    pub candidate_checkpoint_id: CheckpointId,
    pub change_node_id: NodeId,
    pub run_node_id: NodeId,
    pub run_id: RunId,
    pub analysis_node_id: Option<NodeId>,
    pub decision_node_id: NodeId,
    pub result: ExperimentResult,
    pub note: FrontierNote,
    pub verdict: FrontierVerdict,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierProjection {
    pub frontier: FrontierRecord,
    pub champion_checkpoint_id: Option<CheckpointId>,
    pub candidate_checkpoint_ids: BTreeSet<CheckpointId>,
    pub experiment_count: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use camino::Utf8PathBuf;
    use serde_json::json;

    use super::{
        CommandRecipe, DagNode, DiagnosticSeverity, FieldPresence, FieldRole, FieldValueType,
        InferencePolicy, JsonObject, NodeClass, NodePayload, NonEmptyText, ProjectFieldSpec,
        ProjectSchema,
    };
    use crate::CoreError;

    #[test]
    fn non_empty_text_rejects_blank_input() {
        let text = NonEmptyText::new("   ");
        assert_eq!(text, Err(CoreError::EmptyText));
    }

    #[test]
    fn command_recipe_requires_argv() {
        let recipe = CommandRecipe::new(
            Utf8PathBuf::from("/tmp/worktree"),
            Vec::new(),
            BTreeMap::new(),
        );
        assert_eq!(recipe, Err(CoreError::EmptyCommand));
    }

    #[test]
    fn schema_validation_warns_without_rejecting_ingest() -> Result<(), CoreError> {
        let schema = ProjectSchema {
            namespace: NonEmptyText::new("local.libgrid")?,
            version: 1,
            fields: vec![ProjectFieldSpec {
                name: NonEmptyText::new("hypothesis")?,
                node_classes: BTreeSet::from([NodeClass::Change]),
                presence: FieldPresence::Required,
                severity: DiagnosticSeverity::Warning,
                role: FieldRole::ProjectionGate,
                inference_policy: InferencePolicy::ManualOnly,
                value_type: None,
            }],
        };
        let payload = NodePayload::with_schema(schema.schema_ref(), JsonObject::new());
        let diagnostics = schema.validate_node(NodeClass::Change, &payload);

        assert_eq!(diagnostics.admission, super::AdmissionState::Admitted);
        assert_eq!(diagnostics.items.len(), 1);
        assert_eq!(diagnostics.items[0].severity, DiagnosticSeverity::Warning);
        Ok(())
    }

    #[test]
    fn schema_validation_warns_on_type_mismatch() -> Result<(), CoreError> {
        let schema = ProjectSchema {
            namespace: NonEmptyText::new("local.libgrid")?,
            version: 1,
            fields: vec![ProjectFieldSpec {
                name: NonEmptyText::new("improvement")?,
                node_classes: BTreeSet::from([NodeClass::Analysis]),
                presence: FieldPresence::Recommended,
                severity: DiagnosticSeverity::Warning,
                role: FieldRole::RenderOnly,
                inference_policy: InferencePolicy::ManualOnly,
                value_type: Some(FieldValueType::Numeric),
            }],
        };
        let payload = NodePayload::with_schema(
            schema.schema_ref(),
            JsonObject::from_iter([("improvement".to_owned(), json!("not a number"))]),
        );
        let diagnostics = schema.validate_node(NodeClass::Analysis, &payload);

        assert_eq!(diagnostics.admission, super::AdmissionState::Admitted);
        assert_eq!(diagnostics.items.len(), 1);
        assert_eq!(diagnostics.items[0].code, "type.improvement");
        Ok(())
    }

    #[test]
    fn research_nodes_default_to_off_path() -> Result<(), CoreError> {
        let payload = NodePayload {
            schema: None,
            fields: JsonObject::from_iter([("topic".to_owned(), json!("ideas"))]),
        };
        let node = DagNode::new(
            NodeClass::Research,
            None,
            NonEmptyText::new("feature scouting")?,
            None,
            payload,
            super::NodeDiagnostics::admitted(),
        );

        assert!(!node.is_core_path());
        Ok(())
    }
}
