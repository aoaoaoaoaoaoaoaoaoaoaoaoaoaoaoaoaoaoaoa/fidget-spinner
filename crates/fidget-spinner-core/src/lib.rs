//! Core domain types for the Fidget Spinner frontier machine.
//!
//! The product direction is intentionally local-first and agent-first: the DAG
//! is the canonical truth, while frontier state is a derived operational
//! projection over that graph. The global spine is intentionally narrow so
//! projects can carry richer payloads and annotations without fossilizing the
//! whole system into one universal schema.

mod error;
mod id;
mod model;

pub use crate::error::CoreError;
pub use crate::id::{
    AgentSessionId, AnnotationId, ArtifactId, CheckpointId, ExperimentId, FrontierId, NodeId, RunId,
};
pub use crate::model::{
    AdmissionState, AnnotationVisibility, ArtifactKind, ArtifactRef, CheckpointDisposition,
    CheckpointRecord, CheckpointSnapshotRef, CodeSnapshotRef, CommandRecipe, CompletedExperiment,
    DagEdge, DagNode, DiagnosticSeverity, EdgeKind, EvaluationProtocol, ExecutionBackend,
    ExperimentResult, FieldPresence, FieldRole, FrontierContract, FrontierNote, FrontierProjection,
    FrontierRecord, FrontierStatus, FrontierVerdict, GitCommitHash, InferencePolicy, JsonObject,
    MetricObservation, MetricSpec, MetricUnit, NodeAnnotation, NodeClass, NodeDiagnostics,
    NodePayload, NodeTrack, NonEmptyText, OptimizationObjective, PayloadSchemaRef,
    ProjectFieldSpec, ProjectSchema, RunRecord, RunStatus, ValidationDiagnostic,
};
