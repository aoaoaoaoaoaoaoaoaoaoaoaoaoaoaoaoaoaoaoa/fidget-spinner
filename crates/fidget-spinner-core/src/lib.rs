//! Core domain types for the Fidget Spinner frontier machine.
//!
//! Fidget Spinner is intentionally austere. The canonical ledger is a narrow
//! experimental spine: frontiers scope work, hypotheses and experiments are the
//! only graph vertices, and bulky context lives off the hot path as artifact
//! references.

mod error;
mod id;
mod model;

pub use crate::error::CoreError;
pub use crate::id::{ArtifactId, ExperimentId, FrontierId, HypothesisId};
pub use crate::model::{
    ArtifactKind, ArtifactRecord, AttachmentTargetKind, AttachmentTargetRef, CommandRecipe,
    ExecutionBackend, ExperimentAnalysis, ExperimentOutcome, ExperimentRecord, ExperimentStatus,
    FieldValueType, FrontierBrief, FrontierRecord, FrontierRoadmapItem, FrontierStatus,
    FrontierVerdict, GitCommitHash, HypothesisRecord, KnownMetricUnit, MetricDefinition,
    MetricUnit, MetricValue, MetricVisibility, NonEmptyText, OptimizationObjective,
    RunDimensionDefinition, RunDimensionValue, Slug, TagName, TagRecord, VertexKind, VertexRef,
};
