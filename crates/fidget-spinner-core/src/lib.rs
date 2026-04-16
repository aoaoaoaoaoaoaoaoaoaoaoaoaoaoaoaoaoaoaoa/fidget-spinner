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
pub use crate::id::{
    ArtifactId, ExperimentId, FrontierId, HypothesisId, KpiId, MetricId, RegistryLockId,
    TagFamilyId, TagId,
};
pub use crate::model::{
    ArtifactKind, ArtifactRecord, AttachmentTargetKind, AttachmentTargetRef, CommandRecipe,
    ExecutionBackend, ExperimentAnalysis, ExperimentOutcome, ExperimentRecord, ExperimentStatus,
    FieldValueType, FrontierBrief, FrontierKpiRecord, FrontierRecord, FrontierRoadmapItem,
    FrontierStatus, FrontierVerdict, GitCommitHash, HypothesisRecord, KnownMetricUnit,
    KpiMetricAlternativeRecord, MetricAggregation, MetricDefinition, MetricDimension, MetricUnit,
    MetricValue, MetricVisibility, NonEmptyText, OptimizationObjective, RegistryLockMode,
    RegistryLockRecord, RegistryName, RunDimensionDefinition, RunDimensionValue, Slug,
    TagFamilyName, TagFamilyRecord, TagName, TagNameDisposition, TagNameHistoryRecord, TagRecord,
    TagRegistrySnapshot, TagStatus, VertexKind, VertexRef,
};
