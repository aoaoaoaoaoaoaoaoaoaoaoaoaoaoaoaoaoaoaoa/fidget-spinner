//! Core domain types for the Fidget Spinner frontier machine.
//!
//! Fidget Spinner is intentionally austere. The canonical ledger is a narrow
//! experimental spine: frontiers scope work, hypotheses and experiments are the
//! only graph vertices.

mod error;
mod id;
mod model;

pub use crate::error::CoreError;
pub use crate::id::{
    ExperimentId, FrontierId, HypothesisId, KpiId, MetricId, RegistryLockId, TagFamilyId, TagId,
};
pub use crate::model::{
    CommandRecipe, DefaultVisibility, ExecutionBackend, ExperimentAnalysis, ExperimentOutcome,
    ExperimentRecord, ExperimentStatus, FieldValueType, FrontierBrief, FrontierKpiRecord,
    FrontierRecord, FrontierRoadmapItem, FrontierStatus, FrontierVerdict, GitCommitHash,
    HiddenByDefaultReason, HypothesisRecord, KnownMetricUnit, KpiOrdinal, MetricAggregation,
    MetricBaseDimension, MetricDefinition, MetricDefinitionKind, MetricDimension,
    MetricDisplayUnit, MetricQuantity, MetricUnit, MetricValue, NonEmptyText,
    OptimizationObjective, RationalExponent, RegistryLockMode, RegistryLockRecord, RegistryName,
    ReportedMetricValue, RunDimensionDefinition, RunDimensionValue, Slug,
    SyntheticMetricExpression, TagFamilyName, TagFamilyRecord, TagName, TagNameDisposition,
    TagNameHistoryRecord, TagRecord, TagRegistrySnapshot, VertexKind, VertexRef,
};
