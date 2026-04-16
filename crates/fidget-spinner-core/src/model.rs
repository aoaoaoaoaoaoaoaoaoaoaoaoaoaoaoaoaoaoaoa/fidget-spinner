use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::{
    ArtifactId, CoreError, ExperimentId, FrontierId, HypothesisId, RegistryLockId, TagFamilyId,
    TagId,
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TagFamilyName(String);

impl TagFamilyName {
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
            if matches!(character, '-' | '_') && !previous_was_separator {
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

impl TryFrom<String> for TagFamilyName {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TagFamilyName> for String {
    fn from(value: TagFamilyName) -> Self {
        value.0
    }
}

impl Display for TagFamilyName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Slug(String);

impl Slug {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let normalized = value.into().trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(CoreError::EmptySlug);
        }
        if Uuid::parse_str(&normalized).is_ok() {
            return Err(CoreError::UuidLikeSlug(normalized));
        }
        let mut previous_was_separator = true;
        for character in normalized.chars() {
            if character.is_ascii_lowercase() || character.is_ascii_digit() {
                previous_was_separator = false;
                continue;
            }
            if matches!(character, '-' | '_') && !previous_was_separator {
                previous_was_separator = true;
                continue;
            }
            return Err(CoreError::InvalidSlug(normalized));
        }
        if previous_was_separator {
            return Err(CoreError::InvalidSlug(normalized));
        }
        Ok(Self(normalized))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Slug {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Slug> for String {
    fn from(value: Slug) -> Self {
        value.0
    }
}

impl Display for Slug {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GitCommitHash(String);

impl GitCommitHash {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let normalized = value.into().trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(CoreError::EmptyGitCommitHash);
        }
        if !matches!(normalized.len(), 40 | 64)
            || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CoreError::InvalidGitCommitHash(normalized));
        }
        Ok(Self(normalized))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for GitCommitHash {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<GitCommitHash> for String {
    fn from(value: GitCommitHash) -> Self {
        value.0
    }
}

impl Display for GitCommitHash {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontierStatus {
    Exploring,
    Paused,
    Archived,
}

impl FrontierStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exploring => "exploring",
            Self::Paused => "paused",
            Self::Archived => "archived",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum KnownMetricUnit {
    Scalar,
    Count,
    Ratio,
    Percent,
    Bytes,
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl KnownMetricUnit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Count => "count",
            Self::Ratio => "ratio",
            Self::Percent => "percent",
            Self::Bytes => "bytes",
            Self::Nanoseconds => "nanoseconds",
            Self::Microseconds => "microseconds",
            Self::Milliseconds => "milliseconds",
            Self::Seconds => "seconds",
        }
    }

    fn parse_alias(raw: &str) -> Option<Self> {
        match raw {
            "1" | "scalar" | "unitless" | "dimensionless" => Some(Self::Scalar),
            "count" | "counts" => Some(Self::Count),
            "ratio" | "fraction" => Some(Self::Ratio),
            "%" | "percent" | "percentage" | "pct" => Some(Self::Percent),
            "bytes" | "byte" | "b" | "by" => Some(Self::Bytes),
            "nanoseconds" | "nanosecond" | "ns" => Some(Self::Nanoseconds),
            "microseconds" | "microsecond" | "us" | "µs" | "micros" => Some(Self::Microseconds),
            "milliseconds" | "millisecond" | "ms" | "millis" => Some(Self::Milliseconds),
            "seconds" | "second" | "s" | "sec" | "secs" => Some(Self::Seconds),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MetricUnit(String);

impl MetricUnit {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let raw = value.into();
        let normalized = normalize_metric_unit(&raw)?;
        Ok(Self(normalized))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn known_kind(&self) -> Option<KnownMetricUnit> {
        KnownMetricUnit::parse_alias(&self.0)
    }

    #[must_use]
    pub fn scalar() -> Self {
        Self(KnownMetricUnit::Scalar.as_str().to_owned())
    }
}

impl TryFrom<String> for MetricUnit {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<MetricUnit> for String {
    fn from(value: MetricUnit) -> Self {
        value.0
    }
}

impl Display for MetricUnit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn normalize_metric_unit(raw: &str) -> Result<String, CoreError> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(CoreError::EmptyMetricUnit);
    }
    if let Some(unit) = KnownMetricUnit::parse_alias(&normalized) {
        return Ok(unit.as_str().to_owned());
    }
    if normalized == "custom" {
        return Err(CoreError::InvalidMetricUnit(normalized));
    }
    let mut previous_was_separator = true;
    let mut has_alphanumeric = false;
    for character in normalized.chars() {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            previous_was_separator = false;
            has_alphanumeric = true;
            continue;
        }
        if matches!(character, '-' | '_' | '/' | '.') && !previous_was_separator {
            previous_was_separator = true;
            continue;
        }
        return Err(CoreError::InvalidMetricUnit(normalized));
    }
    if !has_alphanumeric || previous_was_separator {
        return Err(CoreError::InvalidMetricUnit(normalized));
    }
    Ok(normalized)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizationObjective {
    Minimize,
    Maximize,
    Target,
}

impl OptimizationObjective {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minimize => "minimize",
            Self::Maximize => "maximize",
            Self::Target => "target",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricVisibility {
    Canonical,
    Minor,
    Hidden,
    Archived,
}

impl MetricVisibility {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::Minor => "minor",
            Self::Hidden => "hidden",
            Self::Archived => "archived",
        }
    }

    #[must_use]
    pub const fn is_default_visible(self) -> bool {
        matches!(self, Self::Canonical | Self::Minor)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricDefinition {
    pub key: NonEmptyText,
    pub unit: MetricUnit,
    pub objective: OptimizationObjective,
    pub visibility: MetricVisibility,
    pub description: Option<NonEmptyText>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl MetricDefinition {
    #[must_use]
    pub fn new(
        key: NonEmptyText,
        unit: MetricUnit,
        objective: OptimizationObjective,
        visibility: MetricVisibility,
        description: Option<NonEmptyText>,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            key,
            unit,
            objective,
            visibility,
            description,
            created_at: now,
            updated_at: now,
        }
    }
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
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Numeric => "numeric",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
        }
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
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum RunDimensionValue {
    String(NonEmptyText),
    Numeric(f64),
    Boolean(bool),
    Timestamp(NonEmptyText),
}

impl RunDimensionValue {
    #[must_use]
    pub const fn value_type(&self) -> FieldValueType {
        match self {
            Self::String(_) => FieldValueType::String,
            Self::Numeric(_) => FieldValueType::Numeric,
            Self::Boolean(_) => FieldValueType::Boolean,
            Self::Timestamp(_) => FieldValueType::Timestamp,
        }
    }

    #[must_use]
    pub fn as_json(&self) -> Value {
        match self {
            Self::String(value) | Self::Timestamp(value) => Value::String(value.to_string()),
            Self::Numeric(value) => {
                serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
            }
            Self::Boolean(value) => Value::Bool(*value),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunDimensionDefinition {
    pub key: NonEmptyText,
    pub value_type: FieldValueType,
    pub description: Option<NonEmptyText>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl RunDimensionDefinition {
    #[must_use]
    pub fn new(
        key: NonEmptyText,
        value_type: FieldValueType,
        description: Option<NonEmptyText>,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            key,
            value_type,
            description,
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GitCommitHash, KnownMetricUnit, MetricUnit};

    #[test]
    fn metric_unit_normalizes_known_aliases() {
        let microseconds = MetricUnit::new("micros");
        assert!(microseconds.is_ok());
        let microseconds = match microseconds {
            Ok(value) => value,
            Err(_) => return,
        };
        assert_eq!(microseconds.as_str(), "microseconds");
        assert_eq!(
            microseconds.known_kind(),
            Some(KnownMetricUnit::Microseconds)
        );

        let percent = MetricUnit::new("%");
        assert!(percent.is_ok());
        let percent = match percent {
            Ok(value) => value,
            Err(_) => return,
        };
        assert_eq!(percent.as_str(), "percent");
        assert_eq!(percent.known_kind(), Some(KnownMetricUnit::Percent));
    }

    #[test]
    fn metric_unit_accepts_real_custom_tokens_and_rejects_placeholder_custom() {
        let objective = MetricUnit::new("objective");
        assert!(objective.is_ok());
        let objective = match objective {
            Ok(value) => value,
            Err(_) => return,
        };
        assert_eq!(objective.as_str(), "objective");
        assert_eq!(objective.known_kind(), None);

        let placeholder = MetricUnit::new("custom");
        assert!(placeholder.is_err());
    }

    #[test]
    fn git_commit_hash_normalizes_case_and_rejects_bad_shapes() {
        let sha1 = GitCommitHash::new("ABCDEF1234567890ABCDEF1234567890ABCDEF12");
        assert!(sha1.is_ok());
        let sha1 = match sha1 {
            Ok(value) => value,
            Err(_) => return,
        };
        assert_eq!(sha1.as_str(), "abcdef1234567890abcdef1234567890abcdef12");

        let short = GitCommitHash::new("deadbeef");
        assert!(short.is_err());

        let non_hex = GitCommitHash::new("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
        assert!(non_hex.is_err());
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricValue {
    pub key: NonEmptyText,
    pub value: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBackend {
    Manual,
    LocalProcess,
    WorktreeProcess,
    SshProcess,
}

impl ExecutionBackend {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::LocalProcess => "local_process",
            Self::WorktreeProcess => "worktree_process",
            Self::SshProcess => "ssh_process",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontierVerdict {
    Accepted,
    Kept,
    Parked,
    Rejected,
}

impl FrontierVerdict {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Kept => "kept",
            Self::Parked => "parked",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TagStatus {
    Active,
}

impl TagStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TagNameDisposition {
    Renamed,
    Merged,
    Deleted,
}

impl TagNameDisposition {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Renamed => "renamed",
            Self::Merged => "merged",
            Self::Deleted => "deleted",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RegistryName(String);

impl RegistryName {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let normalized = value.into().trim().to_ascii_lowercase();
        let _ = NonEmptyText::new(normalized.clone())?;
        Ok(Self(normalized))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn tags() -> Self {
        Self("tags".to_owned())
    }
}

impl Display for RegistryName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryLockMode {
    Definition,
    Assignment,
    Family,
}

impl RegistryLockMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Definition => "definition",
            Self::Assignment => "assignment",
            Self::Family => "family",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagFamilyRecord {
    pub id: TagFamilyId,
    pub name: TagFamilyName,
    pub description: NonEmptyText,
    pub mandatory: bool,
    pub status: TagStatus,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagRecord {
    pub id: TagId,
    pub name: TagName,
    pub description: NonEmptyText,
    pub family_id: Option<TagFamilyId>,
    pub family: Option<TagFamilyName>,
    pub status: TagStatus,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagNameHistoryRecord {
    pub name: TagName,
    pub target_tag_id: Option<TagId>,
    pub target_tag_name: Option<TagName>,
    pub disposition: TagNameDisposition,
    pub message: NonEmptyText,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegistryLockRecord {
    pub id: RegistryLockId,
    pub registry: RegistryName,
    pub mode: RegistryLockMode,
    pub scope_kind: NonEmptyText,
    pub scope_id: NonEmptyText,
    pub reason: NonEmptyText,
    pub revision: u64,
    pub locked_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagRegistrySnapshot {
    pub tags: Vec<TagRecord>,
    pub families: Vec<TagFamilyRecord>,
    pub locks: Vec<RegistryLockRecord>,
    pub name_history: Vec<TagNameHistoryRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandRecipe {
    #[serde(default)]
    pub working_directory: Option<Utf8PathBuf>,
    pub argv: Vec<NonEmptyText>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl CommandRecipe {
    pub fn new(
        working_directory: Option<Utf8PathBuf>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierRoadmapItem {
    pub rank: u32,
    pub hypothesis_id: HypothesisId,
    pub summary: Option<NonEmptyText>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierBrief {
    pub situation: Option<NonEmptyText>,
    pub roadmap: Vec<FrontierRoadmapItem>,
    pub unknowns: Vec<NonEmptyText>,
    #[serde(default)]
    pub scoreboard_metric_keys: Vec<NonEmptyText>,
    pub revision: u64,
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierRecord {
    pub id: FrontierId,
    pub slug: Slug,
    pub label: NonEmptyText,
    pub objective: NonEmptyText,
    pub status: FrontierStatus,
    pub brief: FrontierBrief,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HypothesisRecord {
    pub id: HypothesisId,
    pub slug: Slug,
    pub frontier_id: FrontierId,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: NonEmptyText,
    pub body: NonEmptyText,
    pub tags: Vec<TagName>,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentStatus {
    Open,
    Closed,
}

impl ExperimentStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExperimentAnalysis {
    pub summary: NonEmptyText,
    pub body: NonEmptyText,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentOutcome {
    pub backend: ExecutionBackend,
    pub command: CommandRecipe,
    pub dimensions: BTreeMap<NonEmptyText, RunDimensionValue>,
    pub primary_metric: MetricValue,
    pub supporting_metrics: Vec<MetricValue>,
    pub verdict: FrontierVerdict,
    pub rationale: NonEmptyText,
    pub analysis: Option<ExperimentAnalysis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_hash: Option<GitCommitHash>,
    pub closed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperimentRecord {
    pub id: ExperimentId,
    pub slug: Slug,
    pub frontier_id: FrontierId,
    pub hypothesis_id: HypothesisId,
    pub archived: bool,
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: Vec<TagName>,
    pub status: ExperimentStatus,
    pub outcome: Option<ExperimentOutcome>,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Document,
    Link,
    Log,
    Table,
    Plot,
    Dump,
    Binary,
    Other,
}

impl ArtifactKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Link => "link",
            Self::Log => "log",
            Self::Table => "table",
            Self::Plot => "plot",
            Self::Dump => "dump",
            Self::Binary => "binary",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub slug: Slug,
    pub kind: ArtifactKind,
    pub label: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub locator: NonEmptyText,
    pub media_type: Option<NonEmptyText>,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VertexKind {
    Hypothesis,
    Experiment,
}

impl VertexKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis",
            Self::Experiment => "experiment",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum VertexRef {
    Hypothesis(HypothesisId),
    Experiment(ExperimentId),
}

impl VertexRef {
    #[must_use]
    pub const fn kind(self) -> VertexKind {
        match self {
            Self::Hypothesis(_) => VertexKind::Hypothesis,
            Self::Experiment(_) => VertexKind::Experiment,
        }
    }

    #[must_use]
    pub fn opaque_id(self) -> String {
        match self {
            Self::Hypothesis(id) => id.to_string(),
            Self::Experiment(id) => id.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentTargetKind {
    Frontier,
    Hypothesis,
    Experiment,
}

impl AttachmentTargetKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Frontier => "frontier",
            Self::Hypothesis => "hypothesis",
            Self::Experiment => "experiment",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum AttachmentTargetRef {
    Frontier(FrontierId),
    Hypothesis(HypothesisId),
    Experiment(ExperimentId),
}

impl AttachmentTargetRef {
    #[must_use]
    pub const fn kind(self) -> AttachmentTargetKind {
        match self {
            Self::Frontier(_) => AttachmentTargetKind::Frontier,
            Self::Hypothesis(_) => AttachmentTargetKind::Hypothesis,
            Self::Experiment(_) => AttachmentTargetKind::Experiment,
        }
    }

    #[must_use]
    pub fn opaque_id(self) -> String {
        match self {
            Self::Frontier(id) => id.to_string(),
            Self::Hypothesis(id) => id.to_string(),
            Self::Experiment(id) => id.to_string(),
        }
    }
}
