use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::ops::{Div, Mul};

use camino::Utf8PathBuf;
use num_rational::Ratio;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::{
    CoreError, ExperimentId, FrontierId, HypothesisId, KpiId, MetricId, RegistryLockId,
    TagFamilyId, TagId,
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricBaseDimension {
    Time,
    Count,
    Byte,
}

impl MetricBaseDimension {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Time => "time",
            Self::Count => "count",
            Self::Byte => "byte",
        }
    }
}

impl Display for MetricBaseDimension {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricDimension {
    Time,
    Count,
    Bytes,
    Dimensionless,
}

impl MetricDimension {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Time => "time",
            Self::Count => "count",
            Self::Bytes => "bytes",
            Self::Dimensionless => "dimensionless",
        }
    }

    #[must_use]
    pub const fn default_display_unit(self) -> MetricUnit {
        match self {
            Self::Time => MetricUnit::Milliseconds,
            Self::Count => MetricUnit::Count,
            Self::Bytes => MetricUnit::Kibibytes,
            Self::Dimensionless => MetricUnit::Dimensionless,
        }
    }

    #[must_use]
    pub const fn canonical_unit(self) -> MetricUnit {
        match self {
            Self::Time => MetricUnit::Nanoseconds,
            Self::Count => MetricUnit::Count,
            Self::Bytes => MetricUnit::Bytes,
            Self::Dimensionless => MetricUnit::Dimensionless,
        }
    }

    #[must_use]
    pub fn quantity(self) -> MetricQuantity {
        match self {
            Self::Time => MetricQuantity::time(),
            Self::Count => MetricQuantity::count(),
            Self::Bytes => MetricQuantity::byte(),
            Self::Dimensionless => MetricQuantity::dimensionless(),
        }
    }

    #[must_use]
    pub fn implicit_unit(self) -> Option<MetricUnit> {
        match self {
            Self::Count => Some(MetricUnit::Count),
            Self::Dimensionless => Some(MetricUnit::Dimensionless),
            Self::Time | Self::Bytes => None,
        }
    }

    #[must_use]
    pub fn supports(self, unit: MetricUnit) -> bool {
        unit.quantity() == self.quantity()
    }

    #[must_use]
    pub fn known_units(self) -> &'static [MetricUnit] {
        match self {
            Self::Time => &[
                MetricUnit::Nanoseconds,
                MetricUnit::Microseconds,
                MetricUnit::Milliseconds,
                MetricUnit::Seconds,
            ],
            Self::Count => &[MetricUnit::Count],
            Self::Bytes => &[
                MetricUnit::Bytes,
                MetricUnit::Kibibytes,
                MetricUnit::Mebibytes,
                MetricUnit::Gibibytes,
            ],
            Self::Dimensionless => &[MetricUnit::Dimensionless, MetricUnit::Percent],
        }
    }

    #[must_use]
    pub fn unit_catalog(self) -> String {
        self.known_units()
            .iter()
            .map(|unit| unit.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub type RationalExponent = Ratio<i32>;

#[derive(Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MetricQuantity {
    exponents: BTreeMap<MetricBaseDimension, RationalExponent>,
}

impl MetricQuantity {
    #[must_use]
    pub fn dimensionless() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn base(base: MetricBaseDimension) -> Self {
        Self {
            exponents: BTreeMap::from([(base, Ratio::from_integer(1))]),
        }
    }

    #[must_use]
    pub fn time() -> Self {
        Self::base(MetricBaseDimension::Time)
    }

    #[must_use]
    pub fn count() -> Self {
        Self::base(MetricBaseDimension::Count)
    }

    #[must_use]
    pub fn byte() -> Self {
        Self::base(MetricBaseDimension::Byte)
    }

    #[must_use]
    pub fn checked_root(&self, degree: u32) -> Option<Self> {
        if degree == 0 {
            return None;
        }
        self.exponents
            .iter()
            .map(|(dimension, exponent)| {
                let divisor = i32::try_from(degree).ok()?;
                Some((*dimension, exponent / divisor))
            })
            .collect::<Option<BTreeMap<_, _>>>()
            .map(Self::from_exponents)
    }

    #[must_use]
    pub fn is_dimensionless(&self) -> bool {
        self.exponents.is_empty()
    }

    #[must_use]
    pub fn simple_display_unit(&self) -> Option<MetricUnit> {
        if self == &Self::time() {
            return Some(MetricUnit::Milliseconds);
        }
        if self == &Self::count() {
            return Some(MetricUnit::Count);
        }
        if self == &Self::byte() {
            return Some(MetricUnit::Kibibytes);
        }
        if self.is_dimensionless() {
            return Some(MetricUnit::Dimensionless);
        }
        None
    }

    #[must_use]
    pub fn implicit_report_unit(&self) -> Option<MetricUnit> {
        if self == &Self::count() {
            return Some(MetricUnit::Count);
        }
        if self.is_dimensionless() {
            return Some(MetricUnit::Dimensionless);
        }
        None
    }

    #[must_use]
    pub fn supports_unit(&self, unit: MetricUnit) -> bool {
        unit.quantity() == *self
    }

    #[must_use]
    pub fn unit_catalog(&self) -> String {
        if self == &Self::time() {
            return MetricDimension::Time.unit_catalog();
        }
        if self == &Self::count() {
            return MetricDimension::Count.unit_catalog();
        }
        if self == &Self::byte() {
            return MetricDimension::Bytes.unit_catalog();
        }
        if self.is_dimensionless() {
            return MetricDimension::Dimensionless.unit_catalog();
        }
        self.canonical_unit_label()
    }

    #[must_use]
    pub fn canonical_unit_label(&self) -> String {
        if self.is_dimensionless() {
            return "dimensionless".to_owned();
        }
        let (numerator, denominator) = self.exponents.iter().fold(
            (Vec::<String>::new(), Vec::<String>::new()),
            |(mut numerator, mut denominator), (dimension, exponent)| {
                let target = if exponent.numer().is_negative() {
                    &mut denominator
                } else {
                    &mut numerator
                };
                target.push(format_dimension_factor(*dimension, *exponent));
                (numerator, denominator)
            },
        );
        let numerator = if numerator.is_empty() {
            "dimensionless".to_owned()
        } else {
            numerator.join("*")
        };
        if denominator.is_empty() {
            numerator
        } else {
            format!("{numerator}/{}", denominator.join("*"))
        }
    }

    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        parse_metric_quantity(raw)
    }

    #[must_use]
    fn from_exponents(exponents: BTreeMap<MetricBaseDimension, RationalExponent>) -> Self {
        Self {
            exponents: exponents
                .into_iter()
                .filter(|(_, exponent)| *exponent != Ratio::from_integer(0))
                .collect(),
        }
    }
}

impl Display for MetricQuantity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.canonical_unit_label())
    }
}

impl Mul for MetricQuantity {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        combine_quantities(self, rhs, |left, right| left + right)
    }
}

impl Div for MetricQuantity {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        combine_quantities(self, rhs, |left, right| left - right)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricAggregation {
    Point,
    Mean,
    Geomean,
    Median,
    P95,
    Min,
    Max,
    Sum,
}

impl MetricAggregation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Mean => "mean",
            Self::Geomean => "geomean",
            Self::Median => "median",
            Self::P95 => "p95",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum MetricUnit {
    Dimensionless,
    Count,
    Percent,
    Bytes,
    Kibibytes,
    Mebibytes,
    Gibibytes,
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

pub type KnownMetricUnit = MetricUnit;

impl MetricUnit {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let raw = value.into();
        normalize_metric_unit(&raw)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dimensionless => "dimensionless",
            Self::Count => "count",
            Self::Percent => "percent",
            Self::Bytes => "bytes",
            Self::Kibibytes => "kibibytes",
            Self::Mebibytes => "mebibytes",
            Self::Gibibytes => "gibibytes",
            Self::Nanoseconds => "nanoseconds",
            Self::Microseconds => "microseconds",
            Self::Milliseconds => "milliseconds",
            Self::Seconds => "seconds",
        }
    }

    #[must_use]
    pub const fn known_kind(self) -> Option<Self> {
        Some(self)
    }

    #[must_use]
    pub fn quantity(self) -> MetricQuantity {
        match self {
            Self::Nanoseconds | Self::Microseconds | Self::Milliseconds | Self::Seconds => {
                MetricQuantity::time()
            }
            Self::Count => MetricQuantity::count(),
            Self::Bytes | Self::Kibibytes | Self::Mebibytes | Self::Gibibytes => {
                MetricQuantity::byte()
            }
            Self::Percent | Self::Dimensionless => MetricQuantity::dimensionless(),
        }
    }

    #[must_use]
    pub fn canonical_value(self, value: f64) -> f64 {
        match self {
            Self::Nanoseconds => value,
            Self::Microseconds => value * 1_000.0,
            Self::Milliseconds => value * 1_000_000.0,
            Self::Seconds => value * 1_000_000_000.0,
            Self::Bytes => value,
            Self::Kibibytes => value * 1_024.0,
            Self::Mebibytes => value * 1_048_576.0,
            Self::Gibibytes => value * 1_073_741_824.0,
            Self::Percent => value / 100.0,
            Self::Count | Self::Dimensionless => value,
        }
    }

    #[must_use]
    pub fn display_value(self, canonical_value: f64) -> f64 {
        match self {
            Self::Nanoseconds => canonical_value,
            Self::Microseconds => canonical_value / 1_000.0,
            Self::Milliseconds => canonical_value / 1_000_000.0,
            Self::Seconds => canonical_value / 1_000_000_000.0,
            Self::Bytes => canonical_value,
            Self::Kibibytes => canonical_value / 1_024.0,
            Self::Mebibytes => canonical_value / 1_048_576.0,
            Self::Gibibytes => canonical_value / 1_073_741_824.0,
            Self::Percent => canonical_value * 100.0,
            Self::Count | Self::Dimensionless => canonical_value,
        }
    }

    #[must_use]
    pub fn scalar() -> Self {
        Self::Dimensionless
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
        value.as_str().to_owned()
    }
}

impl Display for MetricUnit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

fn normalize_metric_unit(raw: &str) -> Result<MetricUnit, CoreError> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(CoreError::EmptyMetricUnit);
    }
    match normalized.as_str() {
        "1" | "scalar" | "unitless" | "dimensionless" => Ok(MetricUnit::Dimensionless),
        "count" | "counts" => Ok(MetricUnit::Count),
        "ratio" | "fraction" => Ok(MetricUnit::Dimensionless),
        "%" | "percent" | "percentage" | "pct" => Ok(MetricUnit::Percent),
        "bytes" | "byte" | "b" | "by" => Ok(MetricUnit::Bytes),
        "kibibytes" | "kibibyte" | "kib" | "kibs" => Ok(MetricUnit::Kibibytes),
        "mebibytes" | "mebibyte" | "mib" | "mibs" => Ok(MetricUnit::Mebibytes),
        "gibibytes" | "gibibyte" | "gib" | "gibs" => Ok(MetricUnit::Gibibytes),
        "nanoseconds" | "nanosecond" | "ns" => Ok(MetricUnit::Nanoseconds),
        "microseconds" | "microsecond" | "us" | "µs" | "micros" => Ok(MetricUnit::Microseconds),
        "milliseconds" | "millisecond" | "ms" | "millis" => Ok(MetricUnit::Milliseconds),
        "seconds" | "second" | "s" | "sec" | "secs" => Ok(MetricUnit::Seconds),
        _ => Err(CoreError::InvalidMetricUnit(normalized)),
    }
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
pub enum HiddenByDefaultReason {
    InArchivedFrontiersOnly,
}

impl HiddenByDefaultReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InArchivedFrontiersOnly => "in_archived_frontiers_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct DefaultVisibility {
    hidden_by_default: Option<HiddenByDefaultReason>,
}

impl DefaultVisibility {
    #[must_use]
    pub const fn visible() -> Self {
        Self {
            hidden_by_default: None,
        }
    }

    #[must_use]
    pub const fn hidden(reason: HiddenByDefaultReason) -> Self {
        Self {
            hidden_by_default: Some(reason),
        }
    }

    #[must_use]
    pub const fn is_default_visible(self) -> bool {
        self.hidden_by_default.is_none()
    }

    #[must_use]
    pub const fn hidden_by_default_reason(self) -> Option<HiddenByDefaultReason> {
        self.hidden_by_default
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum MetricDisplayUnit {
    Known(MetricUnit),
    Canonical(MetricQuantity),
}

impl MetricDisplayUnit {
    #[must_use]
    pub fn for_quantity(quantity: &MetricQuantity) -> Self {
        quantity
            .simple_display_unit()
            .map_or_else(|| Self::Canonical(quantity.clone()), Self::Known)
    }

    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        match MetricUnit::new(raw) {
            Ok(unit) => Ok(Self::Known(unit)),
            Err(_) => MetricQuantity::parse(raw).map(Self::Canonical),
        }
    }

    #[must_use]
    pub fn quantity(&self) -> MetricQuantity {
        match self {
            Self::Known(unit) => unit.quantity(),
            Self::Canonical(quantity) => quantity.clone(),
        }
    }

    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Known(unit) => unit.as_str().to_owned(),
            Self::Canonical(quantity) => quantity.canonical_unit_label(),
        }
    }

    #[must_use]
    pub fn canonical_value(&self, value: f64) -> f64 {
        match self {
            Self::Known(unit) => unit.canonical_value(value),
            Self::Canonical(_) => value,
        }
    }

    #[must_use]
    pub fn display_value(&self, canonical_value: f64) -> f64 {
        match self {
            Self::Known(unit) => unit.display_value(canonical_value),
            Self::Canonical(_) => canonical_value,
        }
    }
}

impl Display for MetricDisplayUnit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.label())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricDefinitionKind {
    Observed,
    Synthetic,
}

impl MetricDefinitionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Synthetic => "synthetic",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricDefinition {
    pub id: MetricId,
    pub key: NonEmptyText,
    pub dimension: MetricQuantity,
    pub display_unit: MetricDisplayUnit,
    pub aggregation: MetricAggregation,
    pub objective: OptimizationObjective,
    pub description: Option<NonEmptyText>,
    pub kind: MetricDefinitionKind,
    pub revision: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl MetricDefinition {
    #[must_use]
    pub fn new(
        key: NonEmptyText,
        dimension: MetricQuantity,
        display_unit: MetricDisplayUnit,
        aggregation: MetricAggregation,
        objective: OptimizationObjective,
        description: Option<NonEmptyText>,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id: MetricId::fresh(),
            key,
            dimension,
            display_unit,
            aggregation,
            objective,
            description,
            kind: MetricDefinitionKind::Observed,
            revision: 1,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum SyntheticMetricExpression {
    Metric {
        metric: NonEmptyText,
    },
    Constant {
        value: f64,
        quantity: MetricQuantity,
    },
    Add {
        left: Box<SyntheticMetricExpression>,
        right: Box<SyntheticMetricExpression>,
    },
    Sub {
        left: Box<SyntheticMetricExpression>,
        right: Box<SyntheticMetricExpression>,
    },
    Mul {
        left: Box<SyntheticMetricExpression>,
        right: Box<SyntheticMetricExpression>,
    },
    Div {
        left: Box<SyntheticMetricExpression>,
        right: Box<SyntheticMetricExpression>,
    },
    Gmean {
        terms: Vec<SyntheticMetricExpression>,
    },
}

impl SyntheticMetricExpression {
    #[must_use]
    pub fn metric(metric: NonEmptyText) -> Self {
        Self::Metric { metric }
    }

    #[must_use]
    pub fn constant(value: f64, quantity: MetricQuantity) -> Self {
        Self::Constant { value, quantity }
    }
}

fn combine_quantities(
    left: MetricQuantity,
    right: MetricQuantity,
    combine: impl Fn(RationalExponent, RationalExponent) -> RationalExponent,
) -> MetricQuantity {
    let mut exponents = left.exponents;
    for (dimension, right_exponent) in right.exponents {
        let next = exponents.remove(&dimension).map_or_else(
            || combine(Ratio::from_integer(0), right_exponent),
            |left_exponent| combine(left_exponent, right_exponent),
        );
        if next != Ratio::from_integer(0) {
            let _ = exponents.insert(dimension, next);
        }
    }
    MetricQuantity::from_exponents(exponents)
}

fn format_dimension_factor(dimension: MetricBaseDimension, exponent: RationalExponent) -> String {
    let magnitude = Ratio::new(exponent.numer().abs(), *exponent.denom());
    if magnitude == Ratio::from_integer(1) {
        dimension.to_string()
    } else {
        format!("{dimension}^{}", format_rational_exponent(&magnitude))
    }
}

fn parse_metric_quantity(raw: &str) -> Result<MetricQuantity, CoreError> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(CoreError::InvalidMetricUnit(normalized));
    }
    if matches!(
        normalized.as_str(),
        "1" | "scalar" | "unitless" | "dimensionless" | "ratio" | "fraction"
    ) {
        return Ok(MetricQuantity::dimensionless());
    }
    let (numerator, denominator) = normalized
        .split_once('/')
        .map_or((normalized.as_str(), ""), |(numerator, denominator)| {
            (numerator, denominator)
        });
    let numerator_quantity = parse_metric_quantity_product(numerator, 1)?;
    let denominator_quantity = if denominator.trim().is_empty() {
        MetricQuantity::dimensionless()
    } else {
        parse_metric_quantity_product(denominator, -1)?
    };
    Ok(numerator_quantity * denominator_quantity)
}

fn parse_metric_quantity_product(raw: &str, sign: i32) -> Result<MetricQuantity, CoreError> {
    raw.split('*')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .try_fold(MetricQuantity::dimensionless(), |quantity, factor| {
            let (dimension, exponent) = parse_dimension_factor(factor, sign)?;
            Ok(quantity * MetricQuantity::from_exponents(BTreeMap::from([(dimension, exponent)])))
        })
}

fn parse_dimension_factor(
    raw: &str,
    sign: i32,
) -> Result<(MetricBaseDimension, RationalExponent), CoreError> {
    let (dimension, exponent) = raw.split_once('^').unwrap_or((raw, "1"));
    let dimension = match dimension.trim() {
        "time" => MetricBaseDimension::Time,
        "count" | "counts" => MetricBaseDimension::Count,
        "byte" | "bytes" | "b" => MetricBaseDimension::Byte,
        _ => return Err(CoreError::InvalidMetricUnit(raw.to_owned())),
    };
    let (numerator, denominator) = exponent
        .split_once('/')
        .map_or((exponent, "1"), |(numerator, denominator)| {
            (numerator, denominator)
        });
    let numerator = numerator
        .parse::<i32>()
        .map_err(|_| CoreError::InvalidMetricUnit(raw.to_owned()))?
        .saturating_mul(sign);
    let denominator = denominator
        .parse::<u32>()
        .map_err(|_| CoreError::InvalidMetricUnit(raw.to_owned()))?;
    let denominator =
        i32::try_from(denominator).map_err(|_| CoreError::InvalidMetricUnit(raw.to_owned()))?;
    if denominator == 0 {
        return Err(CoreError::InvalidMetricUnit(raw.to_owned()));
    }
    let exponent = Ratio::new(numerator, denominator);
    Ok((dimension, exponent))
}

fn format_rational_exponent(exponent: &RationalExponent) -> String {
    if *exponent.denom() == 1 {
        exponent.numer().to_string()
    } else {
        format!("{}/{}", exponent.numer(), exponent.denom())
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GitCommitHash, MetricQuantity, MetricUnit};

    #[test]
    fn metric_unit_normalizes_known_aliases() {
        let microseconds = MetricUnit::new("micros");
        assert!(microseconds.is_ok());
        let microseconds = match microseconds {
            Ok(value) => value,
            Err(_) => return,
        };
        assert_eq!(microseconds.as_str(), "microseconds");
        assert_eq!(microseconds, MetricUnit::Microseconds);

        let percent = MetricUnit::new("%");
        assert!(percent.is_ok());
        let percent = match percent {
            Ok(value) => value,
            Err(_) => return,
        };
        assert_eq!(percent.as_str(), "percent");
        assert_eq!(percent, MetricUnit::Percent);
    }

    #[test]
    fn metric_quantity_algebra_tracks_default_units_and_rational_roots() {
        assert_eq!(
            MetricQuantity::time().simple_display_unit(),
            Some(MetricUnit::Milliseconds)
        );
        assert_eq!(
            MetricQuantity::byte().simple_display_unit(),
            Some(MetricUnit::Kibibytes)
        );
        assert_eq!(
            (MetricQuantity::count() / MetricQuantity::time()).to_string(),
            "count/time"
        );
        assert_eq!(
            (MetricQuantity::time() * MetricQuantity::count())
                .checked_root(2)
                .map(|quantity| quantity.to_string()),
            Some("time^1/2*count^1/2".to_owned())
        );
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
    pub unit: MetricUnit,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReportedMetricValue {
    pub key: NonEmptyText,
    pub value: f64,
    pub unit: Option<MetricUnit>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontierKpiRecord {
    pub id: KpiId,
    pub frontier_id: FrontierId,
    pub metric_id: MetricId,
    pub ordinal: KpiOrdinal,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct KpiOrdinal(u32);

impl KpiOrdinal {
    pub const FIRST: Self = Self(0);

    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
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
pub enum HypothesisAssessmentLevel {
    Low,
    Medium,
    High,
}

impl HypothesisAssessmentLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
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

    #[must_use]
    pub fn kpis() -> Self {
        Self("kpis".to_owned())
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
    pub title: NonEmptyText,
    pub summary: NonEmptyText,
    pub body: NonEmptyText,
    pub expected_yield: HypothesisAssessmentLevel,
    pub confidence: HypothesisAssessmentLevel,
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
    #[serde(rename = "conditions")]
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
    pub title: NonEmptyText,
    pub summary: Option<NonEmptyText>,
    pub tags: Vec<TagName>,
    pub status: ExperimentStatus,
    pub outcome: Option<ExperimentOutcome>,
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
