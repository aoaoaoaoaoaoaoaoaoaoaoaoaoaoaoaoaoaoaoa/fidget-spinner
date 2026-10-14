use crate::{Slug, Unit};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::{collections::BTreeMap, fmt, str::FromStr};
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MetricKey(String);

impl MetricKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for MetricKey {
    type Error = String;
    fn try_from(key: String) -> Result<Self, Self::Error> {
        if key.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            && key
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            Ok(Self(key))
        } else {
            Err(format!("invalid metric key {key:?}"))
        }
    }
}
impl From<MetricKey> for String {
    fn from(key: MetricKey) -> Self {
        key.0
    }
}
impl FromStr for MetricKey {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_owned())
    }
}
impl fmt::Display for MetricKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TrialId {
    pub hypothesis: Slug,
    pub number: u64,
}
impl FromStr for TrialId {
    type Err = String;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (slug, n) = raw
            .rsplit_once('#')
            .ok_or_else(|| format!("invalid trial identity {raw:?}"))?;
        let number = n.parse::<u64>().map_err(|e| e.to_string())?;
        if number == 0 {
            return Err("trial numbers start at 1".into());
        }
        Ok(Self {
            hypothesis: slug.parse().map_err(|e: crate::SlugError| e.to_string())?,
            number,
        })
    }
}
impl TryFrom<String> for TrialId {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}
impl From<TrialId> for String {
    fn from(id: TrialId) -> Self {
        id.to_string()
    }
}
impl fmt::Display for TrialId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.hypothesis, self.number)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Lower,
    Higher,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Number(f64),
    Text(String),
}
pub type Readings = BTreeMap<MetricKey, Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricSpec {
    pub key: MetricKey,
    pub unit: Unit,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub better: Option<Direction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kpi: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub about: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<BTreeMap<String, f64>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bridge {
    pub slug: Slug,
    pub readings: Readings,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

macro_rules! target {
    ($name:ident { $($variant:ident($ty:ty)),+ }) => {
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "lowercase")]
        pub enum $name { $($variant($ty)),+ }
    };
}
target!(DecisionTarget { Hypothesis(Slug), Baseline(Slug) });
target!(WorkTarget { Hypothesis(Slug), Review(Slug) });
target!(VoidTarget { Trial(TrialId), Baseline(Slug) });
target!(RemoveTarget { Hypothesis(Slug), Review(Slug), Baseline(Slug), Trial(TrialId) });
target!(RenameTarget { Hypothesis(Slug), Review(Slug), Baseline(Slug) });

macro_rules! operations {
    ($($variant:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }),* $(,)?) => {
        #[derive(Clone, Debug, PartialEq, Serialize)]
        #[serde(tag = "op", rename_all = "lowercase")]
        pub enum Op { $($variant { $($(#[$attr])* $field: $ty),* }),* }
        #[derive(Deserialize)]
        #[serde(tag = "op", rename_all = "lowercase")]
        enum WireOp { $($variant { $($(#[$attr])* $field: $ty),* }),* }
        impl From<WireOp> for Op {
            fn from(op: WireOp) -> Self { match op { $(WireOp::$variant { $($field),* } => Self::$variant { $($field),* }),* } }
        }
        impl Op {
            const fn fields(&self) -> &'static [&'static str] { match self { $(Self::$variant { .. } => &[$(stringify!($field)),*]),* } }
            #[must_use]
            pub fn name(&self) -> String { match self { $(Self::$variant { .. } => stringify!($variant)),* }.to_ascii_lowercase() }
        }
    }
}
operations! {
    Frontier { objective: String },
    Conclude { lesson: String },
    Regime { contract: String, metrics: Vec<MetricSpec>, #[serde(skip_serializing_if = "Option::is_none")] reason: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] bridge: Option<Bridge> },
    Gauge { #[serde(skip_serializing_if = "Option::is_none")] contract: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] metrics: Option<Vec<MetricSpec>>, #[serde(skip_serializing_if = "Option::is_none")] retire: Option<Vec<MetricKey>>, #[serde(skip_serializing_if = "Option::is_none")] restore: Option<Vec<MetricKey>> },
    Baseline { slug: Slug, #[serde(skip_serializing_if = "Option::is_none")] artifacts: Option<Vec<String>>, #[serde(skip_serializing_if = "Option::is_none")] readings: Option<Readings>, #[serde(skip_serializing_if = "Option::is_none")] adopts: Option<Vec<Slug>>, #[serde(skip_serializing_if = "Option::is_none")] docket: Option<Vec<Slug>>, #[serde(skip_serializing_if = "Option::is_none")] note: Option<String> },
    Docket { baseline: Slug, #[serde(skip_serializing_if = "Option::is_none")] add: Option<Vec<Slug>>, #[serde(skip_serializing_if = "Option::is_none")] drop: Option<Vec<Slug>> },
    Hypothesis { slug: Slug, #[serde(skip_serializing_if = "Option::is_none")] title: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] mechanism: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] claim: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] after: Option<Vec<Slug>>, #[serde(skip_serializing_if = "Option::is_none")] reviews: Option<Vec<Slug>> },
    Edge { from: Slug, to: Slug, #[serde(skip_serializing_if = "Option::is_none")] carries: Option<String> },
    Unedge { from: Slug, to: Slug },
    Review { slug: Slug, #[serde(skip_serializing_if = "Option::is_none")] title: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] charter: Option<String>, #[serde(skip_serializing_if = "Option::is_none")] members: Option<Vec<Slug>>, #[serde(skip_serializing_if = "Option::is_none")] add: Option<Vec<Slug>>, #[serde(skip_serializing_if = "Option::is_none")] drop: Option<Vec<Slug>> },
    Trial { hypothesis: Slug, note: String, #[serde(skip_serializing_if = "Option::is_none")] readings: Option<Readings>, #[serde(skip_serializing_if = "Option::is_none")] against: Option<Slug>, #[serde(skip_serializing_if = "Option::is_none")] artifacts: Option<Vec<String>> },
    Accept { #[serde(flatten)] target: DecisionTarget, rationale: String },
    Reject { #[serde(flatten)] target: DecisionTarget, rationale: String },
    Shelve { hypothesis: Slug, rationale: String },
    Reopen { #[serde(flatten)] target: WorkTarget },
    Void { #[serde(flatten)] target: VoidTarget, reason: String },
    Unvoid { #[serde(flatten)] target: VoidTarget },
    Remove { #[serde(flatten)] target: RemoveTarget },
    Lesson { review: Slug, text: String },
    Waive { finding: String, note: String },
    Assign { #[serde(flatten)] target: WorkTarget, owner: String, #[serde(skip_serializing_if = "Option::is_none")] instructions: Option<String> },
    Release { #[serde(flatten)] target: WorkTarget },
    Start { #[serde(flatten)] target: WorkTarget },
    Stop { #[serde(flatten)] target: WorkTarget },
    Rename { #[serde(flatten)] target: RenameTarget, to: Slug },
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("operation must be an object"))?;
        let name = object
            .get("op")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let targets: &[&str] = match name {
            "accept" | "reject" => &["hypothesis", "baseline"],
            "reopen" | "assign" | "release" | "start" | "stop" => &["hypothesis", "review"],
            "void" | "unvoid" => &["trial", "baseline"],
            "remove" => &["hypothesis", "review", "baseline", "trial"],
            "rename" => &["hypothesis", "review", "baseline"],
            _ => &[],
        };
        if !targets.is_empty()
            && targets
                .iter()
                .filter(|key| object.contains_key(**key))
                .count()
                != 1
        {
            return Err(D::Error::custom(format!(
                "{name} requires exactly one of {}",
                targets.join(", ")
            )));
        }
        let op =
            Self::from(serde_json::from_value::<WireOp>(value.clone()).map_err(D::Error::custom)?);
        let canonical = serde_json::to_value(&op).map_err(D::Error::custom)?;
        let fields = canonical
            .as_object()
            .ok_or_else(|| D::Error::custom("invalid operation representation"))?;
        if let Some(key) = object.keys().find(|key| {
            !fields.contains_key(*key)
                && (key.as_str() == "target" || !op.fields().contains(&key.as_str()))
        }) {
            return Err(D::Error::custom(format!("unknown field {key:?} in {name}")));
        }
        Ok(op)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Ownership {
    pub owner: Option<Assignment>,
    pub started: Option<OffsetDateTime>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Assignment {
    pub name: String,
    pub instructions: Option<String>,
    pub at: OffsetDateTime,
}
