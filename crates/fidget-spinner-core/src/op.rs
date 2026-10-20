use crate::{Name, Pos, SAFE_INTEGER, Seq, Slug, Unit};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::{Map, Value as Json};
use std::{collections::BTreeMap, fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MetricKey(String);

impl MetricKey {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Lower,
    Higher,
}

/// A written reading: a number in the metric's unit, or text holding a number and a unit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReadingValue {
    Number(f64),
    Text(String),
}
pub type Readings = BTreeMap<MetricKey, ReadingValue>;

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricSpec {
    pub key: MetricKey,
    pub unit: Unit,
    pub better: Option<Direction>,
    pub kpi: Option<bool>,
    pub about: Option<String>,
    pub refs: Option<BTreeMap<String, f64>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bridge {
    pub slug: Slug,
    pub readings: Readings,
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Work(Name),
    Baseline(Slug),
}

/// A written treatment identity.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TreatmentSpec {
    Baseline(Slug),
    Identity(String),
}

/// A requested schema binding: `id`, or `id@rev` for one definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaRef {
    pub id: Slug,
    pub rev: Option<Pos>,
}

/// The plan revision a report executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Executed {
    Unknown,
    Revision(Pos),
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetPath {
    pub path: Vec<String>,
    pub value: Json,
}

/// A seq written as an integral JSON number within the safe range, however spelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SafeSeq(pub Seq);

impl FromStr for SchemaRef {
    type Err = String;
    fn from_str(raw: &str) -> Result<Self, String> {
        let (id, rev) = match raw.split_once('@') {
            Some((id, rev)) => (id, Some(rev.parse()?)),
            None => (raw, None),
        };
        Ok(Self {
            id: id.parse().map_err(|e: crate::SlugError| e.to_string())?,
            rev,
        })
    }
}
impl fmt::Display for SchemaRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.rev {
            Some(rev) => write!(f, "{}@{rev}", self.id),
            None => self.id.fmt(f),
        }
    }
}
impl FromStr for Executed {
    type Err = String;
    fn from_str(raw: &str) -> Result<Self, String> {
        if raw == "unknown" {
            Ok(Self::Unknown)
        } else {
            raw.parse()
                .map(Self::Revision)
                .map_err(|e| format!("{e}, or \"unknown\""))
        }
    }
}
impl fmt::Display for Executed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::Revision(pos) => pos.fmt(f),
        }
    }
}
macro_rules! textual {
    ($($ty:ty),+) => {$(
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                String::deserialize(deserializer)?.parse().map_err(D::Error::custom)
            }
        }
    )+};
}
textual!(SchemaRef, Executed);

impl<'de> Deserialize<'de> for SafeSeq {
    #[allow(
        clippy::float_cmp,
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        reason = "integrality is exact within ±(2⁵³ − 1)"
    )]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let number = serde_json::Number::deserialize(deserializer)?;
        let seq = number.as_i64().or_else(|| {
            number
                .as_f64()
                .filter(|v| v.trunc() == *v && v.abs() <= SAFE_INTEGER as f64)
                .map(|v| v as i64)
        });
        seq.filter(|seq| crate::name::safe_seq(*seq))
            .map(Self)
            .ok_or_else(|| D::Error::custom(format!("expected a seq in 1..={SAFE_INTEGER}")))
    }
}

macro_rules! operations {
    ($($name:literal => $variant:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }),* $(,)?) => {
        /// The operation language. Its meanings replay forever.
        #[derive(Clone, Debug, PartialEq, Deserialize)]
        #[serde(tag = "op")]
        pub enum Operation { $(#[serde(rename = $name)] $variant { $($(#[$attr])* $field: $ty),* }),* }

        impl Operation {
            pub const NAMES: &'static [&'static str] = &[$($name),*];
            pub const fn name(&self) -> &'static str {
                match self { $(Self::$variant { .. } => $name),* }
            }
            pub fn fields(name: &str) -> Option<Vec<&'static str>> {
                match name {
                    $($name => {
                        let mut fields: Vec<&'static str> = vec![$(stringify!($field)),*];
                        fields.retain(|field| *field != "target");
                        fields.extend(Self::targets(name));
                        Some(fields)
                    })*
                    _ => None,
                }
            }
            fn check_fields(name: &str, object: &Map<String, Json>) -> Result<(), String> {
                match name {
                    $($name => {
                        $(if stringify!($field) != "target" {
                            if let Some(value) = object.get(stringify!($field)) {
                                serde_path_to_error::deserialize::<_, $ty>(value.clone())
                                    .map(|_| ())
                                    .map_err(|error| {
                                        let path = error.path().to_string();
                                        let suffix = if path.is_empty() || path == "." { String::new() } else { format!(".{path}") };
                                        format!("{}{suffix}: {}", stringify!($field), error.inner())
                                    })?;
                            }
                        })*
                        Ok(())
                    })*
                    _ => Err("op: unknown operation".into()),
                }
            }
        }
    };
}

operations! {
    "frontier" => Frontier { objective: String, legacy_through: Option<SafeSeq> },
    "conclude" => Conclude { lesson: String },
    "archive" => Archive {},
    "unarchive" => Unarchive {},
    "guard" => Guard { head: SafeSeq },
    "regime" => Regime { contract: String, metrics: Vec<MetricSpec>, reason: Option<String>, bridge: Option<Bridge> },
    "gauge" => Gauge { contract: Option<String>, metrics: Option<Vec<MetricSpec>>, retire: Option<Vec<MetricKey>>, restore: Option<Vec<MetricKey>> },
    "define" => Define { schema: Slug, definition: Json, rev: Option<Pos> },
    "iset" => Iset { slug: Slug, schema: Option<SchemaRef>, data: Option<Json>, members: Option<Vec<Name>>, into: Option<Vec<Slug>>, after: Option<Vec<Name>> },
    "plan" => Plan { trial: Option<Name>, schema: Option<SchemaRef>, data: Option<Json>, control: Option<Slug>, treatment: Option<TreatmentSpec>, into: Option<Vec<Slug>>, after: Option<Vec<Name>> },
    "report" => Report { trial: Option<Name>, plan: Option<Executed>, readings: Option<Readings>, control_readings: Option<Readings>, artifacts: Option<Vec<String>>, data: Option<Json>, schema: Option<SchemaRef>, control: Option<Slug>, treatment: Option<TreatmentSpec>, into: Option<Vec<Slug>>, after: Option<Vec<Name>> },
    "edit" => Edit { work: Name, rev: Option<Pos>, data: Option<Json>, set: Option<Vec<SetPath>>, delete: Option<Vec<Vec<String>>>, schema: Option<SchemaRef>, unbind: Option<bool>, control: Option<Slug>, treatment: Option<TreatmentSpec> },
    "member" => Member { iset: Slug, add: Option<Vec<Name>>, drop: Option<Vec<Name>> },
    "after" => After { work: Name, add: Option<Vec<Name>>, drop: Option<Vec<Name>> },
    "judge" => Judge { iset: Slug, answer: Json, seen: Option<SafeSeq> },
    "baseline" => Baseline { slug: Slug, artifacts: Option<Vec<String>>, readings: Option<Readings>, note: Option<String> },
    "elect" => Elect { baseline: Slug, rationale: String, adopts: Option<Vec<Slug>> },
    "void" => Void { #[serde(flatten)] target: Target, reason: String },
    "remove" => Remove { #[serde(flatten)] target: Target },
    "rename" => Rename { #[serde(flatten)] target: Target, to: Name },
    "assign" => Assign { work: Name, owner: String, instructions: Option<String> },
    "release" => Release { work: Name },
    "start" => Start { work: Name },
    "stop" => Stop { work: Name },
    "reopen" => Reopen { iset: Slug },
}

impl Operation {
    fn targets(name: &str) -> &'static [&'static str] {
        match name {
            "void" | "remove" | "rename" => &["work", "baseline"],
            _ => &[],
        }
    }

    /// Why a caller may not write this operation, if it is reserved.
    pub fn reserved(&self) -> Option<&'static str> {
        match self {
            Self::Archive {} | Self::Unarchive {} => {
                Some("archive and unarchive are the owner's, in the navigator")
            }
            Self::Reopen { .. } => Some("reopen exists only in migrated FS 3 history"),
            Self::Frontier {
                legacy_through: Some(_),
                ..
            } => Some("legacy_through exists only in migrated FS 3 history"),
            _ => None,
        }
    }
}

/// One operation as written, with its parsed meaning.
#[derive(Clone, Debug, PartialEq)]
pub struct Op {
    raw: Json,
    operation: Operation,
}

impl Op {
    pub fn parse(raw: Json) -> Result<Self, String> {
        let object = raw
            .as_object()
            .ok_or_else(|| "op: an operation is an object".to_owned())?;
        let name = object.get("op").and_then(Json::as_str).unwrap_or("");
        let fields = Operation::fields(name).ok_or_else(|| {
            let nearby = crate::slug::nearest(name, Operation::NAMES.iter().copied());
            format!(
                "op: {}",
                crate::slug::suggestion(&format!("operation {name:?}"), &nearby)
            )
        })?;
        if let Some(key) = object
            .keys()
            .find(|key| key.as_str() != "op" && !fields.contains(&key.as_str()))
        {
            let nearby = crate::slug::nearest(key, fields.iter().copied());
            return Err(format!(
                "{key}: {}; fields: {}",
                crate::slug::suggestion(&format!("field {key:?}"), &nearby),
                fields.join(", ")
            ));
        }
        let targets = Operation::targets(name);
        if !targets.is_empty()
            && targets
                .iter()
                .filter(|key| object.contains_key(**key))
                .count()
                != 1
        {
            return Err(format!(
                "target: requires exactly one of {}",
                targets.join(", ")
            ));
        }
        Operation::check_fields(name, object)?;
        for (key, value) in object
            .iter()
            .filter(|(key, _)| targets.contains(&key.as_str()))
        {
            let checked = if key == "work" {
                serde_json::from_value::<Name>(value.clone()).map(|_| ())
            } else {
                serde_json::from_value::<Slug>(value.clone()).map(|_| ())
            };
            checked.map_err(|error| format!("{key}: {error}"))?;
        }
        let operation = serde_json::from_value(raw.clone()).map_err(|error| {
            let message = error.to_string();
            let path = message
                .strip_prefix("missing field `")
                .and_then(|rest| rest.split('`').next())
                .unwrap_or("op");
            format!("{path}: {message}")
        })?;
        Ok(Self { raw, operation })
    }
    pub const fn raw(&self) -> &Json {
        &self.raw
    }
    pub const fn operation(&self) -> &Operation {
        &self.operation
    }
    pub const fn name(&self) -> &'static str {
        self.operation.name()
    }
}

impl Serialize for Op {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.raw.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(Json::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}
