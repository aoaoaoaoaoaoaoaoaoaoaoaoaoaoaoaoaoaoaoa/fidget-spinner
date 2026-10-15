use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Unit(Box<str>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum QuantityKind {
    Time,
    Memory,
    Dimensionless,
    Count(String),
}

impl fmt::Display for QuantityKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Time => formatter.write_str("time"),
            Self::Memory => formatter.write_str("memory"),
            Self::Dimensionless => formatter.write_str("dimensionless"),
            Self::Count(label) => write!(formatter, "count ({label})"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize)]
pub enum UnitError {
    #[error("invalid unit {0:?}")]
    Invalid(String),
    #[error("cannot convert {from} to {to}")]
    Incompatible { from: Unit, to: Unit },
    #[error("reading must be finite")]
    Nonfinite,
}

impl Unit {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn kind(&self) -> QuantityKind {
        self.scale().0
    }
    fn scale(&self) -> (QuantityKind, f64) {
        use QuantityKind::{Count, Dimensionless, Memory, Time};
        match self.as_str() {
            "ns" => (Time, 1e-9),
            "µs" => (Time, 1e-6),
            "ms" => (Time, 1e-3),
            "s" => (Time, 1.0),
            "min" => (Time, 60.0),
            "h" => (Time, 3600.0),
            "B" => (Memory, 1.0),
            "kB" => (Memory, 1e3),
            "MB" => (Memory, 1e6),
            "GB" => (Memory, 1e9),
            "TB" => (Memory, 1e12),
            "KiB" => (Memory, 1024.0),
            "MiB" => (Memory, 1_048_576.0),
            "GiB" => (Memory, 1_073_741_824.0),
            "TiB" => (Memory, 1_099_511_627_776.0),
            "1" => (Dimensionless, 1.0),
            "%" => (Dimensionless, 0.01),
            label => (Count(label.to_owned()), 1.0),
        }
    }
    pub fn convert(&self, value: f64, to: &Self) -> Result<f64, UnitError> {
        let (kind, scale) = self.scale();
        let (target, divisor) = to.scale();
        if kind != target {
            return Err(UnitError::Incompatible {
                from: self.clone(),
                to: to.clone(),
            });
        }
        let converted = value * (scale / divisor);
        if converted.is_finite() {
            Ok(converted)
        } else {
            Err(UnitError::Nonfinite)
        }
    }
}
impl FromStr for Unit {
    type Err = UnitError;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let unit = match raw {
            "s" | "sec" | "secs" | "second" | "seconds" => "s",
            "ms" | "msec" | "millisecond" | "milliseconds" => "ms",
            "us" | "µs" | "microsecond" | "microseconds" => "µs",
            "ns" | "nanosecond" | "nanoseconds" => "ns",
            "min" | "minute" | "minutes" => "min",
            "h" | "hr" | "hour" | "hours" => "h",
            "B" | "byte" | "bytes" => "B",
            unit => unit,
        };
        if matches!(
            unit,
            "µs" | "B" | "kB" | "MB" | "GB" | "TB" | "KiB" | "MiB" | "GiB" | "TiB" | "1" | "%"
        ) || (!unit.is_empty() && unit.bytes().all(|b| b.is_ascii_lowercase()))
        {
            Ok(Self(unit.into()))
        } else {
            Err(UnitError::Invalid(raw.to_owned()))
        }
    }
}
impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Serialize for Unit {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Unit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}
