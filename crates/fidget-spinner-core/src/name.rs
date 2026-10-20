use crate::slug::{Slug, SlugError};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use std::{fmt, str::FromStr};
use time::OffsetDateTime;

/// A global patch sequence number.
pub type Seq = i64;

/// The largest integer every JSON reader holds exactly, 2⁵³ − 1.
pub const SAFE_INTEGER: i64 = (1 << 53) - 1;

/// A record incarnation, allocated in creation order within its frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(pub(crate) u32);

/// An operation's position: its patch's seq and its zero-based index in the patch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos {
    pub seq: Seq,
    pub index: u32,
}

/// A patch's seq and server time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub seq: Seq,
    pub time: OffsetDateTime,
}

/// Where and when an operation applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Moment {
    pub pos: Pos,
    pub time: OffsetDateTime,
}

/// A work name: a slug, or a numbered trial name `s#n`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Name {
    Slug(Slug),
    Numbered(Slug, u64),
}

pub(crate) fn safe_seq(seq: Seq) -> bool {
    (1..=SAFE_INTEGER).contains(&seq)
}

impl fmt::Display for Pos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.seq, self.index)
    }
}

impl FromStr for Pos {
    type Err = String;
    fn from_str(raw: &str) -> Result<Self, String> {
        let invalid = || format!("invalid revision {raw:?}: expected seq.index");
        let (seq, index) = raw.split_once('.').ok_or_else(invalid)?;
        let seq: Seq = seq.parse().map_err(|_| invalid())?;
        let index = index.parse().map_err(|_| invalid())?;
        if safe_seq(seq) {
            Ok(Self { seq, index })
        } else {
            Err(invalid())
        }
    }
}

impl Name {
    pub const fn slug(&self) -> &Slug {
        match self {
            Self::Slug(slug) | Self::Numbered(slug, _) => slug,
        }
    }
    pub fn lane(&self) -> &str {
        self.slug().lane()
    }
    pub const fn as_slug(&self) -> Option<&Slug> {
        match self {
            Self::Slug(slug) => Some(slug),
            Self::Numbered(..) => None,
        }
    }
}

impl FromStr for Name {
    type Err = String;
    fn from_str(raw: &str) -> Result<Self, String> {
        match raw.trim().rsplit_once('#') {
            Some((slug, n)) => {
                let n: u64 =
                    n.parse().ok().filter(|n| *n > 0).ok_or_else(|| {
                        format!("invalid trial name {raw:?}: expected slug#n, n ≥ 1")
                    })?;
                Ok(Self::Numbered(
                    slug.parse().map_err(|e: SlugError| e.to_string())?,
                    n,
                ))
            }
            None => raw
                .parse()
                .map(Self::Slug)
                .map_err(|e: SlugError| e.to_string()),
        }
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Slug(slug) => slug.fmt(f),
            Self::Numbered(slug, n) => write!(f, "{slug}#{n}"),
        }
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), f)
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
textual!(Pos, Name);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Id {
    type Err = std::num::ParseIntError;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse().map(Self)
    }
}
