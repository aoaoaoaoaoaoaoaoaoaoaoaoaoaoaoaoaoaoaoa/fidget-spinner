use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slug(Box<str>);

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid slug {0:?}: expected dot-separated segments of [a-z0-9_-], each starting alphanumeric, at most 96 bytes"
)]
pub struct SlugError(String);

impl Slug {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn lane(&self) -> &str {
        self.0.split_once('.').map_or(&*self.0, |(lane, _)| lane)
    }
}

impl FromStr for Slug {
    type Err = SlugError;
    fn from_str(raw: &str) -> Result<Self, SlugError> {
        let lowered = raw.trim().to_ascii_lowercase();
        let valid = lowered.len() <= 96
            && lowered.split('.').all(|segment| {
                segment
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                    && segment
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            });
        if valid {
            Ok(Self(lowered.into()))
        } else {
            Err(SlugError(raw.to_owned()))
        }
    }
}

impl fmt::Display for Slug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl fmt::Debug for Slug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}
impl Serialize for Slug {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Slug {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}
