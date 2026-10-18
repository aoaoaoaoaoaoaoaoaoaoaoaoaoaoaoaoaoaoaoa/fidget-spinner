use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

pub fn nearest<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let candidates: Vec<_> = candidates
        .into_iter()
        .filter(|candidate| *candidate != name)
        .collect();
    let prefixes: Vec<_> = candidates
        .iter()
        .filter(|candidate| candidate.starts_with(name))
        .collect();
    if let [candidate] = prefixes.as_slice() {
        return vec![(***candidate).to_owned()];
    }
    let mut ranked: Vec<_> = candidates
        .into_iter()
        .filter_map(|candidate| {
            let distance = edit_distance(name, candidate);
            (distance <= 2).then_some((distance, candidate))
        })
        .collect();
    ranked.sort_unstable();
    ranked
        .into_iter()
        .take(3)
        .map(|(_, candidate)| candidate.to_owned())
        .collect()
}

pub fn suggestion(name: &str, candidates: &[String]) -> String {
    if candidates.is_empty() {
        format!("unknown {name}")
    } else {
        format!("unknown {name}; nearest: {}", candidates.join(", "))
    }
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<_> = right.chars().collect();
    let mut row: Vec<_> = (0..=right.len()).collect();
    for (i, l) in left.chars().enumerate() {
        let mut next = vec![i + 1];
        for (j, r) in right.iter().enumerate() {
            next.push(
                (next[j] + 1)
                    .min(row[j + 1] + 1)
                    .min(row[j] + usize::from(l != *r)),
            );
        }
        row = next;
    }
    row[right.len()]
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slug(Box<str>);

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid slug {0:?}: expected dot-separated segments of [a-z0-9_-], each starting alphanumeric, at most 96 bytes"
)]
pub struct SlugError(String);

impl Slug {
    pub fn as_str(&self) -> &str {
        &self.0
    }

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
