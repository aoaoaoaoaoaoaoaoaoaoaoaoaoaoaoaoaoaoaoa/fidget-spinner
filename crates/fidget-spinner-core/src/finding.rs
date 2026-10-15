use crate::{MetricKey, RegimeNumber, Slug};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum FindingId {
    Unbacked(Slug),
    Unbridged(RegimeNumber),
    Unlisted(RegimeNumber, MetricKey),
    Unread(Slug, MetricKey),
    Unaccepted(Slug, Slug),
}

impl FindingId {
    pub(crate) fn has_subject(&self, target: &crate::RemoveTarget) -> bool {
        use crate::RemoveTarget::{Baseline, Hypothesis, Review, Trial};
        match self {
            Self::Unbacked(h) => matches!(target,Hypothesis(removed) if h==removed),
            Self::Unread(b, _) => matches!(target,Baseline(removed) if b==removed),
            Self::Unaccepted(b, h) => match target {
                Baseline(removed) => b == removed,
                Hypothesis(removed) => h == removed,
                Review(_) | Trial(_) => false,
            },
            Self::Unbridged(_) | Self::Unlisted(_, _) => false,
        }
    }

    pub(crate) fn renamed(self, target: &crate::RenameTarget, to: &Slug) -> Self {
        let hypothesis = |slug| match target {
            crate::RenameTarget::Hypothesis(from) if &slug == from => to.clone(),
            _ => slug,
        };
        let baseline = |slug| match target {
            crate::RenameTarget::Baseline(from) if &slug == from => to.clone(),
            _ => slug,
        };
        match self {
            Self::Unbacked(h) => Self::Unbacked(hypothesis(h)),
            Self::Unread(b, k) => Self::Unread(baseline(b), k),
            Self::Unaccepted(b, h) => Self::Unaccepted(baseline(b), hypothesis(h)),
            Self::Unbridged(n) => Self::Unbridged(n),
            Self::Unlisted(n, k) => Self::Unlisted(n, k),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid finding identity {0:?}")]
pub struct FindingIdError(String);

impl fmt::Display for FindingId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unbacked(hypothesis) => write!(formatter, "unbacked/{hypothesis}"),
            Self::Unbridged(regime) => write!(formatter, "unbridged/{regime}"),
            Self::Unlisted(regime, key) => write!(formatter, "unlisted/{regime}/{key}"),
            Self::Unread(baseline, key) => write!(formatter, "unread/{baseline}/{key}"),
            Self::Unaccepted(baseline, hypothesis) => {
                write!(formatter, "unaccepted/{baseline}/{hypothesis}")
            }
        }
    }
}

impl FromStr for FindingId {
    type Err = FindingIdError;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let invalid = || FindingIdError(raw.to_owned());
        match raw.split('/').collect::<Vec<_>>().as_slice() {
            ["unbacked", hypothesis] => {
                Ok(Self::Unbacked(hypothesis.parse().map_err(|_| invalid())?))
            }
            ["unbridged", regime] => Ok(Self::Unbridged(regime.parse().map_err(|_| invalid())?)),
            ["unlisted", regime, key] => Ok(Self::Unlisted(
                regime.parse().map_err(|_| invalid())?,
                key.parse().map_err(|_| invalid())?,
            )),
            ["unread", baseline, key] => Ok(Self::Unread(
                baseline.parse().map_err(|_| invalid())?,
                key.parse().map_err(|_| invalid())?,
            )),
            ["unaccepted", baseline, hypothesis] => Ok(Self::Unaccepted(
                baseline.parse().map_err(|_| invalid())?,
                hypothesis.parse().map_err(|_| invalid())?,
            )),
            _ => Err(invalid()),
        }
    }
}

impl TryFrom<String> for FindingId {
    type Error = FindingIdError;
    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse()
    }
}
impl From<FindingId> for String {
    fn from(identity: FindingId) -> Self {
        identity.to_string()
    }
}
