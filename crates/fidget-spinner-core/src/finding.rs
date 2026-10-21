use crate::{Frontier, Id, MetricKey, RegimeNumber};

/// A rule's present violation, identified by rule and subject incarnations.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Finding {
    Unsettled(Id),
    Unjudged(Id, Id),
    Nonconforming(Id),
    Unvalidated(Id),
    Stale(Id),
    Superseded(Id),
    Unbridged(RegimeNumber),
    Unlisted(RegimeNumber, MetricKey),
    Unread(Id, MetricKey),
}

impl Finding {
    pub const fn rule(&self) -> &'static str {
        match self {
            Self::Unsettled(_) => "unsettled",
            Self::Unjudged(..) => "unjudged",
            Self::Nonconforming(_) => "nonconforming",
            Self::Unvalidated(_) => "unvalidated",
            Self::Stale(_) => "stale",
            Self::Superseded(_) => "superseded",
            Self::Unbridged(_) => "unbridged",
            Self::Unlisted(..) => "unlisted",
            Self::Unread(..) => "unread",
        }
    }

    pub fn identity(&self, frontier: &Frontier) -> String {
        let name = |id: &Id| frontier.name_of(*id).unwrap_or_else(|| "-".into());
        let rule = self.rule();
        match self {
            Self::Unsettled(id)
            | Self::Nonconforming(id)
            | Self::Unvalidated(id)
            | Self::Stale(id)
            | Self::Superseded(id) => format!("{rule}/{}", name(id)),
            Self::Unjudged(baseline, iset) => format!("{rule}/{}/{}", name(baseline), name(iset)),
            Self::Unbridged(k) => format!("{rule}/{k}"),
            Self::Unlisted(k, key) => format!("{rule}/{k}/{key}"),
            Self::Unread(baseline, key) => format!("{rule}/{}/{key}", name(baseline)),
        }
    }

    pub fn statement(&self, frontier: &Frontier) -> String {
        let name = |id: &Id| {
            frontier
                .name_of(*id)
                .unwrap_or_else(|| "a removed record".into())
        };
        match self {
            Self::Unsettled(id) => format!(
                "{} is judged with an open member or an input not done",
                name(id)
            ),
            Self::Unjudged(baseline, iset) => {
                format!(
                    "the incumbent baseline {} adopted {}, which is neither judged nor void",
                    name(baseline),
                    name(iset)
                )
            }
            Self::Nonconforming(id) => format!("{} violates its schema binding", name(id)),
            Self::Unvalidated(id) => format!(
                "{} requests a schema definition that does not exist",
                name(id)
            ),
            Self::Stale(id) => format!(
                "{}'s specification changed after its assignment was bound",
                name(id)
            ),
            Self::Superseded(id) => format!(
                "{}'s defaulted control is no longer the incumbent baseline",
                name(id)
            ),
            Self::Unbridged(k) => format!("regime {k} has no bridge"),
            Self::Unlisted(k, key) => {
                format!("three or more trials of regime {k} carry the unlisted reading {key}")
            }
            Self::Unread(baseline, key) => format!(
                "the incumbent baseline {} has no reading for KPI {key}",
                name(baseline)
            ),
        }
    }
}
