use std::collections::BTreeSet;

use clap::ValueEnum;
use fidget_spinner_core::*;
use libmcp::DetailLevel;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use trebuchet::inspect::{Part, Route, Window, parts, preview};

pub use trebuchet::inspect::{Cursor, Paging, SnapshotKey};

pub const COLLECTIONS: &[&str] = &[
    "status", "project", "trials", "inputs", "outputs", "reviews", "members", "ops", "history",
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum View {
    #[default]
    Summary,
    Hypotheses,
    Reviews,
    Trials,
    Baselines,
    Regimes,
    Findings,
    Frontiers,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub under: Option<Slug>,
    pub review: Option<Slug>,
    pub docket: Option<Slug>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectScope {
    Project,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Scope {
    Project(ProjectScope),
    Selected(Selection),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DecisionName {
    Accepted,
    Rejected,
    Shelved,
    Candidate,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(untagged)]
pub enum StateFilter {
    Hypothesis(State),
    Review(Completed),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Completed {
    Completed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub state: Option<BTreeSet<StateFilter>>,
    pub disposition: Option<BTreeSet<DecisionName>>,
    pub owner: Option<String>,
    pub lane: Option<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StatusArgs {
    #[serde(default)]
    pub view: View,
    pub scope: Option<Scope>,
    #[serde(default)]
    pub filter: Filter,
    #[serde(flatten)]
    pub window: Window,
}

impl StatusArgs {
    pub fn normalize(&mut self) -> Vec<String> {
        let mut warnings = Vec::new();
        let view = self.view;
        let name = serde_json::to_value(view)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        let fields: [(&str, bool); 5] = [
            (
                "state",
                matches!(view, View::Regimes | View::Baselines | View::Frontiers),
            ),
            (
                "disposition",
                matches!(view, View::Reviews | View::Regimes | View::Frontiers),
            ),
            (
                "owner",
                matches!(view, View::Regimes | View::Baselines | View::Frontiers),
            ),
            ("lane", matches!(view, View::Regimes | View::Frontiers)),
            ("text", view == View::Regimes),
        ];
        for (key, ignored) in fields {
            if !ignored {
                continue;
            }
            let present = match key {
                "state" => self.filter.state.take().is_some(),
                "disposition" => self.filter.disposition.take().is_some(),
                "owner" => self.filter.owner.take().is_some(),
                "lane" => self.filter.lane.take().is_some(),
                "text" => self.filter.text.take().is_some(),
                _ => unreachable!(),
            };
            if present {
                warnings.push(format!("filter.{key} does not apply to {name}; ignored"));
            }
        }
        if (view == View::Regimes && self.scope.is_some())
            || (view == View::Frontiers && matches!(self.scope, Some(Scope::Selected(_))))
        {
            self.scope = None;
            warnings.push(format!("scope does not apply to {name}; ignored"));
        }
        warnings
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Section {
    Claim,
    Trials,
    Disposition,
    Inputs,
    Outputs,
    Reviews,
    Docket,
    Owner,
    Charter,
    Members,
    Lesson,
}

impl Part for Section {
    fn name(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Trials => "trials",
            Self::Disposition => "disposition",
            Self::Inputs => "inputs",
            Self::Outputs => "outputs",
            Self::Reviews => "reviews",
            Self::Docket => "docket",
            Self::Owner => "owner",
            Self::Charter => "charter",
            Self::Members => "members",
            Self::Lesson => "lesson",
        }
    }
    fn paged(self) -> bool {
        matches!(
            self,
            Self::Trials | Self::Inputs | Self::Outputs | Self::Reviews | Self::Members
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadArgs {
    pub hypothesis: Slug,
    pub sections: Option<BTreeSet<Section>>,
    #[serde(flatten)]
    pub window: Window,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReviewArgs {
    pub review: Slug,
    pub sections: Option<BTreeSet<Section>>,
    #[serde(flatten)]
    pub window: Window,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HistoryArgs {
    #[serde(default, deserialize_with = "trebuchet::inspect::sequence")]
    pub seq: Option<i64>,
    #[serde(flatten)]
    pub window: Window,
}

pub struct Lens<'a> {
    pub frontier: &'a Frontier,
    pub assay: Assay,
    pub address: Json,
    pub full: bool,
}

impl<'a> Lens<'a> {
    #[must_use]
    pub fn new(frontier: &'a Frontier, address: Json, detail: DetailLevel) -> Self {
        Self {
            frontier,
            assay: Assay::new(frontier),
            address,
            full: detail == DetailLevel::Full,
        }
    }

    fn route(&self, tool: &str, fields: Json) -> Route {
        let mut arguments = self.address.clone();
        if let (Some(to), Json::Object(from)) = (arguments.as_object_mut(), fields) {
            to.extend(from);
        }
        Route {
            tool: tool.into(),
            arguments,
        }
    }

    fn text(&self, body: &str, tool: &str, fields: Json) -> Json {
        json!(preview(body, self.full, 160, || self.route(tool, fields)))
    }

    pub fn selected(&self, scope: &Option<Scope>) -> Result<BTreeSet<Slug>, String> {
        let all = || self.frontier.hypotheses().keys().cloned().collect();
        let Some(Scope::Selected(selection)) = scope else {
            return Ok(all());
        };
        if usize::from(selection.under.is_some())
            + usize::from(selection.review.is_some())
            + usize::from(selection.docket.is_some())
            > 1
        {
            return Err("scope selects exactly one of under, review and docket".into());
        }
        if let Some(review) = &selection.review {
            return self
                .frontier
                .reviews()
                .get(review)
                .map(|r| r.members.clone())
                .ok_or_else(|| {
                    slug::suggestion(
                        &format!("review {review}"),
                        &slug::nearest(
                            review.as_str(),
                            self.frontier.reviews().keys().map(Slug::as_str),
                        ),
                    )
                });
        }
        if let Some(docket) = &selection.docket {
            return self
                .assay
                .dockets
                .get(docket)
                .map(|d| d.members.clone())
                .ok_or_else(|| {
                    slug::suggestion(
                        &format!("baseline {docket}"),
                        &slug::nearest(
                            docket.as_str(),
                            self.frontier.baselines().keys().map(Slug::as_str),
                        ),
                    )
                });
        }
        Ok(self
            .frontier
            .hypotheses()
            .keys()
            .filter(|h| {
                selection.under.as_ref().is_none_or(|under| {
                    *h == under
                        || h.as_str()
                            .strip_prefix(under.as_str())
                            .is_some_and(|s| s.starts_with('.'))
                })
            })
            .cloned()
            .collect())
    }

    fn hypothesis_matches(&self, slug: &Slug, h: &Hypothesis, filter: &Filter) -> bool {
        filter.state.as_ref().is_none_or(|xs| {
            self.assay
                .hypotheses
                .get(slug)
                .is_some_and(|s| xs.contains(&StateFilter::Hypothesis(*s)))
        }) && filter
            .disposition
            .as_ref()
            .is_none_or(|xs| hypothesis_decision(h).is_some_and(|d| xs.contains(&d)))
            && filter
                .owner
                .as_ref()
                .is_none_or(|owner| h.ownership.owner.as_ref().is_some_and(|o| &o.name == owner))
            && filter.lane.as_ref().is_none_or(|lane| slug.lane() == lane)
            && filter.text.as_ref().is_none_or(|q| {
                hypothesis_text(h, q) || h.trials.values().any(|t| contains(&t.note, q))
            })
    }

    #[must_use]
    pub fn hypothesis_row(&self, slug: &Slug, h: &Hypothesis) -> Json {
        json!({"slug":slug,"state":self.assay.hypotheses.get(slug),"disposition":hypothesis_decision(h),
            "owner":h.ownership.owner.as_ref().map(|o| &o.name),"docket":h.docket,
            "trials":h.trials.len(),"trials_regime":h.trials.values().filter(|t|t.void.is_none()).map(|t|t.regime).max().filter(|n|Some(*n)!=self.frontier.current_regime().map(|r|r.number)),"title":self.text(&h.title,"hypothesis.read",json!({"hypothesis":slug,"sections":["claim"],"detail":"full"}))})
    }

    #[must_use]
    pub fn review_row(&self, slug: &Slug, r: &Review) -> Json {
        let mut counts = Counts::new();
        for h in &r.members {
            if let Some(state) = self.assay.hypotheses.get(h) {
                *counts.entry(*state).or_default() += 1;
            }
        }
        json!({"slug":slug,"state":self.assay.reviews.get(slug),
            "owner":r.ownership.owner.as_ref().map(|o| &o.name),"members":counts,
            "title":self.text(&r.title,"review.read",json!({"review":slug,"sections":["charter"],"detail":"full"}))})
    }

    fn disposition(&self, slug: &Slug, h: &Hypothesis) -> Json {
        h.disposition.as_ref().map_or(Json::Null, |d| json!({
            "decision":d.decision,"at":stamp(d.at,self.full),
            "rationale":self.text(&d.rationale,"hypothesis.read",json!({"hypothesis":slug,"sections":["disposition"],"detail":"full"}))
        }))
    }

    fn readings(&self, regime: RegimeNumber, values: &RecordedReadings, kpi_only: bool) -> Json {
        let gauge = self
            .frontier
            .regimes()
            .iter()
            .find(|r| r.number == regime)
            .map(|r| &r.gauge);
        Json::Object(values.iter().filter(|(key, _)| !kpi_only || gauge.is_some_and(|g|
            g.metrics.get(*key).is_some_and(|m| m.kpi && !m.retired))).map(|(key, value)| {
            let reading = match value {
                Reading::Listed { value } => json!({"value":value,"unit":gauge.and_then(|g| g.metrics.get(key)).map(|m| &m.unit)}),
                Reading::Unlisted { value } => json!({"value":value,"unlisted":true}),
            };
            (key.to_string(), reading)
        }).collect())
    }

    #[must_use]
    pub fn trial_row(&self, slug: &Slug, n: u64, t: &Trial, packet: bool) -> Json {
        let id = TrialId {
            hypothesis: slug.clone(),
            number: n,
        };
        let comparisons = self.assay.comparisons.get(&id).map(|xs| {
            xs.iter()
                .filter(|(key, _)| {
                    packet
                        || self.full
                        || self
                            .frontier
                            .regimes()
                            .iter()
                            .find(|r| r.number == t.regime)
                            .is_some_and(|r| {
                                r.gauge
                                    .metrics
                                    .get(*key)
                                    .is_some_and(|m| m.kpi && !m.retired)
                            })
                })
                .map(|(key, comparison)| {
                    (
                        key,
                        json!({"ratio":comparison.ratio,"movement":comparison.movement}),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        });
        json!({"trial":id,"at":stamp(t.at,self.full),"regime":t.regime,"against":t.against,
        "readings":self.readings(t.regime,&t.readings,!packet&&!self.full),
        "comparisons":comparisons,
        "note":self.text(&t.note,"hypothesis.read",json!({"hypothesis":slug,"sections":["trials"],"detail":"full"})),
        "artifacts":t.artifacts,"void":t.void})
    }

    #[must_use]
    pub fn baseline_row(&self, slug: &Slug, b: &Baseline) -> Json {
        json!({"slug":slug,"regime":b.regime,"epoch":b.epoch,
            "disposition":b.disposition.as_ref().map(|d| &d.decision),"artifacts":b.artifacts,
            "readings":self.readings(b.regime,&b.readings,!self.full),"docket":self.docket(slug),"void":b.void,
            "note":b.note.as_deref().map(|s| self.text(s,"frontier.status",json!({"view":"baselines","detail":"full"}))),
            "adopts":b.adopts,
            "rationale":b.disposition.as_ref().map(|d| self.text(&d.rationale,"frontier.status",json!({"view":"baselines","detail":"full"})))})
    }

    fn docket(&self, slug: &Slug) -> Json {
        self.assay.dockets.get(slug).map_or(
            json!({"settled":0,"total":0}),
            |d| json!({"settled":d.settled,"total":d.members.len()}),
        )
    }

    fn review_matches(&self, slug: &Slug, r: &Review, filter: &Filter) -> bool {
        filter
            .owner
            .as_ref()
            .is_none_or(|owner| r.ownership.owner.as_ref().is_some_and(|o| &o.name == owner))
            && filter.lane.as_ref().is_none_or(|lane| slug.lane() == lane)
            && filter.text.as_ref().is_none_or(|q| {
                contains(&r.title, q)
                    || contains(&r.charter, q)
                    || r.lesson.as_ref().is_some_and(|l| contains(&l.text, q))
            })
            && filter.disposition.is_none()
            && filter.state.as_ref().is_none_or(|xs| {
                xs.contains(&match self.assay.reviews.get(slug) {
                    Some(ReviewState::Running) => StateFilter::Hypothesis(State::Running),
                    Some(ReviewState::Ready) => StateFilter::Hypothesis(State::Ready),
                    Some(ReviewState::Completed) => StateFilter::Review(Completed::Completed),
                    _ => StateFilter::Hypothesis(State::Waiting),
                })
            })
    }

    #[must_use]
    pub fn search(&self, filter: &Filter) -> Vec<Json> {
        let Some(query) = filter.text.as_deref() else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        let mut hit = |record: String, field: &str, body: &str| {
            if let Some(excerpt) = excerpt(body, query) {
                rows.push(json!({"record":record,"field":field,"excerpt":excerpt}));
                true
            } else {
                false
            }
        };
        let mut selection = filter.clone();
        selection.text = None;
        for (slug, h) in self.frontier.hypotheses() {
            if !self.hypothesis_matches(slug, h, &selection) {
                continue;
            }
            for (field, body) in [
                ("title", Some(h.title.as_str())),
                ("mechanism", h.mechanism.as_deref()),
                ("claim", h.claim.as_deref()),
                (
                    "rationale",
                    h.disposition.as_ref().map(|d| d.rationale.as_str()),
                ),
            ] {
                if let Some(body) = body
                    && hit(slug.to_string(), field, body)
                {
                    break;
                }
            }
            for (n, trial) in &h.trials {
                let _ = hit(format!("{slug}#{n}"), "note", &trial.note);
            }
        }
        selection.disposition = None;
        for (slug, r) in self.frontier.reviews() {
            if !self.review_matches(slug, r, &selection) {
                continue;
            }
            for (field, body) in [
                ("title", Some(r.title.as_str())),
                ("charter", Some(r.charter.as_str())),
                ("lesson", r.lesson.as_ref().map(|l| l.text.as_str())),
            ] {
                if let Some(body) = body
                    && hit(slug.to_string(), field, body)
                {
                    break;
                }
            }
        }
        for (slug, b) in self.frontier.baselines() {
            if filter.lane.as_ref().is_none_or(|l| slug.lane() == l)
                && filter
                    .disposition
                    .as_ref()
                    .is_none_or(|xs| xs.contains(&baseline_decision(b)))
                && let Some(disposition) = &b.disposition
            {
                let _ = hit(slug.to_string(), "rationale", &disposition.rationale);
            }
        }
        rows
    }

    fn baseline_selected(
        &self,
        slug: &Slug,
        selected: &BTreeSet<Slug>,
        scope: Option<&Scope>,
    ) -> bool {
        match scope {
            Some(Scope::Selected(s)) if s.docket.is_some() => s.docket.as_ref() == Some(slug),
            Some(Scope::Selected(s)) if s.under.is_some() || s.review.is_some() => self
                .assay
                .dockets
                .get(slug)
                .is_some_and(|d| !d.members.is_disjoint(selected)),
            _ => true,
        }
    }

    fn finding_hypothesis(&self, id: &FindingId) -> Option<(&Slug, &Hypothesis)> {
        match id {
            FindingId::Unbacked(slug) | FindingId::Unaccepted(_, slug) => {
                self.frontier.hypotheses().get_key_value(slug)
            }
            FindingId::Unbridged(_) | FindingId::Unlisted(_, _) | FindingId::Unread(_, _) => None,
        }
    }

    fn finding_baseline(&self, id: &FindingId) -> Option<(&Slug, &Baseline)> {
        match id {
            FindingId::Unread(slug, _) | FindingId::Unaccepted(slug, _) => {
                self.frontier.baselines().get_key_value(slug)
            }
            FindingId::Unbacked(_) | FindingId::Unbridged(_) | FindingId::Unlisted(_, _) => None,
        }
    }

    fn finding_row(&self, id: &FindingId, waiver: Option<&str>) -> Json {
        let statement = match id {
            FindingId::Unbacked(h) => format!("accepted hypothesis {h} has no backing trial"),
            FindingId::Unbridged(n) => format!("regime {n} has no bridge"),
            FindingId::Unlisted(n, key) => {
                format!("regime {n} has at least three trials with unlisted reading {key}")
            }
            FindingId::Unread(b, key) => {
                format!("incumbent baseline {b} has no reading for KPI {key}")
            }
            FindingId::Unaccepted(b, h) => {
                format!("accepted baseline {b} adopts unaccepted hypothesis {h}")
            }
        };
        json!({"identity":id,"statement":statement,"waiver":waiver.map(|s|self.text(s,"frontier.status",json!({"view":"findings","detail":"full"})))})
    }

    fn finding_selected(
        &self,
        id: &FindingId,
        selected: &BTreeSet<Slug>,
        scope: Option<&Scope>,
    ) -> bool {
        if let Some((slug, _)) = self.finding_hypothesis(id) {
            return selected.contains(slug);
        }
        if let Some((slug, _)) = self.finding_baseline(id) {
            return self.baseline_selected(slug, selected, scope);
        }
        true
    }

    fn finding_matches(&self, id: &FindingId, filter: &Filter) -> bool {
        if let Some((slug, h)) = self.finding_hypothesis(id) {
            return self.hypothesis_matches(slug, h, filter);
        }
        if let Some((slug, b)) = self.finding_baseline(id) {
            return filter.state.is_none()
                && filter.owner.is_none()
                && filter.lane.as_ref().is_none_or(|l| slug.lane() == l)
                && filter
                    .disposition
                    .as_ref()
                    .is_none_or(|xs| xs.contains(&baseline_decision(b)))
                && filter.text.as_ref().is_none_or(|q| {
                    b.disposition
                        .as_ref()
                        .is_some_and(|d| contains(&d.rationale, q))
                });
        }
        filter.state.is_none()
            && filter.owner.is_none()
            && filter.lane.is_none()
            && filter.disposition.is_none()
            && filter.text.is_none()
    }

    pub fn status(&self, args: &StatusArgs, paging: &Paging<'_>) -> Result<Json, String> {
        let mut normalized = args.clone();
        let warnings = normalized.normalize();
        let args = &normalized;
        if args.view == View::Summary {
            args.window.validate(false)?;
            let mut result = self.summary(args)?;
            if !warnings.is_empty() {
                result["ignored"] = json!(warnings);
            }
            return Ok(result);
        }
        if args.view == View::Frontiers {
            return Err("frontiers is a project view".into());
        }
        let selected = self.selected(&args.scope)?;
        let h: Vec<_> = self
            .frontier
            .hypotheses()
            .iter()
            .filter(|(slug, _)| selected.contains(*slug))
            .collect();
        let scoped = matches!(&args.scope,Some(Scope::Selected(s)) if s.under.is_some()||s.review.is_some()||s.docket.is_some());
        let mut denominator = 0;
        let mut rows = Vec::new();
        match args.view {
            View::Hypotheses => {
                denominator = h.len();
                rows = h
                    .into_iter()
                    .filter(|(s, h)| self.hypothesis_matches(s, h, &args.filter))
                    .map(|(s, h)| self.hypothesis_row(s, h))
                    .collect();
            }
            View::Reviews => {
                let reviews: Vec<_> = self
                    .frontier
                    .reviews()
                    .iter()
                    .filter(|(_, r)| !scoped || !r.members.is_disjoint(&selected))
                    .collect();
                denominator = reviews.len();
                rows = reviews
                    .into_iter()
                    .filter(|(s, r)| self.review_matches(s, r, &args.filter))
                    .map(|(s, r)| self.review_row(s, r))
                    .collect();
            }
            View::Trials => {
                let mut trials: Vec<_> = h
                    .into_iter()
                    .flat_map(|(s, h)| h.trials.iter().map(move |(n, t)| (s, h, *n, t)))
                    .collect();
                denominator = trials.len();
                trials.sort_by_key(|(s, _, n, t)| (std::cmp::Reverse(t.at), (*s).clone(), *n));
                rows = trials
                    .into_iter()
                    .filter(|(s, h, _, t)| {
                        let mut filter = args.filter.clone();
                        filter.text = None;
                        self.hypothesis_matches(s, h, &filter)
                            && args
                                .filter
                                .text
                                .as_ref()
                                .is_none_or(|q| contains(&t.note, q) || hypothesis_text(h, q))
                    })
                    .map(|(s, _, n, t)| self.trial_row(s, n, t, false))
                    .collect();
            }
            View::Baselines => {
                let baselines: Vec<_> = self
                    .frontier
                    .baselines()
                    .iter()
                    .filter(|(s, _)| self.baseline_selected(s, &selected, args.scope.as_ref()))
                    .collect();
                denominator = baselines.len();
                rows = baselines
                    .into_iter()
                    .filter(|(s, b)| {
                        args.filter.state.is_none()
                            && args.filter.owner.is_none()
                            && args.filter.lane.as_ref().is_none_or(|l| s.lane() == l)
                            && args
                                .filter
                                .disposition
                                .as_ref()
                                .is_none_or(|xs| xs.contains(&baseline_decision(b)))
                            && args.filter.text.as_ref().is_none_or(|q| {
                                b.disposition
                                    .as_ref()
                                    .is_some_and(|d| contains(&d.rationale, q))
                            })
                    })
                    .map(|(s, b)| self.baseline_row(s, b))
                    .collect();
            }
            View::Regimes => {
                denominator = self.frontier.regimes().len();
                rows=self.frontier.regimes().iter().rev().map(|r|json!({"number":r.number,
                    "reason":r.reason.as_deref().map(|s|self.text(s,"frontier.status",json!({"view":"regimes","detail":"full"}))),
                    "bridge":r.bridge,"contract":self.text(&r.gauge.contract,"frontier.status",json!({"view":"regimes","detail":"full"})),
                    "metrics":r.gauge.metrics})).collect();
            }
            View::Findings => {
                let findings: Vec<_> = self
                    .assay
                    .findings
                    .iter()
                    .filter(|(id, _)| self.finding_selected(id, &selected, args.scope.as_ref()))
                    .collect();
                denominator = findings.len();
                rows = findings
                    .into_iter()
                    .filter(|(id, _)| self.finding_matches(id, &args.filter))
                    .map(|(id, waiver)| self.finding_row(id, waiver.as_deref()))
                    .collect();
            }
            View::Summary | View::Frontiers => {}
        }
        let query = semantic(args);
        let page = paging.page(rows, &query, "status", &args.window, 20, |r| r)?;
        Ok(
            json!({"scope_total":denominator,"items":page.items,"total":page.total,"next_cursor":page.next_cursor,"ignored":warnings}),
        )
    }

    fn summary(&self, args: &StatusArgs) -> Result<Json, String> {
        let selected = self.selected(&args.scope)?;
        let hs: Vec<_> = self
            .frontier
            .hypotheses()
            .iter()
            .filter(|(s, h)| selected.contains(*s) && self.hypothesis_matches(s, h, &args.filter))
            .collect();
        let reviews: Vec<_> = self
            .frontier
            .reviews()
            .iter()
            .filter(|(s, r)| {
                r.members.iter().any(|h| selected.contains(h))
                    && self.review_matches(s, r, &args.filter)
            })
            .collect();
        let mut counts = Counts::new();
        for slug in &selected {
            if let Some(state) = self.assay.hypotheses.get(slug) {
                *counts.entry(*state).or_default() += 1;
            }
        }
        let mut dispositions: Vec<_> = hs
            .iter()
            .filter_map(|(s, h)| h.disposition.as_ref().map(|d| (*s, *h, d)))
            .collect();
        dispositions.sort_by_key(|(s, _, d)| (std::cmp::Reverse(d.at), (*s).clone()));
        let mut lessons: Vec<_> = reviews
            .iter()
            .filter_map(|(s, r)| r.lesson.as_ref().map(|l| (*s, l)))
            .collect();
        lessons.sort_by_key(|(s, l)| (std::cmp::Reverse(l.at), (*s).clone()));
        let list = |items: Vec<Json>, view: &str, state: Option<&str>| {
            let mut filter = json!(args.filter);
            if let Some(state) = state {
                filter["state"] = json!([state]);
            }
            summary_list(
                items,
                self.route(
                    "frontier.status",
                    json!({"view":view,"scope":args.scope,"filter":filter}),
                ),
            )
        };
        let baseline = self
            .assay
            .incumbent_baseline
            .as_ref()
            .and_then(|s| self.frontier.baselines().get(s).map(|b| (s, b)));
        let regime = self.frontier.current_regime().map(|r| {
            let first = r.gauge.contract.lines().next().unwrap_or("");
            let contract = if self.full {
                json!(r.gauge.contract)
            } else if first == r.gauge.contract && first.chars().count() <= 160 {
                json!(first)
            } else {
                json!(trebuchet::inspect::Text::Preview {
                    preview: first.chars().take(160).collect(),
                    full_read: self.route(
                        "frontier.status",
                        json!({"view":"regimes","limit":1,"detail":"full"})
                    )
                })
            };
            json!({"number":r.number,"contract":contract})
        });
        let kpis=self.frontier.current_regime().map(|r|list(
            r.gauge.metrics.iter().filter(|(_,m)|m.kpi&&!m.retired).map(|(key,metric)| {
                let incumbent=baseline.and_then(|(_,b)|b.readings.get(key)).and_then(|reading|match reading {
                    Reading::Listed{value}=>Some(*value),Reading::Unlisted{..}=>None,
                });
                json!({"key":key,"unit":metric.unit,"better":metric.better,"incumbent":incumbent,"refs":metric.refs})
            }).collect(),"regimes",None));
        let incumbent =
            baseline.map(|(slug, b)| json!({"slug":slug,"epoch":b.epoch,"artifacts":b.artifacts}));
        let state_h = |state| {
            hs.iter()
                .filter(|(s, _)| self.assay.hypotheses.get(*s) == Some(&state))
                .map(|(s, h)| {
                    let mut row = self.hypothesis_row(s, h);
                    if let Some(fields) = row.as_object_mut() {
                        let _ = fields.shift_remove("state");
                        let _ = fields.shift_remove("disposition");
                        if state == State::Ready {
                            let _ = fields.shift_remove("trials");
                        }
                    }
                    row
                })
                .collect::<Vec<_>>()
        };
        let state_r = |state| {
            reviews
                .iter()
                .filter(|(s, _)| self.assay.reviews.get(*s) == Some(&state))
                .map(|(s, r)| {
                    let mut row = self.review_row(s, r);
                    if let Some(fields) = row.as_object_mut() {
                        let _ = fields.shift_remove("state");
                    }
                    row
                })
                .collect::<Vec<_>>()
        };
        Ok(
            json!({"objective":self.text(self.frontier.objective().unwrap_or(""),"frontier.status",json!({"detail":"full"})),
        "conclusion":self.frontier.conclusion().map(|l|self.text(&l.text,"frontier.status",json!({"detail":"full"}))),
        "counts":counts,"regime":regime,"incumbent_baseline":incumbent,"kpis":kpis,
        "dockets":list(self.frontier.baselines().iter().filter(|(s,b)|(b.disposition.is_none()||self.assay.incumbent_baseline.as_ref()==Some(*s))&&self.baseline_selected(s,&selected,args.scope.as_ref()))
            .filter(|(s,_)|self.assay.dockets.get(*s).is_some_and(|d|!d.members.is_empty())).map(|(s,_)|json!({"baseline":s,"progress":self.docket(s)})).collect(),"baselines",None),
        "running":{"hypotheses":list(state_h(State::Running),"hypotheses",Some("running")),"reviews":list(state_r(ReviewState::Running),"reviews",Some("running"))},
        "tested":list(state_h(State::Tested),"hypotheses",Some("tested")),
        "ready":{"hypotheses":list(state_h(State::Ready),"hypotheses",Some("ready")),"reviews":list(state_r(ReviewState::Ready),"reviews",Some("ready"))},
        "dispositions":list(dispositions.into_iter().map(|(s,h,d)|json!({"hypothesis":s,"disposition":hypothesis_decision(h),"rationale":preview(&d.rationale,self.full,120,||self.route("hypothesis.read",json!({"hypothesis":s,"sections":["disposition"]})))})).collect(),"hypotheses",None),
        "lessons":list(lessons.into_iter().map(|(s,l)|json!({"review":s,"at":stamp(l.at,self.full),
            "text":self.text(&l.text,"review.read",json!({"review":s,"sections":["lesson"],"detail":"full"}))})).collect(),"reviews",Some("completed")),
        "findings":list(self.assay.findings.iter().filter(|(id,w)|w.is_none()&&self.finding_selected(id,&selected,args.scope.as_ref())&&self.finding_matches(id,&args.filter)).map(|(id,_)|self.finding_row(id,None)).collect(),"findings",None)}),
        )
    }

    pub fn hypothesis(&self, args: &ReadArgs, paging: &Paging<'_>) -> Result<Json, String> {
        let h = self
            .frontier
            .hypotheses()
            .get(&args.hypothesis)
            .ok_or_else(|| {
                slug::suggestion(
                    &format!("hypothesis {}", args.hypothesis),
                    &slug::nearest(
                        args.hypothesis.as_str(),
                        self.frontier.hypotheses().keys().map(Slug::as_str),
                    ),
                )
            })?;
        let defaults = [
            Section::Claim,
            Section::Trials,
            Section::Disposition,
            Section::Inputs,
            Section::Outputs,
            Section::Reviews,
            Section::Docket,
            Section::Owner,
        ];
        let sections = parts(args.sections.as_ref(), &defaults, &args.window, COLLECTIONS)?;
        if sections.iter().any(|s| !defaults.contains(s)) {
            return Err("invalid hypothesis section".into());
        }
        let mut result = json!({"hypothesis":args.hypothesis,"state":self.assay.hypotheses.get(&args.hypothesis)});
        let query = json!({"hypothesis":args.hypothesis});
        for section in sections {
            let value = match section {
                Section::Claim => json!({"title":h.title,"mechanism":h.mechanism,"claim":h.claim}),
                Section::Disposition => h.disposition.as_ref().map_or(
                    Json::Null,
                    |d| json!({"decision":d.decision,"rationale":d.rationale,"at":stamp(d.at,self.full)}),
                ),
                Section::Owner => ownership(&h.ownership,self.full),
                Section::Docket => h.docket.as_ref().map_or(
                    Json::Null,
                    |b| json!({"baseline":b,"progress":self.docket(b)}),
                ),
                Section::Trials => {
                    let mut rows: Vec<_> = h.trials.iter().collect();
                    rows.sort_by_key(|(n, t)| (std::cmp::Reverse(t.at), std::cmp::Reverse(**n)));
                    json!(paging.page(
                        rows,
                        &query,
                        section.name(),
                        &args.window,
                        20,
                        |(n, t)| self.trial_row(&args.hypothesis, *n, t, true)
                    )?)
                }
                Section::Inputs | Section::Outputs => {
                    let rows: Vec<_> = self
                        .frontier
                        .edges()
                        .iter()
                        .filter_map(|((from, to), edge)| {
                            let other = if section == Section::Inputs && to == &args.hypothesis {
                                from
                            } else if section == Section::Outputs && from == &args.hypothesis {
                                to
                            } else {
                                return None;
                            };
                            self.frontier.hypotheses().get(other).map(|h| {
                                json!({"hypothesis":other,"state":self.assay.hypotheses.get(other),
                            "disposition":self.disposition(other,h),"carries":edge.carries})
                            })
                        })
                        .collect();
                    json!(paging.page(rows, &query, section.name(), &args.window, 20, |r| r)?)
                }
                Section::Reviews => {
                    let rows:Vec<_>=self.frontier.reviews().iter().filter(|(_,r)|r.members.contains(&args.hypothesis))
                        .map(|(s,r)|json!({"review":s,"state":self.assay.reviews.get(s),
                            "lesson":r.lesson.as_ref().map(|l|self.text(&l.text,"review.read",json!({"review":s,"sections":["lesson"],"detail":"full"})))})).collect();
                    json!(paging.page(rows, &query, section.name(), &args.window, 20, |r| r)?)
                }
                Section::Charter | Section::Members | Section::Lesson => unreachable!(),
            };
            result[section.name()] = value;
        }
        Ok(result)
    }

    pub fn review(&self, args: &ReviewArgs, paging: &Paging<'_>) -> Result<Json, String> {
        let r = self.frontier.reviews().get(&args.review).ok_or_else(|| {
            slug::suggestion(
                &format!("review {}", args.review),
                &slug::nearest(
                    args.review.as_str(),
                    self.frontier.reviews().keys().map(Slug::as_str),
                ),
            )
        })?;
        let defaults = [
            Section::Charter,
            Section::Members,
            Section::Lesson,
            Section::Owner,
        ];
        let sections = parts(args.sections.as_ref(), &defaults, &args.window, COLLECTIONS)?;
        if sections.iter().any(|s| !defaults.contains(s)) {
            return Err("invalid review section".into());
        }
        let mut result = json!({"review":args.review,"state":self.assay.reviews.get(&args.review)});
        for section in sections {
            let value = match section {
                Section::Charter => json!({"title":r.title,"charter":r.charter}),
                Section::Lesson => r.lesson.as_ref().map_or(
                    Json::Null,
                    |l| json!({"text":l.text,"at":stamp(l.at,self.full)}),
                ),
                Section::Owner => ownership(&r.ownership, self.full),
                Section::Members => {
                    let rows:Vec<_>=r.members.iter().filter_map(|s|self.frontier.hypotheses().get(s).map(|h|
                        json!({"slug":s,"state":self.assay.hypotheses.get(s),"disposition":hypothesis_decision(h),"rationale":h.disposition.as_ref().map(|d|preview(&d.rationale,self.full,120,||self.route("hypothesis.read",json!({"hypothesis":s,"sections":["disposition"],"detail":"full"}))))}))).collect();
                    json!(paging.page(
                        rows,
                        &json!({"review":args.review}),
                        section.name(),
                        &args.window,
                        20,
                        |r| r
                    )?)
                }
                _ => unreachable!(),
            };
            result[section.name()] = value;
        }
        Ok(result)
    }
}

fn hypothesis_decision(h: &Hypothesis) -> Option<DecisionName> {
    h.disposition.as_ref().map(|d| match d.decision {
        HypothesisDecision::Accepted { .. } => DecisionName::Accepted,
        HypothesisDecision::Rejected => DecisionName::Rejected,
        HypothesisDecision::Shelved => DecisionName::Shelved,
    })
}
fn baseline_decision(b: &Baseline) -> DecisionName {
    b.disposition
        .as_ref()
        .map_or(DecisionName::Candidate, |d| match d.decision {
            BaselineDecision::Accepted => DecisionName::Accepted,
            BaselineDecision::Rejected => DecisionName::Rejected,
        })
}
fn hypothesis_text(h: &Hypothesis, q: &str) -> bool {
    contains(&h.title, q)
        || h.mechanism.as_deref().is_some_and(|s| contains(s, q))
        || h.claim.as_deref().is_some_and(|s| contains(s, q))
        || h.disposition
            .as_ref()
            .is_some_and(|d| contains(&d.rationale, q))
}
fn contains(body: &str, query: &str) -> bool {
    body.to_lowercase().contains(&query.to_lowercase())
}
fn excerpt(body: &str, query: &str) -> Option<String> {
    let mut folded = String::new();
    let mut positions = Vec::new();
    for (index, ch) in body.chars().enumerate() {
        for lower in ch.to_lowercase() {
            folded.push(lower);
            positions.extend(std::iter::repeat_n(index, lower.len_utf8()));
        }
    }
    let at = folded.find(&query.to_lowercase())?;
    let before = positions.get(at).copied().unwrap_or(0);
    let chars: Vec<_> = body.chars().collect();
    let margin = (120 - query.chars().count().min(120)) / 2;
    let start = before.saturating_sub(margin);
    Some(
        chars[start..(start + 120).min(chars.len())]
            .iter()
            .collect(),
    )
}

#[must_use]
pub fn semantic(args: &StatusArgs) -> Json {
    json!({"view":args.view,"scope":args.scope,"filter":args.filter})
}

fn ownership(ownership: &Ownership, full: bool) -> Json {
    json!({"owner":ownership.owner.as_ref().map(|a|json!({"name":a.name,"instructions":a.instructions,"at":stamp(a.at,full)})),
        "started":ownership.started.map(|at|stamp(at,full))})
}

#[must_use]
#[expect(
    clippy::expect_used,
    reason = "Persisted timestamps enter through RFC3339 or current UTC."
)]
pub fn stamp(at: time::OffsetDateTime, full: bool) -> String {
    if full {
        at.format(&time::format_description::well_known::Rfc3339)
            .expect("ledger timestamp must fit RFC3339")
    } else {
        let at = at.to_offset(time::UtcOffset::UTC);
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}Z",
            at.year(),
            u8::from(at.month()),
            at.day(),
            at.hour(),
            at.minute()
        )
    }
}

fn summary_list(mut items: Vec<Json>, read: Route) -> Json {
    let total = items.len();
    let limit = match read.arguments["view"].as_str() {
        Some("regimes") => 8,
        Some("findings") => 3,
        _ => 2,
    };
    items.truncate(limit);
    let mut result = json!({"items":items,"total":total});
    if total > limit {
        let Route { tool, arguments } = read;
        result["read"] = json!({"tool":tool,"arguments":arguments});
    }
    result
}
