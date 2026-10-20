use std::collections::{BTreeMap, BTreeSet};

use clap::ValueEnum;
use fidget_spinner_core::{
    schema::{Show, Slot},
    *,
};
use libmcp::DetailLevel;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, json};
use trebuchet::inspect::{Part, Route, Window, parts, preview};

pub use trebuchet::inspect::{Cursor, Paging, SnapshotKey};

pub const COLLECTIONS: &[&str] = &[
    "status",
    "project",
    "plans",
    "members",
    "isets",
    "inputs",
    "dependents",
    "elections",
    "bound",
    "ops",
    "history",
];
/// The collections a record packet pages.
pub const READ_COLLECTIONS: &[&str] = &[
    "plans",
    "members",
    "elections",
    "isets",
    "inputs",
    "dependents",
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum View {
    #[default]
    Summary,
    Isets,
    Trials,
    Baselines,
    Regimes,
    Schemas,
    Findings,
    Frontiers,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectScope {
    Project,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum StateName {
    Waiting,
    Ready,
    Running,
    Tested,
    Reported,
    Settled,
    Void,
}

impl StateName {
    const fn shown(self) -> Shown {
        match self {
            Self::Waiting => Shown::State(State::Waiting),
            Self::Ready => Shown::State(State::Ready),
            Self::Running => Shown::State(State::Running),
            Self::Tested => Shown::State(State::Tested),
            Self::Reported => Shown::State(State::Reported),
            Self::Settled => Shown::State(State::Settled),
            Self::Void => Shown::Void,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub under: Option<Slug>,
    pub lane: Option<String>,
    pub state: Option<BTreeSet<StateName>>,
    pub owner: Option<String>,
    pub schema: Option<Slug>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StatusArgs {
    #[serde(default)]
    pub view: View,
    pub scope: Option<ProjectScope>,
    #[serde(default)]
    pub filter: Filter,
    #[serde(flatten)]
    pub window: Window,
}

impl StatusArgs {
    /// Drop selectors a view ignores, naming each.
    pub fn normalize(&mut self) -> Vec<String> {
        let applies: &[&str] = match self.view {
            View::Isets | View::Trials => &["under", "lane", "state", "owner", "schema", "text"],
            View::Summary | View::Frontiers => &["text"],
            View::Baselines => &["lane", "text"],
            View::Regimes | View::Schemas | View::Findings => &[],
        };
        let name = serde_json::to_value(self.view)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default();
        let f = &mut self.filter;
        let mut warnings = Vec::new();
        let mut drop = |key: &str, present: bool| {
            if present && !applies.contains(&key) {
                warnings.push(format!("filter.{key} does not apply to {name}; ignored"));
                true
            } else {
                false
            }
        };
        if drop("under", f.under.is_some()) {
            f.under = None;
        }
        if drop("lane", f.lane.is_some()) {
            f.lane = None;
        }
        if drop("state", f.state.is_some()) {
            f.state = None;
        }
        if drop("owner", f.owner.is_some()) {
            f.owner = None;
        }
        if drop("schema", f.schema.is_some()) {
            f.schema = None;
        }
        if drop("text", f.text.is_some()) {
            f.text = None;
        }
        if self.scope.is_some()
            && !matches!(
                self.view,
                View::Summary | View::Isets | View::Trials | View::Baselines | View::Frontiers
            )
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
    Head,
    Data,
    Plans,
    Report,
    Members,
    Judgment,
    Elections,
    Isets,
    Inputs,
    Dependents,
    Owner,
}

impl Part for Section {
    fn name(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Data => "data",
            Self::Plans => "plans",
            Self::Report => "report",
            Self::Members => "members",
            Self::Judgment => "judgment",
            Self::Elections => "elections",
            Self::Isets => "isets",
            Self::Inputs => "inputs",
            Self::Dependents => "dependents",
            Self::Owner => "owner",
        }
    }
    fn paged(self) -> bool {
        matches!(
            self,
            Self::Plans
                | Self::Members
                | Self::Elections
                | Self::Isets
                | Self::Inputs
                | Self::Dependents
        )
    }
}

const TRIAL_SECTIONS: &[Section] = &[
    Section::Head,
    Section::Plans,
    Section::Report,
    Section::Isets,
    Section::Inputs,
    Section::Dependents,
    Section::Owner,
];
const ISET_SECTIONS: &[Section] = &[
    Section::Head,
    Section::Data,
    Section::Members,
    Section::Judgment,
    Section::Isets,
    Section::Inputs,
    Section::Dependents,
    Section::Owner,
];
const BASELINE_SECTIONS: &[Section] = &[Section::Head, Section::Elections, Section::Dependents];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadArgs {
    pub work: Option<Name>,
    pub baseline: Option<Slug>,
    pub sections: Option<BTreeSet<Section>>,
    #[serde(flatten)]
    pub window: Window,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaArgs {
    pub schema: Option<Slug>,
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
    pub address: Json,
    pub full: bool,
    findings: BTreeSet<Finding>,
}

pub fn semantic(args: &StatusArgs) -> Json {
    json!({"view":args.view,"scope":args.scope,"filter":args.filter})
}

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

pub fn contains(body: &str, query: &str) -> bool {
    body.to_lowercase().contains(&query.to_lowercase())
}

pub fn excerpt(body: &str, query: &str) -> Option<String> {
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

pub fn carrier_name(carrier: Carrier) -> &'static str {
    carrier.as_str()
}

fn bounded(mut items: Vec<Json>, limit: usize, read: Option<Route>) -> Json {
    let total = items.len();
    items.truncate(limit);
    let mut result = json!({"items":items,"total":total});
    if total > limit
        && let Some(Route { tool, arguments }) = read
    {
        result["read"] = json!({"tool":tool,"arguments":arguments});
    }
    result
}

fn counts_json(counts: &Counts, carrier: Carrier) -> Json {
    Json::Object(
        counts
            .iter()
            .filter(|((c, _), _)| *c == carrier)
            .map(|((_, shown), n)| (shown.as_str().to_owned(), json!(n)))
            .collect(),
    )
}

impl<'a> Lens<'a> {
    pub fn new(frontier: &'a Frontier, address: Json, detail: DetailLevel) -> Self {
        Self {
            frontier,
            address,
            full: detail == DetailLevel::Full,
            findings: frontier.findings(),
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

    fn read_route(&self, id: Id, section: &str) -> Json {
        match self.frontier.kind_of(id) {
            Some(Kind::Baseline) => {
                json!({"baseline":self.frontier.name_of(id),"sections":[section],"detail":"full"})
            }
            _ => json!({"work":self.frontier.name_of(id),"sections":[section],"detail":"full"}),
        }
    }

    pub fn name(&self, id: Id) -> Json {
        self.frontier.name_of(id).map_or_else(
            || {
                let kind = self.frontier.kind_of(id).map_or("record", Kind::as_str);
                json!(format!("removed {kind}"))
            },
            Json::String,
        )
    }

    fn names(&self, ids: impl IntoIterator<Item = Id>) -> Json {
        Json::Array(ids.into_iter().map(|id| self.name(id)).collect())
    }

    /// Data as JSON, previewed as compact text when long under concise detail.
    pub fn data(&self, data: &Data, id: Id, section: &str) -> Json {
        let name = |r: &Ref| self.frontier.ref_name(r);
        if !self.full {
            let compact = data.compact(&name);
            if compact.chars().count() > 600 {
                return json!(preview(&compact, false, 600, || self
                    .route("record.read", self.read_route(id, section))));
            }
        }
        data.json(&name)
    }

    pub fn binding(&self, binding: Option<&Binding>) -> Json {
        match binding {
            None => json!("untyped"),
            Some(Binding::Exact { id, at }) => json!(format!("{id}@{at}")),
            Some(Binding::Requested { id, rev }) => {
                json!({"requested": rev.map_or_else(|| id.to_string(), |rev| format!("{id}@{rev}")), "unvalidated": true})
            }
        }
    }

    fn schema_of(&self, work: &Work) -> Option<&Schema> {
        work.binding
            .as_ref()
            .and_then(|b| self.frontier.definition(b))
            .map(|d| &d.schema)
    }

    /// A schema's headline field, else a `title` or `note` text field.
    pub fn headline(&self, id: Id) -> Option<String> {
        let work = self.frontier.work().get(&id)?;
        let data = match &work.body {
            Body::Iset(iset) => Some(&iset.data),
            Body::Trial(trial) => trial
                .plan()
                .map(|p| &p.data)
                .filter(|d| d.fields().is_some_and(|f| !f.is_empty()))
                .or_else(|| trial.report.as_ref().map(|r| &r.data)),
        }?;
        let named: Vec<String> = self
            .schema_of(work)
            .map(|s| {
                s.shown(Show::Headline)
                    .into_iter()
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        named
            .iter()
            .map(String::as_str)
            .chain(["title", "note", "question", "statement"])
            .find_map(|field| data.text(field))
    }

    fn headline_json(&self, id: Id) -> Json {
        self.headline(id).map_or(Json::Null, |text| {
            json!(preview(&text, self.full, 120, || self
                .route("record.read", self.read_route(id, "head"))))
        })
    }

    pub fn badges(&self, work: &Work) -> Json {
        let Some(schema) = self.schema_of(work) else {
            return Json::Null;
        };
        let Some(data) = work.data() else {
            return Json::Null;
        };
        let badges: Map<String, Json> = schema
            .shown(Show::Badge)
            .into_iter()
            .filter_map(|field| data.text(field).map(|text| (field.to_owned(), json!(text))))
            .collect();
        if badges.is_empty() {
            Json::Null
        } else {
            Json::Object(badges)
        }
    }

    pub fn shown(&self, id: Id) -> Json {
        self.frontier
            .shown(id)
            .map_or(Json::Null, |s| json!(s.as_str()))
    }

    /// Facts views show beside a record: void readings, resting on void, removed sources, validity.
    pub fn marks(&self, id: Id) -> Vec<String> {
        let mut marks = Vec::new();
        let f = self.frontier;
        if let Some(work) = f.work().get(&id) {
            if work.void.is_some() {
                marks.push(
                    if f.invalidated(id) {
                        "invalidated"
                    } else {
                        "withdrawn"
                    }
                    .to_owned(),
                );
            }
            let consumes = match &work.body {
                Body::Trial(t) => t.report.as_ref().map(|r| &r.consumes),
                Body::Iset(i) => i.judgment.as_ref().map(|j| &j.consumes),
            };
            if let Some(consumes) = consumes {
                let void = f.void_sources(consumes);
                if !void.is_empty() {
                    marks.push(format!(
                        "rests on void {}",
                        void.iter()
                            .filter_map(|v| f.name_of(*v))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if f.removed_source(consumes) {
                    marks.push("source removed".into());
                }
            }
            match &work.binding {
                Some(Binding::Requested { .. }) => marks.push("unvalidated".into()),
                Some(_) if !f.violations(id).is_empty() => marks.push("nonconforming".into()),
                _ => {}
            }
        }
        marks
    }

    fn owner(work: &Work) -> Json {
        work.owner
            .as_ref()
            .map(|o| &o.name)
            .map_or(Json::Null, |o| json!(o))
    }

    fn readings(
        &self,
        regime: Option<RegimeNumber>,
        values: &RecordedReadings,
        kpi_only: bool,
    ) -> Json {
        let gauge = regime
            .and_then(|r| self.frontier.regime(r))
            .map(|r| &r.gauge);
        Json::Object(
            values
                .iter()
                .filter(|(key, _)| {
                    !kpi_only || gauge.is_some_and(|g| g.metrics.get(*key).is_some_and(|m| m.kpi && !m.retired))
                })
                .map(|(key, value)| {
                    let reading = match value {
                        Reading::Listed(v) => json!({"value":v,"unit":gauge.and_then(|g| g.metrics.get(key)).map(|m| m.unit.as_str())}),
                        Reading::Unlisted(v) => json!({"value":v,"unlisted":true}),
                    };
                    (key.to_string(), reading)
                })
                .collect(),
        )
    }

    /// Ratios of treatment to control observations under the trial's regime.
    pub fn comparisons(&self, trial: &Trial, kpi_only: bool) -> Json {
        let Some(report) = &trial.report else {
            return Json::Null;
        };
        let Some(regime) = trial.regime.and_then(|k| self.frontier.regime(k)) else {
            return Json::Null;
        };
        let control = report.control_readings.as_ref().or_else(|| {
            report
                .control
                .and_then(|c| self.frontier.baselines().get(&c.baseline))
                .filter(|b| b.regime == trial.regime && b.void.is_none())
                .map(|b| &b.readings)
        });
        let Some(control) = control else {
            return Json::Null;
        };
        let rows: Map<String, Json> = report
            .readings
            .iter()
            .filter_map(|(key, reading)| {
                let metric = regime.gauge.metrics.get(key).filter(|m| !m.retired)?;
                if kpi_only && !metric.kpi {
                    return None;
                }
                let (Reading::Listed(t), Some(Reading::Listed(c))) = (reading, control.get(key))
                else {
                    return None;
                };
                if *c == 0.0 {
                    return None;
                }
                let ratio = t / c;
                let movement = match metric.better {
                    None => "undirected",
                    Some(_) if (ratio - 1.0).abs() < f64::EPSILON => "same",
                    Some(Direction::Lower) if ratio < 1.0 => "better",
                    Some(Direction::Higher) if ratio > 1.0 => "better",
                    Some(_) => "worse",
                };
                Some((
                    key.to_string(),
                    json!({"ratio":(ratio * 1000.0).round() / 1000.0,"movement":movement}),
                ))
            })
            .collect();
        if rows.is_empty() {
            Json::Null
        } else {
            Json::Object(rows)
        }
    }

    fn control(&self, control: Option<Control>) -> Json {
        control.map_or(Json::Null, |c| {
            let name = self.name(c.baseline);
            if c.defaulted {
                json!(format!("{} (defaulted)", name.as_str().unwrap_or_default()))
            } else {
                name
            }
        })
    }

    fn treatment(&self, treatment: Option<&Treatment>) -> Json {
        match treatment {
            None => Json::Null,
            Some(Treatment::Baseline(id)) => json!({"baseline":self.name(*id)}),
            Some(Treatment::Identity(text)) => json!({"identity":text}),
        }
    }

    pub fn iset_row(&self, id: Id, work: &Work, iset: &Iset) -> Json {
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for member in &iset.members {
            if let Some(shown) = self.frontier.shown(*member) {
                *counts.entry(shown.as_str()).or_default() += 1;
            }
        }
        json!({"name":work.name,"state":self.shown(id),"owner":Self::owner(work),"members":counts,
            "schema":work.binding.as_ref().map(|b| self.binding(Some(b))),"headline":self.headline_json(id),
            "badges":self.badges(work),"marks":self.marks(id)})
    }

    pub fn trial_row(&self, id: Id, work: &Work, trial: &Trial) -> Json {
        let control = trial
            .report
            .as_ref()
            .map_or_else(|| trial.plan().and_then(|p| p.control), |r| r.control);
        json!({"name":work.name,"state":self.shown(id),"owner":Self::owner(work),"regime":trial.regime,
            "control":self.control(control),
            "readings":trial.report.as_ref().map(|r| self.readings(trial.regime,&r.readings,!self.full)),
            "comparisons":self.comparisons(trial,!self.full),
            "schema":work.binding.as_ref().map(|b| self.binding(Some(b))),"headline":self.headline_json(id),
            "marks":self.marks(id)})
    }

    pub fn baseline_row(&self, id: Id, b: &Baseline) -> Json {
        let elections: Vec<String> = self
            .frontier
            .elections()
            .iter()
            .filter(|e| e.baseline == id)
            .map(|e| stamp(e.at.time, self.full))
            .collect();
        let incumbent = self.frontier.incumbent().map(|(_, b)| b) == Some(id);
        json!({"slug":b.slug,"regime":b.regime,"elected":elections,"incumbent":incumbent.then_some(true),
            "frozen":Self::frozen(b),"artifacts":b.artifacts,"readings":self.readings(b.regime,&b.readings,!self.full),
            "void":b.void.as_ref().map(|v| self.text(&v.reason,"record.read",json!({"baseline":b.slug,"sections":["head"],"detail":"full"}))),
            "note":b.note.as_deref().map(|n| self.text(n,"record.read",json!({"baseline":b.slug,"sections":["head"],"detail":"full"})))})
    }

    fn frozen(b: &Baseline) -> Json {
        let parts: Vec<String> = [
            ("identity", b.identity_frozen),
            ("readings", b.readings_frozen),
        ]
        .into_iter()
        .filter_map(|(part, at)| at.map(|at| format!("{part} at {at}")))
        .collect();
        if parts.is_empty() {
            Json::Null
        } else {
            json!(parts.join("; "))
        }
    }

    fn below(&self, filter: &Filter) -> Result<Option<BTreeSet<Id>>, String> {
        let Some(under) = &filter.under else {
            return Ok(None);
        };
        let (id, work) = self
            .frontier
            .named(&Name::Slug(under.clone()))
            .ok_or_else(|| self.unknown_work(&Name::Slug(under.clone())))?;
        if work.iset().is_none() {
            return Err(format!("filter.under: {under} is a trial, not an iset"));
        }
        Ok(Some(self.frontier.below(id)))
    }

    fn unknown_work(&self, name: &Name) -> String {
        let text = name.to_string();
        let names: Vec<String> = self
            .frontier
            .names()
            .keys()
            .map(ToString::to_string)
            .collect();
        slug::suggestion(
            &format!("work {text:?}"),
            &slug::nearest(&text, names.iter().map(String::as_str)),
        )
    }

    fn work_matches(&self, id: Id, work: &Work, filter: &Filter) -> bool {
        filter
            .state
            .as_ref()
            .is_none_or(|states| self.frontier.shown(id).is_some_and(|s| states.iter().any(|n| n.shown() == s)))
            && filter
                .owner
                .as_ref()
                .is_none_or(|owner| work.owner.as_ref().is_some_and(|o| &o.name == owner))
            && filter.lane.as_ref().is_none_or(|lane| work.name.lane() == lane)
            && filter.schema.as_ref().is_none_or(|schema| {
                matches!(&work.binding, Some(Binding::Exact { id, .. } | Binding::Requested { id, .. }) if id == schema)
            })
            && filter.text.as_ref().is_none_or(|q| self.work_text(id, work, q).is_some())
    }

    /// The first field of a work record containing the query, with its excerpt.
    fn work_text(&self, id: Id, work: &Work, query: &str) -> Option<(String, String)> {
        let q = query.to_lowercase();
        let name = |r: &Ref| self.frontier.ref_name(r);
        let _ = name;
        if contains(&work.name.to_string(), &q) {
            return Some(("name".into(), work.name.to_string()));
        }
        let mut slots: Vec<(String, &Data)> = Vec::new();
        match &work.body {
            Body::Iset(iset) => {
                slots.push(("data".into(), &iset.data));
                if let Some(j) = &iset.judgment {
                    slots.push(("answer".into(), &j.answer));
                }
                for j in &iset.superseded {
                    slots.push(("superseded answer".into(), &j.answer));
                }
            }
            Body::Trial(trial) => {
                if let Some(plan) = trial.plan() {
                    slots.push(("plan".into(), &plan.data));
                }
                if let Some(report) = &trial.report {
                    slots.push(("report".into(), &report.data));
                }
            }
        }
        for (slot, data) in slots {
            if let Some((path, text)) = data.find(&q) {
                let field = if path.is_empty() {
                    slot
                } else {
                    format!("{slot}.{path}")
                };
                let body = if text.is_empty() { field.clone() } else { text };
                return Some((field, excerpt(&body, query).unwrap_or(body)));
            }
        }
        let _ = id;
        work.void
            .as_ref()
            .and_then(|v| excerpt(&v.reason, query).map(|e| ("void".into(), e)))
            .or_else(|| {
                work.owner
                    .as_ref()
                    .and_then(|o| o.instructions.as_deref())
                    .and_then(|i| excerpt(i, query).map(|e| ("instructions".into(), e)))
            })
    }

    pub fn search(&self, filter: &Filter) -> Vec<Json> {
        let Some(query) = filter.text.as_deref() else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        let mut row = |record: Json, field: &str, excerpt: String| {
            rows.push(json!({"record":record,"field":field,"excerpt":excerpt}));
        };
        if let Some(e) = self.frontier.objective().and_then(|o| excerpt(o, query)) {
            row(json!("frontier"), "objective", e);
        }
        if let Some(e) = self
            .frontier
            .conclusion()
            .and_then(|c| excerpt(&c.lesson, query))
        {
            row(json!("frontier"), "lesson", e);
        }
        for (id, work) in self.frontier.work() {
            if let Some((field, e)) = self.work_text(*id, work, query) {
                row(json!(work.name), &field, e);
            }
        }
        for b in self.frontier.baselines().values() {
            let hit = excerpt(b.slug.as_str(), query)
                .map(|e| ("slug", e))
                .or_else(|| {
                    b.note
                        .as_deref()
                        .and_then(|n| excerpt(n, query))
                        .map(|e| ("note", e))
                })
                .or_else(|| {
                    b.artifacts
                        .iter()
                        .find_map(|a| excerpt(a, query))
                        .map(|e| ("artifacts", e))
                });
            if let Some((field, e)) = hit {
                row(json!(b.slug), field, e);
            }
        }
        for e in self.frontier.elections() {
            if let Some(x) = excerpt(&e.rationale, query) {
                row(self.name(e.baseline), "election rationale", x);
            }
        }
        rows
    }

    pub fn denominator(&self) -> usize {
        self.frontier.work().len() + self.frontier.baselines().len()
    }

    pub fn status(&self, args: &StatusArgs, paging: &Paging<'_>) -> Result<Json, String> {
        let mut normalized = args.clone();
        let warnings = normalized.normalize();
        let args = &normalized;
        if args.view == View::Frontiers {
            return Err("frontiers is a project view".into());
        }
        let ignored = if warnings.is_empty() {
            Json::Null
        } else {
            json!(warnings)
        };
        if args.view == View::Summary && args.filter.text.is_none() {
            args.window.validate(false)?;
            let mut result = self.summary();
            result["ignored"] = ignored;
            return Ok(result);
        }
        let below = self.below(&args.filter)?;
        let in_scope = |id: &Id| below.as_ref().is_none_or(|b| b.contains(id));
        let (denominator, rows): (usize, Vec<Json>) = match args.view {
            View::Summary => (self.denominator(), self.search(&args.filter)),
            View::Isets => {
                let isets: Vec<_> = self
                    .frontier
                    .work()
                    .iter()
                    .filter(|(id, w)| w.iset().is_some() && in_scope(id))
                    .collect();
                (
                    isets.len(),
                    isets
                        .into_iter()
                        .filter(|(id, w)| self.work_matches(**id, w, &args.filter))
                        .filter_map(|(id, w)| w.iset().map(|i| self.iset_row(*id, w, i)))
                        .collect(),
                )
            }
            View::Trials => {
                let mut trials: Vec<_> = self
                    .frontier
                    .work()
                    .iter()
                    .filter(|(id, w)| w.trial().is_some() && in_scope(id))
                    .collect();
                trials.sort_by_key(|(_, w)| {
                    std::cmp::Reverse(w.trial().and_then(|t| t.report.as_ref()).map_or(w.created.pos, |r| r.at.pos))
                });
                (
                    trials.len(),
                    trials
                        .into_iter()
                        .filter(|(id, w)| self.work_matches(**id, w, &args.filter))
                        .filter_map(|(id, w)| w.trial().map(|t| self.trial_row(*id, w, t)))
                        .collect(),
                )
            }
            View::Baselines => (
                self.frontier.baselines().len(),
                self.frontier
                    .baselines()
                    .iter()
                    .filter(|(_, b)| {
                        args.filter.lane.as_ref().is_none_or(|l| b.slug.lane() == l)
                            && args.filter.text.as_ref().is_none_or(|q| {
                                contains(b.slug.as_str(), q)
                                    || b.note.as_deref().is_some_and(|n| contains(n, q))
                                    || b.artifacts.iter().any(|a| contains(a, q))
                            })
                    })
                    .map(|(id, b)| self.baseline_row(*id, b))
                    .collect(),
            ),
            View::Regimes => (
                self.frontier.regimes().len(),
                self.frontier.regimes().iter().rev().map(|r| self.regime_row(r)).collect(),
            ),
            View::Schemas => (
                self.frontier.schemas().len(),
                self.frontier.schemas().iter().map(|(id, defs)| self.schema_row(id, defs)).collect(),
            ),
            View::Findings => (
                self.findings.len(),
                self.findings
                    .iter()
                    .map(|f| json!({"identity":f.identity(self.frontier),"statement":f.statement(self.frontier)}))
                    .collect(),
            ),
            View::Frontiers => (0, Vec::new()),
        };
        let page = paging.page(rows, &semantic(args), "status", &args.window, 20, |r| r)?;
        Ok(
            json!({"scope_total":denominator,"items":page.items,"total":page.total,"next_cursor":page.next_cursor,"ignored":ignored}),
        )
    }

    fn regime_row(&self, r: &Regime) -> Json {
        let route = json!({"view":"regimes","detail":"full"});
        json!({"number":r.number,"at":stamp(r.at.time,self.full),
            "reason":r.reason.as_deref().map(|s| self.text(s,"frontier.status",route.clone())),
            "bridge":r.bridge.map(|b| self.name(b)),
            "contract":self.text(&r.gauge.contract,"frontier.status",route),
            "metrics":r.gauge.metrics.iter().map(|(key,m)| json!({"key":key,"unit":m.unit.as_str(),"better":m.better,
                "kpi":m.kpi.then_some(true),"retired":m.retired.then_some(true),"about":m.about,"refs":m.refs})).collect::<Vec<_>>()})
    }

    fn bound(&self, schema: &Slug) -> Vec<(Id, &Work)> {
        self.frontier
            .work()
            .iter()
            .filter(|(_, w)| matches!(&w.binding, Some(Binding::Exact { id, .. } | Binding::Requested { id, .. }) if id == schema))
            .map(|(id, w)| (*id, w))
            .collect()
    }

    fn schema_row(&self, id: &Slug, definitions: &[Definition]) -> Json {
        let bound = self.bound(id);
        let unvalidated = bound
            .iter()
            .filter(|(_, w)| matches!(w.binding, Some(Binding::Requested { .. })))
            .count();
        let nonconforming = bound
            .iter()
            .filter(|(i, w)| {
                matches!(w.binding, Some(Binding::Exact { .. }))
                    && !self.frontier.violations(*i).is_empty()
            })
            .count();
        let current = definitions.last();
        json!({"schema":id,"carrier":current.map(|d| d.schema.carrier().as_str()),"definitions":definitions.len(),
            "current":current.map(|d| d.at.pos.to_string()),"bound":bound.len(),
            "unvalidated":(unvalidated > 0).then_some(unvalidated),"nonconforming":(nonconforming > 0).then_some(nonconforming)})
    }

    fn summary(&self) -> Json {
        let f = self.frontier;
        let status = |view: &str| Some(self.route("frontier.status", json!({"view":view})));
        let mut result = json!({});
        if let Some(objective) = f.objective() {
            result["objective"] = self.text(objective, "frontier.status", json!({"detail":"full"}));
        }
        if let Some(c) = f.conclusion() {
            result["conclusion"] = json!({"lesson":self.text(&c.lesson,"frontier.status",json!({"detail":"full"})),
                "at":stamp(c.at.time,self.full),"archived":f.archived().then_some(true),
                "open_left":(!c.open.is_empty()).then(|| self.names(c.open.iter().copied())),
                "marks":(!f.void_sources(&c.consumes).is_empty()).then(|| format!("rests on void {}", f.void_sources(&c.consumes).iter().filter_map(|v| f.name_of(*v)).collect::<Vec<_>>().join(", ")))});
        }
        if let Some(regime) = f.current_regime() {
            let line = regime.gauge.contract.lines().next().unwrap_or_default();
            result["regime"] = json!({"number":regime.number,"contract":json!(preview(line,false,160,|| self.route("frontier.status",json!({"view":"regimes","detail":"full"}))))});
        }
        if let Some((index, baseline)) = f.incumbent() {
            let election = &f.elections()[index];
            let b = f.baselines().get(&baseline);
            result["incumbent_baseline"] = json!({"slug":self.name(baseline),"since":stamp(election.at.time,self.full),
                "artifacts":b.map(|b| &b.artifacts)});
            if let (Some(regime), Some(b)) = (f.current_regime(), b) {
                let kpis: Vec<Json> = regime
                    .gauge
                    .metrics
                    .iter()
                    .filter(|(_, m)| m.kpi && !m.retired)
                    .map(|(key, m)| {
                        let reading = match b.readings.get(key) {
                            Some(Reading::Listed(v)) => json!(v),
                            _ => Json::Null,
                        };
                        json!({"key":key,"unit":m.unit.as_str(),"better":m.better,"incumbent":reading,"refs":m.refs})
                    })
                    .collect();
                result["kpis"] = bounded(kpis, 8, status("regimes"));
            }
        }
        result["counts"] = self.counts();
        let (mut untyped, mut unvalidated, mut nonconforming) = (0, 0, 0);
        for (id, w) in f.work() {
            match &w.binding {
                None => untyped += 1,
                Some(Binding::Requested { .. }) => unvalidated += 1,
                Some(_) if !f.violations(*id).is_empty() => nonconforming += 1,
                Some(_) => {}
            }
        }
        result["validity"] =
            json!({"untyped":untyped,"unvalidated":unvalidated,"nonconforming":nonconforming});
        let rows = |shown: Shown| -> Vec<(Id, &Work)> {
            f.work()
                .iter()
                .filter(|(id, _)| f.shown(**id) == Some(shown))
                .map(|(id, w)| (*id, w))
                .collect()
        };
        let running: Vec<Json> = rows(Shown::State(State::Running))
            .into_iter()
            .map(|(id, w)| json!({"name":w.name,"carrier":w.carrier().as_str(),"owner":Self::owner(w),"headline":self.headline_json(id)}))
            .collect();
        result["running"] = bounded(
            running,
            3,
            Some(self.route(
                "frontier.status",
                json!({"view":"isets","filter":{"state":["running"]}}),
            )),
        );
        let tested: Vec<Json> = rows(Shown::State(State::Tested))
            .into_iter()
            .map(|(id, w)| json!({"name":w.name,"headline":self.headline_json(id)}))
            .collect();
        result["tested"] = bounded(
            tested,
            3,
            Some(self.route(
                "frontier.status",
                json!({"view":"isets","filter":{"state":["tested"]}}),
            )),
        );
        let ready: Vec<Json> = rows(Shown::State(State::Ready))
            .into_iter()
            .map(|(id, w)| json!({"name":w.name,"carrier":w.carrier().as_str(),"headline":self.headline_json(id)}))
            .collect();
        result["ready"] = bounded(
            ready,
            3,
            Some(self.route(
                "frontier.status",
                json!({"view":"trials","filter":{"state":["ready"]}}),
            )),
        );
        let mut judged: Vec<(Id, &Work, &Judgment)> = f
            .work()
            .iter()
            .filter_map(|(id, w)| {
                w.iset()
                    .and_then(|i| i.judgment.as_ref())
                    .map(|j| (*id, w, j))
            })
            .collect();
        judged.sort_by_key(|(_, _, j)| std::cmp::Reverse(j.at.pos));
        let judgments: Vec<Json> = judged
            .into_iter()
            .map(|(id, w, j)| {
                let name = |r: &Ref| f.ref_name(r);
                let compact = j.answer.compact(&name);
                let answer = if compact.chars().count() > 120 {
                    json!(preview(&compact, false, 120, || self
                        .route("record.read", self.read_route(id, "judgment"))))
                } else {
                    j.answer.json(&name)
                };
                json!({"iset":w.name,"at":stamp(j.at.time,self.full),"answer":answer})
            })
            .collect();
        result["judgments"] = bounded(
            judgments,
            3,
            Some(self.route(
                "frontier.status",
                json!({"view":"isets","filter":{"state":["settled"]}}),
            )),
        );
        let findings: Vec<Json> = self
            .findings
            .iter()
            .map(|finding| json!({"identity":finding.identity(f),"statement":finding.statement(f)}))
            .collect();
        result["findings"] = bounded(findings, 3, status("findings"));
        result
    }

    pub fn read(&self, args: &ReadArgs, paging: &Paging<'_>) -> Result<Json, String> {
        let f = self.frontier;
        let (id, defaults, query) = match (&args.work, &args.baseline) {
            (Some(name), None) => {
                let (id, work) = f.named(name).ok_or_else(|| self.unknown_work(name))?;
                let defaults = if work.iset().is_some() {
                    ISET_SECTIONS
                } else {
                    TRIAL_SECTIONS
                };
                (id, defaults, json!({"work":name}))
            }
            (None, Some(slug)) => {
                let (id, _) = f.baseline_named(slug).ok_or_else(|| {
                    let names: Vec<&str> = f.baseline_names().keys().map(Slug::as_str).collect();
                    slug::suggestion(
                        &format!("baseline {:?}", slug.as_str()),
                        &slug::nearest(slug.as_str(), names),
                    )
                })?;
                (id, BASELINE_SECTIONS, json!({"baseline":slug}))
            }
            _ => return Err("record.read names exactly one of work and baseline".into()),
        };
        if let Some(sections) = &args.sections
            && let Some(wrong) = sections.iter().find(|s| !defaults.contains(s))
        {
            return Err(format!(
                "section {} does not apply to this record",
                wrong.name()
            ));
        }
        let mut window = args.window.clone();
        window.sections(args.sections.as_ref())?;
        let sections = parts(args.sections.as_ref(), defaults, &window, COLLECTIONS)?;
        let mut out = json!({});
        for section in sections {
            let page = |rows: Vec<Json>| -> Result<Json, String> {
                let page = paging.page(rows, &query, section.name(), &window, 20, |r| r)?;
                Ok(json!({"items":page.items,"total":page.total,"next_cursor":page.next_cursor}))
            };
            out[section.name()] = match section {
                Section::Head => self.head(id),
                Section::Data => self.data_section(id),
                Section::Plans => page(self.plans(id))?,
                Section::Report => self.report(id),
                Section::Members => page(self.members(id))?,
                Section::Judgment => self.judgment(id),
                Section::Elections => page(self.elections(id))?,
                Section::Isets => page(f.containers(id).into_iter().map(|i| json!({"name":self.name(i),"state":self.shown(i)})).collect())?,
                Section::Inputs => page(
                    f.work()
                        .get(&id)
                        .map(|w| w.inputs.iter().map(|i| json!({"name":self.name(*i),"state":self.shown(*i),"marks":self.marks(*i)})).collect())
                        .unwrap_or_default(),
                )?,
                Section::Dependents => page(self.dependents(id))?,
                Section::Owner => self.owner_section(id),
            };
        }
        Ok(out)
    }

    fn head(&self, id: Id) -> Json {
        let f = self.frontier;
        if let Some(b) = f.baselines().get(&id) {
            let mut row = self.baseline_row(id, b);
            row["void"] = b.void.as_ref().map_or(Json::Null, |v| self.void(id, v));
            row["rev"] = json!(b.rev.to_string());
            return row;
        }
        let Some(work) = f.work().get(&id) else {
            return Json::Null;
        };
        let validity = self.validity(id, work);
        json!({"name":work.name,"carrier":work.carrier().as_str(),"state":self.shown(id),
            "void":work.void.as_ref().map(|v| self.void(id, v)),
            "regime":work.trial().and_then(|t| t.regime),
            "schema":self.binding(work.binding.as_ref()),"violations":validity["violations"],"current":validity["current"],
            "headline":self.headline_json(id),"badges":self.badges(work),
            "created":stamp(work.created.time,self.full),"rev":work.rev.to_string(),"marks":self.marks(id)})
    }

    fn void(&self, id: Id, void: &Void) -> Json {
        json!({"reads":if self.frontier.invalidated(id) {"invalidated"} else {"withdrawn"},"at":stamp(void.at.time,self.full),
            "reason":self.text(&void.reason,"record.read",self.read_route(id,"head"))})
    }

    fn validity(&self, id: Id, work: &Work) -> Json {
        let f = self.frontier;
        let violations = f.violations(id);
        let mut out = json!({"schema":self.binding(work.binding.as_ref())});
        if !violations.is_empty() {
            out["violations"] = json!(violations);
        }
        if let Some(Binding::Exact { id: schema, at }) = &work.binding
            && let Some(current) = f.schemas().get(schema).and_then(|d| d.last())
            && current.at.pos != *at
        {
            let data = work.data().cloned().unwrap_or_else(Data::empty);
            let slot = if work.iset().is_some() {
                Slot::Data
            } else {
                Slot::Plan
            };
            let against = current.schema.check(slot, &data);
            out["current"] = json!({"definition":format!("{schema}@{}", current.at.pos),
                "conforms":against.is_empty(),"violations":(!against.is_empty()).then_some(against)});
        }
        out
    }

    fn data_section(&self, id: Id) -> Json {
        let Some(work) = self.frontier.work().get(&id) else {
            return Json::Null;
        };
        let Some(iset) = work.iset() else {
            return Json::Null;
        };
        self.data(&iset.data, id, "data")
    }

    fn plans(&self, id: Id) -> Vec<Json> {
        let Some(trial) = self.frontier.work().get(&id).and_then(Work::trial) else {
            return Vec::new();
        };
        let executed = trial.report.as_ref().and_then(|r| r.plan);
        trial
            .plans
            .iter()
            .enumerate()
            .rev()
            .map(|(index, plan)| {
                json!({"rev":plan.at.pos.to_string(),"at":stamp(plan.at.time,self.full),"executed":(executed == Some(index)).then_some(true),
                    "control":self.control(plan.control),"treatment":self.treatment(plan.treatment.as_ref()),"data":self.data(&plan.data,id,"plans")})
            })
            .collect()
    }

    fn report(&self, id: Id) -> Json {
        let f = self.frontier;
        let Some(work) = f.work().get(&id) else {
            return Json::Null;
        };
        let Some(trial) = work.trial() else {
            return Json::Null;
        };
        let Some(report) = &trial.report else {
            return json!({"reported":false,"plan":if trial.plans.is_empty() {"no plan recorded"} else {"planned"}});
        };
        let executed = report
            .plan
            .and_then(|i| trial.plans.get(i))
            .map_or_else(|| "unknown".to_owned(), |p| p.at.pos.to_string());
        let control_source = match (&report.control_readings, report.control) {
            (Some(_), _) => json!("control readings of this report"),
            (None, Some(c)) => json!(format!(
                "baseline {} as consumed",
                self.name(c.baseline).as_str().unwrap_or_default()
            )),
            (None, None) => Json::Null,
        };
        let void = f.void_sources(&report.consumes);
        let mut out = json!({"at":stamp(report.at.time,self.full),"plan":executed,
            "planned":if trial.plans.is_empty() {"no plan recorded"} else if trial.plans[0].at.pos.seq < report.at.pos.seq {"before the report"} else {"with the report"},
            "control":self.control(report.control),"control_source":control_source,
            "treatment":self.treatment(report.treatment.as_ref()).as_object().map_or(json!("unspecified"),|t| Json::Object(t.clone())),
            "readings":self.readings(trial.regime,&report.readings,false),
            "control_readings":report.control_readings.as_ref().map(|r| self.readings(trial.regime,r,false)),
            "comparisons":self.comparisons(trial,false),"artifacts":report.artifacts,
            "data":self.data(&report.data,id,"report"),
            "consumed":self.names(report.consumes.iter().copied())});
        if !void.is_empty() {
            out["rests_on_void"] = json!(
                void.iter()
                    .map(|v| json!({"record":self.name(*v),"reason":self.void_reason(*v)}))
                    .collect::<Vec<_>>()
            );
        }
        if f.removed_source(&report.consumes) {
            out["source_removed"] = json!(true);
        }
        let mut validity = self.validity(id, work);
        if let Some(fields) = validity.as_object_mut() {
            fields.remove("current");
            if fields.contains_key("violations") {
                out["validity"] = validity;
            }
        }
        out
    }

    fn void_reason(&self, id: Id) -> Json {
        let f = self.frontier;
        f.work()
            .get(&id)
            .and_then(|w| w.void.as_ref())
            .or_else(|| f.baselines().get(&id).and_then(|b| b.void.as_ref()))
            .map_or(Json::Null, |v| {
                self.text(&v.reason, "record.read", self.read_route(id, "head"))
            })
    }

    fn members(&self, id: Id) -> Vec<Json> {
        self.frontier
            .members(id)
            .map(|m| {
                let carrier = self.frontier.work().get(m).map(|w| w.carrier().as_str());
                json!({"name":self.name(*m),"carrier":carrier,"state":self.shown(*m),"headline":self.headline_json(*m),"marks":self.marks(*m)})
            })
            .collect()
    }

    fn judgment(&self, id: Id) -> Json {
        let f = self.frontier;
        let Some(work) = f.work().get(&id) else {
            return Json::Null;
        };
        let Some(iset) = work.iset() else {
            return Json::Null;
        };
        let one = |j: &Judgment| {
            let void = f.void_sources(&j.consumes);
            json!({"at":stamp(j.at.time,self.full),"answer":self.data(&j.answer,id,"judgment"),
                "guard":j.seen.map_or_else(|| "read not recorded".to_owned(), |s| format!("guarded at {s}")),
                "consumed":self.names(j.consumes.iter().copied()),"passed_over":self.names(j.passed.iter().copied()),
                "rests_on_void":(!void.is_empty()).then(|| void.iter().map(|v| json!({"record":self.name(*v),"reason":self.void_reason(*v)})).collect::<Vec<_>>()),
                "source_removed":f.removed_source(&j.consumes).then_some(true)})
        };
        let mut out = iset
            .judgment
            .as_ref()
            .map_or_else(|| json!({"judged":false}), one);
        if !iset.superseded.is_empty() {
            out["superseded"] = json!({"total":iset.superseded.len(),
                "items":iset.superseded.iter().rev().map(one).collect::<Vec<_>>()});
        }
        out
    }

    fn elections(&self, id: Id) -> Vec<Json> {
        let f = self.frontier;
        f.elections()
            .iter()
            .enumerate()
            .filter(|(_, e)| e.baseline == id)
            .rev()
            .map(|(_, e)| {
                let void = f.void_sources(&e.consumes);
                json!({"at":stamp(e.at.time,self.full),"regime":e.regime,
                    "rationale":self.text(&e.rationale,"record.read",self.read_route(id,"elections")),
                    "adopts":e.adopts.iter().map(|(i, judged)| json!({"iset":self.name(*i),"judged":judged.map(|p| p.to_string())})).collect::<Vec<_>>(),
                    "rests_on_void":(!void.is_empty()).then(|| self.names(void))})
            })
            .collect()
    }

    pub fn consumer(&self, consumer: Consumer) -> Json {
        match consumer {
            Consumer::Report(id) => json!(format!(
                "report of {}",
                self.name(id).as_str().unwrap_or_default()
            )),
            Consumer::Judgment(id) => json!(format!(
                "judgment of {}",
                self.name(id).as_str().unwrap_or_default()
            )),
            Consumer::Election(index) => {
                let e = &self.frontier.elections()[index];
                json!(format!(
                    "election of {} at {}",
                    self.name(e.baseline).as_str().unwrap_or_default(),
                    stamp(e.at.time, self.full)
                ))
            }
            Consumer::Conclusion => json!("conclusion"),
        }
    }

    fn dependents(&self, id: Id) -> Vec<Json> {
        let f = self.frontier;
        let mut rows: Vec<Json> = f
            .consumers(id)
            .into_iter()
            .map(|c| json!({"record":self.consumer(c),"relation":"consumed it"}))
            .collect();
        rows.extend(f.waiting_on(id).into_iter().map(
            |w| json!({"record":self.name(w),"relation":"waits on it","state":self.shown(w)}),
        ));
        rows.extend(f.containers(id).into_iter().map(
            |i| json!({"record":self.name(i),"relation":"contains it","state":self.shown(i)}),
        ));
        rows
    }

    fn owner_section(&self, id: Id) -> Json {
        let Some(work) = self.frontier.work().get(&id) else {
            return Json::Null;
        };
        json!({"owner":work.owner.as_ref().map(|o| json!({"name":o.name,
                "instructions":o.instructions.as_deref().map(|i| self.text(i,"record.read",self.read_route(id,"owner"))),
                "bound":o.bound.to_string(),"stale":(o.bound < work.rev && !work.resolved()).then_some(true)})),
            "running":work.running.map(|m| stamp(m.time,self.full))})
    }

    pub fn schema(&self, args: &SchemaArgs, paging: &Paging<'_>) -> Result<Json, String> {
        let Some(id) = &args.schema else {
            return Ok(json!({"grammar":crate::mcp::SCHEMA_GRAMMAR}));
        };
        let definitions = self.frontier.schemas().get(id).ok_or_else(|| {
            slug::suggestion(
                &format!("schema {:?}", id.as_str()),
                &slug::nearest(
                    id.as_str(),
                    self.frontier.schemas().keys().map(Slug::as_str),
                ),
            )
        })?;
        let rows: Vec<Json> = self
            .bound(id)
            .into_iter()
            .map(|(i, w)| {
                let current = matches!(&w.binding, Some(Binding::Exact { at, .. }) if Some(*at) == definitions.last().map(|d| d.at.pos));
                json!({"name":w.name,"binding":self.binding(w.binding.as_ref()),"current":current,"marks":self.marks(i)})
            })
            .collect();
        let page = paging.page(
            rows,
            &json!({"schema":id}),
            "bound",
            &args.window,
            20,
            |r| r,
        )?;
        Ok(
            json!({"schema":id,"definitions":definitions.iter().rev().map(|d| json!({"rev":d.at.pos.to_string(),"at":stamp(d.at.time,self.full),"definition":d.schema.json()})).collect::<Vec<_>>(),
            "bound":{"items":page.items,"total":page.total,"next_cursor":page.next_cursor}}),
        )
    }
}

impl Lens<'_> {
    /// Isets and trials counted by state.
    pub fn counts(&self) -> Json {
        let counts = self.frontier.counts();
        json!({"isets":counts_json(&counts, Carrier::Iset),"trials":counts_json(&counts, Carrier::Trial)})
    }

    /// A committed or rehearsed patch's receipt facts, named in its candidate state.
    pub fn receipt(&self, patched: &Patched, warnings: &[(usize, Warning)]) -> Json {
        let mut out = json!({});
        if !warnings.is_empty() {
            out["warnings"] = json!({"total":warnings.len(),"items":warnings.iter()
                .map(|(index, w)| json!({"index":index,"subject":w.subject,"message":w.message}))
                .collect::<Vec<_>>()});
        }
        if !patched.created.is_empty() {
            out["created"] = Json::Array(patched.created.iter().map(|c| self.created(c)).collect());
        }
        if !patched.raised.is_empty() {
            out["raised"] = json!({"total":patched.raised.len(),"items":patched.raised.iter()
                .map(|(identity, statement)| json!({"identity":identity,"statement":statement}))
                .collect::<Vec<_>>()});
        }
        if !patched.cleared.is_empty() {
            out["cleared"] = json!({"total":patched.cleared.len(),"items":patched.cleared});
        }
        let mut changes = Map::new();
        for key in patched
            .before
            .keys()
            .chain(patched.after.keys())
            .collect::<BTreeSet<_>>()
        {
            let before = patched.before.get(key).copied().unwrap_or(0);
            let after = patched.after.get(key).copied().unwrap_or(0);
            if before != after {
                changes.insert(
                    format!("{} {}", key.0.as_str(), key.1.as_str()),
                    json!({"before":before,"after":after}),
                );
            }
        }
        if !changes.is_empty() {
            out["changes"] = Json::Object(changes);
        }
        if !patched.unblocked.is_empty() {
            out["ready"] = Json::Array(patched.unblocked.iter().map(|id| self.name(*id)).collect());
        }
        if !patched.voided.is_empty() {
            out["voided"] = Json::Array(
                patched
                    .voided
                    .iter()
                    .map(|v| {
                        json!({"void":self.name(v.id),
                            "consumers":v.consumers.iter().map(|c| self.consumer(*c)).collect::<Vec<_>>(),
                            "waiting":v.waiting.iter().map(|id| self.name(*id)).collect::<Vec<_>>()})
                    })
                    .collect(),
            );
        }
        out
    }

    fn created(&self, created: &Created) -> Json {
        let f = self.frontier;
        match created {
            Created::Work(id) => match f.work().get(id).map(|w| &w.body) {
                Some(Body::Trial(trial)) => {
                    let mut row = json!({"trial":self.name(*id)});
                    let control = trial.report.as_ref().map_or_else(
                        || trial.plan().and_then(|p| p.control.as_ref()),
                        |r| r.control.as_ref(),
                    );
                    if let Some(control) = control.filter(|c| c.defaulted) {
                        row["control"] = self.control(Some(*control));
                    }
                    row
                }
                Some(Body::Iset(_)) => json!({"iset":self.name(*id)}),
                None => json!({f.kind_of(*id).map_or("work", Kind::as_str):"removed"}),
            },
            Created::Baseline(id) => json!({"baseline":self.name(*id)}),
            Created::Regime(number) => json!({"regime":number.to_string()}),
            Created::Election(index) => {
                let e = &f.elections()[*index];
                json!({"epoch":{"baseline":self.name(e.baseline),"since":stamp(e.at.time,self.full)}})
            }
            Created::Definition(slug) => {
                let rev = f
                    .schemas()
                    .get(slug)
                    .and_then(|d| d.last())
                    .map(|d| d.at.pos.to_string());
                json!({"schema":slug.as_str(),"rev":rev})
            }
        }
    }
}
