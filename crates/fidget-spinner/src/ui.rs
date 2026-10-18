//! Fidget Spinner's navigator projections; trebuchet owns transport, geometry and chrome.
use crate::{
    inspect::stamp,
    mcp::Fault,
    store::{self, Ledger, Listing, Store, StoreError},
};
use fidget_spinner_core::{
    Assay, Baseline, BaselineDecision, Comparison, Conclusion, Frontier, Hypothesis,
    HypothesisDecision, MetricKey, Movement, Op, Reading, RecordedReadings, RegimeNumber, Review,
    Slug, State as HypothesisState, TrialId, Value,
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{Mutex, PoisonError},
};
use time::OffsetDateTime;
use trebuchet::{
    board::{
        self, ActionResult, Attribute as A, BoardRoutes, BoardScene, BoardSource, Card,
        Content as C, Field, IndexRow, IndexScene, Marker, Place, Query, RouteTarget, SetStyle,
        StatusCode, Tag as T, Theme, button, count, meter, stats,
    },
    layout::{Cell, CellSet, LayoutInput, Occupant, Separator, elide, focused, foreground},
};
mod charts;

const STYLE: &str = include_str!("ui/style.css");
const FAVICON: &str = include_str!("ui/favicon.svg");
/// Characters of free text a hover card keeps.
const CARD_TEXT: usize = 320;
/// Hypotheses a hover card or aggregate tooltip lists.
const CARD_LINKS: usize = 12;
/// Dispositions since the second-latest epoch began.
const HORIZON: Horizon = Horizon::Epochs(2);

struct Navigator {
    path: PathBuf,
    ledgers: Mutex<BTreeMap<i64, Ledger<Frontier>>>,
}

/// How far back a board shows settled hypotheses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Horizon {
    /// Dispositions since the n-th latest surviving epoch began; none for zero.
    Epochs(usize),
    /// Every disposition.
    All,
}

impl Horizon {
    const CHOICES: [Self; 6] = [
        Self::Epochs(0),
        Self::Epochs(1),
        Self::Epochs(2),
        Self::Epochs(3),
        Self::Epochs(5),
        Self::All,
    ];
    fn token(self) -> String {
        match self {
            Self::Epochs(n) => n.to_string(),
            Self::All => "all".into(),
        }
    }
    fn label(self) -> String {
        match self {
            Self::Epochs(0) => "none".into(),
            Self::Epochs(1) => "this epoch".into(),
            Self::Epochs(n) => format!("last {n} epochs"),
            Self::All => "all".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Selection {
    id: i64,
    horizon: Horizon,
}

/// How much of a record's free text a card keeps.
#[derive(Clone, Copy)]
enum Depth {
    /// A hover card: one line, elided.
    Hover,
    /// A sheet: everything, line breaks kept.
    Sheet,
}

impl Depth {
    fn text(self, text: &str) -> String {
        match self {
            Self::Hover => elide(
                &text.split_whitespace().collect::<Vec<_>>().join(" "),
                CARD_TEXT,
            ),
            Self::Sheet => text.to_owned(),
        }
    }
}

/// A hypothesis's colouring: its decision once settled, else its state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Livery {
    Accepted,
    Rejected,
    Shelved,
    Running,
    Tested,
    Ready,
    Waiting,
}

impl Livery {
    const ALL: [Self; 7] = [
        Self::Accepted,
        Self::Rejected,
        Self::Shelved,
        Self::Running,
        Self::Tested,
        Self::Ready,
        Self::Waiting,
    ];
    fn of(h: &Hypothesis, state: HypothesisState) -> Self {
        match (h.disposition.as_ref().map(|d| &d.decision), state) {
            (Some(HypothesisDecision::Accepted { .. }), _) => Self::Accepted,
            (Some(HypothesisDecision::Rejected), _) => Self::Rejected,
            (Some(HypothesisDecision::Shelved), _) => Self::Shelved,
            (None, HypothesisState::Running) => Self::Running,
            (None, HypothesisState::Tested) => Self::Tested,
            (None, HypothesisState::Ready) => Self::Ready,
            (None, HypothesisState::Waiting) => Self::Waiting,
            (None, HypothesisState::Settled) => {
                unreachable!("an assay settles exactly the hypotheses with dispositions")
            }
        }
    }
    const fn name(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Shelved => "shelved",
            Self::Running => "running",
            Self::Tested => "tested",
            Self::Ready => "ready",
            Self::Waiting => "waiting",
        }
    }
}

/// Hypotheses by livery.
struct Tally([u64; Livery::ALL.len()]);

impl Tally {
    fn new(f: &Frontier, a: &Assay) -> Self {
        let mut counts = [0; Livery::ALL.len()];
        for (slug, h) in f.hypotheses() {
            counts[Livery::of(h, a.hypotheses[slug]) as usize] += 1;
        }
        Self(counts)
    }
    const fn of(&self, livery: Livery) -> u64 {
        self.0[livery as usize]
    }
    fn meter(&self) -> Vec<C> {
        use Livery::*;
        let total = self.0.iter().sum();
        let settled = self.of(Accepted) + self.of(Rejected) + self.of(Shelved);
        let running = self.of(Running);
        meter(
            &[Accepted, Rejected, Shelved].map(|livery| (livery.name(), self.of(livery))),
            running,
            total,
            format!("{settled} settled + {running} running of {total} hypotheses"),
        )
    }
    fn counts(&self) -> Vec<C> {
        Livery::ALL
            .into_iter()
            .map(|livery| {
                count(
                    livery.name(),
                    usize::try_from(self.of(livery)).unwrap_or(usize::MAX),
                )
            })
            .collect()
    }
}

pub fn serve(path: PathBuf, bind: SocketAddr) -> Result<(), Fault> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(board::serve(
        bind,
        Navigator {
            path,
            ledgers: Mutex::default(),
        },
        BoardRoutes {
            prefix: "frontier".into(),
            parse: selection,
            target,
        },
        Theme {
            product: "Fidget Spinner".into(),
            css: STYLE.into(),
            favicon: FAVICON.into(),
            ..Theme::default()
        },
    ))?;
    Ok(())
}

fn selection(segment: &str, query: &Query) -> Result<Selection, String> {
    let id = segment
        .parse()
        .map_err(|_| format!("frontier identity must be an integer, not {segment}"))?;
    let mut horizon = HORIZON;
    for (key, value) in query.pairs() {
        match &*key {
            "horizon" => {
                horizon =
                    match &*value {
                        "all" => Horizon::All,
                        epochs => Horizon::Epochs(epochs.parse().map_err(|_| {
                            format!("horizon is an epoch count or all, not {epochs}")
                        })?),
                    };
            }
            other => return Err(format!("unknown navigator selector {other}")),
        }
    }
    Ok(Selection { id, horizon })
}
fn target(s: &Selection) -> RouteTarget {
    RouteTarget {
        segment: s.id.to_string(),
        query: query(s.horizon),
    }
}
/// The canonical query of a horizon, empty for the default.
fn query(horizon: Horizon) -> Query {
    if horizon == HORIZON {
        Query::default()
    } else {
        Query::new([("horizon", horizon.token())])
    }
}
fn url(s: &Selection) -> String {
    format!("/frontier/{}{}", s.id, query(s.horizon))
}
fn place(listing: &Listing) -> Place {
    Place {
        project: listing.address.project.to_path_buf(),
        name: listing.address.slug.to_string(),
    }
}
fn record(kind: &str, slug: &Slug) -> trebuchet::Slug {
    format!("{kind}.{slug}")
        .parse()
        .unwrap_or_else(|error| unreachable!("record prefix and validated slug: {error}"))
}
fn stat(text: String) -> C {
    C::classed(T::Span, "stat", vec![text.into()])
}
fn plural(n: usize, noun: &str) -> String {
    format!("{n} {noun}{}", if n == 1 { "" } else { "s" })
}
fn placed_at(h: &Hypothesis) -> OffsetDateTime {
    h.disposition.as_ref().map_or(h.at, |d| d.at)
}

impl Navigator {
    fn store(&self) -> Result<Option<Store>, Fault> {
        Ok(Store::open_read_only(&self.path, store::format()?)?)
    }
    fn ledger(&self, id: i64) -> Result<Option<(Listing, Ledger<Frontier>)>, Fault> {
        let Some(store) = self.store()? else {
            return Ok(None);
        };
        let Some(listing) = store.listing(id)? else {
            return Ok(None);
        };
        let mut ledgers = self.ledgers.lock().unwrap_or_else(PoisonError::into_inner);
        let ledger = ledgers.entry(id).or_default();
        store.refresh(&listing.address, ledger)?;
        Ok(Some((listing, ledger.clone())))
    }
}

impl BoardSource for Navigator {
    type Selector = Selection;
    type Error = Fault;
    fn head(&self) -> Result<String, Fault> {
        Ok(self
            .store()?
            .map(|s| s.head())
            .transpose()?
            .unwrap_or(0)
            .to_string())
    }
    fn missing(&self, fragment: bool) -> &'static str {
        if fragment {
            "no such frontier or record"
        } else {
            "no such frontier"
        }
    }
    fn action(&self, s: &Selection, action: &str) -> Result<Option<ActionResult>, Fault> {
        if action != "archive" {
            return Ok(None);
        }
        let Some(listing) = self
            .store()?
            .map(|store| store.listing(s.id))
            .transpose()?
            .flatten()
        else {
            return Ok(Some(ActionResult::Reply {
                status: StatusCode::NOT_FOUND,
                text: "No such frontier.".into(),
            }));
        };
        match store::open(&self.path)?.commit(
            &listing.address,
            &mut Ledger::default(),
            &[Op::Archive {}],
            OffsetDateTime::now_utc(),
            false,
        ) {
            Ok(_) => Ok(Some(ActionResult::Redirect("/".into()))),
            Err(StoreError::Refused(refusal)) => Ok(Some(ActionResult::Reply {
                status: StatusCode::CONFLICT,
                text: refusal.to_string(),
            })),
            Err(error) => Err(error.into()),
        }
    }
    fn index(&self) -> Result<IndexScene, Fault> {
        let mut rows = Vec::new();
        if let Some(store) = self.store()? {
            let mut ledgers = self.ledgers.lock().unwrap_or_else(PoisonError::into_inner);
            for listing in store.listings()? {
                let ledger = ledgers.entry(listing.id).or_default();
                store.refresh(&listing.address, ledger)?;
                let f = ledger.state();
                let concluded = match f.conclusion() {
                    Some(Conclusion { archived: true, .. }) => continue,
                    conclusion => conclusion.is_some(),
                };
                let tally = Tally::new(f, &Assay::new(f));
                rows.push((
                    (concluded, Reverse(listing.updated)),
                    IndexRow {
                        href: format!("/frontier/{}", listing.id),
                        place: place(&listing),
                        about: f.objective().map(str::to_owned),
                        columns: vec![tally.meter(), tally.counts()],
                        updated: listing.updated,
                        action: if concluded {
                            vec![
                                stat("concluded".into()),
                                button(
                                    format!("/frontier/{}/archive", listing.id),
                                    "Archive",
                                    "Archiving…",
                                    "Hide this frontier; keep all data",
                                ),
                            ]
                        } else {
                            Vec::new()
                        },
                    },
                ));
            }
        }
        rows.sort_by_key(|(order, _)| *order);
        Ok(IndexScene {
            rows: rows.into_iter().map(|(_, row)| row).collect(),
            empty:
                "No visible frontiers. Start one with frontier.patch through the Fidget Spinner MCP."
                    .into(),
        })
    }
    fn scene(&self, s: &Selection) -> Result<Option<BoardScene>, Fault> {
        let Some((listing, ledger)) = self.ledger(s.id)? else {
            return Ok(None);
        };
        let f = ledger.state();
        let a = Assay::new(f);
        let mut scene = geometry(f, &a);
        scene.input = focused(&scene.input, &visible(f, &a, s.horizon));
        scene.place = place(&listing);
        scene.controls = controls(s);
        scene.above = overview(f, &a, s)?;
        scene.empty = "No hypotheses within this horizon.".into();
        Ok(Some(scene))
    }
    fn foreground(
        &self,
        s: &Selection,
        selected: &trebuchet::Slug,
    ) -> Result<Option<BoardScene>, Fault> {
        let Some((listing, ledger)) = self.ledger(s.id)? else {
            return Ok(None);
        };
        let f = ledger.state();
        let a = Assay::new(f);
        let mut scene = geometry(f, &a);
        scene.place = place(&listing);
        let Some((kind, key)) = selected.as_str().split_once('.') else {
            return Ok(None);
        };
        match kind {
            "hypothesis" => {
                let Some((id, slug, h)) = hypothesis(f, key) else {
                    return Ok(None);
                };
                let shown = visible(f, &a, s.horizon);
                scene.input = foreground(
                    &scene.input,
                    id,
                    |v| v == id || shown.contains(&v),
                    |v| Some(f.hypotheses().keys().nth(v).map_or("", Slug::lane)),
                    |n| format!("{n} settled hypotheses"),
                )
                .unwrap_or_else(|| unreachable!("a selected hypothesis is always shown"));
                aggregates(f, &mut scene);
                (scene.above, scene.below) = sheet(f, &a, s, slug, h, None);
            }
            "trial" => {
                let Some((key, n)) = key.rsplit_once('.') else {
                    return Ok(None);
                };
                let (Some((_, slug, h)), Ok(n)) = (hypothesis(f, key), n.parse::<u64>()) else {
                    return Ok(None);
                };
                if !h.trials.contains_key(&n) {
                    return Ok(None);
                }
                scene.input = LayoutInput::default();
                (scene.above, scene.below) = sheet(f, &a, s, slug, h, Some(n));
            }
            "baseline" => {
                let Some((slug, b)) = key
                    .parse::<Slug>()
                    .ok()
                    .and_then(|slug| f.baselines().get_key_value(&slug))
                else {
                    return Ok(None);
                };
                let docket = a
                    .dockets
                    .get(slug)
                    .map(|d| members(f, &d.members))
                    .unwrap_or_default();
                scene.input = focused(&scene.input, &docket);
                scene.above = vec![sheet_record(&baseline(f, &a, slug, b))];
            }
            "review" => {
                let Some((slug, r)) = key
                    .parse::<Slug>()
                    .ok()
                    .and_then(|slug| f.reviews().get_key_value(&slug))
                else {
                    return Ok(None);
                };
                scene.input = focused(&scene.input, &members(f, &r.members));
                scene.above = vec![sheet_record(&review(&a, slug, r))];
            }
            _ => return Ok(None),
        }
        Ok(Some(scene))
    }
}

fn hypothesis<'f>(f: &'f Frontier, key: &str) -> Option<(usize, &'f Slug, &'f Hypothesis)> {
    let slug = key.parse::<Slug>().ok()?;
    f.hypotheses()
        .iter()
        .enumerate()
        .find(|(_, (s, _))| **s == slug)
        .map(|(id, (slug, h))| (id, slug, h))
}
/// Dense cell identities of the named hypotheses.
fn members(f: &Frontier, slugs: &BTreeSet<Slug>) -> BTreeSet<usize> {
    f.hypotheses()
        .keys()
        .enumerate()
        .filter(|(_, slug)| slugs.contains(*slug))
        .map(|(id, _)| id)
        .collect()
}
fn visible(f: &Frontier, a: &Assay, horizon: Horizon) -> BTreeSet<usize> {
    let since = a.epochs.iter().rev();
    f.hypotheses()
        .iter()
        .enumerate()
        .filter(|(_, (slug, h))| {
            a.hypotheses[*slug] != HypothesisState::Settled
                || match horizon {
                    Horizon::All => true,
                    Horizon::Epochs(0) => false,
                    Horizon::Epochs(n) => since
                        .clone()
                        .take(n)
                        .next_back()
                        .is_none_or(|epoch| placed_at(h) >= epoch.at),
                }
        })
        .map(|(id, _)| id)
        .collect()
}
fn controls(s: &Selection) -> Vec<C> {
    let mut choices = Horizon::CHOICES.to_vec();
    if !choices.contains(&s.horizon) {
        choices.push(s.horizon);
        choices.sort_unstable();
    }
    vec![C::element(
        T::Form,
        vec![
            (A::Method, "get".into()),
            (A::Action, format!("/frontier/{}", s.id)),
        ],
        vec![C::classed(
            T::Span,
            "fog",
            vec![C::element(
                T::Label,
                vec![(
                    A::Title,
                    "Settled hypotheses to show, by the epochs their dispositions fall in".into(),
                )],
                vec![
                    "Settled".into(),
                    C::element(
                        T::Select,
                        vec![(A::Name, "horizon".into())],
                        choices
                            .into_iter()
                            .map(|choice| {
                                let mut attributes = vec![(A::Value, choice.token())];
                                if choice == s.horizon {
                                    attributes.push((A::Selected, String::new()));
                                }
                                C::element(T::Option, attributes, vec![choice.label().into()])
                            })
                            .collect(),
                    ),
                ],
            )],
        )],
    )]
}
fn overview(f: &Frontier, a: &Assay, s: &Selection) -> Result<Vec<C>, Fault> {
    let tally = Tally::new(f, a);
    let mut summary = tally.meter();
    summary.extend(tally.counts());
    if let Some(regime) = f.current_regime() {
        summary.push(stat(format!("regime {}", regime.number)));
    }
    if let Some(epoch) = a
        .incumbent_baseline
        .as_ref()
        .and_then(|incumbent| a.epochs.iter().rev().find(|e| &e.baseline == incumbent))
    {
        summary.push(stat(format!(
            "incumbent {} since {}",
            epoch.baseline,
            stamp(epoch.at, false)
        )));
    }
    let mut lines = vec![summary];
    let prose = |lead: String, text: &str| {
        C::element(
            T::P,
            vec![(A::Class, "prose".into()), (A::Title, text.to_owned())],
            vec![C::element(T::B, Vec::new(), vec![lead.into()]), text.into()],
        )
    };
    if let Some(objective) = f.objective() {
        lines.push(vec![prose("Objective ".into(), objective)]);
    }
    if let Some(Conclusion { lesson, .. }) = f.conclusion() {
        lines.push(vec![prose(
            format!("Concluded {} ", stamp(lesson.at, false)),
            &lesson.text,
        )]);
    }
    let charts = charts::charts(f, a, s)?;
    if !charts.is_empty() {
        lines.push(charts);
    }
    let mut reviews = f.reviews().iter().collect::<Vec<_>>();
    reviews.sort_by_key(|(_, r)| r.lesson.is_some());
    if !reviews.is_empty() {
        let mut legend = vec![C::element(T::B, Vec::new(), vec!["Reviews".into()])];
        legend.extend(reviews.into_iter().map(|(slug, r)| C::Palette {
            key: record("review", slug),
            content: Box::new(C::element(
                T::A,
                vec![
                    (
                        A::Class,
                        if r.lesson.is_some() {
                            "review completed"
                        } else {
                            "review"
                        }
                        .into(),
                    ),
                    (A::Href, format!("{}#{}", url(s), record("review", slug))),
                    (A::Title, Depth::Hover.text(&r.charter)),
                ],
                vec![r.title.clone().into()],
            )),
        }));
        lines.push(legend);
    }
    Ok(vec![stats(lines)])
}
fn geometry(f: &Frontier, a: &Assay) -> BoardScene {
    let ids: BTreeMap<_, _> = f
        .hypotheses()
        .keys()
        .enumerate()
        .map(|(id, s)| (s, id))
        .collect();
    let mut lanes = BTreeMap::<&str, Vec<usize>>::new();
    let mut scene = BoardScene::default();
    let mut chronological: Vec<_> = f.hypotheses().iter().collect();
    chronological.sort_by_key(|(s, h)| (placed_at(h), (*s).clone()));
    let ranks: BTreeMap<_, _> = chronological
        .into_iter()
        .enumerate()
        .map(|(n, (s, _))| (s, u64::try_from(n).unwrap_or(u64::MAX)))
        .collect();
    for (slug, h) in f.hypotheses() {
        let id = ids[slug];
        lanes.entry(slug.lane()).or_default().push(id);
        scene.input.cells.push(Cell {
            key: record("hypothesis", slug),
            occupant: Occupant::Cell(id),
            title: h.title.clone(),
            size: u32::try_from(h.trials.len()).unwrap_or(u32::MAX),
        });
        scene.input.rank.push(ranks[slug]);
        let card = card(f, a, slug, h, Depth::Hover);
        scene.cell_classes.insert(id, card.class.clone());
        scene.cards.insert(id, card);
    }
    scene.input.lanes = lanes.into_values().collect();
    scene.input.precedence = f
        .edges()
        .keys()
        .map(|(from, to)| (ids[from], ids[to]))
        .collect();
    scene.input.drawn_edges.clone_from(&scene.input.precedence);
    for (slug, r) in f.reviews() {
        let key = record("review", slug);
        scene.input.sets.push(CellSet {
            key: key.clone(),
            members: r
                .members
                .iter()
                .filter_map(|h| ids.get(h).copied())
                .collect(),
        });
        scene.set_styles.insert(
            key,
            SetStyle {
                class: if r.lesson.is_some() {
                    "completed"
                } else {
                    "review"
                }
                .into(),
                marker: if r.lesson.is_some() {
                    Marker::Circle
                } else {
                    Marker::HollowCircle
                },
            },
        );
    }
    for slug in f.baselines().keys() {
        let mut holds: BTreeSet<_> = a
            .dockets
            .get(slug)
            .into_iter()
            .flat_map(|d| d.members.iter())
            .filter_map(|h| ids.get(h).copied())
            .collect();
        if let Some(epoch) = a.epochs.iter().find(|e| &e.baseline == slug) {
            let until = a.epochs.iter().find(|e| e.at > epoch.at).map(|e| e.at);
            holds.extend(
                f.hypotheses()
                    .iter()
                    .filter(|(_, h)| {
                        placed_at(h) >= epoch.at && until.is_none_or(|at| placed_at(h) < at)
                    })
                    .map(|(h, _)| ids[h]),
            );
        }
        let title = a
            .epochs
            .iter()
            .rev()
            .find(|epoch| &epoch.baseline == slug)
            .map_or_else(
                || format!("Baseline {slug}"),
                |epoch| format!("Epoch of {slug} · since {}", stamp(epoch.at, false)),
            );
        scene.input.separators.push(Separator {
            index: scene.input.separators.len(),
            holds: holds.into_iter().collect(),
            title,
            class: "epoch".into(),
        });
    }
    // A lone regime marks no boundary.
    for (index, r) in f
        .regimes()
        .iter()
        .enumerate()
        .filter(|_| f.regimes().len() > 1)
    {
        let until = f.regimes().get(index + 1).map(|r| r.at);
        let holds = f
            .hypotheses()
            .iter()
            .filter(|(_, h)| placed_at(h) >= r.at && until.is_none_or(|at| placed_at(h) < at))
            .map(|(h, _)| ids[h])
            .collect();
        let separator = scene.input.separators.len();
        let bridge = if r.bridge.is_some() {
            "bridged"
        } else {
            "unbridged"
        };
        scene.separator_tooltips.insert(
            separator,
            format!(
                "Regime {} · {bridge} · since {}\n{}",
                r.number,
                stamp(r.at, false),
                Depth::Hover.text(&r.gauge.contract)
            ),
        );
        scene.input.separators.push(Separator {
            index: separator,
            holds,
            title: format!("Regime {} · {bridge}", r.number),
            class: "regime".into(),
        });
    }
    scene
}
/// Lists the hypotheses an aggregate block stands for.
fn aggregates(f: &Frontier, scene: &mut BoardScene) {
    for cell in &scene.input.cells {
        if let Occupant::Aggregate(members) = &cell.occupant {
            let mut tip = members
                .iter()
                .take(CARD_LINKS)
                .filter_map(|&v| f.hypotheses().keys().nth(v).map(ToString::to_string))
                .collect::<Vec<_>>();
            if members.len() > CARD_LINKS {
                tip.push(format!("+{} more", members.len() - CARD_LINKS));
            }
            scene.cell_tooltips.insert(members[0], tip.join("\n"));
        }
    }
}
fn card(f: &Frontier, a: &Assay, slug: &Slug, h: &Hypothesis, depth: Depth) -> Card {
    let livery = Livery::of(h, a.hypotheses[slug]);
    let mut state = vec![livery.name().to_owned()];
    if let Some(d) = &h.disposition {
        state.push(stamp(d.at, false));
    } else if let Some(owner) = &h.ownership.owner {
        state.push(format!("owner {}", owner.name));
    }
    if let Some(docket) = &h.docket {
        state.push(format!("docket {docket}"));
    }
    if !h.trials.is_empty() {
        state.push(plural(h.trials.len(), "trial"));
    }
    let mut fields = Vec::new();
    for (label, text) in [("claim", &h.claim), ("mechanism", &h.mechanism)] {
        if let Some(text) = text {
            fields.push(Field::text(label, depth.text(text)));
        }
    }
    if let Some(d) = &h.disposition {
        fields.push(Field::text(
            format!("{} because", livery.name()),
            depth.text(&d.rationale),
        ));
    }
    if let Depth::Hover = depth
        && let Some((n, trial)) = h.trials.iter().rev().find(|(_, t)| t.void.is_none())
    {
        let comparisons = a.comparisons.get(&TrialId {
            hypothesis: slug.clone(),
            number: *n,
        });
        let mut body = readings(f, trial.regime, &trial.readings, comparisons);
        if !trial.note.is_empty() {
            body.push(C::classed(
                T::Span,
                "note",
                vec![depth.text(&trial.note).into()],
            ));
        }
        fields.push(Field {
            label: format!("latest trial #{n} · {}", stamp(trial.at, false)),
            body,
        });
    }
    let reviews = f
        .reviews()
        .iter()
        .filter(|(_, r)| r.members.contains(slug))
        .map(|(key, r)| {
            (
                record("review", key),
                C::classed(T::Li, "set", vec![r.title.clone().into()]),
            )
        })
        .collect::<Vec<_>>();
    if !reviews.is_empty() {
        fields.push(Field {
            label: "reviews".into(),
            body: vec![C::element(T::Ul, Vec::new(), vec![C::PaletteList(reviews)])],
        });
    }
    let after = f
        .edges()
        .iter()
        .filter(|((_, to), _)| to == slug)
        .map(|((from, _), edge)| (from, edge.carries.as_deref()))
        .collect::<Vec<_>>();
    if !after.is_empty() {
        let settled = after
            .iter()
            .filter(|(from, _)| f.hypotheses()[*from].disposition.is_some())
            .count();
        fields.push(Field {
            label: format!("after · {settled}/{} settled", after.len()),
            body: vec![links(f, a, &after)],
        });
    }
    let before = f
        .edges()
        .keys()
        .filter(|(from, _)| from == slug)
        .map(|(_, to)| (to, None))
        .collect::<Vec<_>>();
    if !before.is_empty() {
        fields.push(Field {
            label: format!("before · {}", before.len()),
            body: vec![links(f, a, &before)],
        });
    }
    Card {
        class: livery.name().into(),
        slug: slug.to_string(),
        title: h.title.clone(),
        state: state.join(" · "),
        fields,
    }
}
fn links(f: &Frontier, a: &Assay, links: &[(&Slug, Option<&str>)]) -> C {
    let mut items = links
        .iter()
        .take(CARD_LINKS)
        .map(|&(slug, carries)| {
            let mut children = vec![slug.to_string().into()];
            if let Some(carries) = carries.filter(|c| !c.is_empty()) {
                children.push(C::classed(
                    T::Span,
                    "carries",
                    vec![format!(" · {carries}").into()],
                ));
            }
            C::classed(
                T::Li,
                Livery::of(&f.hypotheses()[slug], a.hypotheses[slug]).name(),
                children,
            )
        })
        .collect::<Vec<_>>();
    if links.len() > CARD_LINKS {
        items.push(C::element(
            T::Li,
            Vec::new(),
            vec![format!("+{} more", links.len() - CARD_LINKS).into()],
        ));
    }
    C::element(T::Ul, Vec::new(), items)
}
/// Readings in gauge order, each with its unit and its change against the
/// baseline the trial was measured against.
fn readings(
    f: &Frontier,
    regime: RegimeNumber,
    values: &RecordedReadings,
    comparisons: Option<&BTreeMap<MetricKey, Comparison>>,
) -> Vec<C> {
    let gauge = f
        .regimes()
        .iter()
        .find(|r| r.number == regime)
        .map(|r| &r.gauge);
    values
        .iter()
        .map(|(key, value)| {
            let (text, class) = match value {
                Reading::Listed { value } => {
                    let unit = gauge
                        .and_then(|g| g.metrics.get(key))
                        .map_or(String::new(), |m| format!(" {}", m.unit));
                    let comparison = comparisons.and_then(|c| c.get(key));
                    let change = comparison
                        .and_then(|c| c.ratio)
                        .map(|ratio| format!(" {}", change(ratio)))
                        .unwrap_or_default();
                    let class = match comparison.and_then(|c| c.movement.as_ref()) {
                        Some(Movement::Better) => "reading better",
                        Some(Movement::Worse) => "reading worse",
                        Some(Movement::Equal) | None => "reading",
                    };
                    (
                        format!("{key} {}{unit}{change}", charts::significant(*value)),
                        class,
                    )
                }
                Reading::Unlisted { value } => (
                    format!(
                        "{key} {} (unlisted)",
                        match value {
                            Value::Number(n) => charts::significant(*n),
                            Value::Text(t) => t.clone(),
                        }
                    ),
                    "reading unlisted",
                ),
            };
            C::classed(T::Span, class, vec![text.into()])
        })
        .collect()
}
/// A ratio as a signed percentage change, such as `−1.7%`.
fn change(ratio: f64) -> String {
    let percent = (ratio - 1.0) * 100.0;
    let sign = if percent < 0.0 { "−" } else { "+" };
    let magnitude = percent.abs();
    if magnitude < 10.0 {
        format!("{sign}{magnitude:.1}%")
    } else {
        format!("{sign}{magnitude:.0}%")
    }
}
fn sheet_record(card: &Card) -> C {
    C::classed(T::Section, format!("record {}", card.class), card.content())
}
/// A hypothesis's record and, below its neighbourhood, its trial table.
fn sheet(
    f: &Frontier,
    a: &Assay,
    s: &Selection,
    slug: &Slug,
    h: &Hypothesis,
    selected: Option<u64>,
) -> (Vec<C>, Vec<C>) {
    let head = vec![sheet_record(&card(f, a, slug, h, Depth::Sheet))];
    if h.trials.is_empty() {
        return (head, Vec::new());
    }
    let headings = [
        "trial",
        "at",
        "regime",
        "against",
        "readings",
        "note",
        "artifacts",
        "void",
    ];
    let rows: Vec<_> = h
        .trials
        .iter()
        .map(|(number, trial)| {
            let comparisons = a.comparisons.get(&TrialId {
                hypothesis: slug.clone(),
                number: *number,
            });
            let text = |value: String| vec![C::Text(value)];
            (
                *number,
                trial.void.is_some(),
                [
                    vec![C::link(
                        format!("{}#trial.{slug}.{number}", url(s)),
                        vec![format!("#{number}").into()],
                    )],
                    text(stamp(trial.at, false)),
                    text(trial.regime.to_string()),
                    trial
                        .against
                        .as_ref()
                        .map(|b| {
                            vec![C::link(
                                format!("{}#{}", url(s), record("baseline", b)),
                                vec![b.to_string().into()],
                            )]
                        })
                        .unwrap_or_default(),
                    readings(f, trial.regime, &trial.readings, comparisons),
                    text(trial.note.clone()),
                    trial
                        .artifacts
                        .iter()
                        .map(|artifact| {
                            C::classed(T::Span, "artifact", vec![artifact.clone().into()])
                        })
                        .collect(),
                    trial.void.clone().map(text).unwrap_or_default(),
                ],
            )
        })
        .collect();
    let columns: Vec<_> = (0..headings.len())
        .filter(|&column| {
            matches!(column, 0..=2) || rows.iter().any(|(_, _, row)| !row[column].is_empty())
        })
        .collect();
    let body = rows
        .into_iter()
        .map(|(number, void, row)| {
            let mut classes = Vec::new();
            if selected == Some(number) {
                classes.push("selected");
            }
            if void {
                classes.push("void");
            }
            C::element(
                T::Tr,
                vec![(A::Class, classes.join(" "))],
                columns
                    .iter()
                    .map(|&column| C::element(T::Td, Vec::new(), row[column].clone()))
                    .collect(),
            )
        })
        .collect();
    let table = C::classed(
        T::Table,
        "trials",
        vec![
            C::element(
                T::Thead,
                Vec::new(),
                vec![C::element(
                    T::Tr,
                    Vec::new(),
                    columns
                        .iter()
                        .map(|&column| C::element(T::Th, Vec::new(), vec![headings[column].into()]))
                        .collect(),
                )],
            ),
            C::element(T::Tbody, Vec::new(), body),
        ],
    );
    (head, vec![table])
}
fn baseline(f: &Frontier, a: &Assay, slug: &Slug, b: &Baseline) -> Card {
    let epoch = a.epochs.iter().rev().find(|e| &e.baseline == slug);
    let role = if b.void.is_some() {
        "void"
    } else if a.incumbent_baseline.as_ref() == Some(slug) {
        "incumbent"
    } else if b
        .disposition
        .as_ref()
        .is_some_and(|d| matches!(d.decision, BaselineDecision::Rejected))
    {
        "rejected"
    } else if epoch.is_some() {
        "accepted"
    } else {
        "candidate"
    };
    let mut state = vec![role.to_owned()];
    state.push(format!("regime {}", b.regime));
    if let Some(epoch) = epoch {
        state.push(format!("since {}", stamp(epoch.at, false)));
    }
    let mut fields = vec![Field {
        label: "readings".into(),
        body: readings(f, b.regime, &b.readings, None),
    }];
    if let Some(note) = &b.note {
        fields.push(Field::text("note", note.clone()));
    }
    if let Some(reason) = &b.void {
        fields.push(Field::text("void because", reason.clone()));
    }
    if let Some(d) = &b.disposition {
        fields.push(Field::text("disposition", d.rationale.clone()));
    }
    if !b.artifacts.is_empty() {
        fields.push(Field::text("artifacts", b.artifacts.join("\n")));
    }
    if !b.adopts.is_empty() {
        let adopts = b.adopts.iter().map(|h| (h, None)).collect::<Vec<_>>();
        fields.push(Field {
            label: format!("adopts · {}", adopts.len()),
            body: vec![links(f, a, &adopts)],
        });
    }
    if let Some(docket) = a.dockets.get(slug) {
        fields.push(Field::text(
            "docket",
            format!(
                "{}/{} settled, drawn below",
                docket.settled,
                docket.members.len()
            ),
        ));
    }
    Card {
        class: "baseline".into(),
        slug: slug.to_string(),
        title: epoch.map_or_else(
            || format!("Baseline {slug}"),
            |_| format!("Epoch of {slug}"),
        ),
        state: state.join(" · "),
        fields,
    }
}
fn review(a: &Assay, slug: &Slug, r: &Review) -> Card {
    let mut state = vec![
        if r.lesson.is_some() {
            "completed"
        } else {
            "open"
        }
        .to_owned(),
    ];
    if let Some(owner) = &r.ownership.owner {
        state.push(format!("owner {}", owner.name));
    }
    let settled = r
        .members
        .iter()
        .filter(|h| a.hypotheses.get(*h) == Some(&HypothesisState::Settled))
        .count();
    state.push(format!("{settled}/{} settled", r.members.len()));
    let mut fields = vec![Field::text("charter", r.charter.clone())];
    if let Some(lesson) = &r.lesson {
        fields.push(Field::text(
            format!("lesson · {}", stamp(lesson.at, false)),
            lesson.text.clone(),
        ));
    }
    Card {
        class: if r.lesson.is_some() {
            "completed"
        } else {
            "review"
        }
        .into(),
        slug: slug.to_string(),
        title: r.title.clone(),
        state: state.join(" · "),
        fields,
    }
}
