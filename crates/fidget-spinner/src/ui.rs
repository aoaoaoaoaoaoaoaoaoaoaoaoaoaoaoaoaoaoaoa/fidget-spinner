//! Fidget Spinner's navigator projections; trebuchet owns transport, geometry and chrome.

mod charts;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{Mutex, PoisonError},
};

use fidget_spinner_core::{
    Annotation, Baseline, Body, Data, Frontier, Id, Name, Op, Reading, RecordedReadings,
    RegimeNumber, Shown, Slug, Trial, Work, schema::Show,
};
use libmcp::DetailLevel;
use serde_json::{Value as Json, json};
use time::OffsetDateTime;
use trebuchet::{
    board::{
        self, ActionResult, Attribute as A, BoardRoutes, BoardScene, BoardSource, Card,
        Content as C, Field, Fold, IndexRow, IndexScene, Marker, Place, Query, RouteTarget,
        SetStyle, StatusCode, Tag as T, Theme, button, count, meter, stats,
    },
    layout::{Cell, CellSet, Occupant, Separator, elide, focused, foreground},
};

use crate::{
    inspect::{Lens, stamp},
    mcp::{Fault, standing},
    store::{self, Ledger, Listing, Store, StoreError},
};

const STYLE: &str = include_str!("ui/style.css");
const FAVICON: &str = include_str!("ui/favicon.svg");
/// Characters of free text a hover card keeps.
const CARD_TEXT: usize = 320;
/// Records a hover card or aggregate tooltip lists.
const CARD_LINKS: usize = 12;
/// Resolutions since the second-latest epoch began.
const HORIZON: Horizon = Horizon::Epochs(2);
/// Work states in display order, then void.
const SHOWN: [&str; 7] = [
    "waiting", "ready", "running", "tested", "reported", "settled", "void",
];

struct Navigator {
    path: PathBuf,
    ledgers: Mutex<BTreeMap<i64, Ledger<Frontier>>>,
}

/// How far back a board shows resolved work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Horizon {
    /// Resolutions since the n-th latest epoch began; none for zero.
    Epochs(usize),
    /// Every resolution.
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
    /// An iset whose members, at any depth, the board is scoped to.
    under: Option<Slug>,
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

/// A frontier's work by shown state, and its isets' progress.
struct Tally {
    counts: [usize; SHOWN.len()],
    isets: u64,
    settled: u64,
    running: u64,
}

impl Tally {
    fn new(f: &Frontier) -> Self {
        let mut tally = Self {
            counts: [0; SHOWN.len()],
            isets: 0,
            settled: 0,
            running: 0,
        };
        for (id, work) in f.work() {
            let shown = shown(f, *id);
            if let Some(n) = SHOWN.iter().position(|s| *s == shown) {
                tally.counts[n] += 1;
            }
            if work.iset().is_some() && work.void.is_none() {
                tally.isets += 1;
                tally.settled += u64::from(shown == "settled");
                tally.running += u64::from(shown == "running");
            }
        }
        tally
    }
    fn meter(&self) -> Vec<C> {
        meter(
            &[("settled", self.settled)],
            self.running,
            self.isets,
            format!(
                "{} settled + {} running of {} isets",
                self.settled, self.running, self.isets
            ),
        )
    }
    fn counts(&self) -> Vec<C> {
        SHOWN
            .iter()
            .zip(self.counts)
            .filter(|(_, n)| *n > 0)
            .map(|(state, n)| count(state, n))
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
    let mut s = Selection {
        id,
        horizon: HORIZON,
        under: None,
    };
    for (key, value) in query.pairs() {
        match &*key {
            "horizon" => {
                s.horizon =
                    match &*value {
                        "all" => Horizon::All,
                        epochs => Horizon::Epochs(epochs.parse().map_err(|_| {
                            format!("horizon is an epoch count or all, not {epochs}")
                        })?),
                    };
            }
            "under" => {
                s.under = Some(
                    value
                        .parse()
                        .map_err(|e| format!("under names an iset: {e}"))?,
                );
            }
            other => return Err(format!("unknown navigator selector {other}")),
        }
    }
    Ok(s)
}
fn target(s: &Selection) -> RouteTarget {
    RouteTarget {
        segment: s.id.to_string(),
        query: query(s),
    }
}
/// The canonical query of a selection, empty for the defaults.
fn query(s: &Selection) -> Query {
    let mut pairs = Vec::new();
    if s.horizon != HORIZON {
        pairs.push(("horizon", s.horizon.token()));
    }
    if let Some(under) = &s.under {
        pairs.push(("under", under.to_string()));
    }
    Query::new(pairs)
}
fn url(s: &Selection) -> String {
    format!("/frontier/{}{}", s.id, query(s))
}
fn place(listing: &Listing) -> Place {
    Place {
        project: listing.address.project.to_path_buf(),
        name: listing.address.slug.to_string(),
    }
}
/// A record's anchor on a board: its kind and incarnation, stable across renames.
fn anchor(kind: &str, id: Id) -> trebuchet::Slug {
    format!("{kind}.{id}")
        .parse()
        .unwrap_or_else(|error| unreachable!("kind and numeric identity form a slug: {error}"))
}
fn stat(text: String) -> C {
    C::classed(T::Span, "stat", vec![text.into()])
}
fn plural(n: usize, noun: &str) -> String {
    format!("{n} {noun}{}", if n == 1 { "" } else { "s" })
}
fn shown(f: &Frontier, id: Id) -> &'static str {
    f.shown(id).map_or("waiting", Shown::as_str)
}
/// When work resolved: a trial's report or an iset's judgment.
fn resolved_at(work: &Work) -> Option<OffsetDateTime> {
    match &work.body {
        Body::Trial(t) => t.report.as_ref().map(|r| r.at.time),
        Body::Iset(i) => i.judgment.as_ref().map(|j| j.at.time),
    }
}
/// When work left the open set, if it has.
fn closed_at(work: &Work) -> Option<OffsetDateTime> {
    resolved_at(work).or_else(|| work.void.as_ref().map(|v| v.at.time))
}
/// Where work sits in time: when it closed, else when it was created.
fn placed_at(work: &Work) -> OffsetDateTime {
    closed_at(work).unwrap_or(work.created.time)
}

impl Navigator {
    fn store(&self) -> Result<Option<Store>, Fault> {
        store::read_only(&self.path)
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
        if !matches!(action, "archive" | "unarchive") {
            return Ok(None);
        }
        let op = Op::parse(json!({"op":action})).map_err(Fault::Arguments)?;
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
            &[op],
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
        let mut folded = Vec::new();
        if let Some(store) = self.store()? {
            let mut ledgers = self.ledgers.lock().unwrap_or_else(PoisonError::into_inner);
            for listing in store.listings()? {
                let ledger = ledgers.entry(listing.id).or_default();
                store.refresh(&listing.address, ledger)?;
                let f = ledger.state();
                let tally = Tally::new(f);
                let mut row = IndexRow {
                    href: format!("/frontier/{}", listing.id),
                    place: place(&listing),
                    about: f.objective().map(str::to_owned),
                    columns: vec![tally.meter(), tally.counts()],
                    updated: listing.updated,
                    action: Vec::new(),
                };
                match standing(f) {
                    "archived" => {
                        row.action = vec![button(
                            format!("/frontier/{}/unarchive", listing.id),
                            "Unarchive",
                            "Unarchiving…",
                            "Return this frontier to the index and accept its writes",
                        )];
                        folded.push((Reverse(listing.updated), row));
                    }
                    "concluded" => {
                        row.action = vec![
                            stat("concluded".into()),
                            button(
                                format!("/frontier/{}/archive", listing.id),
                                "Archive",
                                "Archiving…",
                                "Hide this frontier and refuse its writes; keep all data",
                            ),
                        ];
                        rows.push(((true, Reverse(listing.updated)), row));
                    }
                    _ => rows.push(((false, Reverse(listing.updated)), row)),
                }
            }
        }
        rows.sort_by_key(|(order, _)| *order);
        folded.sort_by_key(|(order, _)| *order);
        Ok(IndexScene {
            rows: rows.into_iter().map(|(_, row)| row).collect(),
            fold: Fold {
                label: "Archived".into(),
                rows: folded.into_iter().map(|(_, row)| row).collect(),
            },
            empty:
                "No visible frontiers. Start one with frontier.patch through the Fidget Spinner MCP."
                    .into(),
        })
    }
    fn scene(&self, s: &Selection) -> Result<Option<BoardScene>, Fault> {
        let Some((listing, ledger)) = self.ledger(s.id)? else {
            return Ok(None);
        };
        let board = Board::new(ledger.state(), s);
        let mut scene = board.geometry();
        scene.place = place(&listing);
        scene.controls = controls(s);
        scene.above = board.overview()?;
        scene.empty = match &board.scope {
            Scope::Missing(slug) => format!("No iset named {slug}."),
            Scope::Frontier | Scope::Under(_) => "No work within this horizon.".into(),
        };
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
        let board = Board::new(f, s);
        let mut scene = board.geometry();
        scene.place = place(&listing);
        let Some((kind, raw)) = selected.as_str().split_once('.') else {
            return Ok(None);
        };
        let Ok(id) = raw.parse::<Id>() else {
            return Ok(None);
        };
        match kind {
            "work" if f.work().contains_key(&id) => {
                scene.input = if let Some(&cell) = board.index.get(&id) {
                    foreground(
                        &scene.input,
                        cell,
                        |_| true,
                        |_| None::<()>,
                        |n| plural(n, "record"),
                    )
                    .unwrap_or_else(|| unreachable!("a drawn cell is always shown"))
                } else {
                    let homes = board.homes(id);
                    focused(&scene.input, &homes.into_iter().collect())
                };
                (scene.above, scene.below) = board.sheet(id);
            }
            "baseline" => {
                let Some(b) = f.baselines().get(&id) else {
                    return Ok(None);
                };
                let adopted: BTreeSet<usize> = f
                    .elections()
                    .iter()
                    .filter(|e| e.baseline == id)
                    .flat_map(|e| e.adopts.keys())
                    .flat_map(|iset| board.homes(*iset))
                    .collect();
                if !adopted.is_empty() {
                    scene.input = focused(&scene.input, &adopted);
                }
                scene.above = vec![sheet_record(&board.baseline(id, b))];
            }
            _ => return Ok(None),
        }
        Ok(Some(scene))
    }
}

fn controls(s: &Selection) -> Vec<C> {
    let mut choices = Horizon::CHOICES.to_vec();
    if !choices.contains(&s.horizon) {
        choices.push(s.horizon);
        choices.sort_unstable();
    }
    let mut form = vec![C::classed(
        T::Span,
        "fog",
        vec![C::element(
            T::Label,
            vec![(
                A::Title,
                "Resolved work to show, by the epochs it was resolved in".into(),
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
    )];
    if let Some(under) = &s.under {
        form.push(C::element(
            T::Input,
            vec![
                (A::Type, "hidden".into()),
                (A::Name, "under".into()),
                (A::Value, under.to_string()),
            ],
            Vec::new(),
        ));
    }
    vec![C::element(
        T::Form,
        vec![
            (A::Method, "get".into()),
            (A::Action, format!("/frontier/{}", s.id)),
        ],
        form,
    )]
}
/// Annotations newest first, one per line after its time.
fn annotation_lines(annotations: &[Annotation]) -> String {
    annotations
        .iter()
        .rev()
        .map(|a| format!("{} · {}", stamp(a.at.time, false), a.text))
        .collect::<Vec<_>>()
        .join("\n")
}

fn sheet_record(card: &Card) -> C {
    C::classed(T::Section, format!("record {}", card.class), card.content())
}

/// What a board draws.
enum Scope {
    /// The whole frontier.
    Frontier,
    /// An unknown iset name.
    Missing(Slug),
    /// An iset and its records at any depth.
    Under(Id),
}

/// A frontier as a board. Every trial within the horizon is a cell; an iset
/// is a wall around the cells below it, or a cell itself while none of them
/// is drawn.
struct Board<'f> {
    f: &'f Frontier,
    lens: Lens<'f>,
    url: String,
    scope: Scope,
    /// Drawn records in creation order; a cell's index is its identity.
    cells: Vec<Id>,
    index: BTreeMap<Id, usize>,
    /// Walled isets in creation order, with the cells below them.
    walls: Vec<(Id, BTreeSet<usize>)>,
    /// Line style rank of each iset schema title, the commonest solid.
    kinds: BTreeMap<&'f str, usize>,
}

/// The isets holding a record that hold none of its other isets.
fn innermost(f: &Frontier, id: Id) -> Vec<Id> {
    let containers = f.containers(id);
    containers
        .iter()
        .filter(|&&c| {
            !containers
                .iter()
                .any(|&d| d != c && f.below(c).contains(&d))
        })
        .copied()
        .collect()
}

/// The title of a work record's schema.
fn schema_title<'f>(f: &'f Frontier, work: &Work) -> Option<&'f str> {
    work.binding
        .as_ref()
        .and_then(|b| f.definition(b))
        .and_then(|d| d.schema.title.as_deref())
}

/// A line sample for the kinds legend.
fn sample() -> C {
    C::element(
        T::Svg,
        vec![
            (A::Width, "24".into()),
            (A::Height, "8".into()),
            (A::ViewBox, "0 0 24 8".into()),
        ],
        vec![C::element(
            T::Line,
            vec![
                (A::X1, "0".into()),
                (A::Y1, "4".into()),
                (A::X2, "24".into()),
                (A::Y2, "4".into()),
            ],
            Vec::new(),
        )],
    )
}

impl<'f> Board<'f> {
    fn new(f: &'f Frontier, s: &Selection) -> Self {
        let scope = match &s.under {
            None => Scope::Frontier,
            Some(slug) => match f.named(&Name::Slug(slug.clone())) {
                Some((id, work)) if work.iset().is_some() => Scope::Under(id),
                _ => Scope::Missing(slug.clone()),
            },
        };
        let within = match &scope {
            Scope::Frontier => None,
            Scope::Missing(_) => Some(BTreeSet::new()),
            Scope::Under(id) => {
                let mut below = f.below(*id);
                below.insert(*id);
                Some(below)
            }
        };
        let in_scope = |id: &Id| within.as_ref().is_none_or(|w| w.contains(id));
        let shows = |at: OffsetDateTime| match s.horizon {
            Horizon::All => true,
            Horizon::Epochs(0) => false,
            Horizon::Epochs(n) => f
                .elections()
                .iter()
                .rev()
                .take(n)
                .next_back()
                .is_none_or(|e| at >= e.at.time),
        };
        // Open, or closed within the horizon.
        let live = |id: &Id| closed_at(&f.work()[id]).is_none_or(shows);
        // A trial lives with its innermost isets; one in none, by itself.
        let mut drawn = f
            .work()
            .iter()
            .filter(|(id, work)| work.trial().is_some() && in_scope(id))
            .filter(|(id, _)| {
                let homes = innermost(f, **id);
                if homes.is_empty() {
                    live(id)
                } else {
                    homes.iter().any(live)
                }
            })
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        // Fewer records below first: a live iset with nothing drawn below is drawn itself.
        let mut isets = f
            .work()
            .iter()
            .filter(|(id, work)| work.iset().is_some() && in_scope(id))
            .map(|(id, _)| (*id, f.below(*id)))
            .collect::<Vec<_>>();
        isets.sort_by_key(|(id, below)| (below.len(), *id));
        for (id, below) in &isets {
            if live(id) && below.is_disjoint(&drawn) {
                drawn.insert(*id);
            }
        }
        let cells = drawn.into_iter().collect::<Vec<_>>();
        let index = cells
            .iter()
            .enumerate()
            .map(|(c, id)| (*id, c))
            .collect::<BTreeMap<_, _>>();
        let mut walls = isets
            .iter()
            .filter_map(|(id, below)| {
                let members = below
                    .iter()
                    .filter_map(|m| index.get(m).copied())
                    .collect::<BTreeSet<_>>();
                (!members.is_empty()).then_some((*id, members))
            })
            .collect::<Vec<_>>();
        walls.sort_by_key(|(id, _)| *id);
        let mut counts = BTreeMap::<&str, usize>::new();
        for work in f.work().values().filter(|w| w.iset().is_some()) {
            if let Some(title) = schema_title(f, work) {
                *counts.entry(title).or_default() += 1;
            }
        }
        let mut ranked = counts.into_iter().collect::<Vec<_>>();
        ranked.sort_by_key(|&(title, count)| (Reverse(count), title));
        Self {
            f,
            lens: Lens::new(f, json!({}), DetailLevel::Full),
            url: url(s),
            scope,
            cells,
            index,
            walls,
            kinds: ranked
                .into_iter()
                .enumerate()
                .map(|(rank, (title, _))| (title, rank))
                .collect(),
        }
    }

    /// The class giving an iset its kind's line style.
    fn kind(&self, id: Id) -> String {
        schema_title(self.f, &self.f.work()[&id])
            .and_then(|title| self.kinds.get(title))
            .map_or_else(String::new, |rank| format!("kind{rank}"))
    }

    /// The cells drawing a record: its own, or those its wall holds.
    fn homes(&self, id: Id) -> Vec<usize> {
        if let Some(&cell) = self.index.get(&id) {
            return vec![cell];
        }
        self.walls
            .iter()
            .find(|(wall, _)| *wall == id)
            .map(|(_, members)| members.iter().copied().collect())
            .unwrap_or_default()
    }

    fn trials(&self, id: Id) -> Vec<Id> {
        self.f
            .members(id)
            .filter(|m| self.f.work().get(m).is_some_and(|w| w.trial().is_some()))
            .copied()
            .collect()
    }

    fn name(&self, id: Id) -> String {
        self.f.name_of(id).unwrap_or_else(|| "removed".into())
    }

    /// The record's own display text: an iset's title or a trial's synopsis.
    fn given(&self, id: Id) -> Option<&'f str> {
        match &self.f.work()[&id].body {
            Body::Iset(iset) => iset.title.as_deref(),
            Body::Trial(trial) => trial.synopsis.as_deref(),
        }
    }

    /// The board's words for a record: its display text, else its headline or name.
    fn label(&self, id: Id) -> String {
        self.given(id).map_or_else(
            || {
                self.lens
                    .headline(id)
                    .unwrap_or_else(|| self.f.work()[&id].name.to_string())
            },
            str::to_owned,
        )
    }

    /// A cell's words: a trial's synopsis fills its card alone; otherwise the title.
    fn face(&self, id: Id) -> String {
        match &self.f.work()[&id].body {
            Body::Trial(Trial {
                synopsis: Some(synopsis),
                ..
            }) => synopsis.clone(),
            _ => self.title(id),
        }
    }

    /// The board's words for a record, then badge values.
    fn title(&self, id: Id) -> String {
        let mut title = self.label(id);
        if let Json::Object(badges) = self.lens.badges(&self.f.work()[&id]) {
            for value in badges.values().filter_map(Json::as_str) {
                title.push_str(" · ");
                title.push_str(value);
            }
        }
        title
    }

    /// Inputs as precedences between cells, a walled iset standing for its
    /// earliest cell as a target and its latest as a source; skips any that
    /// would close a cycle.
    fn edges(&self, rank: &[u64]) -> Vec<(usize, usize)> {
        let reach = |id: &Id, latest: bool| {
            let homes = self.homes(*id).into_iter();
            if latest {
                homes.max_by_key(|&c| rank[c])
            } else {
                homes.min_by_key(|&c| rank[c])
            }
        };
        let mut successors = vec![BTreeSet::new(); self.cells.len()];
        let mut edges = Vec::new();
        for (id, work) in self.f.work() {
            let Some(to) = reach(id, false) else {
                continue;
            };
            for input in &work.inputs {
                if let Some(from) = reach(input, true)
                    && from != to
                    && !successors[from].contains(&to)
                    && !reaches(&successors, to, from)
                {
                    successors[from].insert(to);
                    edges.push((from, to));
                }
            }
        }
        edges
    }

    fn geometry(&self) -> BoardScene {
        let f = self.f;
        let mut scene = BoardScene::default();
        let n = self.cells.len();
        let own = |c: usize| placed_at(&f.work()[&self.cells[c]]);
        // Cells sharing walls form a group, placed at its latest record's time.
        let mut parent = (0..n).collect::<Vec<_>>();
        let root = |parent: &[usize], mut c: usize| {
            while parent[c] != c {
                c = parent[c];
            }
            c
        };
        for (_, members) in &self.walls {
            let mut members = members.iter().copied();
            if let Some(first) = members.next() {
                for other in members {
                    let (a, b) = (root(&parent, first), root(&parent, other));
                    parent[a.max(b)] = a.min(b);
                }
            }
        }
        let groups = (0..n).map(|c| root(&parent, c)).collect::<Vec<_>>();
        let mut latest = BTreeMap::<usize, OffsetDateTime>::new();
        for (c, &group) in groups.iter().enumerate() {
            let at = own(c);
            latest
                .entry(group)
                .and_modify(|t| *t = (*t).max(at))
                .or_insert(at);
        }
        let time = |c: usize| latest[&groups[c]];
        // Within a group a cell follows its innermost wall, and walls their first record.
        let innermost = (0..n)
            .map(|c| {
                self.walls
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, members))| members.contains(&c))
                    .min_by_key(|(w, (_, members))| (members.len(), *w))
                    .map(|(w, _)| w)
            })
            .collect::<Vec<_>>();
        let opened = self
            .walls
            .iter()
            .map(|(_, members)| members.iter().map(|&c| own(c)).min())
            .collect::<Vec<_>>();
        let mut order = (0..n).collect::<Vec<_>>();
        order.sort_by_key(|&c| {
            let wall = innermost[c];
            (
                time(c),
                groups[c],
                wall.and_then(|w| opened[w]),
                wall,
                own(c),
                c,
            )
        });
        scene.input.rank = vec![0; n];
        for (rank, &c) in order.iter().enumerate() {
            scene.input.rank[c] = u64::try_from(rank).unwrap_or(u64::MAX);
        }
        let mut lanes = BTreeMap::<&str, Vec<usize>>::new();
        for (c, id) in self.cells.iter().enumerate() {
            let work = &f.work()[id];
            lanes.entry(work.name.lane()).or_default().push(c);
            let card = self.card(*id, Depth::Hover);
            let (class, size) = if work.iset().is_some() {
                (
                    format!("{} iset {}", card.class, self.kind(*id)),
                    self.trials(*id).len(),
                )
            } else {
                (card.class.clone(), 0)
            };
            scene.input.cells.push(Cell {
                key: anchor("work", *id),
                occupant: Occupant::Cell(c),
                title: self.face(*id),
                size: u32::try_from(size).unwrap_or(u32::MAX),
            });
            scene.cell_classes.insert(c, class);
            scene.cards.insert(c, card);
        }
        scene.input.lanes = lanes.into_values().collect();
        scene.input.precedence = self.edges(&scene.input.rank);
        scene.input.drawn_edges.clone_from(&scene.input.precedence);
        for (id, members) in &self.walls {
            let key = anchor("work", *id);
            let card = self.card(*id, Depth::Hover);
            scene.set_styles.insert(
                key.clone(),
                SetStyle {
                    class: format!("{} {}", card.class, self.kind(*id)),
                    marker: Marker::default(),
                },
            );
            scene.input.sets.push(CellSet {
                key: key.clone(),
                members: members.clone(),
                label: Some(self.label(*id)),
            });
            scene.set_cards.insert(key, card);
        }
        self.separators(&mut scene, time);
        scene
    }

    /// Elections draw thin separators; regime changes draw thick ones.
    fn separators(&self, scene: &mut BoardScene, time: impl Fn(usize) -> OffsetDateTime) {
        let f = self.f;
        let within = |from: OffsetDateTime, until: Option<OffsetDateTime>| {
            (0..self.cells.len())
                .filter(|&c| {
                    let at = time(c);
                    at >= from && until.is_none_or(|until| at < until)
                })
                .collect::<Vec<_>>()
        };
        let elections = f.elections();
        for (n, e) in elections.iter().enumerate() {
            let holds = within(e.at.time, elections.get(n + 1).map(|e| e.at.time));
            scene.input.separators.push(Separator {
                index: scene.input.separators.len(),
                holds,
                title: format!(
                    "Epoch of {} · since {}",
                    self.name(e.baseline),
                    stamp(e.at.time, false)
                ),
                class: "epoch".into(),
            });
        }
        // A lone regime marks no boundary.
        let regimes = f.regimes();
        for (n, r) in regimes.iter().enumerate().filter(|_| regimes.len() > 1) {
            let holds = within(r.at.time, regimes.get(n + 1).map(|r| r.at.time));
            let index = scene.input.separators.len();
            let bridge = if r.bridge.is_some() {
                "bridged"
            } else {
                "unbridged"
            };
            scene.separator_tooltips.insert(
                index,
                format!(
                    "Regime {} · {bridge} · since {}\n{}",
                    r.number,
                    stamp(r.at.time, false),
                    Depth::Hover.text(&r.gauge.contract)
                ),
            );
            scene.input.separators.push(Separator {
                index,
                holds,
                title: format!("Regime {} · {bridge}", r.number),
                class: "regime".into(),
            });
        }
    }

    /// Counts, regime, incumbent baseline and the iset kinds drawn, then
    /// objective, conclusion and KPI tiles.
    fn overview(&self) -> Result<Vec<C>, Fault> {
        let f = self.f;
        let tally = Tally::new(f);
        let mut summary = tally.meter();
        summary.extend(tally.counts());
        if let Some(regime) = f.current_regime() {
            summary.push(stat(format!("regime {}", regime.number)));
        }
        if let Some((election, baseline)) = f.incumbent() {
            summary.push(stat(format!(
                "incumbent {} since {}",
                self.name(baseline),
                stamp(f.elections()[election].at.time, false)
            )));
        }
        let kinds = self
            .walls
            .iter()
            .map(|(id, _)| *id)
            .chain(
                self.cells
                    .iter()
                    .copied()
                    .filter(|id| f.work()[id].iset().is_some()),
            )
            .filter_map(|id| schema_title(f, &f.work()[&id]))
            .filter_map(|title| Some((*self.kinds.get(title)?, title)))
            .collect::<BTreeSet<_>>();
        if kinds.len() > 1 {
            summary.push(C::classed(
                T::Span,
                "kinds",
                kinds
                    .into_iter()
                    .map(|(rank, title)| {
                        C::classed(T::Span, format!("kind{rank}"), vec![sample(), title.into()])
                    })
                    .collect(),
            ));
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
        if let Some(conclusion) = f.conclusion() {
            lines.push(vec![prose(
                format!("Concluded {} ", stamp(conclusion.at.time, false)),
                &conclusion.lesson,
            )]);
        }
        let charts = charts::charts(f, &self.url)?;
        if !charts.is_empty() {
            lines.push(charts);
        }
        Ok(vec![stats(lines)])
    }

    /// A work record's card: a hover card, or a sheet with the whole record.
    fn card(&self, id: Id, depth: Depth) -> Card {
        let f = self.f;
        let work = &f.work()[&id];
        let class = shown(f, id);
        let marks = self.lens.marks(id);
        let mut state = vec![class.to_owned()];
        if let Some(at) = closed_at(work) {
            state.push(stamp(at, false));
        } else if let Some(owner) = &work.owner {
            state.push(format!("owner {}", owner.name));
        }
        let trials = self.trials(id);
        if !trials.is_empty() {
            state.push(plural(trials.len(), "trial"));
        }
        state.extend(marks.iter().cloned());
        let mut fields = Vec::new();
        if let Some(void) = &work.void {
            fields.push(Field::text("void because", depth.text(&void.reason)));
        }
        if let (Depth::Hover, Some(_), Some((field, text))) =
            (depth, self.given(id), self.lens.headline_field(id))
        {
            fields.push(Field::text(field, depth.text(&text)));
        }
        match (&work.body, depth) {
            (Body::Iset(iset), Depth::Sheet) => {
                fields.extend(self.data(work, &iset.data, depth));
                fields.extend(self.answer(id, depth));
            }
            (Body::Iset(_), Depth::Hover) => {
                fields.extend(self.answer(id, depth));
                if let Some(latest) = trials
                    .iter()
                    .filter_map(|t| {
                        let w = &f.work()[t];
                        let report = w.trial()?.report.as_ref()?;
                        w.void.is_none().then_some((report.at.time, *t))
                    })
                    .max()
                {
                    fields.extend(self.report(latest.1, depth));
                }
            }
            (Body::Trial(trial), _) => {
                if let (Depth::Sheet, Some(plan)) = (depth, trial.plan()) {
                    let n = trial.plans.len();
                    let mut planned = stamp(plan.at.time, false);
                    if n > 1 {
                        planned = format!("{planned} · latest of {n} revisions");
                    }
                    fields.push(Field::text("plan", planned));
                    fields.extend(self.data(work, &plan.data, depth));
                }
                fields.extend(self.report(id, depth));
            }
        }
        if let (Depth::Sheet, Some(owner)) = (depth, &work.owner) {
            fields.push(Field::text(
                "owner",
                owner
                    .instructions
                    .as_ref()
                    .map_or_else(|| owner.name.clone(), |i| format!("{}\n{}", owner.name, i)),
            ));
        }
        if matches!(depth, Depth::Sheet) && !work.annotations.is_empty() {
            fields.push(Field::text(
                "annotations",
                annotation_lines(&work.annotations),
            ));
        }
        let within = f.containers(id);
        if !within.is_empty() {
            fields.push(Field {
                label: "in".into(),
                body: vec![self.links(&within)],
            });
        }
        let after: Vec<Id> = work.inputs.iter().copied().collect();
        if !after.is_empty() {
            let done = after.iter().filter(|i| f.done(**i)).count();
            fields.push(Field {
                label: format!("after · {done}/{} done", after.len()),
                body: vec![self.links(&after)],
            });
        }
        let before = f.waiting_on(id);
        if !before.is_empty() {
            fields.push(Field {
                label: format!("before · {}", before.len()),
                body: vec![self.links(&before)],
            });
        }
        let marked = marks.iter().any(|m| m != "withdrawn" && m != "invalidated");
        Card {
            class: if marked {
                format!("{class} marked")
            } else {
                class.into()
            },
            slug: work.name.to_string(),
            title: self.title(id),
            state: state.join(" · "),
            fields,
        }
    }

    /// Data fields, except hidden ones, as text: text whole, anything else as
    /// compact JSON with references by current name.
    fn data(&self, work: &Work, data: &Data, depth: Depth) -> Vec<Field> {
        let hidden: Vec<&str> = work
            .binding
            .as_ref()
            .and_then(|b| self.f.definition(b))
            .map(|d| d.schema.shown(Show::Hidden))
            .unwrap_or_default();
        let Some(fields) = data.fields() else {
            return vec![Field::text("data", depth.text(&self.compact(data)))];
        };
        fields
            .iter()
            .filter(|(key, _)| !hidden.contains(&key.as_str()))
            .map(|(key, value)| {
                let text = match value {
                    Data::Text(text) => text.clone(),
                    value => self.compact(value),
                };
                Field::text(key.clone(), depth.text(&text))
            })
            .collect()
    }

    fn compact(&self, data: &Data) -> String {
        data.compact(&|r| self.f.ref_name(r))
    }

    fn answer(&self, id: Id, depth: Depth) -> Option<Field> {
        let judgment = self.f.work()[&id].iset()?.judgment.as_ref()?;
        let text = judgment.answer.fields().map_or_else(
            || self.compact(&judgment.answer),
            |fields| {
                fields
                    .iter()
                    .map(|(key, value)| match value {
                        Data::Text(text) => format!("{key}: {}", depth.text(text)),
                        value => format!("{key}: {}", depth.text(&self.compact(value))),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            },
        );
        Some(Field::text(
            format!("answer · {}", stamp(judgment.at.time, false)),
            text,
        ))
    }

    /// A trial's report: readings with their change against its control,
    /// then its data.
    fn report(&self, id: Id, depth: Depth) -> Vec<Field> {
        let work = &self.f.work()[&id];
        let Some(trial) = work.trial() else {
            return Vec::new();
        };
        let Some(report) = &trial.report else {
            return Vec::new();
        };
        let mut body = self.readings(trial, &report.readings, true);
        if let Depth::Hover = depth
            && let Some(headline) = self.lens.headline(id)
        {
            body.push(C::classed(
                T::Span,
                "note",
                vec![depth.text(&headline).into()],
            ));
        }
        let mut fields = vec![Field {
            label: format!(
                "report {} · {}",
                self.name(id),
                stamp(report.at.time, false)
            ),
            body,
        }];
        if let Depth::Sheet = depth {
            if !trial.plans.is_empty() {
                fields.push(Field::text(
                    "executed",
                    report.plan.map_or_else(
                        || "unknown plan revision".to_owned(),
                        |k| format!("plan revision {} of {}", k + 1, trial.plans.len()),
                    ),
                ));
            }
            fields.extend(self.data(work, &report.data, depth));
            if !report.artifacts.is_empty() {
                fields.push(Field::text("artifacts", report.artifacts.join("\n")));
            }
        }
        fields
    }

    /// Readings in gauge order, each with its unit and, for a trial, its
    /// change against its control.
    fn readings(&self, trial: &Trial, values: &RecordedReadings, compare: bool) -> Vec<C> {
        let comparisons = if compare {
            self.lens.comparisons(trial, false)
        } else {
            Json::Null
        };
        readings(self.f, trial.regime, values, &comparisons)
    }

    fn links(&self, ids: &[Id]) -> C {
        let mut items = ids
            .iter()
            .take(CARD_LINKS)
            .map(|&id| {
                C::classed(
                    T::Li,
                    shown(self.f, id),
                    vec![C::link(
                        format!("{}#{}", self.url, anchor("work", id)),
                        vec![self.name(id).into()],
                    )],
                )
            })
            .collect::<Vec<_>>();
        if ids.len() > CARD_LINKS {
            items.push(C::element(
                T::Li,
                Vec::new(),
                vec![format!("+{} more", ids.len() - CARD_LINKS).into()],
            ));
        }
        C::element(T::Ul, Vec::new(), items)
    }

    /// A work record's sheet and, below its neighbourhood, the trials it
    /// holds or shares an iset with.
    fn sheet(&self, id: Id) -> (Vec<C>, Vec<C>) {
        let head = vec![sheet_record(&self.card(id, Depth::Sheet))];
        let work = &self.f.work()[&id];
        let (holder, selected) = if work.iset().is_some() {
            (Some(id), None)
        } else {
            (innermost(self.f, id).into_iter().next(), Some(id))
        };
        let Some(holder) = holder else {
            return (head, Vec::new());
        };
        let trials = self.trials(holder);
        if trials.is_empty() {
            return (head, Vec::new());
        }
        (head, vec![self.table(&trials, selected)])
    }

    fn table(&self, trials: &[Id], selected: Option<Id>) -> C {
        let headings = [
            "trial",
            "state",
            "at",
            "regime",
            "control",
            "readings",
            "report",
            "artifacts",
        ];
        let text = |value: String| vec![C::Text(value)];
        let rows: Vec<_> = trials
            .iter()
            .filter_map(|&id| {
                let work = &self.f.work()[&id];
                let trial = work.trial()?;
                let report = trial.report.as_ref();
                let control = report
                    .and_then(|r| r.control)
                    .or_else(|| trial.plan().and_then(|p| p.control));
                Some((
                    id,
                    work.void.is_some(),
                    [
                        vec![C::link(
                            format!("{}#{}", self.url, anchor("work", id)),
                            vec![work.name.to_string().into()],
                        )],
                        text(shown(self.f, id).into()),
                        text(stamp(
                            report.map_or(work.created.time, |r| r.at.time),
                            false,
                        )),
                        trial
                            .regime
                            .map(|r| text(r.to_string()))
                            .unwrap_or_default(),
                        control
                            .map(|c| text(self.name(c.baseline)))
                            .unwrap_or_default(),
                        report
                            .map(|r| self.readings(trial, &r.readings, true))
                            .unwrap_or_default(),
                        self.lens.headline(id).map(text).unwrap_or_default(),
                        report
                            .into_iter()
                            .flat_map(|r| &r.artifacts)
                            .map(|artifact| {
                                C::classed(T::Span, "artifact", vec![artifact.clone().into()])
                            })
                            .collect(),
                    ],
                ))
            })
            .collect();
        let columns: Vec<_> = (0..headings.len())
            .filter(|&column| {
                matches!(column, 0..=2) || rows.iter().any(|(_, _, row)| !row[column].is_empty())
            })
            .collect();
        let body = rows
            .into_iter()
            .map(|(id, void, row)| {
                let mut classes = Vec::new();
                if selected == Some(id) {
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
        C::classed(
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
                            .map(|&column| {
                                C::element(T::Th, Vec::new(), vec![headings[column].into()])
                            })
                            .collect(),
                    )],
                ),
                C::element(T::Tbody, Vec::new(), body),
            ],
        )
    }

    fn baseline(&self, id: Id, b: &Baseline) -> Card {
        let f = self.f;
        let elections: Vec<_> = f.elections().iter().filter(|e| e.baseline == id).collect();
        let role = if b.void.is_some() {
            "void"
        } else if f.incumbent().is_some_and(|(_, incumbent)| incumbent == id) {
            "incumbent"
        } else if elections.is_empty() {
            "candidate"
        } else {
            "elected"
        };
        let mut state = vec![role.to_owned()];
        if let Some(regime) = b.regime {
            state.push(format!("regime {regime}"));
        }
        if let Some(e) = elections.last() {
            state.push(format!("since {}", stamp(e.at.time, false)));
        }
        let mut fields = vec![Field {
            label: "readings".into(),
            body: readings(f, b.regime, &b.readings, &Json::Null),
        }];
        if let Some(note) = &b.note {
            fields.push(Field::text("note", note.clone()));
        }
        if !b.annotations.is_empty() {
            fields.push(Field::text("annotations", annotation_lines(&b.annotations)));
        }
        if let Some(void) = &b.void {
            fields.push(Field::text("void because", void.reason.clone()));
        }
        for e in &elections {
            let adopted: Vec<Id> = e.adopts.keys().copied().collect();
            let mut body = vec![C::Text(e.rationale.clone())];
            if !adopted.is_empty() {
                body.push(self.links(&adopted));
            }
            fields.push(Field {
                label: format!("elected {}", stamp(e.at.time, false)),
                body,
            });
        }
        if !b.artifacts.is_empty() {
            fields.push(Field::text("artifacts", b.artifacts.join("\n")));
        }
        Card {
            class: "baseline".into(),
            slug: b.slug.to_string(),
            title: format!("Baseline {}", b.slug),
            state: state.join(" · "),
            fields,
        }
    }
}

/// Whether `to` is reachable from `from` through `successors`.
fn reaches(successors: &[BTreeSet<usize>], from: usize, to: usize) -> bool {
    let mut stack = vec![from];
    let mut seen = BTreeSet::new();
    while let Some(next) = stack.pop() {
        if next == to {
            return true;
        }
        if seen.insert(next) {
            stack.extend(successors[next].iter().copied());
        }
    }
    false
}

/// Readings in gauge order, each with its unit and its change against the
/// control it was compared with.
fn readings(
    f: &Frontier,
    regime: Option<RegimeNumber>,
    values: &RecordedReadings,
    comparisons: &Json,
) -> Vec<C> {
    let gauge = regime.and_then(|r| f.regime(r)).map(|r| &r.gauge);
    values
        .iter()
        .map(|(key, value)| {
            let (text, class) = match value {
                Reading::Listed(value) => {
                    let unit = gauge
                        .and_then(|g| g.metrics.get(key))
                        .map_or(String::new(), |m| format!(" {}", m.unit));
                    let comparison = &comparisons[key.as_str()];
                    let change = comparison["ratio"]
                        .as_f64()
                        .map(|ratio| format!(" {}", change(ratio)))
                        .unwrap_or_default();
                    let class = match comparison["movement"].as_str() {
                        Some("better") => "reading better",
                        Some("worse") => "reading worse",
                        _ => "reading",
                    };
                    (
                        format!("{key} {}{unit}{change}", charts::significant(*value)),
                        class,
                    )
                }
                Reading::Unlisted(value) => (
                    format!("{key} {} (unlisted)", unlisted(value)),
                    "reading unlisted",
                ),
            };
            C::classed(T::Span, class, vec![text.into()])
        })
        .collect()
}
fn unlisted(value: &fidget_spinner_core::ReadingValue) -> String {
    match value {
        fidget_spinner_core::ReadingValue::Number(n) => charts::significant(*n),
        fidget_spinner_core::ReadingValue::Text(t) => t.clone(),
    }
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
