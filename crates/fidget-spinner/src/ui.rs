use crate::{
    mcp::Fault,
    store::{self, Ledger, Store},
};
use fidget_spinner_core::{
    Assay, Frontier, Hypothesis, HypothesisDecision, Reading, Slug, State as HypothesisState,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
};
use trebuchet::{
    board::{
        self, Attribute as A, BoardRoutes, BoardScene, BoardSource, Card, Content, IndexScene,
        Marker, RouteTarget, SetStyle, Tag as T, Theme,
    },
    layout::{Cell, CellSet, LayoutInput, Occupant, Separator, focused, foreground},
};
mod charts;

struct Navigator {
    path: PathBuf,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Selection {
    id: i64,
    horizon: usize,
    settled: bool,
}

pub fn serve(path: PathBuf, bind: SocketAddr) -> Result<(), Fault> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(board::serve(
        bind,
        Navigator { path },
        BoardRoutes {
            prefix: "frontier".into(),
            parse: selection,
            target,
        },
        Theme {
            css: include_str!("ui/style.css").into(),
            ..Theme::default()
        },
    ))?;
    Ok(())
}

fn selection(segment: &str, query: Option<&str>) -> Result<Selection, String> {
    let id = segment
        .parse()
        .map_err(|_| "frontier identity must be an integer")?;
    let mut selection = Selection {
        id,
        horizon: 2,
        settled: false,
    };
    for pair in query.unwrap_or("").split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').ok_or("expected query name=value")?;
        match key {
            "horizon" => {
                selection.horizon = value
                    .parse()
                    .map_err(|_| "horizon must be an epoch count")?;
            }
            "settled" if value == "show" => selection.settled = true,
            _ => return Err(format!("unknown navigator selector {key}")),
        }
    }
    Ok(selection)
}
fn target(s: &Selection) -> RouteTarget {
    RouteTarget {
        segment: s.id.to_string(),
        query: format!(
            "?horizon={}{}",
            s.horizon,
            if s.settled { "&settled=show" } else { "" }
        ),
    }
}
fn url(s: &Selection) -> String {
    let t = target(s);
    format!("/frontier/{}{}", t.segment, t.query)
}
fn element(tag: T, attributes: Vec<(A, String)>, children: Vec<Content>) -> Content {
    Content::Element {
        tag,
        attributes,
        children,
    }
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "content projection accepts both owned and borrowed display values"
)]
fn text(value: impl ToString) -> Content {
    Content::Text(value.to_string())
}
fn node(tag: T, children: Vec<Content>) -> Content {
    element(tag, vec![], children)
}
fn link(href: impl Into<String>, label: impl ToString) -> Content {
    element(T::A, vec![(A::Href, href.into())], vec![text(label)])
}
fn field(label: &str, value: impl ToString) -> Content {
    node(
        T::P,
        vec![node(T::B, vec![text(format!("{label}: "))]), text(value)],
    )
}
fn state(h: &Hypothesis, assay: HypothesisState) -> &'static str {
    match h.disposition.as_ref().map(|d| &d.decision) {
        Some(HypothesisDecision::Accepted { .. }) => "accepted",
        Some(HypothesisDecision::Rejected) => "rejected",
        Some(HypothesisDecision::Shelved) => "shelved",
        None => match assay {
            HypothesisState::Waiting => "waiting",
            HypothesisState::Ready => "ready",
            HypothesisState::Running => "running",
            HypothesisState::Tested => "tested",
            HypothesisState::Settled => "settled",
        },
    }
}
fn record(kind: &str, slug: &Slug) -> trebuchet::Slug {
    format!("{kind}.{slug}")
        .parse()
        .unwrap_or_else(|error| unreachable!("record prefix and validated slug: {error}"))
}
fn reading(
    frontier: &Frontier,
    regime: fidget_spinner_core::RegimeNumber,
    values: &fidget_spinner_core::RecordedReadings,
) -> String {
    let gauge = frontier
        .regimes()
        .iter()
        .find(|r| r.number == regime)
        .map(|r| &r.gauge);
    values
        .iter()
        .map(|(key, value)| match value {
            Reading::Listed { value } => format!(
                "{key} {} {}",
                charts::significant(*value),
                gauge
                    .and_then(|g| g.metrics.get(key))
                    .map_or("", |m| m.unit.as_str())
            ),
            Reading::Unlisted { value } => format!(
                "{key} {} (unlisted)",
                match value {
                    fidget_spinner_core::Value::Number(n) => charts::significant(*n),
                    fidget_spinner_core::Value::Text(t) => t.clone(),
                }
            ),
        })
        .collect::<Vec<_>>()
        .join(" · ")
}
impl Navigator {
    fn store(&self) -> Result<Option<Store>, Fault> {
        Ok(Store::open_read_only(&self.path, store::format()?)?)
    }
    fn ledger(&self, id: i64) -> Result<Option<(String, Ledger<Frontier>)>, Fault> {
        let Some(store) = self.store()? else {
            return Ok(None);
        };
        let Some(listing) = store.listing(id)? else {
            return Ok(None);
        };
        let mut ledger = Ledger::default();
        store.refresh(&listing.address, &mut ledger)?;
        Ok(Some((
            format!(
                "{} / {}",
                listing.address.project.as_path().display(),
                listing.address.slug
            ),
            ledger,
        )))
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
    fn index(&self) -> Result<IndexScene, Fault> {
        let mut projects = BTreeMap::<PathBuf, Vec<Content>>::new();
        if let Some(store) = self.store()? {
            for listing in store.listings()? {
                let mut ledger = Ledger::default();
                store.refresh(&listing.address, &mut ledger)?;
                let s = Selection {
                    id: listing.id,
                    horizon: 2,
                    settled: false,
                };
                let mut row = vec![link(url(&s), &listing.address.slug)];
                if let Some(objective) = ledger.state().objective() {
                    row.push(text(format!(" — {objective}")));
                }
                if ledger.state().conclusion().is_some() {
                    row.push(text(" · concluded"));
                }
                projects
                    .entry(listing.address.project.as_path().to_path_buf())
                    .or_default()
                    .push(node(T::Li, row));
            }
        }
        let entries = if projects.is_empty() {
            vec![node(T::P, vec![text("No frontiers yet.")])]
        } else {
            projects
                .into_iter()
                .map(|(project, entries)| {
                    element(
                        T::Details,
                        vec![(A::Open, String::new())],
                        vec![
                            node(T::Summary, vec![text(project.display())]),
                            node(T::Ul, entries),
                        ],
                    )
                })
                .collect()
        };
        Ok(IndexScene {
            title: "Fidget Spinner".into(),
            header: vec![text("Fidget Spinner")],
            entries,
        })
    }
    fn scene(&self, s: &Selection) -> Result<Option<BoardScene>, Fault> {
        let Some((address, ledger)) = self.ledger(s.id)? else {
            return Ok(None);
        };
        let f = ledger.state();
        let assay = Assay::new(f);
        let mut scene = geometry(f, &assay);
        let visible = visible(f, &assay, s);
        scene.input = focused(&scene.input, &visible);
        scene.title.clone_from(&address);
        scene.header = header(s, &address);
        scene.above = vec![element(
            T::Section,
            vec![(A::Class, "overview".into())],
            vec![
                node(T::H2, vec![text(f.objective().unwrap_or(""))]),
                charts::charts(f, s)?,
            ],
        )];
        if let Some(conclusion) = f.conclusion() {
            scene.above.push(field("Conclusion", &conclusion.text));
        }
        scene.empty = "No hypotheses within this horizon.".into();
        Ok(Some(scene))
    }
    fn foreground(
        &self,
        s: &Selection,
        selected: &trebuchet::Slug,
    ) -> Result<Option<BoardScene>, Fault> {
        let Some((_, ledger)) = self.ledger(s.id)? else {
            return Ok(None);
        };
        let f = ledger.state();
        let assay = Assay::new(f);
        let mut scene = geometry(f, &assay);
        let key = selected.as_str();
        if let Some(key) = key.strip_prefix("hypothesis.") {
            let slug: Slug = key.parse().map_err(|e| Fault::Arguments(format!("{e}")))?;
            let Some(h) = f.hypotheses().get(&slug) else {
                return Ok(None);
            };
            let id = f
                .hypotheses()
                .keys()
                .position(|s| s == &slug)
                .unwrap_or_else(|| unreachable!("existing hypothesis"));
            let shown = visible(f, &assay, s);
            scene.input = foreground(
                &scene.input,
                id,
                |v| v == id || shown.contains(&v),
                |v| Some(f.hypotheses().keys().nth(v).map_or("", Slug::lane)),
                |n| format!("{n} settled hypotheses"),
            )
            .unwrap_or_else(|| unreachable!("selected hypothesis is visible"));
            scene.above = hypothesis(f, &slug, h, None);
        } else if let Some(key) = key.strip_prefix("trial.") {
            let Some((key, n)) = key.rsplit_once('.') else {
                return Ok(None);
            };
            let slug: Slug = key.parse().map_err(|e| Fault::Arguments(format!("{e}")))?;
            let n = key_number(n)?;
            let Some(h) = f
                .hypotheses()
                .get(&slug)
                .filter(|h| h.trials.contains_key(&n))
            else {
                return Ok(None);
            };
            scene.input = LayoutInput::default();
            scene.above = hypothesis(f, &slug, h, Some(n));
        } else if let Some(key) = key.strip_prefix("baseline.") {
            let slug: Slug = key.parse().map_err(|e| Fault::Arguments(format!("{e}")))?;
            let Some(b) = f.baselines().get(&slug) else {
                return Ok(None);
            };
            scene.input = LayoutInput::default();
            scene.above = vec![
                node(T::H2, vec![text(format!("Baseline {slug}"))]),
                field("Readings", reading(f, b.regime, &b.readings)),
            ];
            if let Some(epoch) = f.epochs().iter().rev().find(|epoch| epoch.baseline == slug) {
                scene
                    .above
                    .push(field("Since", crate::inspect::stamp(epoch.at, false)));
            }
            if let Some(note) = &b.note {
                scene.above.push(node(T::P, vec![text(note)]));
            }
            if let Some(reason) = &b.void {
                scene.above.push(field("Void", reason));
            }
            for artifact in &b.artifacts {
                scene.above.push(field("Artifact", artifact));
            }
            if let Some(docket) = assay.dockets.get(&slug) {
                scene.below = vec![node(
                    T::Ul,
                    docket
                        .members
                        .iter()
                        .map(|h| {
                            node(
                                T::Li,
                                vec![link(format!("{}#{}", url(s), record("hypothesis", h)), h)],
                            )
                        })
                        .collect(),
                )];
            }
        } else if let Some(key) = key.strip_prefix("review.") {
            let slug: Slug = key.parse().map_err(|e| Fault::Arguments(format!("{e}")))?;
            let Some(r) = f.reviews().get(&slug) else {
                return Ok(None);
            };
            scene.input = LayoutInput::default();
            scene.above = vec![
                node(T::H2, vec![text(&r.title)]),
                node(T::P, vec![text(&r.charter)]),
            ];
            if let Some(lesson) = &r.lesson {
                scene.above.push(field("Lesson", &lesson.text));
            }
            scene.below = vec![node(
                T::Ul,
                r.members
                    .iter()
                    .map(|h| {
                        node(
                            T::Li,
                            vec![link(format!("{}#{}", url(s), record("hypothesis", h)), h)],
                        )
                    })
                    .collect(),
            )];
        } else {
            return Ok(None);
        }
        Ok(Some(scene))
    }
}
fn key_number(raw: &str) -> Result<u64, Fault> {
    raw.parse()
        .map_err(|_| Fault::Arguments("invalid trial number".into()))
}
fn visible(f: &Frontier, a: &Assay, s: &Selection) -> BTreeSet<usize> {
    f.hypotheses()
        .iter()
        .enumerate()
        .filter(|(_, (slug, h))| {
            if s.settled || a.hypotheses.get(*slug) != Some(&HypothesisState::Settled) {
                return true;
            }
            let at = h.disposition.as_ref().map_or(h.at, |d| d.at);
            s.horizon > 0
                && a.epochs
                    .iter()
                    .rev()
                    .take(s.horizon)
                    .next_back()
                    .is_none_or(|epoch| at >= epoch.at)
        })
        .map(|(id, _)| id)
        .collect()
}
fn header(s: &Selection, address: &str) -> Vec<Content> {
    let mut checkbox = vec![
        (A::Type, "checkbox".into()),
        (A::Name, "settled".into()),
        (A::Value, "show".into()),
    ];
    if s.settled {
        checkbox.push((A::Checked, String::new()));
    }
    vec![
        link("/", "Fidget Spinner"),
        text(address),
        element(
            T::Form,
            vec![
                (A::Method, "get".into()),
                (A::Action, format!("/frontier/{}", s.id)),
            ],
            vec![
                element(
                    T::Span,
                    vec![(A::Class, "fog".into())],
                    vec![
                        node(
                            T::Label,
                            vec![
                                text("Recent epochs"),
                                element(
                                    T::Input,
                                    vec![
                                        (A::Type, "number".into()),
                                        (A::Name, "horizon".into()),
                                        (A::Min, "0".into()),
                                        (A::Value, s.horizon.to_string()),
                                    ],
                                    vec![],
                                ),
                            ],
                        ),
                        node(
                            T::Label,
                            vec![
                                element(T::Input, checkbox, vec![]),
                                text("Show all settled"),
                            ],
                        ),
                    ],
                ),
                node(T::Button, vec![text("Apply")]),
            ],
        ),
    ]
}
fn placed_at(h: &Hypothesis) -> time::OffsetDateTime {
    h.disposition.as_ref().map_or(h.at, |d| d.at)
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
            title: format!("{slug}\n{}", h.title),
            size: 1,
        });
        scene.input.rank.push(ranks[slug]);
        let class = state(h, a.hypotheses[slug]).to_string();
        let _ = scene.cell_classes.insert(id, class.clone());
        let _ = scene
            .cell_tooltips
            .insert(id, format!("{slug} · {}", h.title));
        let _ = scene.cards.insert(
            id,
            Card {
                class,
                content: vec![
                    node(T::B, vec![text(&h.title)]),
                    text(state(h, a.hypotheses[slug])),
                    text(h.claim.as_deref().unwrap_or("")),
                ],
            },
        );
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
        let _ = scene.set_styles.insert(
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
                |epoch| {
                    format!(
                        "Epoch of {slug} · since {}",
                        crate::inspect::stamp(epoch.at, false)
                    )
                },
            );
        scene.input.separators.push(Separator {
            index: scene.input.separators.len(),
            holds: holds.into_iter().collect(),
            title,
            class: "epoch".into(),
        });
    }
    for (index, r) in f.regimes().iter().enumerate() {
        let until = f.regimes().get(index + 1).map(|r| r.at);
        let holds = f
            .hypotheses()
            .iter()
            .filter(|(_, h)| placed_at(h) >= r.at && until.is_none_or(|at| placed_at(h) < at))
            .map(|(h, _)| ids[h])
            .collect();
        scene.input.separators.push(Separator {
            index: scene.input.separators.len(),
            holds,
            title: format!(
                "Regime {} · {}",
                r.number,
                if r.bridge.is_some() {
                    "bridged"
                } else {
                    "unbridged"
                }
            ),
            class: "regime".into(),
        });
    }
    scene
}
fn hypothesis(f: &Frontier, slug: &Slug, h: &Hypothesis, selected: Option<u64>) -> Vec<Content> {
    let mut content = vec![node(T::H2, vec![text(format!("{slug} · {}", h.title))])];
    for value in [&h.mechanism, &h.claim].into_iter().flatten() {
        content.push(node(T::P, vec![text(value)]));
    }
    if let Some(d) = &h.disposition {
        content.push(field(state(h, HypothesisState::Settled), &d.rationale));
    }
    if h.trials.is_empty() {
        return content;
    }
    let headings = [
        "Trial",
        "Time",
        "Regime",
        "Baseline",
        "Readings",
        "Note",
        "Artifacts",
        "Void",
    ];
    let rows: Vec<_> = h
        .trials
        .iter()
        .map(|(number, trial)| {
            (
                *number,
                [
                    format!("{slug}#{number}"),
                    trial.at.to_string(),
                    trial.regime.to_string(),
                    trial
                        .against
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    reading(f, trial.regime, &trial.readings),
                    trial.note.clone(),
                    trial.artifacts.join(" · "),
                    trial.void.clone().unwrap_or_default(),
                ],
            )
        })
        .collect();
    let columns: Vec<_> = (0..headings.len())
        .filter(|&column| {
            matches!(column, 0 | 1 | 2 | 5) || rows.iter().any(|(_, row)| !row[column].is_empty())
        })
        .collect();
    let body = rows
        .into_iter()
        .map(|(number, row)| {
            element(
                T::Tr,
                if selected == Some(number) {
                    vec![(A::Class, "selected".into())]
                } else {
                    vec![]
                },
                columns
                    .iter()
                    .map(|&column| {
                        node(
                            T::Td,
                            vec![text(if row[column].is_empty() {
                                "—"
                            } else {
                                &row[column]
                            })],
                        )
                    })
                    .collect(),
            )
        })
        .collect();
    content.push(node(
        T::Table,
        vec![
            node(
                T::Thead,
                vec![node(
                    T::Tr,
                    columns
                        .iter()
                        .map(|&column| node(T::Th, vec![text(headings[column])]))
                        .collect(),
                )],
            ),
            node(T::Tbody, body),
        ],
    ));
    content
}
