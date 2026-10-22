//! KPI charts: elected baselines as steps across epochs, reported trials as
//! dots, reference lines dashed.

use std::{collections::BTreeMap, fmt::Write as _};

use fidget_spinner_core::{
    Direction, Frontier, MetricKey, Reading, RecordedReadings, RegimeNumber, Unit,
};
use time::OffsetDateTime;

use super::{A, C, T, anchor};
use crate::{inspect::stamp, mcp::Fault};

const WIDTH: f64 = 340.0;
const HEIGHT: f64 = 96.0;
/// Plot margins: value labels on the left, epoch labels above, dates below.
const LEFT: f64 = 44.0;
const RIGHT: f64 = 10.0;
const TOP: f64 = 16.0;
const BOTTOM: f64 = 16.0;
/// Approximate advance of one 9px label character.
const LABEL_ADVANCE: f64 = 5.6;

/// Three significant figures, switching to scientific notation outside 0.01..1000.
pub(super) fn significant(value: f64) -> String {
    let scientific = format!("{value:.2e}");
    let Some((_, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    let exponent = exponent
        .parse::<i32>()
        .unwrap_or_else(|error| unreachable!("formatted finite exponent: {error}"));
    if !(-2..=2).contains(&exponent) {
        return scientific;
    }
    let decimals = usize::try_from(2 - exponent)
        .unwrap_or_else(|error| unreachable!("exponent restricted to -2..=2: {error}"));
    format!("{value:.decimals$}")
}

struct Point {
    at: OffsetDateTime,
    value: f64,
    record: String,
    title: String,
    class: &'static str,
}

/// A recorded value in the unit of the current metric, or `None` when it is
/// unlisted, absent from its gauge, or of another quantity.
fn value(
    f: &Frontier,
    regime: Option<RegimeNumber>,
    key: &MetricKey,
    readings: &RecordedReadings,
    unit: &Unit,
) -> Result<Option<f64>, Fault> {
    let Some(Reading::Listed(value)) = readings.get(key) else {
        return Ok(None);
    };
    let Some(metric) = regime
        .and_then(|r| f.regime(r))
        .and_then(|r| r.gauge.metrics.get(key))
    else {
        return Ok(None);
    };
    // A changed quantity is a different series even when a gauge reuses its key.
    if metric.unit.kind() != unit.kind() {
        return Ok(None);
    }
    Ok(Some(
        metric
            .unit
            .convert(*value, unit)
            .map_err(|e| Fault::Arguments(e.to_string()))?,
    ))
}

fn element(tag: T, attributes: Vec<(A, String)>, children: Vec<C>) -> C {
    C::element(tag, attributes, children)
}
fn text_at(x: f64, y: f64, anchor: &str, class: &str, text: String) -> C {
    element(
        T::Text,
        vec![
            (A::Class, class.into()),
            (A::X, format!("{x:.1}")),
            (A::Y, format!("{y:.1}")),
            (A::TextAnchor, anchor.into()),
        ],
        vec![text.into()],
    )
}
fn rule(class: &str, (x1, y1): (f64, f64), (x2, y2): (f64, f64)) -> C {
    element(
        T::Line,
        vec![
            (A::Class, class.into()),
            (A::X1, format!("{x1:.1}")),
            (A::Y1, format!("{y1:.1}")),
            (A::X2, format!("{x2:.1}")),
            (A::Y2, format!("{y2:.1}")),
        ],
        Vec::new(),
    )
}
/// A short date, such as `10-14`.
fn day(at: OffsetDateTime) -> String {
    let at = at.to_offset(time::UtcOffset::UTC);
    format!("{:02}-{:02}", u8::from(at.month()), at.day())
}
fn baseline_name(f: &Frontier, id: fidget_spinner_core::Id) -> String {
    f.name_of(id).unwrap_or_else(|| "removed baseline".into())
}

/// One tile per KPI of the current regime.
pub(super) fn charts(f: &Frontier, url: &str) -> Result<Vec<C>, Fault> {
    let Some(regime) = f.current_regime() else {
        return Ok(Vec::new());
    };
    let mut tiles = Vec::new();
    for (key, metric) in regime
        .gauge
        .metrics
        .iter()
        .filter(|(_, m)| m.kpi && !m.retired)
    {
        let unit = &metric.unit;
        let mut baselines = Vec::new();
        for e in f.elections() {
            if let Some(b) = f.baselines().get(&e.baseline).filter(|b| b.void.is_none())
                && let Some(value) = value(f, b.regime, key, &b.readings, unit)?
            {
                baselines.push(Point {
                    at: e.at.time,
                    value,
                    record: anchor("baseline", e.baseline).to_string(),
                    title: format!(
                        "Epoch of {} · since {} · {} {unit}",
                        b.slug,
                        stamp(e.at.time, false),
                        significant(value),
                    ),
                    class: "baseline",
                });
            }
        }
        let mut trials = Vec::new();
        for (id, work) in f.work().iter().filter(|(_, w)| w.void.is_none()) {
            let Some(trial) = work.trial() else {
                continue;
            };
            let Some(report) = &trial.report else {
                continue;
            };
            if let Some(value) = value(f, trial.regime, key, &report.readings, unit)? {
                trials.push(Point {
                    at: report.at.time,
                    value,
                    record: anchor("work", *id).to_string(),
                    title: format!("{} · {} {unit}", work.name, significant(value)),
                    class: "reported",
                });
            }
        }
        let incumbent = f
            .incumbent()
            .and_then(|(_, id)| f.baselines().get(&id))
            .and_then(|b| value(f, b.regime, key, &b.readings, unit).transpose())
            .transpose()?;
        let (arrow, better) = match metric.better {
            Some(Direction::Lower) => ("↓", "lower is better"),
            Some(Direction::Higher) => ("↑", "higher is better"),
            None => ("", ""),
        };
        let head = vec![
            element(
                T::B,
                vec![(A::Title, metric.about.clone().unwrap_or_default())],
                vec![key.to_string().into()],
            ),
            element(
                T::Span,
                vec![(A::Class, "better".into()), (A::Title, better.into())],
                vec![arrow.into()],
            ),
            match incumbent {
                Some(value) => element(
                    T::Span,
                    vec![
                        (A::Class, "value".into()),
                        (A::Title, "incumbent baseline".into()),
                    ],
                    vec![
                        format!("{} ", significant(value)).into(),
                        C::classed(T::Span, "unit", vec![unit.to_string().into()]),
                    ],
                ),
                None => C::classed(T::Span, "unit", vec![unit.to_string().into()]),
            },
        ];
        let mut tile = vec![C::classed(T::Div, "head", head)];
        // One reading has no history to draw.
        if baselines.len() + trials.len() > 1 {
            tile.push(plot(f, url, &baselines, &trials, &metric.refs, unit));
        } else {
            let mut notes = metric
                .refs
                .iter()
                .map(|(label, value)| format!("{label} {}", significant(*value)))
                .collect::<Vec<_>>();
            if baselines.len() + trials.len() == 0 {
                notes.insert(0, "no readings yet".into());
            }
            tile.push(C::classed(T::P, "none", vec![notes.join(" · ").into()]));
        }
        tiles.push(C::classed(T::Section, "kpi", tile));
    }
    Ok(tiles)
}

fn plot(
    f: &Frontier,
    url: &str,
    baselines: &[Point],
    trials: &[Point],
    refs: &BTreeMap<String, f64>,
    unit: &Unit,
) -> C {
    let points = || baselines.iter().chain(trials);
    let first = points()
        .map(|p| p.at)
        .min()
        .unwrap_or_else(OffsetDateTime::now_utc);
    let last = points().map(|p| p.at).max().unwrap_or(first);
    let span = (last - first).as_seconds_f64().max(1.0);
    let lo = points()
        .map(|p| p.value)
        .chain(refs.values().copied())
        .fold(f64::INFINITY, f64::min);
    let hi = points()
        .map(|p| p.value)
        .chain(refs.values().copied())
        .fold(f64::NEG_INFINITY, f64::max);
    let pad = if hi > lo {
        (hi - lo) * 0.08
    } else {
        lo.abs().max(1.0) * 0.05
    };
    let (low, high) = (lo - pad, hi + pad);
    // A lone instant sits mid-plot.
    let x = |at: OffsetDateTime| {
        if last == first {
            f64::midpoint(LEFT, WIDTH - RIGHT)
        } else {
            LEFT + (at - first).as_seconds_f64() / span * (WIDTH - LEFT - RIGHT)
        }
    };
    let y = |value: f64| HEIGHT - BOTTOM - (value - low) / (high - low) * (HEIGHT - TOP - BOTTOM);
    let mut drawing = vec![
        element(
            T::Rect,
            vec![
                (A::Class, "plot".into()),
                (A::X, format!("{LEFT}")),
                (A::Y, format!("{}", TOP - 2.0)),
                (A::Width, format!("{}", WIDTH - LEFT - RIGHT)),
                (A::Height, format!("{}", HEIGHT - TOP - BOTTOM + 2.0)),
            ],
            Vec::new(),
        ),
        rule("frame", (LEFT, y(hi)), (WIDTH - RIGHT, y(hi))),
        rule("frame", (LEFT, y(lo)), (WIDTH - RIGHT, y(lo))),
        text_at(LEFT - 4.0, y(hi) + 3.5, "end", "tick", significant(hi)),
        text_at(LEFT - 4.0, y(lo) + 3.5, "end", "tick", significant(lo)),
        text_at(LEFT, HEIGHT - 3.0, "start", "tick", day(first)),
    ];
    if last != first && day(last) != day(first) {
        drawing.push(text_at(
            WIDTH - RIGHT,
            HEIGHT - 3.0,
            "end",
            "tick",
            day(last),
        ));
    }
    let mut label_end = f64::NEG_INFINITY;
    for e in f
        .elections()
        .iter()
        .filter(|e| e.at.time >= first && e.at.time <= last)
    {
        let at = x(e.at.time);
        let label = baseline_name(f, e.baseline);
        drawing.push(element(
            T::G,
            vec![(A::Class, "epoch".into())],
            vec![
                element(
                    T::Title,
                    Vec::new(),
                    vec![format!("Epoch of {label} · since {}", stamp(e.at.time, false)).into()],
                ),
                rule("epoch", (at, TOP - 2.0), (at, HEIGHT - BOTTOM)),
            ],
        ));
        #[expect(
            clippy::cast_precision_loss,
            reason = "slug lengths are far below 2^52"
        )]
        let width = label.chars().count() as f64 * LABEL_ADVANCE;
        if at + 2.0 > label_end && at + 2.0 + width <= WIDTH {
            drawing.push(text_at(at + 2.0, TOP - 5.0, "start", "tick", label));
            label_end = at + 2.0 + width + 4.0;
        }
    }
    for (label, value) in refs {
        drawing.push(rule(
            "reference",
            (LEFT, y(*value)),
            (WIDTH - RIGHT, y(*value)),
        ));
        drawing.push(text_at(
            WIDTH - RIGHT,
            y(*value) - 3.0,
            "end",
            "reference",
            format!("{label} {}", significant(*value)),
        ));
    }
    let mut steps = String::new();
    for (index, b) in baselines.iter().enumerate() {
        let next = f
            .elections()
            .iter()
            .find(|e| e.at.time > b.at)
            .map_or(last, |e| e.at.time.min(last));
        let break_at = f
            .regimes()
            .iter()
            .filter(|r| r.bridge.is_none() && r.at.time > b.at && r.at.time <= next)
            .map(|r| r.at.time)
            .min();
        let end = break_at.unwrap_or(next).max(b.at);
        let _ = write!(steps, "M{:.1} {:.1}H{:.1}", x(b.at), y(b.value), x(end));
        if break_at.is_none()
            && let Some(following) = baselines.get(index + 1).filter(|p| p.at == next)
        {
            let _ = write!(steps, "V{:.1}", y(following.value));
        }
    }
    if !steps.is_empty() {
        drawing.push(element(
            T::Path,
            vec![(A::Class, "steps".into()), (A::D, steps)],
            Vec::new(),
        ));
    }
    for p in trials.iter().chain(baselines) {
        drawing.push(element(
            T::A,
            vec![(A::Href, format!("{url}#{}", p.record))],
            vec![
                element(T::Title, Vec::new(), vec![p.title.clone().into()]),
                element(
                    T::Circle,
                    vec![
                        (A::Class, format!("point {}", p.class)),
                        (A::Cx, format!("{:.1}", x(p.at))),
                        (A::Cy, format!("{:.1}", y(p.value))),
                        (A::R, if p.class == "baseline" { "3.5" } else { "3" }.into()),
                    ],
                    Vec::new(),
                ),
            ],
        ));
    }
    element(
        T::Svg,
        vec![
            (A::Width, format!("{WIDTH}")),
            (A::Height, format!("{HEIGHT}")),
            (A::ViewBox, format!("0 0 {WIDTH} {HEIGHT}")),
            (A::AriaLabel, format!("readings in {unit}")),
        ],
        drawing,
    )
}
