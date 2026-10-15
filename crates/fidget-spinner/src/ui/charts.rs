use super::{A, Content, Frontier, Reading, Selection, T, element, link, node, record, text, url};
use fidget_spinner_core::{HypothesisDecision, MetricKey, Unit};
use std::fmt::Write as _;
use time::OffsetDateTime;

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
fn value(
    f: &Frontier,
    regime: fidget_spinner_core::RegimeNumber,
    key: &MetricKey,
    readings: &fidget_spinner_core::RecordedReadings,
    unit: &Unit,
) -> Result<Option<f64>, crate::mcp::Fault> {
    let Some(Reading::Listed { value }) = readings.get(key) else {
        return Ok(None);
    };
    let Some(metric) = f
        .regimes()
        .iter()
        .find(|r| r.number == regime)
        .and_then(|r| r.gauge.metrics.get(key))
    else {
        return Ok(None);
    };
    // A changed quantity is a different series even when a gauge reuses its key.
    if metric.unit.kind() != unit.kind() {
        return Ok(None);
    }
    Ok(Some(metric.unit.convert(*value, unit).map_err(|e| {
        crate::mcp::Fault::Arguments(e.to_string())
    })?))
}
pub(super) fn charts(f: &Frontier, s: &Selection) -> Result<Content, crate::mcp::Fault> {
    let mut charts = Vec::new();
    let Some(regime) = f.current_regime() else {
        return Ok(node(T::Div, charts));
    };
    for (key, metric) in regime
        .gauge
        .metrics
        .iter()
        .filter(|(_, m)| m.kpi && !m.retired)
    {
        let mut baselines = Vec::new();
        for epoch in f.epochs() {
            if let Some(b) = f
                .baselines()
                .get(&epoch.baseline)
                .filter(|b| b.void.is_none())
                && let Some(value) = value(f, b.regime, key, &b.readings, &metric.unit)?
            {
                baselines.push(Point {
                    at: epoch.at,
                    value,
                    record: record("baseline", &epoch.baseline).to_string(),
                    title: format!(
                        "Baseline {} · since {} · {} {}",
                        epoch.baseline,
                        crate::inspect::stamp(epoch.at, false),
                        significant(value),
                        metric.unit
                    ),
                    class: "baseline",
                });
            }
        }
        let mut trials = Vec::new();
        for (slug, h) in f.hypotheses() {
            let class = match h.disposition.as_ref().map(|d| &d.decision) {
                Some(HypothesisDecision::Accepted { .. }) => "accepted",
                Some(HypothesisDecision::Rejected) => "rejected",
                Some(HypothesisDecision::Shelved) => "shelved",
                None => "tested",
            };
            for (n, t) in h.trials.iter().filter(|(_, t)| t.void.is_none()) {
                if let Some(value) = value(f, t.regime, key, &t.readings, &metric.unit)? {
                    trials.push(Point {
                        at: t.at,
                        value,
                        record: format!("trial.{slug}.{n}"),
                        title: format!("{slug}#{n} · {} {}", significant(value), metric.unit),
                        class,
                    });
                }
            }
        }
        let first = baselines
            .iter()
            .chain(&trials)
            .map(|p| p.at)
            .chain(f.regimes().iter().map(|r| r.at))
            .min()
            .unwrap_or(regime.at);
        let last = baselines
            .iter()
            .chain(&trials)
            .map(|p| p.at)
            .chain(f.regimes().iter().map(|r| r.at))
            .max()
            .unwrap_or(first);
        let duration = (last - first).as_seconds_f64().max(1.0);
        let lo = baselines
            .iter()
            .chain(&trials)
            .map(|p| p.value)
            .chain(metric.refs.values().copied())
            .reduce(f64::min)
            .unwrap_or(0.);
        let hi = baselines
            .iter()
            .chain(&trials)
            .map(|p| p.value)
            .chain(metric.refs.values().copied())
            .reduce(f64::max)
            .unwrap_or(1.);
        let scale = lo.abs().max(hi.abs()).max(f64::MIN_POSITIVE);
        let mut low = lo / scale;
        let mut high = hi / scale;
        if high - low <= 0. {
            low -= 0.1;
            high += 0.1;
        }
        let x = |at: OffsetDateTime| 60. + (at - first).as_seconds_f64() / duration * 500.;
        let y = |value: f64| 155. - (value / scale - low) / (high - low) * 120.;
        let mut drawing = vec![
            node(T::Title, vec![text(format!("{key} ({})", metric.unit))]),
            element(
                T::Line,
                vec![
                    (A::X1, "60".into()),
                    (A::X2, "560".into()),
                    (A::Y1, "155".into()),
                    (A::Y2, "155".into()),
                ],
                vec![],
            ),
            element(
                T::Text,
                vec![(A::X, "8".into()), (A::Y, "20".into())],
                vec![text(&metric.unit)],
            ),
            element(
                T::Text,
                vec![(A::X, "4".into()), (A::Y, "40".into())],
                vec![text(significant(hi))],
            ),
            element(
                T::Text,
                vec![(A::X, "4".into()), (A::Y, "155".into())],
                vec![text(significant(lo))],
            ),
        ];
        for (label, value) in &metric.refs {
            drawing.push(element(
                T::Line,
                vec![
                    (A::Class, "reference".into()),
                    (A::X1, "60".into()),
                    (A::X2, "560".into()),
                    (A::Y1, y(*value).to_string()),
                    (A::Y2, y(*value).to_string()),
                    (A::StrokeDasharray, "5 4".into()),
                ],
                vec![],
            ));
            drawing.push(element(
                T::Text,
                vec![
                    (A::X, "560".into()),
                    (A::Y, (y(*value) - 4.).to_string()),
                    (A::TextAnchor, "end".into()),
                ],
                vec![text(format!("{label} {}", significant(*value)))],
            ));
        }
        for (index, b) in baselines.iter().enumerate() {
            let next = f
                .epochs()
                .iter()
                .find(|epoch| epoch.at > b.at)
                .map_or(last, |epoch| epoch.at);
            let break_at = f
                .regimes()
                .iter()
                .filter(|r| r.bridge.is_none() && r.at > b.at && r.at <= next)
                .map(|r| r.at)
                .min();
            let end = break_at.unwrap_or(next);
            let mut d = format!("M {} {} H {}", x(b.at), y(b.value), x(end));
            if break_at.is_none()
                && let Some(next) = baselines.get(index + 1).filter(|p| p.at == next)
            {
                let _ = write!(d, " V {}", y(next.value));
            }
            drawing.push(element(
                T::Path,
                vec![(A::Class, "baseline-step".into()), (A::D, d)],
                vec![],
            ));
        }
        for p in baselines.iter().chain(&trials) {
            drawing.push(element(
                T::A,
                vec![(A::Href, format!("{}#{}", url(s), p.record))],
                vec![
                    node(T::Title, vec![text(&p.title)]),
                    element(
                        T::Circle,
                        vec![
                            (A::Class, p.class.into()),
                            (A::Cx, x(p.at).to_string()),
                            (A::Cy, y(p.value).to_string()),
                            (A::R, if p.class == "baseline" { "4" } else { "3" }.into()),
                        ],
                        vec![],
                    ),
                ],
            ));
        }
        for epoch in f.epochs() {
            drawing.push(element(
                T::Text,
                vec![
                    (A::X, x(epoch.at).to_string()),
                    (A::Y, "177".into()),
                    (A::TextAnchor, "middle".into()),
                ],
                vec![text(format!(
                    "{} · {}",
                    epoch.baseline,
                    crate::inspect::stamp(epoch.at, false)
                ))],
            ));
        }
        charts.push(element(
            T::Section,
            vec![(A::Class, "kpi".into())],
            vec![
                node(T::B, vec![text(key)]),
                element(
                    T::Svg,
                    vec![
                        (A::ViewBox, "0 0 580 185".into()),
                        (A::AriaLabel, format!("{key}, {}", metric.unit)),
                    ],
                    drawing,
                ),
            ],
        ));
    }
    let mut timeline = Vec::new();
    for r in f.regimes() {
        timeline.push(node(
            T::Li,
            vec![text(format!(
                "Regime {} · {} · {}",
                r.number,
                if r.bridge.is_some() {
                    "bridged"
                } else {
                    "unbridged"
                },
                r.gauge.contract
            ))],
        ));
    }
    for epoch in f.epochs() {
        timeline.push(node(
            T::Li,
            vec![link(
                format!("{}#{}", url(s), record("baseline", &epoch.baseline)),
                format!(
                    "Epoch of {} · since {}",
                    epoch.baseline,
                    crate::inspect::stamp(epoch.at, false)
                ),
            )],
        ));
    }
    if !timeline.is_empty() {
        charts.push(node(
            T::Details,
            vec![
                node(T::Summary, vec![text("Epochs and regimes")]),
                node(T::Ul, timeline),
            ],
        ));
    }
    for (slug, r) in f.reviews() {
        charts.push(link(
            format!("{}#{}", url(s), record("review", slug)),
            format!("Review: {}", r.title),
        ));
    }
    Ok(element(T::Div, vec![(A::Class, "charts".into())], charts))
}
