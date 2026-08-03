use fidget_spinner_core::FrontierVerdict;
use fidget_spinner_store_sqlite::FrontierChartScene;
use maud::{Markup, html};

use super::axis::ValueAxisPlan;
use super::{
    CHART_HEIGHT, CHART_WIDTH, ChartPlan, ChartPointMarker, ChartSeriesPlan, format_metric_value,
    point_marker,
};
use crate::ui::experiment_href;

const BACKGROUND: &str = "#FFFAF2";
const GRID: &str = "#DFD1BD";
const GRID_STRONG: &str = "#CFBEA8";
const INK: &str = "#4F473A";
const MUTED: &str = "#6F6557";
const FONT: &str = "Iosevka Web, IBM Plex Mono, SFMono-Regular, monospace";

macro_rules! chart_svg {
    ($plan:ident, $layout:ident, $($body:tt)*) => {
        html! {
            svg
                xmlns="http://www.w3.org/2000/svg"
                width=(CHART_WIDTH)
                height=(CHART_HEIGHT)
                viewBox=(format!("0 0 {CHART_WIDTH} {CHART_HEIGHT}"))
                role="group"
                tabindex="0"
                aria-label="Experiment KPI plot. Drag horizontally to zoom. Use Left and Right Arrow to inspect experiments and Enter to open one."
                data-chart-navigator="true"
                data-plot-left=(pixel($layout.left))
                data-plot-right=(pixel($layout.right))
                data-plot-top=(pixel($layout.top))
                data-plot-bottom=(pixel($layout.bottom))
                data-window-first=($plan.x.first)
                data-window-last=($plan.x.last)
            {
                $($body)*
            }
        }
    };
}

#[derive(Clone, Copy, Debug)]
struct ChartLayout {
    left: f64,
    right: f64,
    top: f64,
    bottom: f64,
}

impl ChartLayout {
    fn new(plan: &ChartPlan) -> Self {
        Self {
            left: 78.0,
            right: if plan.axes.len() > 1 {
                1_020.0
            } else {
                1_064.0
            },
            top: 24.0 + legend_rows(plan) as f64 * 18.0,
            bottom: 372.0,
        }
    }
}

pub(in crate::ui) fn render_chart_svg(plan: &ChartPlan, scene: &FrontierChartScene) -> String {
    let layout = ChartLayout::new(plan);
    chart_svg! { plan, layout,
        rect x="0" y="0" width=(CHART_WIDTH) height=(CHART_HEIGHT) fill=(BACKGROUND) {}
        metadata data-chart-metadata="true" { (chart_metadata(plan)) }
        (render_legend(plan))
        (render_axes(plan, &layout))
        (render_references(plan, &layout))
        (render_series(plan, &layout))
        (render_hit_bands(plan, scene, &layout))
    }
    .into_string()
}

fn legend_rows(plan: &ChartPlan) -> usize {
    let mut rows = 1;
    let mut x = 18_usize;
    for series in &plan.series {
        let width = legend_label(series).chars().count() * 7 + 38;
        if x + width > 1_070 {
            rows += 1;
            x = 18;
        }
        x += width;
    }
    rows.min(4)
}

fn render_legend(plan: &ChartPlan) -> Markup {
    let mut x = 18_i32;
    let mut y = 18_i32;
    let entries = plan
        .series
        .iter()
        .filter_map(|series| {
            let label = legend_label(series);
            let width = i32::try_from(label.chars().count()).unwrap_or(24) * 7 + 38;
            if x + width > 1_070 {
                x = 18;
                y += 18;
            }
            let position = (y <= 72).then_some((series, label, x, y));
            x += width;
            position
        })
        .collect::<Vec<_>>();
    let omitted = plan.series.len().saturating_sub(entries.len());
    html! {
        @for (series, label, x, y) in entries {
            g data-chart-legend-series=(series.metric.key.as_str()) {
                line
                    x1=(x)
                    y1=(y - 4)
                    x2=(x + 20)
                    y2=(y - 4)
                    stroke=(series.color)
                    stroke-width="2"
                {}
                text
                    x=(x + 25)
                    y=(y)
                    fill=(INK)
                    font-family=(FONT)
                    font-size="11"
                { (label) }
            }
        }
        @if omitted != 0 {
            text x="18" y="90" fill=(MUTED) font-family=(FONT) font-size="11" {
                "+" (omitted) " more series"
            }
        }
    }
}

fn legend_label(series: &ChartSeriesPlan) -> String {
    let mut chars = series.metric.key.as_str().chars();
    let prefix = chars.by_ref().take(28).collect::<String>();
    if chars.next().is_some() {
        prefix + "…"
    } else {
        prefix
    }
}

fn render_axes(plan: &ChartPlan, layout: &ChartLayout) -> Markup {
    html! {
        g fill="none" stroke=(GRID_STRONG) stroke-width="1" {
            line
                x1=(pixel(layout.left)) y1=(pixel(layout.bottom))
                x2=(pixel(layout.right)) y2=(pixel(layout.bottom))
            {}
            line
                x1=(pixel(layout.left)) y1=(pixel(layout.top))
                x2=(pixel(layout.left)) y2=(pixel(layout.bottom))
            {}
        }
        @for tick in &plan.x.ticks {
            @let x = plan.x.position(*tick, layout.left, layout.right);
            line
                x1=(pixel(x)) y1=(pixel(layout.top))
                x2=(pixel(x)) y2=(pixel(layout.bottom))
                stroke=(GRID) stroke-width="1" stroke-dasharray="1 5"
            {}
            text
                x=(pixel(x)) y="391" text-anchor="middle"
                fill=(MUTED) font-family=(FONT) font-size="11"
            { (tick) }
        }
        text
            x="550" y="411" text-anchor="middle"
            fill=(MUTED) font-family=(FONT) font-size="12"
        { "close order" }
        @for (index, axis) in plan.axes.iter().enumerate() {
            (render_value_axis(axis, index, layout))
        }
    }
}

fn render_value_axis(axis: &ValueAxisPlan, index: usize, layout: &ChartLayout) -> Markup {
    let left = index == 0;
    let x = if left { layout.left } else { layout.right };
    let ticks = axis
        .ticks
        .iter()
        .filter_map(|tick| {
            axis.position_display(tick.value, layout.top, layout.bottom)
                .map(|y| (tick, y))
        })
        .collect::<Vec<_>>();
    let title_x = if left { 16 } else { 1_086 };
    let transform = if left {
        "rotate(-90 16 220)"
    } else {
        "rotate(90 1086 220)"
    };
    html! {
        @for (tick, y) in ticks {
            @if left {
                line
                    x1=(pixel(layout.left)) y1=(pixel(y))
                    x2=(pixel(layout.right)) y2=(pixel(y))
                    stroke=(GRID) stroke-width="1" stroke-dasharray="1 5"
                {}
            }
            line
                x1=(pixel(x)) y1=(pixel(y))
                x2=(pixel(if left { x - 5.0 } else { x + 5.0 })) y2=(pixel(y))
                stroke=(GRID_STRONG)
            {}
            text
                x=(pixel(if left { x - 9.0 } else { x + 9.0 }))
                y=(pixel(y + 4.0))
                text-anchor=(if left { "end" } else { "start" })
                fill=(MUTED) font-family=(FONT) font-size="11"
            { (tick.label) }
        }
        text
            x=(title_x) y="220" text-anchor="middle" transform=(transform)
            fill=(INK) font-family=(FONT) font-size="12"
        { (axis.unit.label()) }
    }
}

fn render_references(plan: &ChartPlan, layout: &ChartLayout) -> Markup {
    html! {
        @for series in &plan.series {
            @if let Some(axis) = plan.axes.get(series.axis_index) {
                @for reference in series.kpi.iter().flat_map(|kpi| &kpi.references) {
                    @if let Some(y) = axis.position(reference.canonical_value, layout.top, layout.bottom) {
                        line
                            x1=(pixel(layout.left)) y1=(pixel(y))
                            x2=(pixel(layout.right)) y2=(pixel(y))
                            stroke=(series.color) stroke-width="1"
                            stroke-opacity="0.55" stroke-dasharray="5 5"
                        {}
                    }
                }
            }
        }
    }
}

fn render_series(plan: &ChartPlan, layout: &ChartLayout) -> Markup {
    html! {
        @for series in &plan.series {
            @if let Some(axis) = plan.axes.get(series.axis_index) {
                g data-chart-series=(series.metric.key.as_str()) {
                    polyline
                        fill="none" stroke=(series.color) stroke-width="2"
                        stroke-linejoin="round"
                        points=(polyline_points(plan, series, axis, layout))
                    {}
                    @for point in &series.points {
                        @let x = plan.x.position(point.ordinal, layout.left, layout.right);
                        @if let Some(y) = axis.position(point.canonical_value, layout.top, layout.bottom) {
                            (render_marker(x, y, series.color, point.verdict))
                        }
                    }
                }
            }
        }
    }
}

fn polyline_points(
    plan: &ChartPlan,
    series: &ChartSeriesPlan,
    axis: &ValueAxisPlan,
    layout: &ChartLayout,
) -> String {
    series
        .points
        .iter()
        .filter_map(|point| {
            let x = plan.x.position(point.ordinal, layout.left, layout.right);
            axis.position(point.canonical_value, layout.top, layout.bottom)
                .map(|y| format!("{},{}", pixel(x), pixel(y)))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn render_marker(x: f64, y: f64, color: &str, verdict: FrontierVerdict) -> Markup {
    html! {
        @match point_marker(verdict) {
            ChartPointMarker::Circle => {
                circle cx=(pixel(x)) cy=(pixel(y)) r="3" fill=(color) {}
            }
            ChartPointMarker::Triangle => {
                polygon
                    points=(format!(
                        "{},{} {},{} {},{}",
                        pixel(x), pixel(y - 4.0),
                        pixel(x - 4.0), pixel(y + 4.0),
                        pixel(x + 4.0), pixel(y + 4.0),
                    ))
                    fill=(BACKGROUND) stroke=(color) stroke-width="2"
                {}
            }
            ChartPointMarker::Cross => {
                path
                    d=(format!(
                        "M {} {} L {} {} M {} {} L {} {}",
                        pixel(x - 3.0), pixel(y - 3.0),
                        pixel(x + 3.0), pixel(y + 3.0),
                        pixel(x - 3.0), pixel(y + 3.0),
                        pixel(x + 3.0), pixel(y - 3.0),
                    ))
                    fill="none" stroke=(color) stroke-width="2"
                {}
            }
        }
    }
}

fn render_hit_bands(plan: &ChartPlan, scene: &FrontierChartScene, layout: &ChartLayout) -> Markup {
    let denominator = plan.x.last.saturating_sub(plan.x.first).max(1) as f64;
    let half_width = (layout.right - layout.left) / denominator / 2.0;
    html! {
        @for ordinal in &plan.hit_ordinals {
            @if let Some(experiment) = scene.experiments.get(*ordinal) {
                @let center = plan.x.position(*ordinal, layout.left, layout.right);
                @let left = (center - half_width).max(layout.left);
                @let right = (center + half_width).min(layout.right);
                a
                    href=(experiment_href(&experiment.slug))
                    tabindex="-1"
                    data-chart-hit="true"
                    data-ordinal=(ordinal)
                {
                    rect
                        x=(pixel(left)) y=(pixel(layout.top))
                        width=(pixel((right - left).max(1.0)))
                        height=(pixel(layout.bottom - layout.top))
                        fill="transparent" pointer-events="all"
                    {
                        title { (experiment.title) }
                    }
                }
            }
        }
    }
}

fn chart_metadata(plan: &ChartPlan) -> String {
    let labels = plan
        .series
        .iter()
        .map(|series| series.metric.key.as_str())
        .collect::<Vec<_>>();
    let values = plan
        .hit_ordinals
        .iter()
        .map(|ordinal| {
            let values = plan
                .series
                .iter()
                .map(|series| {
                    series
                        .points
                        .binary_search_by_key(ordinal, |point| point.ordinal)
                        .ok()
                        .and_then(|index| series.points.get(index))
                        .map(|point| format_metric_value(&series.metric, point.canonical_value))
                })
                .collect::<Vec<_>>();
            (*ordinal, values)
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&(labels, values)).unwrap_or_else(|_| "[]".to_owned())
}

fn pixel(value: f64) -> i32 {
    value.round() as i32
}
