mod axis;
mod cache;
mod svg;

use std::collections::{BTreeMap, BTreeSet};

use fidget_spinner_core::{FrontierVerdict, MetricQuantity};
use fidget_spinner_store_sqlite::{FrontierChartScene, KpiSummary, MetricKeySummary};

use super::{MetricAxisLogScales, render_dimension_value};
use axis::ValueAxisPlan;

pub(super) use cache::{ChartSceneCacheKey, SharedChartSceneCache};
pub(super) use svg::render_chart_svg;

pub(super) const CHART_WIDTH: i32 = 1100;
pub(super) const CHART_HEIGHT: i32 = 420;

const SERIES_COLORS: [&str; 12] = [
    "#8A4F3D", "#59778A", "#6B7A48", "#8A6B38", "#735D8B", "#3E7B73", "#A05252", "#4F6D92",
    "#7A673E", "#7D526A", "#4F7652", "#8A5A44",
];

pub(super) fn series_color(index: usize) -> &'static str {
    SERIES_COLORS[index % SERIES_COLORS.len()]
}

pub(super) fn ui_scalar(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

pub(super) fn format_metric_value(metric: &MetricKeySummary, canonical_value: f64) -> String {
    ValueAxisPlan::build(
        &metric.dimension,
        std::iter::once(metric.display_unit.clone()),
        &[canonical_value],
        false,
    )
    .map_or_else(
        || canonical_value.to_string(),
        |axis| axis.format_value(canonical_value),
    )
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ChartWindowRequest {
    pub(super) from: Option<String>,
    pub(super) to: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ChartSelection {
    pub(super) metric_selection_explicit: bool,
    pub(super) hidden_metrics: BTreeSet<String>,
    pub(super) conditions: BTreeMap<String, String>,
    pub(super) window: ChartWindowRequest,
    pub(super) logarithmic: MetricAxisLogScales,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ChartPointMarker {
    Circle,
    Triangle,
    Cross,
}

pub(super) fn point_marker(verdict: FrontierVerdict) -> ChartPointMarker {
    match verdict {
        FrontierVerdict::Accepted | FrontierVerdict::Kept => ChartPointMarker::Circle,
        FrontierVerdict::Parked => ChartPointMarker::Triangle,
        FrontierVerdict::Rejected | FrontierVerdict::Scuffed => ChartPointMarker::Cross,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ChartPointPlan {
    pub(super) ordinal: usize,
    pub(super) canonical_value: f64,
    pub(super) verdict: FrontierVerdict,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ChartSeriesPlan {
    pub(super) metric: MetricKeySummary,
    pub(super) kpi: Option<KpiSummary>,
    pub(super) axis_index: usize,
    pub(super) color: &'static str,
    pub(super) points: Vec<ChartPointPlan>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct OrdinalAxisPlan {
    pub(super) first: usize,
    pub(super) last: usize,
    pub(super) ticks: Vec<usize>,
}

impl OrdinalAxisPlan {
    pub(super) fn position(&self, ordinal: usize, left: f64, right: f64) -> f64 {
        if self.first == self.last {
            return left.midpoint(right);
        }
        let numerator = ui_scalar(ordinal.saturating_sub(self.first));
        let denominator = ui_scalar(self.last.saturating_sub(self.first));
        left + numerator / denominator * (right - left)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ChartPlan {
    pub(super) x: OrdinalAxisPlan,
    axes: Vec<ValueAxisPlan>,
    pub(super) series: Vec<ChartSeriesPlan>,
    pub(super) hit_ordinals: Vec<usize>,
    pub(super) logarithmic_support: MetricAxisLogScales,
    pub(super) window_warning: Option<String>,
}

impl ChartPlan {
    pub(super) fn build(
        scene: &FrontierChartScene,
        selected_metrics: &[MetricKeySummary],
        selection: &ChartSelection,
    ) -> Self {
        let (first, last, window_warning) = resolve_window(scene, &selection.window);
        let x = OrdinalAxisPlan {
            first,
            last,
            ticks: ordinal_ticks(first, last),
        };
        let selected_by_key = selected_metrics
            .iter()
            .enumerate()
            .map(|(index, metric)| (metric.key.as_str(), (index, metric)))
            .collect::<BTreeMap<_, _>>();
        let mut candidates = scene
            .series
            .iter()
            .filter_map(|series| {
                let (color_index, selected_metric) =
                    selected_by_key.get(series.metric.key.as_str()).copied()?;
                if selection
                    .hidden_metrics
                    .contains(series.metric.key.as_str())
                {
                    return None;
                }
                let points = series
                    .canonical_values
                    .iter()
                    .enumerate()
                    .filter_map(|(ordinal, value)| {
                        let experiment = scene.experiments.get(ordinal)?;
                        let value = value.as_ref().copied()?;
                        (ordinal >= first
                            && ordinal <= last
                            && value.is_finite()
                            && experiment.verdict != FrontierVerdict::Scuffed
                            && experiment_matches_conditions(experiment, &selection.conditions))
                        .then_some(ChartPointPlan {
                            ordinal,
                            canonical_value: value,
                            verdict: experiment.verdict,
                        })
                    })
                    .collect::<Vec<_>>();
                (!points.is_empty()).then_some((
                    (*selected_metric).clone(),
                    series.kpi.clone(),
                    series_color(color_index),
                    points,
                ))
            })
            .collect::<Vec<_>>();

        let mut quantities = Vec::<MetricQuantity>::new();
        candidates.retain(|(metric, _, _, _)| {
            if quantities.contains(&metric.dimension) {
                return true;
            }
            if quantities.len() == 2 {
                return false;
            }
            quantities.push(metric.dimension.clone());
            true
        });

        let values_by_axis = quantities
            .iter()
            .map(|quantity| {
                candidates
                    .iter()
                    .filter(|(metric, _, _, _)| &metric.dimension == quantity)
                    .flat_map(|(_, kpi, _, points)| {
                        points
                            .iter()
                            .map(|point| point.canonical_value)
                            .chain(kpi.iter().flat_map(|kpi| {
                                kpi.references
                                    .iter()
                                    .map(|reference| reference.canonical_value)
                            }))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let logarithmic_support = MetricAxisLogScales {
            primary: values_by_axis
                .first()
                .is_some_and(|values| ValueAxisPlan::supports_logarithmic(values)),
            secondary: values_by_axis
                .get(1)
                .is_some_and(|values| ValueAxisPlan::supports_logarithmic(values)),
        };
        let effective_logarithmic = MetricAxisLogScales {
            primary: selection.logarithmic.primary && logarithmic_support.primary,
            secondary: selection.logarithmic.secondary && logarithmic_support.secondary,
        };
        let axes = quantities
            .iter()
            .enumerate()
            .filter_map(|(index, quantity)| {
                ValueAxisPlan::build(
                    quantity,
                    candidates
                        .iter()
                        .filter(|(metric, _, _, _)| metric.dimension == *quantity)
                        .map(|(metric, _, _, _)| metric.display_unit.clone()),
                    &values_by_axis[index],
                    if index == 0 {
                        effective_logarithmic.primary
                    } else {
                        effective_logarithmic.secondary
                    },
                )
            })
            .collect::<Vec<_>>();
        let series = candidates
            .into_iter()
            .filter_map(|(metric, kpi, color, points)| {
                let axis_index = quantities
                    .iter()
                    .position(|quantity| quantity == &metric.dimension)?;
                let _ = axes.get(axis_index)?;
                Some(ChartSeriesPlan {
                    metric,
                    kpi,
                    axis_index,
                    color,
                    points,
                })
            })
            .collect::<Vec<_>>();
        let hit_ordinals = series
            .iter()
            .flat_map(|series| series.points.iter().map(|point| point.ordinal))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self {
            x,
            axes,
            series,
            hit_ordinals,
            logarithmic_support,
            window_warning,
        }
    }

    pub(super) fn has_visible_data(&self) -> bool {
        !self.series.is_empty()
    }

    pub(super) fn has_secondary_axis(&self) -> bool {
        self.axes.len() > 1
    }

    pub(super) fn format_value(&self, series: &ChartSeriesPlan, canonical_value: f64) -> String {
        self.axes.get(series.axis_index).map_or_else(
            || canonical_value.to_string(),
            |axis| axis.format_value(canonical_value),
        )
    }
}

fn experiment_matches_conditions(
    experiment: &fidget_spinner_store_sqlite::FrontierChartExperiment,
    conditions: &BTreeMap<String, String>,
) -> bool {
    conditions.iter().all(|(key, expected)| {
        experiment.dimensions.iter().any(|(observed_key, value)| {
            observed_key.as_str() == key && render_dimension_value(value) == *expected
        })
    })
}

fn resolve_window(
    scene: &FrontierChartScene,
    request: &ChartWindowRequest,
) -> (usize, usize, Option<String>) {
    let full_last = scene.experiments.len().saturating_sub(1);
    let (Some(from), Some(to)) = (request.from.as_deref(), request.to.as_deref()) else {
        let warning = (request.from.is_some() || request.to.is_some()).then(|| {
            "The requested experiment window is incomplete; showing the full history.".to_owned()
        });
        return (0, full_last, warning);
    };
    let from = scene
        .experiments
        .iter()
        .position(|experiment| experiment.slug.as_str() == from);
    let to = scene
        .experiments
        .iter()
        .position(|experiment| experiment.slug.as_str() == to);
    match (from, to) {
        (Some(from), Some(to)) => (from.min(to), from.max(to), None),
        _ => (
            0,
            full_last,
            Some(
                "The requested experiment window no longer exists; showing the full history."
                    .to_owned(),
            ),
        ),
    }
}

fn ordinal_ticks(first: usize, last: usize) -> Vec<usize> {
    let span = last.saturating_sub(first);
    if span <= 8 {
        return (first..=last).collect();
    }
    let raw_stride = span.div_ceil(8);
    let magnitude = 10_usize.pow(raw_stride.ilog10());
    let fraction = raw_stride.div_ceil(magnitude);
    let stride = match fraction {
        0 | 1 => magnitude,
        2 => magnitude * 2,
        3..=5 => magnitude * 5,
        _ => magnitude * 10,
    };
    let start = first.div_ceil(stride) * stride;
    let mut ticks = (start..=last).step_by(stride).collect::<Vec<_>>();
    if ticks.first().copied() != Some(first) {
        ticks.insert(0, first);
    }
    if ticks.last().copied() != Some(last) {
        ticks.push(last);
    }
    ticks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinal_ticks_are_dense_for_small_windows() {
        assert_eq!(ordinal_ticks(3, 7), vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn ordinal_ticks_are_bounded_for_large_windows() {
        assert!(ordinal_ticks(0, 1_914).len() <= 10);
    }
}
