use super::detail::{
    render_experiment_card, render_experiment_link_chip, render_experiment_section,
    render_experiment_summary_line, render_frontier_active_sets, render_frontier_brief,
    render_frontier_header,
};
use super::{
    BLACK, BTreeMap, BTreeSet, ChartBuilder, Circle, Color, Cross, DashedLineSeries,
    DimensionFacet, ExperimentStatus, ExperimentSummary, FrontierMetricSeries,
    FrontierOpenProjection, FrontierPageQuery, FrontierTab, FrontierVerdict,
    HypothesisCurrentState, IntoDrawingArea, IntoFont, IntoLogRange, LineSeries,
    ListExperimentsQuery, ListHypothesesQuery, METRIC_TABLE_TITLE_MIN_BUDGET_CH,
    METRIC_TABLE_TITLE_PERCENT_BUDGET, Markup, MetricAxisLogScales, MetricDisplayUnit,
    MetricKeysQuery, MetricQuantity, MetricScope, NonEmptyText, PathElement, PreEscaped, RGBColor,
    SVGBackend, SeriesLabelPosition, ShapeStyle, Slug, StoreError, experiment_href,
    format_metric_value, format_timestamp, frontier_href, frontier_tab_href_with_query, html,
    hypothesis_href, limit_items, project_metrics_frontier_href, render_dimension_value,
    status_chip_classes, verdict_class,
};
use plotters::coord::ranged1d::{LightPoints, Ranged};
use plotters::coord::types::RangedCoordf64;

const METRIC_CHART_ACCEPTED_MARKER_RADIUS: i32 = 2;
const METRIC_CHART_REJECTED_MARKER_SIZE: i32 = 3;
const METRIC_CHART_LIGHT_LINE_LIMIT: usize = 5;
const METRIC_CHART_Y_LABEL_COUNT: usize = 6;
const METRIC_CHART_DOTTED_GRID_DASH: i32 = 1;
const METRIC_CHART_DOTTED_GRID_GAP: i32 = 5;
const METRIC_CHART_LOG_BUCKET_REFINEMENT_COUNT: usize = 4;

pub(super) fn render_frontier_tab_content(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    projection: &FrontierOpenProjection,
    tab: FrontierTab,
    query: &FrontierPageQuery,
    limit: Option<u32>,
) -> Result<Markup, StoreError> {
    match tab {
        FrontierTab::Brief => Ok(html! {
            (render_frontier_header(&projection.frontier))
            (render_frontier_brief(projection))
            (render_frontier_active_sets(projection))
        }),
        FrontierTab::Open => Ok(html! {
            (render_frontier_header(&projection.frontier))
            (render_hypothesis_current_state_grid(&projection.active_hypotheses, limit))
            (render_open_experiment_grid(&projection.open_experiments, limit))
        }),
        FrontierTab::Closed => {
            let closed_hypotheses = store
                .list_hypotheses(ListHypothesesQuery {
                    frontier: Some(projection.frontier.slug.to_string()),
                    limit: None,
                    ..ListHypothesesQuery::default()
                })?
                .into_iter()
                .filter(|hypothesis| hypothesis.open_experiment_count == 0)
                .collect::<Vec<_>>();
            let closed_experiments = store.list_experiments(ListExperimentsQuery {
                frontier: Some(projection.frontier.slug.to_string()),
                status: Some(ExperimentStatus::Closed),
                limit: None,
                ..ListExperimentsQuery::default()
            })?;
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_closed_hypothesis_grid(&closed_hypotheses, limit))
                (render_experiment_section("Closed Experiments", &closed_experiments, limit))
            })
        }
        FrontierTab::Results => {
            let kpi_metrics = store.metric_keys(MetricKeysQuery {
                frontier: Some(projection.frontier.slug.to_string()),
                scope: MetricScope::Kpi,
            })?;
            let other_metric_keys = load_other_metric_keys(store, projection)?;
            let visible_metrics = visible_metric_catalog(&kpi_metrics, &other_metric_keys);
            let requested_metrics = requested_or_kpi_metric_keys(&query.metric, &kpi_metrics);
            let selected_metrics =
                resolve_selected_metric_keys(&requested_metrics, &visible_metrics);
            let series = selected_metrics
                .iter()
                .map(|metric| {
                    store.frontier_metric_series(
                        projection.frontier.slug.as_str(),
                        &metric.key,
                        true,
                    )
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            let dimension_filters = query.condition_filters();
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_metric_series_section(
                    &projection.frontier.slug,
                    &kpi_metrics,
                    &other_metric_keys,
                    &selected_metrics,
                    &series,
                    &dimension_filters,
                    query.requested_log_scales(),
                    query.table_metric.as_deref(),
                    limit,
                ))
            })
        }
    }
}

pub(super) fn render_frontier_tab_bar(
    frontier_slug: &Slug,
    active_tab: FrontierTab,
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    log_scales: MetricAxisLogScales,
    dimension_filters: &BTreeMap<String, String>,
    table_metric: Option<&str>,
) -> Markup {
    const TABS: [FrontierTab; 4] = [
        FrontierTab::Results,
        FrontierTab::Brief,
        FrontierTab::Open,
        FrontierTab::Closed,
    ];
    html! {
        nav.tab-row aria-label="Frontier tabs" {
            @for tab in TABS {
                @let href = frontier_tab_href_with_query(
                    frontier_slug,
                    tab,
                    selected_metrics,
                    log_scales,
                    dimension_filters,
                    table_metric,
                );
                a
                    href=(href)
                    class={(if tab == active_tab { "tab-chip active" } else { "tab-chip" })}
                {
                    (tab.label())
                }
            }
        }
    }
}

pub(super) fn visible_metric_catalog(
    kpi_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    other_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Vec<fidget_spinner_store_sqlite::MetricKeySummary> {
    kpi_metric_keys
        .iter()
        .chain(other_metric_keys.iter())
        .cloned()
        .collect()
}

pub(super) fn load_other_metric_keys(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    projection: &FrontierOpenProjection,
) -> Result<Vec<fidget_spinner_store_sqlite::MetricKeySummary>, StoreError> {
    let candidate_metrics = if projection.active_metric_keys.is_empty() {
        store.metric_keys(MetricKeysQuery {
            frontier: Some(projection.frontier.slug.to_string()),
            scope: MetricScope::Default,
        })?
    } else {
        projection.active_metric_keys.clone()
    };
    Ok(candidate_metrics
        .into_iter()
        .filter(|metric| {
            !projection
                .kpis
                .iter()
                .any(|kpi| kpi.metric.key == metric.key)
        })
        .collect())
}

pub(super) fn requested_or_kpi_metric_keys(
    requested_metrics: &[String],
    kpi_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Vec<String> {
    if requested_metrics.is_empty() {
        kpi_metric_keys
            .iter()
            .map(|metric| metric.key.to_string())
            .collect()
    } else {
        requested_metrics.to_vec()
    }
}

pub(super) fn resolve_selected_metric_keys(
    requested_metrics: &[String],
    visible_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Vec<fidget_spinner_store_sqlite::MetricKeySummary> {
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    let mut families = MetricAxisFamilies::default();
    for requested in requested_metrics {
        let selector = requested.trim();
        if selector.is_empty() {
            continue;
        }
        let Some(metric) = visible_metrics
            .iter()
            .find(|metric| metric.key.as_str() == selector)
        else {
            continue;
        };
        if !seen.insert(metric.key.clone()) {
            continue;
        }
        if !families.admit(metric.dimension.clone()) {
            continue;
        }
        selected.push(metric.clone());
    }
    if selected.is_empty() {
        visible_metrics
            .first()
            .cloned()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        selected
    }
}

fn render_closed_hypothesis_grid(
    hypotheses: &[fidget_spinner_store_sqlite::HypothesisSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { "Closed Hypotheses" }
        @if hypotheses.is_empty() {
            p.muted { "No dormant hypotheses yet." }
        } @else {
            div.card-grid {
                @for hypothesis in limit_items(hypotheses, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(hypothesis_href(&hypothesis.slug)) {
                                (hypothesis.title)
                            }
                            @if let Some(verdict) = hypothesis.latest_verdict {
                                span class=(status_chip_classes(verdict_class(verdict))) {
                                    (verdict.as_str())
                                }
                            }
                        }
                        p.prose { (hypothesis.summary) }
                        @if !hypothesis.tags.is_empty() {
                            div.chip-row {
                                @for tag in &hypothesis.tags {
                                    span.tag-chip { (tag) }
                                }
                            }
                        }
                        div.meta-row.muted {
                            span { "updated " (format_timestamp(hypothesis.updated_at)) }
                        }
                    }
                }
            }
        }
    }
    }
}

struct FilteredMetricSeries<'a> {
    metric: &'a fidget_spinner_store_sqlite::MetricKeySummary,
    points: Vec<&'a fidget_spinner_store_sqlite::FrontierMetricPoint>,
}

struct MetricChartSeries {
    label: String,
    color: RGBColor,
    side: MetricAxisSide,
    points: Vec<(i32, f64, FrontierVerdict)>,
}

pub(super) fn render_metric_series_section(
    frontier_slug: &Slug,
    kpi_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    other_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    series: &[FrontierMetricSeries],
    dimension_filters: &BTreeMap<String, String>,
    requested_log_scales: MetricAxisLogScales,
    requested_table_metric: Option<&str>,
    limit: Option<u32>,
) -> Markup {
    let facets = collect_dimension_facets_from_series(series);
    let filtered_series = filter_metric_series(series, dimension_filters);
    let plotted_series = filtered_series
        .iter()
        .filter(|series| !series.points.is_empty())
        .collect::<Vec<_>>();
    let experiment_positions = collect_metric_experiment_positions(&plotted_series);
    let chart_axes = MetricAxisSet::from_series(&plotted_series);
    let log_support = chart_axes
        .as_ref()
        .map(|axes| metric_chart_log_support(axes, &plotted_series))
        .unwrap_or_default();
    let effective_log_scales = log_support.clamp(requested_log_scales);
    let no_metric_history =
        selected_metrics.is_empty() || series.iter().all(|series| series.points.is_empty());
    let table_series = filtered_series
        .iter()
        .find(|series| {
            requested_table_metric.is_some_and(|requested| series.metric.key.as_str() == requested)
        })
        .or_else(|| filtered_series.first());
    let active_table_metric = table_series.map(|series| series.metric.key.as_str());
    html! {
    section.card id="metric-plot-card" {
        div.card-header.plot-card-header {
            h2 { "Plot" }
            a.form-button href=(project_metrics_frontier_href(frontier_slug)) { "KPIs" }
            div.plot-toolbar {
                (render_metric_filter_popout(
                    frontier_slug,
                    selected_metrics,
                    &facets,
                    dimension_filters,
                    effective_log_scales,
                    active_table_metric,
                ))
                (render_metric_selection_popout(
                    frontier_slug,
                    kpi_metric_keys,
                    other_metric_keys,
                    selected_metrics,
                    dimension_filters,
                    effective_log_scales,
                    log_support,
                    active_table_metric,
                ))
            }
        }
        @if kpi_metric_keys.is_empty() && other_metric_keys.is_empty() {
            p.muted { "No visible metrics registered for this frontier." }
        } @else if no_metric_history {
            p.muted { "No closed experiments for the current metric selection yet." }
        } @else if plotted_series.is_empty() {
            p.muted { "No closed experiments match the current filters." }
        } @else if let Some(axes) = chart_axes.as_ref() {
            div.chart-frame {
                div.chart-action-row {
                    button.plot-copy-png type="button" data-copy-plot-png="true" aria-live="polite" {
                        "Copy PNG"
                    }
                }
                (PreEscaped(render_metric_chart_svg(axes, &plotted_series, effective_log_scales)))
            }
            @if let Some(table_series) = table_series {
                section.subcard.metric-table-section {
                    div.metric-table-header {
                        h3 { "Experiments" }
                        @if filtered_series.len() > 1 {
                            nav.metric-table-tabs aria-label="Experiment table metric" {
                                @for metric_series in &filtered_series {
                                    @let href = frontier_tab_href_with_query(
                                        frontier_slug,
                                        FrontierTab::Results,
                                        selected_metrics,
                                        effective_log_scales,
                                        dimension_filters,
                                        Some(metric_series.metric.key.as_str()),
                                    );
                                    a
                                        href=(href)
                                        data-preserve-viewport="true"
                                        class={(if metric_series.metric.key == table_series.metric.key {
                                            "metric-table-tab active"
                                        } else {
                                            "metric-table-tab"
                                        })}
                                    {
                                        (&metric_series.metric.key)
                                    }
                                }
                            }
                        }
                    }
                    p.muted.metric-table-caption {
                        (&table_series.metric.key) " · " (table_series.points.len()) " rows"
                    }
                    @if table_series.points.is_empty() {
                        p.muted { "No closed experiments match the current filters for this metric." }
                    } @else {
                        @let table_points = recent_first_metric_points(&table_series.points);
                        @let visible_points = limit_items(&table_points, limit);
                        @let table_layout = MetricTableLayout::for_points(visible_points);
                        div.table-scroll {
                            table.metric-table {
                                colgroup {
                                    col.metric-table-fit-col;
                                    col.metric-table-title-col style=(table_layout.experiment_width_style());
                                    col.metric-table-title-col style=(table_layout.hypothesis_width_style());
                                    col.metric-table-fit-col;
                                    col.metric-table-fit-col;
                                    col.metric-table-fit-col;
                                }
                                thead {
                                    tr {
                                        th.metric-table-fit-heading { "#" }
                                        th.metric-table-title-heading { "Experiment" }
                                        th.metric-table-title-heading { "Hypothesis" }
                                        th.metric-table-fit-heading { "Closed" }
                                        th.metric-table-fit-heading { "Verdict" }
                                        th.metric-table-fit-heading { "Value" }
                                    }
                                }
                                tbody {
                                    @for (index, point) in visible_points.iter().copied().enumerate() {
                                        @let display_index = experiment_positions
                                            .get(point.experiment.slug.as_str())
                                            .copied()
                                            .unwrap_or(index)
                                            + 1;
                                        tr {
                                            td.metric-table-rank-cell {
                                                span.metric-table-fixed-text { (display_index.to_string()) }
                                            }
                                            td.metric-table-title-cell {
                                                (render_metric_table_title_link(
                                                    &point.experiment.title,
                                                    &experiment_href(&point.experiment.slug),
                                                ))
                                            }
                                            td.metric-table-title-cell {
                                                (render_metric_table_title_link(
                                                    &point.hypothesis.title,
                                                    &hypothesis_href(&point.hypothesis.slug),
                                                ))
                                            }
                                            td.metric-table-closed-cell.nowrap {
                                                span.metric-table-fixed-text {
                                                    (format_timestamp(point.closed_at))
                                                }
                                            }
                                            td.metric-table-verdict-cell {
                                                span
                                                    class=(format!(
                                                        "{} metric-table-verdict-chip",
                                                        status_chip_classes(verdict_class(point.verdict)),
                                                    ))
                                                {
                                                    (point.verdict.as_str())
                                                }
                                            }
                                            td.metric-table-value-cell.nowrap {
                                                span.metric-table-fixed-text {
                                                    (format_metric_value(point.value, &table_series.metric.display_unit))
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_metric_filter_popout(
    frontier_slug: &Slug,
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    facets: &[DimensionFacet],
    active_filters: &BTreeMap<String, String>,
    log_scales: MetricAxisLogScales,
    table_metric: Option<&str>,
) -> Markup {
    let clear_href = frontier_tab_href_with_query(
        frontier_slug,
        FrontierTab::Results,
        selected_metrics,
        log_scales,
        &BTreeMap::new(),
        table_metric,
    );
    let label = if active_filters.is_empty() {
        "Filters".to_owned()
    } else {
        format!("Filters {}", active_filters.len())
    };
    html! {
    details.control-popout id="metric-filter-popout" data-preserve-open="true" {
        summary.control-popout-toggle { (label) }
        div.control-popout-panel {
            h3 id="condition-filters" { "Condition Filters" }
            @if facets.is_empty() {
                p.muted { "No conditions for the current selection." }
            } @else {
                form.filter-form.auto-submit-form method="get" action=(frontier_href(frontier_slug)) data-preserve-viewport="true" {
                    input type="hidden" name="tab" value="results";
                    (render_metric_selection_hidden_inputs(selected_metrics))
                    (render_log_hidden_inputs(log_scales))
                    (render_table_metric_hidden_input(table_metric))
                    div.filter-form-grid {
                        @for facet in facets {
                            label.filter-control id=(metric_filter_anchor_id(&facet.key)) {
                                span.filter-label { (&facet.key) }
                                select.filter-select data-auto-submit="true" name=(format!("condition.{}", facet.key)) {
                                    option
                                        value=""
                                        selected[active_filters.get(&facet.key).is_none()]
                                    { "all" }
                                    @for value in &facet.values {
                                        option
                                            value=(value)
                                            selected[active_filters.get(&facet.key) == Some(value)]
                                        { (value) }
                                    }
                                }
                            }
                        }
                    }
                    div.filter-actions {
                        a.clear-filter href=(clear_href) data-preserve-viewport="true" { "Clear all" }
                    }
                }
            }
            @if active_filters.is_empty() {
                p.muted { "No condition filters active." }
            } @else {
                div.chip-row {
                    @for (key, value) in active_filters {
                        @let href = frontier_tab_href_with_query(
                            frontier_slug,
                            FrontierTab::Results,
                            selected_metrics,
                            log_scales,
                            &remove_dimension_filter(active_filters, key),
                            table_metric,
                        );
                        a.metric-filter-chip.active href=(href) data-preserve-viewport="true" {
                            (key) "=" (value) " ×"
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_metric_selection_popout(
    frontier_slug: &Slug,
    kpi_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    other_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    dimension_filters: &BTreeMap<String, String>,
    log_scales: MetricAxisLogScales,
    log_support: MetricAxisLogSupport,
    table_metric: Option<&str>,
) -> Markup {
    let label = metric_popout_label(selected_metrics, log_scales);
    let selected_families = MetricAxisFamilies::from_metrics(selected_metrics);
    html! {
    details.control-popout id="metric-selection-popout" data-preserve-open="true" {
        summary.control-popout-toggle { (label) }
        div.control-popout-panel.metric-popout-panel {
            form.metric-picker-form.auto-submit-form method="get" action=(frontier_href(frontier_slug)) data-preserve-viewport="true" {
                input type="hidden" name="tab" value="results";
                (render_dimension_filter_hidden_inputs(dimension_filters))
                (render_table_metric_hidden_input(table_metric))
                div.metric-popout-layout {
                    div.metric-picker-main {
                        @if !kpi_metric_keys.is_empty() {
                            section.metric-picker-group {
                                h4 { "KPIs" }
                                div.metric-picker-list {
                                    @for metric in kpi_metric_keys {
                                        (render_metric_picker_option(
                                            frontier_slug,
                                            metric,
                                            selected_metrics,
                                            &selected_families,
                                            dimension_filters,
                                            log_scales,
                                        ))
                                    }
                                }
                            }
                        }
                        @if !other_metric_keys.is_empty() {
                            details.metric-picker-disclosure id="metric-other-metrics-disclosure" data-preserve-open="true" {
                                summary.metric-picker-disclosure-toggle {
                                    "Other Metrics " (other_metric_keys.len())
                                }
                                div.metric-picker-list {
                                    @for metric in other_metric_keys {
                                        (render_metric_picker_option(
                                            frontier_slug,
                                            metric,
                                            selected_metrics,
                                            &selected_families,
                                            dimension_filters,
                                            log_scales,
                                        ))
                                    }
                                }
                            }
                        }
                    }
                    aside.metric-picker-sidecar {
                        h4 { "Options" }
                        label.metric-checkbox-row.metric-checkbox-row-compact title=(if log_support.primary {
                            "Positive-only filtered values on the left axis. Toggles logarithmic scaling on the left y axis."
                        } else {
                            "Left-axis logarithmic scaling is only available when all plotted left-axis values are strictly positive."
                        }) {
                            input
                                type="checkbox"
                                data-auto-submit="true"
                                name="log_y_primary"
                                value="1"
                                checked[log_scales.primary]
                                disabled[!log_support.primary];
                            span.metric-checkbox-copy {
                                span.metric-checkbox-title { "Left Log" }
                            }
                        }
                        @if log_support.has_secondary {
                            label.metric-checkbox-row.metric-checkbox-row-compact title=(if log_support.secondary {
                                "Positive-only filtered values on the right axis. Toggles logarithmic scaling on the right y axis."
                            } else {
                                "Right-axis logarithmic scaling is only available when all plotted right-axis values are strictly positive."
                            }) {
                                input
                                    type="checkbox"
                                    data-auto-submit="true"
                                    name="log_y_secondary"
                                    value="1"
                                    checked[log_scales.secondary]
                                    disabled[!log_support.secondary];
                                span.metric-checkbox-copy {
                                    span.metric-checkbox-title { "Right Log" }
                                }
                            }
                        }
                        p.muted.compact-note {
                            "The first two unit families become left and right axes; later metrics must match one of them."
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_metric_picker_option(
    frontier_slug: &Slug,
    metric: &fidget_spinner_store_sqlite::MetricKeySummary,
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    selected_families: &MetricAxisFamilies,
    dimension_filters: &BTreeMap<String, String>,
    log_scales: MetricAxisLogScales,
) -> Markup {
    let selected = selected_metrics
        .iter()
        .any(|selected_metric| selected_metric.key == metric.key);
    let compatible = selected_families.supports(&metric.dimension);
    let detail = format!(
        "{} · {}",
        metric.objective.as_str(),
        metric.display_unit.label()
    );
    if compatible || selected {
        html! {
            label class={(if selected {
                "metric-checkbox-row selected"
            } else {
                "metric-checkbox-row"
            })} title=(detail) {
                input type="checkbox" data-auto-submit="true" name="metric" value=(metric.key.as_str()) checked[selected];
                span.metric-checkbox-copy {
                    span.metric-checkbox-title { (&metric.key) }
                }
            }
        }
    } else {
        let replacement = std::slice::from_ref(metric);
        let href = frontier_tab_href_with_query(
            frontier_slug,
            FrontierTab::Results,
            replacement,
            log_scales,
            dimension_filters,
            Some(metric.key.as_str()),
        );
        html! {
            a.metric-checkbox-row.incompatible href=(href) data-preserve-viewport="true" title=(format!("{detail} · click to switch metric family")) {
                span.metric-checkbox-copy {
                    span.metric-checkbox-title { (&metric.key) }
                }
            }
        }
    }
}

fn render_metric_chart_svg(
    axes: &MetricAxisSet,
    series: &[&FilteredMetricSeries<'_>],
    log_scales: MetricAxisLogScales,
) -> String {
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (1100, 420)).into_drawing_area();
        if root.fill(&RGBColor(255, 250, 242)).is_err() {
            return chart_error_markup("chart fill failed");
        }
        let chart_series = match build_metric_chart_series(axes, series) {
            Some(series) if !series.is_empty() => series,
            _ => return chart_error_markup("no plottable metric points"),
        };
        let primary_values = chart_series
            .iter()
            .filter(|series| series.side == MetricAxisSide::Primary)
            .flat_map(|series| series.points.iter().map(|(_, value, _)| *value))
            .collect::<Vec<_>>();
        let Some((primary_min, primary_max)) =
            metric_chart_y_range(&primary_values, log_scales.primary)
        else {
            return chart_error_markup("metric values are non-finite");
        };
        let secondary_values = chart_series
            .iter()
            .filter(|series| series.side == MetricAxisSide::Secondary)
            .flat_map(|series| series.points.iter().map(|(_, value, _)| *value))
            .collect::<Vec<_>>();
        let secondary_range = if axes.secondary.is_some() {
            let Some(range) = metric_chart_y_range(&secondary_values, log_scales.secondary) else {
                return chart_error_markup("secondary metric values are non-finite");
            };
            Some(range)
        } else {
            None
        };
        let x_end = chart_series
            .iter()
            .flat_map(|series| series.points.iter().map(|(x, _, _)| *x))
            .max()
            .unwrap_or(0)
            .max(1);

        macro_rules! draw_metric_side {
            ($chart:expr, $method:ident, $side:expr) => {{
                for series in chart_series.iter().filter(|series| series.side == $side) {
                    let line_points = series
                        .points
                        .iter()
                        .map(|(x, value, _)| (*x, *value))
                        .collect::<Vec<_>>();
                    if $chart
                        .$method(LineSeries::new(line_points, &series.color))
                        .map(|series_plot| {
                            series_plot.label(series.label.clone()).legend(|(x, y)| {
                                PathElement::new(vec![(x, y), (x + 18, y)], series.color)
                            })
                        })
                        .is_err()
                    {
                        return chart_error_markup("line draw failed");
                    }

                    let accepted_points = series
                        .points
                        .iter()
                        .filter(|(_, _, verdict)| *verdict != FrontierVerdict::Rejected)
                        .map(|(x, value, _)| {
                            Circle::new(
                                (*x, *value),
                                METRIC_CHART_ACCEPTED_MARKER_RADIUS,
                                ShapeStyle::from(&series.color).filled(),
                            )
                        });
                    if $chart.$method(accepted_points).is_err() {
                        return chart_error_markup("accepted marker draw failed");
                    }

                    let rejected_points = series
                        .points
                        .iter()
                        .filter(|(_, _, verdict)| *verdict == FrontierVerdict::Rejected)
                        .map(|(x, value, _)| {
                            Cross::new(
                                (*x, *value),
                                METRIC_CHART_REJECTED_MARKER_SIZE,
                                ShapeStyle::from(&series.color).stroke_width(2),
                            )
                        });
                    if $chart.$method(rejected_points).is_err() {
                        return chart_error_markup("rejected marker draw failed");
                    }
                }
            }};
        }

        macro_rules! draw_primary_chart {
            ($chart:expr, $primary_min:expr, $primary_max:expr) => {{
                let chart = &mut $chart;
                if chart
                    .configure_mesh()
                    .light_line_style(RGBColor(223, 209, 189).mix(0.6))
                    .bold_line_style(RGBColor(207, 190, 168).mix(0.8))
                    .axis_style(RGBColor(103, 86, 63))
                    .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
                    .y_labels(METRIC_CHART_Y_LABEL_COUNT)
                    .max_light_lines(METRIC_CHART_LIGHT_LINE_LIMIT)
                    .x_desc("close order")
                    .y_desc(axes.primary.display_unit.label())
                    .x_label_formatter(&|value| format!("{}", value + 1))
                    .draw()
                    .is_err()
                {
                    return chart_error_markup("mesh draw failed");
                }

                if log_scales.primary {
                    let refinement_style =
                        ShapeStyle::from(&RGBColor(223, 209, 189).mix(0.72)).stroke_width(1);
                    for value in
                        metric_chart_log_bucket_refinement_values($primary_min, $primary_max)
                    {
                        if chart
                            .draw_series(DashedLineSeries::new(
                                [(0_i32, value), (x_end, value)],
                                METRIC_CHART_DOTTED_GRID_DASH,
                                METRIC_CHART_DOTTED_GRID_GAP,
                                refinement_style,
                            ))
                            .is_err()
                        {
                            return chart_error_markup("primary refinement grid draw failed");
                        }
                    }
                }

                draw_metric_side!(chart, draw_series, MetricAxisSide::Primary);

                if chart
                    .configure_series_labels()
                    .position(SeriesLabelPosition::UpperLeft)
                    .background_style(RGBColor(255, 250, 242).mix(0.92))
                    .border_style(RGBColor(207, 190, 168))
                    .label_font(("Iosevka Web", 11).into_font().color(&BLACK))
                    .draw()
                    .is_err()
                {
                    return chart_error_markup("legend draw failed");
                }
            }};
        }

        macro_rules! draw_dual_chart {
            ($chart:expr, $primary_min:expr, $primary_max:expr, $secondary_min:expr, $secondary_max:expr) => {{
                let chart = &mut $chart;
                if chart
                    .configure_mesh()
                    .light_line_style(RGBColor(223, 209, 189).mix(0.6))
                    .bold_line_style(RGBColor(207, 190, 168).mix(0.8))
                    .axis_style(RGBColor(103, 86, 63))
                    .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
                    .y_labels(METRIC_CHART_Y_LABEL_COUNT)
                    .max_light_lines(METRIC_CHART_LIGHT_LINE_LIMIT)
                    .x_desc("close order")
                    .y_desc(axes.primary.display_unit.label())
                    .x_label_formatter(&|value| format!("{}", value + 1))
                    .draw()
                    .is_err()
                {
                    return chart_error_markup("mesh draw failed");
                }

                if log_scales.primary {
                    let refinement_style =
                        ShapeStyle::from(&RGBColor(223, 209, 189).mix(0.72)).stroke_width(1);
                    for value in
                        metric_chart_log_bucket_refinement_values($primary_min, $primary_max)
                    {
                        if chart
                            .draw_series(DashedLineSeries::new(
                                [(0_i32, value), (x_end, value)],
                                METRIC_CHART_DOTTED_GRID_DASH,
                                METRIC_CHART_DOTTED_GRID_GAP,
                                refinement_style,
                            ))
                            .is_err()
                        {
                            return chart_error_markup("primary refinement grid draw failed");
                        }
                    }
                }

                if let Some(secondary_axis) = axes.secondary.as_ref() {
                    let secondary_grid_style =
                        ShapeStyle::from(&RGBColor(89, 119, 138).mix(0.28)).stroke_width(1);
                    for value in metric_chart_secondary_grid_values(
                        $secondary_min,
                        $secondary_max,
                        log_scales.secondary,
                    ) {
                        if chart
                            .draw_secondary_series(DashedLineSeries::new(
                                [(0_i32, value), (x_end, value)],
                                METRIC_CHART_DOTTED_GRID_DASH,
                                METRIC_CHART_DOTTED_GRID_GAP,
                                secondary_grid_style,
                            ))
                            .is_err()
                        {
                            return chart_error_markup("secondary grid draw failed");
                        }
                    }

                    if chart
                        .configure_secondary_axes()
                        .axis_style(RGBColor(103, 86, 63))
                        .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
                        .y_labels(METRIC_CHART_Y_LABEL_COUNT)
                        .y_desc(secondary_axis.display_unit.label())
                        .draw()
                        .is_err()
                    {
                        return chart_error_markup("secondary axis draw failed");
                    }
                }

                draw_metric_side!(chart, draw_series, MetricAxisSide::Primary);
                draw_metric_side!(chart, draw_secondary_series, MetricAxisSide::Secondary);

                if chart
                    .configure_series_labels()
                    .position(SeriesLabelPosition::UpperLeft)
                    .background_style(RGBColor(255, 250, 242).mix(0.92))
                    .border_style(RGBColor(207, 190, 168))
                    .label_font(("Iosevka Web", 11).into_font().color(&BLACK))
                    .draw()
                    .is_err()
                {
                    return chart_error_markup("legend draw failed");
                }
            }};
        }

        if let Some((secondary_min, secondary_max)) = secondary_range {
            match (log_scales.primary, log_scales.secondary) {
                (true, true) => {
                    let mut chart = match ChartBuilder::on(&root)
                        .margin(18)
                        .x_label_area_size(32)
                        .y_label_area_size(84)
                        .right_y_label_area_size(84)
                        .build_cartesian_2d(0_i32..x_end, (primary_min..primary_max).log_scale())
                    {
                        Ok(chart) => chart.set_secondary_coord(
                            0_i32..x_end,
                            (secondary_min..secondary_max).log_scale(),
                        ),
                        Err(error) => {
                            return chart_error_markup(&format!("chart build failed: {error:?}"));
                        }
                    };
                    draw_dual_chart!(
                        chart,
                        primary_min,
                        primary_max,
                        secondary_min,
                        secondary_max
                    );
                }
                (true, false) => {
                    let mut chart = match ChartBuilder::on(&root)
                        .margin(18)
                        .x_label_area_size(32)
                        .y_label_area_size(84)
                        .right_y_label_area_size(84)
                        .build_cartesian_2d(0_i32..x_end, (primary_min..primary_max).log_scale())
                    {
                        Ok(chart) => {
                            chart.set_secondary_coord(0_i32..x_end, secondary_min..secondary_max)
                        }
                        Err(error) => {
                            return chart_error_markup(&format!("chart build failed: {error:?}"));
                        }
                    };
                    draw_dual_chart!(
                        chart,
                        primary_min,
                        primary_max,
                        secondary_min,
                        secondary_max
                    );
                }
                (false, true) => {
                    let mut chart = match ChartBuilder::on(&root)
                        .margin(18)
                        .x_label_area_size(32)
                        .y_label_area_size(84)
                        .right_y_label_area_size(84)
                        .build_cartesian_2d(0_i32..x_end, primary_min..primary_max)
                    {
                        Ok(chart) => chart.set_secondary_coord(
                            0_i32..x_end,
                            (secondary_min..secondary_max).log_scale(),
                        ),
                        Err(error) => {
                            return chart_error_markup(&format!("chart build failed: {error:?}"));
                        }
                    };
                    draw_dual_chart!(
                        chart,
                        primary_min,
                        primary_max,
                        secondary_min,
                        secondary_max
                    );
                }
                (false, false) => {
                    let mut chart = match ChartBuilder::on(&root)
                        .margin(18)
                        .x_label_area_size(32)
                        .y_label_area_size(84)
                        .right_y_label_area_size(84)
                        .build_cartesian_2d(0_i32..x_end, primary_min..primary_max)
                    {
                        Ok(chart) => {
                            chart.set_secondary_coord(0_i32..x_end, secondary_min..secondary_max)
                        }
                        Err(error) => {
                            return chart_error_markup(&format!("chart build failed: {error:?}"));
                        }
                    };
                    draw_dual_chart!(
                        chart,
                        primary_min,
                        primary_max,
                        secondary_min,
                        secondary_max
                    );
                }
            }
        } else if log_scales.primary {
            let mut chart = match ChartBuilder::on(&root)
                .margin(18)
                .x_label_area_size(32)
                .y_label_area_size(84)
                .build_cartesian_2d(0_i32..x_end, (primary_min..primary_max).log_scale())
            {
                Ok(chart) => chart,
                Err(error) => return chart_error_markup(&format!("chart build failed: {error:?}")),
            };
            draw_primary_chart!(chart, primary_min, primary_max);
        } else {
            let mut chart = match ChartBuilder::on(&root)
                .margin(18)
                .x_label_area_size(32)
                .y_label_area_size(84)
                .build_cartesian_2d(0_i32..x_end, primary_min..primary_max)
            {
                Ok(chart) => chart,
                Err(error) => return chart_error_markup(&format!("chart build failed: {error:?}")),
            };
            draw_primary_chart!(chart, primary_min, primary_max);
        }

        if root.present().is_err() {
            return chart_error_markup("chart present failed");
        }
    }
    svg
}

pub(super) fn metric_chart_secondary_grid_values(
    min_value: f64,
    max_value: f64,
    log_y: bool,
) -> Vec<f64> {
    if !(min_value.is_finite() && max_value.is_finite()) || min_value >= max_value {
        return Vec::new();
    }
    if log_y {
        return metric_chart_log_grid_values(min_value, max_value);
    }
    metric_chart_linear_grid_values(min_value, max_value)
}

fn metric_chart_linear_grid_values(min_value: f64, max_value: f64) -> Vec<f64> {
    let coord = RangedCoordf64::from(min_value..max_value);
    coord
        .key_points(LightPoints::new(
            METRIC_CHART_Y_LABEL_COUNT,
            METRIC_CHART_Y_LABEL_COUNT * METRIC_CHART_LIGHT_LINE_LIMIT,
        ))
        .into_iter()
        .filter(|value| *value > min_value && *value < max_value)
        .collect()
}

fn metric_chart_log_grid_values(min_value: f64, max_value: f64) -> Vec<f64> {
    if min_value <= 0.0 {
        return Vec::new();
    }
    let log_min = min_value.log10();
    let log_max = max_value.log10();
    let point_count = METRIC_CHART_Y_LABEL_COUNT * METRIC_CHART_LIGHT_LINE_LIMIT;
    let step_count = point_count.saturating_sub(1);
    if step_count == 0 {
        return Vec::new();
    }
    let mut values = (0..=step_count)
        .map(|index| 10_f64.powf(log_min + (log_max - log_min) * index as f64 / step_count as f64))
        .filter(|value| value.is_finite() && *value > min_value && *value < max_value)
        .collect::<Vec<_>>();
    values.extend(metric_chart_log_bucket_refinement_values(
        min_value, max_value,
    ));
    values.sort_by(f64::total_cmp);
    values.dedup_by(|left, right| {
        (*left - *right).abs() <= f64::EPSILON * left.abs().max(right.abs()).max(1.0)
    });
    values
}

fn metric_chart_log_bucket_refinement_values(min_value: f64, max_value: f64) -> Vec<f64> {
    if !(min_value.is_finite() && max_value.is_finite())
        || min_value <= 0.0
        || min_value >= max_value
    {
        return Vec::new();
    }
    let lower_bucket_floor = 10_f64.powf(min_value.log10().floor());
    let lower_bucket_ceiling = lower_bucket_floor * 10.0;
    if min_value / lower_bucket_floor < 9.0 {
        return Vec::new();
    }
    let visible_bucket_ceiling = lower_bucket_ceiling.min(max_value);
    if visible_bucket_ceiling <= min_value {
        return Vec::new();
    }
    (1..=METRIC_CHART_LOG_BUCKET_REFINEMENT_COUNT)
        .map(|step| {
            let ratio = step as f64 / (METRIC_CHART_LOG_BUCKET_REFINEMENT_COUNT + 1) as f64;
            min_value * (visible_bucket_ceiling / min_value).powf(ratio)
        })
        .filter(|value| value.is_finite() && *value > min_value && *value < visible_bucket_ceiling)
        .collect()
}

fn chart_error_markup(message: &str) -> String {
    format!(
        "<div class=\"chart-error\">chart render failed: {}</div>",
        html_escape(message)
    )
}

fn html_escape(raw: &str) -> String {
    raw.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn render_hypothesis_current_state_grid(
    states: &[HypothesisCurrentState],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { "Active Hypotheses" }
        @if states.is_empty() {
            p.muted { "No active hypotheses." }
        } @else {
            div.card-grid {
                @for state in limit_items(states, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(hypothesis_href(&state.hypothesis.slug)) {
                                (state.hypothesis.title)
                            }
                            @if let Some(verdict) = state.hypothesis.latest_verdict {
                                span class=(status_chip_classes(verdict_class(verdict))) {
                                    (verdict.as_str())
                                }
                            }
                        }
                        p.prose { (state.hypothesis.summary) }
                        @if !state.hypothesis.tags.is_empty() {
                            div.chip-row {
                                @for tag in &state.hypothesis.tags {
                                    span.tag-chip { (tag) }
                                }
                            }
                        }
                        div.meta-row {
                            span { (format!("{} open", state.open_experiments.len())) }
                            @if let Some(latest) = state.latest_closed_experiment.as_ref() {
                                span {
                                    "latest "
                                    a href=(experiment_href(&latest.slug)) { (latest.title) }
                                }
                            } @else {
                                span.muted { "no closed experiments" }
                            }
                        }
                        @if !state.open_experiments.is_empty() {
                            div.related-block {
                                h3 { "Open" }
                                div.link-list {
                                    @for experiment in &state.open_experiments {
                                        (render_experiment_link_chip(experiment))
                                    }
                                }
                            }
                        }
                        @if let Some(latest) = state.latest_closed_experiment.as_ref() {
                            div.related-block {
                                h3 { "Latest Closed" }
                                (render_experiment_summary_line(latest))
                            }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_open_experiment_grid(experiments: &[ExperimentSummary], limit: Option<u32>) -> Markup {
    html! {
    section.card {
        h2 { "Open Experiments" }
        @if experiments.is_empty() {
            p.muted { "No open experiments." }
        } @else {
            div.card-grid {
                @for experiment in limit_items(experiments, limit) {
                    (render_experiment_card(experiment))
                }
            }
        }
    }
    }
}
fn render_metric_table_title_link(title: &NonEmptyText, href: &str) -> Markup {
    html! {
        a href=(href) class="metric-table-link" title=(title.as_str()) {
            (title.as_str())
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MetricAxisFamilies {
    families: Vec<MetricQuantity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricAxisSide {
    Primary,
    Secondary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetricAxisSet {
    primary: MetricChartAxis,
    secondary: Option<MetricChartAxis>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MetricAxisLogSupport {
    primary: bool,
    secondary: bool,
    has_secondary: bool,
}

impl MetricAxisLogSupport {
    fn clamp(self, requested: MetricAxisLogScales) -> MetricAxisLogScales {
        MetricAxisLogScales {
            primary: requested.primary && self.primary,
            secondary: requested.secondary && self.has_secondary && self.secondary,
        }
    }
}

impl MetricAxisFamilies {
    fn from_metrics(metrics: &[fidget_spinner_store_sqlite::MetricKeySummary]) -> Self {
        let mut families = Self::default();
        for metric in metrics {
            let _ = families.admit(metric.dimension.clone());
        }
        families
    }

    fn admit(&mut self, family: MetricQuantity) -> bool {
        if self.families.iter().any(|active| active == &family) {
            return true;
        }
        if self.families.len() >= 2 {
            return false;
        }
        self.families.push(family);
        true
    }

    fn supports(&self, quantity: &MetricQuantity) -> bool {
        self.families.len() < 2 || self.families.iter().any(|family| family == quantity)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MetricChartAxis {
    display_unit: MetricDisplayUnit,
    quantity: MetricQuantity,
}

impl MetricChartAxis {
    pub(super) fn from_metric(metric: &fidget_spinner_store_sqlite::MetricKeySummary) -> Self {
        Self {
            display_unit: metric.display_unit.clone(),
            quantity: metric.dimension.clone(),
        }
    }

    pub(super) fn normalize_value(&self, value: f64, unit: &MetricDisplayUnit) -> Option<f64> {
        if unit.quantity() != self.quantity {
            return None;
        }
        Some(self.display_unit.display_value(unit.canonical_value(value)))
    }
}

impl MetricAxisSet {
    fn from_series(series: &[&FilteredMetricSeries<'_>]) -> Option<Self> {
        let primary = MetricChartAxis::from_metric(series.first()?.metric);
        let secondary = series
            .iter()
            .map(|series| MetricChartAxis::from_metric(series.metric))
            .find(|axis| axis.quantity != primary.quantity);
        Some(Self { primary, secondary })
    }

    fn axis_for_metric(
        &self,
        metric: &fidget_spinner_store_sqlite::MetricKeySummary,
    ) -> Option<(MetricAxisSide, &MetricChartAxis)> {
        if metric.dimension == self.primary.quantity {
            return Some((MetricAxisSide::Primary, &self.primary));
        }
        self.secondary
            .as_ref()
            .filter(|axis| axis.quantity == metric.dimension)
            .map(|axis| (MetricAxisSide::Secondary, axis))
    }
}

fn collect_dimension_facets_from_series(series: &[FrontierMetricSeries]) -> Vec<DimensionFacet> {
    let mut values_by_key: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for series in series {
        for point in &series.points {
            for (key, value) in &point.dimensions {
                let _ = values_by_key
                    .entry(key.to_string())
                    .or_default()
                    .insert(render_dimension_value(value));
            }
        }
    }
    values_by_key
        .into_iter()
        .map(|(key, values)| DimensionFacet {
            key,
            values: values.into_iter().collect(),
        })
        .collect()
}

fn filter_metric_series<'a>(
    series: &'a [FrontierMetricSeries],
    dimension_filters: &BTreeMap<String, String>,
) -> Vec<FilteredMetricSeries<'a>> {
    series
        .iter()
        .map(|series| FilteredMetricSeries {
            metric: &series.metric,
            points: filter_metric_points(&series.points, dimension_filters),
        })
        .collect()
}

fn filter_metric_points<'a>(
    points: &'a [fidget_spinner_store_sqlite::FrontierMetricPoint],
    dimension_filters: &BTreeMap<String, String>,
) -> Vec<&'a fidget_spinner_store_sqlite::FrontierMetricPoint> {
    points
        .iter()
        .filter(|point| point_matches_dimension_filters(point, dimension_filters))
        .collect()
}

fn point_matches_dimension_filters(
    point: &fidget_spinner_store_sqlite::FrontierMetricPoint,
    dimension_filters: &BTreeMap<String, String>,
) -> bool {
    dimension_filters.iter().all(|(key, expected)| {
        point.dimensions.iter().any(|(point_key, point_value)| {
            point_key.as_str() == key && render_dimension_value(point_value) == *expected
        })
    })
}

fn render_metric_selection_hidden_inputs(
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Markup {
    html! {
        @for metric in selected_metrics {
            input type="hidden" name="metric" value=(metric.key.as_str());
        }
    }
}

fn render_dimension_filter_hidden_inputs(filters: &BTreeMap<String, String>) -> Markup {
    html! {
        @for (key, value) in filters {
            input type="hidden" name=(format!("condition.{key}")) value=(value);
        }
    }
}

fn render_log_hidden_inputs(log_scales: MetricAxisLogScales) -> Markup {
    html! {
        @if log_scales.primary {
            input type="hidden" name="log_y_primary" value="1";
        }
        @if log_scales.secondary {
            input type="hidden" name="log_y_secondary" value="1";
        }
    }
}

fn render_table_metric_hidden_input(table_metric: Option<&str>) -> Markup {
    html! {
        @if let Some(table_metric) = table_metric.filter(|table_metric| !table_metric.trim().is_empty()) {
            input type="hidden" name="table_metric" value=(table_metric);
        }
    }
}

fn remove_dimension_filter(
    filters: &BTreeMap<String, String>,
    key: &str,
) -> BTreeMap<String, String> {
    let mut next = filters.clone();
    let _ = next.remove(key);
    next
}

fn metric_filter_anchor_id(key: &str) -> String {
    format!("filter-{}", sanitize_fragment_id(key))
}

fn metric_popout_label(
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    log_scales: MetricAxisLogScales,
) -> String {
    let mut label = if selected_metrics.len() <= 1 {
        "Metric".to_owned()
    } else {
        format!("Metrics {}", selected_metrics.len())
    };
    match (log_scales.primary, log_scales.secondary) {
        (true, true) => label.push_str(" · log L+R"),
        (true, false) => label.push_str(" · log L"),
        (false, true) => label.push_str(" · log R"),
        (false, false) => {}
    }
    label
}

fn metric_chart_log_support(
    axes: &MetricAxisSet,
    series: &[&FilteredMetricSeries<'_>],
) -> MetricAxisLogSupport {
    let mut support = MetricAxisLogSupport {
        primary: true,
        secondary: axes.secondary.is_some(),
        has_secondary: axes.secondary.is_some(),
    };
    let mut saw_primary = false;
    let mut saw_secondary = false;
    for series in series {
        let Some((side, axis)) = axes.axis_for_metric(series.metric) else {
            return MetricAxisLogSupport::default();
        };
        for point in &series.points {
            let Some(value) = axis.normalize_value(point.value, &series.metric.display_unit) else {
                match side {
                    MetricAxisSide::Primary => support.primary = false,
                    MetricAxisSide::Secondary => support.secondary = false,
                }
                continue;
            };
            match side {
                MetricAxisSide::Primary => saw_primary = true,
                MetricAxisSide::Secondary => saw_secondary = true,
            }
            if value <= 0.0 || !value.is_finite() {
                match side {
                    MetricAxisSide::Primary => support.primary = false,
                    MetricAxisSide::Secondary => support.secondary = false,
                }
            }
        }
    }
    support.primary &= saw_primary;
    if support.has_secondary {
        support.secondary &= saw_secondary;
    }
    support
}

fn collect_metric_experiment_positions(
    series: &[&FilteredMetricSeries<'_>],
) -> BTreeMap<String, usize> {
    let mut experiment_positions = BTreeMap::new();
    let mut ordered_experiments = series
        .iter()
        .flat_map(|series| {
            series
                .points
                .iter()
                .map(|point| (point.closed_at, point.experiment.slug.as_str().to_owned()))
        })
        .collect::<Vec<_>>();
    ordered_experiments.sort_by_key(|(closed_at, _)| *closed_at);
    for (_, slug) in ordered_experiments {
        let next_index = experiment_positions.len();
        let _ = experiment_positions.entry(slug).or_insert(next_index);
    }
    experiment_positions
}

fn recent_first_metric_points<'a>(
    points: &[&'a fidget_spinner_store_sqlite::FrontierMetricPoint],
) -> Vec<&'a fidget_spinner_store_sqlite::FrontierMetricPoint> {
    let mut points = points.to_vec();
    points.sort_by(|left, right| {
        right.closed_at.cmp(&left.closed_at).then_with(|| {
            left.experiment
                .slug
                .as_str()
                .cmp(right.experiment.slug.as_str())
        })
    });
    points
}

fn build_metric_chart_series(
    axes: &MetricAxisSet,
    series: &[&FilteredMetricSeries<'_>],
) -> Option<Vec<MetricChartSeries>> {
    let experiment_positions = collect_metric_experiment_positions(series)
        .into_iter()
        .map(|(slug, index)| Some((slug, i32::try_from(index).ok()?)))
        .collect::<Option<BTreeMap<_, _>>>()?;

    series
        .iter()
        .enumerate()
        .map(|(index, series)| {
            let (side, axis) = axes.axis_for_metric(series.metric)?;
            let points = series
                .points
                .iter()
                .filter_map(|point| {
                    let x = *experiment_positions.get(point.experiment.slug.as_str())?;
                    let value = axis.normalize_value(point.value, &series.metric.display_unit)?;
                    Some((x, value, point.verdict))
                })
                .collect::<Vec<_>>();
            (!points.is_empty()).then(|| MetricChartSeries {
                color: metric_chart_color(index),
                label: series.metric.key.to_string(),
                side,
                points,
            })
        })
        .collect()
}

fn metric_chart_y_range(values: &[f64], log_y: bool) -> Option<(f64, f64)> {
    let (mut min_value, mut max_value) = values
        .iter()
        .copied()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), value| {
            (min.min(value), max.max(value))
        });
    if !min_value.is_finite() || !max_value.is_finite() {
        return None;
    }
    if log_y {
        if min_value <= 0.0 {
            return None;
        }
        if (max_value - min_value).abs() < f64::EPSILON {
            min_value *= 0.8;
            max_value *= 1.2;
        } else {
            min_value /= 1.18;
            max_value *= 1.18;
        }
        return Some((min_value, max_value));
    }
    if (max_value - min_value).abs() < f64::EPSILON {
        let pad = if max_value.abs() < 1.0 {
            1.0
        } else {
            max_value.abs() * 0.05
        };
        min_value -= pad;
        max_value += pad;
    } else {
        let pad = (max_value - min_value) * 0.08;
        min_value -= pad;
        max_value += pad;
    }
    Some((min_value, max_value))
}

fn metric_chart_color(index: usize) -> RGBColor {
    const COLORS: [RGBColor; 8] = [
        RGBColor(78, 121, 167),
        RGBColor(242, 142, 43),
        RGBColor(225, 87, 89),
        RGBColor(118, 183, 178),
        RGBColor(89, 161, 79),
        RGBColor(237, 201, 72),
        RGBColor(176, 122, 161),
        RGBColor(255, 157, 167),
    ];
    COLORS[index % COLORS.len()]
}

fn sanitize_fragment_id(raw: &str) -> String {
    raw.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetricTableLayout {
    experiment_percent: usize,
    hypothesis_percent: usize,
}

impl MetricTableLayout {
    fn for_points(points: &[&fidget_spinner_store_sqlite::FrontierMetricPoint]) -> Self {
        let experiment_lengths = points
            .iter()
            .map(|point| point.experiment.title.as_str().chars().count())
            .collect::<Vec<_>>();
        let hypothesis_lengths = points
            .iter()
            .map(|point| point.hypothesis.title.as_str().chars().count())
            .collect::<Vec<_>>();
        let (experiment_percent, hypothesis_percent) = best_metric_table_title_split(
            &experiment_lengths,
            &hypothesis_lengths,
            METRIC_TABLE_TITLE_PERCENT_BUDGET,
        );
        Self {
            experiment_percent,
            hypothesis_percent,
        }
    }

    fn experiment_width_style(self) -> String {
        Self::width_style(self.experiment_percent)
    }

    fn hypothesis_width_style(self) -> String {
        Self::width_style(self.hypothesis_percent)
    }

    fn width_style(percent: usize) -> String {
        format!("width: {percent}%;")
    }
}

pub(super) fn best_metric_table_title_split(
    experiment_lengths: &[usize],
    hypothesis_lengths: &[usize],
    total_budget: usize,
) -> (usize, usize) {
    if total_budget <= METRIC_TABLE_TITLE_MIN_BUDGET_CH * 2 {
        let experiment_chars = total_budget / 2;
        return (
            experiment_chars,
            total_budget.saturating_sub(experiment_chars),
        );
    }

    let candidate_bounds =
        METRIC_TABLE_TITLE_MIN_BUDGET_CH..=(total_budget - METRIC_TABLE_TITLE_MIN_BUDGET_CH);
    candidate_bounds
        .map(|experiment_chars| {
            let hypothesis_chars = total_budget - experiment_chars;
            let experiment_truncated_entries =
                truncated_entry_count(experiment_lengths, experiment_chars);
            let hypothesis_truncated_entries =
                truncated_entry_count(hypothesis_lengths, hypothesis_chars);
            let total_truncated_entries =
                experiment_truncated_entries + hypothesis_truncated_entries;
            let max_column_truncated_entries =
                experiment_truncated_entries.max(hypothesis_truncated_entries);
            let truncation_gap =
                experiment_truncated_entries.abs_diff(hypothesis_truncated_entries);
            let overflow_chars = truncated_overflow_chars(experiment_lengths, experiment_chars)
                + truncated_overflow_chars(hypothesis_lengths, hypothesis_chars);
            let imbalance = experiment_chars.abs_diff(hypothesis_chars);
            (
                (
                    total_truncated_entries,
                    max_column_truncated_entries,
                    truncation_gap,
                    overflow_chars,
                    imbalance,
                ),
                (experiment_chars, hypothesis_chars),
            )
        })
        .min_by_key(|(score, _)| *score)
        .map(|(_, split)| split)
        .unwrap_or_else(|| {
            let experiment_chars = total_budget / 2;
            (
                experiment_chars,
                total_budget.saturating_sub(experiment_chars),
            )
        })
}

pub(super) fn truncated_entry_count(lengths: &[usize], budget: usize) -> usize {
    lengths.iter().filter(|&&length| length > budget).count()
}

fn truncated_overflow_chars(lengths: &[usize], budget: usize) -> usize {
    lengths
        .iter()
        .map(|&length| length.saturating_sub(budget))
        .sum()
}
