use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fidget_spinner_core::{ExperimentStatus, FrontierVerdict, MetricQuantity, NonEmptyText, Slug};
use fidget_spinner_store_sqlite::{
    FrontierChartExperiment, FrontierChartScene, FrontierChartSeries, FrontierOpenProjection,
    HypothesisAttentionFilter, HypothesisCurrentState, HypothesisLifecycleFilter,
    ListExperimentsQuery, ListHypothesesQuery, MetricKeySummary, MetricKeysQuery, MetricScope,
    ProjectStore, StoreError,
};
use maud::{Markup, PreEscaped, html};

use super::chart::{
    ChartPlan, ChartSceneCacheKey, ChartSelection, format_metric_value as format_chart_value,
    render_chart_svg, series_color,
};
use super::detail::{
    render_experiment_card, render_experiment_section, render_frontier_active_sets,
    render_frontier_brief, render_frontier_header,
};
use super::{
    DimensionFacet, ExperimentSummary, FrontierPageQuery, FrontierTab, ProjectRenderContext,
    experiment_href, format_timestamp, frontier_href, frontier_tab_href_with_query,
    hypothesis_href, limit_items, metric_choice_detail, project_metrics_frontier_href,
    render_dimension_value, render_hypothesis_meta_chips, render_metric_kind_chip, scuff_icon,
    status_chip_classes, verdict_class,
};

const DEFAULT_METRIC_TABLE_ROWS: u32 = 250;

pub(super) fn render_frontier_tab_content(
    store: &ProjectStore,
    projection: &FrontierOpenProjection,
    tab: FrontierTab,
    query: &FrontierPageQuery,
    context: &ProjectRenderContext,
) -> Result<Markup, StoreError> {
    match tab {
        FrontierTab::Brief => Ok(html! {
            (render_frontier_header(&projection.frontier))
            (render_frontier_brief(projection))
            (render_frontier_active_sets(projection))
        }),
        FrontierTab::Open => Ok(html! {
            (render_frontier_header(&projection.frontier))
            (render_hypothesis_current_state_grid(
                &projection.worklist_hypotheses,
                context.limit,
            ))
            (render_open_experiment_grid(&projection.open_experiments, context.limit))
        }),
        FrontierTab::Experiments => {
            let experiments = store.list_experiments(ListExperimentsQuery {
                frontier: Some(projection.frontier.slug.to_string()),
                limit: None,
                ..ListExperimentsQuery::default()
            })?;
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_frontier_experiment_pane(&experiments, context.limit))
            })
        }
        FrontierTab::Closed => {
            let hypotheses = store.list_hypotheses(ListHypothesesQuery {
                frontier: Some(projection.frontier.slug.to_string()),
                attention: HypothesisAttentionFilter::Shelved,
                lifecycle: HypothesisLifecycleFilter::All,
                limit: None,
                ..ListHypothesesQuery::default()
            })?;
            let experiments = store.list_experiments(ListExperimentsQuery {
                frontier: Some(projection.frontier.slug.to_string()),
                status: Some(ExperimentStatus::Closed),
                limit: None,
                ..ListExperimentsQuery::default()
            })?;
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_history_hypothesis_grid(&hypotheses, context.limit))
                (render_experiment_section("Closed Experiments", &experiments, context.limit))
            })
        }
        FrontierTab::Results => {
            let kpi_metrics = projection
                .kpis
                .iter()
                .map(|kpi| kpi.metric.clone())
                .collect::<Vec<_>>();
            let other_metrics = load_other_metric_keys(store, projection)?;
            let visible_metrics = visible_metric_catalog(&kpi_metrics, &other_metrics);
            let scene = load_chart_scene(context, store, projection, &visible_metrics)?;
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_metric_series_section(
                    &scene,
                    &kpi_metrics,
                    &other_metrics,
                    query,
                    context,
                ))
            })
        }
    }
}

pub(super) fn render_frontier_chart_fragment(
    context: &ProjectRenderContext,
    selector: &str,
    query: &FrontierPageQuery,
) -> Result<Markup, StoreError> {
    let store = super::open_store(context.project_root.as_std_path())?;
    let frontier = store.read_frontier(selector)?;
    let key = ChartSceneCacheKey::new(
        context.project_root.clone(),
        frontier.slug.clone(),
        context.refresh_token.clone(),
    );
    let scene = if let Some(scene) = context.chart_cache.get(&key)? {
        scene
    } else {
        let projection = store.frontier_open(frontier.slug.as_str())?;
        let kpi_metrics = projection
            .kpis
            .iter()
            .map(|kpi| kpi.metric.clone())
            .collect::<Vec<_>>();
        let other_metrics = load_other_metric_keys(&store, &projection)?;
        let visible_metrics = visible_metric_catalog(&kpi_metrics, &other_metrics);
        load_chart_scene(context, &store, &projection, &visible_metrics)?
    };
    let (kpi_metrics, other_metrics) = scene_metric_catalog(&scene);
    Ok(render_metric_series_section(
        &scene,
        &kpi_metrics,
        &other_metrics,
        query,
        context,
    ))
}

fn load_chart_scene(
    context: &ProjectRenderContext,
    store: &ProjectStore,
    projection: &FrontierOpenProjection,
    visible_metrics: &[MetricKeySummary],
) -> Result<Arc<FrontierChartScene>, StoreError> {
    let key = ChartSceneCacheKey::new(
        context.project_root.clone(),
        projection.frontier.slug.clone(),
        context.refresh_token.clone(),
    );
    if let Some(scene) = context.chart_cache.get(&key)? {
        return Ok(scene);
    }
    let scene = Arc::new(store.frontier_chart_scene(
        projection.frontier.slug.as_str(),
        visible_metrics,
        &projection.kpis,
    )?);
    context.chart_cache.insert(key, Arc::clone(&scene))?;
    Ok(scene)
}

fn scene_metric_catalog(
    scene: &FrontierChartScene,
) -> (Vec<MetricKeySummary>, Vec<MetricKeySummary>) {
    let mut kpis = Vec::new();
    let mut others = Vec::new();
    for series in &scene.series {
        if series.kpi.is_some() {
            kpis.push(series.metric.clone());
        } else {
            others.push(series.metric.clone());
        }
    }
    (kpis, others)
}

pub(super) fn render_frontier_tab_bar(
    frontier_slug: &Slug,
    active_tab: FrontierTab,
    selected_metrics: &[MetricKeySummary],
    selection: &ChartSelection,
    table_metric: Option<&str>,
) -> Markup {
    const TABS: [FrontierTab; 5] = [
        FrontierTab::Results,
        FrontierTab::Experiments,
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
                    selection.logarithmic,
                    &selection.conditions,
                    table_metric,
                    Some(selection),
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
    kpi_metrics: &[MetricKeySummary],
    other_metrics: &[MetricKeySummary],
) -> Vec<MetricKeySummary> {
    kpi_metrics.iter().chain(other_metrics).cloned().collect()
}

pub(super) fn load_other_metric_keys(
    store: &ProjectStore,
    projection: &FrontierOpenProjection,
) -> Result<Vec<MetricKeySummary>, StoreError> {
    let candidates = if projection.active_metric_keys.is_empty() {
        store.metric_keys(MetricKeysQuery {
            frontier: Some(projection.frontier.slug.to_string()),
            scope: MetricScope::Default,
        })?
    } else {
        projection.active_metric_keys.clone()
    };
    Ok(candidates
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
    kpi_metrics: &[MetricKeySummary],
    explicit: bool,
) -> Vec<String> {
    if requested_metrics.is_empty() && !explicit {
        kpi_metrics
            .iter()
            .map(|metric| metric.key.to_string())
            .collect()
    } else {
        requested_metrics.to_vec()
    }
}

pub(super) fn resolve_selected_metric_keys(
    requested_metrics: &[String],
    visible_metrics: &[MetricKeySummary],
) -> Vec<MetricKeySummary> {
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    let mut quantities = Vec::<MetricQuantity>::new();
    for selector in requested_metrics.iter().map(|selector| selector.trim()) {
        let Some(metric) = visible_metrics
            .iter()
            .find(|metric| metric.key.as_str() == selector)
        else {
            continue;
        };
        if !seen.insert(metric.key.clone()) {
            continue;
        }
        if !quantities.contains(&metric.dimension) {
            if quantities.len() == 2 {
                continue;
            }
            quantities.push(metric.dimension.clone());
        }
        selected.push(metric.clone());
    }
    selected
}

fn unresolved_metric_selectors(requested: &[String], selected: &[MetricKeySummary]) -> Vec<String> {
    requested
        .iter()
        .map(|selector| selector.trim())
        .filter(|selector| !selector.is_empty())
        .filter(|selector| {
            !selected
                .iter()
                .any(|metric| metric.key.as_str() == *selector)
        })
        .map(str::to_owned)
        .collect()
}

fn render_metric_series_section(
    scene: &FrontierChartScene,
    kpi_metrics: &[MetricKeySummary],
    other_metrics: &[MetricKeySummary],
    query: &FrontierPageQuery,
    context: &ProjectRenderContext,
) -> Markup {
    let visible_metrics = visible_metric_catalog(kpi_metrics, other_metrics);
    let requested =
        requested_or_kpi_metric_keys(&query.metric, kpi_metrics, query.metric_selection_explicit);
    let selected = resolve_selected_metric_keys(&requested, &visible_metrics);
    let unresolved = unresolved_metric_selectors(&requested, &selected);
    let mut selection = query.chart_selection();
    selection
        .hidden_metrics
        .retain(|key| selected.iter().any(|metric| metric.key.as_str() == key));
    let plan = ChartPlan::build(scene, &selected, &selection);
    let facets = collect_dimension_facets(scene, &selected);
    let table_series = selected
        .iter()
        .find(|metric| {
            query
                .table_metric
                .as_deref()
                .is_some_and(|key| metric.key.as_str() == key)
        })
        .or_else(|| selected.first())
        .and_then(|metric| scene_series(scene, metric.key.as_str()));
    let active_table_metric = table_series.map(|series| series.metric.key.as_str());
    let fragment_url = format!("{}/chart", frontier_href(&scene.frontier_slug));
    html! {
        section.card
            id="metric-plot-card"
            data-chart-fragment-url=(fragment_url)
            data-chart-scene-token=(&context.refresh_token)
            aria-busy="false"
        {
            div.card-header.plot-card-header {
                h2 { "Plot" }
                a.form-button href=(project_metrics_frontier_href(&scene.frontier_slug)) { "KPIs" }
                div.plot-toolbar {
                    (render_metric_filter_popout(
                        &scene.frontier_slug,
                        &selected,
                        &facets,
                        &selection,
                        active_table_metric,
                    ))
                    (render_metric_selection_popout(
                        &scene.frontier_slug,
                        kpi_metrics,
                        other_metrics,
                        &selected,
                        &selection,
                        &plan,
                        active_table_metric,
                    ))
                }
            }
            @if !unresolved.is_empty() {
                p.chart-warning role="status" {
                    "Ignored unavailable or third-axis metrics: " (unresolved.join(", ")) "."
                }
            }
            @if let Some(warning) = &plan.window_warning {
                p.chart-warning role="status" { (warning) }
            }
            @if visible_metrics.is_empty() {
                p.muted { "No visible metrics registered for this frontier." }
            } @else if selected.is_empty() {
                p.muted { "No metrics selected." }
            } @else {
                (render_direct_series_controls(&selected, &selection))
                @if !plan.has_visible_data() {
                    p.muted { "No plottable non-scuffed points match the current display." }
                } @else {
                    div.chart-frame data-chart-frame="true" {
                        div.chart-action-row {
                            @if selection.window.from.is_some() && selection.window.to.is_some() {
                                button.plot-reset type="button" data-chart-reset-window="true" {
                                    "Reset zoom"
                                }
                            }
                            button.plot-copy-png type="button" data-copy-plot-png="true" aria-live="polite" {
                                "Copy PNG"
                            }
                        }
                        div.chart-rubber-band data-chart-rubber-band="true" hidden {}
                        (PreEscaped(render_chart_svg(&plan, scene)))
                        div.chart-hover-card data-chart-tooltip="true" hidden {}
                    }
                    (render_metric_reference_legend(&plan))
                }
                @if let Some(table_series) = table_series {
                    (render_metric_table(
                        scene,
                        table_series,
                        &selected,
                        &selection,
                        active_table_metric,
                        context.limit,
                    ))
                }
            }
        }
    }
}

fn render_direct_series_controls(
    selected: &[MetricKeySummary],
    selection: &ChartSelection,
) -> Markup {
    html! {
        fieldset.plot-series-controls {
            legend { "Displayed series" }
            div.plot-series-toggle-list {
                @for (index, metric) in selected.iter().enumerate() {
                    label.plot-series-toggle title=(metric_choice_detail(metric)) {
                        input
                            type="checkbox"
                            data-chart-series-toggle="true"
                            value=(metric.key.as_str())
                            checked[!selection.hidden_metrics.contains(metric.key.as_str())];
                        span.plot-series-swatch style=(format!("border-color: {}", series_color(index))) {}
                        span.plot-series-label { (&metric.key) }
                    }
                }
            }
        }
    }
}

fn render_metric_reference_legend(plan: &ChartPlan) -> Markup {
    html! {
        @if plan.series.iter().any(|series| series.kpi.as_ref().is_some_and(|kpi| !kpi.references.is_empty())) {
            div.chart-reference-list aria-label="KPI reference lines" {
                @for series in &plan.series {
                    @for reference in series.kpi.iter().flat_map(|kpi| &kpi.references) {
                        span.chart-reference {
                            span.chart-reference-swatch style=(format!("border-color: {}", series.color)) {}
                            strong { (&series.metric.key) }
                            " · " (&reference.label) " "
                            (plan.format_value(series, reference.canonical_value))
                        }
                    }
                }
            }
        }
    }
}

fn render_metric_filter_popout(
    frontier_slug: &Slug,
    selected: &[MetricKeySummary],
    facets: &[DimensionFacet],
    selection: &ChartSelection,
    table_metric: Option<&str>,
) -> Markup {
    let mut cleared = selection.clone();
    cleared.conditions.clear();
    let clear_href = frontier_tab_href_with_query(
        frontier_slug,
        FrontierTab::Results,
        selected,
        selection.logarithmic,
        &BTreeMap::new(),
        table_metric,
        Some(&cleared),
    );
    let label = if selection.conditions.is_empty() {
        "Filters".to_owned()
    } else {
        format!("Filters {}", selection.conditions.len())
    };
    html! {
        details.control-popout id="metric-filter-popout" data-preserve-open="true" {
            summary.control-popout-toggle { (label) }
            div.control-popout-panel {
                h3 { "Condition Filters" }
                @if facets.is_empty() {
                    p.muted { "No conditions for the current selection." }
                } @else {
                    form.filter-form.auto-submit-form
                        method="get"
                        action=(frontier_href(frontier_slug))
                        data-preserve-viewport="true"
                    {
                        input type="hidden" name="tab" value="results";
                        (render_metric_selection_hidden_inputs(selected))
                        (render_chart_state_hidden_inputs(selection, false, true))
                        (render_table_metric_hidden_input(table_metric))
                        div.filter-form-grid {
                            @for facet in facets {
                                label.filter-control {
                                    span.filter-label { (&facet.key) }
                                    select.filter-select data-auto-submit="true" name=(format!("condition.{}", facet.key)) {
                                        option value="" selected[!selection.conditions.contains_key(&facet.key)] { "all" }
                                        @for value in &facet.values {
                                            option
                                                value=(value)
                                                selected[selection.conditions.get(&facet.key) == Some(value)]
                                            { (value) }
                                        }
                                    }
                                }
                            }
                        }
                        a.clear-filter href=(clear_href) data-preserve-viewport="true" { "Clear all" }
                    }
                }
            }
        }
    }
}

fn render_metric_selection_popout(
    frontier_slug: &Slug,
    kpi_metrics: &[MetricKeySummary],
    other_metrics: &[MetricKeySummary],
    selected: &[MetricKeySummary],
    selection: &ChartSelection,
    plan: &ChartPlan,
    table_metric: Option<&str>,
) -> Markup {
    html! {
        details.control-popout id="metric-selection-popout" data-preserve-open="true" {
            summary.control-popout-toggle { "Metrics " (selected.len()) }
            div.control-popout-panel.metric-popout-panel {
                form.metric-picker-form.auto-submit-form
                    method="get"
                    action=(frontier_href(frontier_slug))
                    data-preserve-viewport="true"
                {
                    input type="hidden" name="tab" value="results";
                    input type="hidden" name="metric_mode" value="explicit";
                    (render_chart_state_hidden_inputs(selection, true, false))
                    (render_table_metric_hidden_input(table_metric))
                    div.metric-popout-layout {
                        div.metric-picker-main {
                            (render_metric_picker_group("KPIs", kpi_metrics, selected))
                            @if !other_metrics.is_empty() {
                                details.metric-picker-disclosure id="metric-other-metrics-disclosure" data-preserve-open="true" {
                                    summary.metric-picker-disclosure-toggle {
                                        "Other Metrics " (other_metrics.len())
                                    }
                                    (render_metric_picker_list(other_metrics, selected))
                                }
                            }
                        }
                        aside.metric-picker-sidecar {
                            h4 { "Scale" }
                            (render_log_control(
                                "Left Log",
                                "log_y_primary",
                                selection.logarithmic.primary,
                                plan.logarithmic_support.primary,
                            ))
                            @if plan.has_secondary_axis() {
                                (render_log_control(
                                    "Right Log",
                                    "log_y_secondary",
                                    selection.logarithmic.secondary,
                                    plan.logarithmic_support.secondary,
                                ))
                            }
                            p.muted.compact-note {
                                "At most two quantities become y axes."
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_metric_picker_group(
    label: &str,
    metrics: &[MetricKeySummary],
    selected: &[MetricKeySummary],
) -> Markup {
    html! {
        @if !metrics.is_empty() {
            section.metric-picker-group {
                h4 { (label) }
                (render_metric_picker_list(metrics, selected))
            }
        }
    }
}

fn render_metric_picker_list(
    metrics: &[MetricKeySummary],
    selected: &[MetricKeySummary],
) -> Markup {
    html! {
        div.metric-picker-list {
            @for metric in metrics {
                @let checked = selected.iter().any(|selected| selected.key == metric.key);
                label class={(if checked { "metric-checkbox-row selected" } else { "metric-checkbox-row" })}
                    title=(metric_choice_detail(metric))
                {
                    input
                        type="checkbox"
                        data-auto-submit="true"
                        name="metric"
                        value=(metric.key.as_str())
                        checked[checked];
                    span.metric-checkbox-copy {
                        (render_metric_kind_chip(metric))
                        span.metric-checkbox-title { (&metric.key) }
                    }
                }
            }
        }
    }
}

fn render_log_control(label: &str, name: &str, checked: bool, supported: bool) -> Markup {
    html! {
        @if checked && !supported {
            input type="hidden" name=(name) value="1";
        }
        label.metric-checkbox-row.metric-checkbox-row-compact {
            input
                type="checkbox"
                data-auto-submit="true"
                name=(name)
                value="1"
                checked[checked]
                disabled[!supported];
            span.metric-checkbox-copy {
                span.metric-checkbox-title { (label) }
            }
        }
    }
}

fn render_metric_table(
    scene: &FrontierChartScene,
    table_series: &FrontierChartSeries,
    selected: &[MetricKeySummary],
    selection: &ChartSelection,
    active_table_metric: Option<&str>,
    limit: Option<u32>,
) -> Markup {
    let (window_first, window_last) = chart_window(scene, selection);
    let mut ordinals = table_series
        .canonical_values
        .iter()
        .enumerate()
        .filter_map(|(ordinal, value)| {
            let experiment = scene.experiments.get(ordinal)?;
            value
                .filter(|value| value.is_finite())
                .filter(|_| ordinal >= window_first)
                .filter(|_| ordinal <= window_last)
                .filter(|_| experiment_matches_conditions(experiment, &selection.conditions))
                .map(|_| ordinal)
        })
        .collect::<Vec<_>>();
    ordinals.reverse();
    let matching_row_count = ordinals.len();
    let ordinals = limit_items(&ordinals, Some(limit.unwrap_or(DEFAULT_METRIC_TABLE_ROWS)));
    html! {
        section.subcard.metric-table-section {
            div.metric-table-header {
                h3 { "Experiments" }
                @if selected.len() > 1 {
                    nav.metric-table-tabs aria-label="Experiment table metric" {
                        @for metric in selected {
                            @let href = frontier_tab_href_with_query(
                                &scene.frontier_slug,
                                FrontierTab::Results,
                                selected,
                                selection.logarithmic,
                                &selection.conditions,
                                Some(metric.key.as_str()),
                                Some(selection),
                            );
                            a
                                href=(href)
                                data-preserve-viewport="true"
                                class={(if active_table_metric == Some(metric.key.as_str()) {
                                    "metric-table-tab active"
                                } else {
                                    "metric-table-tab"
                                })}
                            { (&metric.key) }
                        }
                    }
                }
            }
            p.muted.metric-table-caption {
                (&table_series.metric.key) " · " (ordinals.len()) " of "
                (matching_row_count) " matching rows"
                @if ordinals.len() < matching_row_count {
                    " · zoom or filter to inspect another subset"
                }
            }
            @if ordinals.is_empty() {
                p.muted { "No closed experiments match the current filters for this metric." }
            } @else {
                div.table-scroll {
                    table.metric-table {
                        thead {
                            tr {
                                th.metric-table-fit-heading aria-label="Row actions" { "" }
                                th.metric-table-fit-heading { "#" }
                                th.metric-table-title-heading { "Experiment" }
                                th.metric-table-title-heading { "Hypothesis" }
                                th.metric-table-fit-heading { "Closed" }
                                th.metric-table-fit-heading { "Verdict" }
                                th.metric-table-fit-heading { "Value" }
                            }
                        }
                        tbody {
                            @for ordinal in ordinals {
                                @if let (Some(experiment), Some(Some(canonical))) = (
                                    scene.experiments.get(*ordinal),
                                    table_series.canonical_values.get(*ordinal),
                                ) {
                                    @let return_to = frontier_tab_href_with_query(
                                        &scene.frontier_slug,
                                        FrontierTab::Results,
                                        selected,
                                        selection.logarithmic,
                                        &selection.conditions,
                                        active_table_metric,
                                        Some(selection),
                                    );
                                    tr {
                                        td.metric-table-action-cell {
                                            @if experiment.verdict != FrontierVerdict::Scuffed {
                                                form.inline-action-form
                                                    method="post"
                                                    action=(format!("{}/scuff", experiment_href(&experiment.slug)))
                                                    data-preserve-viewport="true"
                                                {
                                                    input type="hidden" name="rationale" value="Operator marked this experiment scuffed: the setup or recorded value was invalid, so the result is preserved for audit but excluded from plots and KPI rankings.";
                                                    input type="hidden" name="return_to" value=(return_to);
                                                    button.inline-icon-button.danger-icon-button.metric-table-scuff-button
                                                        type="submit"
                                                        title="Mark this experiment scuffed"
                                                        aria-label="Mark experiment scuffed"
                                                    { (scuff_icon()) }
                                                }
                                            }
                                        }
                                        td.metric-table-rank-cell { "#" (ordinal) }
                                        td.metric-table-title-cell {
                                            (render_metric_table_title_link(
                                                &experiment.title,
                                                &experiment_href(&experiment.slug),
                                            ))
                                        }
                                        td.metric-table-title-cell {
                                            (render_metric_table_title_link(
                                                &experiment.hypothesis_title,
                                                &hypothesis_href(&experiment.hypothesis_slug),
                                            ))
                                        }
                                        td.metric-table-closed-cell { (format_timestamp(experiment.closed_at)) }
                                        td.metric-table-verdict-cell {
                                            span class=(status_chip_classes(verdict_class(experiment.verdict))) {
                                                (experiment.verdict.as_str())
                                            }
                                        }
                                        td.metric-table-value-cell {
                                            (format_chart_value(&table_series.metric, *canonical))
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

fn collect_dimension_facets(
    scene: &FrontierChartScene,
    selected: &[MetricKeySummary],
) -> Vec<DimensionFacet> {
    let selected_keys = selected
        .iter()
        .map(|metric| metric.key.as_str())
        .collect::<BTreeSet<_>>();
    let ordinals = scene
        .series
        .iter()
        .filter(|series| selected_keys.contains(series.metric.key.as_str()))
        .flat_map(|series| {
            series
                .canonical_values
                .iter()
                .enumerate()
                .filter_map(|(ordinal, value)| value.is_some().then_some(ordinal))
        })
        .collect::<BTreeSet<_>>();
    let mut facets = BTreeMap::<String, BTreeSet<String>>::new();
    for experiment in ordinals
        .into_iter()
        .filter_map(|ordinal| scene.experiments.get(ordinal))
    {
        for (key, value) in &experiment.dimensions {
            let _ = facets
                .entry(key.to_string())
                .or_default()
                .insert(render_dimension_value(value));
        }
    }
    facets
        .into_iter()
        .map(|(key, values)| DimensionFacet {
            key,
            values: values.into_iter().collect(),
        })
        .collect()
}

fn scene_series<'a>(scene: &'a FrontierChartScene, key: &str) -> Option<&'a FrontierChartSeries> {
    scene
        .series
        .iter()
        .find(|series| series.metric.key.as_str() == key)
}

fn experiment_matches_conditions(
    experiment: &FrontierChartExperiment,
    conditions: &BTreeMap<String, String>,
) -> bool {
    conditions.iter().all(|(key, expected)| {
        experiment.dimensions.iter().any(|(observed, value)| {
            observed.as_str() == key && render_dimension_value(value) == *expected
        })
    })
}

fn chart_window(scene: &FrontierChartScene, selection: &ChartSelection) -> (usize, usize) {
    let full = (0, scene.experiments.len().saturating_sub(1));
    let Some(from) = selection.window.from.as_deref().and_then(|slug| {
        scene
            .experiments
            .iter()
            .position(|experiment| experiment.slug.as_str() == slug)
    }) else {
        return full;
    };
    let Some(to) = selection.window.to.as_deref().and_then(|slug| {
        scene
            .experiments
            .iter()
            .position(|experiment| experiment.slug.as_str() == slug)
    }) else {
        return full;
    };
    (from.min(to), from.max(to))
}

fn render_metric_selection_hidden_inputs(selected: &[MetricKeySummary]) -> Markup {
    html! {
        input type="hidden" name="metric_mode" value="explicit";
        @for metric in selected {
            input type="hidden" name="metric" value=(metric.key.as_str());
        }
    }
}

fn render_chart_state_hidden_inputs(
    selection: &ChartSelection,
    omit_logarithmic: bool,
    omit_conditions: bool,
) -> Markup {
    html! {
        @for metric in &selection.hidden_metrics {
            input type="hidden" name="hidden_metric" value=(metric);
        }
        @if !omit_conditions {
            @for (key, value) in &selection.conditions {
                input type="hidden" name=(format!("condition.{key}")) value=(value);
            }
        }
        @if let Some(from) = &selection.window.from {
            input type="hidden" name="plot_from" value=(from);
        }
        @if let Some(to) = &selection.window.to {
            input type="hidden" name="plot_to" value=(to);
        }
        @if !omit_logarithmic && selection.logarithmic.primary {
            input type="hidden" name="log_y_primary" value="1";
        }
        @if !omit_logarithmic && selection.logarithmic.secondary {
            input type="hidden" name="log_y_secondary" value="1";
        }
    }
}

fn render_table_metric_hidden_input(table_metric: Option<&str>) -> Markup {
    html! {
        @if let Some(metric) = table_metric {
            input type="hidden" name="table_metric" value=(metric);
        }
    }
}

fn render_metric_table_title_link(title: &NonEmptyText, href: &str) -> Markup {
    html! {
        a.metric-table-link href=(href) title=(title.as_str()) { (title) }
    }
}

fn render_history_hypothesis_grid(
    hypotheses: &[fidget_spinner_store_sqlite::HypothesisSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
        section.card {
            h2 { "Closed Hypotheses" }
            @if hypotheses.is_empty() {
                p.muted { "No closed hypotheses." }
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
                            (render_hypothesis_meta_chips(
                                hypothesis.expected_yield,
                                hypothesis.confidence,
                                &hypothesis.tags,
                            ))
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

fn render_frontier_experiment_pane(
    experiments: &[ExperimentSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "Experiments" }
                span.kind-chip { (experiments.len()) " total" }
            }
            @if experiments.is_empty() {
                p.muted { "No experiments." }
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

fn render_hypothesis_current_state_grid(
    hypotheses: &[HypothesisCurrentState],
    limit: Option<u32>,
) -> Markup {
    html! {
        section.card {
            h2 { "Worklist Hypotheses" }
            @if hypotheses.is_empty() {
                p.muted { "No worklist hypotheses." }
            } @else {
                div.card-grid {
                    @for state in limit_items(hypotheses, limit) {
                        article.mini-card {
                            a.title-link href=(hypothesis_href(&state.hypothesis.slug)) {
                                (state.hypothesis.title)
                            }
                            p.prose { (state.hypothesis.summary) }
                            (render_hypothesis_meta_chips(
                                state.hypothesis.expected_yield,
                                state.hypothesis.confidence,
                                &state.hypothesis.tags,
                            ))
                            div.meta-row.muted {
                                span { (state.open_experiments.len()) " open experiments" }
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
