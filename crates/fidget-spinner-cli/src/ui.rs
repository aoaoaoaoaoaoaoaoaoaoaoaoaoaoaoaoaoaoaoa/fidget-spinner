use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use camino::Utf8PathBuf;
use fidget_spinner_core::{
    AttachmentTargetRef, ExperimentAnalysis, ExperimentOutcome, ExperimentStatus, FrontierRecord,
    FrontierVerdict, KnownMetricUnit, MetricUnit, NonEmptyText, RunDimensionValue, Slug, VertexRef,
};
use fidget_spinner_store_sqlite::{
    ExperimentDetail, ExperimentSummary, FrontierMetricSeries, FrontierOpenProjection,
    FrontierSummary, HypothesisCurrentState, HypothesisDetail, ListExperimentsQuery,
    ListHypothesesQuery, MetricKeysQuery, MetricScope, ProjectStatus, StoreError, VertexSummary,
};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use plotters::prelude::{
    BLACK, ChartBuilder, Circle, IntoDrawingArea, LineSeries, PathElement, SVGBackend, ShapeStyle,
    Text,
};
use plotters::style::{Color, IntoFont, RGBColor};
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::open_store;

#[derive(Clone)]
struct NavigatorState {
    project_root: Utf8PathBuf,
    limit: Option<u32>,
}

#[derive(Clone)]
struct ShellFrame {
    active_frontier_slug: Option<Slug>,
    frontiers: Vec<FrontierSummary>,
    project_status: ProjectStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrontierTab {
    Brief,
    Open,
    Closed,
    Metrics,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FrontierPageQuery {
    metric: Option<String>,
    tab: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DimensionFacet {
    key: String,
    values: Vec<String>,
}

struct AttachmentDisplay {
    kind: &'static str,
    href: String,
    title: String,
    summary: Option<String>,
}

impl FrontierTab {
    fn from_query(raw: Option<&str>) -> Self {
        match raw {
            Some("open") => Self::Open,
            Some("closed") => Self::Closed,
            Some("metrics") => Self::Metrics,
            _ => Self::Brief,
        }
    }

    const fn as_query(self) -> &'static str {
        match self {
            Self::Brief => "brief",
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Metrics => "metrics",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Brief => "Brief",
            Self::Open => "Open",
            Self::Closed => "Closed",
            Self::Metrics => "Metrics",
        }
    }
}

impl FrontierPageQuery {
    fn dimension_filters(&self) -> BTreeMap<String, String> {
        self.extra
            .iter()
            .filter_map(|(key, value)| {
                let value = value.trim();
                (!value.is_empty())
                    .then(|| {
                        key.strip_prefix("dim.")
                            .map(|dimension| (dimension.to_owned(), value.to_owned()))
                    })
                    .flatten()
            })
            .collect()
    }
}

pub(crate) fn serve(
    project_root: Utf8PathBuf,
    bind: SocketAddr,
    limit: Option<u32>,
) -> Result<(), StoreError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .map_err(StoreError::from)?;
    runtime.block_on(async move {
        let state = NavigatorState {
            project_root,
            limit,
        };
        let app = Router::new()
            .route("/", get(project_home))
            .route("/frontier/{selector}", get(frontier_detail))
            .route("/hypothesis/{selector}", get(hypothesis_detail))
            .route("/experiment/{selector}", get(experiment_detail))
            .route("/artifact/{selector}", get(artifact_detail))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .map_err(StoreError::from)?;
        println!("navigator: http://{bind}/");
        axum::serve(listener, app)
            .await
            .map_err(|error| StoreError::Io(io::Error::other(error.to_string())))
    })
}

async fn project_home(State(state): State<NavigatorState>) -> Response {
    render_response(render_project_home(state))
}

async fn frontier_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
    Query(query): Query<FrontierPageQuery>,
) -> Response {
    render_response(render_frontier_detail(state, selector, query))
}

async fn hypothesis_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_hypothesis_detail(state, selector))
}

async fn experiment_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_experiment_detail(state, selector))
}

async fn artifact_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_artifact_detail(state, selector))
}

fn render_response(result: Result<Markup, StoreError>) -> Response {
    match result {
        Ok(markup) => Html(markup.into_string()).into_response(),
        Err(StoreError::UnknownFrontierSelector(_))
        | Err(StoreError::UnknownHypothesisSelector(_))
        | Err(StoreError::UnknownExperimentSelector(_))
        | Err(StoreError::UnknownArtifactSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("navigator render failed: {error}"),
        )
            .into_response(),
    }
}

fn render_project_home(state: NavigatorState) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None)?;
    let title = format!("{} navigator", shell.project_status.display_name);
    let content = html! {
        (render_project_status(&shell.project_status))
        (render_frontier_grid(&shell.frontiers, state.limit))
    };
    Ok(render_shell(
        &title,
        &shell,
        true,
        Some(&shell.project_status.display_name.to_string()),
        None,
        None,
        content,
    ))
}

fn render_frontier_detail(
    state: NavigatorState,
    selector: String,
    query: FrontierPageQuery,
) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let projection = store.frontier_open(&selector)?;
    let shell = load_shell_frame(&store, Some(projection.frontier.slug.clone()))?;
    let tab = FrontierTab::from_query(query.tab.as_deref());
    let title = format!("{} · frontier", projection.frontier.label);
    let subtitle = format!(
        "{} hypotheses active · {} experiments open",
        projection.active_hypotheses.len(),
        projection.open_experiments.len()
    );
    let content = render_frontier_tab_content(&store, &projection, tab, &query, state.limit)?;
    Ok(render_shell(
        &title,
        &shell,
        false,
        Some(&subtitle),
        None,
        Some(render_frontier_tab_bar(
            &projection.frontier.slug,
            tab,
            query.metric.as_deref(),
            &query.dimension_filters(),
        )),
        content,
    ))
}

fn render_hypothesis_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let detail = store.read_hypothesis(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let shell = load_shell_frame(&store, Some(frontier.slug.clone()))?;
    let title = format!("{} · hypothesis", detail.record.title);
    let subtitle = detail.record.summary.to_string();
    let content = html! {
        (render_hypothesis_header(&detail, &frontier))
        (render_prose_block("Body", detail.record.body.as_str()))
        (render_vertex_relation_sections(&detail.parents, &detail.children, state.limit))
        (render_artifact_section(&detail.artifacts, state.limit))
        (render_experiment_section(
            "Open Experiments",
            &detail.open_experiments,
            state.limit,
        ))
        (render_experiment_section(
            "Closed Experiments",
            &detail.closed_experiments,
            state.limit,
        ))
    };
    Ok(render_shell(
        &title,
        &shell,
        true,
        Some(&subtitle),
        Some((frontier.label.as_str(), frontier_href(&frontier.slug))),
        None,
        content,
    ))
}

fn render_experiment_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let detail = store.read_experiment(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let shell = load_shell_frame(&store, Some(frontier.slug.clone()))?;
    let title = format!("{} · experiment", detail.record.title);
    let subtitle = detail.record.summary.as_ref().map_or_else(
        || detail.record.status.as_str().to_owned(),
        ToString::to_string,
    );
    let content = html! {
        (render_experiment_header(&detail, &frontier))
        (render_vertex_relation_sections(&detail.parents, &detail.children, state.limit))
        (render_artifact_section(&detail.artifacts, state.limit))
        @if let Some(outcome) = detail.record.outcome.as_ref() {
            (render_experiment_outcome(outcome))
        } @else {
            section.card {
                h2 { "Outcome" }
                p.muted { "Open experiment. No outcome recorded yet." }
            }
        }
    };
    Ok(render_shell(
        &title,
        &shell,
        true,
        Some(&subtitle),
        Some((frontier.label.as_str(), frontier_href(&frontier.slug))),
        None,
        content,
    ))
}

fn render_artifact_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let detail = store.read_artifact(&selector)?;
    let shell = load_shell_frame(&store, None)?;
    let attachments = detail
        .attachments
        .iter()
        .map(|target| resolve_attachment_display(&store, *target))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let title = format!("{} · artifact", detail.record.label);
    let subtitle = detail.record.summary.as_ref().map_or_else(
        || detail.record.kind.as_str().to_owned(),
        ToString::to_string,
    );
    let content = html! {
        section.card {
            h2 { "Artifact" }
            div.kv-grid {
                (render_kv("Kind", detail.record.kind.as_str()))
                (render_kv("Slug", detail.record.slug.as_str()))
                (render_kv("Locator", detail.record.locator.as_str()))
                @if let Some(media_type) = detail.record.media_type.as_ref() {
                    (render_kv("Media type", media_type.as_str()))
                }
                (render_kv("Updated", &format_timestamp(detail.record.updated_at)))
            }
            @if let Some(summary) = detail.record.summary.as_ref() {
                p.prose { (summary) }
            }
            p.muted {
                "Artifact bodies are intentionally out of band. Spinner only preserves references."
            }
        }
        section.card {
            h2 { "Attachments" }
            @if attachments.is_empty() {
                p.muted { "No attachments." }
            } @else {
                div.link-list {
                    @for attachment in &attachments {
                        (render_attachment_chip(attachment))
                    }
                }
            }
        }
    };
    Ok(render_shell(
        &title,
        &shell,
        true,
        Some(&subtitle),
        None,
        None,
        content,
    ))
}

fn load_shell_frame(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    active_frontier_slug: Option<Slug>,
) -> Result<ShellFrame, StoreError> {
    Ok(ShellFrame {
        active_frontier_slug,
        frontiers: store.list_frontiers()?,
        project_status: store.status()?,
    })
}

fn render_frontier_tab_content(
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
        FrontierTab::Metrics => {
            let other_metric_keys = if projection.active_metric_keys.is_empty() {
                store.metric_keys(MetricKeysQuery {
                    frontier: Some(projection.frontier.slug.to_string()),
                    scope: MetricScope::Visible,
                })?
            } else {
                projection
                    .active_metric_keys
                    .iter()
                    .filter(|metric| {
                        !projection
                            .scoreboard_metric_keys
                            .iter()
                            .any(|scoreboard| scoreboard.key == metric.key)
                    })
                    .cloned()
                    .collect()
            };
            let selected_metric = query
                .metric
                .as_deref()
                .and_then(|selector| NonEmptyText::new(selector.to_owned()).ok())
                .or_else(|| {
                    projection
                        .scoreboard_metric_keys
                        .first()
                        .or_else(|| other_metric_keys.first())
                        .map(|metric| metric.key.clone())
                });
            let series = selected_metric
                .as_ref()
                .map(|metric| {
                    store.frontier_metric_series(projection.frontier.slug.as_str(), metric, true)
                })
                .transpose()?;
            let dimension_filters = query.dimension_filters();
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_metric_series_section(
                    &projection.frontier.slug,
                    &projection.scoreboard_metric_keys,
                    &other_metric_keys,
                    selected_metric.as_ref(),
                    series.as_ref(),
                    &dimension_filters,
                    limit,
                ))
            })
        }
    }
}

fn render_frontier_tab_bar(
    frontier_slug: &Slug,
    active_tab: FrontierTab,
    metric: Option<&str>,
    dimension_filters: &BTreeMap<String, String>,
) -> Markup {
    const TABS: [FrontierTab; 4] = [
        FrontierTab::Brief,
        FrontierTab::Open,
        FrontierTab::Closed,
        FrontierTab::Metrics,
    ];
    html! {
        nav.tab-row aria-label="Frontier tabs" {
            @for tab in TABS {
                @let href = frontier_tab_href_with_filters(frontier_slug, tab, metric, dimension_filters);
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

fn render_metric_series_section(
    frontier_slug: &Slug,
    scoreboard_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    other_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    selected_metric: Option<&NonEmptyText>,
    series: Option<&FrontierMetricSeries>,
    dimension_filters: &BTreeMap<String, String>,
    limit: Option<u32>,
) -> Markup {
    let facets = series
        .map(|series| collect_dimension_facets(&series.points))
        .unwrap_or_default();
    let filtered_points = series
        .map(|series| filter_metric_points(&series.points, dimension_filters))
        .unwrap_or_default();
    html! {
    section.card {
        h2 { "Metrics" }
        p.prose {
            "Server-rendered SVG over the frontier’s closed experiment ledger. Choose a live metric, then walk to the underlying experiments deliberately."
        }
        @if scoreboard_metric_keys.is_empty() && other_metric_keys.is_empty() {
            p.muted { "No visible metrics registered for this frontier." }
        } @else {
            @if !scoreboard_metric_keys.is_empty() {
                div.metric-picker-group {
                    h3 { "Scoreboard" }
                    div.metric-picker {
                        @for metric in scoreboard_metric_keys {
                            @let href = frontier_tab_href(frontier_slug, FrontierTab::Metrics, Some(metric.key.as_str()));
                            a
                                href=(href)
                                class={(if selected_metric.is_some_and(|selected| selected == &metric.key) {
                                    "metric-choice active"
                                } else {
                                    "metric-choice"
                                })}
                            {
                                span.metric-choice-key { (metric.key) }
                                span.metric-choice-meta {
                                    (metric.objective.as_str()) " · "
                                    (metric.unit.as_str())
                                }
                            }
                        }
                    }
                }
            }
            @if !other_metric_keys.is_empty() {
                div.metric-picker-group {
                    h3 { "Other Live Metrics" }
                    div.metric-picker {
                        @for metric in other_metric_keys {
                            @let href = frontier_tab_href(frontier_slug, FrontierTab::Metrics, Some(metric.key.as_str()));
                            a
                                href=(href)
                                class={(if selected_metric.is_some_and(|selected| selected == &metric.key) {
                                    "metric-choice active"
                                } else {
                                    "metric-choice"
                                })}
                            {
                                span.metric-choice-key { (metric.key) }
                                span.metric-choice-meta {
                                    (metric.objective.as_str()) " · "
                                    (metric.unit.as_str())
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    @if let Some(series) = series {
        section.card {
            div.card-header {
                h2 { "Plot" }
                span.metric-pill {
                    (series.metric.key) " · "
                    (series.metric.objective.as_str()) " · "
                    (series.metric.unit.as_str())
                }
            }
            @if let Some(description) = series.metric.description.as_ref() {
                p.muted { (description) }
            }
            @if !facets.is_empty() {
                (render_metric_filter_panel(
                    frontier_slug,
                    &series.metric.key,
                    &facets,
                    dimension_filters,
                ))
            }
            @if filtered_points.is_empty() {
                p.muted { "No closed experiments match the current filters." }
            } @else if series.points.is_empty() {
                p.muted { "No closed experiments for this metric yet." }
            } @else {
                div.chart-frame {
                    (PreEscaped(render_metric_chart_svg(&series.metric, &filtered_points)))
                }
                p.muted {
                    "x = close order, y = metric value. Point color tracks verdict."
                }
                div.table-scroll {
                    table.metric-table {
                        thead {
                            tr {
                                th { "#" }
                                th { "Experiment" }
                                th { "Hypothesis" }
                                th { "Closed" }
                                th { "Verdict" }
                                th { "Value" }
                            }
                        }
                        tbody {
                            @for (index, point) in limit_items(&filtered_points, limit).iter().copied().enumerate() {
                                tr {
                                    td { ((index + 1).to_string()) }
                                    td {
                                        a href=(experiment_href(&point.experiment.slug)) {
                                            (point.experiment.title)
                                        }
                                    }
                                    td {
                                        a href=(hypothesis_href(&point.hypothesis.slug)) {
                                            (point.hypothesis.title)
                                        }
                                    }
                                    td.nowrap { (format_timestamp(point.closed_at)) }
                                    td {
                                        span class=(status_chip_classes(verdict_class(point.verdict))) {
                                            (point.verdict.as_str())
                                        }
                                    }
                                    td.nowrap { (format_metric_value(point.value, &series.metric.unit)) }
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

fn render_metric_filter_panel(
    frontier_slug: &Slug,
    metric_key: &NonEmptyText,
    facets: &[DimensionFacet],
    active_filters: &BTreeMap<String, String>,
) -> Markup {
    let clear_href = frontier_tab_href_with_filters(
        frontier_slug,
        FrontierTab::Metrics,
        Some(metric_key.as_str()),
        &BTreeMap::new(),
    );
    html! {
    section.subcard {
        h3 id="slice-filters" { "Slice Filters" }
        form.filter-form method="get" action=(frontier_href(frontier_slug)) {
            input type="hidden" name="tab" value="metrics";
            input type="hidden" name="metric" value=(metric_key.as_str());
            div.filter-form-grid {
                @for facet in facets {
                    label.filter-control id=(metric_filter_anchor_id(&facet.key)) {
                        span.filter-label { (&facet.key) }
                        select.filter-select name=(format!("dim.{}", facet.key)) {
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
                button.filter-apply type="submit" { "Apply" }
                a.clear-filter href=(clear_href) { "Clear all" }
            }
        }
        @if active_filters.is_empty() {
            p.muted { "No slice filters active." }
        } @else {
            div.chip-row {
                @for (key, value) in active_filters {
                    @let href = frontier_tab_href_with_filters(
                        frontier_slug,
                        FrontierTab::Metrics,
                        Some(metric_key.as_str()),
                        &remove_dimension_filter(active_filters, key),
                    );
                    a.metric-filter-chip.active href=(href) {
                        (key) "=" (value) " ×"
                    }
                }
            }
        }
    }
    }
}

fn render_metric_chart_svg(
    metric: &fidget_spinner_store_sqlite::MetricKeySummary,
    points: &[&fidget_spinner_store_sqlite::FrontierMetricPoint],
) -> String {
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (960, 360)).into_drawing_area();
        if root.fill(&RGBColor(255, 250, 242)).is_err() {
            return chart_error_markup("chart fill failed");
        }
        let values = points.iter().map(|point| point.value).collect::<Vec<_>>();
        let (mut min_value, mut max_value) = values
            .iter()
            .copied()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), value| {
                (min.min(value), max.max(value))
            });
        if !min_value.is_finite() || !max_value.is_finite() {
            return chart_error_markup("metric values are non-finite");
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
        let x_end = i32::try_from(points.len().saturating_sub(1))
            .unwrap_or(0)
            .max(1);
        let mut chart = match ChartBuilder::on(&root)
            .margin(18)
            .x_label_area_size(32)
            .y_label_area_size(72)
            .caption(
                format!("{} over closed experiments", metric.key),
                ("Iosevka Web", 18).into_font().color(&BLACK),
            )
            .build_cartesian_2d(0_i32..x_end, min_value..max_value)
        {
            Ok(chart) => chart,
            Err(error) => return chart_error_markup(&format!("chart build failed: {error:?}")),
        };
        if chart
            .configure_mesh()
            .light_line_style(RGBColor(223, 209, 189).mix(0.6))
            .bold_line_style(RGBColor(207, 190, 168).mix(0.8))
            .axis_style(RGBColor(103, 86, 63))
            .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
            .x_desc("close order")
            .y_desc(metric.unit.as_str())
            .x_label_formatter(&|value| format!("{}", value + 1))
            .draw()
            .is_err()
        {
            return chart_error_markup("mesh draw failed");
        }

        let line_points = points
            .iter()
            .enumerate()
            .filter_map(|(index, point)| i32::try_from(index).ok().map(|x| (x, point.value)))
            .collect::<Vec<_>>();
        if chart
            .draw_series(LineSeries::new(line_points, &RGBColor(103, 86, 63)))
            .map(|series| {
                series.label("series").legend(|(x, y)| {
                    PathElement::new(vec![(x, y), (x + 18, y)], RGBColor(103, 86, 63))
                })
            })
            .is_err()
        {
            return chart_error_markup("line draw failed");
        }

        let plotted_points = points
            .iter()
            .enumerate()
            .filter_map(|(index, point)| i32::try_from(index).ok().map(|x| (x, *point)))
            .collect::<Vec<_>>();
        if chart
            .draw_series(plotted_points.iter().map(|(x, point)| {
                Circle::new(
                    (*x, point.value),
                    4,
                    ShapeStyle::from(&verdict_color(point.verdict)).filled(),
                )
            }))
            .is_err()
        {
            return chart_error_markup("point draw failed");
        }
        if chart
            .draw_series(plotted_points.iter().map(|(x, point)| {
                Text::new(
                    format!("{}", x + 1),
                    (*x, point.value),
                    ("Iosevka Web", 11)
                        .into_font()
                        .color(&verdict_color(point.verdict)),
                )
            }))
            .is_err()
        {
            return chart_error_markup("label draw failed");
        }
        if root.present().is_err() {
            return chart_error_markup("chart present failed");
        }
    }
    svg
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

fn verdict_color(verdict: FrontierVerdict) -> RGBColor {
    match verdict {
        FrontierVerdict::Accepted => RGBColor(71, 102, 63),
        FrontierVerdict::Kept => RGBColor(90, 105, 82),
        FrontierVerdict::Parked => RGBColor(138, 98, 48),
        FrontierVerdict::Rejected => RGBColor(138, 58, 52),
    }
}

fn render_frontier_grid(frontiers: &[FrontierSummary], limit: Option<u32>) -> Markup {
    html! {
    section.card {
        h2 { "Frontiers" }
        @if frontiers.is_empty() {
            p.muted { "No frontiers yet." }
        } @else {
            div.card-grid {
                @for frontier in limit_items(frontiers, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(frontier_href(&frontier.slug)) { (frontier.label) }
                            span class=(status_chip_classes(frontier_status_class(frontier.status.as_str()))) {
                                (frontier.status.as_str())
                            }
                        }
                        p.prose { (frontier.objective) }
                        div.meta-row {
                            span { (format!("{} active hypotheses", frontier.active_hypothesis_count)) }
                            span { (format!("{} open experiments", frontier.open_experiment_count)) }
                        }
                        div.meta-row.muted {
                            span { "updated " (format_timestamp(frontier.updated_at)) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_project_status(status: &ProjectStatus) -> Markup {
    html! {
    section.card {
        h1 { (status.display_name) }
        p.prose {
            "Austere experimental ledger. Frontier overview is the only sanctioned dump; everything else is deliberate traversal."
        }
        div.kv-grid {
            (render_kv("Project root", status.project_root.as_str()))
            (render_kv("Store format", &status.store_format_version.to_string()))
            (render_kv("Frontiers", &status.frontier_count.to_string()))
            (render_kv("Hypotheses", &status.hypothesis_count.to_string()))
            (render_kv("Experiments", &status.experiment_count.to_string()))
            (render_kv("Open experiments", &status.open_experiment_count.to_string()))
            (render_kv("Artifacts", &status.artifact_count.to_string()))
        }
    }
    }
}

fn render_frontier_header(frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (frontier.label) }
        p.prose { (frontier.objective) }
        div.meta-row {
            span { "slug " code { (frontier.slug) } }
            span class=(status_chip_classes(frontier_status_class(frontier.status.as_str()))) {
                (frontier.status.as_str())
            }
            span.muted { "updated " (format_timestamp(frontier.updated_at)) }
        }
    }
    }
}

fn render_frontier_brief(projection: &FrontierOpenProjection) -> Markup {
    let frontier = &projection.frontier;
    html! {
    section.card {
        h2 { "Frontier Brief" }
        @if let Some(situation) = frontier.brief.situation.as_ref() {
            div.block {
                h3 { "Situation" }
                p.prose { (situation) }
            }
        } @else {
            p.muted { "No situation summary recorded." }
        }
        div.split {
            div.subcard {
                h3 { "Roadmap" }
                @if frontier.brief.roadmap.is_empty() {
                    p.muted { "No roadmap ordering recorded." }
                } @else {
                    ol.roadmap-list {
                                @for item in &frontier.brief.roadmap {
                                    @let title = hypothesis_title_for_roadmap_item(projection, item.hypothesis_id);
                                    li {
                                        a href=(hypothesis_href_from_id(item.hypothesis_id)) {
                                            (title)
                                        }
                                        @if let Some(summary) = item.summary.as_ref() {
                                            span.muted { " · " (summary) }
                                        }
                            }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "Unknowns" }
                @if frontier.brief.unknowns.is_empty() {
                    p.muted { "No explicit unknowns." }
                } @else {
                    ul.simple-list {
                        @for unknown in &frontier.brief.unknowns {
                            li { (unknown) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_frontier_active_sets(projection: &FrontierOpenProjection) -> Markup {
    html! {
    section.card {
        h2 { "Active Surface" }
        div.stack {
            div.subcard.compact-subcard {
                h3 { "Active Tags" }
                @if projection.active_tags.is_empty() {
                    p.muted { "No active tags." }
                } @else {
                    div.chip-row.tag-cloud {
                        @for tag in &projection.active_tags {
                            span.tag-chip { (tag) }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "Scoreboard Metrics" }
                @if projection.scoreboard_metric_keys.is_empty() {
                    p.muted { "No frontier scoreboard metrics configured." }
                } @else {
                    div.table-scroll {
                        table.metric-table {
                            thead {
                                tr {
                                    th { "Key" }
                                    th { "Unit" }
                                    th { "Objective" }
                                    th { "Refs" }
                                }
                            }
                            tbody {
                                @for metric in &projection.scoreboard_metric_keys {
                                    tr {
                                        td {
                                            a href=(frontier_tab_href(
                                                &projection.frontier.slug,
                                                FrontierTab::Metrics,
                                                Some(metric.key.as_str()),
                                            )) {
                                                (metric.key)
                                            }
                                        }
                                        td { (metric.unit.as_str()) }
                                        td { (metric.objective.as_str()) }
                                        td { (metric.reference_count) }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "Live Metrics" }
                @if projection.active_metric_keys.is_empty() {
                    p.muted { "No live metrics." }
                } @else {
                    div.table-scroll {
                        table.metric-table {
                            thead {
                                tr {
                                    th { "Key" }
                                    th { "Unit" }
                                    th { "Objective" }
                                    th { "Refs" }
                                }
                            }
                            tbody {
                                @for metric in &projection.active_metric_keys {
                                    tr {
                                        td {
                                            a href=(frontier_tab_href(
                                                &projection.frontier.slug,
                                                FrontierTab::Metrics,
                                                Some(metric.key.as_str()),
                                            )) {
                                                (metric.key)
                                            }
                                        }
                                        td { (metric.unit.as_str()) }
                                        td { (metric.objective.as_str()) }
                                        td { (metric.reference_count) }
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

fn render_hypothesis_header(detail: &HypothesisDetail, frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (detail.record.title) }
        p.prose { (detail.record.summary) }
        div.meta-row {
            span { "frontier " a href=(frontier_href(&frontier.slug)) { (frontier.label) } }
            span { "slug " code { (detail.record.slug) } }
            @if detail.record.archived {
                span class="status-chip status-archived" { "archived" }
            }
            span.muted { "updated " (format_timestamp(detail.record.updated_at)) }
        }
        @if !detail.record.tags.is_empty() {
            div.chip-row {
                @for tag in &detail.record.tags {
                    span.tag-chip { (tag) }
                }
            }
        }
    }
    }
}

fn render_experiment_header(detail: &ExperimentDetail, frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (detail.record.title) }
        @if let Some(summary) = detail.record.summary.as_ref() {
            p.prose { (summary) }
        }
        div.meta-row {
            span {
                "frontier "
                a href=(frontier_href(&frontier.slug)) { (frontier.label) }
            }
            span {
                "hypothesis "
                a href=(hypothesis_href(&detail.owning_hypothesis.slug)) {
                    (detail.owning_hypothesis.title)
                }
            }
            span class=(status_chip_classes(experiment_status_class(detail.record.status))) {
                (detail.record.status.as_str())
            }
            @if let Some(verdict) = detail
                .record
                .outcome
                .as_ref()
                .map(|outcome| outcome.verdict)
            {
                span class=(status_chip_classes(verdict_class(verdict))) { (verdict.as_str()) }
            }
            span.muted { "updated " (format_timestamp(detail.record.updated_at)) }
        }
        @if !detail.record.tags.is_empty() {
            div.chip-row {
                @for tag in &detail.record.tags {
                    span.tag-chip { (tag) }
                }
            }
        }
    }
    }
}

fn render_experiment_outcome(outcome: &ExperimentOutcome) -> Markup {
    html! {
    section.card {
        h2 { "Outcome" }
        div.kv-grid {
            (render_kv("Verdict", outcome.verdict.as_str()))
            (render_kv("Backend", outcome.backend.as_str()))
            (render_kv("Closed", &format_timestamp(outcome.closed_at)))
        }
        (render_command_recipe(&outcome.command))
        (render_metric_panel("Primary metric", std::slice::from_ref(&outcome.primary_metric), outcome))
        @if !outcome.supporting_metrics.is_empty() {
            (render_metric_panel("Supporting metrics", &outcome.supporting_metrics, outcome))
        }
        @if !outcome.dimensions.is_empty() {
            section.subcard {
                h3 { "Dimensions" }
                div.table-scroll {
                    table.metric-table {
                        thead { tr { th { "Key" } th { "Value" } } }
                        tbody {
                            @for (key, value) in &outcome.dimensions {
                                tr {
                                    td { (key) }
                                    td { (render_dimension_value(value)) }
                                }
                            }
                        }
                    }
                }
            }
        }
        section.subcard {
            h3 { "Rationale" }
            p.prose { (outcome.rationale) }
        }
        @if let Some(analysis) = outcome.analysis.as_ref() {
            (render_experiment_analysis(analysis))
        }
    }
    }
}

fn render_experiment_analysis(analysis: &ExperimentAnalysis) -> Markup {
    html! {
    section.subcard {
        h3 { "Analysis" }
        p.prose { (analysis.summary) }
        div.code-block {
            (analysis.body)
        }
    }
    }
}

fn render_command_recipe(command: &fidget_spinner_core::CommandRecipe) -> Markup {
    html! {
    section.subcard {
        h3 { "Command" }
        div.kv-grid {
            (render_kv(
                "argv",
                &command
                    .argv
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" "),
            ))
            @if let Some(working_directory) = command.working_directory.as_ref() {
                (render_kv("cwd", working_directory.as_str()))
            }
        }
        @if !command.env.is_empty() {
            div.table-scroll {
                table.metric-table {
                    thead { tr { th { "Env" } th { "Value" } } }
                    tbody {
                        @for (key, value) in &command.env {
                            tr {
                                td { (key) }
                                td { (value) }
                            }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_metric_panel(
    title: &str,
    metrics: &[fidget_spinner_core::MetricValue],
    outcome: &ExperimentOutcome,
) -> Markup {
    html! {
    section.subcard {
        h3 { (title) }
        div.table-scroll {
            table.metric-table {
                thead {
                    tr {
                        th { "Key" }
                        th { "Value" }
                    }
                }
                tbody {
                    @for metric in metrics {
                        tr {
                            td { (metric.key) }
                            td { (format_metric_value(metric.value, &metric_unit_for(metric, outcome))) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn metric_unit_for(
    metric: &fidget_spinner_core::MetricValue,
    outcome: &ExperimentOutcome,
) -> MetricUnit {
    if metric.key == outcome.primary_metric.key {
        return MetricUnit::scalar();
    }
    MetricUnit::scalar()
}

fn render_vertex_relation_sections(
    parents: &[VertexSummary],
    children: &[VertexSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
        section.card {
            h2 { "Influence Network" }
            div.split {
                div.subcard {
                    h3 { "Parents" }
                    @if parents.is_empty() {
                        p.muted { "No parent influences." }
                    } @else {
                        div.link-list {
                            @for parent in limit_items(parents, limit) {
                                (render_vertex_chip(parent))
                            }
                        }
                    }
                }
                div.subcard {
                    h3 { "Children" }
                    @if children.is_empty() {
                        p.muted { "No downstream influences." }
                    } @else {
                        div.link-list {
                            @for child in limit_items(children, limit) {
                                (render_vertex_chip(child))
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_artifact_section(
    artifacts: &[fidget_spinner_store_sqlite::ArtifactSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { "Artifacts" }
        @if artifacts.is_empty() {
            p.muted { "No attached artifacts." }
        } @else {
            div.card-grid {
                @for artifact in limit_items(artifacts, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(artifact_href(&artifact.slug)) { (artifact.label) }
                            span class="status-chip classless" { (artifact.kind.as_str()) }
                        }
                        @if let Some(summary) = artifact.summary.as_ref() {
                            p.prose { (summary) }
                        }
                        div.meta-row {
                            span.muted { (artifact.locator) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_experiment_section(
    title: &str,
    experiments: &[ExperimentSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { (title) }
        @if experiments.is_empty() {
            p.muted { "None." }
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

fn render_experiment_card(experiment: &ExperimentSummary) -> Markup {
    html! {
    article.mini-card {
        div.card-header {
            a.title-link href=(experiment_href(&experiment.slug)) { (experiment.title) }
            span class=(status_chip_classes(experiment_status_class(experiment.status))) {
                (experiment.status.as_str())
            }
            @if let Some(verdict) = experiment.verdict {
                span class=(status_chip_classes(verdict_class(verdict))) { (verdict.as_str()) }
            }
        }
        @if let Some(summary) = experiment.summary.as_ref() {
            p.prose { (summary) }
        }
        @if let Some(metric) = experiment.primary_metric.as_ref() {
            div.meta-row {
                span.metric-pill {
                    (metric.key) ": "
                    (format_metric_value(metric.value, &metric.unit))
                }
            }
        }
        @if !experiment.tags.is_empty() {
            div.chip-row {
                @for tag in &experiment.tags {
                    span.tag-chip { (tag) }
                }
            }
        }
        div.meta-row.muted {
            span { "updated " (format_timestamp(experiment.updated_at)) }
        }
    }
    }
}

fn render_experiment_summary_line(experiment: &ExperimentSummary) -> Markup {
    html! {
    div.link-list {
        (render_experiment_link_chip(experiment))
        @if let Some(metric) = experiment.primary_metric.as_ref() {
            span.metric-pill {
                (metric.key) ": "
                (format_metric_value(metric.value, &metric.unit))
            }
        }
    }
    }
}

fn render_experiment_link_chip(experiment: &ExperimentSummary) -> Markup {
    html! {
        a.link-chip href=(experiment_href(&experiment.slug)) {
            span.link-chip-main {
                span.link-chip-title { (experiment.title) }
                @if let Some(verdict) = experiment.verdict {
                    span class=(status_chip_classes(verdict_class(verdict))) { (verdict.as_str()) }
                }
            }
            @if experiment.verdict.is_none() && experiment.status == ExperimentStatus::Open {
                span.link-chip-summary { "open experiment" }
            }
        }
    }
}

fn render_vertex_chip(summary: &VertexSummary) -> Markup {
    let href = match summary.vertex {
        VertexRef::Hypothesis(_) => hypothesis_href(&summary.slug),
        VertexRef::Experiment(_) => experiment_href(&summary.slug),
    };
    let kind = match summary.vertex {
        VertexRef::Hypothesis(_) => "hypothesis",
        VertexRef::Experiment(_) => "experiment",
    };
    html! {
        a.link-chip href=(href) {
            span.link-chip-main {
                span.kind-chip { (kind) }
                span.link-chip-title { (summary.title) }
            }
            @if let Some(summary_text) = summary.summary.as_ref() {
                span.link-chip-summary { (summary_text) }
            }
        }
    }
}

fn render_attachment_chip(attachment: &AttachmentDisplay) -> Markup {
    html! {
        a.link-chip href=(&attachment.href) {
            span.link-chip-main {
                span.kind-chip { (attachment.kind) }
                span.link-chip-title { (&attachment.title) }
            }
            @if let Some(summary) = attachment.summary.as_ref() {
                span.link-chip-summary { (summary) }
            }
        }
    }
}

fn render_prose_block(title: &str, body: &str) -> Markup {
    html! {
    section.card {
        h2 { (title) }
        p.prose { (body) }
    }
    }
}

fn render_shell(
    title: &str,
    shell: &ShellFrame,
    show_page_header: bool,
    subtitle: Option<&str>,
    breadcrumb: Option<(&str, String)>,
    tab_bar: Option<Markup>,
    content: Markup,
) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                style { (PreEscaped(styles())) }
            }
            body {
                main.shell {
                    aside.sidebar {
                        (render_sidebar(shell))
                    }
                    div.main-column {
                        @if show_page_header {
                            header.page-header {
                                div.eyebrow {
                                    a href="/" { "home" }
                                    @if let Some((label, href)) = breadcrumb {
                                        span.sep { "/" }
                                        a href=(href) { (label) }
                                    }
                                }
                                h1.page-title { (title) }
                                @if let Some(subtitle) = subtitle {
                                    p.page-subtitle { (subtitle) }
                                }
                            }
                        }
                        @if let Some(tab_bar) = tab_bar {
                            (tab_bar)
                        }
                        (content)
                    }
                }
            }
        }
    }
}

fn render_sidebar(shell: &ShellFrame) -> Markup {
    html! {
    section.sidebar-panel {
        div.sidebar-project {
            a.sidebar-home href="/" { (&shell.project_status.display_name) }
            p.sidebar-copy {
                "Frontier-scoped navigator. Open one frontier, then walk hypotheses and experiments deliberately."
            }
        }
        div.sidebar-section {
            h2 { "Frontiers" }
            @if shell.frontiers.is_empty() {
                p.muted { "No frontiers yet." }
            } @else {
                nav.frontier-nav aria-label="Frontiers" {
                    @for frontier in &shell.frontiers {
                        a
                            href=(frontier_href(&frontier.slug))
                            class={(if shell
                                .active_frontier_slug
                                .as_ref()
                                .is_some_and(|active| active == &frontier.slug)
                            {
                                "frontier-nav-link active"
                            } else {
                                "frontier-nav-link"
                            })}
                        {
                            span.frontier-nav-title { (&frontier.label) }
                            span.frontier-nav-meta {
                                (frontier.active_hypothesis_count) " active · "
                                (frontier.open_experiment_count) " open"
                            }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_kv(label: &str, value: &str) -> Markup {
    html! {
        div.kv {
            div.kv-label { (label) }
            div.kv-value { (value) }
        }
    }
}

fn render_dimension_value(value: &RunDimensionValue) -> String {
    match value {
        RunDimensionValue::String(value) => value.to_string(),
        RunDimensionValue::Numeric(value) => format_float(*value),
        RunDimensionValue::Boolean(value) => value.to_string(),
        RunDimensionValue::Timestamp(value) => value.to_string(),
    }
}

fn format_metric_value(value: f64, unit: &MetricUnit) -> String {
    match unit.known_kind() {
        Some(KnownMetricUnit::Bytes) => format!("{} B", format_integerish(value)),
        Some(KnownMetricUnit::Seconds) => format!("{value:.3} s"),
        Some(KnownMetricUnit::Milliseconds) => format!("{value:.3} ms"),
        Some(KnownMetricUnit::Microseconds) => format!("{} us", format_integerish(value)),
        Some(KnownMetricUnit::Nanoseconds) => format!("{} ns", format_integerish(value)),
        Some(KnownMetricUnit::Count) => format_integerish(value),
        Some(KnownMetricUnit::Ratio) => format!("{value:.4}"),
        Some(KnownMetricUnit::Percent) => format!("{value:.2}%"),
        Some(KnownMetricUnit::Scalar) | None => {
            if unit.as_str() == "scalar" {
                format_float(value)
            } else {
                format!("{} {}", format_float(value), unit.as_str())
            }
        }
    }
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format_integerish(value)
    } else {
        format!("{value:.4}")
    }
}

fn format_integerish(value: f64) -> String {
    let negative = value.is_sign_negative();
    let digits = format!("{:.0}", value.abs());
    let mut grouped = String::with_capacity(digits.len() + (digits.len() / 3));
    for (index, ch) in digits.chars().rev().enumerate() {
        if index != 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    let grouped: String = grouped.chars().rev().collect();
    if negative {
        format!("-{grouped}")
    } else {
        grouped
    }
}

fn format_timestamp(value: OffsetDateTime) -> String {
    const TIMESTAMP: &[time::format_description::FormatItem<'static>] =
        format_description!("[year]-[month]-[day] [hour]:[minute]");
    value.format(TIMESTAMP).unwrap_or_else(|_| {
        value
            .format(&Rfc3339)
            .unwrap_or_else(|_| value.unix_timestamp().to_string())
    })
}

fn frontier_href(slug: &Slug) -> String {
    format!("/frontier/{}", encode_path_segment(slug.as_str()))
}

fn frontier_tab_href(slug: &Slug, tab: FrontierTab, metric: Option<&str>) -> String {
    frontier_tab_href_with_filters(slug, tab, metric, &BTreeMap::new())
}

fn frontier_tab_href_with_filters(
    slug: &Slug,
    tab: FrontierTab,
    metric: Option<&str>,
    dimension_filters: &BTreeMap<String, String>,
) -> String {
    let mut href = format!(
        "/frontier/{}?tab={}",
        encode_path_segment(slug.as_str()),
        tab.as_query()
    );
    if let Some(metric) = metric.filter(|metric| !metric.trim().is_empty()) {
        href.push_str("&metric=");
        href.push_str(&encode_path_segment(metric));
    }
    for (key, value) in dimension_filters {
        href.push_str("&dim.");
        href.push_str(&encode_path_segment(key));
        href.push('=');
        href.push_str(&encode_path_segment(value));
    }
    href
}

fn hypothesis_href(slug: &Slug) -> String {
    format!("/hypothesis/{}", encode_path_segment(slug.as_str()))
}

fn hypothesis_href_from_id(id: fidget_spinner_core::HypothesisId) -> String {
    format!("/hypothesis/{}", encode_path_segment(&id.to_string()))
}

fn hypothesis_title_for_roadmap_item(
    projection: &FrontierOpenProjection,
    hypothesis_id: fidget_spinner_core::HypothesisId,
) -> String {
    projection
        .active_hypotheses
        .iter()
        .find(|state| state.hypothesis.id == hypothesis_id)
        .map(|state| state.hypothesis.title.to_string())
        .unwrap_or_else(|| hypothesis_id.to_string())
}

fn experiment_href(slug: &Slug) -> String {
    format!("/experiment/{}", encode_path_segment(slug.as_str()))
}

fn artifact_href(slug: &Slug) -> String {
    format!("/artifact/{}", encode_path_segment(slug.as_str()))
}

fn resolve_attachment_display(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    target: AttachmentTargetRef,
) -> Result<AttachmentDisplay, StoreError> {
    match target {
        AttachmentTargetRef::Frontier(id) => {
            let frontier = store.read_frontier(&id.to_string())?;
            Ok(AttachmentDisplay {
                kind: "frontier",
                href: frontier_href(&frontier.slug),
                title: frontier.label.to_string(),
                summary: Some(frontier.objective.to_string()),
            })
        }
        AttachmentTargetRef::Hypothesis(id) => {
            let detail = store.read_hypothesis(&id.to_string())?;
            Ok(AttachmentDisplay {
                kind: "hypothesis",
                href: hypothesis_href(&detail.record.slug),
                title: detail.record.title.to_string(),
                summary: Some(detail.record.summary.to_string()),
            })
        }
        AttachmentTargetRef::Experiment(id) => {
            let detail = store.read_experiment(&id.to_string())?;
            Ok(AttachmentDisplay {
                kind: "experiment",
                href: experiment_href(&detail.record.slug),
                title: detail.record.title.to_string(),
                summary: detail.record.summary.as_ref().map(ToString::to_string),
            })
        }
    }
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn frontier_status_class(status: &str) -> &'static str {
    match status {
        "exploring" => "status-exploring",
        "paused" => "status-parked",
        "archived" => "status-archived",
        _ => "status-neutral",
    }
}

fn experiment_status_class(status: ExperimentStatus) -> &'static str {
    match status {
        ExperimentStatus::Open => "status-open",
        ExperimentStatus::Closed => "status-neutral",
    }
}

fn status_chip_classes(extra_class: &str) -> String {
    format!("status-chip {extra_class}")
}

fn verdict_class(verdict: FrontierVerdict) -> &'static str {
    match verdict {
        FrontierVerdict::Accepted => "status-accepted",
        FrontierVerdict::Kept => "status-kept",
        FrontierVerdict::Parked => "status-parked",
        FrontierVerdict::Rejected => "status-rejected",
    }
}

fn limit_items<T>(items: &[T], limit: Option<u32>) -> &[T] {
    let Some(limit) = limit else {
        return items;
    };
    let Ok(limit) = usize::try_from(limit) else {
        return items;
    };
    let end = items.len().min(limit);
    &items[..end]
}

fn collect_dimension_facets(
    points: &[fidget_spinner_store_sqlite::FrontierMetricPoint],
) -> Vec<DimensionFacet> {
    let mut values_by_key: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for point in points {
        for (key, value) in &point.dimensions {
            let _ = values_by_key
                .entry(key.to_string())
                .or_default()
                .insert(render_dimension_value(value));
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

fn styles() -> &'static str {
    r#"
    :root {
        color-scheme: light;
        --bg: #faf5ec;
        --panel: #fffaf2;
        --panel-2: #f6eee1;
        --border: #dfd1bd;
        --border-strong: #cfbea8;
        --text: #241d16;
        --muted: #6f6557;
        --accent: #67563f;
        --accent-soft: #ece2d2;
        --tag: #efe5d7;
        --accepted: #47663f;
        --kept: #5a6952;
        --parked: #8a6230;
        --rejected: #8a3a34;
        --shadow: rgba(83, 61, 33, 0.055);
    }
    * { box-sizing: border-box; }
    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font: 15px/1.55 "Iosevka Web", "IBM Plex Mono", "SFMono-Regular", monospace;
        overflow-x: hidden;
    }
    a {
        color: var(--accent);
        text-decoration: none;
    }
    a:hover { text-decoration: underline; }
    .shell {
        width: 100%;
        max-width: none;
        margin: 0 auto;
        padding: 24px 24px 40px;
        display: grid;
        gap: 20px;
        grid-template-columns: 280px minmax(0, 1fr);
        align-items: start;
        min-width: 0;
        overflow-x: clip;
    }
    .sidebar {
        position: sticky;
        top: 18px;
        min-width: 0;
    }
    .sidebar-panel {
        border: 1px solid var(--border);
        background: var(--panel);
        padding: 18px 16px;
        display: grid;
        gap: 16px;
        box-shadow: 0 1px 0 var(--shadow);
    }
    .sidebar-project {
        display: grid;
        gap: 8px;
    }
    .sidebar-home {
        color: var(--text);
        font-size: 18px;
        font-weight: 700;
    }
    .sidebar-copy {
        margin: 0;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.5;
    }
    .sidebar-section {
        display: grid;
        gap: 10px;
    }
    .frontier-nav {
        display: grid;
        gap: 8px;
    }
    .frontier-nav-link {
        display: grid;
        gap: 4px;
        padding: 10px 12px;
        border: 1px solid var(--border);
        background: var(--panel-2);
    }
    .frontier-nav-link.active {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .frontier-nav-title {
        color: var(--text);
        font-weight: 700;
    }
    .frontier-nav-meta {
        color: var(--muted);
        font-size: 12px;
    }
    .main-column {
        display: grid;
        gap: 18px;
        min-width: 0;
    }
    .page-header {
        display: grid;
        gap: 10px;
        padding: 18px 20px;
        border: 1px solid var(--border);
        background: var(--panel);
        box-shadow: 0 1px 0 var(--shadow);
        min-width: 0;
    }
    .eyebrow {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        color: var(--muted);
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .sep { color: #a08d70; }
    .page-title {
        margin: 0;
        font-size: clamp(22px, 3.8vw, 34px);
        line-height: 1.1;
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    .page-subtitle {
        margin: 0;
        color: var(--muted);
        max-width: 90ch;
        overflow-wrap: anywhere;
    }
    .tab-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
    }
    .tab-chip {
        display: inline-flex;
        align-items: center;
        padding: 8px 12px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .tab-chip.active {
        color: var(--text);
        border-color: var(--border-strong);
        background: var(--accent-soft);
        font-weight: 700;
    }
    .card {
        border: 1px solid var(--border);
        background: var(--panel);
        padding: 18px 20px;
        display: grid;
        gap: 14px;
        box-shadow: 0 1px 0 var(--shadow);
        min-width: 0;
    }
    .subcard {
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 12px 14px;
        display: grid;
        gap: 10px;
        min-width: 0;
        align-content: start;
    }
    .compact-subcard {
        justify-items: start;
    }
    .block { display: grid; gap: 10px; }
    .stack {
        display: grid;
        gap: 14px;
    }
    .split {
        display: grid;
        gap: 16px;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        align-items: start;
    }
    .card-grid {
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
        align-items: start;
    }
    .mini-card {
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 12px 14px;
        display: grid;
        gap: 9px;
        min-width: 0;
        align-content: start;
    }
    .card-header {
        display: flex;
        gap: 10px;
        align-items: flex-start;
        flex-wrap: wrap;
    }
    .title-link {
        font-size: 16px;
        font-weight: 700;
        color: var(--text);
        overflow-wrap: anywhere;
        word-break: break-word;
        flex: 1 1 auto;
        min-width: 0;
    }
    h1, h2, h3 {
        margin: 0;
        line-height: 1.15;
        overflow-wrap: anywhere;
        word-break: break-word;
        min-width: 0;
    }
    h2 { font-size: 19px; }
    h3 { font-size: 14px; color: #4f473a; }
    .prose {
        margin: 0;
        color: var(--text);
        max-width: 92ch;
        white-space: pre-wrap;
    }
    .muted { color: var(--muted); }
    .meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px 14px;
        align-items: center;
        font-size: 13px;
    }
    .kv-grid {
        display: grid;
        gap: 10px 14px;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }
    .kv {
        display: grid;
        gap: 4px;
        min-width: 0;
    }
    .kv-label {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .kv-value {
        overflow-wrap: anywhere;
    }
    .chip-row, .link-list {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        align-items: flex-start;
        align-content: flex-start;
        justify-content: flex-start;
    }
    .tag-cloud { max-width: 100%; }
    .tag-chip, .kind-chip, .status-chip, .metric-pill {
        display: inline-flex;
        align-items: center;
        flex: 0 0 auto;
        width: auto;
        max-width: 100%;
        border: 1px solid var(--border-strong);
        background: var(--tag);
        padding: 4px 8px;
        font-size: 12px;
        line-height: 1.2;
        white-space: nowrap;
    }
    .metric-picker {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
    }
    .metric-choice {
        display: grid;
        gap: 4px;
        padding: 10px 12px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        min-width: 0;
    }
    .metric-choice.active {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .metric-choice-key {
        color: var(--text);
        font-weight: 700;
    }
    .metric-choice-meta {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .filter-form {
        display: grid;
        gap: 12px;
    }
    .filter-form-grid {
        display: grid;
        gap: 10px 12px;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }
    .filter-control {
        display: grid;
        gap: 6px;
        min-width: 0;
    }
    .filter-label {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .filter-select {
        width: 100%;
        min-width: 0;
        padding: 7px 9px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font: inherit;
    }
    .filter-actions {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
    }
    .filter-apply {
        padding: 7px 11px;
        border: 1px solid var(--border-strong);
        background: var(--accent-soft);
        color: var(--text);
        font: inherit;
        cursor: pointer;
    }
    .metric-filter-chip {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 5px 9px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font-size: 12px;
        white-space: nowrap;
    }
    .metric-filter-chip.active {
        border-color: var(--border-strong);
        background: var(--accent-soft);
        font-weight: 700;
    }
    .clear-filter {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .link-chip {
        display: inline-grid;
        gap: 4px;
        align-content: start;
        max-width: min(100%, 72ch);
        padding: 8px 10px;
        border: 1px solid var(--border);
        background: var(--panel);
        min-width: 0;
    }
    .link-chip-main {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 8px;
        align-items: flex-start;
        min-width: 0;
    }
    .link-chip-title {
        overflow-wrap: anywhere;
    }
    .link-chip-summary {
        color: var(--muted);
        font-size: 12px;
        line-height: 1.4;
        overflow-wrap: anywhere;
    }
    .kind-chip {
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .status-chip {
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 700;
    }
    .status-accepted { color: var(--accepted); border-color: color-mix(in srgb, var(--accepted) 24%, white); background: color-mix(in srgb, var(--accepted) 10%, white); }
    .status-kept { color: var(--kept); border-color: color-mix(in srgb, var(--kept) 22%, white); background: color-mix(in srgb, var(--kept) 9%, white); }
    .status-parked { color: var(--parked); border-color: color-mix(in srgb, var(--parked) 24%, white); background: color-mix(in srgb, var(--parked) 10%, white); }
    .status-rejected { color: var(--rejected); border-color: color-mix(in srgb, var(--rejected) 24%, white); background: color-mix(in srgb, var(--rejected) 10%, white); }
    .status-open, .status-exploring { color: var(--accent); border-color: color-mix(in srgb, var(--accent) 22%, white); background: var(--accent-soft); }
    .status-neutral, .classless { color: #5f584d; border-color: var(--border-strong); background: var(--panel); }
    .status-archived { color: #7a756d; border-color: var(--border); background: var(--panel); }
    .metric-table {
        width: max-content;
        min-width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    .table-scroll {
        width: 100%;
        overflow-x: auto;
    }
    .metric-table th,
    .metric-table td {
        padding: 7px 8px;
        border-top: 1px solid var(--border);
        text-align: left;
        vertical-align: top;
        white-space: nowrap;
        overflow-wrap: normal;
        word-break: normal;
    }
    .metric-table th {
        color: var(--muted);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 12px;
    }
    .related-block {
        display: grid;
        gap: 8px;
    }
    .chart-frame {
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 10px;
        overflow-x: auto;
    }
    .chart-frame svg {
        display: block;
        width: 100%;
        height: auto;
    }
    .chart-error {
        color: var(--rejected);
        font-size: 13px;
    }
    .roadmap-list, .simple-list {
        margin: 0;
        padding-left: 18px;
        display: grid;
        gap: 6px;
    }
    .roadmap-list li, .simple-list li {
        overflow-wrap: anywhere;
    }
    .code-block {
        white-space: pre-wrap;
        overflow-wrap: anywhere;
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 12px 14px;
    }
    code {
        font-family: inherit;
        font-size: 0.95em;
        background: var(--panel-2);
        padding: 0.05rem 0.3rem;
    }
    @media (max-width: 980px) {
        .shell {
            grid-template-columns: 1fr;
        }
        .sidebar {
            position: static;
        }
    }
    @media (max-width: 720px) {
        .shell { padding: 12px; }
        .card, .page-header { padding: 14px; }
        .subcard, .mini-card { padding: 12px; }
        .card-grid, .split, .kv-grid { grid-template-columns: 1fr; }
        .page-title { font-size: 18px; }
    }
    "#
}
