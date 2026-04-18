use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::time::UNIX_EPOCH;

use axum::Router;
use axum::extract::{Form, Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{StatusCode, Uri};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use camino::Utf8PathBuf;
use fidget_spinner_core::{
    ExperimentAnalysis, ExperimentOutcome, ExperimentStatus, FrontierRecord, FrontierStatus,
    FrontierVerdict, KnownMetricUnit, MetricAggregation, MetricDimension, MetricUnit, NonEmptyText,
    OptimizationObjective, RegistryLockMode, RegistryName, RunDimensionValue, Slug, TagFamilyName,
    TagName, VertexRef,
};
use fidget_spinner_store_sqlite::{
    AssignTagFamilyRequest, CreateKpiRequest, CreateTagFamilyRequest, DefineMetricRequest,
    DeleteKpiRequest, DeleteMetricRequest, DeleteTagRequest, ExperimentDetail, ExperimentSummary,
    FrontierMetricSeries, FrontierOpenProjection, FrontierSummary, HypothesisCurrentState,
    HypothesisDetail, KpiSummary, ListExperimentsQuery, ListFrontiersQuery, ListHypothesesQuery,
    MergeMetricRequest, MergeTagRequest, MetricKeysQuery, MetricScope, MoveKpiDirection,
    MoveKpiRequest, ProjectStatus, RenameMetricRequest, RenameTagRequest, STATE_DB_NAME,
    SetFrontierRegistryLockRequest, SetRegistryLockRequest, SetTagFamilyMandatoryRequest,
    StoreError, TextPatch, UpdateFrontierRequest, UpdateProjectRequest, VertexSummary,
};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use plotters::prelude::{
    BLACK, ChartBuilder, Circle, Cross, IntoDrawingArea, IntoLogRange, LineSeries, PathElement,
    SVGBackend, SeriesLabelPosition, ShapeStyle,
};
use plotters::style::{Color, IntoFont, RGBColor};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::open_store;
mod assets;
mod detail;
mod registry;
mod results;
mod routes;

use assets::harden_autofill_controls;

use routes::ProjectDescriptionForm;
pub(crate) use routes::serve;

const FAVICON_SVG: &str = include_str!("../../../../assets/ui/favicon.svg");
const UI_NAV_STATE_KEY: &str = "fidget-spinner-ui-nav-state";
const METRIC_TABLE_TITLE_PERCENT_BUDGET: usize = 96;
const METRIC_TABLE_TITLE_MIN_BUDGET_CH: usize = 22;

#[derive(Clone)]
struct NavigatorState {
    scope: NavigatorScope,
    limit: Option<u32>,
}

#[derive(Clone)]
pub(crate) enum NavigatorScope {
    Single(Utf8PathBuf),
    Multi {
        scan_root: Utf8PathBuf,
        project_roots: BTreeSet<Utf8PathBuf>,
    },
}

#[derive(Clone)]
struct ShellFrame {
    active_frontier_slug: Option<Slug>,
    frontiers: Vec<FrontierSummary>,
    archived_frontiers: Vec<FrontierSummary>,
    project_status: ProjectStatus,
    base_href: String,
    project_home_href: String,
    refresh_token_href: String,
}

#[derive(Clone)]
struct ProjectRenderContext {
    project_root: Utf8PathBuf,
    base_href: String,
    project_home_href: String,
    refresh_token_href: String,
    limit: Option<u32>,
}

impl ProjectRenderContext {
    fn root(project_root: Utf8PathBuf, limit: Option<u32>) -> Self {
        Self {
            project_root,
            base_href: "/".to_owned(),
            project_home_href: ".".to_owned(),
            refresh_token_href: "/refresh-token".to_owned(),
            limit,
        }
    }

    fn nested(project_root: Utf8PathBuf, limit: Option<u32>) -> Self {
        let base_href = project_base_href(&project_root);
        Self {
            project_root,
            refresh_token_href: format!("{base_href}refresh-token"),
            base_href,
            project_home_href: ".".to_owned(),
            limit,
        }
    }
}

#[derive(Clone)]
struct ProjectIndexItem {
    project_root: Utf8PathBuf,
    project_status: ProjectStatus,
}

#[derive(Clone, Copy, Default)]
struct TagUsage {
    hypotheses: u64,
    experiments: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrontierTab {
    Brief,
    Open,
    Closed,
    Results,
}

#[derive(Clone, Debug, Default)]
struct FrontierPageQuery {
    metric: Vec<String>,
    table_metric: Option<String>,
    tab: Option<String>,
    extra: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct ProjectMetricsQuery {
    frontier: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DimensionFacet {
    key: String,
    values: Vec<String>,
}

impl FrontierTab {
    fn from_query(raw: Option<&str>) -> Self {
        match raw {
            Some("brief") => Self::Brief,
            Some("open") => Self::Open,
            Some("closed") => Self::Closed,
            _ => Self::Results,
        }
    }

    const fn as_query(self) -> &'static str {
        match self {
            Self::Brief => "brief",
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Results => "results",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Brief => "Brief",
            Self::Open => "Open",
            Self::Closed => "Closed",
            Self::Results => "Results",
        }
    }
}

impl ProjectMetricsQuery {
    fn parse(raw_query: Option<&str>) -> Result<Self, StoreError> {
        let mut query = Self::default();
        for segment in raw_query
            .unwrap_or_default()
            .split('&')
            .filter(|segment| !segment.is_empty())
        {
            let (raw_key, raw_value) = segment.split_once('=').unwrap_or((segment, ""));
            let key = decode_query_component(raw_key)?;
            let value = decode_query_component(raw_value)?;
            if key == "frontier" {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    query.frontier = Some(trimmed.to_owned());
                }
            }
        }
        Ok(query)
    }
}

impl FrontierPageQuery {
    fn parse(raw_query: Option<&str>) -> Result<Self, StoreError> {
        let mut query = Self::default();
        for segment in raw_query
            .unwrap_or_default()
            .split('&')
            .filter(|segment| !segment.is_empty())
        {
            let (raw_key, raw_value) = segment.split_once('=').unwrap_or((segment, ""));
            let key = decode_query_component(raw_key)?;
            let value = decode_query_component(raw_value)?;
            match key.as_str() {
                "metric" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.metric.push(trimmed.to_owned());
                    }
                }
                "table_metric" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.table_metric = Some(trimmed.to_owned());
                    }
                }
                "tab" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.tab = Some(trimmed.to_owned());
                    }
                }
                _ => {
                    let _ = query.extra.insert(key, value);
                }
            }
        }
        Ok(query)
    }

    fn log_y_requested(&self) -> bool {
        self.extra
            .get("log_y")
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "on" | "yes"))
    }

    fn condition_filters(&self) -> BTreeMap<String, String> {
        self.extra
            .iter()
            .filter_map(|(key, value)| {
                let value = value.trim();
                (!value.is_empty())
                    .then(|| {
                        key.strip_prefix("condition.")
                            .map(|condition| (condition.to_owned(), value.to_owned()))
                    })
                    .flatten()
            })
            .collect()
    }
}

fn render_response(result: Result<Markup, StoreError>) -> Response {
    match result {
        Ok(markup) => Html(harden_autofill_controls(markup.into_string())).into_response(),
        Err(StoreError::UnknownFrontierSelector(_))
        | Err(StoreError::UnknownHypothesisSelector(_))
        | Err(StoreError::UnknownExperimentSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("navigator render failed: {error}"),
        )
            .into_response(),
    }
}

fn refresh_token_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(token) => ([(CONTENT_TYPE, "text/plain; charset=utf-8")], token).into_response(),
        Err(StoreError::MissingProjectStore(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("navigator refresh-token failed: {error}"),
        )
            .into_response(),
    }
}

fn frontier_status_mutation_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err(StoreError::RevisionMismatch { .. }) => (
            StatusCode::CONFLICT,
            "frontier changed before the archive request landed; reload and retry".to_owned(),
        )
            .into_response(),
        Err(StoreError::UnknownFrontierSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("frontier archive update failed: {error}"),
        )
            .into_response(),
    }
}

fn project_mutation_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("project metadata update failed: {error}"),
        )
            .into_response(),
    }
}

fn tag_mutation_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err(StoreError::RevisionMismatch { .. }) => (
            StatusCode::CONFLICT,
            "tag registry changed before the supervisor request landed; reload and retry"
                .to_owned(),
        )
            .into_response(),
        Err(StoreError::UnknownTag(_)) | Err(StoreError::UnknownTagFamily(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(StoreError::PolicyViolation(message)) => {
            (StatusCode::CONFLICT, message).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("tag supervisor update failed: {error}"),
        )
            .into_response(),
    }
}

fn metric_mutation_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err(StoreError::UnknownMetricDefinition(_))
        | Err(StoreError::UnknownKpi(_))
        | Err(StoreError::UnknownFrontierSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(StoreError::DuplicateMetricDefinition(_)) | Err(StoreError::DuplicateKpi(_)) => {
            (StatusCode::CONFLICT, "metric registry conflict".to_owned()).into_response()
        }
        Err(StoreError::PolicyViolation(message)) => {
            (StatusCode::CONFLICT, message).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metric supervisor update failed: {error}"),
        )
            .into_response(),
    }
}

fn parse_ui_lock_mode(raw: &str) -> Result<RegistryLockMode, StoreError> {
    match raw {
        "add" => Ok(RegistryLockMode::Definition),
        "edit" => Ok(RegistryLockMode::Family),
        _ => Err(StoreError::InvalidInput(format!(
            "invalid registry lock mode `{raw}`"
        ))),
    }
}

fn optional_text_field(value: String) -> Result<Option<NonEmptyText>, StoreError> {
    if value.trim().is_empty() {
        Ok(None)
    } else {
        NonEmptyText::new(value).map(Some).map_err(StoreError::from)
    }
}

fn text_patch_field(value: String) -> Result<TextPatch<NonEmptyText>, StoreError> {
    if value.trim().is_empty() {
        Ok(TextPatch::Clear)
    } else {
        NonEmptyText::new(value)
            .map(TextPatch::Set)
            .map_err(StoreError::from)
    }
}

fn metrics_frontier_href(context: &ProjectRenderContext, frontier: &str) -> String {
    format!(
        "{}metrics?frontier={}",
        context.base_href,
        encode_path_segment(frontier)
    )
}

fn parse_optimization_objective_ui(raw: &str) -> Result<OptimizationObjective, StoreError> {
    match raw {
        "minimize" => Ok(OptimizationObjective::Minimize),
        "maximize" => Ok(OptimizationObjective::Maximize),
        "target" => Ok(OptimizationObjective::Target),
        _ => Err(StoreError::InvalidInput(format!(
            "invalid optimization objective `{raw}`"
        ))),
    }
}

fn parse_metric_dimension_ui(raw: &str) -> Result<MetricDimension, StoreError> {
    match raw {
        "time" => Ok(MetricDimension::Time),
        "count" => Ok(MetricDimension::Count),
        "bytes" => Ok(MetricDimension::Bytes),
        "ratio" => Ok(MetricDimension::Ratio),
        "dimensionless" => Ok(MetricDimension::Dimensionless),
        _ => Err(StoreError::InvalidInput(format!(
            "invalid metric dimension `{raw}`"
        ))),
    }
}

fn parse_metric_aggregation_ui(raw: &str) -> Result<MetricAggregation, StoreError> {
    match raw {
        "point" => Ok(MetricAggregation::Point),
        "mean" => Ok(MetricAggregation::Mean),
        "geomean" => Ok(MetricAggregation::Geomean),
        "median" => Ok(MetricAggregation::Median),
        "p95" => Ok(MetricAggregation::P95),
        "min" => Ok(MetricAggregation::Min),
        "max" => Ok(MetricAggregation::Max),
        "sum" => Ok(MetricAggregation::Sum),
        _ => Err(StoreError::InvalidInput(format!(
            "invalid metric aggregation `{raw}`"
        ))),
    }
}

fn resolve_project_context(
    state: &NavigatorState,
    encoded_project_root: &str,
) -> Result<ProjectRenderContext, StoreError> {
    let project_root = decode_project_root(encoded_project_root)?;
    match &state.scope {
        NavigatorScope::Single(expected_root) if expected_root == &project_root => {}
        NavigatorScope::Single(_) => {
            return Err(StoreError::MissingProjectStore(project_root));
        }
        NavigatorScope::Multi { project_roots, .. } if project_roots.contains(&project_root) => {}
        NavigatorScope::Multi { .. } => {
            return Err(StoreError::MissingProjectStore(project_root));
        }
    }
    Ok(ProjectRenderContext::nested(project_root, state.limit))
}

fn project_refresh_token_for(context: &ProjectRenderContext) -> Result<String, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let database_path = store.state_root().join(STATE_DB_NAME);
    refresh_file_token(&database_path)
}

fn refresh_file_token(path: &camino::Utf8Path) -> Result<String, StoreError> {
    let metadata = std::fs::metadata(path.as_std_path())?;
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(format!(
        "{}.{}:{}",
        modified.as_secs(),
        modified.subsec_nanos(),
        metadata.len()
    ))
}

fn update_project_description(
    context: ProjectRenderContext,
    form: ProjectDescriptionForm,
) -> Result<String, StoreError> {
    let mut store = open_store(context.project_root.as_std_path())?;
    let description = match NonEmptyText::new(form.description) {
        Ok(description) => TextPatch::Set(description),
        Err(_) => TextPatch::Clear,
    };
    let _status = store.update_project(UpdateProjectRequest { description })?;
    Ok(context.base_href)
}

fn update_frontier_status(
    context: ProjectRenderContext,
    selector: String,
    expected_revision: Option<u64>,
    status: FrontierStatus,
) -> Result<String, StoreError> {
    let mut store = open_store(context.project_root.as_std_path())?;
    let updated = store.update_frontier(UpdateFrontierRequest {
        frontier: selector,
        expected_revision,
        label: None,
        objective: None,
        status: Some(status),
        situation: None,
        roadmap: None,
        unknowns: None,
    })?;
    Ok(format!(
        "{}{}",
        context.base_href,
        frontier_href(&updated.slug)
    ))
}

fn load_shell_frame(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    active_frontier_slug: Option<Slug>,
    context: &ProjectRenderContext,
) -> Result<ShellFrame, StoreError> {
    let mut active_frontiers = Vec::new();
    let mut archived_frontiers = Vec::new();
    for frontier in store.list_frontiers(ListFrontiersQuery {
        include_archived: true,
    })? {
        if frontier.status == FrontierStatus::Archived {
            archived_frontiers.push(frontier);
        } else {
            active_frontiers.push(frontier);
        }
    }
    Ok(ShellFrame {
        active_frontier_slug,
        base_href: context.base_href.clone(),
        frontiers: active_frontiers,
        archived_frontiers,
        project_home_href: context.project_home_href.clone(),
        project_status: store.status()?,
        refresh_token_href: context.refresh_token_href.clone(),
    })
}

fn render_sidebar(shell: &ShellFrame) -> Markup {
    html! {
    section.sidebar-panel {
        div.sidebar-project {
            div.sidebar-title-row {
                a.sidebar-home href=(&shell.project_home_href) { (&shell.project_status.display_name) }
                a.sidebar-home-chip href="/" { "Home" }
            }
            div.sidebar-actions {
                a.sidebar-tags href=(format!("{}tags", shell.base_href)) { "Tags" }
                a.sidebar-tags href=(format!("{}metrics", shell.base_href)) { "Metrics" }
            }
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
                        (render_sidebar_frontier_item(
                            frontier,
                            shell.active_frontier_slug.as_ref(),
                            FrontierSidebarAction::Archive,
                        ))
                    }
                }
            }
            @if !shell.archived_frontiers.is_empty() {
                details.sidebar-archived {
                    summary.sidebar-archived-toggle {
                        "Archived (" (shell.archived_frontiers.len()) ")"
                    }
                    nav.frontier-nav.sidebar-archived-list aria-label="Archived frontiers" {
                        @for frontier in &shell.archived_frontiers {
                            (render_sidebar_frontier_item(
                                frontier,
                                shell.active_frontier_slug.as_ref(),
                                FrontierSidebarAction::Unarchive,
                            ))
                        }
                    }
                }
            }
        }
    }
    }
}

#[derive(Clone, Copy)]
enum FrontierSidebarAction {
    Archive,
    Unarchive,
}

fn render_sidebar_frontier_item(
    frontier: &FrontierSummary,
    active_frontier_slug: Option<&Slug>,
    action: FrontierSidebarAction,
) -> Markup {
    let active = active_frontier_slug.is_some_and(|active| active == &frontier.slug);
    html! {
    div.frontier-nav-item {
        a
            href=(frontier_href(&frontier.slug))
            class={(if active {
                "frontier-nav-link active"
            } else {
                "frontier-nav-link"
            })}
        {
            span.frontier-nav-title { (&frontier.label) }
            span.frontier-nav-meta {
                @if frontier.status == FrontierStatus::Archived {
                    "archived"
                } @else {
                    (frontier.active_hypothesis_count) " active · "
                    (frontier.open_experiment_count) " open"
                }
            }
        }
        (render_frontier_sidebar_action(frontier, action))
    }
    }
}

fn render_frontier_sidebar_action(
    frontier: &FrontierSummary,
    action: FrontierSidebarAction,
) -> Markup {
    match action {
        FrontierSidebarAction::Archive => html! {
            form.frontier-action-form method="post" action=(format!("{}/archive", frontier_href(&frontier.slug))) {
                input type="hidden" name="expected_revision" value=(frontier.revision);
                button.frontier-action-button type="submit" aria-label=(format!("Archive {}", frontier.label)) title="Archive frontier" {
                    (archive_icon())
                }
            }
        },
        FrontierSidebarAction::Unarchive => html! {
            form.frontier-action-form method="post" action=(format!("{}/unarchive", frontier_href(&frontier.slug))) {
                input type="hidden" name="expected_revision" value=(frontier.revision);
                button.frontier-action-button type="submit" aria-label=(format!("Unarchive {}", frontier.label)) title="Unarchive frontier" {
                    (unarchive_icon())
                }
            }
        },
    }
}

fn archive_icon() -> Markup {
    html! {
        svg.frontier-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M4 7.5h16" {}
            path d="M6 7.5v10a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-10" {}
            path d="M7 4.5h10l1 3H6l1-3Z" {}
            path d="M10 11h4" {}
        }
    }
}

fn unarchive_icon() -> Markup {
    html! {
        svg.frontier-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M4 7.5h16" {}
            path d="M6 7.5v10a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-10" {}
            path d="M7 4.5h10l1 3H6l1-3Z" {}
            path d="M12 15V10" {}
            path d="M9.5 12.5 12 10l2.5 2.5" {}
        }
    }
}

fn pencil_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M4.5 17.5 16.8 5.2a1.8 1.8 0 0 1 2.5 0l.5.5a1.8 1.8 0 0 1 0 2.5L7.5 20.5h-3v-3Z" {}
            path d="m14.5 7.5 2 2" {}
        }
    }
}

fn trash_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M5 7h14" {}
            path d="M9 7V4.5h6V7" {}
            path d="M7 7l1 13h8l1-13" {}
            path d="M10.5 11v5" {}
            path d="M13.5 11v5" {}
        }
    }
}

fn plus_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M12 5v14" {}
            path d="M5 12h14" {}
        }
    }
}

fn arrow_up_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M12 19V5" {}
            path d="M6.5 10.5 12 5l5.5 5.5" {}
        }
    }
}

fn arrow_down_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M12 5v14" {}
            path d="M6.5 13.5 12 19l5.5-5.5" {}
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

fn render_fact(label: &str, value: &str) -> Markup {
    html! {
        span.fact {
            span.fact-label { (label) }
            span.fact-value { (value) }
        }
    }
}

fn short_commit_hash(commit_hash: &str) -> &str {
    commit_hash.get(..12).unwrap_or(commit_hash)
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
        Some(KnownMetricUnit::Kibibytes) => format!("{value:.2} KiB"),
        Some(KnownMetricUnit::Mebibytes) => format!("{value:.2} MiB"),
        Some(KnownMetricUnit::Gibibytes) => format!("{value:.2} GiB"),
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

fn project_root_href(project_root: &Utf8PathBuf) -> String {
    format!("/project/{}/", encode_path_segment(project_root.as_str()))
}

fn project_base_href(project_root: &Utf8PathBuf) -> String {
    project_root_href(project_root)
}

fn decode_project_root(encoded: &str) -> Result<Utf8PathBuf, StoreError> {
    let decoded = percent_decode_str(encoded)
        .decode_utf8()
        .map_err(|error| StoreError::InvalidInput(format!("invalid project path: {error}")))?;
    Ok(Utf8PathBuf::from(decoded.into_owned()))
}

fn decode_query_component(raw: &str) -> Result<String, StoreError> {
    let plus_decoded = raw.replace('+', " ");
    percent_decode_str(&plus_decoded)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|error| StoreError::InvalidInput(format!("invalid query string: {error}")))
}

fn frontier_href(slug: &Slug) -> String {
    format!("frontier/{}", encode_path_segment(slug.as_str()))
}

fn frontier_results_href(slug: &Slug) -> String {
    frontier_tab_href(slug, FrontierTab::Results, &[], false, None)
}

fn project_metrics_frontier_href(slug: &Slug) -> String {
    format!("metrics?frontier={}", encode_path_segment(slug.as_str()))
}

fn frontier_tab_href(
    slug: &Slug,
    tab: FrontierTab,
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    log_y: bool,
    table_metric: Option<&str>,
) -> String {
    frontier_tab_href_with_query(
        slug,
        tab,
        selected_metrics,
        log_y,
        &BTreeMap::new(),
        table_metric,
    )
}

fn frontier_tab_href_with_query(
    slug: &Slug,
    tab: FrontierTab,
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    log_y: bool,
    condition_filters: &BTreeMap<String, String>,
    table_metric: Option<&str>,
) -> String {
    let mut href = format!(
        "frontier/{}?tab={}",
        encode_path_segment(slug.as_str()),
        tab.as_query()
    );
    for metric in selected_metrics {
        href.push_str("&metric=");
        href.push_str(&encode_path_segment(metric.key.as_str()));
    }
    if log_y {
        href.push_str("&log_y=1");
    }
    if let Some(table_metric) = table_metric.filter(|table_metric| !table_metric.trim().is_empty())
    {
        href.push_str("&table_metric=");
        href.push_str(&encode_path_segment(table_metric));
    }
    for (key, value) in condition_filters {
        href.push_str("&condition.");
        href.push_str(&encode_path_segment(key));
        href.push('=');
        href.push_str(&encode_path_segment(value));
    }
    href
}

fn hypothesis_href(slug: &Slug) -> String {
    format!("hypothesis/{}", encode_path_segment(slug.as_str()))
}

fn hypothesis_href_from_id(id: fidget_spinner_core::HypothesisId) -> String {
    format!("hypothesis/{}", encode_path_segment(&id.to_string()))
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
    format!("experiment/{}", encode_path_segment(slug.as_str()))
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

#[cfg(test)]
mod tests {
    use super::assets::harden_autofill_controls;
    use super::registry::{metric_registry_filter_text, render_metric_registry_table};
    use super::results::{
        MetricChartAxis, best_metric_table_title_split, render_metric_series_section,
        resolve_selected_metric_keys, truncated_entry_count,
    };
    use super::{FrontierPageQuery, FrontierTab, METRIC_TABLE_TITLE_MIN_BUDGET_CH};
    use std::collections::BTreeMap;

    use fidget_spinner_core::{
        DefaultVisibility, ExperimentStatus, FrontierBrief, FrontierId, FrontierRecord,
        FrontierStatus, FrontierVerdict, HypothesisId, MetricAggregation, MetricUnit, NonEmptyText,
        OptimizationObjective, Slug,
    };
    use fidget_spinner_store_sqlite::{
        ExperimentSummary, FrontierMetricPoint, FrontierMetricSeries, HypothesisSummary,
        MetricKeySummary,
    };
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;

    #[allow(clippy::panic, reason = "test constructors should fail loudly")]
    fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error}"),
        }
    }

    fn test_metric(key: &str, unit: &str) -> MetricKeySummary {
        let unit = must(MetricUnit::new(unit), "metric unit");
        MetricKeySummary {
            key: must(NonEmptyText::new(key.to_owned()), "metric key"),
            dimension: unit.dimension(),
            display_unit: unit,
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Minimize,
            default_visibility: DefaultVisibility::visible(),
            description: None,
            reference_count: 0,
        }
    }

    fn test_timestamp(raw: &str) -> OffsetDateTime {
        must(OffsetDateTime::parse(raw, &Rfc3339), "timestamp")
    }

    #[test]
    fn autofill_hardening_marks_visible_form_controls_once() {
        let document = r#"<form method="post"><input type="text" name="tag"><select name="family"></select><textarea name="body"></textarea><input type="hidden" name="revision"></form>"#;
        let hardened = harden_autofill_controls(document.to_owned());
        assert!(hardened.contains(r#"<form method="post" autocomplete="off">"#));
        assert!(hardened.contains(
            r#"<input type="text" name="tag" autocomplete="off" data-protonpass-ignore="true">"#
        ));
        assert!(hardened.contains(
            r#"<select name="family" autocomplete="off" data-protonpass-ignore="true">"#
        ));
        assert!(hardened.contains(
            r#"<textarea name="body" autocomplete="off" data-protonpass-ignore="true">"#
        ));
        assert!(hardened.contains(r#"<input type="hidden" name="revision">"#));

        let rehardened = harden_autofill_controls(hardened);
        assert_eq!(rehardened.matches(r#"autocomplete="off""#).count(), 4);
        assert_eq!(
            rehardened
                .matches(r#"data-protonpass-ignore="true""#)
                .count(),
            3
        );
    }

    #[test]
    fn metric_registry_table_exposes_reactive_filter_hooks() {
        let metrics = vec![test_metric("presolve_wallclock", "milliseconds")];
        let markup = render_metric_registry_table(&metrics).into_string();
        let filter_text = metric_registry_filter_text(&metrics[0]);

        assert!(markup.contains(r#"data-table-filter-input="metric-registry""#));
        assert!(markup.contains(r#"data-table-filter-row="metric-registry""#));
        assert!(markup.contains(r#"data-table-filter-empty="metric-registry" hidden"#));
        assert_eq!(filter_text, "presolve_wallclock time point minimize ");
        assert!(markup.contains(r#"<td class="no-truncate">time</td>"#));
        assert!(markup.contains(r#"action="metrics/description""#));
        assert!(markup.contains(r#"data-inline-edit-allow-clear="true""#));
        assert!(markup.contains(r#"class="metric-identity-stack""#));
        assert!(markup.contains(r#"class="tag-inline-rename-form metric-description-form""#));
    }

    fn test_frontier() -> FrontierRecord {
        let timestamp = test_timestamp("2026-04-11T00:00:00Z");
        FrontierRecord {
            id: FrontierId::fresh(),
            slug: must(Slug::new("test-frontier"), "frontier slug"),
            label: must(NonEmptyText::new("Test frontier"), "frontier label"),
            objective: must(NonEmptyText::new("Test objective"), "frontier objective"),
            status: FrontierStatus::Exploring,
            brief: FrontierBrief::default(),
            revision: 1,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    fn test_hypothesis(frontier_id: FrontierId, slug: &str, title: &str) -> HypothesisSummary {
        HypothesisSummary {
            id: HypothesisId::fresh(),
            slug: must(Slug::new(slug), "hypothesis slug"),
            frontier_id,
            title: must(NonEmptyText::new(title), "hypothesis title"),
            summary: must(
                NonEmptyText::new(format!("{title} summary")),
                "hypothesis summary",
            ),
            tags: Vec::new(),
            open_experiment_count: 0,
            latest_verdict: None,
            updated_at: test_timestamp("2026-04-11T00:00:00Z"),
        }
    }

    fn test_experiment(
        frontier_id: FrontierId,
        hypothesis_id: HypothesisId,
        slug: &str,
        title: &str,
        closed_at: OffsetDateTime,
    ) -> ExperimentSummary {
        ExperimentSummary {
            id: fidget_spinner_core::ExperimentId::fresh(),
            slug: must(Slug::new(slug), "experiment slug"),
            frontier_id,
            hypothesis_id,
            title: must(NonEmptyText::new(title), "experiment title"),
            summary: None,
            tags: Vec::new(),
            status: ExperimentStatus::Closed,
            verdict: Some(FrontierVerdict::Accepted),
            primary_metric: None,
            updated_at: closed_at,
            closed_at: Some(closed_at),
        }
    }

    fn test_metric_point(
        frontier_id: FrontierId,
        hypothesis: &HypothesisSummary,
        slug: &str,
        title: &str,
        value: f64,
        closed_at: OffsetDateTime,
    ) -> FrontierMetricPoint {
        FrontierMetricPoint {
            experiment: test_experiment(frontier_id, hypothesis.id, slug, title, closed_at),
            hypothesis: hypothesis.clone(),
            metric_key: must(NonEmptyText::new("test_metric"), "metric key"),
            value,
            verdict: FrontierVerdict::Accepted,
            closed_at,
            dimensions: BTreeMap::new(),
        }
    }

    #[test]
    fn best_metric_table_title_split_favors_the_more_constrained_column() {
        let experiment_lengths = [58, 56, 54, 52];
        let hypothesis_lengths = [18, 16, 14, 12];
        let (experiment_chars, hypothesis_chars) =
            best_metric_table_title_split(&experiment_lengths, &hypothesis_lengths, 52);
        assert!(experiment_chars > hypothesis_chars);
        assert!(hypothesis_chars >= METRIC_TABLE_TITLE_MIN_BUDGET_CH);
        let truncated_entries = truncated_entry_count(&experiment_lengths, experiment_chars)
            + truncated_entry_count(&hypothesis_lengths, hypothesis_chars);
        assert_eq!(truncated_entries, 4);
    }

    #[test]
    fn best_metric_table_title_split_preserves_minimum_widths() {
        let (experiment_chars, hypothesis_chars) =
            best_metric_table_title_split(&[120, 100], &[120, 100], 24);
        assert_eq!(experiment_chars + hypothesis_chars, 24);
        assert_eq!(experiment_chars, 12);
        assert_eq!(hypothesis_chars, 12);
    }

    #[test]
    fn best_metric_table_title_split_penalizes_one_sided_starvation() {
        let experiment_lengths = [62, 60, 58, 56, 54, 52];
        let hypothesis_lengths = [34, 33, 32, 31, 30, 29];
        let (experiment_chars, hypothesis_chars) =
            best_metric_table_title_split(&experiment_lengths, &hypothesis_lengths, 74);
        assert!(experiment_chars <= 45);
        assert!(hypothesis_chars >= 29);
    }

    #[test]
    fn resolve_selected_metric_keys_allows_two_unit_families() {
        let visible_metrics = vec![
            test_metric("presolve_ms", "ms"),
            test_metric("presolve_nz", "count"),
            test_metric("report_bytes", "bytes"),
            test_metric("presolve_ms_gmean", "ms"),
            test_metric("presolve_rows", "count"),
        ];
        let selected = resolve_selected_metric_keys(
            &[
                "presolve_ms".to_owned(),
                "presolve_nz".to_owned(),
                "report_bytes".to_owned(),
                "presolve_ms_gmean".to_owned(),
                "presolve_rows".to_owned(),
            ],
            &visible_metrics,
        );
        assert_eq!(
            selected
                .iter()
                .map(|metric| metric.key.as_str())
                .collect::<Vec<_>>(),
            vec![
                "presolve_ms",
                "presolve_nz",
                "presolve_ms_gmean",
                "presolve_rows"
            ]
        );
    }

    #[test]
    fn metric_chart_axis_normalizes_time_units_into_primary_unit() {
        let axis = MetricChartAxis::from_metric(&test_metric("presolve_ms", "ms"));
        let seconds = must(MetricUnit::new("seconds"), "seconds unit");
        assert_eq!(axis.normalize_value(1.5, &seconds), Some(1500.0));
    }

    #[test]
    fn frontier_page_query_accepts_result_metric_selector() {
        let query = must(
            FrontierPageQuery::parse(Some("tab=results&metric=presolve_ms_gmean")),
            "query should parse",
        );
        assert_eq!(query.tab.as_deref(), Some("results"));
        assert_eq!(query.metric, vec!["presolve_ms_gmean".to_owned()]);
    }

    #[test]
    fn frontier_tab_defaults_to_results() {
        assert_eq!(FrontierTab::from_query(None), FrontierTab::Results);
        assert_eq!(
            FrontierTab::from_query(Some("unknown")),
            FrontierTab::Results
        );
        assert_eq!(FrontierTab::from_query(Some("brief")), FrontierTab::Brief);
    }

    #[test]
    fn frontier_page_query_accepts_repeated_metric_selectors() {
        let query = FrontierPageQuery::parse(Some(
            "metric=presolve_ms&metric=ingress_ms_gmean&table_metric=ingress_ms_gmean&log_y=1",
        ));
        let query = must(query, "query should parse");
        assert_eq!(
            query.metric,
            vec!["presolve_ms".to_owned(), "ingress_ms_gmean".to_owned()]
        );
        assert_eq!(query.table_metric.as_deref(), Some("ingress_ms_gmean"));
        assert!(query.log_y_requested());
    }

    #[test]
    fn metric_table_indices_follow_chart_close_order_with_gaps() {
        let frontier = test_frontier();
        let hypothesis_one = test_hypothesis(frontier.id, "hyp-one", "Hypothesis One");
        let hypothesis_two = test_hypothesis(frontier.id, "hyp-two", "Hypothesis Two");
        let metric_a = test_metric("presolve_ms", "ms");
        let metric_b = test_metric("presolve_nz", "count");
        let series = vec![
            FrontierMetricSeries {
                frontier: frontier.clone(),
                metric: metric_a.clone(),
                kpi: None,
                points: vec![
                    test_metric_point(
                        frontier.id,
                        &hypothesis_one,
                        "exp-a",
                        "Experiment A",
                        10.0,
                        test_timestamp("2026-04-11T01:00:00Z"),
                    ),
                    test_metric_point(
                        frontier.id,
                        &hypothesis_one,
                        "exp-c",
                        "Experiment C With A Long Full Title Kept In The DOM",
                        30.0,
                        test_timestamp("2026-04-11T03:00:00Z"),
                    ),
                ],
            },
            FrontierMetricSeries {
                frontier: frontier.clone(),
                metric: metric_b.clone(),
                kpi: None,
                points: vec![
                    test_metric_point(
                        frontier.id,
                        &hypothesis_one,
                        "exp-a",
                        "Experiment A",
                        100.0,
                        test_timestamp("2026-04-11T01:00:00Z"),
                    ),
                    test_metric_point(
                        frontier.id,
                        &hypothesis_two,
                        "exp-b",
                        "Experiment B",
                        200.0,
                        test_timestamp("2026-04-11T02:00:00Z"),
                    ),
                    test_metric_point(
                        frontier.id,
                        &hypothesis_one,
                        "exp-c",
                        "Experiment C With A Long Full Title Kept In The DOM",
                        300.0,
                        test_timestamp("2026-04-11T03:00:00Z"),
                    ),
                ],
            },
        ];
        let selected_metrics = vec![metric_a.clone(), metric_b];
        let markup = render_metric_series_section(
            &frontier.slug,
            &selected_metrics,
            &[],
            &selected_metrics,
            &series,
            &BTreeMap::new(),
            false,
            Some(metric_a.key.as_str()),
            None,
        )
        .into_string();
        let rank_cell_one = "<td class=\"metric-table-rank-cell\"><span class=\"metric-table-fixed-text\">1</span></td>";
        let rank_cell_two = "<td class=\"metric-table-rank-cell\"><span class=\"metric-table-fixed-text\">2</span></td>";
        let rank_cell_three = "<td class=\"metric-table-rank-cell\"><span class=\"metric-table-fixed-text\">3</span></td>";
        assert!(markup.contains(rank_cell_one));
        assert!(markup.contains(rank_cell_three));
        assert!(!markup.contains(rank_cell_two));
        assert!(markup.contains("id=\"metric-selection-popout\""));
        assert!(markup.contains("id=\"metric-filter-popout\""));
        assert!(markup.contains("data-preserve-viewport=\"true\""));
        assert!(markup.contains("data-copy-plot-png=\"true\""));
        assert!(!markup.contains("plot-copy-status"));
        assert!(markup.contains("metric-table-fit-col"));
        assert!(markup.contains("metric-table-title-col"));
        assert!(markup.contains("presolve_nz"));
        assert!(markup.contains("count"));
        assert!(!markup.contains("chart render failed"));
        assert!(markup.contains("Experiment C With A Long Full Title Kept In The DOM"));
        assert!(!markup.contains("Experiment C With A Long Full Title..."));
        assert!(markup.contains("table_metric=presolve%5Fms"));
        assert!(markup.contains("class=\"metric-table-tab active\""));
        let rank_three_offset = markup.find(rank_cell_three);
        let rank_one_offset = markup.find(rank_cell_one);
        assert!(
            matches!((rank_three_offset, rank_one_offset), (Some(left), Some(right)) if left < right)
        );
    }
}
