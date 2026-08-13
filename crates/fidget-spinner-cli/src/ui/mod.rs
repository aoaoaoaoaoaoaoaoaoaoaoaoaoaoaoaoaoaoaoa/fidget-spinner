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
    FrontierVerdict, HypothesisAssessmentLevel, HypothesisAttention, KnownMetricUnit,
    MetricAggregation, MetricDimension, MetricDisplayUnit, MetricUnit, NonEmptyText,
    OptimizationObjective, RegistryLockMode, RegistryName, ReportedMetricValue, RunDimensionValue,
    Slug, SyntheticMetricExpression, TagFamilyName, TagName, VertexRef,
};
use fidget_spinner_store_sqlite::{
    AssignTagFamilyRequest, CreateKpiRequest, CreateTagFamilyRequest, DefineMetricRequest,
    DefineSyntheticMetricRequest, DeleteKpiReferenceRequest, DeleteKpiRequest, DeleteMetricRequest,
    DeleteTagRequest, ExperimentDetail, ExperimentOutcomePatch, ExperimentSummary,
    FrontierOpenProjection, FrontierSummary, HypothesisDetail, KpiSummary, ListFrontiersQuery,
    MergeMetricRequest, MergeTagRequest, MetricKeySummary, MetricKeysQuery, MetricScope,
    MoveKpiDirection, MoveKpiRequest, ProjectStatus, RenameMetricRequest, RenameTagRequest,
    STATE_DB_NAME, ScuffExperimentRequest, SetFrontierRegistryLockRequest, SetKpiReferenceRequest,
    SetRegistryLockRequest, SetTagFamilyMandatoryRequest, StoreError, TextPatch,
    UpdateExperimentRequest, UpdateFrontierRequest, UpdateHypothesisRequest, UpdateProjectRequest,
    VertexSummary, list_project_manifests, project_state_home,
};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use pulldown_cmark::html::push_html;
use pulldown_cmark::{Event, Options, Parser};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::open_store;
mod assets;
mod chart;
mod detail;
mod number;
mod registry;
mod results;
mod routes;

use assets::harden_autofill_controls;

use routes::ProjectDescriptionForm;
pub(crate) use routes::serve;

const FAVICON_SVG: &str = include_str!("../../../../assets/ui/favicon.svg");
const UI_NAV_STATE_KEY: &str = "fidget-spinner-ui-nav-state";

#[derive(Clone)]
struct NavigatorState {
    limit: Option<u32>,
    chart_cache: chart::SharedChartSceneCache,
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
    refresh_token: String,
}

#[derive(Clone)]
struct ProjectRenderContext {
    project_root: Utf8PathBuf,
    base_href: String,
    project_home_href: String,
    refresh_token_href: String,
    refresh_token: String,
    chart_cache: chart::SharedChartSceneCache,
    limit: Option<u32>,
}

impl ProjectRenderContext {
    fn nested(
        project_root: Utf8PathBuf,
        refresh_token: String,
        chart_cache: chart::SharedChartSceneCache,
        limit: Option<u32>,
    ) -> Self {
        let base_href = project_base_href(&project_root);
        Self {
            project_root,
            refresh_token_href: format!("{base_href}refresh-token"),
            refresh_token,
            chart_cache,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrontierTab {
    Brief,
    Experiments,
    Open,
    Closed,
    Results,
}

#[derive(Clone, Debug, Default)]
struct FrontierPageQuery {
    metric: Vec<String>,
    hidden_metric: Vec<String>,
    metric_selection_explicit: bool,
    table_metric: Option<String>,
    plot_from: Option<String>,
    plot_to: Option<String>,
    tab: Option<String>,
    extra: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MetricAxisLogScales {
    primary: bool,
    secondary: bool,
}

#[derive(Clone, Debug, Default)]
struct ProjectMetricsQuery {
    frontier: Option<String>,
    page: u32,
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
            Some("experiments") => Self::Experiments,
            Some("open") => Self::Open,
            Some("closed") => Self::Closed,
            _ => Self::Results,
        }
    }

    const fn as_query(self) -> &'static str {
        match self {
            Self::Brief => "brief",
            Self::Experiments => "experiments",
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Results => "results",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Brief => "Brief",
            Self::Experiments => "Experiments",
            Self::Open => "Worklist",
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
            match key.as_str() {
                "frontier" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.frontier = Some(trimmed.to_owned());
                    }
                }
                "page" => {
                    query.page = value
                        .parse::<u32>()
                        .map_err(|error| {
                            StoreError::InvalidInput(format!(
                                "invalid metric page `{value}`: {error}"
                            ))
                        })?
                        .saturating_sub(1);
                }
                _ => {}
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
                "hidden_metric" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.hidden_metric.push(trimmed.to_owned());
                    }
                }
                "metric_mode" => {
                    query.metric_selection_explicit = value.trim() == "explicit";
                }
                "table_metric" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.table_metric = Some(trimmed.to_owned());
                    }
                }
                "plot_from" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.plot_from = Some(trimmed.to_owned());
                    }
                }
                "plot_to" => {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        query.plot_to = Some(trimmed.to_owned());
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

    fn requested_log_scales(&self) -> MetricAxisLogScales {
        MetricAxisLogScales {
            primary: query_flag_enabled(&self.extra, "log_y_primary"),
            secondary: query_flag_enabled(&self.extra, "log_y_secondary"),
        }
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

    fn chart_selection(&self) -> chart::ChartSelection {
        chart::ChartSelection {
            metric_selection_explicit: self.metric_selection_explicit,
            hidden_metrics: self.hidden_metric.iter().cloned().collect(),
            conditions: self.condition_filters(),
            window: chart::ChartWindowRequest {
                from: self.plot_from.clone(),
                to: self.plot_to.clone(),
            },
            logarithmic: self.requested_log_scales(),
        }
    }
}

fn query_flag_enabled(flags: &BTreeMap<String, String>, key: &str) -> bool {
    flags
        .get(key)
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "on" | "yes"))
}

fn render_response(result: Result<Markup, StoreError>) -> Response {
    match result {
        Ok(markup) => Html(harden_autofill_controls(&markup.into_string())).into_response(),
        Err(
            StoreError::UnknownFrontierSelector(_)
            | StoreError::UnknownHypothesisSelector(_)
            | StoreError::UnknownExperimentSelector(_),
        ) => (StatusCode::NOT_FOUND, "not found".to_owned()).into_response(),
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
            "frontier changed before the update landed; reload and retry".to_owned(),
        )
            .into_response(),
        Err(StoreError::UnknownFrontierSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(StoreError::InvalidInput(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("frontier update failed: {error}"),
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
        Err(StoreError::UnknownTag(_) | StoreError::UnknownTagFamily(_)) => {
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
        Err(
            StoreError::UnknownMetricDefinition(_)
            | StoreError::UnknownKpi(_)
            | StoreError::UnknownKpiReference(_)
            | StoreError::UnknownFrontierSelector(_),
        ) => (StatusCode::NOT_FOUND, "not found".to_owned()).into_response(),
        Err(StoreError::DuplicateMetricDefinition(_) | StoreError::DuplicateKpi(_)) => {
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

fn hypothesis_mutation_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err(StoreError::UnknownHypothesisSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(StoreError::RevisionMismatch { .. }) => (
            StatusCode::CONFLICT,
            "hypothesis changed before the edit landed; reload and retry".to_owned(),
        )
            .into_response(),
        Err(StoreError::WorkingHypothesisCannotBeShelved { hypothesis }) => (
            StatusCode::CONFLICT,
            format!("hypothesis `{hypothesis}` has open experiments and cannot be closed"),
        )
            .into_response(),
        Err(StoreError::HypothesisBodyMustBeSingleParagraph) => (
            StatusCode::BAD_REQUEST,
            "hypothesis body must stay a single paragraph".to_owned(),
        )
            .into_response(),
        Err(StoreError::InvalidInput(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("hypothesis state update failed: {error}"),
        )
            .into_response(),
    }
}

fn experiment_mutation_response(result: Result<String, StoreError>) -> Response {
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err(StoreError::UnknownExperimentSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(StoreError::RevisionMismatch { .. }) => (
            StatusCode::CONFLICT,
            "experiment changed before the edit landed; reload and retry".to_owned(),
        )
            .into_response(),
        Err(StoreError::ExperimentStillOpen(_)) => (
            StatusCode::CONFLICT,
            "open experiments must be closed before they can be retroactively scuffed".to_owned(),
        )
            .into_response(),
        Err(StoreError::InvalidInput(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("experiment prose update failed: {error}"),
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
        "ratio" | "dimensionless" | "scalar" => Ok(MetricDimension::Dimensionless),
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
    let store = open_store(project_root.as_std_path())?;
    let project_root = store.status()?.project_root;
    let refresh_token = refresh_file_token(&store.state_root().join(STATE_DB_NAME))?;
    Ok(ProjectRenderContext::nested(
        project_root,
        refresh_token,
        state.chart_cache.clone(),
        state.limit,
    ))
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
    context: &ProjectRenderContext,
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
        refresh_token: context.refresh_token.clone(),
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
                    (frontier.worklist_hypothesis_count) " worklist · "
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

fn scuff_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            circle cx="12" cy="12" r="7.5" {}
            path d="M7 17 17 7" {}
        }
    }
}

fn chevron_up_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M6.5 14.5 12 9l5.5 5.5" {}
        }
    }
}

fn chevron_down_icon() -> Markup {
    html! {
        svg.inline-action-icon aria-hidden="true" viewBox="0 0 24 24" fill="none" {
            path d="M6.5 9.5 12 15l5.5-5.5" {}
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
        RunDimensionValue::String(value) | RunDimensionValue::Timestamp(value) => value.to_string(),
        RunDimensionValue::Numeric(value) => format_float(*value),
        RunDimensionValue::Boolean(value) => value.to_string(),
    }
}

trait MetricValueUnit {
    fn known_unit(&self) -> Option<MetricUnit>;
    fn label(&self) -> String;
}

impl MetricValueUnit for MetricUnit {
    fn known_unit(&self) -> Option<MetricUnit> {
        self.known_kind()
    }

    fn label(&self) -> String {
        self.as_str().to_owned()
    }
}

impl MetricValueUnit for MetricDisplayUnit {
    fn known_unit(&self) -> Option<MetricUnit> {
        match self {
            Self::Known(unit) => unit.known_kind(),
            Self::Canonical(_) => None,
        }
    }

    fn label(&self) -> String {
        MetricDisplayUnit::label(self)
    }
}

fn format_metric_value(value: f64, unit: &impl MetricValueUnit) -> String {
    match unit.known_unit() {
        Some(KnownMetricUnit::Bytes) => format!("{} B", format_integerish(value)),
        Some(KnownMetricUnit::Kibibytes) => {
            format!("{} KiB", number::format_significant(value, 2))
        }
        Some(KnownMetricUnit::Mebibytes) => {
            format!("{} MiB", number::format_significant(value, 2))
        }
        Some(KnownMetricUnit::Gibibytes) => {
            format!("{} GiB", number::format_significant(value, 2))
        }
        Some(KnownMetricUnit::Seconds) => {
            format!("{} s", number::format_significant(value, 3))
        }
        Some(KnownMetricUnit::Milliseconds) => {
            format!("{} ms", number::format_significant(value, 3))
        }
        Some(KnownMetricUnit::Microseconds) => format!("{} us", format_integerish(value)),
        Some(KnownMetricUnit::Nanoseconds) => format!("{} ns", format_integerish(value)),
        Some(KnownMetricUnit::Count) => format_integerish(value),
        Some(KnownMetricUnit::Percent) => {
            format!("{}%", number::format_significant(value, 2))
        }
        Some(KnownMetricUnit::Dimensionless) | None => {
            let label = unit.label();
            if label == "dimensionless" {
                format_float(value)
            } else {
                format!("{} {label}", format_float(value))
            }
        }
    }
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format_integerish(value)
    } else {
        number::format_significant(value, 4)
    }
}

fn format_integerish(value: f64) -> String {
    if value.fract() != 0.0 {
        return number::format_significant(value, 0);
    }
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
        .map(std::borrow::Cow::into_owned)
        .map_err(|error| StoreError::InvalidInput(format!("invalid query string: {error}")))
}

fn frontier_href(slug: &Slug) -> String {
    format!("frontier/{}", encode_path_segment(slug.as_str()))
}

fn frontier_results_href(slug: &Slug) -> String {
    frontier_tab_href(
        slug,
        FrontierTab::Results,
        &[],
        MetricAxisLogScales::default(),
        None,
    )
}

fn project_metrics_frontier_href(slug: &Slug) -> String {
    format!("metrics?frontier={}", encode_path_segment(slug.as_str()))
}

fn metric_choice_detail(metric: &MetricKeySummary) -> String {
    format!(
        "{} · {} · {} · {} · {}",
        metric.kind.as_str(),
        metric.objective.as_str(),
        metric.dimension,
        metric.display_unit.label(),
        metric.aggregation.as_str()
    )
}

fn metric_is_synthetic(metric: &MetricKeySummary) -> bool {
    metric.kind.as_str() == "synthetic"
}

fn render_metric_kind_chip(metric: &MetricKeySummary) -> Markup {
    html! {
        @if metric_is_synthetic(metric) {
            span.metric-kind-chip title="Synthetic metric" { "SYNTH" }
        }
    }
}

fn frontier_tab_href(
    slug: &Slug,
    tab: FrontierTab,
    selected_metrics: &[MetricKeySummary],
    log_scales: MetricAxisLogScales,
    table_metric: Option<&str>,
) -> String {
    frontier_tab_href_with_query(
        slug,
        tab,
        selected_metrics,
        log_scales,
        &BTreeMap::new(),
        table_metric,
        None,
    )
}

fn frontier_tab_href_with_query(
    slug: &Slug,
    tab: FrontierTab,
    selected_metrics: &[MetricKeySummary],
    log_scales: MetricAxisLogScales,
    condition_filters: &BTreeMap<String, String>,
    table_metric: Option<&str>,
    chart_selection: Option<&chart::ChartSelection>,
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
    if log_scales.primary {
        href.push_str("&log_y_primary=1");
    }
    if log_scales.secondary {
        href.push_str("&log_y_secondary=1");
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
    if let Some(selection) = chart_selection {
        if selection.metric_selection_explicit {
            href.push_str("&metric_mode=explicit");
        }
        for metric in &selection.hidden_metrics {
            href.push_str("&hidden_metric=");
            href.push_str(&encode_path_segment(metric));
        }
        if let Some(from) = selection
            .window
            .from
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            href.push_str("&plot_from=");
            href.push_str(&encode_path_segment(from));
        }
        if let Some(to) = selection
            .window
            .to
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            href.push_str("&plot_to=");
            href.push_str(&encode_path_segment(to));
        }
    }
    href
}

fn hypothesis_href(slug: &Slug) -> String {
    format!("hypothesis/{}", encode_path_segment(slug.as_str()))
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

fn hypothesis_attention_label(attention: HypothesisAttention) -> &'static str {
    match attention {
        HypothesisAttention::Worklist => "active",
        HypothesisAttention::Shelved => "closed",
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
        FrontierVerdict::Scuffed => "status-archived",
    }
}

fn render_hypothesis_meta_chips(
    expected_yield: HypothesisAssessmentLevel,
    confidence: HypothesisAssessmentLevel,
    tags: &[TagName],
) -> Markup {
    html! {
        div.chip-row {
            span.kind-chip title="Expected KPI-moving yield vibe check" {
                "yield " (expected_yield.as_str())
            }
            span.kind-chip title="Confidence vibe check for the hypothesis" {
                "confidence " (confidence.as_str())
            }
            @for tag in tags {
                span.tag-chip { (tag) }
            }
        }
    }
}

fn render_markdown_prose(source: &str) -> Markup {
    html! {
        div.prose.markdown-prose {
            (PreEscaped(markdown_html(source)))
        }
    }
}

fn markdown_html(source: &str) -> String {
    let parser = Parser::new_ext(source, markdown_options()).map(|event| match event {
        Event::Html(raw_html) | Event::InlineHtml(raw_html) => Event::Text(raw_html),
        event => event,
    });
    let mut html = String::new();
    push_html(&mut html, parser);
    html
}

fn markdown_options() -> Options {
    Options::ENABLE_TABLES
        | Options::ENABLE_FOOTNOTES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_SMART_PUNCTUATION
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
    use super::assets::{harden_autofill_controls, styles};
    use super::registry::{
        metric_registry_filter_text, render_kpi_registry, render_metric_registry_table,
    };
    use super::results::resolve_selected_metric_keys;
    use super::{
        FrontierPageQuery, FrontierTab, NavigatorState, ProjectMetricsQuery, StoreError,
        Utf8PathBuf, encode_path_segment, format_metric_value, markdown_html,
        resolve_project_context,
    };
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::fs;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    use fidget_spinner_core::{
        DefaultVisibility, FrontierBrief, FrontierId, FrontierRecord, FrontierStatus,
        FrontierVerdict, KpiId, KpiOrdinal, KpiReferenceId, KpiReferenceOrdinal, MetricAggregation,
        MetricDefinitionKind, MetricDisplayUnit, MetricUnit, NonEmptyText, OptimizationObjective,
        Slug,
    };
    use fidget_spinner_store_sqlite::{
        FrontierSummary, KpiReferenceSummary, KpiSummary, MetricKeySummary, ProjectStore,
    };
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;

    static TEST_STATE_HOME: OnceLock<Result<Utf8PathBuf, String>> = OnceLock::new();
    static TEST_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[allow(clippy::panic, reason = "test constructors should fail loudly")]
    fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error}"),
        }
    }

    fn ensure_test_state_home() -> Result<(), Box<dyn Error>> {
        let state_home = TEST_STATE_HOME
            .get_or_init(|| {
                let root = std::env::temp_dir()
                    .join(format!("fidget-spinner-cli-state-{}", std::process::id()));
                fs::create_dir_all(&root)
                    .map_err(|error| error.to_string())
                    .map(|()| Utf8PathBuf::from(root.to_string_lossy().into_owned()))
            })
            .as_ref()
            .map_err(Clone::clone)?
            .clone();
        fidget_spinner_store_sqlite::install_state_home_override(state_home)?;
        Ok(())
    }

    fn fresh_temp_root(label: &str) -> Result<Utf8PathBuf, Box<dyn Error>> {
        ensure_test_state_home()?;
        loop {
            let sequence = TEST_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "fidget-spinner-ui-{label}-{}-{sequence}",
                std::process::id()
            ));
            match fs::create_dir(&root) {
                Ok(()) => {
                    return Ok(fidget_spinner_store_sqlite::canonical_project_root(
                        &Utf8PathBuf::from(root.to_string_lossy().into_owned()),
                    )?);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn test_metric(key: &str, unit: &str) -> MetricKeySummary {
        let unit = must(MetricUnit::new(unit), "metric unit");
        MetricKeySummary {
            key: must(NonEmptyText::new(key.to_owned()), "metric key"),
            dimension: unit.quantity(),
            display_unit: MetricDisplayUnit::Known(unit),
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Minimize,
            kind: MetricDefinitionKind::Observed,
            default_visibility: DefaultVisibility::visible(),
            description: None,
            reference_count: 0,
        }
    }

    #[test]
    fn metric_tables_preserve_small_measurements() {
        assert_eq!(
            format_metric_value(0.000_123, &MetricUnit::Seconds),
            "0.000123 s"
        );
        assert_eq!(
            format_metric_value(1.234_5, &MetricUnit::Microseconds),
            "1.23 us"
        );
    }

    #[test]
    fn explicit_initialized_project_url_resolves_context() -> Result<(), Box<dyn Error>> {
        let project_root = fresh_temp_root("outside-project")?;
        drop(ProjectStore::init(
            &project_root,
            NonEmptyText::new("Outside".to_owned())?,
        )?);
        let state = NavigatorState {
            limit: None,
            chart_cache: super::chart::SharedChartSceneCache::default(),
        };

        let context = resolve_project_context(&state, &encode_path_segment(project_root.as_str()))?;

        assert_eq!(context.project_root, project_root);
        Ok(())
    }

    fn test_synthetic_metric(key: &str, unit: &str) -> MetricKeySummary {
        MetricKeySummary {
            kind: MetricDefinitionKind::Synthetic,
            ..test_metric(key, unit)
        }
    }

    fn test_kpi(metric: MetricKeySummary) -> KpiSummary {
        KpiSummary {
            id: KpiId::fresh(),
            ordinal: KpiOrdinal::FIRST,
            metric,
            references: Vec::new(),
        }
    }

    fn test_timestamp(raw: &str) -> OffsetDateTime {
        must(OffsetDateTime::parse(raw, &Rfc3339), "timestamp")
    }

    #[test]
    fn autofill_hardening_marks_visible_form_controls_once() {
        let document = r#"<form method="post"><input type="text" name="tag"><select name="family"></select><textarea name="body"></textarea><input type="hidden" name="revision"></form>"#;
        let hardened = harden_autofill_controls(document);
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

        let rehardened = harden_autofill_controls(&hardened);
        assert_eq!(rehardened.matches(r#"autocomplete="off""#).count(), 4);
        assert_eq!(
            rehardened
                .matches(r#"data-protonpass-ignore="true""#)
                .count(),
            3
        );
    }

    #[test]
    fn markdown_prose_renders_commonmark_and_escapes_raw_html() {
        let rendered = markdown_html("A **bold** point.\n\n- `code`\n\n<script>alert(1)</script>");

        assert!(rendered.contains("<strong>bold</strong>"));
        assert!(rendered.contains("<li><code>code</code></li>"));
        assert!(rendered.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(!rendered.contains("<script>"));
    }

    #[test]
    fn stylesheet_codifies_text_containment_contract() {
        let css = styles();
        assert!(css.contains("minmax(min(100%, 320px), 1fr)"));
        assert!(css.contains("minmax(min(100%, 260px), 1fr)"));
        assert!(css.contains("overflow-wrap: anywhere"));
        assert!(css.contains(".status-chip {\n        text-transform: uppercase;"));
        assert!(css.contains("white-space: nowrap;\n        overflow-wrap: normal;"));
        assert!(css.contains(".metric-create-stack {\n        display: grid;"));
        assert!(!css.contains("minmax(320px, 1fr)"));
        assert!(!css.contains("minmax(260px, 1fr)"));
        assert!(!css.contains("overflow-x: hidden;\n    }\n    a"));
        assert!(css.contains(".control-popout:not([open]) > .control-popout-panel"));
        assert!(css.contains(".frontier-summary-editor[open]"));
        assert!(css.contains(".chart-frame svg {\n            width: 780px;"));
    }

    #[test]
    fn metric_registry_query_uses_one_based_public_pages() -> Result<(), StoreError> {
        let query = ProjectMetricsQuery::parse(Some("frontier=the-hill&page=3"))?;
        assert_eq!(query.frontier.as_deref(), Some("the-hill"));
        assert_eq!(query.page, 2);
        assert_eq!(ProjectMetricsQuery::parse(Some("page=0"))?.page, 0);
        assert!(ProjectMetricsQuery::parse(Some("page=none")).is_err());
        Ok(())
    }

    #[test]
    fn metric_registry_table_exposes_reactive_filter_hooks() {
        let metrics = vec![
            test_metric("presolve_wallclock", "milliseconds"),
            test_synthetic_metric("presolve_wallclock_per_row", "milliseconds"),
        ];
        let frontier = test_frontier_summary();
        let kpi = test_kpi(metrics[0].clone());
        let markup = render_metric_registry_table(
            &metrics,
            &metrics,
            Some(&frontier),
            &[kpi],
            None,
            0,
            0,
            metrics.len(),
        )
        .into_string();
        let filter_text = metric_registry_filter_text(&metrics[0]);

        assert!(markup.contains(r#"data-table-filter-input="metric-registry""#));
        assert!(markup.contains(r#"data-table-filter-row="metric-registry""#));
        assert!(markup.contains(r#"data-table-filter-empty="metric-registry" hidden"#));
        assert_eq!(filter_text, "presolve_wallclock time minimize ");
        assert!(!markup.contains("<th>Shape</th>"));
        assert!(!markup.contains(r#"aria-label="Aggregation""#));
        assert!(markup.contains(r#"class="tag-create-form metric-create-form""#));
        assert!(markup.contains(
            r#"class="tag-create-form metric-create-form synthetic-metric-create-form""#
        ));
        assert!(markup.contains(r#"class="metric-objective-chip metric-objective-minimize""#));
        assert!(
            markup.contains(r#"class="metric-kind-chip" title="Synthetic metric">SYNTH</span>"#)
        );
        assert!(markup.contains(r#"datalist id="metric-choices""#));
        assert!(markup.contains(r#"data-synthetic-operation-select="true""#));
        assert!(markup.contains("data-synthetic-gmean-extra"));
        assert!(markup.contains(r#"placeholder="Extra gmean term 3""#));
        assert!(!markup.contains(">optional</option>"));
        assert!(markup.contains(r#"title="synthetic · minimize · time · milliseconds · point""#));
        assert!(markup.contains(
            r#"<option value="presolve_wallclock_per_row" title="synthetic · minimize · time · milliseconds · point"></option>"#
        ));
        assert!(!markup.contains("SYNTH · presolve_wallclock_per_row"));
        assert!(!markup.contains(">BASE</span>"));
        assert!(markup.contains(">MIN</span>"));
        assert!(markup.contains(r#"<td class="no-truncate">time</td>"#));
        assert!(markup.contains(r#"action="metrics/description""#));
        assert!(markup.contains(r#"data-inline-edit-allow-clear="true""#));
        assert!(markup.contains(r#"class="metric-identity-stack""#));
        assert!(markup.contains(r#"class="tag-inline-rename-form metric-description-form""#));
        assert!(markup.contains(r#"title="Already a KPI for selected frontier" disabled"#));
        assert!(markup.contains(r#"title="Promote metric to KPI""#));
        assert!(markup.contains(r#"class="inline-icon-button promote-icon-button""#));
        assert!(markup.contains(r#"d="M6.5 14.5 12 9l5.5 5.5""#));
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

    fn test_frontier_summary() -> FrontierSummary {
        let frontier = test_frontier();
        FrontierSummary {
            id: frontier.id,
            slug: frontier.slug,
            label: frontier.label,
            objective: frontier.objective,
            status: frontier.status,
            worklist_hypothesis_count: 0,
            open_experiment_count: 0,
            revision: frontier.revision,
            updated_at: frontier.updated_at,
        }
    }

    #[test]
    fn kpi_registry_renders_references_as_sibling_rows() {
        let frontier = test_frontier_summary();
        let mut metric = test_synthetic_metric("post_native_ingress_wallclock", "milliseconds");
        metric.description = Some(must(
            NonEmptyText::new(
                "Wallclock after native ingress/presolve, computed as total solve elapsed minus native ingress elapsed.".to_owned(),
            ),
            "metric description",
        ));
        metric.reference_count = 4;
        let timestamp = test_timestamp("2026-04-11T01:00:00Z");
        let reference = KpiReferenceSummary {
            id: KpiReferenceId::fresh(),
            ordinal: KpiReferenceOrdinal::FIRST,
            label: must(NonEmptyText::new("highs-owner-4x5"), "reference label"),
            value: 3418.847,
            canonical_value: 3_418_847_000.0,
            display_unit: metric.display_unit.clone(),
            created_at: timestamp,
            updated_at: timestamp,
        };
        let kpi = KpiSummary {
            id: KpiId::fresh(),
            ordinal: KpiOrdinal::FIRST,
            metric,
            references: vec![reference],
        };
        let markup = render_kpi_registry(&frontier, &[kpi]).into_string();

        assert!(markup.contains(r#"<tr class="kpi-reference-row">"#));
        assert!(markup.contains(r#"<td class="kpi-reference-lane" colspan="3">"#));
        assert!(markup.contains(r#"<div class="kpi-description muted">"#));
        assert!(markup.contains("highs-owner-4x5"));
        assert!(
            markup.contains(r#"class="metric-kind-chip" title="Synthetic metric">SYNTH</span>"#)
        );
        assert!(markup.contains(r#"title="Demote KPI metric""#));
        assert!(markup.contains(r#"d="M6.5 9.5 12 15l5.5-5.5""#));
        assert!(!markup.contains("<th>Shape</th>"));
        assert!(!markup.contains("<th>Reference Lines</th>"));
    }

    fn chart_experiment(
        slug: &str,
        title: &str,
        verdict: FrontierVerdict,
    ) -> fidget_spinner_store_sqlite::FrontierChartExperiment {
        fidget_spinner_store_sqlite::FrontierChartExperiment {
            id: fidget_spinner_core::ExperimentId::fresh(),
            slug: must(Slug::new(slug), "experiment slug"),
            title: must(NonEmptyText::new(title), "experiment title"),
            hypothesis_slug: must(Slug::new(format!("{slug}-hypothesis")), "hypothesis slug"),
            hypothesis_title: must(
                NonEmptyText::new(format!("{title} hypothesis")),
                "hypothesis title",
            ),
            verdict,
            closed_at: test_timestamp("2026-04-11T01:00:00Z"),
            dimensions: BTreeMap::new(),
        }
    }

    fn chart_scene(
        metric: MetricKeySummary,
        experiments: Vec<fidget_spinner_store_sqlite::FrontierChartExperiment>,
        canonical_values: Vec<Option<f64>>,
    ) -> fidget_spinner_store_sqlite::FrontierChartScene {
        let frontier = test_frontier();
        fidget_spinner_store_sqlite::FrontierChartScene {
            frontier_id: frontier.id,
            frontier_slug: frontier.slug,
            experiments,
            series: vec![fidget_spinner_store_sqlite::FrontierChartSeries {
                metric,
                kpi: None,
                canonical_values,
            }],
        }
    }

    #[test]
    fn resolve_selected_metrics_admits_only_two_quantities_without_substitution() {
        let time = test_metric("elapsed", "milliseconds");
        let count = test_metric("nodes", "count");
        let bytes = test_metric("memory", "bytes");
        let requested = vec![
            "elapsed".to_owned(),
            "nodes".to_owned(),
            "memory".to_owned(),
            "missing".to_owned(),
        ];
        let selected =
            resolve_selected_metric_keys(&requested, &[time.clone(), count.clone(), bytes]);
        assert_eq!(selected, vec![time, count]);
        assert!(resolve_selected_metric_keys(&[], &selected).is_empty());
    }

    #[test]
    fn frontier_page_query_preserves_explicit_empty_hidden_and_window_state() {
        let query = must(
            FrontierPageQuery::parse(Some(
                "tab=results&metric_mode=explicit&hidden_metric=elapsed&hidden_metric=nodes&plot_from=exp-a&plot_to=exp-z&condition.compiler=clang&log_y_primary=1",
            )),
            "query should parse",
        );
        assert!(query.metric_selection_explicit);
        assert!(query.metric.is_empty());
        assert_eq!(query.hidden_metric, vec!["elapsed", "nodes"]);
        assert_eq!(query.plot_from.as_deref(), Some("exp-a"));
        assert_eq!(query.plot_to.as_deref(), Some("exp-z"));
        assert_eq!(
            query
                .condition_filters()
                .get("compiler")
                .map(String::as_str),
            Some("clang")
        );
        assert!(query.requested_log_scales().primary);
    }

    #[test]
    fn frontier_tab_defaults_to_results() {
        assert_eq!(FrontierTab::from_query(None), FrontierTab::Results);
        assert_eq!(
            FrontierTab::from_query(Some("unknown")),
            FrontierTab::Results
        );
        assert_eq!(FrontierTab::from_query(Some("brief")), FrontierTab::Brief);
        assert_eq!(FrontierTab::Open.label(), "Worklist");
    }

    #[test]
    fn chart_marker_shape_respects_verdict_semantics() {
        use super::chart::{ChartPointMarker, point_marker};

        assert_eq!(
            point_marker(FrontierVerdict::Accepted),
            ChartPointMarker::Circle
        );
        assert_eq!(
            point_marker(FrontierVerdict::Kept),
            ChartPointMarker::Circle
        );
        assert_eq!(
            point_marker(FrontierVerdict::Parked),
            ChartPointMarker::Triangle
        );
        assert_eq!(
            point_marker(FrontierVerdict::Rejected),
            ChartPointMarker::Cross
        );
        assert_eq!(
            point_marker(FrontierVerdict::Scuffed),
            ChartPointMarker::Cross
        );
    }

    #[test]
    fn chart_plan_omits_scuffed_points_without_renumbering_ordinals() {
        use super::chart::{ChartPlan, ChartSelection};

        let metric = test_metric("elapsed", "milliseconds");
        let scene = chart_scene(
            metric.clone(),
            vec![
                chart_experiment("exp-a", "A", FrontierVerdict::Accepted),
                chart_experiment("exp-b", "B", FrontierVerdict::Scuffed),
                chart_experiment("exp-c", "C", FrontierVerdict::Rejected),
            ],
            vec![Some(1_000_000.0), Some(2_000_000.0), Some(3_000_000.0)],
        );
        let plan = ChartPlan::build(&scene, &[metric], &ChartSelection::default());
        assert_eq!(plan.hit_ordinals, vec![0, 2]);
        assert_eq!(
            plan.series[0]
                .points
                .iter()
                .map(|point| point.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn chart_plan_honors_hidden_series_and_slug_window() {
        use super::chart::{ChartPlan, ChartSelection, ChartWindowRequest};

        let metric = test_metric("elapsed", "milliseconds");
        let scene = chart_scene(
            metric.clone(),
            vec![
                chart_experiment("exp-a", "A", FrontierVerdict::Accepted),
                chart_experiment("exp-b", "B", FrontierVerdict::Accepted),
                chart_experiment("exp-c", "C", FrontierVerdict::Accepted),
            ],
            vec![Some(1_000_000.0), Some(2_000_000.0), Some(3_000_000.0)],
        );
        let selection = ChartSelection {
            window: ChartWindowRequest {
                from: Some("exp-b".to_owned()),
                to: Some("exp-c".to_owned()),
            },
            ..ChartSelection::default()
        };
        let plan = ChartPlan::build(&scene, std::slice::from_ref(&metric), &selection);
        assert_eq!((plan.x.first, plan.x.last), (1, 2));
        assert_eq!(plan.hit_ordinals, vec![1, 2]);

        let mut hidden = selection;
        let _ = hidden.hidden_metrics.insert("elapsed".to_owned());
        assert!(
            !ChartPlan::build(&scene, std::slice::from_ref(&metric), &hidden).has_visible_data()
        );
    }

    #[test]
    fn semantic_svg_is_style_closed_linked_and_xml_escaped() {
        use super::chart::{ChartPlan, ChartSelection, render_chart_svg};

        let metric = test_metric("elapsed", "milliseconds");
        let scene = chart_scene(
            metric.clone(),
            vec![chart_experiment(
                "exp-a",
                "Hostile <title> & datum",
                FrontierVerdict::Accepted,
            )],
            vec![Some(30_000_000.0)],
        );
        let plan = ChartPlan::build(&scene, &[metric], &ChartSelection::default());
        let svg = render_chart_svg(&plan, &scene);

        assert!(svg.starts_with("<svg "));
        assert!(svg.contains(r#"xmlns="http://www.w3.org/2000/svg""#));
        assert!(svg.contains(r#"width="1100" height="420""#));
        assert!(svg.contains(r#"viewBox="0 0 1100 420""#));
        assert!(svg.contains(r#"data-chart-hit="true""#));
        assert!(svg.contains(r#"href="experiment/exp%2Da""#));
        assert!(svg.contains("Hostile &lt;title&gt; &amp; datum"));
        assert!(svg.contains("stroke-dasharray"));
        assert!(!svg.contains("class="));
        assert!(!svg.contains("style="));
        assert!(!svg.contains("<style"));
        assert!(!svg.contains("foreignObject"));
        assert!(!svg.contains("plotters"));
    }
}
