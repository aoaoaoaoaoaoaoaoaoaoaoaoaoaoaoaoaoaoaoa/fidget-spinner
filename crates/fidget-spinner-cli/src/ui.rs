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
    AttachmentTargetRef, ExperimentAnalysis, ExperimentOutcome, ExperimentStatus, FrontierRecord,
    FrontierStatus, FrontierVerdict, KnownMetricUnit, MetricAggregation, MetricUnit,
    MetricVisibility, NonEmptyText, OptimizationObjective, RegistryLockMode, RegistryName,
    RunDimensionValue, Slug, TagFamilyName, TagName, VertexRef,
};
use fidget_spinner_store_sqlite::{
    AssignTagFamilyRequest, CreateKpiRequest, CreateTagFamilyRequest, DefineMetricRequest,
    DeleteKpiRequest, DeleteMetricRequest, DeleteTagRequest, ExperimentDetail, ExperimentSummary,
    FrontierMetricSeries, FrontierOpenProjection, FrontierSummary, HypothesisCurrentState,
    HypothesisDetail, KpiSummary, ListExperimentsQuery, ListFrontiersQuery, ListHypothesesQuery,
    MergeMetricRequest, MergeTagRequest, MetricKeysQuery, MetricScope, ProjectStatus,
    RenameMetricRequest, RenameTagRequest, STATE_DB_NAME, SetRegistryLockRequest,
    SetTagFamilyMandatoryRequest, StoreError, UpdateFrontierRequest, UpdateKpiRequest,
    VertexSummary,
};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use plotters::prelude::{
    BLACK, ChartBuilder, Circle, Cross, IntoDrawingArea, IntoLogRange, LineSeries, PathElement,
    SVGBackend, SeriesLabelPosition, ShapeStyle,
};
use plotters::style::{Color, IntoFont, RGBColor};
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::open_store;

const FAVICON_SVG: &str = include_str!("../../../assets/ui/favicon.svg");
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
    Metrics,
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
    scope: NavigatorScope,
    bind: SocketAddr,
    limit: Option<u32>,
) -> Result<(), StoreError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .map_err(StoreError::from)?;
    runtime.block_on(async move {
        let state = NavigatorState { scope, limit };
        let app = Router::new()
            .route("/favicon.svg", get(favicon_svg))
            .route("/favicon.ico", get(favicon_svg))
            .route("/", get(root_page))
            .route("/refresh-token", get(root_project_refresh_token))
            .route("/project/{project}", get(project_home))
            .route("/project/{project}/", get(project_home))
            .route(
                "/project/{project}/refresh-token",
                get(project_refresh_token),
            )
            .route("/project/{project}/tags", get(project_tags))
            .route("/project/{project}/metrics", get(project_metrics))
            .route("/project/{project}/tags/create", post(create_tag))
            .route(
                "/project/{project}/tags/families/create",
                post(create_tag_family),
            )
            .route("/project/{project}/tags/lock", post(set_tag_lock))
            .route(
                "/project/{project}/tags/family-mandatory",
                post(set_tag_family_mandatory),
            )
            .route("/project/{project}/tags/rename", post(rename_tag))
            .route("/project/{project}/tags/merge", post(merge_tag))
            .route("/project/{project}/tags/delete", post(delete_tag))
            .route(
                "/project/{project}/tags/tag-family",
                post(assign_tag_family),
            )
            .route("/project/{project}/metrics/create", post(create_metric))
            .route("/project/{project}/metrics/rename", post(rename_metric))
            .route("/project/{project}/metrics/merge", post(merge_metric))
            .route("/project/{project}/metrics/delete", post(delete_metric))
            .route("/project/{project}/metrics/kpi", post(create_kpi))
            .route("/project/{project}/metrics/kpi/update", post(update_kpi))
            .route("/project/{project}/metrics/kpi/delete", post(delete_kpi))
            .route(
                "/project/{project}/frontier/{selector}",
                get(frontier_detail),
            )
            .route(
                "/project/{project}/frontier/{selector}/archive",
                post(archive_frontier),
            )
            .route(
                "/project/{project}/frontier/{selector}/unarchive",
                post(unarchive_frontier),
            )
            .route(
                "/project/{project}/hypothesis/{selector}",
                get(hypothesis_detail),
            )
            .route(
                "/project/{project}/experiment/{selector}",
                get(experiment_detail),
            )
            .route(
                "/project/{project}/artifact/{selector}",
                get(artifact_detail),
            )
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

async fn favicon_svg() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        FAVICON_SVG,
    )
}

async fn root_page(State(state): State<NavigatorState>) -> Response {
    render_response(match &state.scope {
        NavigatorScope::Single(project_root) => render_project_home(ProjectRenderContext::root(
            project_root.clone(),
            state.limit,
        )),
        NavigatorScope::Multi { .. } => render_project_index(state),
    })
}

async fn root_project_refresh_token(State(state): State<NavigatorState>) -> Response {
    match &state.scope {
        NavigatorScope::Single(project_root) => refresh_token_response(project_refresh_token_for(
            &ProjectRenderContext::root(project_root.clone(), state.limit),
        )),
        NavigatorScope::Multi { .. } => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn project_home(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
) -> Response {
    render_response(resolve_project_context(&state, &project).and_then(render_project_home))
}

async fn project_refresh_token(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
) -> Response {
    refresh_token_response(
        resolve_project_context(&state, &project)
            .and_then(|context| project_refresh_token_for(&context)),
    )
}

async fn project_tags(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
) -> Response {
    render_response(resolve_project_context(&state, &project).and_then(render_project_tags))
}

async fn project_metrics(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    uri: Uri,
) -> Response {
    render_response(
        resolve_project_context(&state, &project).and_then(|context| {
            ProjectMetricsQuery::parse(uri.query())
                .and_then(|query| render_project_metrics(context, query))
        }),
    )
}

#[derive(Deserialize)]
struct CreateTagForm {
    name: String,
    description: String,
    family: Option<String>,
}

async fn create_tag(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<CreateTagForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let family = form
                .family
                .filter(|family| !family.trim().is_empty())
                .map(TagFamilyName::new)
                .transpose()?;
            let _ = store.register_tag_in_family(
                TagName::new(form.name)?,
                NonEmptyText::new(form.description)?,
                family,
            )?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct CreateTagFamilyForm {
    name: String,
    description: String,
    mandatory: Option<String>,
}

async fn create_tag_family(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<CreateTagFamilyForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.create_tag_family(CreateTagFamilyRequest {
                name: TagFamilyName::new(form.name)?,
                description: NonEmptyText::new(form.description)?,
                mandatory: form.mandatory.is_some(),
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct SetTagLockForm {
    mode: String,
    locked: String,
}

async fn set_tag_lock(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<SetTagLockForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.set_registry_lock(SetRegistryLockRequest {
                registry: RegistryName::tags(),
                mode: parse_ui_lock_mode(&form.mode)?,
                locked: matches!(form.locked.as_str(), "1" | "true" | "on" | "lock"),
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct SetTagFamilyMandatoryForm {
    family: String,
    expected_revision: Option<u64>,
    mandatory: String,
}

async fn set_tag_family_mandatory(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<SetTagFamilyMandatoryForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.set_tag_family_mandatory(SetTagFamilyMandatoryRequest {
                family: TagFamilyName::new(form.family)?,
                expected_revision: form.expected_revision,
                mandatory: matches!(form.mandatory.as_str(), "1" | "true" | "on" | "mandatory"),
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct RenameTagForm {
    tag: String,
    expected_revision: Option<u64>,
    new_name: String,
}

async fn rename_tag(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<RenameTagForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.rename_tag(RenameTagRequest {
                tag: TagName::new(form.tag)?,
                expected_revision: form.expected_revision,
                new_name: TagName::new(form.new_name)?,
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct MergeTagForm {
    source: String,
    expected_revision: Option<u64>,
    target: String,
}

async fn merge_tag(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<MergeTagForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.merge_tag(MergeTagRequest {
                source: TagName::new(form.source)?,
                expected_revision: form.expected_revision,
                target: TagName::new(form.target)?,
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct DeleteTagForm {
    tag: String,
    expected_revision: Option<u64>,
}

async fn delete_tag(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<DeleteTagForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            store.delete_tag(DeleteTagRequest {
                tag: TagName::new(form.tag)?,
                expected_revision: form.expected_revision,
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct AssignTagFamilyForm {
    tag: String,
    expected_revision: Option<u64>,
    family: String,
}

async fn assign_tag_family(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<AssignTagFamilyForm>,
) -> Response {
    tag_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let family = if form.family.trim().is_empty() {
                None
            } else {
                Some(TagFamilyName::new(form.family)?)
            };
            let _ = store.assign_tag_family(AssignTagFamilyRequest {
                tag: TagName::new(form.tag)?,
                expected_revision: form.expected_revision,
                family,
            })?;
            Ok(format!("{}tags", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct CreateMetricForm {
    key: String,
    unit: String,
    aggregation: String,
    objective: String,
    visibility: String,
    description: String,
}

async fn create_metric(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<CreateMetricForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.define_metric(DefineMetricRequest {
                key: NonEmptyText::new(form.key)?,
                unit: MetricUnit::new(form.unit)?,
                aggregation: parse_metric_aggregation_ui(&form.aggregation)?,
                objective: parse_optimization_objective_ui(&form.objective)?,
                visibility: parse_metric_visibility_ui(&form.visibility)?,
                description: optional_text_field(form.description)?,
            })?;
            Ok(format!("{}metrics", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct RenameMetricForm {
    metric: String,
    new_key: String,
}

async fn rename_metric(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<RenameMetricForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.rename_metric(RenameMetricRequest {
                metric: NonEmptyText::new(form.metric)?,
                new_key: NonEmptyText::new(form.new_key)?,
            })?;
            Ok(format!("{}metrics", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct MergeMetricForm {
    source: String,
    target: String,
}

async fn merge_metric(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<MergeMetricForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            store.merge_metric(MergeMetricRequest {
                source: NonEmptyText::new(form.source)?,
                target: NonEmptyText::new(form.target)?,
            })?;
            Ok(format!("{}metrics", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct DeleteMetricForm {
    metric: String,
}

async fn delete_metric(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<DeleteMetricForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            store.delete_metric(DeleteMetricRequest {
                metric: NonEmptyText::new(form.metric)?,
            })?;
            Ok(format!("{}metrics", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct CreateKpiForm {
    frontier: String,
    name: String,
    objective: String,
    description: String,
    metric_keys: String,
}

async fn create_kpi(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<CreateKpiForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            let metric_keys = parse_metric_key_list(&form.metric_keys)?;
            let _ = store.create_kpi(CreateKpiRequest {
                frontier: frontier.clone(),
                name: NonEmptyText::new(form.name)?,
                objective: parse_optimization_objective_ui(&form.objective)?,
                description: optional_text_field(form.description)?,
                metric_keys,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
}

#[derive(Deserialize)]
struct UpdateKpiForm {
    frontier: String,
    kpi: String,
    name: String,
    objective: String,
    description: String,
    metric_keys: String,
}

async fn update_kpi(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<UpdateKpiForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            let metric_keys = parse_metric_key_list(&form.metric_keys)?;
            let _ = store.update_kpi(UpdateKpiRequest {
                frontier: frontier.clone(),
                kpi: form.kpi,
                name: NonEmptyText::new(form.name)?,
                objective: parse_optimization_objective_ui(&form.objective)?,
                description: optional_text_field(form.description)?,
                metric_keys,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
}

#[derive(Deserialize)]
struct DeleteKpiForm {
    frontier: String,
    kpi: String,
}

async fn delete_kpi(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<DeleteKpiForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            store.delete_kpi(DeleteKpiRequest {
                frontier: frontier.clone(),
                kpi: form.kpi,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
}

async fn frontier_detail(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
    uri: Uri,
) -> Response {
    render_response(
        resolve_project_context(&state, &project).and_then(|context| {
            FrontierPageQuery::parse(uri.query())
                .and_then(|query| render_frontier_detail(context, selector, query))
        }),
    )
}

#[derive(Debug, Deserialize)]
struct FrontierArchiveForm {
    expected_revision: Option<u64>,
}

async fn archive_frontier(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
    Form(form): Form<FrontierArchiveForm>,
) -> Response {
    frontier_status_mutation_response(resolve_project_context(&state, &project).and_then(
        |context| {
            update_frontier_status(
                context,
                selector,
                form.expected_revision,
                FrontierStatus::Archived,
            )
        },
    ))
}

async fn unarchive_frontier(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
    Form(form): Form<FrontierArchiveForm>,
) -> Response {
    frontier_status_mutation_response(resolve_project_context(&state, &project).and_then(
        |context| {
            update_frontier_status(
                context,
                selector,
                form.expected_revision,
                FrontierStatus::Exploring,
            )
        },
    ))
}

async fn hypothesis_detail(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
) -> Response {
    render_response(
        resolve_project_context(&state, &project)
            .and_then(|context| render_hypothesis_detail(context, selector)),
    )
}

async fn experiment_detail(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
) -> Response {
    render_response(
        resolve_project_context(&state, &project)
            .and_then(|context| render_experiment_detail(context, selector)),
    )
}

async fn artifact_detail(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
) -> Response {
    render_response(
        resolve_project_context(&state, &project)
            .and_then(|context| render_artifact_detail(context, selector)),
    )
}

fn render_response(result: Result<Markup, StoreError>) -> Response {
    match result {
        Ok(markup) => Html(harden_autofill_controls(markup.into_string())).into_response(),
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

fn harden_autofill_controls(document: String) -> String {
    let mut hardened = String::with_capacity(document.len() + 512);
    let mut cursor = 0;
    while let Some(tag_offset) = document[cursor..].find('<') {
        let tag_start = cursor + tag_offset;
        hardened.push_str(&document[cursor..tag_start]);
        let Some(tag_len) = document[tag_start..].find('>').map(|offset| offset + 1) else {
            hardened.push_str(&document[tag_start..]);
            return hardened;
        };
        let tag_end = tag_start + tag_len;
        hardened.push_str(&harden_autofill_tag(&document[tag_start..tag_end]));
        cursor = tag_end;
    }
    hardened.push_str(&document[cursor..]);
    hardened
}

fn harden_autofill_tag(tag: &str) -> String {
    let Some(tag_kind) = AutofillTagKind::from_tag(tag) else {
        return tag.to_owned();
    };
    if tag_kind == AutofillTagKind::HiddenInput {
        return tag.to_owned();
    }
    let mut attributes = Vec::with_capacity(2);
    if tag_kind.accepts_autocomplete_off() && !has_html_attribute(tag, "autocomplete") {
        attributes.push(r#" autocomplete="off""#);
    }
    if tag_kind.accepts_password_manager_ignore()
        && !has_html_attribute(tag, "data-protonpass-ignore")
    {
        attributes.push(r#" data-protonpass-ignore="true""#);
    }
    if attributes.is_empty() {
        return tag.to_owned();
    }
    let Some(insert_at) = tag.rfind('>') else {
        return tag.to_owned();
    };
    let mut hardened =
        String::with_capacity(tag.len() + attributes.iter().map(|attr| attr.len()).sum::<usize>());
    hardened.push_str(&tag[..insert_at]);
    for attribute in attributes {
        hardened.push_str(attribute);
    }
    hardened.push_str(&tag[insert_at..]);
    hardened
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AutofillTagKind {
    Form,
    Field,
    HiddenInput,
}

impl AutofillTagKind {
    fn from_tag(tag: &str) -> Option<Self> {
        if !tag.starts_with('<') || tag.starts_with("</") || tag.starts_with("<!") {
            return None;
        }
        if tag_has_name(tag, "form") {
            return Some(Self::Form);
        }
        if tag_has_name(tag, "input") {
            return Some(if has_html_attribute_value(tag, "type", "hidden") {
                Self::HiddenInput
            } else {
                Self::Field
            });
        }
        (tag_has_name(tag, "select") || tag_has_name(tag, "textarea")).then_some(Self::Field)
    }

    const fn accepts_autocomplete_off(self) -> bool {
        matches!(self, Self::Form | Self::Field)
    }

    const fn accepts_password_manager_ignore(self) -> bool {
        matches!(self, Self::Field)
    }
}

fn tag_has_name(tag: &str, name: &str) -> bool {
    let Some(rest) = tag.strip_prefix('<').and_then(|tag| tag.strip_prefix(name)) else {
        return false;
    };
    rest.as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'>' | b'/'))
}

fn has_html_attribute(tag: &str, name: &str) -> bool {
    tag.match_indices(name).any(|(index, _)| {
        let before = tag.as_bytes().get(index.wrapping_sub(1)).copied();
        let after = tag.as_bytes().get(index + name.len()).copied();
        before.is_some_and(|byte| byte.is_ascii_whitespace())
            && after.is_some_and(|byte| {
                matches!(byte, b'=' | b'>' | b'/') || byte.is_ascii_whitespace()
            })
    })
}

fn has_html_attribute_value(tag: &str, name: &str, value: &str) -> bool {
    let needle = format!(r#"{name}="{value}""#);
    has_html_attribute(tag, name) && tag.contains(&needle)
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
        Err(StoreError::EmptyKpi { .. })
        | Err(StoreError::KpiMetricObjectiveMismatch { .. })
        | Err(StoreError::KpiMetricDimensionMismatch { .. }) => {
            (StatusCode::BAD_REQUEST, "invalid KPI contract".to_owned()).into_response()
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

fn parse_metric_key_list(raw: &str) -> Result<Vec<NonEmptyText>, StoreError> {
    raw.split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(|key| NonEmptyText::new(key.to_owned()).map_err(StoreError::from))
        .collect()
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

fn parse_metric_visibility_ui(raw: &str) -> Result<MetricVisibility, StoreError> {
    match raw {
        "canonical" => Ok(MetricVisibility::Canonical),
        "minor" => Ok(MetricVisibility::Minor),
        "hidden" => Ok(MetricVisibility::Hidden),
        "archived" => Ok(MetricVisibility::Archived),
        _ => Err(StoreError::InvalidInput(format!(
            "invalid metric visibility `{raw}`"
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

fn render_project_index(state: NavigatorState) -> Result<Markup, StoreError> {
    let NavigatorScope::Multi {
        scan_root,
        project_roots,
    } = state.scope
    else {
        return Err(StoreError::InvalidInput(
            "project index requested for single-project navigator".to_owned(),
        ));
    };
    let mut projects = project_roots
        .into_iter()
        .map(|project_root| {
            let store = open_store(project_root.as_std_path())?;
            Ok(ProjectIndexItem {
                project_root,
                project_status: store.status()?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    projects.sort_by(|left, right| {
        left.project_status
            .display_name
            .cmp(&right.project_status.display_name)
            .then_with(|| left.project_root.cmp(&right.project_root))
    });

    Ok(html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                (render_favicon_links())
                title { "Fidget Spinner navigator" }
                style { (PreEscaped(styles())) }
            }
            body {
                main.index-shell {
                    header.page-header {
                        div.eyebrow { "home" }
                        h1.page-title { "Fidget Spinner navigator" }
                        p.page-subtitle {
                            "Central project index rooted at "
                            code { (scan_root.as_str()) }
                        }
                    }
                    section.card {
                        h2 { "Projects" }
                        @if projects.is_empty() {
                            p.muted { "No Spinner projects were discovered under this root." }
                        } @else {
                            div.card-grid {
                                @for project in limit_items(&projects, state.limit) {
                                    article.mini-card {
                                        div.card-header {
                                            a.title-link href=(project_root_href(&project.project_root)) {
                                                (&project.project_status.display_name)
                                            }
                                        }
                                        p.prose { (project.project_root.as_str()) }
                                        div.meta-row {
                                            span { (format!("{} frontiers", project.project_status.frontier_count)) }
                                            span { (format!("{} hypotheses", project.project_status.hypothesis_count)) }
                                        }
                                        div.meta-row {
                                            span { (format!("{} experiments", project.project_status.experiment_count)) }
                                            span { (format!("{} open", project.project_status.open_experiment_count)) }
                                        }
                                        div.meta-row.muted {
                                            span { (project.project_status.state_root.as_str()) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
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
    let metadata = std::fs::metadata(database_path.as_std_path())?;
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

fn render_project_home(context: ProjectRenderContext) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None, &context)?;
    let title = format!("{} navigator", shell.project_status.display_name);
    let content = html! {
        (render_project_status(&shell.project_status))
        (render_frontier_grid(&shell.frontiers, context.limit))
    };
    Ok(render_shell(&title, &shell, None, content))
}

fn render_project_tags(context: ProjectRenderContext) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None, &context)?;
    let registry = store.tag_registry()?;
    let usage = load_tag_usage(&store)?;
    let title = format!("{} · tags", shell.project_status.display_name);
    let mandatory_count = registry
        .families
        .iter()
        .filter(|family| family.mandatory)
        .count();
    let lock_state = TagLockState::from_locks(&registry.locks);
    let orphan_count = registry
        .tags
        .iter()
        .filter(|tag| {
            usage
                .get(&tag.name)
                .is_none_or(|usage| usage.hypotheses + usage.experiments == 0)
        })
        .count();
    let content = html! {
        section.card.tag-state-card {
            div.tag-state-band {
                div.fact-strip {
                    (render_fact("active tags", &registry.tags.len().to_string()))
                    (render_fact("families", &registry.families.len().to_string()))
                    (render_fact("mandatory", &mandatory_count.to_string()))
                    (render_fact("orphans", &orphan_count.to_string()))
                }
                div.tag-state-controls {
                    (render_tag_lock_switch(
                        "new tags",
                        "add",
                        lock_state.add_locked,
                        "When locked, MCP cannot create new tags. Supervisor UI can still curate the registry.",
                    ))
                    (render_tag_lock_switch(
                        "registry edits",
                        "edit",
                        lock_state.edit_locked,
                        "When locked, MCP-origin registry editing is forbidden. Supervisor UI remains authoritative; model assignment of existing tags stays open.",
                    ))
                }
            }
        }
        (render_tag_families(&registry.families))
        (render_tag_table(&registry.tags, &registry.families, &usage))
        @if !registry.name_history.is_empty() {
            section.card {
                h2 { "Name History" }
                div.tag-history-list {
                    @for history in &registry.name_history {
                        div.tag-history-row {
                            span.tag-chip { (history.name) }
                            span.muted { (history.disposition.as_str()) }
                            span { (history.message) }
                        }
                    }
                }
            }
        }
    };
    Ok(render_shell(&title, &shell, None, content))
}

fn render_project_metrics(
    context: ProjectRenderContext,
    query: ProjectMetricsQuery,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None, &context)?;
    let metrics = store.metric_keys(MetricKeysQuery {
        frontier: None,
        scope: MetricScope::All,
    })?;
    let active_frontiers = &shell.frontiers;
    let selected_frontier = selected_kpi_frontier(active_frontiers, query.frontier.as_deref());
    let selected_kpis = selected_frontier
        .as_ref()
        .map(|frontier| {
            store.list_kpis(fidget_spinner_store_sqlite::KpiListQuery {
                frontier: frontier.slug.to_string(),
            })
        })
        .transpose()?
        .unwrap_or_default();
    let kpi_count = active_frontiers
        .iter()
        .map(|frontier| {
            store
                .list_kpis(fidget_spinner_store_sqlite::KpiListQuery {
                    frontier: frontier.slug.to_string(),
                })
                .map(|kpis| kpis.len())
        })
        .collect::<Result<Vec<_>, StoreError>>()?
        .into_iter()
        .sum::<usize>();
    let hidden_count = metrics
        .iter()
        .filter(|metric| {
            matches!(
                metric.visibility,
                MetricVisibility::Hidden | MetricVisibility::Archived
            )
        })
        .count();
    let orphan_count = metrics
        .iter()
        .filter(|metric| metric.reference_count == 0)
        .count();
    let title = format!("{} · metrics", shell.project_status.display_name);
    let content = html! {
        section.card.tag-state-card {
            div.tag-state-band {
                div.fact-strip {
                    (render_fact("metrics", &metrics.len().to_string()))
                    (render_fact("hidden", &hidden_count.to_string()))
                    (render_fact("KPIs", &kpi_count.to_string()))
                    (render_fact("orphans", &orphan_count.to_string()))
                }
            }
        }
        (render_kpi_manager(active_frontiers, selected_frontier.as_ref(), &selected_kpis, &metrics))
        (render_metric_registry_table(&metrics))
    };
    Ok(render_shell(&title, &shell, None, content))
}

fn render_frontier_detail(
    context: ProjectRenderContext,
    selector: String,
    query: FrontierPageQuery,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let projection = store.frontier_open(&selector)?;
    let shell = load_shell_frame(&store, Some(projection.frontier.slug.clone()), &context)?;
    let kpi_metric_keys_for_tab_bar = store.metric_keys(MetricKeysQuery {
        frontier: Some(projection.frontier.slug.to_string()),
        scope: MetricScope::Kpi,
    })?;
    let other_metric_keys_for_tab_bar = load_other_metric_keys(&store, &projection)?;
    let requested_metrics_for_tab_bar =
        requested_or_kpi_metric_keys(&query.metric, &kpi_metric_keys_for_tab_bar);
    let tab = FrontierTab::from_query(query.tab.as_deref());
    let title = format!("{} · frontier", projection.frontier.label);
    let content = render_frontier_tab_content(&store, &projection, tab, &query, context.limit)?;
    Ok(render_shell(
        &title,
        &shell,
        Some(render_frontier_tab_bar(
            &projection.frontier.slug,
            tab,
            &resolve_selected_metric_keys(
                &requested_metrics_for_tab_bar,
                &visible_metric_catalog(
                    &kpi_metric_keys_for_tab_bar,
                    &other_metric_keys_for_tab_bar,
                ),
            ),
            query.log_y_requested(),
            &query.dimension_filters(),
            query.table_metric.as_deref(),
        )),
        content,
    ))
}

fn render_hypothesis_detail(
    context: ProjectRenderContext,
    selector: String,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let detail = store.read_hypothesis(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let shell = load_shell_frame(&store, Some(frontier.slug.clone()), &context)?;
    let title = format!("{} · hypothesis", detail.record.title);
    let content = html! {
        (render_hypothesis_header(&detail, &frontier))
        (render_prose_block("Body", detail.record.body.as_str()))
        (render_vertex_relation_sections(&detail.parents, &detail.children, context.limit))
        (render_artifact_section(&detail.artifacts, context.limit))
        (render_experiment_section(
            "Open Experiments",
            &detail.open_experiments,
            context.limit,
        ))
        (render_experiment_section(
            "Closed Experiments",
            &detail.closed_experiments,
            context.limit,
        ))
    };
    Ok(render_shell(&title, &shell, None, content))
}

fn render_experiment_detail(
    context: ProjectRenderContext,
    selector: String,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let detail = store.read_experiment(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let shell = load_shell_frame(&store, Some(frontier.slug.clone()), &context)?;
    let title = format!("{} · experiment", detail.record.title);
    let content = html! {
        (render_experiment_header(&detail, &frontier))
        @if let Some(outcome) = detail.record.outcome.as_ref() {
            (render_experiment_outcome(outcome))
        } @else {
            (render_open_experiment_outcome())
        }
        (render_artifact_section(&detail.artifacts, context.limit))
        (render_vertex_relation_sections(&detail.parents, &detail.children, context.limit))
    };
    Ok(render_shell(&title, &shell, None, content))
}

fn render_artifact_detail(
    context: ProjectRenderContext,
    selector: String,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let detail = store.read_artifact(&selector)?;
    let shell = load_shell_frame(&store, None, &context)?;
    let attachments = detail
        .attachments
        .iter()
        .map(|target| resolve_attachment_display(&store, *target))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let title = format!("{} · artifact", detail.record.label);
    let content = html! {
        section.card {
            h1 { (detail.record.label) }
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
    Ok(render_shell(&title, &shell, None, content))
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

fn load_tag_usage(
    store: &fidget_spinner_store_sqlite::ProjectStore,
) -> Result<BTreeMap<TagName, TagUsage>, StoreError> {
    let mut usage = BTreeMap::<TagName, TagUsage>::new();
    for hypothesis in store.list_hypotheses(ListHypothesesQuery {
        include_archived: true,
        limit: None,
        ..ListHypothesesQuery::default()
    })? {
        for tag in hypothesis.tags {
            usage.entry(tag).or_default().hypotheses += 1;
        }
    }
    for experiment in store.list_experiments(ListExperimentsQuery {
        include_archived: true,
        limit: None,
        ..ListExperimentsQuery::default()
    })? {
        for tag in experiment.tags {
            usage.entry(tag).or_default().experiments += 1;
        }
    }
    Ok(usage)
}

#[derive(Clone, Copy, Default)]
struct TagLockState {
    add_locked: bool,
    edit_locked: bool,
}

impl TagLockState {
    fn from_locks(locks: &[fidget_spinner_core::RegistryLockRecord]) -> Self {
        Self {
            add_locked: locks
                .iter()
                .any(|lock| lock.mode == RegistryLockMode::Definition),
            edit_locked: locks
                .iter()
                .any(|lock| lock.mode == RegistryLockMode::Family),
        }
    }
}

fn render_tag_lock_switch(label: &str, mode: &str, locked: bool, help: &str) -> Markup {
    html! {
        form.tag-lock-switch-form method="post" action="tags/lock" data-preserve-viewport="true" {
            input type="hidden" name="mode" value=(mode);
            input type="hidden" name="locked" value=(if locked { "unlock" } else { "lock" });
            button
                type="submit"
                class=(if locked { "tag-lock-switch locked" } else { "tag-lock-switch" })
                aria-pressed=(if locked { "true" } else { "false" })
                title=(help)
            {
                span.switch-track aria-hidden="true" {
                    span.switch-thumb {}
                }
                span.switch-label { (label) }
                span.switch-state { (if locked { "locked" } else { "open" }) }
            }
        }
    }
}

fn render_tag_families(families: &[fidget_spinner_core::TagFamilyRecord]) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "Families" }
            }
            form.tag-create-form method="post" action="tags/families/create" data-preserve-viewport="true" {
                input.compact-input type="text" name="name" placeholder="family name";
                input.compact-input type="text" name="description" placeholder="description";
                label.inline-check {
                    input type="checkbox" name="mandatory" value="1";
                    "mandatory"
                }
                button.form-button type="submit" { "Create Family" }
            }
            @if families.is_empty() {
                p.muted { "No families yet." }
            } @else {
                div.tag-family-grid {
                    @for family in families {
                        article.mini-card {
                            div.card-header {
                                strong { (family.name) }
                                div.family-policy-row {
                                    span.status-chip { (if family.mandatory { "mandatory" } else { "optional" }) }
                                    form.tag-inline-form method="post" action="tags/family-mandatory" data-preserve-viewport="true" {
                                        input type="hidden" name="family" value=(family.name.as_str());
                                        input type="hidden" name="expected_revision" value=(family.revision);
                                        input type="hidden" name="mandatory" value=(if family.mandatory { "optional" } else { "mandatory" });
                                        button.form-button type="submit" {
                                            (if family.mandatory { "Make Optional" } else { "Make Mandatory" })
                                        }
                                    }
                                }
                            }
                            p.prose { (family.description) }
                        }
                    }
                }
            }
        }
    }
}

fn render_tag_table(
    tags: &[fidget_spinner_core::TagRecord],
    families: &[fidget_spinner_core::TagFamilyRecord],
    usage: &BTreeMap<TagName, TagUsage>,
) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "Tag Registry" }
                (render_create_tag_form(families))
            }
            @if tags.is_empty() {
                p.muted { "No tags yet." }
            } @else {
                div.table-wrap {
                    table.dense-table.tag-registry-table {
                        thead {
                            tr {
                                th { "Tag" }
                                th { "Family" }
                                th { "Description" }
                                th { "Use" }
                                th { "Merge" }
                            }
                        }
                        tbody {
                            @for tag in tags {
                                @let tag_usage = usage.get(&tag.name).copied().unwrap_or_default();
                                tr {
                                    td.no-truncate {
                                        div.tag-identity-row {
                                            form.tag-icon-form method="post" action="tags/delete" data-preserve-viewport="true" {
                                                input type="hidden" name="tag" value=(tag.name.as_str());
                                                input type="hidden" name="expected_revision" value=(tag.revision);
                                                button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Delete {}", tag.name)) title="Delete tag" {
                                                    (trash_icon())
                                                }
                                            }
                                            form.tag-inline-rename-form method="post" action="tags/rename" data-preserve-viewport="true" data-inline-rename-form="true" data-original-name=(tag.name.as_str()) {
                                                input type="hidden" name="tag" value=(tag.name.as_str());
                                                input type="hidden" name="expected_revision" value=(tag.revision);
                                                span.tag-chip data-inline-rename-label="true" { (tag.name) }
                                                button.inline-icon-button type="button" data-inline-rename-trigger="true" aria-label=(format!("Rename {}", tag.name)) title="Rename tag" {
                                                    (pencil_icon())
                                                }
                                                input.inline-rename-input type="text" name="new_name" value=(tag.name.as_str()) aria-label=(format!("New name for {}", tag.name)) data-inline-rename-input="true";
                                            }
                                        }
                                    }
                                    td.no-truncate {
                                        form.tag-inline-form method="post" action="tags/tag-family" data-preserve-viewport="true" {
                                            input type="hidden" name="tag" value=(tag.name.as_str());
                                            input type="hidden" name="expected_revision" value=(tag.revision);
                                            select.compact-select name="family" data-auto-submit="true" aria-label=(format!("Family for {}", tag.name)) {
                                                option value="" selected[tag.family.is_none()] { "none" }
                                                @for family in families {
                                                    option
                                                        value=(family.name.as_str())
                                                        selected[tag.family.as_ref() == Some(&family.name)]
                                                    {
                                                        (family.name)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    td {
                                        (tag.description)
                                    }
                                    td.no-truncate {
                                        (tag_usage.hypotheses) " H · " (tag_usage.experiments) " E"
                                    }
                                    td.no-truncate {
                                        form.tag-inline-form method="post" action="tags/merge" data-preserve-viewport="true" {
                                            input type="hidden" name="source" value=(tag.name.as_str());
                                            input type="hidden" name="expected_revision" value=(tag.revision);
                                            select.compact-select name="target" {
                                                @for target in tags {
                                                    @if target.name != tag.name {
                                                        option value=(target.name.as_str()) { (target.name) }
                                                    }
                                                }
                                            }
                                            button.form-button type="submit" { "Merge" }
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

fn render_create_tag_form(families: &[fidget_spinner_core::TagFamilyRecord]) -> Markup {
    html! {
        form.tag-create-form method="post" action="tags/create" data-preserve-viewport="true" {
            input.compact-input type="text" name="name" placeholder="new tag" aria-label="New tag name";
            input.compact-input.wide-compact-input type="text" name="description" placeholder="description shown to agents" aria-label="New tag description";
            select.compact-select name="family" aria-label="New tag family" title="Optional family for the new tag." {
                option value="" { "no family" }
                @for family in families {
                    option value=(family.name.as_str()) { (family.name) }
                }
            }
            button.inline-icon-button type="submit" aria-label="Add tag" title="Add tag" {
                (plus_icon())
            }
        }
    }
}

fn render_create_metric_form() -> Markup {
    html! {
        form.tag-create-form method="post" action="metrics/create" data-preserve-viewport="true" {
            input.compact-input type="text" name="key" placeholder="metric_key" aria-label="Metric key" required;
            input.compact-input type="text" name="unit" placeholder="milliseconds" aria-label="Display unit" required;
            select.compact-select name="aggregation" aria-label="Aggregation" {
                @for aggregation in ["point", "mean", "geomean", "median", "p95", "min", "max", "sum"] {
                    option value=(aggregation) { (aggregation) }
                }
            }
            select.compact-select name="objective" aria-label="Objective" {
                option value="minimize" { "minimize" }
                option value="maximize" { "maximize" }
                option value="target" { "target" }
            }
            select.compact-select name="visibility" aria-label="Visibility" {
                option value="canonical" { "canonical" }
                option value="minor" { "minor" }
                option value="hidden" { "hidden" }
                option value="archived" { "archived" }
            }
            input.compact-input.wide-compact-input type="text" name="description" placeholder="description" aria-label="Metric description";
            button.inline-icon-button type="submit" aria-label="Add metric" title="Add metric" {
                (plus_icon())
            }
        }
    }
}

fn selected_kpi_frontier(
    frontiers: &[FrontierSummary],
    requested: Option<&str>,
) -> Option<FrontierSummary> {
    requested
        .and_then(|selector| {
            frontiers.iter().find(|frontier| {
                frontier.slug.as_str() == selector || frontier.id.to_string() == selector
            })
        })
        .or_else(|| frontiers.first())
        .cloned()
}

fn render_kpi_manager(
    frontiers: &[FrontierSummary],
    selected_frontier: Option<&FrontierSummary>,
    kpis: &[KpiSummary],
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "KPI Contracts" }
                @if let Some(frontier) = selected_frontier {
                    span.muted { (frontier.label) }
                }
            }
            @if frontiers.is_empty() {
                p.muted { "No active frontiers. Archived frontiers are intentionally excluded from KPI management." }
            } @else if let Some(frontier) = selected_frontier {
                form.tag-create-form method="get" action="metrics" data-preserve-viewport="true" {
                    select.compact-select.wide-compact-select name="frontier" aria-label="KPI frontier" data-auto-submit="true" {
                        @for option in frontiers {
                            option value=(option.slug.as_str()) selected[option.slug == frontier.slug] {
                                (option.label)
                            }
                        }
                    }
                }
                (render_create_kpi_form(frontier, metrics))
                (render_kpi_registry(frontier, kpis))
            }
        }
    }
}

fn render_create_kpi_form(
    frontier: &FrontierSummary,
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Markup {
    html! {
        form.tag-create-form method="post" action="metrics/kpi" data-preserve-viewport="true" {
            input type="hidden" name="frontier" value=(frontier.slug.as_str());
            input.compact-input type="text" name="name" placeholder="kpi name" aria-label="KPI name" required;
            select.compact-select name="objective" aria-label="KPI objective" {
                option value="minimize" { "minimize" }
                option value="maximize" { "maximize" }
                option value="target" { "target" }
            }
            input.compact-input.wide-compact-input type="text" name="metric_keys" placeholder="metric_a, metric_b" list="metric-key-list" aria-label="Metric alternatives" required;
            datalist id="metric-key-list" {
                @for metric in metrics {
                    option value=(metric.key.as_str()) {}
                }
            }
            input.compact-input.wide-compact-input type="text" name="description" placeholder="description" aria-label="KPI description";
            button.inline-icon-button type="submit" aria-label="Add KPI" title="Add KPI" {
                (plus_icon())
            }
        }
    }
}

fn render_kpi_registry(frontier: &FrontierSummary, kpis: &[KpiSummary]) -> Markup {
    html! {
        @if kpis.is_empty() {
            p.muted { "No KPIs for this frontier yet." }
        } @else {
            div.table-scroll {
                table.metric-table.kpi-table {
                    thead {
                        tr {
                            th { "" }
                            th { "Contract" }
                        }
                    }
                    tbody {
                        @for kpi in kpis {
                            tr {
                                td.no-truncate {
                                    form.tag-icon-form method="post" action="metrics/kpi/delete" data-preserve-viewport="true" {
                                        input type="hidden" name="frontier" value=(frontier.slug.as_str());
                                        input type="hidden" name="kpi" value=(kpi.id.to_string());
                                        button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Delete KPI {}", kpi.name)) title="Delete KPI" {
                                            (trash_icon())
                                        }
                                    }
                                }
                                td.no-truncate {
                                    form.tag-create-form.kpi-edit-form method="post" action="metrics/kpi/update" data-preserve-viewport="true" {
                                        input type="hidden" name="frontier" value=(frontier.slug.as_str());
                                        input type="hidden" name="kpi" value=(kpi.id.to_string());
                                        input.compact-input type="text" name="name" value=(kpi.name.as_str()) aria-label=(format!("KPI name {}", kpi.name)) required;
                                        select.compact-select name="objective" aria-label=(format!("KPI objective {}", kpi.name)) {
                                            option value="minimize" selected[kpi.objective == OptimizationObjective::Minimize] { "minimize" }
                                            option value="maximize" selected[kpi.objective == OptimizationObjective::Maximize] { "maximize" }
                                            option value="target" selected[kpi.objective == OptimizationObjective::Target] { "target" }
                                        }
                                        input.compact-input.wide-compact-input type="text" name="metric_keys" value=(kpi.metrics.iter().map(|metric| metric.key.to_string()).collect::<Vec<_>>().join(", ")) list="metric-key-list" aria-label=(format!("KPI alternatives {}", kpi.name)) required;
                                        input.compact-input.wide-compact-input type="text" name="description" value=(kpi.description.as_ref().map_or("", NonEmptyText::as_str)) aria-label=(format!("KPI description {}", kpi.name));
                                        button.inline-icon-button type="submit" aria-label=(format!("Update KPI {}", kpi.name)) title="Update KPI" {
                                            (pencil_icon())
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

fn render_metric_registry_table(
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Markup {
    html! {
        section.card {
            div.card-header { h2 { "Metric Registry" } }
            (render_create_metric_form())
            @if metrics.is_empty() {
                p.muted { "No metrics yet." }
            } @else {
                div.table-scroll {
                    table.metric-table {
                        thead {
                            tr {
                                th { "" }
                                th { "Metric" }
                                th { "Unit" }
                                th { "Shape" }
                                th { "Refs" }
                                th { "Merge" }
                            }
                        }
                        tbody {
                            @for metric in metrics {
                                tr {
                                    td.no-truncate {
                                        form.tag-icon-form method="post" action="metrics/delete" data-preserve-viewport="true" {
                                            input type="hidden" name="metric" value=(metric.key.as_str());
                                            button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Delete {}", metric.key)) title="Delete unused metric" {
                                                (trash_icon())
                                            }
                                        }
                                    }
                                    td.no-truncate {
                                        form.tag-inline-rename-form method="post" action="metrics/rename" data-preserve-viewport="true" data-inline-rename-form="true" data-original-name=(metric.key.as_str()) {
                                            input type="hidden" name="metric" value=(metric.key.as_str());
                                            span.tag-chip data-inline-rename-label="true" { (metric.key) }
                                            button.inline-icon-button type="button" data-inline-rename-trigger="true" aria-label=(format!("Rename {}", metric.key)) title="Rename metric" {
                                                (pencil_icon())
                                            }
                                            input.inline-rename-input type="text" name="new_key" value=(metric.key.as_str()) aria-label=(format!("New key for {}", metric.key)) data-inline-rename-input="true";
                                        }
                                        @if let Some(description) = metric.description.as_ref() {
                                            div.muted { (description) }
                                        }
                                    }
                                    td.no-truncate { (metric.unit.as_str()) " · " (metric.dimension.as_str()) }
                                    td.no-truncate { (metric.aggregation.as_str()) " · " (metric.objective.as_str()) " · " (metric.visibility.as_str()) }
                                    td.no-truncate { (metric.reference_count) }
                                    td.no-truncate {
                                        form.tag-inline-form method="post" action="metrics/merge" data-preserve-viewport="true" {
                                            input type="hidden" name="source" value=(metric.key.as_str());
                                            select.compact-select name="target" data-auto-submit="true" aria-label=(format!("Merge target for {}", metric.key)) {
                                                option value="" { "merge into..." }
                                                @for target in metrics {
                                                    @if target.key != metric.key {
                                                        option value=(target.key.as_str()) { (target.key) }
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
            let dimension_filters = query.dimension_filters();
            Ok(html! {
                (render_frontier_header(&projection.frontier))
                (render_metric_series_section(
                    &projection.frontier.slug,
                    &kpi_metrics,
                    &other_metric_keys,
                    &selected_metrics,
                    &series,
                    &dimension_filters,
                    query.log_y_requested(),
                    query.table_metric.as_deref(),
                    limit,
                ))
            })
        }
    }
}

fn render_frontier_tab_bar(
    frontier_slug: &Slug,
    active_tab: FrontierTab,
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    log_y: bool,
    dimension_filters: &BTreeMap<String, String>,
    table_metric: Option<&str>,
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
                @let href = frontier_tab_href_with_query(
                    frontier_slug,
                    tab,
                    selected_metrics,
                    log_y,
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

fn visible_metric_catalog(
    kpi_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    other_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Vec<fidget_spinner_store_sqlite::MetricKeySummary> {
    kpi_metric_keys
        .iter()
        .chain(other_metric_keys.iter())
        .cloned()
        .collect()
}

fn load_other_metric_keys(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    projection: &FrontierOpenProjection,
) -> Result<Vec<fidget_spinner_store_sqlite::MetricKeySummary>, StoreError> {
    let candidate_metrics = if projection.active_metric_keys.is_empty() {
        store.metric_keys(MetricKeysQuery {
            frontier: Some(projection.frontier.slug.to_string()),
            scope: MetricScope::Visible,
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
                .flat_map(|kpi| kpi.metrics.iter())
                .any(|kpi_metric| kpi_metric.key == metric.key)
        })
        .collect())
}

fn requested_or_kpi_metric_keys(
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

fn resolve_selected_metric_keys(
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
        let metric_family = MetricUnitFamily::from_unit(&metric.unit);
        if !families.admit(metric_family) {
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

fn render_metric_series_section(
    frontier_slug: &Slug,
    kpi_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    other_metric_keys: &[fidget_spinner_store_sqlite::MetricKeySummary],
    selected_metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    series: &[FrontierMetricSeries],
    dimension_filters: &BTreeMap<String, String>,
    log_y: bool,
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
    let can_use_log_y = chart_axes
        .as_ref()
        .is_some_and(|axes| metric_chart_supports_log_y(axes, &plotted_series));
    let effective_log_y = log_y && can_use_log_y;
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
            div.plot-toolbar {
                (render_metric_filter_popout(
                    frontier_slug,
                    selected_metrics,
                    &facets,
                    dimension_filters,
                    log_y,
                    active_table_metric,
                ))
                (render_metric_selection_popout(
                    frontier_slug,
                    kpi_metric_keys,
                    other_metric_keys,
                    selected_metrics,
                    dimension_filters,
                    log_y,
                    can_use_log_y,
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
                (PreEscaped(render_metric_chart_svg(axes, &plotted_series, effective_log_y)))
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
                                        FrontierTab::Metrics,
                                        selected_metrics,
                                        log_y,
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
                                                    (format_metric_value(point.value, &table_series.metric.unit))
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
    log_y: bool,
    table_metric: Option<&str>,
) -> Markup {
    let clear_href = frontier_tab_href_with_query(
        frontier_slug,
        FrontierTab::Metrics,
        selected_metrics,
        log_y,
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
            h3 id="slice-filters" { "Slice Filters" }
            @if facets.is_empty() {
                p.muted { "No dimension filters for the current selection." }
            } @else {
                form.filter-form.auto-submit-form method="get" action=(frontier_href(frontier_slug)) data-preserve-viewport="true" {
                    input type="hidden" name="tab" value="metrics";
                    (render_metric_selection_hidden_inputs(selected_metrics))
                    (render_log_hidden_input(log_y))
                    (render_table_metric_hidden_input(table_metric))
                    div.filter-form-grid {
                        @for facet in facets {
                            label.filter-control id=(metric_filter_anchor_id(&facet.key)) {
                                span.filter-label { (&facet.key) }
                                select.filter-select data-auto-submit="true" name=(format!("dim.{}", facet.key)) {
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
                p.muted { "No slice filters active." }
            } @else {
                div.chip-row {
                    @for (key, value) in active_filters {
                        @let href = frontier_tab_href_with_query(
                            frontier_slug,
                            FrontierTab::Metrics,
                            selected_metrics,
                            log_y,
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
    log_y: bool,
    can_use_log_y: bool,
    table_metric: Option<&str>,
) -> Markup {
    let label = metric_popout_label(selected_metrics, log_y);
    let selected_families = MetricAxisFamilies::from_metrics(selected_metrics);
    html! {
    details.control-popout id="metric-selection-popout" data-preserve-open="true" {
        summary.control-popout-toggle { (label) }
        div.control-popout-panel.metric-popout-panel {
            form.metric-picker-form.auto-submit-form method="get" action=(frontier_href(frontier_slug)) data-preserve-viewport="true" {
                input type="hidden" name="tab" value="metrics";
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
                                            log_y,
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
                                            log_y,
                                        ))
                                    }
                                }
                            }
                        }
                    }
                    aside.metric-picker-sidecar {
                        h4 { "Options" }
                        label.metric-checkbox-row.metric-checkbox-row-compact title=(if can_use_log_y {
                            "Positive-only filtered values. Toggles logarithmic scaling on the y axis."
                        } else {
                            "Logarithmic y scale is only available when all plotted values are strictly positive."
                        }) {
                            input type="checkbox" data-auto-submit="true" name="log_y" value="1" checked[log_y];
                            span.metric-checkbox-copy {
                                span.metric-checkbox-title { "Log Y" }
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
    log_y: bool,
) -> Markup {
    let selected = selected_metrics
        .iter()
        .any(|selected_metric| selected_metric.key == metric.key);
    let compatible = selected_families.supports(&metric.unit);
    let detail = format!("{} · {}", metric.objective.as_str(), metric.unit.as_str());
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
            FrontierTab::Metrics,
            replacement,
            log_y,
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
    log_y: bool,
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
        let Some((primary_min, primary_max)) = metric_chart_y_range(&primary_values, log_y) else {
            return chart_error_markup("metric values are non-finite");
        };
        let secondary_values = chart_series
            .iter()
            .filter(|series| series.side == MetricAxisSide::Secondary)
            .flat_map(|series| series.points.iter().map(|(_, value, _)| *value))
            .collect::<Vec<_>>();
        let secondary_range = if axes.secondary.is_some() {
            let Some(range) = metric_chart_y_range(&secondary_values, log_y) else {
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
                            Circle::new((*x, *value), 4, ShapeStyle::from(&series.color).filled())
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
                                6,
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
            ($chart:expr) => {{
                let chart = &mut $chart;
                if chart
                    .configure_mesh()
                    .light_line_style(RGBColor(223, 209, 189).mix(0.6))
                    .bold_line_style(RGBColor(207, 190, 168).mix(0.8))
                    .axis_style(RGBColor(103, 86, 63))
                    .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
                    .x_desc("close order")
                    .y_desc(axes.primary.unit.as_str())
                    .x_label_formatter(&|value| format!("{}", value + 1))
                    .draw()
                    .is_err()
                {
                    return chart_error_markup("mesh draw failed");
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
            ($chart:expr) => {{
                let chart = &mut $chart;
                if chart
                    .configure_mesh()
                    .light_line_style(RGBColor(223, 209, 189).mix(0.6))
                    .bold_line_style(RGBColor(207, 190, 168).mix(0.8))
                    .axis_style(RGBColor(103, 86, 63))
                    .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
                    .x_desc("close order")
                    .y_desc(axes.primary.unit.as_str())
                    .x_label_formatter(&|value| format!("{}", value + 1))
                    .draw()
                    .is_err()
                {
                    return chart_error_markup("mesh draw failed");
                }

                if let Some(secondary_axis) = axes.secondary.as_ref() {
                    if chart
                        .configure_secondary_axes()
                        .axis_style(RGBColor(103, 86, 63))
                        .label_style(("Iosevka Web", 12).into_font().color(&RGBColor(79, 71, 58)))
                        .y_desc(secondary_axis.unit.as_str())
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
            if log_y {
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
                draw_dual_chart!(chart);
            } else {
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
                draw_dual_chart!(chart);
            }
        } else if log_y {
            let mut chart = match ChartBuilder::on(&root)
                .margin(18)
                .x_label_area_size(32)
                .y_label_area_size(84)
                .build_cartesian_2d(0_i32..x_end, (primary_min..primary_max).log_scale())
            {
                Ok(chart) => chart,
                Err(error) => return chart_error_markup(&format!("chart build failed: {error:?}")),
            };
            draw_primary_chart!(chart);
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
            draw_primary_chart!(chart);
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
            (render_kv("State root", status.state_root.as_str()))
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
                h3 { "KPIs" }
                @if projection.kpis.is_empty() {
                    p.muted { "No frontier KPIs configured." }
                } @else {
                    div.table-scroll {
                        table.metric-table {
                            thead {
                                tr {
                                    th { "KPI" }
                                    th { "Metrics" }
                                    th { "Objective" }
                                }
                            }
                            tbody {
                                @for kpi in &projection.kpis {
                                    tr {
                                        td { (kpi.name) }
                                        td {
                                            div.chip-row {
                                                @for metric in &kpi.metrics {
                                                    span.tag-chip title=(format!("precedence {}", metric.precedence)) {
                                                        (metric.key)
                                                    }
                                                }
                                            }
                                        }
                                        td { (kpi.objective.as_str()) }
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
                                                std::slice::from_ref(metric),
                                                false,
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
    section.card.experiment-outcome {
        div.card-header.outcome-header {
            h2 { "Outcome" }
            div.fact-strip.outcome-verdict-strip {
                span.fact {
                    span.fact-label { "verdict" }
                    span class=(status_chip_classes(verdict_class(outcome.verdict))) {
                        (outcome.verdict.as_str())
                    }
                }
            }
        }
        section.subcard.narrative-block {
            h3 { "Rationale" }
            p.prose { (outcome.rationale) }
        }
        @if let Some(analysis) = outcome.analysis.as_ref() {
            (render_experiment_analysis(analysis))
        }
        (render_metric_panel("Primary metric", std::slice::from_ref(&outcome.primary_metric), outcome))
        @if !outcome.supporting_metrics.is_empty() {
            (render_metric_panel("Supporting metrics", &outcome.supporting_metrics, outcome))
        }
        (render_experiment_provenance(outcome))
    }
    }
}

fn render_open_experiment_outcome() -> Markup {
    html! {
    section.card.experiment-outcome {
        div.card-header.outcome-header {
            h2 { "Outcome" }
            div.fact-strip.outcome-verdict-strip {
                span.fact {
                    span.fact-label { "state" }
                    span class=(status_chip_classes(experiment_status_class(ExperimentStatus::Open))) {
                        "open"
                    }
                }
            }
        }
        p.muted { "No outcome recorded yet." }
    }
    }
}

fn render_experiment_analysis(analysis: &ExperimentAnalysis) -> Markup {
    html! {
    section.subcard.narrative-block {
        h3 { "Analysis" }
        p.prose { (analysis.summary) }
        div.code-block {
            (analysis.body)
        }
    }
    }
}

fn render_experiment_provenance(outcome: &ExperimentOutcome) -> Markup {
    html! {
    details.subcard.provenance-disclosure {
        summary.provenance-summary {
            span { "Provenance" }
            span.provenance-summary-facts {
                span { (outcome.backend.as_str()) }
                @if let Some(commit_hash) = outcome.commit_hash.as_ref() {
                    span { (short_commit_hash(commit_hash.as_str())) }
                }
                span { (format_timestamp(outcome.closed_at)) }
            }
        }
        div.provenance-body {
            div.fact-strip {
                (render_fact("backend", outcome.backend.as_str()))
                @if let Some(commit_hash) = outcome.commit_hash.as_ref() {
                    (render_fact("commit", commit_hash.as_str()))
                }
                (render_fact("closed", &format_timestamp(outcome.closed_at)))
            }
            (render_command_recipe(&outcome.command))
            @if !outcome.dimensions.is_empty() {
                (render_dimension_ledger("Dimensions", &outcome.dimensions))
            }
        }
    }
    }
}

fn render_command_recipe(command: &fidget_spinner_core::CommandRecipe) -> Markup {
    html! {
    div.provenance-block {
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

fn render_dimension_ledger(
    title: &str,
    dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> Markup {
    html! {
    div.provenance-block {
        h3 { (title) }
        div.table-scroll {
            table.metric-table {
                thead { tr { th { "Key" } th { "Value" } } }
                tbody {
                    @for (key, value) in dimensions {
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
    if parents.is_empty() && children.is_empty() {
        return html! {};
    }
    html! {
        section.card {
            h2 { "Influence Network" }
            div.split {
                @if !parents.is_empty() {
                    div.subcard {
                        h3 { "Parents" }
                        div.link-list {
                            @for parent in limit_items(parents, limit) {
                                (render_vertex_chip(parent))
                            }
                        }
                    }
                }
                @if !children.is_empty() {
                    div.subcard {
                        h3 { "Children" }
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
    if artifacts.is_empty() {
        return html! {};
    }
    html! {
    section.card {
        h2 { "Artifacts" }
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
    tab_bar: Option<Markup>,
    content: Markup,
) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                (render_favicon_links())
                base href=(&shell.base_href);
                title { (title) }
                style { (PreEscaped(styles())) }
            }
            body {
                main.shell data-refresh-token-url=(&shell.refresh_token_href) {
                    aside.sidebar {
                        (render_sidebar(shell))
                    }
                    div.main-column {
                        @if let Some(tab_bar) = tab_bar {
                            (tab_bar)
                        }
                        (content)
                    }
                }
                script { (PreEscaped(interaction_script())) }
            }
        }
    }
}

fn render_favicon_links() -> Markup {
    html! {
        link rel="icon" type="image/svg+xml" href="/favicon.svg";
        link rel="shortcut icon" href="/favicon.svg";
    }
}

fn interaction_script() -> String {
    format!(
        r#"
const UI_NAV_STATE_KEY = "{UI_NAV_STATE_KEY}";
const AUTO_REFRESH_INTERVAL_MS = 5000;
const PLOT_COPY_RESET_MS = 1600;
const plotCopyResetTimers = new WeakMap();

function stashViewportState() {{
    try {{
        const openDetails = Array.from(
            document.querySelectorAll("details[data-preserve-open][open][id]")
        ).map((details) => details.id);
        sessionStorage.setItem(
            UI_NAV_STATE_KEY,
            JSON.stringify({{
                path: window.location.pathname,
                scrollX: window.scrollX,
                scrollY: window.scrollY,
                openDetails,
            }})
        );
    }} catch (_error) {{
        // Best-effort only. If sessionStorage is unavailable we degrade to normal reload behavior.
    }}
}}

function restoreViewportState() {{
    let rawState = null;
    try {{
        rawState = sessionStorage.getItem(UI_NAV_STATE_KEY);
    }} catch (_error) {{
        return;
    }}
    if (!rawState) {{
        return;
    }}
    try {{
        sessionStorage.removeItem(UI_NAV_STATE_KEY);
    }} catch (_error) {{
        // Ignore removal failure and keep going with restoration.
    }}

    let state = null;
    try {{
        state = JSON.parse(rawState);
    }} catch (_error) {{
        return;
    }}
    if (!state || state.path !== window.location.pathname) {{
        return;
    }}
    if (Array.isArray(state.openDetails)) {{
        for (const detailsId of state.openDetails) {{
            const details = document.getElementById(detailsId);
            if (details instanceof HTMLDetailsElement) {{
                details.open = true;
            }}
        }}
    }}
    const scrollX = Number.isFinite(state.scrollX) ? state.scrollX : 0;
    const scrollY = Number.isFinite(state.scrollY) ? state.scrollY : 0;
    requestAnimationFrame(() => {{
        window.scrollTo(scrollX, scrollY);
        requestAnimationFrame(() => {{
            window.scrollTo(scrollX, scrollY);
        }});
    }});
}}

restoreViewportState();

function plotCopyOriginalLabel(button) {{
    if (!button.dataset.copyLabel) {{
        button.dataset.copyLabel = button.textContent?.trim() || "Copy PNG";
    }}
    return button.dataset.copyLabel;
}}

function cancelPlotCopyReset(button) {{
    const existingTimer = plotCopyResetTimers.get(button);
    if (existingTimer) {{
        clearTimeout(existingTimer);
        plotCopyResetTimers.delete(button);
    }}
}}

function setPlotCopyButtonState(button, label, state, title) {{
    cancelPlotCopyReset(button);
    plotCopyOriginalLabel(button);
    button.textContent = label;
    button.toggleAttribute("data-copied", state === "copied");
    button.toggleAttribute("data-failed", state === "failed");
    if (title) {{
        button.title = title;
    }} else {{
        button.removeAttribute("title");
    }}
}}

function resetPlotCopyButton(button) {{
    button.textContent = plotCopyOriginalLabel(button);
    button.removeAttribute("data-copied");
    button.removeAttribute("data-failed");
    button.removeAttribute("title");
}}

function schedulePlotCopyReset(button) {{
    cancelPlotCopyReset(button);
    const timer = setTimeout(() => {{
        resetPlotCopyButton(button);
        plotCopyResetTimers.delete(button);
    }}, PLOT_COPY_RESET_MS);
    plotCopyResetTimers.set(button, timer);
}}

function autoRefreshRoot() {{
    return document.querySelector("[data-refresh-token-url]");
}}

function autoRefreshDeferred() {{
    return Boolean(
        document.hidden
        || document.querySelector("details.control-popout[open]")
        || document.querySelector("button[data-copy-plot-png=\"true\"]:disabled")
    );
}}

async function pollRefreshToken() {{
    const root = autoRefreshRoot();
    if (!(root instanceof HTMLElement) || autoRefreshDeferred()) {{
        return;
    }}
    const tokenUrl = root.dataset.refreshTokenUrl;
    if (!tokenUrl) {{
        return;
    }}
    try {{
        const response = await fetch(tokenUrl, {{
            cache: "no-store",
            headers: {{ "Accept": "text/plain" }},
        }});
        if (!response.ok) {{
            return;
        }}
        const nextToken = (await response.text()).trim();
        if (!nextToken) {{
            return;
        }}
        const previousToken = root.dataset.refreshToken;
        if (!previousToken) {{
            root.dataset.refreshToken = nextToken;
            return;
        }}
        if (previousToken !== nextToken) {{
            stashViewportState();
            window.location.reload();
        }}
    }} catch (_error) {{
        // Auto-refresh must never degrade the navigator if the probe races shutdown.
    }}
}}

async function rasterizeSvgToPngBlob(svg) {{
    const svgClone = svg.cloneNode(true);
    if (svgClone instanceof SVGElement && !svgClone.getAttribute("xmlns")) {{
        svgClone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    }}
    const viewBox = svg.viewBox?.baseVal;
    const width = Math.ceil(
        (viewBox && viewBox.width) || Number(svg.getAttribute("width")) || svg.clientWidth
    );
    const height = Math.ceil(
        (viewBox && viewBox.height) || Number(svg.getAttribute("height")) || svg.clientHeight
    );
    if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {{
        throw new Error("plot dimensions are unavailable");
    }}

    const svgText = new XMLSerializer().serializeToString(svgClone);
    const svgBlob = new Blob([svgText], {{ type: "image/svg+xml;charset=utf-8" }});
    const svgUrl = URL.createObjectURL(svgBlob);
    try {{
        const image = new Image();
        const imageLoaded = new Promise((resolve, reject) => {{
            image.onload = resolve;
            image.onerror = () => reject(new Error("plot image rasterization failed"));
        }});
        image.src = svgUrl;
        await imageLoaded;

        const canvas = document.createElement("canvas");
        canvas.width = width;
        canvas.height = height;
        const context = canvas.getContext("2d");
        if (!context) {{
            throw new Error("canvas rendering is unavailable");
        }}
        context.drawImage(image, 0, 0, width, height);
        return await new Promise((resolve, reject) => {{
            canvas.toBlob((blob) => {{
                if (blob) {{
                    resolve(blob);
                }} else {{
                    reject(new Error("PNG encoding failed"));
                }}
            }}, "image/png");
        }});
    }} finally {{
        URL.revokeObjectURL(svgUrl);
    }}
}}

async function copyPlotPng(button) {{
    if (!navigator.clipboard || typeof ClipboardItem === "undefined") {{
        throw new Error("PNG clipboard is unavailable in this browser");
    }}
    const frame = button.closest(".chart-frame");
    const svg = frame?.querySelector("svg");
    if (!(svg instanceof SVGSVGElement)) {{
        throw new Error("plot SVG was not found");
    }}
    const pngBlob = await rasterizeSvgToPngBlob(svg);
    await navigator.clipboard.write([
        new ClipboardItem({{ "image/png": pngBlob }}),
    ]);
}}

function inlineRenameInput(form) {{
    const input = form.querySelector("[data-inline-rename-input=\"true\"]");
    return input instanceof HTMLInputElement ? input : null;
}}

function openInlineRename(form) {{
    const input = inlineRenameInput(form);
    if (!input) {{
        return;
    }}
    const original = form.dataset.originalName || input.defaultValue || input.value;
    form.dataset.originalName = original;
    input.value = original;
    form.classList.add("editing");
    window.requestAnimationFrame(() => {{
        input.focus();
        input.select();
    }});
}}

function closeInlineRename(form) {{
    const input = inlineRenameInput(form);
    if (input) {{
        input.value = form.dataset.originalName || input.defaultValue || "";
    }}
    form.classList.remove("editing");
}}

function prepareInlineRenameSubmit(form, event) {{
    const input = inlineRenameInput(form);
    if (!input) {{
        return;
    }}
    const original = form.dataset.originalName || input.defaultValue || "";
    const next = input.value.trim();
    if (!next || next === original) {{
        event.preventDefault();
        closeInlineRename(form);
        return;
    }}
    input.value = next;
}}

window.setInterval(pollRefreshToken, AUTO_REFRESH_INTERVAL_MS);
window.addEventListener("focus", pollRefreshToken);
document.addEventListener("visibilitychange", () => {{
    if (!document.hidden) {{
        pollRefreshToken();
    }}
}});
pollRefreshToken();

document.addEventListener("click", (event) => {{
    const target = event.target;
    if (!(target instanceof Element)) {{
        return;
    }}
    const copyButton = target.closest("button[data-copy-plot-png=\"true\"]");
    if (copyButton instanceof HTMLButtonElement) {{
        copyButton.disabled = true;
        setPlotCopyButtonState(copyButton, "Copying...", "busy");
        copyPlotPng(copyButton)
            .then(() => {{
                setPlotCopyButtonState(copyButton, "Copied", "copied");
            }})
            .catch((error) => {{
                setPlotCopyButtonState(
                    copyButton,
                    "Copy failed",
                    "failed",
                    error?.message || "Copy failed"
                );
            }})
            .finally(() => {{
                copyButton.disabled = false;
                schedulePlotCopyReset(copyButton);
        }});
        return;
    }}
    const renameButton = target.closest("button[data-inline-rename-trigger=\"true\"]");
    if (renameButton instanceof HTMLButtonElement) {{
        const form = renameButton.closest("form[data-inline-rename-form=\"true\"]");
        if (form instanceof HTMLFormElement) {{
            openInlineRename(form);
        }}
        return;
    }}
    for (const renameForm of document.querySelectorAll("form[data-inline-rename-form=\"true\"].editing")) {{
        if (!renameForm.contains(target) && renameForm instanceof HTMLFormElement) {{
            closeInlineRename(renameForm);
        }}
    }}
    const navigationLink = target.closest("a[data-preserve-viewport=\"true\"]");
    if (
        navigationLink instanceof HTMLAnchorElement
        && event.button === 0
        && !event.defaultPrevented
        && !event.metaKey
        && !event.ctrlKey
        && !event.shiftKey
        && !event.altKey
        && (!navigationLink.target || navigationLink.target === "_self")
    ) {{
        stashViewportState();
    }}
    for (const popout of document.querySelectorAll("details.control-popout[open]")) {{
        if (!popout.contains(target)) {{
            popout.removeAttribute("open");
        }}
    }}
}});

document.addEventListener("submit", (event) => {{
    const target = event.target;
    if (!(target instanceof HTMLFormElement)) {{
        return;
    }}
    if (target.hasAttribute("data-inline-rename-form")) {{
        prepareInlineRenameSubmit(target, event);
        if (event.defaultPrevented) {{
            return;
        }}
    }}
    if (!target.hasAttribute("data-preserve-viewport")) {{
        return;
    }}
    stashViewportState();
}});

document.addEventListener("keydown", (event) => {{
    const target = event.target;
    if (
        target instanceof HTMLInputElement
        && target.hasAttribute("data-inline-rename-input")
    ) {{
        const form = target.closest("form[data-inline-rename-form=\"true\"]");
        if (form instanceof HTMLFormElement && event.key === "Escape") {{
            event.preventDefault();
            closeInlineRename(form);
            return;
        }}
        if (form instanceof HTMLFormElement && event.key === "Enter") {{
            prepareInlineRenameSubmit(form, event);
        }}
    }}
    if (event.key !== "Escape") {{
        return;
    }}
    for (const popout of document.querySelectorAll("details.control-popout[open]")) {{
        popout.removeAttribute("open");
    }}
}});

document.addEventListener("change", (event) => {{
    const target = event.target;
    if (!(target instanceof HTMLElement)) {{
        return;
    }}
    if (!target.hasAttribute("data-auto-submit")) {{
        return;
    }}
    const form = target.closest("form");
    if (!(form instanceof HTMLFormElement)) {{
        return;
    }}
    form.requestSubmit();
}});
"#
    )
}

fn render_metric_table_title_link(title: &NonEmptyText, href: &str) -> Markup {
    html! {
        a href=(href) class="metric-table-link" title=(title.as_str()) {
            (title.as_str())
        }
    }
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
    dimension_filters: &BTreeMap<String, String>,
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
    for (key, value) in dimension_filters {
        href.push_str("&dim.");
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

fn artifact_href(slug: &Slug) -> String {
    format!("artifact/{}", encode_path_segment(slug.as_str()))
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum MetricUnitFamily {
    Time,
    Exact(MetricUnit),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MetricAxisFamilies {
    families: Vec<MetricUnitFamily>,
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

impl MetricUnitFamily {
    fn from_unit(unit: &MetricUnit) -> Self {
        match unit.known_kind() {
            Some(
                KnownMetricUnit::Nanoseconds
                | KnownMetricUnit::Microseconds
                | KnownMetricUnit::Milliseconds
                | KnownMetricUnit::Seconds,
            ) => Self::Time,
            _ => Self::Exact(unit.clone()),
        }
    }

    fn supports(&self, unit: &MetricUnit) -> bool {
        match self {
            Self::Time => matches!(
                unit.known_kind(),
                Some(
                    KnownMetricUnit::Nanoseconds
                        | KnownMetricUnit::Microseconds
                        | KnownMetricUnit::Milliseconds
                        | KnownMetricUnit::Seconds
                )
            ),
            Self::Exact(expected) => expected == unit,
        }
    }
}

impl MetricAxisFamilies {
    fn from_metrics(metrics: &[fidget_spinner_store_sqlite::MetricKeySummary]) -> Self {
        let mut families = Self::default();
        for metric in metrics {
            let _ = families.admit(MetricUnitFamily::from_unit(&metric.unit));
        }
        families
    }

    fn admit(&mut self, family: MetricUnitFamily) -> bool {
        if self.families.iter().any(|active| active == &family) {
            return true;
        }
        if self.families.len() >= 2 {
            return false;
        }
        self.families.push(family);
        true
    }

    fn supports(&self, unit: &MetricUnit) -> bool {
        self.families.len() < 2 || self.families.iter().any(|family| family.supports(unit))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MetricChartAxis {
    unit: MetricUnit,
    family: MetricUnitFamily,
}

impl MetricChartAxis {
    fn from_metric(metric: &fidget_spinner_store_sqlite::MetricKeySummary) -> Self {
        Self {
            family: MetricUnitFamily::from_unit(&metric.unit),
            unit: metric.unit.clone(),
        }
    }

    fn normalize_value(&self, value: f64, unit: &MetricUnit) -> Option<f64> {
        match &self.family {
            MetricUnitFamily::Time => convert_time_metric_value(value, unit, &self.unit),
            MetricUnitFamily::Exact(expected) if expected == unit => Some(value),
            MetricUnitFamily::Exact(_) => None,
        }
    }
}

impl MetricAxisSet {
    fn from_series(series: &[&FilteredMetricSeries<'_>]) -> Option<Self> {
        let primary = MetricChartAxis::from_metric(series.first()?.metric);
        let secondary = series
            .iter()
            .map(|series| MetricChartAxis::from_metric(series.metric))
            .find(|axis| axis.family != primary.family);
        Some(Self { primary, secondary })
    }

    fn axis_for_metric(
        &self,
        metric: &fidget_spinner_store_sqlite::MetricKeySummary,
    ) -> Option<(MetricAxisSide, &MetricChartAxis)> {
        let family = MetricUnitFamily::from_unit(&metric.unit);
        if family == self.primary.family {
            return Some((MetricAxisSide::Primary, &self.primary));
        }
        self.secondary
            .as_ref()
            .filter(|axis| axis.family == family)
            .map(|axis| (MetricAxisSide::Secondary, axis))
    }
}

fn convert_time_metric_value(value: f64, from: &MetricUnit, to: &MetricUnit) -> Option<f64> {
    let nanoseconds = match from.known_kind()? {
        KnownMetricUnit::Nanoseconds => value,
        KnownMetricUnit::Microseconds => value * 1_000.0,
        KnownMetricUnit::Milliseconds => value * 1_000_000.0,
        KnownMetricUnit::Seconds => value * 1_000_000_000.0,
        _ => return None,
    };
    Some(match to.known_kind()? {
        KnownMetricUnit::Nanoseconds => nanoseconds,
        KnownMetricUnit::Microseconds => nanoseconds / 1_000.0,
        KnownMetricUnit::Milliseconds => nanoseconds / 1_000_000.0,
        KnownMetricUnit::Seconds => nanoseconds / 1_000_000_000.0,
        _ => return None,
    })
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
            input type="hidden" name=(format!("dim.{key}")) value=(value);
        }
    }
}

fn render_log_hidden_input(log_y: bool) -> Markup {
    html! {
        @if log_y {
            input type="hidden" name="log_y" value="1";
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
    log_y: bool,
) -> String {
    let mut label = if selected_metrics.len() <= 1 {
        "Metric".to_owned()
    } else {
        format!("Metrics {}", selected_metrics.len())
    };
    if log_y {
        label.push_str(" · log");
    }
    label
}

fn metric_chart_supports_log_y(axes: &MetricAxisSet, series: &[&FilteredMetricSeries<'_>]) -> bool {
    let mut saw_value = false;
    for series in series {
        let Some((_, axis)) = axes.axis_for_metric(series.metric) else {
            return false;
        };
        for point in &series.points {
            let Some(value) = axis.normalize_value(point.value, &series.metric.unit) else {
                return false;
            };
            saw_value = true;
            if value <= 0.0 || !value.is_finite() {
                return false;
            }
        }
    }
    saw_value
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
                    let value = axis.normalize_value(point.value, &series.metric.unit)?;
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

fn best_metric_table_title_split(
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

fn truncated_entry_count(lengths: &[usize], budget: usize) -> usize {
    lengths.iter().filter(|&&length| length > budget).count()
}

fn truncated_overflow_chars(lengths: &[usize], budget: usize) -> usize {
    lengths
        .iter()
        .map(|&length| length.saturating_sub(budget))
        .sum()
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
        padding: 18px 20px 34px;
        display: grid;
        gap: 16px;
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
        padding: 14px;
        display: grid;
        gap: 12px;
        box-shadow: 0 1px 0 var(--shadow);
    }
    .sidebar-project {
        display: grid;
        gap: 7px;
    }
    .sidebar-title-row {
        display: flex;
        gap: 8px;
        align-items: baseline;
        justify-content: space-between;
        min-width: 0;
    }
    .sidebar-home {
        color: var(--text);
        font-size: 18px;
        font-weight: 700;
        min-width: 0;
        overflow-wrap: anywhere;
    }
    .sidebar-home-chip {
        flex: 0 0 auto;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--accent);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        padding: 2px 6px;
        text-transform: uppercase;
    }
    .sidebar-tags {
        padding: 3px 7px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .sidebar-actions {
        display: inline-flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
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
    .frontier-nav-item {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 6px;
        align-items: stretch;
        min-width: 0;
    }
    .frontier-nav-link {
        display: grid;
        gap: 4px;
        padding: 10px 12px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        min-width: 0;
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
    .frontier-action-form {
        display: grid;
        margin: 0;
        align-self: stretch;
    }
    .frontier-action-button {
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        cursor: pointer;
        display: grid;
        place-items: center;
        height: 100%;
        min-width: 30px;
        padding: 0;
        user-select: none;
    }
    .frontier-action-button:hover {
        color: var(--text);
        border-color: var(--border-strong);
    }
    .frontier-action-icon {
        width: 17px;
        height: 17px;
        stroke: currentColor;
        stroke-width: 1.8;
        stroke-linecap: round;
        stroke-linejoin: round;
    }
    .sidebar-archived {
        display: grid;
        gap: 8px;
    }
    .sidebar-archived-toggle {
        color: var(--muted);
        cursor: pointer;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        user-select: none;
    }
    .sidebar-archived-list {
        margin-top: 8px;
    }
    .main-column {
        display: grid;
        gap: 12px;
        min-width: 0;
    }
    .tag-family-grid {
        display: grid;
        gap: 10px;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }
    .tag-create-form,
    .tag-inline-form {
        display: flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
        margin: 0;
    }
    .family-policy-row {
        display: inline-flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
        justify-content: flex-end;
    }
    .tag-identity-row,
    .tag-icon-form,
    .tag-inline-rename-form {
        display: inline-flex;
        gap: 5px;
        align-items: center;
        min-width: 0;
        margin: 0;
    }
    .tag-inline-rename-form {
        gap: 4px;
    }
    .inline-icon-button {
        display: grid;
        place-items: center;
        width: 24px;
        height: 24px;
        flex: 0 0 24px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        cursor: pointer;
        padding: 0;
    }
    .inline-icon-button:hover {
        border-color: var(--border-strong);
        color: var(--text);
    }
    .danger-icon-button {
        color: var(--rejected);
    }
    .inline-action-icon {
        width: 14px;
        height: 14px;
        stroke: currentColor;
        stroke-width: 1.8;
        stroke-linecap: round;
        stroke-linejoin: round;
    }
    .inline-rename-input {
        display: none;
        width: min(240px, 42vw);
    }
    .tag-inline-rename-form.editing .tag-chip,
    .tag-inline-rename-form.editing .inline-icon-button {
        display: none;
    }
    .tag-inline-rename-form.editing .inline-rename-input {
        display: inline-block;
    }
    .compact-input,
    .compact-select,
    .inline-rename-input {
        min-width: 0;
        max-width: 180px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font: inherit;
        font-size: 12px;
        padding: 5px 7px;
    }
    .compact-select {
        max-width: 150px;
    }
    .wide-compact-select {
        max-width: 360px;
        width: min(360px, 64vw);
    }
    .wide-compact-input {
        max-width: 280px;
        width: min(280px, 42vw);
    }
    .form-button {
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--accent);
        cursor: pointer;
        font: inherit;
        font-size: 11px;
        font-weight: 700;
        padding: 5px 7px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .form-button:hover {
        border-color: var(--border-strong);
        color: var(--text);
    }
    .form-button:disabled,
    .inline-icon-button:disabled,
    .compact-input:disabled,
    .compact-select:disabled {
        cursor: not-allowed;
        opacity: 0.48;
    }
    .danger-button {
        color: var(--rejected);
    }
    .inline-check {
        display: inline-flex;
        gap: 5px;
        align-items: center;
        color: var(--muted);
        font-size: 12px;
    }
    .table-wrap {
        width: 100%;
        overflow-x: auto;
    }
    .dense-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: auto;
    }
    .dense-table th,
    .dense-table td {
        border-bottom: 1px solid var(--border);
        padding: 7px 8px;
        text-align: left;
        vertical-align: top;
    }
    .dense-table th {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .dense-table td {
        overflow-wrap: anywhere;
    }
    .dense-table .no-truncate {
        white-space: nowrap;
        overflow-wrap: normal;
    }
    .tag-history-list {
        display: grid;
        gap: 7px;
    }
    .tag-history-row {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
    }
    .page-header {
        display: grid;
        gap: 6px;
        padding: 12px 14px;
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
        font-size: clamp(18px, 1.9vw, 24px);
        line-height: 1.15;
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
        padding: 14px 16px;
        display: grid;
        gap: 10px;
        box-shadow: 0 1px 0 var(--shadow);
        min-width: 0;
    }
    .subcard {
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 10px 12px;
        display: grid;
        gap: 8px;
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
    h1 { font-size: 18px; }
    h2 { font-size: 16px; }
    h3 { font-size: 13px; color: #4f473a; }
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
        gap: 6px 12px;
        align-items: center;
        font-size: 13px;
    }
    .kv-grid {
        display: grid;
        gap: 6px 12px;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
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
    .fact-strip {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 16px;
        align-items: center;
        min-width: 0;
    }
    .tag-state-card {
        padding-block: 14px;
    }
    .tag-state-band {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px 18px;
        min-width: 0;
        flex-wrap: wrap;
    }
    .tag-state-controls {
        display: inline-flex;
        align-items: center;
        justify-content: flex-end;
        gap: 8px;
        flex-wrap: wrap;
        margin-left: auto;
    }
    .tag-lock-switch-form {
        margin: 0;
    }
    .tag-lock-switch {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        cursor: pointer;
        font: inherit;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        padding: 4px 7px;
        text-transform: uppercase;
        white-space: nowrap;
    }
    .tag-lock-switch:hover {
        border-color: var(--border-strong);
        color: var(--text);
    }
    .tag-lock-switch.locked {
        border-color: color-mix(in srgb, var(--rejected) 45%, var(--border));
        color: var(--rejected);
        background: color-mix(in srgb, var(--rejected) 8%, var(--panel));
    }
    .switch-track {
        position: relative;
        width: 24px;
        height: 12px;
        border: 1px solid currentColor;
        background: var(--panel-2);
    }
    .switch-thumb {
        position: absolute;
        top: 2px;
        left: 2px;
        width: 6px;
        height: 6px;
        background: currentColor;
    }
    .tag-lock-switch.locked .switch-thumb {
        left: 14px;
    }
    .switch-state {
        color: var(--muted);
    }
    .fact {
        display: inline-flex;
        gap: 5px;
        align-items: baseline;
        min-width: 0;
        white-space: nowrap;
    }
    .fact-label {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .fact-value {
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .outcome-header {
        align-items: center;
        justify-content: space-between;
    }
    .outcome-verdict-strip {
        margin-left: auto;
    }
    .narrative-block {
        background: color-mix(in srgb, var(--panel-2) 70%, var(--panel));
    }
    .provenance-disclosure {
        align-content: start;
    }
    .provenance-summary {
        display: flex;
        gap: 8px 14px;
        align-items: center;
        justify-content: space-between;
        cursor: pointer;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        user-select: none;
    }
    .provenance-summary::-webkit-details-marker {
        display: none;
    }
    .provenance-summary::before {
        content: ">";
        color: var(--accent);
    }
    .provenance-disclosure[open] > .provenance-summary::before {
        content: "v";
    }
    .provenance-summary-facts {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        justify-content: flex-end;
        color: var(--muted);
        text-transform: none;
        letter-spacing: normal;
    }
    .provenance-body {
        display: grid;
        gap: 10px;
    }
    .provenance-block {
        display: grid;
        gap: 8px;
        min-width: 0;
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
    .plot-card-header {
        align-items: center;
    }
    .plot-toolbar {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
        margin-left: auto;
    }
    .control-popout {
        position: relative;
    }
    .control-popout[open] {
        z-index: 4;
    }
    .control-popout-toggle {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 7px 11px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--text);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        cursor: pointer;
        list-style: none;
        user-select: none;
    }
    .control-popout-toggle::-webkit-details-marker {
        display: none;
    }
    .control-popout[open] > .control-popout-toggle {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .control-popout-panel {
        position: absolute;
        top: calc(100% + 8px);
        right: 0;
        width: min(520px, calc(100vw - 80px));
        max-height: min(72vh, 640px);
        overflow-y: auto;
        border: 1px solid var(--border-strong);
        background: var(--panel);
        padding: 14px 16px;
        display: grid;
        gap: 12px;
        box-shadow: 0 16px 36px rgba(83, 61, 33, 0.16);
    }
    .metric-popout-panel {
        width: min(760px, calc(100vw - 80px));
    }
    .metric-picker-form,
    .metric-picker-groups {
        display: grid;
        gap: 12px;
    }
    .metric-popout-layout {
        display: grid;
        gap: 14px;
        grid-template-columns: minmax(0, 1.6fr) minmax(180px, 0.8fr);
        align-items: start;
    }
    .metric-picker-main,
    .metric-picker-sidecar {
        display: grid;
        gap: 10px;
    }
    .metric-picker-group {
        display: grid;
        gap: 8px;
    }
    .metric-picker-group h4,
    .metric-picker-sidecar h4 {
        margin: 0;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-picker-disclosure {
        display: grid;
        gap: 8px;
    }
    .metric-picker-disclosure-toggle {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        cursor: pointer;
        user-select: none;
    }
    .metric-picker-list {
        display: grid;
        gap: 6px;
    }
    .metric-checkbox-row {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr);
        gap: 8px;
        align-items: center;
        padding: 6px 9px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        min-width: 0;
    }
    .metric-checkbox-row:hover {
        text-decoration: none;
        border-color: var(--border-strong);
    }
    .metric-checkbox-row.selected {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .metric-checkbox-row.incompatible {
        opacity: 0.55;
    }
    .metric-checkbox-row input {
        margin: 0;
    }
    .metric-checkbox-copy {
        display: block;
        min-width: 0;
    }
    .metric-checkbox-title {
        color: var(--text);
        font-weight: 700;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .metric-checkbox-row-compact {
        align-self: start;
    }
    .compact-note {
        margin: 0;
        font-size: 12px;
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
        width: 100%;
        min-width: 0;
        border-collapse: collapse;
        table-layout: auto;
        font-size: 13px;
    }
    .metric-table-fit-col {
        width: 1%;
    }
    .metric-table-title-col {
        min-width: 0;
    }
    .table-scroll {
        width: 100%;
        min-width: 0;
        overflow-x: hidden;
    }
    .metric-table th,
    .metric-table td {
        padding: 7px 8px;
        border-top: 1px solid var(--border);
        text-align: left;
        vertical-align: top;
        white-space: nowrap;
        min-width: 0;
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
    .metric-table-fit-heading,
    .metric-table-rank-cell,
    .metric-table-closed-cell,
    .metric-table-verdict-cell,
    .metric-table-value-cell {
        width: 1%;
    }
    .metric-table-title-heading {
        overflow: hidden;
    }
    .metric-table-title-cell {
        max-width: 0;
        overflow: hidden;
    }
    .metric-table-link {
        display: block;
        width: 100%;
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        vertical-align: top;
    }
    .metric-table-fixed-text {
        display: inline;
    }
    .metric-table-verdict-chip {
        max-width: none;
    }
    .related-block {
        display: grid;
        gap: 8px;
    }
    .chart-frame {
        position: relative;
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 8px;
        overflow: hidden;
    }
    .chart-frame svg {
        display: block;
        width: 100%;
        height: auto;
    }
    .chart-action-row {
        position: absolute;
        top: 14px;
        right: 14px;
        z-index: 2;
        display: flex;
        align-items: center;
    }
    .plot-copy-png {
        border: 1px solid var(--border-strong);
        background: color-mix(in srgb, var(--panel) 92%, white);
        color: var(--text);
        padding: 6px 9px;
        font: inherit;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        cursor: pointer;
        box-shadow: 0 8px 18px rgba(83, 61, 33, 0.12);
    }
    .plot-copy-png:disabled {
        cursor: wait;
        opacity: 0.65;
    }
    .plot-copy-png[data-copied] {
        color: var(--accepted);
        border-color: color-mix(in srgb, var(--accepted) 24%, white);
    }
    .plot-copy-png[data-failed] {
        color: var(--rejected);
        border-color: color-mix(in srgb, var(--rejected) 24%, white);
    }
    .metric-table-section {
        margin-top: 2px;
    }
    .metric-table-header {
        display: flex;
        gap: 10px;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
    }
    .metric-table-tabs {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }
    .metric-table-tab {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-table-tab.active {
        color: var(--text);
        border-color: var(--border-strong);
        background: var(--accent-soft);
        font-weight: 700;
    }
    .metric-table-caption {
        margin: 0;
        font-size: 12px;
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
        .plot-toolbar {
            width: 100%;
            margin-left: 0;
        }
    }
    @media (max-width: 720px) {
        .shell { padding: 12px; }
        .card, .page-header { padding: 14px; }
        .subcard, .mini-card { padding: 12px; }
        .card-grid, .split, .kv-grid { grid-template-columns: 1fr; }
        .page-title { font-size: 18px; }
        .control-popout {
            width: 100%;
        }
        .control-popout-toggle {
            width: 100%;
            justify-content: center;
        }
        .control-popout-panel,
        .metric-popout-panel {
            position: static;
            width: 100%;
            max-height: none;
            margin-top: 8px;
            box-shadow: 0 1px 0 var(--shadow);
        }
        .metric-popout-layout {
            grid-template-columns: 1fr;
        }
    }
    "#
}

#[cfg(test)]
mod tests {
    use super::{
        FrontierPageQuery, METRIC_TABLE_TITLE_MIN_BUDGET_CH, MetricChartAxis,
        best_metric_table_title_split, harden_autofill_controls, render_metric_series_section,
        resolve_selected_metric_keys, truncated_entry_count,
    };
    use std::collections::BTreeMap;

    use fidget_spinner_core::{
        ExperimentStatus, FrontierBrief, FrontierId, FrontierRecord, FrontierStatus,
        FrontierVerdict, HypothesisId, MetricAggregation, MetricUnit, MetricVisibility,
        NonEmptyText, OptimizationObjective, Slug,
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
            unit,
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Minimize,
            visibility: MetricVisibility::Canonical,
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
            archived: false,
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
            archived: false,
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
    fn frontier_page_query_accepts_legacy_single_metric_selector() {
        let query = must(
            FrontierPageQuery::parse(Some("tab=metrics&metric=presolve_ms_gmean")),
            "query should parse",
        );
        assert_eq!(query.tab.as_deref(), Some("metrics"));
        assert_eq!(query.metric, vec!["presolve_ms_gmean".to_owned()]);
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
