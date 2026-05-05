use super::detail::{render_experiment_detail, render_frontier_detail, render_hypothesis_detail};
use super::registry::{
    render_project_home, render_project_index, render_project_metrics, render_project_tags,
};
use super::{
    AssignTagFamilyRequest, CONTENT_TYPE, CreateKpiRequest, CreateTagFamilyRequest,
    DefineMetricRequest, DefineSyntheticMetricRequest, DeleteKpiReferenceRequest, DeleteKpiRequest,
    DeleteMetricRequest, DeleteTagRequest, FAVICON_SVG, Form, FrontierPageQuery, FrontierStatus,
    IntoResponse, MergeMetricRequest, MergeTagRequest, MetricDisplayUnit, MetricUnit,
    MoveKpiDirection, MoveKpiRequest, NavigatorScope, NavigatorState, NonEmptyText, Path,
    ProjectMetricsQuery, ProjectRenderContext, RegistryLockMode, RegistryName, RenameMetricRequest,
    RenameTagRequest, Response, Router, SetFrontierRegistryLockRequest, SetKpiReferenceRequest,
    SetRegistryLockRequest, SetTagFamilyMandatoryRequest, SocketAddr, State, StatusCode,
    StoreError, SyntheticMetricExpression, TagFamilyName, TagName, UpdateFrontierRequest, Uri,
    frontier_href, frontier_status_mutation_response, get, io, metric_mutation_response,
    metrics_frontier_href, open_store, optional_text_field, parse_metric_aggregation_ui,
    parse_metric_dimension_ui, parse_optimization_objective_ui, parse_ui_lock_mode, post,
    project_mutation_response, project_refresh_token_for, refresh_token_response, render_response,
    resolve_project_context, tag_mutation_response, text_patch_field, update_frontier_status,
    update_project_description,
};
use serde::Deserialize;

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
            .route("/description", post(root_project_description))
            .route("/project/{project}/description", post(project_description))
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
            .route(
                "/project/{project}/metrics/synthetic/create",
                post(create_synthetic_metric),
            )
            .route("/project/{project}/metrics/rename", post(rename_metric))
            .route(
                "/project/{project}/metrics/description",
                post(update_metric_description),
            )
            .route("/project/{project}/metrics/merge", post(merge_metric))
            .route("/project/{project}/metrics/delete", post(delete_metric))
            .route("/project/{project}/metrics/kpi", post(create_kpi))
            .route("/project/{project}/metrics/kpi/lock", post(set_kpi_lock))
            .route("/project/{project}/metrics/kpi/move", post(move_kpi))
            .route(
                "/project/{project}/metrics/kpi/reference",
                post(set_kpi_reference),
            )
            .route(
                "/project/{project}/metrics/kpi/reference/delete",
                post(delete_kpi_reference),
            )
            .route("/project/{project}/metrics/kpi/delete", post(delete_kpi))
            .route(
                "/project/{project}/frontier/{selector}",
                get(frontier_detail),
            )
            .route(
                "/project/{project}/frontier/{selector}/summary",
                post(update_frontier_summary),
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

#[derive(Debug, Deserialize)]
pub(super) struct ProjectDescriptionForm {
    pub(super) description: String,
}

async fn root_project_description(
    State(state): State<NavigatorState>,
    Form(form): Form<ProjectDescriptionForm>,
) -> Response {
    match &state.scope {
        NavigatorScope::Single(project_root) => {
            project_mutation_response(update_project_description(
                ProjectRenderContext::root(project_root.clone(), state.limit),
                form,
            ))
        }
        NavigatorScope::Multi { .. } => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn project_description(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<ProjectDescriptionForm>,
) -> Response {
    project_mutation_response(
        resolve_project_context(&state, &project)
            .and_then(|context| update_project_description(context, form)),
    )
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
    dimension: String,
    display_unit: String,
    aggregation: String,
    objective: String,
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
                dimension: parse_metric_dimension_ui(&form.dimension)?,
                display_unit: if form.display_unit.trim().is_empty() {
                    None
                } else {
                    Some(MetricUnit::new(form.display_unit)?)
                },
                aggregation: parse_metric_aggregation_ui(&form.aggregation)?,
                objective: parse_optimization_objective_ui(&form.objective)?,
                description: optional_text_field(form.description)?,
            })?;
            Ok(format!("{}metrics", context.base_href))
        }),
    )
}

#[derive(Deserialize)]
struct CreateSyntheticMetricForm {
    key: String,
    operation: String,
    left: String,
    right: String,
    term_3: String,
    term_4: String,
    aggregation: String,
    objective: String,
    description: String,
}

async fn create_synthetic_metric(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<CreateSyntheticMetricForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let expression = synthetic_expression_from_form(&form)?;
            let _ = store.define_synthetic_metric(DefineSyntheticMetricRequest {
                key: NonEmptyText::new(form.key)?,
                expression,
                aggregation: parse_metric_aggregation_ui(&form.aggregation)?,
                objective: parse_optimization_objective_ui(&form.objective)?,
                description: optional_text_field(form.description)?,
            })?;
            Ok(format!("{}metrics", context.base_href))
        }),
    )
}

fn synthetic_expression_from_form(
    form: &CreateSyntheticMetricForm,
) -> Result<SyntheticMetricExpression, StoreError> {
    let left = synthetic_metric_operand(&form.left)?;
    match form.operation.trim() {
        "add" => Ok(SyntheticMetricExpression::Add {
            left: Box::new(left),
            right: Box::new(synthetic_metric_operand(&form.right)?),
        }),
        "sub" => Ok(SyntheticMetricExpression::Sub {
            left: Box::new(left),
            right: Box::new(synthetic_metric_operand(&form.right)?),
        }),
        "mul" => Ok(SyntheticMetricExpression::Mul {
            left: Box::new(left),
            right: Box::new(synthetic_metric_operand(&form.right)?),
        }),
        "div" => Ok(SyntheticMetricExpression::Div {
            left: Box::new(left),
            right: Box::new(synthetic_metric_operand(&form.right)?),
        }),
        "gmean" => {
            let mut terms = vec![left];
            for raw in [&form.right, &form.term_3, &form.term_4] {
                if !raw.trim().is_empty() {
                    terms.push(synthetic_metric_operand(raw)?);
                }
            }
            Ok(SyntheticMetricExpression::Gmean { terms })
        }
        other => Err(StoreError::InvalidInput(format!(
            "unknown synthetic metric operation `{other}`"
        ))),
    }
}

fn synthetic_metric_operand(raw: &str) -> Result<SyntheticMetricExpression, StoreError> {
    Ok(SyntheticMetricExpression::metric(NonEmptyText::new(
        raw.trim().to_owned(),
    )?))
}

#[derive(Deserialize)]
struct RenameMetricForm {
    metric: String,
    new_key: String,
}

#[derive(Deserialize)]
struct UpdateMetricDescriptionForm {
    metric: String,
    description: String,
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

async fn update_metric_description(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<UpdateMetricDescriptionForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let _ = store.update_metric(fidget_spinner_store_sqlite::UpdateMetricRequest {
                metric: NonEmptyText::new(form.metric)?,
                description: text_patch_field(form.description)?,
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
    metric: String,
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
            let _ = store.create_kpi(CreateKpiRequest {
                frontier: frontier.clone(),
                metric: NonEmptyText::new(form.metric)?,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
}

#[derive(Deserialize)]
struct SetKpiLockForm {
    frontier: String,
    locked: String,
}

async fn set_kpi_lock(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<SetKpiLockForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            let _ = store.set_frontier_registry_lock(SetFrontierRegistryLockRequest {
                registry: RegistryName::kpis(),
                mode: RegistryLockMode::Assignment,
                frontier: frontier.clone(),
                locked: matches!(form.locked.as_str(), "1" | "true" | "on" | "lock"),
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

#[derive(Deserialize)]
struct SetKpiReferenceForm {
    frontier: String,
    kpi: String,
    label: String,
    value: f64,
    unit: String,
}

#[derive(Deserialize)]
struct DeleteKpiReferenceForm {
    frontier: String,
    kpi: String,
    reference: String,
}

#[derive(Deserialize)]
struct MoveKpiForm {
    frontier: String,
    kpi: String,
    direction: MoveKpiDirection,
}

async fn move_kpi(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<MoveKpiForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            store.move_kpi(MoveKpiRequest {
                frontier: frontier.clone(),
                kpi: form.kpi,
                direction: form.direction,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
}

async fn set_kpi_reference(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<SetKpiReferenceForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            let unit = optional_metric_display_unit_field(form.unit)?;
            let _ = store.set_kpi_reference(SetKpiReferenceRequest {
                frontier: frontier.clone(),
                kpi: form.kpi,
                label: NonEmptyText::new(form.label)?,
                value: form.value,
                unit,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
}

async fn delete_kpi_reference(
    State(state): State<NavigatorState>,
    Path(project): Path<String>,
    Form(form): Form<DeleteKpiReferenceForm>,
) -> Response {
    metric_mutation_response(
        resolve_project_context(&state, &project).and_then(|context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let frontier = form.frontier;
            store.delete_kpi_reference(DeleteKpiReferenceRequest {
                frontier: frontier.clone(),
                kpi: form.kpi,
                reference: form.reference,
            })?;
            Ok(metrics_frontier_href(&context, &frontier))
        }),
    )
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

fn optional_metric_display_unit_field(
    raw: String,
) -> Result<Option<MetricDisplayUnit>, StoreError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        MetricDisplayUnit::parse(trimmed)
            .map(Some)
            .map_err(StoreError::from)
    }
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
struct FrontierSummaryForm {
    expected_revision: Option<u64>,
    label: String,
    objective: String,
}

async fn update_frontier_summary(
    State(state): State<NavigatorState>,
    Path((project, selector)): Path<(String, String)>,
    Form(form): Form<FrontierSummaryForm>,
) -> Response {
    frontier_status_mutation_response(resolve_project_context(&state, &project).and_then(
        |context| {
            let mut store = open_store(context.project_root.as_std_path())?;
            let updated = store.update_frontier(UpdateFrontierRequest {
                frontier: selector,
                expected_revision: form.expected_revision,
                label: Some(NonEmptyText::new(form.label)?),
                objective: Some(NonEmptyText::new(form.objective)?),
                status: None,
                situation: None,
                roadmap: None,
                unknowns: None,
            })?;
            Ok(format!(
                "{}{}",
                context.base_href,
                frontier_href(&updated.slug)
            ))
        },
    ))
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
