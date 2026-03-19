use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use camino::Utf8PathBuf;
use fidget_spinner_core::{DagNode, FieldValueType, NodeClass, ProjectSchema, TagName};
use linkify::{LinkFinder, LinkKind};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{open_store, to_pretty_json};

#[derive(Clone)]
struct NavigatorState {
    project_root: Utf8PathBuf,
    limit: u32,
}

#[derive(Debug, Default, Deserialize)]
struct NavigatorQuery {
    tag: Option<String>,
}

struct NavigatorEntry {
    node: DagNode,
    frontier_label: Option<String>,
}

struct TagFacet {
    name: TagName,
    description: String,
    count: usize,
}

pub(crate) fn serve(
    project_root: Utf8PathBuf,
    bind: SocketAddr,
    limit: u32,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .map_err(fidget_spinner_store_sqlite::StoreError::from)?;
    runtime.block_on(async move {
        let state = NavigatorState {
            project_root,
            limit,
        };
        let app = Router::new()
            .route("/", get(navigator))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .map_err(fidget_spinner_store_sqlite::StoreError::from)?;
        println!("navigator: http://{bind}/");
        axum::serve(listener, app).await.map_err(|error| {
            fidget_spinner_store_sqlite::StoreError::Io(io::Error::other(error.to_string()))
        })
    })
}

async fn navigator(
    State(state): State<NavigatorState>,
    Query(query): Query<NavigatorQuery>,
) -> Response {
    match render_navigator(state, query) {
        Ok(markup) => Html(markup.into_string()).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("navigator render failed: {error}"),
        )
            .into_response(),
    }
}

fn render_navigator(
    state: NavigatorState,
    query: NavigatorQuery,
) -> Result<Markup, fidget_spinner_store_sqlite::StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let selected_tag = query.tag.map(TagName::new).transpose()?;
    let schema = store.schema().clone();
    let frontiers = store
        .list_frontiers()?
        .into_iter()
        .map(|frontier| (frontier.id, frontier.label.to_string()))
        .collect::<BTreeMap<_, _>>();

    let recent_nodes = load_recent_nodes(&store, None, state.limit)?;
    let visible_nodes = load_recent_nodes(&store, selected_tag.clone(), state.limit)?;
    let tag_facets = store
        .list_tags()?
        .into_iter()
        .map(|tag| TagFacet {
            count: recent_nodes
                .iter()
                .filter(|node| node.tags.contains(&tag.name))
                .count(),
            description: tag.description.to_string(),
            name: tag.name,
        })
        .collect::<Vec<_>>();
    let entries = visible_nodes
        .into_iter()
        .map(|node| NavigatorEntry {
            frontier_label: node
                .frontier_id
                .and_then(|frontier_id| frontiers.get(&frontier_id).cloned()),
            node,
        })
        .collect::<Vec<_>>();

    let title = selected_tag.as_ref().map_or_else(
        || "all recent nodes".to_owned(),
        |tag| format!("tag: {tag}"),
    );
    let project_name = store.config().display_name.to_string();

    Ok(html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "Fidget Spinner Navigator" }
                style { (PreEscaped(stylesheet().to_owned())) }
            }
            body {
                main class="shell" {
                    aside class="rail" {
                        h1 { "Navigator" }
                        p class="project" { (project_name) }
                        nav class="tag-list" {
                            a
                                href="/"
                                class={ "tag-link " (if selected_tag.is_none() { "selected" } else { "" }) } {
                                span class="tag-name" { "all" }
                                span class="tag-count" { (recent_nodes.len()) }
                            }
                            @for facet in &tag_facets {
                                a
                                    href={ "/?tag=" (facet.name.as_str()) }
                                    class={ "tag-link " (if selected_tag.as_ref() == Some(&facet.name) { "selected" } else { "" }) } {
                                    span class="tag-name" { (facet.name.as_str()) }
                                    span class="tag-count" { (facet.count) }
                                    span class="tag-description" { (facet.description.as_str()) }
                                }
                            }
                        }
                    }
                    section class="feed" {
                        header class="feed-header" {
                            h2 { (title) }
                            p class="feed-meta" {
                                (entries.len()) " shown"
                                " · "
                                (recent_nodes.len()) " recent"
                                " · "
                                (state.limit) " max"
                            }
                        }
                        @if entries.is_empty() {
                            article class="empty-state" {
                                h3 { "No matching nodes" }
                                p { "Try clearing the tag filter or recording new notes." }
                            }
                        } @else {
                            @for entry in &entries {
                                (render_entry(entry, &schema))
                            }
                        }
                    }
                }
            }
        }
    })
}

fn load_recent_nodes(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    tag: Option<TagName>,
    limit: u32,
) -> Result<Vec<DagNode>, fidget_spinner_store_sqlite::StoreError> {
    let summaries = store.list_nodes(fidget_spinner_store_sqlite::ListNodesQuery {
        tags: tag.into_iter().collect(),
        limit,
        ..fidget_spinner_store_sqlite::ListNodesQuery::default()
    })?;
    summaries
        .into_iter()
        .map(|summary| {
            store.get_node(summary.id)?.ok_or(
                fidget_spinner_store_sqlite::StoreError::NodeNotFound(summary.id),
            )
        })
        .collect()
}

fn render_entry(entry: &NavigatorEntry, schema: &ProjectSchema) -> Markup {
    let body = entry.node.payload.field("body").and_then(Value::as_str);
    let mut keys = entry
        .node
        .payload
        .fields
        .keys()
        .filter(|name| name.as_str() != "body")
        .cloned()
        .collect::<Vec<_>>();
    keys.sort_unstable();

    html! {
        article class="entry" id={ "node-" (entry.node.id) } {
            header class="entry-header" {
                div class="entry-title-row" {
                    span class={ "class-badge class-" (entry.node.class.as_str()) } {
                        (entry.node.class.as_str())
                    }
                    h3 class="entry-title" {
                        a href={ "#node-" (entry.node.id) } { (entry.node.title.as_str()) }
                    }
                }
                div class="entry-meta" {
                    span { (render_timestamp(entry.node.updated_at)) }
                    @if let Some(label) = &entry.frontier_label {
                        span { "frontier: " (label.as_str()) }
                    }
                    @if !entry.node.tags.is_empty() {
                        span class="tag-strip" {
                            @for tag in &entry.node.tags {
                                a class="entry-tag" href={ "/?tag=" (tag.as_str()) } { (tag.as_str()) }
                            }
                        }
                    }
                }
            }
            @if let Some(summary) = &entry.node.summary {
                p class="entry-summary" { (summary.as_str()) }
            }
            @if let Some(body) = body {
                section class="entry-body" {
                    (render_string_value(body))
                }
            }
            @if !keys.is_empty() {
                dl class="field-list" {
                    @for key in &keys {
                        @if let Some(value) = entry.node.payload.field(key) {
                            (render_field(entry.node.class, schema, key, value))
                        }
                    }
                }
            }
            @if !entry.node.diagnostics.items.is_empty() {
                section class="diagnostics" {
                    h4 { "diagnostics" }
                    ul {
                        @for item in &entry.node.diagnostics.items {
                            li {
                                span class="diag-severity" { (format!("{:?}", item.severity).to_ascii_lowercase()) }
                                " "
                                (item.message.as_str())
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_field(class: NodeClass, schema: &ProjectSchema, key: &str, value: &Value) -> Markup {
    let value_type = schema
        .field_spec(class, key)
        .and_then(|field| field.value_type);
    let is_plottable = schema
        .field_spec(class, key)
        .is_some_and(|field| field.is_plottable());
    html! {
        dt {
            (key)
            @if let Some(value_type) = value_type {
                span class="field-type" { (value_type.as_str()) }
            }
            @if is_plottable {
                span class="field-type plottable" { "plot" }
            }
        }
        dd {
            @match value_type {
                Some(FieldValueType::String) => {
                    @if let Some(text) = value.as_str() {
                        (render_string_value(text))
                    } @else {
                        (render_json_value(value))
                    }
                }
                Some(FieldValueType::Numeric) => {
                    @if let Some(number) = value.as_f64() {
                        code class="numeric" { (number) }
                    } @else {
                        (render_json_value(value))
                    }
                }
                Some(FieldValueType::Boolean) => {
                    @if let Some(boolean) = value.as_bool() {
                        span class={ "boolean " (if boolean { "true" } else { "false" }) } {
                            (if boolean { "true" } else { "false" })
                        }
                    } @else {
                        (render_json_value(value))
                    }
                }
                Some(FieldValueType::Timestamp) => {
                    @if let Some(raw) = value.as_str() {
                        time datetime=(raw) { (render_timestamp_value(raw)) }
                    } @else {
                        (render_json_value(value))
                    }
                }
                None => (render_json_value(value)),
            }
        }
    }
}

fn render_string_value(text: &str) -> Markup {
    let finder = LinkFinder::new();
    html! {
        div class="rich-text" {
            @for line in text.lines() {
                p {
                    @for span in finder.spans(line) {
                        @match span.kind() {
                            Some(LinkKind::Url) => a href=(span.as_str()) { (span.as_str()) },
                            _ => (span.as_str()),
                        }
                    }
                }
            }
        }
    }
}

fn render_json_value(value: &Value) -> Markup {
    let text = to_pretty_json(value).unwrap_or_else(|_| value.to_string());
    html! {
        pre class="json-value" { (text) }
    }
}

fn render_timestamp(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| timestamp.to_string())
}

fn render_timestamp_value(raw: &str) -> String {
    OffsetDateTime::parse(raw, &Rfc3339)
        .map(render_timestamp)
        .unwrap_or_else(|_| raw.to_owned())
}

fn stylesheet() -> &'static str {
    r#"
    :root {
        color-scheme: light;
        --bg: #f6f3ec;
        --panel: #fffdf8;
        --line: #d8d1c4;
        --text: #22201a;
        --muted: #746e62;
        --accent: #2d5c4d;
        --accent-soft: #dbe8e2;
        --tag: #ece5d8;
        --warn: #8b5b24;
    }

    * { box-sizing: border-box; }

    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font: 15px/1.5 "Iosevka Web", "IBM Plex Mono", "SFMono-Regular", monospace;
    }

    a {
        color: var(--accent);
        text-decoration: none;
    }

    a:hover {
        text-decoration: underline;
    }

    .shell {
        display: grid;
        grid-template-columns: 18rem minmax(0, 1fr);
        min-height: 100vh;
    }

    .rail {
        border-right: 1px solid var(--line);
        padding: 1.25rem 1rem;
        position: sticky;
        top: 0;
        align-self: start;
        height: 100vh;
        overflow: auto;
        background: rgba(255, 253, 248, 0.85);
        backdrop-filter: blur(6px);
    }

    .project, .feed-meta, .entry-meta, .entry-summary, .tag-description {
        color: var(--muted);
    }

    .tag-list {
        display: grid;
        gap: 0.5rem;
    }

    .tag-link {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 0.2rem 0.75rem;
        padding: 0.55rem 0.7rem;
        border: 1px solid var(--line);
        background: var(--panel);
    }

    .tag-link.selected {
        border-color: var(--accent);
        background: var(--accent-soft);
    }

    .tag-name {
        font-weight: 700;
        overflow-wrap: anywhere;
    }

    .tag-count {
        color: var(--muted);
    }

    .tag-description {
        grid-column: 1 / -1;
        font-size: 0.9rem;
    }

    .feed {
        padding: 1.5rem;
        display: grid;
        gap: 1rem;
    }

    .feed-header {
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--line);
    }

    .entry, .empty-state {
        background: var(--panel);
        border: 1px solid var(--line);
        padding: 1rem 1.1rem;
    }

    .entry-header {
        display: grid;
        gap: 0.35rem;
        margin-bottom: 0.75rem;
    }

    .entry-title-row {
        display: flex;
        gap: 0.75rem;
        align-items: baseline;
    }

    .entry-title {
        margin: 0;
        font-size: 1.05rem;
    }

    .entry-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        font-size: 0.9rem;
    }

    .class-badge, .field-type, .entry-tag {
        display: inline-block;
        padding: 0.08rem 0.4rem;
        border: 1px solid var(--line);
        background: var(--tag);
        font-size: 0.82rem;
    }

    .field-type.plottable {
        background: var(--accent-soft);
        border-color: var(--accent);
    }

    .tag-strip {
        display: inline-flex;
        flex-wrap: wrap;
        gap: 0.35rem;
    }

    .entry-body {
        margin-bottom: 0.9rem;
    }

    .rich-text p {
        margin: 0 0 0.55rem;
    }

    .rich-text p:last-child {
        margin-bottom: 0;
    }

    .field-list {
        display: grid;
        grid-template-columns: minmax(12rem, 18rem) minmax(0, 1fr);
        gap: 0.55rem 1rem;
        margin: 0;
    }

    .field-list dt {
        font-weight: 700;
        display: flex;
        gap: 0.4rem;
        align-items: center;
        overflow-wrap: anywhere;
    }

    .field-list dd {
        margin: 0;
    }

    .json-value {
        margin: 0;
        padding: 0.6rem 0.7rem;
        background: #f3eee4;
        overflow: auto;
    }

    .boolean.true { color: var(--accent); }
    .boolean.false { color: #8a2f2f; }
    .numeric { font-size: 1rem; }

    .diagnostics {
        margin-top: 1rem;
        padding-top: 0.8rem;
        border-top: 1px dashed var(--line);
    }

    .diagnostics h4 {
        margin: 0 0 0.4rem;
        font-size: 0.9rem;
        text-transform: lowercase;
    }

    .diagnostics ul {
        margin: 0;
        padding-left: 1.1rem;
    }

    .diag-severity {
        color: var(--warn);
        font-weight: 700;
    }

    @media (max-width: 900px) {
        .shell {
            grid-template-columns: 1fr;
        }

        .rail {
            position: static;
            height: auto;
            border-right: 0;
            border-bottom: 1px solid var(--line);
        }

        .field-list {
            grid-template-columns: 1fr;
        }
    }
    "#
}
