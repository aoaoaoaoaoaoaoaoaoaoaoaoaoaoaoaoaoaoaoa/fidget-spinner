#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "selection receives typed in-process projections; a shape mismatch is a programming error, never a malformed client record"
)]

use std::hash::{Hash as _, Hasher as _};

use libmcp::DetailLevel;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use super::fault::{FaultKind, FaultRecord, FaultStage};

#[derive(Debug, Deserialize)]
#[serde(try_from = "usize")]
pub(super) struct PageLimit(usize);

impl TryFrom<usize> for PageLimit {
    type Error = &'static str;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        (1..=200)
            .contains(&value)
            .then_some(Self(value))
            .ok_or("limit must be 1..200")
    }
}

impl PageLimit {
    pub(super) const fn get(&self) -> usize {
        self.0
    }
}

struct Page {
    limit: usize,
    cursor: Option<String>,
}

impl Page {
    fn read(
        args: &mut Map<String, Value>,
        default: usize,
        operation: &str,
    ) -> Result<Self, FaultRecord> {
        Ok(Self {
            limit: take::<PageLimit>(args, "limit", operation)?
                .map_or(default, |limit| limit.get()),
            cursor: take(args, "cursor", operation)?,
        })
    }

    fn select(
        &self,
        rows: Vec<Value>,
        scope: &Value,
        key: &str,
        operation: &str,
    ) -> Result<(Value, Value), FaultRecord> {
        // Cursors bind the complete public collection and query, not its encoding,
        // detail or page size. They carry no authority and retain no server state.
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        (scope, key, &rows).hash(&mut hash);
        let revision = format!("{:016x}", hash.finish());
        let offset = self.cursor.as_ref().map_or(Ok(0), |cursor| {
            let (expected, offset) = cursor
                .split_once(':')
                .ok_or_else(|| invalid(operation, "invalid cursor"))?;
            if expected != revision {
                return Err(invalid(
                    operation,
                    "query or results changed; restart without cursor",
                ));
            }
            offset
                .parse::<usize>()
                .map_err(|_| invalid(operation, "invalid cursor offset"))
        })?;
        let total = rows.len();
        if offset > total {
            return Err(invalid(operation, "cursor exceeds result count"));
        }
        let end = offset.saturating_add(self.limit).min(total);
        let selected = rows
            .into_iter()
            .skip(offset)
            .take(end - offset)
            .collect::<Vec<_>>();
        Ok((
            json!(selected),
            json!({
                "total": total,
                "offset": offset,
                "count": end - offset,
                "next_cursor": (end < total).then(|| format!("{revision}:{end}")),
            }),
        ))
    }
}

#[derive(Clone, Copy)]
enum Surface {
    Record,
    List(&'static str, usize),
    Frontier,
    Tags,
    Entity,
    History,
    Schema,
}

impl Surface {
    fn tool(name: &str) -> Self {
        match name {
            "frontier.list" => Self::List("frontiers", 20),
            "hypothesis.list" => Self::List("hypotheses", 20),
            "experiment.list" => Self::List("experiments", 20),
            "metric.keys" => Self::List("metrics", 20),
            "metric.best" | "kpi.best" => Self::List("entries", 10),
            "kpi.list" => Self::List("kpis", 20),
            "kpi.reference.list" => Self::List("references", 20),
            "condition.list" => Self::List("conditions", 20),
            "frontier.open" => Self::Frontier,
            "tag.list" => Self::Tags,
            "hypothesis.read" | "experiment.read" => Self::Entity,
            "frontier.history" | "hypothesis.history" | "experiment.history" => Self::History,
            "frontier.query.schema" => Self::Schema,
            _ => Self::Record,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FrontierSection {
    #[default]
    Overview,
    Worklist,
    Experiments,
    Tags,
    Metrics,
    Kpis,
}

impl FrontierSection {
    fn key(&self) -> Option<&'static str> {
        match self {
            Self::Overview => None,
            Self::Worklist => Some("worklist_hypotheses"),
            Self::Experiments => Some("open_experiments"),
            Self::Tags => Some("active_tags"),
            Self::Metrics => Some("active_metric_keys"),
            Self::Kpis => Some("kpis"),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TagSection {
    #[default]
    Registry,
    Tags,
    Families,
    Locks,
    History,
}

impl TagSection {
    fn key(&self) -> Option<&'static str> {
        match self {
            Self::Registry => None,
            Self::Tags => Some("tags"),
            Self::Families => Some("families"),
            Self::Locks => Some("locks"),
            Self::History => Some("name_history"),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EntityView {
    #[default]
    Record,
    Parents,
    Children,
}

enum Content {
    Record,
    List {
        key: &'static str,
        page: Page,
    },
    Frontier {
        section: FrontierSection,
        page: Page,
    },
    Tags {
        section: TagSection,
        page: Page,
    },
    Entity {
        view: EntityView,
        page: Page,
    },
    History {
        snapshots: bool,
        revision: Option<u64>,
        page: Page,
    },
    Schema {
        view: Option<String>,
    },
}

pub(super) struct Selection {
    content: Content,
    scope: Value,
}

impl Selection {
    pub(super) fn read(
        name: &str,
        args: &mut Value,
        project: &str,
        operation: &str,
    ) -> Result<Self, FaultRecord> {
        let object = args
            .as_object_mut()
            .ok_or_else(|| invalid(operation, "arguments must be an object"))?;
        let content = match Surface::tool(name) {
            Surface::Record => Content::Record,
            Surface::List(key, default) => Content::List {
                key,
                page: Page::read(object, default, operation)?,
            },
            Surface::Frontier => {
                let section =
                    take::<FrontierSection>(object, "section", operation)?.unwrap_or_default();
                let page = Page::read(object, 10, operation)?;
                if section.key().is_none() && page.cursor.is_some() {
                    return Err(invalid(
                        operation,
                        "a cursor requires a specific frontier section",
                    ));
                }
                Content::Frontier { section, page }
            }
            Surface::Tags => {
                let section = take::<TagSection>(object, "section", operation)?.unwrap_or_default();
                let page = Page::read(object, 20, operation)?;
                if section.key().is_none() && page.cursor.is_some() {
                    return Err(invalid(
                        operation,
                        "a cursor requires a specific tag section",
                    ));
                }
                Content::Tags { section, page }
            }
            Surface::Entity => {
                let view = take::<EntityView>(object, "view", operation)?.unwrap_or_default();
                let page = Page::read(object, 20, operation)?;
                if matches!(view, EntityView::Record) && page.cursor.is_some() {
                    return Err(invalid(
                        operation,
                        "a cursor requires view=parents or children",
                    ));
                }
                Content::Entity { view, page }
            }
            Surface::History => Content::History {
                snapshots: take(object, "snapshots", operation)?.unwrap_or(false),
                revision: take(object, "revision", operation)?,
                page: Page::read(object, 10, operation)?,
            },
            Surface::Schema => Content::Schema {
                view: take(object, "view", operation)?,
            },
        };
        Ok(Self {
            content,
            scope: json!([project, name, args]),
        })
    }

    pub(super) fn select(
        self,
        name: &str,
        mut value: Value,
        detail: DetailLevel,
        operation: &str,
    ) -> Result<Value, FaultRecord> {
        match self.content {
            Content::Record => {}
            Content::List { key, page } => {
                page_field(&mut value, key, &page, &self.scope, operation)?;
            }
            Content::Frontier { section, page } => {
                if let Some(key) = section.key() {
                    let frontier = value["frontier"]["slug"].clone();
                    keep(&mut value, &[key]);
                    let _ = object(&mut value).insert("frontier".to_owned(), frontier);
                    page_field(&mut value, key, &page, &self.scope, operation)?;
                } else {
                    page_sections(
                        &mut value,
                        &[
                            "active_tags",
                            "kpis",
                            "active_metric_keys",
                            "worklist_hypotheses",
                            "open_experiments",
                        ],
                        &page,
                        &self.scope,
                        operation,
                    )?;
                }
            }
            Content::Tags { section, page } => {
                if let Some(key) = section.key() {
                    keep(&mut value, &[key]);
                    page_field(&mut value, key, &page, &self.scope, operation)?;
                } else {
                    let _ = object(&mut value).remove("count");
                    page_sections(
                        &mut value,
                        &["tags", "families", "locks", "name_history"],
                        &page,
                        &self.scope,
                        operation,
                    )?;
                }
            }
            Content::Entity { view, page } => match view {
                EntityView::Record => {
                    for key in [
                        "parents",
                        "children",
                        "open_experiments",
                        "closed_experiments",
                    ] {
                        if let Some(rows) = value.get_mut(key) {
                            *rows =
                                json!(rows.as_array().expect("entity relations are arrays").len());
                        }
                    }
                    if name == "hypothesis.read" {
                        let lifecycle = if value["open_experiments"].as_u64().expect("open count")
                            > 0
                        {
                            "working"
                        } else if value["closed_experiments"].as_u64().expect("closed count") > 0 {
                            "closed"
                        } else {
                            "fresh"
                        };
                        let _ = object(&mut value["record"])
                            .insert("lifecycle".to_owned(), json!(lifecycle));
                    }
                }
                EntityView::Parents | EntityView::Children => {
                    let key = if matches!(view, EntityView::Parents) {
                        "parents"
                    } else {
                        "children"
                    };
                    let slug = value["record"]["slug"].clone();
                    keep(&mut value, &[key]);
                    let _ = object(&mut value).insert("slug".to_owned(), slug);
                    page_field(&mut value, key, &page, &self.scope, operation)?;
                }
            },
            Content::History {
                snapshots,
                revision,
                page,
            } => {
                let rows = value["history"].as_array_mut().expect("history array");
                if let Some(revision) = revision {
                    rows.retain(|row| row["revision"] == revision);
                }
                page_field(&mut value, "history", &page, &self.scope, operation)?;
                if !snapshots {
                    each(&mut value, "history", |row| {
                        let _ = object(row).remove("snapshot");
                    });
                }
            }
            Content::Schema { view } => {
                if let Some(view) = view {
                    let rows = value["views"].as_array_mut().expect("schema views");
                    rows.retain(|row| row["name"] == view);
                    if rows.is_empty() {
                        return Err(invalid(operation, "unknown SQL view"));
                    }
                }
                if detail == DetailLevel::Concise {
                    each(&mut value, "views", |row| {
                        let columns = object(row).remove("columns").expect("view columns");
                        let _ = object(row).insert(
                            "column_count".to_owned(),
                            json!(columns.as_array().expect("columns array").len()),
                        );
                    });
                }
            }
        }
        if detail == DetailLevel::Concise {
            compact(name, &mut value);
        }
        Ok(value)
    }
}

fn page_field(
    value: &mut Value,
    key: &str,
    page: &Page,
    scope: &Value,
    operation: &str,
) -> Result<(), FaultRecord> {
    let rows = object(value).remove(key).expect("declared collection");
    let Value::Array(rows) = rows else {
        panic!("declared collection must be an array")
    };
    let (rows, metadata) = page.select(rows, scope, key, operation)?;
    let _ = object(value).remove("count");
    let _ = object(value).insert(key.to_owned(), rows);
    let _ = object(value).insert("page".to_owned(), metadata);
    Ok(())
}

fn page_sections(
    value: &mut Value,
    keys: &[&str],
    page: &Page,
    scope: &Value,
    operation: &str,
) -> Result<(), FaultRecord> {
    let mut pages = Map::new();
    for key in keys {
        page_field(value, key, page, scope, operation)?;
        let _ = pages.insert(
            (*key).to_owned(),
            object(value).remove("page").expect("section page"),
        );
    }
    let _ = object(value).insert("pages".to_owned(), Value::Object(pages));
    Ok(())
}

fn compact(name: &str, value: &mut Value) {
    match name {
        "project.status" => keep(
            value,
            &[
                "display_name",
                "description",
                "project_root",
                "frontier_count",
                "hypothesis_count",
                "experiment_count",
                "open_experiment_count",
            ],
        ),
        "tag.add" => compact_tag(&mut value["record"]),
        "tag.list" => {
            each(value, "tags", compact_tag);
            each(value, "families", |row| {
                keep(row, &["name", "description", "mandatory", "revision"]);
            });
        }
        "frontier.create" | "frontier.update" => keep(
            &mut value["record"],
            &["slug", "label", "objective", "status", "revision"],
        ),
        "frontier.read" => keep(
            &mut value["record"],
            &["slug", "label", "objective", "status", "revision", "brief"],
        ),
        "frontier.list" => each(value, "frontiers", |row| {
            keep(
                row,
                &[
                    "slug",
                    "label",
                    "objective",
                    "status",
                    "worklist_hypothesis_count",
                    "open_experiment_count",
                ],
            );
        }),
        "frontier.open" => {
            each(value, "worklist_hypotheses", compact_hypothesis);
            each(value, "open_experiments", compact_experiment);
            each(value, "kpis", compact_kpi);
            each(value, "active_metric_keys", compact_metric);
        }
        "hypothesis.record" | "hypothesis.update" | "hypothesis.attention.set" => {
            keep(
                &mut value["record"],
                &[
                    "slug",
                    "summary",
                    "expected_yield",
                    "confidence",
                    "attention",
                    "revision",
                ],
            );
        }
        "hypothesis.list" => each(value, "hypotheses", compact_hypothesis),
        "hypothesis.read" | "experiment.read" => {
            if let Some(record) = value.get_mut("record") {
                if name == "hypothesis.read" {
                    let body_chars = record["body"]
                        .as_str()
                        .expect("hypothesis body")
                        .chars()
                        .count();
                    keep(
                        record,
                        &[
                            "slug",
                            "title",
                            "summary",
                            "expected_yield",
                            "confidence",
                            "attention",
                            "lifecycle",
                            "tags",
                            "revision",
                        ],
                    );
                    let _ = object(record).insert("body_chars".to_owned(), json!(body_chars));
                } else {
                    keep(
                        record,
                        &[
                            "slug", "title", "summary", "tags", "status", "outcome", "revision",
                        ],
                    );
                    if let Some(outcome) = record.get_mut("outcome") {
                        keep(
                            outcome,
                            &[
                                "conditions",
                                "primary_metric",
                                "supporting_metrics",
                                "verdict",
                                "rationale",
                                "analysis",
                                "commit_hash",
                            ],
                        );
                        if let Some(analysis) = outcome.get_mut("analysis") {
                            keep(analysis, &["summary"]);
                        }
                    }
                }
            }
            if let Some(owner) = value.get_mut("owning_hypothesis") {
                keep(owner, &["slug", "summary"]);
            }
            for key in ["parents", "children"] {
                each(value, key, |row| {
                    keep(row, &["kind", "slug", "title", "summary"]);
                });
            }
        }
        "experiment.open" | "experiment.update" | "experiment.close" | "experiment.scuff" => {
            let record = &mut value["record"];
            keep(record, &["slug", "title", "status", "revision", "outcome"]);
            if let Some(outcome) = record.get_mut("outcome") {
                keep(outcome, &["verdict", "primary_metric", "commit_hash"]);
            }
        }
        "experiment.list" => each(value, "experiments", compact_experiment),
        "experiment.nearest" => {
            if let Some(metric) = value.get_mut("metric").filter(|metric| !metric.is_null()) {
                compact_metric(metric);
            }
            for key in ["accepted", "kept", "rejected", "champion"] {
                if let Some(hit) = value.get_mut(key).filter(|hit| !hit.is_null()) {
                    compact_hit(hit);
                }
            }
        }
        "metric.define" | "metric.update" => compact_metric(&mut value["record"]),
        "metric.keys" => each(value, "metrics", compact_metric),
        "metric.best" | "kpi.best" => each(value, "entries", compact_hit),
        "kpi.create" => compact_kpi(&mut value["record"]),
        "kpi.list" => each(value, "kpis", compact_kpi),
        "kpi.reference.set" => compact_reference(&mut value["record"]),
        "kpi.reference.list" => each(value, "references", compact_reference),
        "condition.define" => keep(&mut value["record"], &["key", "value_type", "description"]),
        "condition.list" => each(value, "conditions", |row| {
            keep(row, &["key", "value_type", "description"]);
        }),
        _ => {}
    }
}

fn compact_tag(value: &mut Value) {
    keep(value, &["name", "description", "family", "revision"]);
}
fn compact_hypothesis(value: &mut Value) {
    keep(
        value,
        &[
            "slug",
            "summary",
            "expected_yield",
            "confidence",
            "attention",
            "lifecycle",
            "open_experiment_count",
            "latest_verdict",
        ],
    );
}
fn compact_experiment(value: &mut Value) {
    keep(
        value,
        &["slug", "title", "status", "verdict", "primary_metric"],
    );
}
fn compact_metric(value: &mut Value) {
    keep(
        value,
        &[
            "key",
            "kind",
            "dimension",
            "display_unit",
            "aggregation",
            "objective",
            "description",
            "reference_count",
        ],
    );
}
fn compact_kpi(value: &mut Value) {
    let mut metric = object(value).remove("metric").expect("KPI metric");
    keep(&mut metric, &["key", "display_unit", "objective"]);
    object(value).append(object(&mut metric));
}
fn compact_reference(value: &mut Value) {
    keep(value, &["ordinal", "label", "value", "display_unit"]);
}
fn compact_hit(value: &mut Value) {
    let experiment = object(value)
        .remove("experiment")
        .expect("ranked experiment");
    let hypothesis = object(value)
        .remove("hypothesis")
        .expect("owning hypothesis");
    let verdict = experiment
        .get("verdict")
        .unwrap_or(&experiment["status"])
        .clone();
    let _ = object(value).insert("experiment".to_owned(), experiment["slug"].clone());
    let _ = object(value).insert("hypothesis".to_owned(), hypothesis["slug"].clone());
    let _ = object(value).insert("verdict".to_owned(), verdict);
}

fn keep(value: &mut Value, fields: &[&str]) {
    object(value).retain(|key, _| fields.contains(&key.as_str()));
}
fn each(value: &mut Value, key: &str, mut action: impl FnMut(&mut Value)) {
    if let Some(rows) = value.get_mut(key).and_then(Value::as_array_mut) {
        for row in rows {
            action(row);
        }
    }
}
fn object(value: &mut Value) -> &mut Map<String, Value> {
    value.as_object_mut().expect("public record projection")
}

fn take<T: DeserializeOwned>(
    args: &mut Map<String, Value>,
    key: &str,
    operation: &str,
) -> Result<Option<T>, FaultRecord> {
    args.remove(key)
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|error| invalid(operation, &format!("invalid {key}: {error}")))
        })
        .transpose()
}
fn invalid(operation: &str, message: &str) -> FaultRecord {
    FaultRecord::new(
        FaultKind::InvalidInput,
        FaultStage::Worker,
        operation,
        message,
    )
}

pub(super) fn with_content_properties(name: &str, mut schema: Value) -> Value {
    let properties = schema["properties"]
        .as_object_mut()
        .expect("tool properties");
    let mut page = None;
    match Surface::tool(name) {
        Surface::List(_, default) => page = Some(default),
        Surface::Frontier => {
            let _ = properties.insert("section".to_owned(), json!({"enum":["overview","worklist","experiments","tags","metrics","kpis"],"type":"string","default":"overview","description":"Overview selects the brief and first page of each section. Continue one section with its next_cursor."}));
            page = Some(10);
        }
        Surface::Tags => {
            let _ = properties.insert("section".to_owned(), json!({"enum":["registry","tags","families","locks","history"],"type":"string","default":"registry","description":"Registry selects the first page of each section, including policy when tags are empty. Continue one section with its next_cursor."}));
            page = Some(20);
        }
        Surface::Entity => {
            let _ = properties.insert("view".to_owned(), json!({"enum":["record","parents","children"],"type":"string","default":"record","description":"Record with relation counts, or one paged neighbour direction. Hypothesis experiments use experiment.list hypothesis=…; detail=full includes complete record text, not more neighbours."}));
            page = Some(20);
        }
        Surface::History => {
            let _ = properties.insert("snapshots".to_owned(), json!({"type":"boolean","default":false,"description":"Include complete snapshots for the selected revisions; independent of detail."}));
            let _ = properties.insert(
                "revision".to_owned(),
                json!({"type":"integer","minimum":0,"description":"Select one exact revision."}),
            );
            page = Some(10);
        }
        Surface::Schema => {
            let _ = properties.insert("view".to_owned(), json!({"type":"string","description":"Exact q_* view name. Concise lists views; full includes column definitions."}));
        }
        Surface::Record => {}
    }
    if let Some(default) = page {
        let _ = properties.insert("limit".to_owned(), json!({"type":"integer","minimum":1,"maximum":200,"default":default,"description":"Rows per collection page."}));
        let _ = properties.insert("cursor".to_owned(), json!({"type":"string","description":"Repeat query with next_cursor; changed results invalidate it."}));
    }
    if name == "system.telemetry" {
        let _ = properties.insert("operation".to_owned(), json!({"type":"string","description":"Exact operation filter; totals remain unfiltered."}));
        let _ = properties.insert("offset".to_owned(), json!({"type":"integer","minimum":0,"default":0,"description":"Live page offset; counters and ranking may change between calls."}));
        let _ = properties.insert(
            "limit".to_owned(),
            json!({"type":"integer","minimum":1,"maximum":200,"default":6}),
        );
    }
    if name == "frontier.query.sql" {
        let _ = properties.insert("max_rows".to_owned(), json!({"type":"integer","minimum":1,"maximum":1000,"default":20,"description":"Result row cap. Narrow SQL projections or LIMIT/OFFSET recover rows beyond this page."}));
    }
    schema
}
