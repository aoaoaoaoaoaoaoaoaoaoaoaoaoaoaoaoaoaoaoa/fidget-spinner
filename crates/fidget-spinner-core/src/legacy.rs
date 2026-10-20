//! Translation of FS 3 (store format 21) operations into this language, run once by the format-22 migration.

use crate::Seq;
use serde_json::{Map, Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};

/// The definitions FS 3 history is bound to.
pub fn schemas() -> [(&'static str, Json); 3] {
    [
        (
            "fs3.hypothesis",
            json!({
                "carrier": "iset",
                "title": "FS 3 hypothesis",
                "data": {
                    "title": {"type": "text", "presence": "required", "show": "headline"},
                    "mechanism": {"type": "text", "presence": "optional", "show": "section"},
                    "claim": {"type": "text", "presence": "optional", "show": "section"},
                    "carries": {
                        "type": {"map": "text"},
                        "presence": "optional",
                        "description": "What each input carries forward, keyed by its slug when recorded",
                        "show": "section"
                    }
                },
                "answer": {
                    "disposition": {"type": {"enum": ["accepted", "rejected", "shelved"]}, "presence": "required", "show": "badge"},
                    "rationale": {"type": "text", "presence": "required"}
                }
            }),
        ),
        (
            "fs3.review",
            json!({
                "carrier": "iset",
                "title": "FS 3 review",
                "data": {
                    "title": {"type": "text", "presence": "required", "show": "headline"},
                    "charter": {"type": "text", "presence": "required", "show": "section"}
                },
                "answer": {"lesson": {"type": "text", "presence": "required"}}
            }),
        ),
        (
            "fs3.trial",
            json!({
                "carrier": "trial",
                "title": "FS 3 trial",
                "plan": {},
                "report": {"note": {"type": "text", "presence": "required", "show": "headline"}}
            }),
        ),
    ]
}

/// One frontier's translation state.
#[derive(Debug, Default)]
pub struct Translator {
    through: Seq,
    hypotheses: BTreeSet<String>,
    reviews: BTreeMap<String, BTreeSet<String>>,
    adopts: BTreeMap<String, Json>,
    trials: BTreeMap<String, u64>,
}

fn present<'a>(op: &'a Map<String, Json>, key: &str) -> Option<&'a Json> {
    op.get(key).filter(|v| !v.is_null())
}

fn text(op: &Map<String, Json>, key: &str) -> Result<String, String> {
    present(op, key)
        .and_then(Json::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("{key}: expected text"))
}

fn object(pairs: impl IntoIterator<Item = (&'static str, Option<Json>)>) -> Json {
    Json::Object(
        pairs
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k.to_owned(), v)))
            .collect(),
    )
}

fn slugs(value: Option<&Json>) -> Result<BTreeSet<String>, String> {
    value
        .map(|v| {
            v.as_array()
                .ok_or_else(|| "expected a list of slugs".to_owned())?
                .iter()
                .map(|s| {
                    s.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "expected a slug".to_owned())
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

impl Translator {
    /// `through` is the last FS 3 seq; patches through it replay under FS 3 law.
    pub fn new(through: Seq) -> Self {
        Self {
            through,
            ..Self::default()
        }
    }

    /// Translate one FS 3 patch, preserving operation order.
    pub fn patch(&mut self, ops: &[Json]) -> Result<Vec<Json>, String> {
        let mut out = Vec::new();
        for (index, op) in ops.iter().enumerate() {
            self.op(op, &mut out)
                .map_err(|e| format!("operation {index}: {e}"))?;
        }
        Ok(out)
    }

    fn op(&mut self, op: &Json, out: &mut Vec<Json>) -> Result<(), String> {
        let map = op.as_object().ok_or("expected an object")?;
        let name = map.get("op").and_then(Json::as_str).unwrap_or("");
        let target = |keys: &[&str]| -> Result<(String, String), String> {
            keys.iter()
                .find_map(|k| {
                    present(map, k)
                        .and_then(Json::as_str)
                        .map(|s| ((*k).to_owned(), s.to_owned()))
                })
                .ok_or_else(|| format!("{name}: needs one of {}", keys.join(", ")))
        };
        let unsupported = |field: &str| -> Result<(), String> {
            if present(map, field).is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty())) {
                Err(format!("{name}.{field} has no migration"))
            } else {
                Ok(())
            }
        };
        match name {
            "frontier" => {
                out.push(json!({"op": "frontier", "objective": text(map, "objective")?, "legacy_through": self.through}));
                for (schema, definition) in schemas() {
                    out.push(json!({"op": "define", "schema": schema, "definition": definition}));
                }
            }
            "conclude" => out.push(json!({"op": "conclude", "lesson": text(map, "lesson")?})),
            "archive" => out.push(json!({"op": "archive"})),
            "regime" => {
                unsupported("bridge")?;
                out.push(object([
                    ("op", Some("regime".into())),
                    ("contract", present(map, "contract").cloned()),
                    ("metrics", present(map, "metrics").cloned()),
                    ("reason", present(map, "reason").cloned()),
                ]));
            }
            "baseline" => {
                unsupported("docket")?;
                let slug = text(map, "slug")?;
                if let Some(adopts) = present(map, "adopts") {
                    self.adopts.insert(slug.clone(), adopts.clone());
                }
                out.push(object([
                    ("op", Some("baseline".into())),
                    ("slug", Some(slug.into())),
                    ("artifacts", present(map, "artifacts").cloned()),
                    ("readings", present(map, "readings").cloned()),
                    ("note", present(map, "note").cloned()),
                ]));
            }
            "docket" => {}
            "hypothesis" => {
                unsupported("after")?;
                unsupported("reviews")?;
                let slug = text(map, "slug")?;
                if self.hypotheses.insert(slug.clone()) {
                    let mut data = Map::new();
                    data.insert("title".into(), text(map, "title")?.into());
                    for key in ["mechanism", "claim"] {
                        if let Some(value) = present(map, key) {
                            data.insert(key.into(), value.clone());
                        }
                    }
                    data.insert("carries".into(), json!({}));
                    out.push(json!({"op": "iset", "slug": slug, "schema": "fs3.hypothesis", "data": data}));
                } else {
                    let set: Vec<Json> = ["title", "mechanism", "claim"]
                        .into_iter()
                        .filter_map(|key| present(map, key).map(|v| json!({"path": [key], "value": v})))
                        .collect();
                    if !set.is_empty() {
                        out.push(json!({"op": "edit", "work": slug, "set": set}));
                    }
                }
            }
            "edge" => {
                let from = text(map, "from")?;
                let to = text(map, "to")?;
                out.push(json!({"op": "after", "work": to, "add": [from]}));
                if let Some(carries) = present(map, "carries") {
                    out.push(json!({"op": "edit", "work": to, "set": [{"path": ["carries", from], "value": carries}]}));
                }
            }
            "review" => {
                let slug = text(map, "slug")?;
                let members = slugs(present(map, "members"))?;
                let add = slugs(present(map, "add"))?;
                let drop = slugs(present(map, "drop"))?;
                if let Some(current) = self.reviews.get_mut(&slug) {
                    let set: Vec<Json> = ["title", "charter"]
                        .into_iter()
                        .filter_map(|key| present(map, key).map(|v| json!({"path": [key], "value": v})))
                        .collect();
                    if !set.is_empty() {
                        out.push(json!({"op": "edit", "work": slug, "set": set}));
                    }
                    let mut next = if present(map, "members").is_some() { members } else { current.clone() };
                    next.extend(add);
                    next.retain(|m| !drop.contains(m));
                    let added: Vec<&String> = next.difference(current).collect();
                    let dropped: Vec<&String> = current.difference(&next).collect();
                    if !added.is_empty() || !dropped.is_empty() {
                        out.push(object([
                            ("op", Some("member".into())),
                            ("iset", Some(slug.clone().into())),
                            ("add", (!added.is_empty()).then(|| json!(added))),
                            ("drop", (!dropped.is_empty()).then(|| json!(dropped))),
                        ]));
                    }
                    *current = next;
                } else {
                    let mut next = members;
                    next.extend(add);
                    next.retain(|m| !drop.contains(m));
                    out.push(json!({
                        "op": "iset",
                        "slug": slug,
                        "schema": "fs3.review",
                        "data": {"title": text(map, "title")?, "charter": text(map, "charter")?},
                        "members": next,
                    }));
                    self.reviews.insert(slug, next);
                }
            }
            "trial" => {
                let hypothesis = text(map, "hypothesis")?;
                let n = self.trials.entry(hypothesis.clone()).or_insert(0);
                *n += 1;
                out.push(object([
                    ("op", Some("report".into())),
                    ("trial", Some(format!("{hypothesis}#{n}").into())),
                    ("into", Some(json!([hypothesis]))),
                    ("schema", Some("fs3.trial".into())),
                    ("control", present(map, "against").cloned()),
                    ("readings", present(map, "readings").cloned()),
                    ("artifacts", present(map, "artifacts").cloned()),
                    ("data", Some(json!({"note": text(map, "note")?}))),
                ]));
            }
            "accept" | "reject" | "shelve" => {
                let (kind, slug) = target(&["hypothesis", "baseline"])?;
                let rationale = text(map, "rationale")?;
                if kind == "baseline" {
                    if name != "accept" {
                        return Err(format!("{name} of a baseline has no migration"));
                    }
                    out.push(object([
                        ("op", Some("elect".into())),
                        ("baseline", Some(slug.clone().into())),
                        ("rationale", Some(rationale.into())),
                        ("adopts", self.adopts.get(&slug).cloned()),
                    ]));
                } else {
                    let disposition = match name {
                        "accept" => "accepted",
                        "reject" => "rejected",
                        _ => "shelved",
                    };
                    out.push(json!({"op": "judge", "iset": slug, "answer": {"disposition": disposition, "rationale": rationale}}));
                }
            }
            "lesson" => out.push(json!({"op": "judge", "iset": text(map, "review")?, "answer": {"lesson": text(map, "text")?}})),
            "reopen" => out.push(json!({"op": "reopen", "iset": target(&["hypothesis", "review"])?.1})),
            "void" => {
                let (kind, slug) = target(&["trial", "baseline"])?;
                let key = if kind == "trial" { "work" } else { "baseline" };
                let mut op = Map::new();
                op.insert("op".into(), "void".into());
                op.insert(key.into(), slug.into());
                op.insert("reason".into(), text(map, "reason")?.into());
                out.push(Json::Object(op));
            }
            "assign" => out.push(object([
                ("op", Some("assign".into())),
                ("work", Some(target(&["hypothesis", "review"])?.1.into())),
                ("owner", Some(text(map, "owner")?.into())),
                ("instructions", present(map, "instructions").cloned()),
            ])),
            "start" | "stop" | "release" => {
                out.push(json!({"op": name, "work": target(&["hypothesis", "review"])?.1}));
            }
            _ => return Err(format!("FS 3 operation {name:?} has no migration")),
        }
        Ok(())
    }
}
