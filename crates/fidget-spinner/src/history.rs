use std::collections::{BTreeMap, BTreeSet};

use fidget_spinner_core::Op;
use serde_json::{Value, json};
use trebuchet::ledger::Entry;

type Key = (&'static str, String);

#[derive(Default)]
struct Lineage {
    live: BTreeMap<Key, usize>,
    dead: BTreeSet<usize>,
    trials: BTreeMap<usize, u64>,
    next: usize,
}

impl Lineage {
    fn key(&mut self, kind: &'static str, name: &str, create: bool) -> Option<usize> {
        let key = (kind, name.to_owned());
        if create && !self.live.contains_key(&key) {
            let id = self.next;
            self.next += 1;
            let _ = self.live.insert(key.clone(), id);
        }
        self.live.get(&key).copied()
    }

    fn remove(&mut self, kind: &'static str, name: &str) {
        if let Some(id) = self.live.remove(&(kind, name.to_owned())) {
            let _ = self.dead.insert(id);
        }
        if kind == "hypothesis" {
            let prefix = format!("{name}#");
            let keys: Vec<_> = self
                .live
                .keys()
                .filter(|(k, s)| *k == "trial" && s.starts_with(&prefix))
                .cloned()
                .collect();
            for key in keys {
                if let Some(id) = self.live.remove(&key) {
                    let _ = self.dead.insert(id);
                }
            }
        }
    }

    fn rename(&mut self, kind: &'static str, from: &str, to: &str) {
        if let Some(id) = self.live.remove(&(kind, from.to_owned())) {
            let _ = self.live.insert((kind, to.to_owned()), id);
        }
        if kind == "hypothesis" {
            let prefix = format!("{from}#");
            let keys: Vec<_> = self
                .live
                .keys()
                .filter(|(k, s)| *k == "trial" && s.starts_with(&prefix))
                .cloned()
                .collect();
            for key in keys {
                if let Some(id) = self.live.remove(&key) {
                    let suffix = &key.1[from.len()..];
                    let _ = self.live.insert(("trial", format!("{to}{suffix}")), id);
                }
            }
        }
    }

    fn operation(&mut self, op: &Value) -> Vec<(usize, &'static str)> {
        let name = op["op"].as_str().unwrap_or_default();
        let mut refs = Vec::new();
        for kind in ["hypothesis", "review", "baseline", "trial"] {
            if let Some(slug) = op[kind].as_str()
                && let Some(id) = self.key(kind, slug, false)
            {
                refs.push((id, kind));
            }
        }
        let creation = match name {
            "hypothesis" | "review" | "baseline" => Some(name),
            _ => None,
        };
        if let Some(kind) = creation {
            let kind = match kind {
                "hypothesis" => "hypothesis",
                "review" => "review",
                _ => "baseline",
            };
            if let Some(slug) = op["slug"].as_str()
                && let Some(id) = self.key(kind, slug, true)
            {
                refs.push((id, kind));
            }
        }
        if name == "regime"
            && let Some(slug) = op["bridge"]["slug"].as_str()
            && let Some(id) = self.key("baseline", slug, true)
        {
            refs.push((id, "baseline"));
        }
        for (field, kind) in [
            ("after", "hypothesis"),
            ("adopts", "hypothesis"),
            ("members", "hypothesis"),
            ("add", "hypothesis"),
            ("drop", "hypothesis"),
            ("docket", "hypothesis"),
            ("reviews", "review"),
        ] {
            if let Some(values) = op[field].as_array() {
                for slug in values.iter().filter_map(Value::as_str) {
                    if let Some(id) = self.key(kind, slug, false) {
                        refs.push((id, kind));
                    }
                }
            }
        }
        for (field, kind) in [
            ("from", "hypothesis"),
            ("to", "hypothesis"),
            ("against", "baseline"),
        ] {
            if name != "rename"
                && let Some(slug) = op[field].as_str()
                && let Some(id) = self.key(kind, slug, false)
            {
                refs.push((id, kind));
            }
        }
        if let Some(finding) = op["finding"].as_str() {
            let mut parts = finding.split('/');
            match parts.next() {
                Some("unbacked") => {
                    if let Some(slug) = parts.next()
                        && let Some(id) = self.key("hypothesis", slug, false)
                    {
                        refs.push((id, "hypothesis"));
                    }
                }
                Some("unread" | "unaccepted") => {
                    if let Some(slug) = parts.next()
                        && let Some(id) = self.key("baseline", slug, false)
                    {
                        refs.push((id, "baseline"));
                    }
                    if finding.starts_with("unaccepted/")
                        && let Some(slug) = parts.next()
                        && let Some(id) = self.key("hypothesis", slug, false)
                    {
                        refs.push((id, "hypothesis"));
                    }
                }
                _ => {}
            }
        }
        if name == "trial"
            && let Some(h) = op["hypothesis"].as_str()
            && let Some(id) = self.key("hypothesis", h, false)
        {
            let n = self.trials.entry(id).or_default();
            *n += 1;
            let slug = format!("{h}#{n}");
            if let Some(id) = self.key("trial", &slug, true) {
                refs.push((id, "trial"));
            }
        }
        if name == "remove" {
            for kind in ["hypothesis", "review", "baseline", "trial"] {
                if let Some(slug) = op[kind].as_str() {
                    self.remove(kind, slug);
                }
            }
        }
        if name == "rename"
            && let Some(to) = op["to"].as_str()
        {
            for kind in ["hypothesis", "review", "baseline"] {
                if let Some(from) = op[kind].as_str() {
                    self.rename(kind, from, to);
                }
            }
        }
        refs
    }
}

/// Redaction follows record lifetimes, not slugs: rename keeps a lifetime, while
/// removal and recreation create distinct ones. Current removal also applies
/// to history continued from an older snapshot.
pub fn redacted(entries: &[Entry<Op>]) -> Result<BTreeMap<i64, Vec<Value>>, serde_json::Error> {
    let mut lineage = Lineage::default();
    let mut operations = Vec::new();
    for entry in entries.iter().rev() {
        let mut rows = Vec::new();
        for op in &entry.ops {
            let value = serde_json::to_value(op)?;
            let refs = lineage.operation(&value);
            rows.push((value, refs));
        }
        operations.push((entry.seq, rows));
    }
    Ok(operations
        .into_iter()
        .map(|(seq, rows)| {
            (
                seq,
                rows.into_iter()
                    .map(|(op, refs)| {
                        if let Some((_, kind)) =
                            refs.iter().find(|(id, _)| lineage.dead.contains(id))
                        {
                            json!({"op":op["op"],"kind":kind})
                        } else {
                            op
                        }
                    })
                    .collect(),
            )
        })
        .collect())
}

pub fn targets(op: &Value) -> Value {
    if op.get("kind").is_some() {
        return json!({"op":op["op"],"kind":op["kind"]});
    }
    let mut target = json!({"op":op["op"]});
    for field in [
        "slug",
        "hypothesis",
        "review",
        "baseline",
        "trial",
        "from",
        "to",
    ] {
        if let Some(value) = op.get(field) {
            target[field] = value.clone();
        }
    }
    target
}
