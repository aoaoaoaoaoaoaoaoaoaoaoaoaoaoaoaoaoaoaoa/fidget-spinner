//! History redaction. Operations naming a removed record keep only their
//! operation name and the record's kind; data references to removed records
//! render as removed. Redaction follows incarnations, not names: a rename keeps
//! one, and a name reused after removal is another.

use std::collections::BTreeMap;

use fidget_spinner_core::{Frontier, Id, Op, Refusal, Removed, Stamp, Trace};
use serde_json::{Value, json};
use trebuchet::ledger::Entry;

/// Replay newest-first `entries` from the frontier's first patch and redact
/// each operation against the final state's removals.
pub fn redacted(entries: &[Entry<Op>]) -> Result<BTreeMap<i64, Vec<Value>>, Refusal> {
    let mut frontier = Frontier::default();
    let mut traced = Vec::with_capacity(entries.len());
    for entry in entries.iter().rev() {
        let stamp = Stamp {
            seq: entry.seq,
            time: entry.at,
        };
        traced.push((entry, frontier.trace(&entry.ops, stamp)?));
    }
    let removed = frontier.removed();
    Ok(traced
        .into_iter()
        .map(|(entry, traces)| {
            let ops = entry
                .ops
                .iter()
                .zip(traces)
                .map(|(op, trace)| redact(op, &trace, removed))
                .collect();
            (entry.seq, ops)
        })
        .collect())
}

fn redact(op: &Op, trace: &Trace, removed: &BTreeMap<Id, Removed>) -> Value {
    let named = trace.named.iter().map(|(_, id)| id);
    if let Some(gone) = trace
        .created
        .iter()
        .chain(named)
        .find_map(|id| removed.get(id))
    {
        return json!({"op": op.name(), "kind": gone.kind.as_str()});
    }
    let mut raw = op.raw().clone();
    let gone: Vec<&str> = trace
        .refs
        .iter()
        .filter(|(_, id)| removed.contains_key(id))
        .map(|(name, _)| name.as_str())
        .collect();
    if !gone.is_empty() {
        unname(&mut raw, &gone);
    }
    raw
}

/// Render data references to removed records as removed, as reads do.
fn unname(value: &mut Value, gone: &[&str]) {
    match value {
        Value::Object(fields) => {
            if fields.get("$fs").and_then(Value::as_str) == Some("ref")
                && fields.get("kind").and_then(Value::as_str) != Some("metric")
                && fields
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| gone.contains(&id))
            {
                fields.shift_remove("id");
                fields.insert("removed".into(), Value::Bool(true));
                return;
            }
            for value in fields.values_mut() {
                unname(value, gone);
            }
        }
        Value::Array(values) => {
            for value in values {
                unname(value, gone);
            }
        }
        _ => {}
    }
}

/// A history row's summary of one operation: its name and the fields that
/// name its subject.
pub fn targets(op: &Value) -> Value {
    let mut target = json!({"op": op["op"]});
    let subjects: &[&str] = if op["op"] == "define" {
        &["schema"]
    } else {
        &["kind", "slug", "work", "trial", "iset", "baseline", "to"]
    };
    for field in subjects {
        if let Some(value) = op.get(*field).filter(|v| v.is_string()) {
            target[*field] = value.clone();
        }
    }
    target
}
