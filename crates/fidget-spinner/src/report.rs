//! Porcelain: the model-facing text rendering of tool projections.

use serde_json::{Map, Value};
use std::fmt::Write as _;

/// Keys holding FS data. Projections keep data exactly, and porcelain renders
/// it as compact JSON with references as `@kind:name` atoms.
const DATA: [&str; 2] = ["data", "answer"];

/// Raw operations and data preserve explicit empty values; projections omit absence.
pub fn project(value: Value) -> Result<Value, serde_json::Error> {
    Ok(clean(value, "")?.unwrap_or_else(|| Value::Object(Map::new())))
}

fn clean(value: Value, key: &str) -> Result<Option<Value>, serde_json::Error> {
    if DATA.contains(&key) {
        return Ok(Some(value));
    }
    Ok(match value {
        Value::Null => None,
        Value::Object(fields) if fields.contains_key("op") => Some(Value::Object(fields)),
        Value::Object(fields) => {
            let mut present = Map::new();
            for (key, value) in fields {
                if let Some(value) = clean(value, &key)? {
                    present.insert(key, value);
                }
            }
            (!present.is_empty()).then_some(Value::Object(present))
        }
        Value::Array(values) => {
            let mut present = Vec::new();
            for value in values {
                if let Some(value) = clean(value, key)? {
                    present.push(value);
                }
            }
            (!present.is_empty()).then_some(Value::Array(present))
        }
        Value::Number(number) if number.is_f64() => {
            let number = number
                .as_f64()
                .map_or_else(|| number.to_string(), |n| n.to_string());
            Some(serde_json::from_str(&number)?)
        }
        value => Some(value),
    })
}

pub fn render(value: &Value) -> String {
    let mut value = value.clone();
    if value["total"] == 0 {
        value["matches"] = Value::String(format!(
            "0/{}",
            value.get("scope_total").unwrap_or(&Value::from(0))
        ));
    }
    let context: Map<_, _> = ["frontier", "project"]
        .iter()
        .filter_map(|key| value.get(*key).map(|v| ((*key).to_owned(), v.clone())))
        .collect();
    implied_routes(&mut value, &context);
    let value = thin(value, "").unwrap_or_else(|| Value::Object(Map::new()));
    let bare = value.as_object().is_some_and(|fields| {
        fields.contains_key("frontier")
            && fields.contains_key("seq")
            && fields
                .keys()
                .all(|key| matches!(key.as_str(), "frontier" | "seq" | "project"))
    });
    if bare
        || value["rehearsed"] == true
        || [
            "created", "changes", "warnings", "raised", "cleared", "ready", "voided",
        ]
        .iter()
        .any(|key| value.get(*key).is_some())
    {
        return receipt(&value);
    }
    porcelain(&value)
}

fn porcelain(value: &Value) -> String {
    let mut out = String::new();
    if let Some(fields) = value.as_object() {
        let page = fields.get("items").and_then(Value::as_array);
        for (key, value) in fields {
            if page.is_some() && ["items", "total", "next_cursor"].contains(&key.as_str()) {
                continue;
            }
            if !out.is_empty() {
                out.push('\n');
            }
            field(&mut out, key, value);
        }
        if let Some(rows) = page {
            if !out.is_empty() {
                out.push('\n');
            }
            table(&mut out, "matches", rows, fields);
        }
    } else {
        out.push_str(&scalar(value));
    }
    out
}
fn field(out: &mut String, key: &str, value: &Value) {
    if key == "incumbent_baseline"
        && let Some(fields) = value.as_object()
    {
        let _ = write!(out, "incumbent baseline {}", scalar(&value["slug"]));
        for (key, value) in fields.iter().filter(|(key, _)| key.as_str() != "slug") {
            let _ = write!(out, " · {key} {}", cell(key, value));
        }
        routes(out, value);
        return;
    }
    if key == "grammar"
        && let Some(text) = value.as_str()
    {
        let _ = write!(out, "grammar:\n{text}");
        return;
    }
    if key == "matches" {
        let _ = write!(out, "matches {}", scalar(value));
        return;
    }
    if key == "ignored" {
        let _ = write!(
            out,
            "{}",
            value
                .as_array()
                .into_iter()
                .flatten()
                .map(|warning| format!("warning: {}", scalar(warning)))
                .collect::<Vec<_>>()
                .join("\n")
        );
        return;
    }
    if !DATA.contains(&key)
        && let Some(page) = value.as_object()
    {
        if let Some(rows) = page.get("items").and_then(Value::as_array) {
            table(out, key, rows, page);
            return;
        }
        if page.values().any(|value| {
            value
                .as_object()
                .is_some_and(|fields| fields.contains_key("items"))
        }) {
            let _ = write!(out, "{key}:\n{}", porcelain(value));
            return;
        }
    }
    let _ = write!(out, "{key}: {}", cell(key, value));
    routes(out, value);
}
fn table(out: &mut String, key: &str, rows: &[Value], page: &Map<String, Value>) {
    let _ = write!(
        out,
        "{key} {}/{}",
        rows.len(),
        page.get("total")
            .map_or_else(|| rows.len().to_string(), scalar)
    );
    // Rows omit absent fields, so a column first seen in a later row is placed
    // after its predecessor there, not appended, to keep the projection's order.
    let mut columns: Vec<String> = Vec::new();
    for row in rows.iter().filter_map(Value::as_object) {
        let mut at = 0;
        for name in row.keys() {
            if let Some(index) = columns.iter().position(|column| column == name) {
                at = index + 1;
            } else {
                columns.insert(at, name.clone());
                at += 1;
            }
        }
    }
    if !columns.is_empty() {
        let _ = write!(out, "\n{}", columns.join(" | "));
    }
    for row in rows {
        let line = if let Some(row) = row.as_object() {
            columns
                .iter()
                .map(|name| row.get(name).map_or_else(|| "—".into(), |v| cell(name, v)))
                .collect::<Vec<_>>()
                .join(" | ")
        } else {
            scalar(row)
        };
        let _ = write!(out, "\n{line}");
    }
    let mut seen = Vec::new();
    for row in rows {
        let mut value = row.clone();
        template_routes(&mut value, row);
        let mut rendered = String::new();
        routes(&mut rendered, &value);
        for line in rendered.lines().filter(|line| !line.is_empty()) {
            if !seen.contains(&line.to_owned()) {
                let _ = write!(out, "\n{line}");
                seen.push(line.to_owned());
            }
        }
    }
    if let Some(cursor) = page.get("next_cursor") {
        let _ = write!(out, "\nmore: {key} cursor={}", scalar(cursor));
    }
    if let Some(read) = page.get("read") {
        let _ = write!(out, "\nmore: {}", scalar(read));
    }
}
fn template_routes(value: &mut Value, row: &Value) {
    match value {
        Value::Object(fields) => {
            if fields.contains_key("tool")
                && let Some(arguments) = fields.get_mut("arguments").and_then(Value::as_object_mut)
            {
                for value in arguments.values_mut() {
                    if let Some((name, _)) =
                        row.as_object().into_iter().flat_map(|row| row.iter()).find(
                            |(_, column)| {
                                *column == value && !value.is_object() && !value.is_array()
                            },
                        )
                    {
                        *value = Value::String(format!("<{name}>"));
                    }
                }
            }
            for value in fields.values_mut() {
                template_routes(value, row);
            }
        }
        Value::Array(values) => {
            for value in values {
                template_routes(value, row);
            }
        }
        _ => {}
    }
}
fn routes(out: &mut String, value: &Value) {
    match value {
        Value::Object(fields) => {
            if let Some(route) = fields.get("full_read") {
                let _ = write!(out, "\nmore: {}", scalar(route));
            } else {
                for (name, value) in fields {
                    if name != "items" {
                        routes(out, value);
                    }
                }
            }
        }
        Value::Array(items) => {
            for value in items {
                routes(out, value);
            }
        }
        _ => {}
    }
}
fn implied_routes(value: &mut Value, context: &Map<String, Value>) {
    match value {
        Value::Object(fields) => {
            if fields.contains_key("tool")
                && let Some(arguments) = fields.get_mut("arguments").and_then(Value::as_object_mut)
            {
                for (key, value) in context {
                    if arguments.get(key) == Some(value) {
                        arguments.shift_remove(key);
                    }
                }
            }
            for value in fields.values_mut() {
                implied_routes(value, context);
            }
        }
        Value::Array(values) => {
            for value in values {
                implied_routes(value, context);
            }
        }
        _ => {}
    }
}

fn thin(value: Value, key: &str) -> Option<Value> {
    if DATA.contains(&key) {
        return Some(value);
    }
    match value {
        Value::Object(fields) if fields.contains_key("op") => Some(Value::Object(fields)),
        Value::Object(fields) => {
            if fields.get("total") == Some(&Value::from(0))
                && fields.keys().all(|k| {
                    matches!(
                        k.as_str(),
                        "items" | "total" | "next_cursor" | "read" | "settled"
                    )
                })
            {
                return None;
            }
            let counts = matches!(key, "counts" | "members" | "before" | "after");
            let mut present = Map::new();
            for (key, value) in fields {
                let zero_count = value == 0
                    && (counts
                        || matches!(
                            key.as_str(),
                            "trials"
                                | "total"
                                | "scope_total"
                                | "calls"
                                | "errors"
                                | "rows"
                                | "incomplete_pages"
                                | "previews"
                                | "output_utf8_bytes"
                                | "mcp_result_utf8_bytes"
                                | "micros"
                        ));
                if !zero_count && let Some(value) = thin(value, &key) {
                    present.insert(key, value);
                }
            }
            (!present.is_empty()).then_some(Value::Object(present))
        }
        Value::Array(values) => {
            let values: Vec<_> = values.into_iter().filter_map(|v| thin(v, key)).collect();
            (!values.is_empty()).then_some(Value::Array(values))
        }
        Value::Null => None,
        value => Some(value),
    }
}

fn receipt(value: &Value) -> String {
    let mut out = format!("frontier {}", scalar(&value["frontier"]));
    if value["rehearsed"] == true {
        out.push_str(" · rehearsed, not applied");
    } else if let Some(seq) = value.get("seq") {
        let _ = write!(out, " · seq {}", scalar(seq));
    }
    if let Some(project) = value.get("project") {
        let _ = write!(out, " · project {}", scalar(project));
    }
    for warning in value["warnings"]["items"].as_array().into_iter().flatten() {
        let text = |key: &str| warning[key].as_str().unwrap_or_default().replace('\n', " ");
        let _ = write!(
            out,
            "\nop {}: {}: {}",
            scalar(&warning["index"]),
            text("subject"),
            text("message")
        );
    }
    if let Some(created) = value.get("created") {
        let _ = write!(out, "\ncreated: {}", scalar(created));
    }
    for key in ["raised", "cleared"] {
        if let Some(findings) = value.get(key) {
            out.push('\n');
            out.push_str(&porcelain(&serde_json::json!({key:findings})));
        }
    }
    if let Some(changes) = value["changes"].as_object() {
        let line = changes
            .iter()
            .map(|(state, counts)| {
                format!(
                    "{state} {}→{}",
                    scalar(&counts["before"]),
                    scalar(&counts["after"])
                )
            })
            .collect::<Vec<_>>()
            .join(" · ");
        if !line.is_empty() {
            out.push('\n');
            out.push_str(&line);
        }
    }
    if let Some(ready) = value.get("ready") {
        let _ = write!(out, "\nready: {}", scalar(ready));
    }
    for voided in value["voided"].as_array().into_iter().flatten() {
        let _ = write!(out, "\nvoided {}", scalar(&voided["void"]));
        if let Some(consumers) = voided.get("consumers") {
            let _ = write!(out, " · rests on it: {}", scalar(consumers));
        }
        if let Some(waiting) = voided.get("waiting") {
            let _ = write!(out, " · waiting on it: {}", scalar(waiting));
        }
    }
    out
}

fn atom(fields: &Map<String, Value>) -> Option<String> {
    if fields.len() == 1
        && let Some(epoch) = fields.get("epoch").and_then(Value::as_object)
    {
        return Some(format!(
            "opened epoch of baseline {} · since {}",
            scalar(&epoch["baseline"]),
            scalar(&epoch["since"])
        ));
    }
    if fields.contains_key("items") || fields.contains_key("preview") || fields.contains_key("tool")
    {
        return None;
    }
    if fields.len() == 2
        && let (Some(value), Some(unit)) = (fields.get("value"), fields.get("unit"))
    {
        return Some(format!("{} {}", scalar(value), scalar(unit)));
    }
    if fields.len() == 2
        && let (Some(value), Some(unlisted)) = (fields.get("value"), fields.get("unlisted"))
    {
        return Some(format!("{} unlisted={unlisted}", scalar(value)));
    }
    if !fields.is_empty() && fields.values().all(|v| !v.is_object() && !v.is_array()) {
        return Some(
            fields
                .iter()
                .map(|(key, value)| format!("{key} {}", cell(key, value)))
                .collect::<Vec<_>>()
                .join(" · "),
        );
    }
    if !fields.is_empty()
        && fields
            .values()
            .all(|v| v.as_object().is_some_and(|o| o.contains_key("value")))
    {
        return Some(
            fields
                .iter()
                .map(|(key, value)| {
                    let mut reading = format!("{key} {}", scalar(&value["value"]));
                    if let Some(unit) = value.get("unit") {
                        reading.push(' ');
                        reading.push_str(&scalar(unit));
                    }
                    if value["unlisted"] == true {
                        reading.push_str(" (unlisted)");
                    }
                    reading
                })
                .collect::<Vec<_>>()
                .join(" · "),
        );
    }
    if !fields.is_empty()
        && fields
            .values()
            .all(|v| v.as_object().is_some_and(|o| o.contains_key("ratio")))
    {
        return Some(
            fields
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{key} {}× {}",
                        scalar(&value["ratio"]),
                        value["movement"].as_str().unwrap_or("undirected")
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
        );
    }
    None
}

/// A value under its key: data compactly, anything else as a scalar.
fn cell(key: &str, value: &Value) -> String {
    if DATA.contains(&key) {
        compact(value).replace('|', "\\u007c")
    } else {
        scalar(value)
    }
}

/// Compact JSON with references as `@kind:name`, or `@kind:-` once removed.
fn compact(value: &Value) -> String {
    match value {
        Value::Object(fields) => {
            if fields.get("$fs").and_then(Value::as_str) == Some("ref") {
                let kind = fields
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("record");
                let id = fields.get("id").and_then(Value::as_str).unwrap_or("-");
                return format!("@{kind}:{id}");
            }
            if let (Some(Value::String(preview)), Some(route)) =
                (fields.get("preview"), fields.get("full_read"))
                && route.get("tool").is_some()
            {
                return format!("{preview}…");
            }
            let body = fields
                .iter()
                .map(|(key, value)| format!("{}:{}", Value::String(key.clone()), compact(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(items) => format!(
            "[{}]",
            items.iter().map(compact).collect::<Vec<_>>().join(",")
        ),
        value => value.to_string(),
    }
}

fn scalar(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Number(number) if number.is_f64() => number
            .as_f64()
            .map_or_else(|| number.to_string(), |n| n.to_string()),
        Value::String(text) => {
            let ambiguous = text.is_empty()
                || text.trim() != text
                || matches!(
                    text.as_str(),
                    "null" | "true" | "false" | "—" | "empty list" | "empty object"
                )
                || text.starts_with(['[', '{', '"', '@'])
                || text.contains([';', '·', '\\', '\n', '\r']);
            if ambiguous {
                Value::String(text.clone())
                    .to_string()
                    .replace('|', "\\u007c")
            } else {
                text.replace('|', "\\u007c")
            }
        }
        Value::Array(items) => {
            if items.is_empty() {
                "empty list".into()
            } else {
                items.iter().map(scalar).collect::<Vec<_>>().join("; ")
            }
        }
        Value::Object(fields) => {
            if fields.is_empty() {
                return "empty object".into();
            }
            if fields.get("$fs").and_then(Value::as_str) == Some("ref") {
                return compact(value);
            }
            if let Some(preview) = fields.get("preview") {
                return format!("{}…", scalar(preview));
            }
            if let Some(tool) = fields.get("tool") {
                let arguments = fields
                    .get("arguments")
                    .and_then(Value::as_object)
                    .map(|args| {
                        args.iter()
                            .map(|(key, value)| format!("{key}={}", scalar(value)))
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .unwrap_or_default();
                return format!("{} {arguments}", scalar(tool));
            }
            // Groups of fields separate with semicolons, fields within a group with dots.
            let separator = if fields.values().any(Value::is_object) {
                "; "
            } else {
                " · "
            };
            atom(fields).unwrap_or_else(|| {
                fields
                    .iter()
                    .map(|(key, value)| format!("{key} {}", cell(key, value)))
                    .collect::<Vec<_>>()
                    .join(separator)
            })
        }
        _ => value.to_string(),
    }
}
