use serde_json::{Map, Value};
use std::fmt::Write as _;

/// Raw operations preserve explicit empty assignments; projections omit absence.
pub fn project(value: Value) -> Result<Value, serde_json::Error> {
    Ok(clean(value)?.unwrap_or_else(|| Value::Object(Map::new())))
}

fn clean(value: Value) -> Result<Option<Value>, serde_json::Error> {
    Ok(match value {
        Value::Null => None,
        Value::Object(fields) if fields.contains_key("op") => Some(Value::Object(fields)),
        Value::Object(fields) => {
            let mut present = Map::new();
            for (key, value) in fields {
                if let Some(value) = clean(value)? {
                    let _ = present.insert(key, value);
                }
            }
            (!present.is_empty()).then_some(Value::Object(present))
        }
        Value::Array(values) => {
            let mut present = Vec::new();
            for value in values {
                if let Some(value) = clean(value)? {
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
    mark_trials(&mut value);
    let context: Map<_, _> = ["frontier", "project"]
        .iter()
        .filter_map(|key| value.get(*key).map(|v| ((*key).to_owned(), v.clone())))
        .collect();
    implied_routes(&mut value, &context);
    let value = thin(value, "").unwrap_or_else(|| Value::Object(Map::new()));
    if value.as_object().is_some_and(|fields| {
        fields.contains_key("frontier")
            && fields.contains_key("seq")
            && fields
                .keys()
                .all(|key| matches!(key.as_str(), "frontier" | "seq" | "project"))
    }) || value.get("created").is_some()
        || value.get("changes").is_some()
        || value["rehearsed"] == true
        || value.get("warnings").is_some()
        || value.get("raised").is_some()
        || value.get("cleared").is_some()
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
            let _ = write!(out, " · {key} {}", scalar(value));
        }
        routes(out, value);
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
    if let Some(page) = value.as_object() {
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
    let _ = write!(out, "{key}: {}", scalar(value));
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
    let mut columns = Vec::new();
    for row in rows.iter().filter_map(Value::as_object) {
        for name in row.keys() {
            if !columns.contains(name) {
                columns.push(name.clone());
            }
        }
    }
    if key == "members" && columns.iter().any(|name| name == "slug") {
        for name in ["disposition", "rationale"] {
            if !columns.iter().any(|column| column == name) {
                columns.push(name.into());
            }
        }
    }
    if rows
        .first()
        .is_some_and(|row| row.get("statement").is_some())
        && !columns.iter().any(|name| name == "waiver")
    {
        columns.push("waiver".into());
    }
    if !columns.is_empty() {
        let _ = write!(out, "\n{}", columns.join(" | "));
    }
    for row in rows {
        let line = if let Some(row) = row.as_object() {
            columns
                .iter()
                .map(|name| row.get(name).map_or_else(|| "—".into(), scalar))
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
                        let _ = arguments.shift_remove(key);
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
fn mark_trials(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            if let Some(regime) = fields.shift_remove("trials_regime")
                && let Some(trials) = fields.get_mut("trials")
            {
                *trials = Value::String(format!("{} (regime {})", scalar(trials), scalar(&regime)));
            }
            for value in fields.values_mut() {
                mark_trials(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                mark_trials(value);
            }
        }
        _ => {}
    }
}

fn thin(value: Value, key: &str) -> Option<Value> {
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
                    let _ = present.insert(key, value);
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
    if let Some(warnings) = value["warnings"]["items"].as_array() {
        for warning in warnings {
            let mut warning = warning.clone();
            let index = warning
                .as_object_mut()
                .and_then(|w| w.shift_remove("index"));
            if let Some(index) = index {
                let _ = write!(out, "\nop {}: {}", scalar(&index), porcelain(&warning));
            }
        }
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
    if let (Some(settled), Some(total)) = (fields.get("settled"), fields.get("total"))
        && fields.len() == 2
    {
        return Some(format!("settled {settled}/{total}"));
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
                .map(|(key, value)| format!("{key} {}", scalar(value)))
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
                || text.starts_with(['[', '{', '"'])
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
            atom(fields).unwrap_or_else(|| {
                fields
                    .iter()
                    .map(|(key, value)| format!("{key} {}", scalar(value)))
                    .collect::<Vec<_>>()
                    .join(" · ")
            })
        }
        _ => value.to_string(),
    }
}
