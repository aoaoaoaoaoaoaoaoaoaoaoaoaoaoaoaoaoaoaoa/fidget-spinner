use serde_json::{Map, Value};
use std::fmt::Write as _;
use trebuchet::inspect::porcelain;

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
    let value = thin(value.clone(), "").unwrap_or_else(|| Value::Object(Map::new()));
    if value.as_object().is_some_and(|fields| {
        fields.contains_key("frontier")
            && fields.contains_key("seq")
            && fields
                .keys()
                .all(|key| matches!(key.as_str(), "frontier" | "seq" | "project"))
    }) || value.get("changes").is_some()
        || value["rehearsed"] == true
        || value.get("warnings").is_some()
        || value.get("raised").is_some()
        || value.get("cleared").is_some()
    {
        return receipt(&value);
    }
    porcelain(&value, atom)
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
                let _ = write!(
                    out,
                    "\nop {}: {}",
                    scalar(&index),
                    porcelain(&warning, atom)
                );
            }
        }
    }
    for key in ["raised", "cleared"] {
        if let Some(findings) = value.get(key) {
            out.push('\n');
            out.push_str(&porcelain(&serde_json::json!({key:findings}), atom));
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
                .map(|(key, value)| format!("{key} [{}]", porcelain(value, atom)))
                .collect::<Vec<_>>()
                .join("; "),
        );
    }
    None
}

fn scalar(value: &Value) -> String {
    match value {
        Value::Number(number) if number.is_f64() => number
            .as_f64()
            .map_or_else(|| number.to_string(), |n| n.to_string()),
        Value::String(s)
            if !s.contains(['|', '\n', '\r'])
                && s.trim() == s
                && !s.is_empty()
                && !matches!(s.as_str(), "null" | "true" | "false" | "—") =>
        {
            s.clone()
        }
        _ => value.to_string(),
    }
}
