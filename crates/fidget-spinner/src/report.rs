use serde_json::{Map, Value};
use trebuchet::inspect::porcelain;

pub fn render(value: &Value) -> String {
    porcelain(value, atom)
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
        && fields.values().all(|v| {
            v.as_object()
                .is_some_and(|o| o.contains_key("value") || o.contains_key("ratio"))
        })
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
