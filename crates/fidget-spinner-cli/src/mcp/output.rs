use libmcp::{JsonPorcelainConfig, RenderMode, render_json_porcelain};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};

pub(crate) fn split_render_mode(
    arguments: Value,
    operation: &str,
    stage: FaultStage,
) -> Result<(RenderMode, Value), FaultRecord> {
    let Value::Object(mut object) = arguments else {
        return Ok((RenderMode::Porcelain, arguments));
    };
    let render = object
        .remove("render")
        .map(|value| {
            serde_json::from_value::<RenderMode>(value).map_err(|error| {
                FaultRecord::new(
                    FaultKind::InvalidInput,
                    stage,
                    operation,
                    format!("invalid render mode: {error}"),
                )
            })
        })
        .transpose()?
        .unwrap_or(RenderMode::Porcelain);
    Ok((render, Value::Object(object)))
}

pub(crate) fn tool_success(
    value: &impl Serialize,
    render: RenderMode,
    stage: FaultStage,
    operation: &str,
) -> Result<Value, FaultRecord> {
    let structured = serde_json::to_value(value).map_err(|error| {
        FaultRecord::new(FaultKind::Internal, stage, operation, error.to_string())
    })?;
    tool_success_from_value(structured, render, stage, operation)
}

pub(crate) fn tool_success_from_value(
    structured: Value,
    render: RenderMode,
    stage: FaultStage,
    operation: &str,
) -> Result<Value, FaultRecord> {
    let text = match render {
        RenderMode::Porcelain => render_json_porcelain(&structured, JsonPorcelainConfig::default()),
        RenderMode::Json => crate::to_pretty_json(&structured).map_err(|error| {
            FaultRecord::new(FaultKind::Internal, stage, operation, error.to_string())
        })?,
    };
    Ok(json!({
        "content": [{
            "type": "text",
            "text": text,
        }],
        "structuredContent": structured,
        "isError": false,
    }))
}

pub(crate) fn with_render_property(schema: Value) -> Value {
    let Value::Object(mut object) = schema else {
        return schema;
    };

    let properties = object
        .entry("properties".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(properties) = properties {
        let _ = properties.insert(
            "render".to_owned(),
            json!({
                "type": "string",
                "enum": ["porcelain", "json"],
                "description": "Output mode. Defaults to porcelain for model-friendly summaries."
            }),
        );
    }
    let _ = object
        .entry("additionalProperties".to_owned())
        .or_insert(Value::Bool(false));
    Value::Object(object)
}
