use libmcp::{
    DetailLevel, FallbackJsonProjection, ProjectionError, RenderMode, StructuredProjection,
    SurfaceKind, ToolProjection, render_json_porcelain, with_presentation_properties,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Presentation {
    pub render: RenderMode,
    pub detail: DetailLevel,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolOutput {
    concise: Option<Value>,
    full: Value,
}

impl ToolOutput {
    #[must_use]
    pub(crate) fn from_values(concise: Value, full: Value) -> Self {
        Self {
            concise: Some(concise),
            full,
        }
    }

    fn structured(&self, detail: DetailLevel) -> &Value {
        match detail {
            DetailLevel::Concise => self.concise.as_ref().unwrap_or(&self.full),
            DetailLevel::Full => &self.full,
        }
    }

    pub(crate) fn into_full(self) -> Value {
        self.full
    }
}

pub(crate) fn split_presentation(
    arguments: Value,
    operation: &str,
    stage: FaultStage,
) -> Result<(Presentation, Value), FaultRecord> {
    let Value::Object(mut object) = arguments else {
        return Ok((Presentation::default(), arguments));
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
    let detail = object
        .remove("detail")
        .map(|value| {
            serde_json::from_value::<DetailLevel>(value).map_err(|error| {
                FaultRecord::new(
                    FaultKind::InvalidInput,
                    stage,
                    operation,
                    format!("invalid detail level: {error}"),
                )
            })
        })
        .transpose()?
        .unwrap_or(DetailLevel::Concise);
    Ok((Presentation { render, detail }, Value::Object(object)))
}

pub(crate) fn projected_tool_output(
    projection: &impl ToolProjection,
    stage: FaultStage,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let full = projection
        .full_projection()
        .map_err(|error| projection_fault(&error, stage, operation))?;
    Ok(ToolOutput {
        concise: None,
        full,
    })
}

pub(crate) fn fallback_detailed_tool_output(
    concise: &impl Serialize,
    full: &impl Serialize,
    kind: SurfaceKind,
    stage: FaultStage,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let projection = FallbackJsonProjection::new(concise, full, kind)
        .map_err(|error| projection_fault(&error, stage, operation))?;
    Ok(ToolOutput::from_values(
        projection
            .concise_projection()
            .map_err(|error| projection_fault(&error, stage, operation))?,
        projection
            .full_projection()
            .map_err(|error| projection_fault(&error, stage, operation))?,
    ))
}

pub(crate) fn tool_success(output: &ToolOutput, presentation: Presentation) -> Value {
    selected_success(
        output.structured(presentation.detail),
        presentation.render,
        render_json_porcelain,
    )
}

pub(crate) fn selected_success(
    selected: &Value,
    render: RenderMode,
    porcelain: fn(&Value) -> String,
) -> Value {
    match render {
        RenderMode::Porcelain => json!({
            "content": [{
                "type": "text",
                "text": porcelain(selected),
            }],
            "isError": false,
        }),
        RenderMode::Json => json!({
            "content": [],
            "structuredContent": selected,
            "isError": false,
        }),
    }
}

pub(crate) fn with_common_presentation(schema: Value) -> Value {
    with_presentation_properties(schema)
}

fn projection_fault(error: &ProjectionError, stage: FaultStage, operation: &str) -> FaultRecord {
    FaultRecord::new(FaultKind::Internal, stage, operation, error.to_string())
}

impl Default for Presentation {
    fn default() -> Self {
        Self {
            render: RenderMode::Porcelain,
            detail: DetailLevel::Concise,
        }
    }
}
