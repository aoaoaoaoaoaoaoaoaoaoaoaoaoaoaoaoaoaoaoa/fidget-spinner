use libmcp::{
    DetailLevel, JsonPorcelainConfig, RenderMode, render_json_porcelain,
    with_presentation_properties,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};

const CONCISE_PORCELAIN_MAX_LINES: usize = 12;
const CONCISE_PORCELAIN_MAX_INLINE_CHARS: usize = 160;
const FULL_PORCELAIN_MAX_LINES: usize = 40;
const FULL_PORCELAIN_MAX_INLINE_CHARS: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Presentation {
    pub render: RenderMode,
    pub detail: DetailLevel,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolOutput {
    concise: Value,
    full: Value,
    concise_text: String,
    full_text: Option<String>,
}

impl ToolOutput {
    #[must_use]
    pub(crate) fn from_values(
        concise: Value,
        full: Value,
        concise_text: impl Into<String>,
        full_text: Option<String>,
    ) -> Self {
        Self {
            concise,
            full,
            concise_text: concise_text.into(),
            full_text,
        }
    }

    fn structured(&self, detail: DetailLevel) -> &Value {
        match detail {
            DetailLevel::Concise => &self.concise,
            DetailLevel::Full => &self.full,
        }
    }

    fn porcelain_text(&self, detail: DetailLevel) -> String {
        match detail {
            DetailLevel::Concise => self.concise_text.clone(),
            DetailLevel::Full => self
                .full_text
                .clone()
                .unwrap_or_else(|| render_json_porcelain(&self.full, full_porcelain_config())),
        }
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

pub(crate) fn tool_output(
    value: &impl Serialize,
    stage: FaultStage,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let structured = serde_json::to_value(value).map_err(|error| {
        FaultRecord::new(FaultKind::Internal, stage, operation, error.to_string())
    })?;
    let concise_text = render_json_porcelain(&structured, concise_porcelain_config());
    Ok(ToolOutput::from_values(
        structured.clone(),
        structured,
        concise_text,
        None,
    ))
}

pub(crate) fn detailed_tool_output(
    concise: &impl Serialize,
    full: &impl Serialize,
    concise_text: impl Into<String>,
    full_text: Option<String>,
    stage: FaultStage,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let concise = serde_json::to_value(concise).map_err(|error| {
        FaultRecord::new(FaultKind::Internal, stage, operation, error.to_string())
    })?;
    let full = serde_json::to_value(full).map_err(|error| {
        FaultRecord::new(FaultKind::Internal, stage, operation, error.to_string())
    })?;
    Ok(ToolOutput::from_values(
        concise,
        full,
        concise_text,
        full_text,
    ))
}

pub(crate) fn tool_success(
    output: ToolOutput,
    presentation: Presentation,
    stage: FaultStage,
    operation: &str,
) -> Result<Value, FaultRecord> {
    let structured = output.structured(presentation.detail).clone();
    let text = match presentation.render {
        RenderMode::Porcelain => output.porcelain_text(presentation.detail),
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

pub(crate) fn with_common_presentation(schema: Value) -> Value {
    with_presentation_properties(schema)
}

const fn concise_porcelain_config() -> JsonPorcelainConfig {
    JsonPorcelainConfig {
        max_lines: CONCISE_PORCELAIN_MAX_LINES,
        max_inline_chars: CONCISE_PORCELAIN_MAX_INLINE_CHARS,
    }
}

const fn full_porcelain_config() -> JsonPorcelainConfig {
    JsonPorcelainConfig {
        max_lines: FULL_PORCELAIN_MAX_LINES,
        max_inline_chars: FULL_PORCELAIN_MAX_INLINE_CHARS,
    }
}

impl Default for Presentation {
    fn default() -> Self {
        Self {
            render: RenderMode::Porcelain,
            detail: DetailLevel::Concise,
        }
    }
}
