use fidget_spinner_store_sqlite::{FrontierSqlQueryResult, FrontierSqlSchema};
use serde_json::Value;

use crate::mcp::fault::{FaultRecord, FaultStage};
use crate::mcp::output::{ToolOutput, fallback_detailed_tool_output};

pub(super) fn schema_output(
    schema: &FrontierSqlSchema,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let text = render_schema(schema);
    fallback_detailed_tool_output(
        schema,
        schema,
        text.clone(),
        Some(text),
        libmcp::SurfaceKind::Read,
        FaultStage::Worker,
        operation,
    )
}

pub(super) fn sql_output(
    result: &FrontierSqlQueryResult,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    let text = render_table(result);
    fallback_detailed_tool_output(
        result,
        result,
        text.clone(),
        Some(text),
        libmcp::SurfaceKind::Read,
        FaultStage::Worker,
        operation,
    )
}

fn render_schema(schema: &FrontierSqlSchema) -> String {
    let mut lines = vec!["view|column|type|description".to_owned()];
    for view in &schema.views {
        for column in view.columns {
            lines.push(format!(
                "{}|{}|{}|{}",
                view.name,
                column.name,
                column.sql_type,
                table_cell_text(&Value::String(column.description.to_owned()))
            ));
        }
    }
    lines.join("\n")
}

fn render_table(result: &FrontierSqlQueryResult) -> String {
    if result.columns.is_empty() {
        return "(no columns)".to_owned();
    }
    let mut lines = vec![result.columns.join("|")];
    lines.extend(result.rows.iter().map(|row| {
        row.iter()
            .map(table_cell_text)
            .collect::<Vec<_>>()
            .join("|")
    }));
    if result.rows.is_empty() {
        lines.push("(0 rows)".to_owned());
    }
    if result.truncated {
        lines.push(format!("... truncated at {} rows", result.row_count));
    }
    lines.join("\n")
}

fn table_cell_text(value: &Value) -> String {
    let raw = match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    };
    raw.chars()
        .map(|character| match character {
            '\n' | '\r' | '\t' => ' ',
            '|' => '/',
            character => character,
        })
        .collect()
}
