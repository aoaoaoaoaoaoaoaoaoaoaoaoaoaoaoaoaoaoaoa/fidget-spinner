use fidget_spinner_store_sqlite::{FrontierSqlQueryResult, FrontierSqlSchema};
use serde_json::Value;

use crate::mcp::fault::{FaultRecord, FaultStage};
use crate::mcp::output::{ToolOutput, fallback_detailed_tool_output};

pub(super) fn schema_output(
    schema: &FrontierSqlSchema,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    fallback_detailed_tool_output(
        schema,
        schema,
        libmcp::SurfaceKind::Read,
        FaultStage::Worker,
        operation,
    )
}

pub(super) fn sql_output(
    result: &FrontierSqlQueryResult,
    operation: &str,
) -> Result<ToolOutput, FaultRecord> {
    fallback_detailed_tool_output(
        result,
        result,
        libmcp::SurfaceKind::Read,
        FaultStage::Worker,
        operation,
    )
}

#[expect(
    clippy::expect_used,
    reason = "the encoder receives only the typed SQL result projection"
)]
pub(super) fn sql_porcelain(selected: &Value) -> String {
    if selected["rows"].as_array().expect("SQL rows").len() < 2 {
        return libmcp::render_json_porcelain(selected);
    }
    let mut layout = selected.clone();
    // Numeric headers are positions in `columns`, not SQL names: duplicate and
    // arbitrary SQL column labels remain representable without losing a cell.
    for row in layout["rows"].as_array_mut().expect("SQL rows") {
        let cells = row.as_array().expect("SQL row cells");
        *row = Value::Object(
            cells
                .iter()
                .enumerate()
                .map(|(index, cell)| (index.to_string(), cell.clone()))
                .collect(),
        );
    }
    libmcp::render_json_porcelain(&layout)
}
