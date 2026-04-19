use std::time::{Duration, Instant};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OpenFlags, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use fidget_spinner_core::{ExperimentStatus, FrontierId, MetricDefinitionKind};

use super::{ProjectStore, STATE_DB_NAME, StoreError};

const DEFAULT_MAX_ROWS: usize = 200;
const HARD_MAX_ROWS: usize = 1_000;
const DEFAULT_TIMEOUT_MS: u64 = 250;
const HARD_TIMEOUT_MS: u64 = 2_000;
const MAX_SQL_BYTES: i32 = 32 * 1_024;
const MAX_RESULT_BYTES: usize = 256 * 1_024;

#[derive(Clone, Debug, Deserialize)]
pub struct FrontierSqlQuery {
    pub frontier: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Value>,
    pub max_rows: Option<u32>,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FrontierSqlQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
    pub truncated: bool,
    pub max_rows: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct FrontierSqlSchema {
    pub views: Vec<FrontierSqlView>,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct FrontierSqlView {
    pub name: &'static str,
    pub description: &'static str,
    pub columns: &'static [FrontierSqlColumn],
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct FrontierSqlColumn {
    pub name: &'static str,
    pub sql_type: &'static str,
    pub description: &'static str,
}

#[derive(Clone, Debug)]
struct SyntheticMetricQueryValue {
    experiment_id: String,
    metric_id: String,
    value: f64,
}

impl ProjectStore {
    pub fn frontier_query_schema(&self, frontier: &str) -> Result<FrontierSqlSchema, StoreError> {
        let _ = self.resolve_frontier(frontier)?;
        Ok(FrontierSqlSchema {
            views: QUERY_VIEWS.to_vec(),
        })
    }

    pub fn frontier_query_sql(
        &self,
        request: FrontierSqlQuery,
    ) -> Result<FrontierSqlQueryResult, StoreError> {
        let frontier = self.resolve_frontier(&request.frontier)?;
        let synthetic_values = self.frontier_synthetic_metric_values(frontier.id)?;
        let connection = self.open_frontier_query_connection(
            &frontier.id.to_string(),
            &request,
            &synthetic_values,
        )?;
        execute_frontier_query(&connection, request)
    }

    fn open_frontier_query_connection(
        &self,
        frontier_id: &str,
        request: &FrontierSqlQuery,
        synthetic_values: &[SyntheticMetricQueryValue],
    ) -> Result<Connection, StoreError> {
        let connection = Connection::open_with_flags(
            self.state_root.join(STATE_DB_NAME).as_std_path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        connection.pragma_update(None, "foreign_keys", 1_i64)?;
        connection.pragma_update(None, "temp_store", 2_i64)?;
        tighten_query_limits(&connection)?;
        install_frontier_query_views(&connection, frontier_id, synthetic_values)?;
        connection.pragma_update(None, "query_only", 1_i64)?;
        connection.authorizer(Some(authorize_frontier_query));
        install_progress_deadline(&connection, request.timeout_ms);
        Ok(connection)
    }

    fn frontier_synthetic_metric_values(
        &self,
        frontier_id: FrontierId,
    ) -> Result<Vec<SyntheticMetricQueryValue>, StoreError> {
        let synthetic_metrics = self
            .list_metric_definitions()?
            .into_iter()
            .filter(|metric| metric.kind == MetricDefinitionKind::Synthetic)
            .collect::<Vec<_>>();
        if synthetic_metrics.is_empty() {
            return Ok(Vec::new());
        }
        let experiments = self
            .load_experiment_records(Some(frontier_id), None)?
            .into_iter()
            .filter(|record| record.status == ExperimentStatus::Closed)
            .collect::<Vec<_>>();
        let mut values = Vec::new();
        for experiment in experiments {
            for metric in &synthetic_metrics {
                if let Some(value) =
                    self.experiment_metric_canonical_value(experiment.id, metric.id)?
                {
                    values.push(SyntheticMetricQueryValue {
                        experiment_id: experiment.id.to_string(),
                        metric_id: metric.id.to_string(),
                        value,
                    });
                }
            }
        }
        Ok(values)
    }
}

fn execute_frontier_query(
    connection: &Connection,
    request: FrontierSqlQuery,
) -> Result<FrontierSqlQueryResult, StoreError> {
    if request.sql.len() > MAX_SQL_BYTES as usize {
        return Err(StoreError::InvalidInput(format!(
            "frontier query SQL is too large: {} bytes exceeds {}",
            request.sql.len(),
            MAX_SQL_BYTES
        )));
    }
    let params = request
        .params
        .into_iter()
        .map(json_to_sql_value)
        .collect::<Result<Vec<_>, _>>()?;
    let max_rows = requested_max_rows(request.max_rows);
    let mut statement = connection.prepare(&request.sql).map_err(query_sql_error)?;
    if !statement.readonly() {
        return Err(StoreError::PolicyViolation(
            "frontier.query.sql accepts exactly one read-only SELECT statement against q_* views"
                .to_owned(),
        ));
    }
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut byte_budget = columns.iter().map(String::len).sum::<usize>();
    let mut rows = statement
        .query(params_from_iter(params.iter()))
        .map_err(query_sql_error)?;
    let mut output_rows = Vec::new();
    let mut truncated = false;
    while let Some(row) = rows.next().map_err(query_sql_error)? {
        if output_rows.len() >= max_rows {
            truncated = true;
            break;
        }
        let mut output_row = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            output_row.push(sql_value_to_json(
                row.get_ref(index).map_err(query_sql_error)?,
            ));
        }
        byte_budget += output_row.iter().map(porcelain_cell_len).sum::<usize>();
        if byte_budget > MAX_RESULT_BYTES {
            truncated = true;
            break;
        }
        output_rows.push(output_row);
    }
    Ok(FrontierSqlQueryResult {
        row_count: output_rows.len(),
        columns,
        rows: output_rows,
        truncated,
        max_rows,
    })
}

fn tighten_query_limits(connection: &Connection) -> Result<(), StoreError> {
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, 64 * 1_024)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, MAX_SQL_BYTES)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_COLUMN, 64)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_EXPR_DEPTH, 128)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_COMPOUND_SELECT, 16)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_FUNCTION_ARG, 32)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 64)?;
    let _ = connection.set_limit(Limit::SQLITE_LIMIT_WORKER_THREADS, 0)?;
    Ok(())
}

fn install_progress_deadline(connection: &Connection, timeout_ms: Option<u64>) {
    let timeout_ms = timeout_ms
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1, HARD_TIMEOUT_MS);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    connection.progress_handler(20_000, Some(move || Instant::now() >= deadline));
}

fn requested_max_rows(max_rows: Option<u32>) -> usize {
    max_rows
        .map(|limit| limit as usize)
        .unwrap_or(DEFAULT_MAX_ROWS)
        .clamp(1, HARD_MAX_ROWS)
}

fn json_to_sql_value(value: Value) -> Result<SqlValue, StoreError> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(value) => Ok(SqlValue::Integer(i64::from(value))),
        Value::Number(number) => number.as_i64().map_or_else(
            || {
                number.as_f64().map(SqlValue::Real).ok_or_else(|| {
                    StoreError::InvalidInput("invalid numeric SQL parameter".to_owned())
                })
            },
            |value| Ok(SqlValue::Integer(value)),
        ),
        Value::String(value) => Ok(SqlValue::Text(value)),
        Value::Array(_) | Value::Object(_) => Err(StoreError::InvalidInput(
            "frontier query parameters must be SQL scalar values".to_owned(),
        )),
    }
}

fn sql_value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Number(Number::from(value)),
        ValueRef::Real(value) => Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::String(format!("<blob:{} bytes>", value.len())),
    }
}

fn porcelain_cell_len(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(value) => value.to_string().len(),
        Value::Number(value) => value.to_string().len(),
        Value::String(value) => value.len(),
        Value::Array(_) | Value::Object(_) => 2,
    }
}

fn query_sql_error(error: rusqlite::Error) -> StoreError {
    match error {
        rusqlite::Error::MultipleStatement => StoreError::PolicyViolation(
            "frontier.query.sql accepts exactly one read-only statement; multiple statements are rejected"
                .to_owned(),
        ),
        rusqlite::Error::ExecuteReturnedResults => StoreError::InvalidInput(
            "frontier.query.sql expected a SELECT-style statement that returns rows".to_owned(),
        ),
        rusqlite::Error::SqliteFailure(_, message)
            if message
                .as_deref()
                .is_some_and(is_authorization_error) =>
        {
            StoreError::PolicyViolation(
                "frontier.query.sql is read-only and frontier-scoped; use only q_* views and deterministic SQL functions".to_owned(),
            )
        }
        rusqlite::Error::SqlInputError { msg, .. } if is_authorization_error(&msg) => {
            StoreError::PolicyViolation(
                "frontier.query.sql is read-only and frontier-scoped; use only q_* views and deterministic SQL functions".to_owned(),
            )
        }
        rusqlite::Error::SqliteFailure(_, message)
            if message
                .as_deref()
                .is_some_and(|message| message.contains("interrupted")) =>
        {
            StoreError::PolicyViolation(
                "frontier.query.sql exceeded the query time budget".to_owned(),
            )
        }
        other => StoreError::InvalidInput(format!("invalid frontier query SQL: {other}")),
    }
}

fn is_authorization_error(message: &str) -> bool {
    message.contains("not authorized")
        || message.contains("readonly")
        || message.contains("read-only")
        || message.contains("prohibited")
        || message.contains("authorization")
}

fn authorize_frontier_query(context: AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Select => Authorization::Allow,
        AuthAction::Read { table_name, .. }
            if public_query_view(table_name) || context.accessor.is_some_and(public_query_view) =>
        {
            Authorization::Allow
        }
        AuthAction::Function { function_name } if read_function_allowed(function_name) => {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }
}

fn public_query_view(name: &str) -> bool {
    QUERY_VIEWS.iter().any(|view| view.name == name)
}

fn read_function_allowed(function_name: &str) -> bool {
    let function_name = function_name.to_ascii_lowercase();
    if function_name.starts_with("pragma_") {
        return false;
    }
    !matches!(
        function_name.as_str(),
        "changes"
            | "current_date"
            | "current_time"
            | "current_timestamp"
            | "date"
            | "datetime"
            | "julianday"
            | "last_insert_rowid"
            | "load_extension"
            | "random"
            | "randomblob"
            | "readfile"
            | "strftime"
            | "time"
            | "unixepoch"
            | "writefile"
    )
}

fn install_frontier_query_views(
    connection: &Connection,
    frontier_id: &str,
    synthetic_values: &[SyntheticMetricQueryValue],
) -> Result<(), StoreError> {
    connection.execute_batch(
        "
        CREATE TEMP TABLE __spinner_query_scope (
            frontier_id TEXT PRIMARY KEY NOT NULL
        );

        CREATE TEMP TABLE __spinner_synthetic_metric_values (
            experiment_id TEXT NOT NULL,
            metric_id TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (experiment_id, metric_id)
        );
        ",
    )?;
    let _ = connection.execute(
        "INSERT INTO temp.__spinner_query_scope (frontier_id) VALUES (?1)",
        params![frontier_id],
    )?;
    for value in synthetic_values {
        let _ = connection.execute(
            "INSERT INTO temp.__spinner_synthetic_metric_values (experiment_id, metric_id, value)
             VALUES (?1, ?2, ?3)",
            params![value.experiment_id, value.metric_id, value.value],
        )?;
    }
    connection.execute_batch(CREATE_QUERY_VIEWS_SQL)?;
    Ok(())
}

const CREATE_QUERY_VIEWS_SQL: &str = concat!(
    "
    CREATE TEMP VIEW q_hypothesis AS
    SELECT
        hypotheses.slug AS hypothesis_slug,
        hypotheses.title AS title,
        hypotheses.summary AS summary,
        hypotheses.body AS body,
        hypotheses.revision AS revision,
        hypotheses.created_at AS created_at,
        hypotheses.updated_at AS updated_at
    FROM hypotheses
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_experiment AS
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        experiments.title AS title,
        experiments.summary AS summary,
        CASE
            WHEN experiment_outcomes.experiment_id IS NULL THEN 'open'
            ELSE 'closed'
        END AS status,
        experiment_outcomes.verdict AS verdict,
        primary_metric_definitions.key AS primary_metric_key,
        primary_metrics.value AS primary_canonical_value,
        ",
    "
        CASE primary_metric_definitions.display_unit
            WHEN 'nanoseconds' THEN primary_metrics.value
            WHEN 'microseconds' THEN primary_metrics.value / 1000.0
            WHEN 'milliseconds' THEN primary_metrics.value / 1000000.0
            WHEN 'seconds' THEN primary_metrics.value / 1000000000.0
            WHEN 'bytes' THEN primary_metrics.value
            WHEN 'kibibytes' THEN primary_metrics.value / 1024.0
            WHEN 'mebibytes' THEN primary_metrics.value / 1048576.0
            WHEN 'gibibytes' THEN primary_metrics.value / 1073741824.0
            WHEN 'percent' THEN primary_metrics.value * 100.0
            ELSE primary_metrics.value
        END AS primary_display_value,
        primary_metric_definitions.display_unit AS primary_display_unit,
        experiment_outcomes.rationale AS rationale,
        experiment_outcomes.analysis_summary AS analysis_summary,
        experiment_outcomes.analysis_body AS analysis_body,
        experiment_outcomes.backend AS backend,
        experiment_outcomes.working_directory AS working_directory,
        experiment_outcomes.commit_hash AS commit_hash,
        experiment_outcomes.closed_at AS closed_at,
        experiments.revision AS revision,
        experiments.created_at AS created_at,
        experiments.updated_at AS updated_at
    FROM experiments
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    LEFT JOIN experiment_outcomes
        ON experiment_outcomes.experiment_id = experiments.id
    LEFT JOIN experiment_metrics AS primary_metrics
        ON primary_metrics.experiment_id = experiments.id
       AND primary_metrics.is_primary = 1
    LEFT JOIN metric_definitions AS primary_metric_definitions
        ON primary_metric_definitions.id = primary_metrics.metric_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_experiment_command_arg AS
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        experiment_command_argv.ordinal AS ordinal,
        experiment_command_argv.arg AS arg
    FROM experiment_command_argv
    JOIN experiments ON experiments.id = experiment_command_argv.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_experiment_command_env AS
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        experiment_command_env.key AS key,
        experiment_command_env.value AS value
    FROM experiment_command_env
    JOIN experiments ON experiments.id = experiment_command_env.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_metric AS
    SELECT
        metric_definitions.key AS metric_key,
        CASE
            WHEN synthetic_metric_definitions.metric_id IS NULL THEN 'observed'
            ELSE 'synthetic'
        END AS metric_kind,
        metric_definitions.dimension AS metric_dimension,
        ",
    "
        CASE metric_definitions.dimension
            WHEN 'time' THEN 'nanoseconds'
            WHEN 'bytes' THEN 'bytes'
            WHEN 'dimensionless' THEN 'dimensionless'
            WHEN 'count' THEN 'count'
            ELSE metric_definitions.dimension
        END",
    " AS canonical_unit,
        metric_definitions.display_unit AS display_unit,
        metric_definitions.aggregation AS aggregation,
        metric_definitions.objective AS objective,
        metric_definitions.description AS description,
        frontier_kpis.ordinal AS kpi_ordinal
    FROM metric_definitions
    LEFT JOIN synthetic_metric_definitions
        ON synthetic_metric_definitions.metric_id = metric_definitions.id
    LEFT JOIN frontier_kpis
        ON frontier_kpis.metric_id = metric_definitions.id
       AND frontier_kpis.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
    WHERE frontier_kpis.id IS NOT NULL
       OR EXISTS (
           SELECT 1
           FROM experiment_metrics
           JOIN experiments ON experiments.id = experiment_metrics.experiment_id
           JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
           WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
             AND experiment_metrics.metric_id = metric_definitions.id
       )
       OR EXISTS (
           SELECT 1
           FROM temp.__spinner_synthetic_metric_values synthetic_values
           JOIN experiments ON experiments.id = synthetic_values.experiment_id
           JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
           WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
             AND synthetic_values.metric_id = metric_definitions.id
       );

    CREATE TEMP VIEW q_kpi AS
    SELECT
        q_metric.kpi_ordinal AS kpi_ordinal,
        q_metric.metric_key AS metric_key,
        q_metric.metric_kind AS metric_kind,
        q_metric.metric_dimension AS metric_dimension,
        q_metric.canonical_unit AS canonical_unit,
        q_metric.display_unit AS display_unit,
        q_metric.aggregation AS aggregation,
        q_metric.objective AS objective,
        q_metric.description AS description
    FROM q_metric
    WHERE q_metric.kpi_ordinal IS NOT NULL;

    CREATE TEMP VIEW q_experiment_metric AS
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        metric_definitions.key AS metric_key,
        'observed' AS metric_kind,
        metric_definitions.dimension AS metric_dimension,
        ",
    "
        CASE metric_definitions.dimension
            WHEN 'time' THEN 'nanoseconds'
            WHEN 'bytes' THEN 'bytes'
            WHEN 'dimensionless' THEN 'dimensionless'
            WHEN 'count' THEN 'count'
            ELSE metric_definitions.dimension
        END",
    " AS canonical_unit,
        metric_definitions.display_unit AS display_unit,
        experiment_metrics.value AS canonical_value,
        ",
    "
        CASE metric_definitions.display_unit
            WHEN 'nanoseconds' THEN experiment_metrics.value
            WHEN 'microseconds' THEN experiment_metrics.value / 1000.0
            WHEN 'milliseconds' THEN experiment_metrics.value / 1000000.0
            WHEN 'seconds' THEN experiment_metrics.value / 1000000000.0
            WHEN 'bytes' THEN experiment_metrics.value
            WHEN 'kibibytes' THEN experiment_metrics.value / 1024.0
            WHEN 'mebibytes' THEN experiment_metrics.value / 1048576.0
            WHEN 'gibibytes' THEN experiment_metrics.value / 1073741824.0
            WHEN 'percent' THEN experiment_metrics.value * 100.0
            ELSE experiment_metrics.value
        END",
    " AS display_value,
        experiment_metrics.is_primary AS is_primary,
        experiment_metrics.ordinal AS metric_ordinal,
        experiment_outcomes.verdict AS verdict,
        experiment_outcomes.closed_at AS closed_at
    FROM experiment_metrics
    JOIN metric_definitions ON metric_definitions.id = experiment_metrics.metric_id
    JOIN experiments ON experiments.id = experiment_metrics.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    LEFT JOIN experiment_outcomes ON experiment_outcomes.experiment_id = experiments.id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
    UNION ALL
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        metric_definitions.key AS metric_key,
        'synthetic' AS metric_kind,
        metric_definitions.dimension AS metric_dimension,
        ",
    "
        CASE metric_definitions.dimension
            WHEN 'time' THEN 'nanoseconds'
            WHEN 'bytes' THEN 'bytes'
            WHEN 'dimensionless' THEN 'dimensionless'
            WHEN 'count' THEN 'count'
            ELSE metric_definitions.dimension
        END",
    " AS canonical_unit,
        metric_definitions.display_unit AS display_unit,
        synthetic_values.value AS canonical_value,
        ",
    "
        CASE metric_definitions.display_unit
            WHEN 'nanoseconds' THEN synthetic_values.value
            WHEN 'microseconds' THEN synthetic_values.value / 1000.0
            WHEN 'milliseconds' THEN synthetic_values.value / 1000000.0
            WHEN 'seconds' THEN synthetic_values.value / 1000000000.0
            WHEN 'bytes' THEN synthetic_values.value
            WHEN 'kibibytes' THEN synthetic_values.value / 1024.0
            WHEN 'mebibytes' THEN synthetic_values.value / 1048576.0
            WHEN 'gibibytes' THEN synthetic_values.value / 1073741824.0
            WHEN 'percent' THEN synthetic_values.value * 100.0
            ELSE synthetic_values.value
        END",
    " AS display_value,
        0 AS is_primary,
        NULL AS metric_ordinal,
        experiment_outcomes.verdict AS verdict,
        experiment_outcomes.closed_at AS closed_at
    FROM temp.__spinner_synthetic_metric_values synthetic_values
    JOIN metric_definitions ON metric_definitions.id = synthetic_values.metric_id
    JOIN experiments ON experiments.id = synthetic_values.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    LEFT JOIN experiment_outcomes ON experiment_outcomes.experiment_id = experiments.id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_synthetic_metric_dependency AS
    SELECT
        synthetic_metrics.key AS synthetic_metric_key,
        synthetic_metric_definitions.expression_json AS expression_json,
        dependency_metrics.key AS dependency_metric_key,
        synthetic_metric_dependencies.ordinal AS dependency_ordinal
    FROM synthetic_metric_definitions
    JOIN metric_definitions AS synthetic_metrics
        ON synthetic_metrics.id = synthetic_metric_definitions.metric_id
    LEFT JOIN synthetic_metric_dependencies
        ON synthetic_metric_dependencies.synthetic_metric_id = synthetic_metric_definitions.metric_id
    LEFT JOIN metric_definitions AS dependency_metrics
        ON dependency_metrics.id = synthetic_metric_dependencies.dependency_metric_id
    WHERE EXISTS (
        SELECT 1
        FROM q_metric
        WHERE q_metric.metric_key = synthetic_metrics.key
    );

    CREATE TEMP VIEW q_experiment_condition AS
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        experiment_dimension_strings.key AS condition_key,
        'string' AS value_type,
        experiment_dimension_strings.value AS value_text,
        NULL AS value_number,
        NULL AS value_boolean,
        NULL AS value_timestamp
    FROM experiment_dimension_strings
    JOIN experiments ON experiments.id = experiment_dimension_strings.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
    UNION ALL
    SELECT
        experiments.slug,
        hypotheses.slug,
        experiment_dimension_numbers.key,
        'numeric',
        NULL,
        experiment_dimension_numbers.value,
        NULL,
        NULL
    FROM experiment_dimension_numbers
    JOIN experiments ON experiments.id = experiment_dimension_numbers.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
    UNION ALL
    SELECT
        experiments.slug,
        hypotheses.slug,
        experiment_dimension_booleans.key,
        'boolean',
        NULL,
        NULL,
        experiment_dimension_booleans.value,
        NULL
    FROM experiment_dimension_booleans
    JOIN experiments ON experiments.id = experiment_dimension_booleans.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope)
    UNION ALL
    SELECT
        experiments.slug,
        hypotheses.slug,
        experiment_dimension_timestamps.key,
        'timestamp',
        NULL,
        NULL,
        NULL,
        experiment_dimension_timestamps.value
    FROM experiment_dimension_timestamps
    JOIN experiments ON experiments.id = experiment_dimension_timestamps.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_condition AS
    SELECT DISTINCT
        run_dimension_definitions.key AS condition_key,
        run_dimension_definitions.value_type AS value_type,
        run_dimension_definitions.description AS description
    FROM run_dimension_definitions
    JOIN q_experiment_condition
        ON q_experiment_condition.condition_key = run_dimension_definitions.key;

    CREATE TEMP VIEW q_influence_edge AS
    SELECT
        influence_edges.parent_kind AS parent_kind,
        CASE influence_edges.parent_kind
            WHEN 'hypothesis' THEN parent_hypotheses.slug
            WHEN 'experiment' THEN parent_experiments.slug
        END AS parent_slug,
        influence_edges.child_kind AS child_kind,
        CASE influence_edges.child_kind
            WHEN 'hypothesis' THEN child_hypotheses.slug
            WHEN 'experiment' THEN child_experiments.slug
        END AS child_slug,
        influence_edges.ordinal AS ordinal
    FROM influence_edges
    LEFT JOIN hypotheses AS parent_hypotheses
        ON influence_edges.parent_kind = 'hypothesis'
       AND parent_hypotheses.id = influence_edges.parent_id
    LEFT JOIN experiments AS parent_experiments
        ON influence_edges.parent_kind = 'experiment'
       AND parent_experiments.id = influence_edges.parent_id
    LEFT JOIN hypotheses AS parent_experiment_hypotheses
        ON parent_experiment_hypotheses.id = parent_experiments.hypothesis_id
    LEFT JOIN hypotheses AS child_hypotheses
        ON influence_edges.child_kind = 'hypothesis'
       AND child_hypotheses.id = influence_edges.child_id
    LEFT JOIN experiments AS child_experiments
        ON influence_edges.child_kind = 'experiment'
       AND child_experiments.id = influence_edges.child_id
    LEFT JOIN hypotheses AS child_experiment_hypotheses
        ON child_experiment_hypotheses.id = child_experiments.hypothesis_id
    WHERE COALESCE(child_hypotheses.frontier_id, child_experiment_hypotheses.frontier_id)
        = (SELECT frontier_id FROM temp.__spinner_query_scope)
      AND COALESCE(parent_hypotheses.frontier_id, parent_experiment_hypotheses.frontier_id)
        = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_hypothesis_tag AS
    SELECT
        hypotheses.slug AS hypothesis_slug,
        tags.name AS tag,
        tag_families.name AS tag_family
    FROM hypothesis_tags
    JOIN tags ON tags.id = hypothesis_tags.tag_id
    LEFT JOIN tag_families ON tag_families.id = tags.family_id
    JOIN hypotheses ON hypotheses.id = hypothesis_tags.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_experiment_tag AS
    SELECT
        experiments.slug AS experiment_slug,
        hypotheses.slug AS hypothesis_slug,
        tags.name AS tag,
        tag_families.name AS tag_family
    FROM experiment_tags
    JOIN tags ON tags.id = experiment_tags.tag_id
    LEFT JOIN tag_families ON tag_families.id = tags.family_id
    JOIN experiments ON experiments.id = experiment_tags.experiment_id
    JOIN hypotheses ON hypotheses.id = experiments.hypothesis_id
    WHERE hypotheses.frontier_id = (SELECT frontier_id FROM temp.__spinner_query_scope);

    CREATE TEMP VIEW q_tag AS
    SELECT DISTINCT
        tags.name AS tag,
        tags.description AS description,
        tag_families.name AS tag_family
    FROM tags
    LEFT JOIN tag_families ON tag_families.id = tags.family_id
    WHERE EXISTS (
            SELECT 1
            FROM q_hypothesis_tag
            WHERE q_hypothesis_tag.tag = tags.name
        )
       OR EXISTS (
            SELECT 1
            FROM q_experiment_tag
            WHERE q_experiment_tag.tag = tags.name
        );
    "
);

const QUERY_VIEWS: &[FrontierSqlView] = &[
    FrontierSqlView {
        name: "q_hypothesis",
        description: "Hypotheses owned by the bound frontier.",
        columns: &[
            col("hypothesis_slug", "text", "Stable hypothesis slug."),
            col("title", "text", "Human title."),
            col("summary", "text", "One-line summary."),
            col("body", "text", "Single-paragraph hypothesis body."),
            col("revision", "integer", "Optimistic concurrency revision."),
            col("created_at", "text", "RFC3339 creation timestamp."),
            col("updated_at", "text", "RFC3339 update timestamp."),
        ],
    },
    FrontierSqlView {
        name: "q_experiment",
        description: "Experiments owned by the bound frontier, including outcome prose when closed.",
        columns: &[
            col("experiment_slug", "text", "Stable experiment slug."),
            col("hypothesis_slug", "text", "Owning hypothesis slug."),
            col("title", "text", "Human title."),
            col("summary", "text", "Optional experiment summary."),
            col("status", "text", "open or closed."),
            col(
                "verdict",
                "text",
                "accepted, kept, parked, rejected, or null.",
            ),
            col(
                "primary_metric_key",
                "text",
                "Primary metric key when closed.",
            ),
            col(
                "primary_canonical_value",
                "real",
                "Primary metric value in canonical metric units.",
            ),
            col(
                "primary_display_value",
                "real",
                "Primary metric value converted to its display unit.",
            ),
            col(
                "primary_display_unit",
                "text",
                "Primary metric display unit.",
            ),
            col("rationale", "text", "Decision rationale."),
            col("analysis_summary", "text", "Optional analysis summary."),
            col("analysis_body", "text", "Optional analysis body."),
            col("backend", "text", "Execution backend."),
            col("working_directory", "text", "Command working directory."),
            col("commit_hash", "text", "Captured git commit hash."),
            col("closed_at", "text", "RFC3339 close timestamp."),
            col("revision", "integer", "Optimistic concurrency revision."),
            col("created_at", "text", "RFC3339 creation timestamp."),
            col("updated_at", "text", "RFC3339 update timestamp."),
        ],
    },
    FrontierSqlView {
        name: "q_experiment_command_arg",
        description: "Command argv entries recorded for frontier experiments.",
        columns: &[
            col("experiment_slug", "text", "Stable experiment slug."),
            col("hypothesis_slug", "text", "Owning hypothesis slug."),
            col("ordinal", "integer", "Argument order."),
            col("arg", "text", "Command argument."),
        ],
    },
    FrontierSqlView {
        name: "q_experiment_command_env",
        description: "Command environment entries recorded for frontier experiments.",
        columns: &[
            col("experiment_slug", "text", "Stable experiment slug."),
            col("hypothesis_slug", "text", "Owning hypothesis slug."),
            col("key", "text", "Environment variable name."),
            col("value", "text", "Environment variable value."),
        ],
    },
    FrontierSqlView {
        name: "q_metric",
        description: "Metrics visible through frontier KPIs or frontier experiment observations.",
        columns: METRIC_COLUMNS,
    },
    FrontierSqlView {
        name: "q_kpi",
        description: "Frontier KPI metrics in supervisor-defined order.",
        columns: KPI_COLUMNS,
    },
    FrontierSqlView {
        name: "q_experiment_metric",
        description: "Every observed metric recorded by frontier experiments.",
        columns: &[
            col("experiment_slug", "text", "Stable experiment slug."),
            col("hypothesis_slug", "text", "Owning hypothesis slug."),
            col("metric_key", "text", "Metric key."),
            col("metric_kind", "text", "observed."),
            col("metric_dimension", "text", "Scientific metric dimension."),
            col("canonical_unit", "text", "Canonical backing unit."),
            col("display_unit", "text", "Default display unit."),
            col(
                "canonical_value",
                "real",
                "Metric value in canonical units.",
            ),
            col(
                "display_value",
                "real",
                "Metric value converted to display_unit.",
            ),
            col(
                "is_primary",
                "integer",
                "1 when this is the primary experiment metric.",
            ),
            col(
                "metric_ordinal",
                "integer",
                "Metric ordering within the outcome.",
            ),
            col("verdict", "text", "Experiment verdict when closed."),
            col("closed_at", "text", "RFC3339 close timestamp."),
        ],
    },
    FrontierSqlView {
        name: "q_synthetic_metric_dependency",
        description: "Synthetic metric formulas visible in this frontier and their direct dependencies.",
        columns: &[
            col("synthetic_metric_key", "text", "Synthetic metric key."),
            col(
                "expression_json",
                "text",
                "Typed synthetic expression JSON.",
            ),
            col(
                "dependency_metric_key",
                "text",
                "Direct dependency metric key.",
            ),
            col(
                "dependency_ordinal",
                "integer",
                "Dependency order in the formula.",
            ),
        ],
    },
    FrontierSqlView {
        name: "q_experiment_condition",
        description: "Typed experimental condition values attached to frontier experiments.",
        columns: &[
            col("experiment_slug", "text", "Stable experiment slug."),
            col("hypothesis_slug", "text", "Owning hypothesis slug."),
            col("condition_key", "text", "Condition key."),
            col(
                "value_type",
                "text",
                "string, numeric, boolean, or timestamp.",
            ),
            col(
                "value_text",
                "text",
                "String value when value_type is string.",
            ),
            col(
                "value_number",
                "real",
                "Numeric value when value_type is numeric.",
            ),
            col(
                "value_boolean",
                "integer",
                "0 or 1 when value_type is boolean.",
            ),
            col(
                "value_timestamp",
                "text",
                "RFC3339 value when value_type is timestamp.",
            ),
        ],
    },
    FrontierSqlView {
        name: "q_condition",
        description: "Condition definitions used by frontier experiments.",
        columns: &[
            col("condition_key", "text", "Condition key."),
            col("value_type", "text", "Condition value type."),
            col("description", "text", "Optional condition description."),
        ],
    },
    FrontierSqlView {
        name: "q_influence_edge",
        description: "Influence edges whose parent and child both live in the bound frontier.",
        columns: &[
            col("parent_kind", "text", "hypothesis or experiment."),
            col("parent_slug", "text", "Parent vertex slug."),
            col("child_kind", "text", "hypothesis or experiment."),
            col("child_slug", "text", "Child vertex slug."),
            col("ordinal", "integer", "Influence order."),
        ],
    },
    FrontierSqlView {
        name: "q_hypothesis_tag",
        description: "Tags attached to frontier hypotheses.",
        columns: &[
            col("hypothesis_slug", "text", "Tagged hypothesis slug."),
            col("tag", "text", "Tag name."),
            col("tag_family", "text", "Optional tag family name."),
        ],
    },
    FrontierSqlView {
        name: "q_experiment_tag",
        description: "Tags attached to frontier experiments.",
        columns: &[
            col("experiment_slug", "text", "Tagged experiment slug."),
            col("hypothesis_slug", "text", "Owning hypothesis slug."),
            col("tag", "text", "Tag name."),
            col("tag_family", "text", "Optional tag family name."),
        ],
    },
    FrontierSqlView {
        name: "q_tag",
        description: "Tags used by frontier hypotheses or experiments.",
        columns: &[
            col("tag", "text", "Tag name."),
            col("description", "text", "Tag description."),
            col("tag_family", "text", "Optional tag family name."),
        ],
    },
];

const METRIC_COLUMNS: &[FrontierSqlColumn] = &[
    col("metric_key", "text", "Metric key."),
    col("metric_kind", "text", "observed or synthetic."),
    col("metric_dimension", "text", "Scientific metric dimension."),
    col("canonical_unit", "text", "Canonical backing unit."),
    col("display_unit", "text", "Default display unit."),
    col("aggregation", "text", "Observation aggregation semantics."),
    col("objective", "text", "Optimization objective."),
    col("description", "text", "Optional metric description."),
    col(
        "kpi_ordinal",
        "integer",
        "KPI order or null for non-KPI metrics.",
    ),
];

const KPI_COLUMNS: &[FrontierSqlColumn] = &[
    col("kpi_ordinal", "integer", "Supervisor-defined KPI order."),
    col("metric_key", "text", "Metric key."),
    col("metric_kind", "text", "observed or synthetic."),
    col("metric_dimension", "text", "Scientific metric dimension."),
    col("canonical_unit", "text", "Canonical backing unit."),
    col("display_unit", "text", "Default display unit."),
    col("aggregation", "text", "Observation aggregation semantics."),
    col("objective", "text", "Optimization objective."),
    col("description", "text", "Optional metric description."),
];

const fn col(
    name: &'static str,
    sql_type: &'static str,
    description: &'static str,
) -> FrontierSqlColumn {
    FrontierSqlColumn {
        name,
        sql_type,
        description,
    }
}
