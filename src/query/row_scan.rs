//! Ordered primary-key reads and projected Arrow batches for row-layout tables.
//! Fast SQL plans accept only a complete, literal integer-PK grammar. General SQL keeps
//! `DataFusion` residual filters; policy providers remain a materialization barrier.

use std::time::{Duration, Instant};

use super::{
    ColumnType, DfExpr, QueryError, REL_ROWS_TABLE, RecordBatch, RelTable, Row, ScalarValue,
    SchemaRef, SqlEngine, SqlExpr, SqlOutput, SqlRows, Statement, TableLayout, TableSchema, Value,
    decode_row, make_row_key, parse_with_limits, project_row, project_schema, rows_to_record_batch,
};
use crate::{keyenc, repl::ReadConsistency};
use datafusion::{arrow::record_batch::RecordBatchOptions, logical_expr::Operator};
use sqlparser::ast::{
    BinaryOperator, LimitClause, ObjectNamePart, OrderByKind, SelectItem, SetExpr, TableFactor,
    UnaryOperator, Value as SqlValue,
};

#[derive(Clone, Copy, Debug)]
struct Bounds {
    lower: i64,
    upper: i64,
    empty: bool,
    all_keys: bool,
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            lower: i64::MIN,
            upper: i64::MAX,
            empty: false,
            all_keys: true,
        }
    }
}

#[derive(Clone, Copy)]
enum Comparison {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Comparison {
    fn reversed(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
        }
    }
}

impl Bounds {
    fn constraint(comparison: Comparison, value: i64) -> Self {
        let mut bounds = Self {
            all_keys: false,
            ..Self::default()
        };
        match comparison {
            Comparison::Eq => {
                bounds.lower = value;
                bounds.upper = value;
            }
            Comparison::Ge => bounds.lower = value,
            Comparison::Le => bounds.upper = value,
            Comparison::Gt => match value.checked_add(1) {
                Some(lower) => bounds.lower = lower,
                None => bounds.empty = true,
            },
            Comparison::Lt => match value.checked_sub(1) {
                Some(upper) => bounds.upper = upper,
                None => bounds.empty = true,
            },
        }
        bounds
    }
    fn intersect(self, other: Self) -> Self {
        let lower = self.lower.max(other.lower);
        let upper = self.upper.min(other.upper);
        Self {
            lower,
            upper,
            empty: self.empty || other.empty || lower > upper,
            all_keys: self.all_keys && other.all_keys,
        }
    }
}

pub(super) struct RowRangePlan {
    pub(super) table: String,
    bounds: Bounds,
    projection: Vec<usize>,
    names: Vec<String>,
    descending: bool,
    offset: usize,
    limit: Option<usize>,
    planning: Duration,
}

fn primary_key(schema: &TableSchema) -> Option<&str> {
    let primary = schema.columns.get(schema.primary_key)?;
    (primary.ty == ColumnType::Int).then_some(primary.name.as_str())
}

fn integer(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(raw, _) => raw.parse().ok(),
            _ => None,
        },
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let SqlExpr::Value(value) = expr.as_ref() else {
                return None;
            };
            let SqlValue::Number(raw, _) = &value.value else {
                return None;
            };
            format!("-{raw}").parse().ok()
        }
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        }
        | SqlExpr::Nested(expr) => integer(expr),
        _ => None,
    }
}

fn ast_column(expr: &SqlExpr, primary: &str) -> bool {
    matches!(expr, SqlExpr::Identifier(ident) if ident.value == primary
        && (ident.quote_style.is_some() || ident.value == ident.value.to_ascii_lowercase()))
}

fn ast_bounds(expr: &SqlExpr, primary: &str) -> Option<Bounds> {
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return if let SqlExpr::Nested(expr) = expr {
            ast_bounds(expr, primary)
        } else {
            None
        };
    };
    if *op == BinaryOperator::And {
        return Some(ast_bounds(left, primary)?.intersect(ast_bounds(right, primary)?));
    }
    let comparison = match op {
        BinaryOperator::Eq => Comparison::Eq,
        BinaryOperator::Lt => Comparison::Lt,
        BinaryOperator::LtEq => Comparison::Le,
        BinaryOperator::Gt => Comparison::Gt,
        BinaryOperator::GtEq => Comparison::Ge,
        _ => return None,
    };
    if ast_column(left, primary) {
        Some(Bounds::constraint(comparison, integer(right)?))
    } else if ast_column(right, primary) {
        Some(Bounds::constraint(comparison.reversed(), integer(left)?))
    } else {
        None
    }
}

fn literal_usize(expr: &SqlExpr) -> Option<usize> {
    let SqlExpr::Value(value) = expr else {
        return None;
    };
    let SqlValue::Number(raw, _) = &value.value else {
        return None;
    };
    raw.parse().ok()
}

/// Exact grammar validation by round-tripping the accepted fields only. Any extra AST
/// clause (now or added by sqlparser later) makes the canonical rendering differ.
#[allow(clippy::too_many_lines)] // Keep the complete accepted grammar and its round-trip check together.
pub(super) fn plan(engine: &SqlEngine, sql: &str) -> Result<Option<RowRangePlan>, QueryError> {
    let started = Instant::now();
    let statements = parse_with_limits(sql, &engine.performance.query)?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let [from] = select.from.as_slice() else {
        return Ok(None);
    };
    let TableFactor::Table { name, .. } = &from.relation else {
        return Ok(None);
    };
    let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
        return Ok(None);
    };
    if !ident.value.is_ascii()
        || ident.value != ident.value.to_ascii_lowercase()
        || from.to_string() != name.to_string()
    {
        return Ok(None);
    }
    let Some(table) = engine.tables.get(&ident.value) else {
        return Ok(None);
    };
    if table.layout != TableLayout::Row {
        return Ok(None);
    }
    let Some(schema) = table.schema.as_ref() else {
        return Ok(None);
    };
    let Some(primary) = primary_key(schema) else {
        return Ok(None);
    };
    let Some(selection) = &select.selection else {
        return Ok(None);
    };
    let Some(bounds) = ast_bounds(selection, primary) else {
        return Ok(None);
    };
    // Plain columns and '*' only: ORDER BY can resolve output aliases, so aliases
    // and expression projections must keep DataFusion's name-resolution semantics.
    let mut projection = Vec::new();
    let mut names = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) if item.to_string() == "*" => {
                projection.extend(0..schema.columns.len());
                names.extend(schema.columns.iter().map(|column| column.name.clone()));
            }
            SelectItem::UnnamedExpr(SqlExpr::Identifier(column)) => {
                if column.quote_style.is_none() && column.value != column.value.to_ascii_lowercase()
                {
                    return Ok(None);
                }
                let Some(index) = schema.column_index(&column.value) else {
                    return Ok(None);
                };
                projection.push(index);
                names.push(column.value.clone());
            }
            _ => return Ok(None),
        }
    }
    let mut descending = false;
    if let Some(order) = &query.order_by {
        let OrderByKind::Expressions(expressions) = &order.kind else {
            return Ok(None);
        };
        let [order_expr] = expressions.as_slice() else {
            return Ok(None);
        };
        if !ast_column(&order_expr.expr, primary)
            || order.interpolate.is_some()
            || order_expr.with_fill.is_some()
            || order_expr.options.nulls_first.is_some()
        {
            return Ok(None);
        }
        descending = order_expr.options.asc == Some(false);
    }
    let (limit, offset) = match &query.limit_clause {
        None => (None, 0),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => {
            let limit = match limit {
                Some(expr) => match literal_usize(expr) {
                    Some(value) => Some(value),
                    None => return Ok(None),
                },
                None => None,
            };
            let offset = match offset {
                Some(offset) => match literal_usize(&offset.value) {
                    Some(value) => value,
                    None => return Ok(None),
                },
                None => 0,
            };
            (limit, offset)
        }
        _ => return Ok(None),
    };
    let projection_sql = select
        .projection
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let mut accepted = format!("SELECT {projection_sql} FROM {name} WHERE {selection}");
    if let Some(order) = &query.order_by {
        accepted.push(' ');
        accepted.push_str(&order.to_string());
    }
    if let Some(limit) = &query.limit_clause {
        accepted.push(' ');
        accepted.push_str(limit.to_string().trim_start());
    }
    if query.to_string() != accepted {
        return Ok(None);
    }
    Ok(Some(RowRangePlan {
        table: table.name.clone(),
        bounds,
        projection,
        names,
        descending,
        offset,
        limit,
        planning: started.elapsed(),
    }))
}

/// No data or statistics are read by this advisory explanation.
pub(super) fn explain_details(plan: &RowRangePlan) -> (u64, String) {
    let width = if plan.bounds.empty {
        0
    } else {
        u128::try_from(i128::from(plan.bounds.upper) - i128::from(plan.bounds.lower) + 1)
            .unwrap_or(u128::MAX)
    };
    let offset = u128::try_from(plan.offset).unwrap_or(u128::MAX);
    let limit = plan.limit.map_or(u128::MAX, |limit| {
        u128::try_from(limit).unwrap_or(u128::MAX)
    });
    let estimated = u64::try_from(width.saturating_sub(offset).min(limit)).unwrap_or(u64::MAX);
    (
        estimated,
        format!(
            "literal primary-key interval [{}, {}], {}, offset {}, limit {:?}; advisory interval-width estimate",
            plan.bounds.lower,
            plan.bounds.upper,
            if plan.descending {
                "descending"
            } else {
                "ascending"
            },
            plan.offset,
            plan.limit
        ),
    )
}

pub(super) fn execute(engine: &SqlEngine, plan: &RowRangePlan) -> Result<SqlOutput, QueryError> {
    let table = engine
        .tables
        .get(&plan.table)
        .ok_or_else(|| QueryError::MissingTable(plan.table.clone()))?;
    let timeout_ms = engine.performance.query.timeout_ms;
    let remaining = Duration::from_millis(timeout_ms).saturating_sub(plan.planning);
    let deadline = Instant::now().checked_add(remaining).ok_or_else(|| {
        QueryError::ResourceLimit("query timeout exceeds the monotonic clock range".to_owned())
    })?;
    let cancelled = || Instant::now() >= deadline;
    if cancelled() {
        return Err(timeout(timeout_ms));
    }
    if plan.limit == Some(0) {
        return Ok(SqlOutput::Rows(SqlRows {
            columns: plan.names.clone(),
            rows: Vec::new(),
        }));
    }
    let memory_limit = engine.performance.query.memory_limit_bytes;
    let mut retained = plan.names.iter().try_fold(0_usize, |bytes, name| {
        checked_memory(
            bytes,
            std::mem::size_of::<String>()
                .saturating_mul(4)
                .saturating_add(name.len()),
            memory_limit,
        )
    })?;
    let stop = if plan.descending {
        None
    } else {
        plan.limit.map(|limit| plan.offset.saturating_add(limit))
    };
    let mut rows = Vec::new();
    let mut skipped = 0_usize;
    visit_rows(
        table,
        plan.bounds,
        stop,
        1, // Check the output budget before the backend materializes another stored record.
        &cancelled,
        timeout_ms,
        &mut |row| {
            // Ascending OFFSET rows never become output and need not be retained.
            if !plan.descending && skipped < plan.offset {
                skipped += 1;
                return Ok(());
            }
            // Charge before cloning/projecting, including every duplicated output column.
            let row_bytes = plan.projection.iter().try_fold(
                4 * std::mem::size_of::<Row>(),
                |bytes, index| {
                    let value = row.get(*index).ok_or_else(|| {
                        QueryError::InvalidRow("projection column is missing".to_owned())
                    })?;
                    let fixed = 4 * std::mem::size_of::<Value>();
                    checked_memory(
                        bytes,
                        checked_memory(
                            fixed,
                            value_heap_bytes(value, memory_limit)?,
                            memory_limit,
                        )?,
                        memory_limit,
                    )
                },
            )?;
            retained = checked_memory(retained, row_bytes, memory_limit)?;
            rows.push(project_row(&row, &plan.projection));
            Ok(())
        },
    )?;
    if cancelled() {
        return Err(timeout(timeout_ms));
    }
    if plan.descending {
        rows.reverse();
    }
    let offset = if plan.descending { plan.offset } else { 0 };
    let rows = rows
        .into_iter()
        .skip(offset)
        .take(plan.limit.unwrap_or(usize::MAX))
        .collect();
    Ok(SqlOutput::Rows(SqlRows {
        columns: plan.names.clone(),
        rows,
    }))
}

fn timeout(timeout_ms: u64) -> QueryError {
    QueryError::Timeout(format!("query exceeded {timeout_ms} ms"))
}

fn checked_memory(current: usize, next: usize, limit: usize) -> Result<usize, QueryError> {
    current
        .checked_add(next)
        .filter(|bytes| *bytes <= limit)
        .ok_or_else(|| {
            QueryError::ResourceLimit(format!(
                "primary-key query retained output exceeds {limit} bytes"
            ))
        })
}

/// Conservative allocation accounting: Vec growth is charged at four times its live
/// elements, which covers minimum capacity and collection into the final output Vec.
pub(super) fn value_heap_bytes(value: &Value, limit: usize) -> Result<usize, QueryError> {
    match value {
        Value::Str(value) => Ok(value.len()),
        Value::Bytes(value) => Ok(value.len()),
        Value::Vector(value) => value
            .len()
            .checked_mul(4 * std::mem::size_of::<f32>())
            .filter(|bytes| *bytes <= limit)
            .ok_or_else(|| {
                QueryError::ResourceLimit("vector output exceeds memory limit".to_owned())
            }),
        Value::Array(values) => values.iter().try_fold(0, |bytes, value| {
            checked_memory(
                bytes,
                checked_memory(
                    4 * std::mem::size_of::<Value>(),
                    value_heap_bytes(value, limit)?,
                    limit,
                )?,
                limit,
            )
        }),
        Value::Object(values) => values.iter().try_fold(0, |bytes, (key, value)| {
            checked_memory(
                bytes,
                checked_memory(
                    4096_usize.saturating_add(key.len()),
                    value_heap_bytes(value, limit)?,
                    limit,
                )?,
                limit,
            )
        }),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::GeoPoint { .. } => {
            Ok(0)
        }
    }
}

fn df_integer(expr: &DfExpr) -> Option<i64> {
    match expr {
        DfExpr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        _ => None,
    }
}

fn df_bounds(expr: &DfExpr, primary: &str) -> Option<Bounds> {
    let DfExpr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        return match (
            df_bounds(&binary.left, primary),
            df_bounds(&binary.right, primary),
        ) {
            (Some(left), Some(right)) => Some(left.intersect(right)),
            (Some(bounds), None) | (None, Some(bounds)) => Some(bounds),
            _ => None,
        };
    }
    let comparison = match binary.op {
        Operator::Eq => Comparison::Eq,
        Operator::Lt => Comparison::Lt,
        Operator::LtEq => Comparison::Le,
        Operator::Gt => Comparison::Gt,
        Operator::GtEq => Comparison::Ge,
        _ => return None,
    };
    if matches!(binary.left.as_ref(), DfExpr::Column(column) if column.name == primary) {
        Some(Bounds::constraint(comparison, df_integer(&binary.right)?))
    } else if matches!(binary.right.as_ref(), DfExpr::Column(column) if column.name == primary) {
        Some(Bounds::constraint(
            comparison.reversed(),
            df_integer(&binary.left)?,
        ))
    } else {
        None
    }
}

pub(super) fn supports_filter(table: &RelTable, filter: &DfExpr) -> bool {
    table
        .schema
        .as_ref()
        .and_then(primary_key)
        .is_some_and(|primary| df_bounds(filter, primary).is_some())
}

/// Streams decoded/projected rows into Arrow batches; nonprojected payloads and keys are
/// dropped after each storage record rather than retained for the entire table.
pub(super) fn batches(
    table: &RelTable,
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    filters: &[DfExpr],
    limit: Option<usize>,
    batch_rows: usize,
    budget: Option<&super::budget::Budget>,
) -> Result<Vec<RecordBatch>, QueryError> {
    let primary = table.schema.as_ref().and_then(primary_key);
    let bounds = primary.map_or_else(Bounds::default, |primary| {
        filters
            .iter()
            .filter_map(|filter| df_bounds(filter, primary))
            .fold(Bounds::default(), Bounds::intersect)
    });
    let schema = projection.map_or_else(
        || schema.clone(),
        |projection| project_schema(schema, projection),
    );
    let mut result = Vec::new();
    let mut rows = Vec::new();
    let flush = |rows: &mut Vec<Row>, result: &mut Vec<RecordBatch>| -> Result<(), QueryError> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = if schema.fields().is_empty() {
            RecordBatch::try_new_with_options(
                schema.clone(),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )
            .map_err(|error| QueryError::DataFusion(error.to_string()))?
        } else {
            rows_to_record_batch(schema.clone(), rows)?
        };
        result.push(batch);
        rows.clear();
        Ok(())
    };
    // An inexact filter must run again outside the provider, so never apply a limit
    // before that residual. Strict fast SQL plans apply their own safe early limit.
    let limit = if filters.is_empty() { limit } else { None };
    visit_rows(
        table,
        bounds,
        limit,
        if budget.is_some() { 1 } else { batch_rows },
        &|| budget.is_some_and(super::budget::Budget::expired),
        budget.map_or(0, super::budget::Budget::timeout_ms),
        &mut |row| {
            if let Some(budget) = budget {
                budget.charge_row(&row, projection)?;
            }
            rows.push(match projection {
                Some(projection) => project_row(&row, projection),
                None => row,
            });
            if rows.len() >= batch_rows.max(1) {
                flush(&mut rows, &mut result)?;
            }
            Ok(())
        },
    )?;
    flush(&mut rows, &mut result)?;
    Ok(result)
}

fn visit_rows(
    table: &RelTable,
    bounds: Bounds,
    limit: Option<usize>,
    batch_rows: usize,
    cancelled: &dyn Fn() -> bool,
    timeout_ms: u64,
    visitor: &mut dyn FnMut(Row) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    if cancelled() {
        return Err(timeout(timeout_ms));
    }
    if bounds.empty || limit == Some(0) {
        return Ok(());
    }
    if !bounds.all_keys && bounds.lower == bounds.upper {
        if let Some(row) = table.get(&Value::Int(bounds.lower))? {
            if cancelled() {
                return Err(timeout(timeout_ms));
            }
            visitor(row)?;
        }
        return if cancelled() {
            Err(timeout(timeout_ms))
        } else {
            Ok(())
        };
    }
    let (start, end) = if bounds.all_keys {
        super::table_range_bounds(&table.name)
    } else {
        (
            make_row_key(&table.name, &Value::Int(bounds.lower))?,
            keyenc::range_end(&make_row_key(&table.name, &Value::Int(bounds.upper))?),
        )
    };
    let mut read = 0_usize;
    let mut error = None;
    table.repl.scan_range_batches(
        REL_ROWS_TABLE,
        &start,
        &end,
        ReadConsistency::Strong,
        if limit.is_some() {
            1
        } else {
            batch_rows.max(1)
        },
        cancelled,
        &mut |batch| {
            for (_, bytes) in batch {
                if cancelled() {
                    return Ok(false);
                }
                let result = decode_row(bytes)
                    .and_then(|row| {
                        row.ok_or_else(|| QueryError::InvalidRow("missing stored row".to_owned()))
                    })
                    .and_then(&mut *visitor);
                if let Err(cause) = result {
                    error = Some(cause);
                    return Ok(false);
                }
                read += 1;
                if limit.is_some_and(|limit| read >= limit) {
                    return Ok(false);
                }
            }
            Ok(true)
        },
    )?;
    match error {
        Some(error) => Err(error),
        None if cancelled() => Err(timeout(timeout_ms)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
