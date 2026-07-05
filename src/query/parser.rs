use sqlparser::{
    ast::{Expr as SqlExpr, Query, SetExpr, Statement, Value as SqlValue},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use super::{AnalyzeMode, AnalyzeTarget, QueryError};
use crate::performance::QueryExecutionConfig;

/// Parses SQL text with the `PostgreSQL` dialect.
/// # Errors
/// Fails when the SQL text cannot be parsed.
pub fn parse(sql: &str) -> Result<Vec<Statement>, QueryError> {
    parse_with_limits(sql, &QueryExecutionConfig::default())
}

pub(super) fn parse_with_limits(
    sql: &str,
    limits: &QueryExecutionConfig,
) -> Result<Vec<Statement>, QueryError> {
    if sql.len() > limits.max_sql_bytes {
        return Err(QueryError::InputLimit(format!(
            "SQL text is {} bytes, limit is {} bytes",
            sql.len(),
            limits.max_sql_bytes
        )));
    }

    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(limits.parser_recursion_limit)
        .try_with_sql(sql)
        .map_err(|error| QueryError::Parse(error.to_string()))?;
    let statements = parser
        .parse_statements()
        .map_err(|error| QueryError::Parse(error.to_string()))?;

    if statements.len() > limits.max_statements {
        return Err(QueryError::InputLimit(format!(
            "SQL contains {} statements, limit is {}",
            statements.len(),
            limits.max_statements
        )));
    }

    let values_rows = statements
        .iter()
        .map(count_statement_values_rows)
        .sum::<usize>();
    if values_rows > limits.max_values_rows {
        return Err(QueryError::InputLimit(format!(
            "VALUES contains {values_rows} rows, limit is {}",
            limits.max_values_rows
        )));
    }

    Ok(statements)
}

fn count_statement_values_rows(statement: &Statement) -> usize {
    match statement {
        Statement::Query(query) => count_query_values_rows(query),
        Statement::Insert(insert) => insert.source.as_deref().map_or(0, count_query_values_rows),
        Statement::Explain { statement, .. } => count_statement_values_rows(statement),
        _ => 0,
    }
}

fn count_query_values_rows(query: &Query) -> usize {
    let with_rows = query.with.as_ref().map_or(0, |with| {
        with.cte_tables
            .iter()
            .map(|cte| count_query_values_rows(&cte.query))
            .sum()
    });
    with_rows + count_set_expr_values_rows(query.body.as_ref())
}

fn count_set_expr_values_rows(expr: &SetExpr) -> usize {
    match expr {
        SetExpr::Query(query) => count_query_values_rows(query),
        SetExpr::SetOperation { left, right, .. } => {
            count_set_expr_values_rows(left) + count_set_expr_values_rows(right)
        }
        SetExpr::Values(values) => values.rows.len(),
        SetExpr::Insert(statement)
        | SetExpr::Update(statement)
        | SetExpr::Delete(statement)
        | SetExpr::Merge(statement) => count_statement_values_rows(statement),
        SetExpr::Select(_) | SetExpr::Table(_) => 0,
    }
}

/// Parses the lightweight `ANALYZE FULL` extension used before general SQL parsing.
/// # Errors
/// Fails when the command starts with `ANALYZE` but has unsupported extra tokens.
pub fn parse_analyze_for_authz(
    sql: &str,
) -> Result<Option<(AnalyzeTarget, AnalyzeMode)>, QueryError> {
    parse_analyze_command(sql)
}

pub(super) fn parse_analyze_command(
    sql: &str,
) -> Result<Option<(AnalyzeTarget, AnalyzeMode)>, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    let Some(first) = parts.next() else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("analyze") {
        return Ok(None);
    }

    let mut mode = AnalyzeMode::default();
    let mut target = AnalyzeTarget::All;
    let mut next = parts.next();
    if next.is_some_and(|part| part.eq_ignore_ascii_case("full")) {
        mode = AnalyzeMode::Full;
        next = parts.next();
    }
    if next.is_some_and(|part| part.eq_ignore_ascii_case("table")) {
        next = parts.next();
    }
    if let Some(name) = next {
        target = AnalyzeTarget::Named(name.trim_matches('"').to_owned());
    }
    if parts.next().is_some() {
        return Err(QueryError::Unsupported(trimmed.to_owned()));
    }

    Ok(Some((target, mode)))
}

pub(super) fn parse_limit(query: &Query) -> Result<Option<usize>, QueryError> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(None);
    };

    match limit_clause {
        sqlparser::ast::LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by,
        } if limit_by.is_empty() => literal_usize(limit).map(Some),
        sqlparser::ast::LimitClause::LimitOffset {
            limit: None,
            offset: None,
            limit_by,
        } if limit_by.is_empty() => Ok(None),
        _ => Err(QueryError::Unsupported(limit_clause.to_string())),
    }
}

fn literal_usize(expr: &SqlExpr) -> Result<usize, QueryError> {
    match expr {
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(raw, _) => raw
                .parse::<usize>()
                .map_err(|error| QueryError::InvalidValue(error.to_string())),
            other => Err(QueryError::Unsupported(other.to_string())),
        },
        other => Err(QueryError::Unsupported(other.to_string())),
    }
}
