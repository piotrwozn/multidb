use std::ops::ControlFlow;

use sqlparser::{
    ast::{
        Expr as SqlExpr, Query, SetExpr, Statement, Value as SqlValue, ValueWithSpan, Visit,
        VisitMut, Visitor, VisitorMut,
    },
    dialect::{GenericDialect, PostgreSqlDialect},
    parser::Parser,
    tokenizer::Tokenizer,
};

use super::{AnalyzeMode, AnalyzeTarget, QueryError};
use crate::performance::QueryExecutionConfig;

/// Parses SQL text with the `PostgreSQL` dialect.
/// # Errors
/// Fails when the SQL text cannot be parsed.
pub fn parse(sql: &str) -> Result<Vec<Statement>, QueryError> {
    parse_with_limits(sql, &QueryExecutionConfig::default())
}

/// Parses SQL text with the `PostgreSQL` dialect under the configured SQL text size, statement
/// count, `VALUES` row and parser recursion limits. The engine and the compatibility, RBAC and
/// `AS OF` retention pre-parses parse through it, so they agree on what SQL is accepted.
/// `DataFusion` parses the SELECTs it plans again with its own recursion limit of 50; the pgwire
/// extended-protocol Describe pre-parse and best-effort internal parses that ignore parse errors
/// keep sqlparser's defaults.
pub(crate) fn parse_with_limits(
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

/// Renders a parsed statement as SQL text that the engine's planners parse back
/// to the same statement.
///
/// The simple planners and `DataFusion` plan re-rendered text. sqlparser's
/// printer does not escape every quoted value or name: inside `'...'` and
/// `"..."` it leaves a quote that follows a backslash undoubled and passes an
/// existing doubled quote through, and it prints `N'...'` unescaped. Neither
/// the `PostgreSQL` nor the generic dialect treats a backslash as an escape
/// there, so such a value or name would end early when the text is parsed
/// again and the rest of it would run as SQL. String literals that contain a
/// quote are therefore rendered as `E'...'`, whose escaping both dialects
/// reverse exactly. The text is then checked: parsed again with the
/// `PostgreSQL` dialect under `limits` it must give back the same statement,
/// and the generic dialect `DataFusion` parses it with must split it into the
/// same tokens.
/// # Errors
/// Fails with `Unsupported` when no text passes that check, for example for a
/// quoted name that holds a backslash before a quote.
pub(crate) fn render_statement(
    mut statement: Statement,
    limits: &QueryExecutionConfig,
) -> Result<String, QueryError> {
    if let ControlFlow::Break(error) = VisitMut::visit(&mut statement, &mut QuoteSafeLiterals) {
        return Err(error);
    }
    let sql = statement.to_string();
    let reparsed = Tokenizer::new(&PostgreSqlDialect {}, &sql)
        .tokenize_with_location()
        .ok()
        .filter(|tokens| {
            Tokenizer::new(&GenericDialect {}, &sql)
                .tokenize_with_location()
                .is_ok_and(|generic| generic == *tokens)
        })
        .and_then(|tokens| {
            Parser::new(&PostgreSqlDialect {})
                .with_recursion_limit(limits.parser_recursion_limit)
                .with_tokens_with_locations(tokens)
                .parse_statements()
                .ok()
        });
    if reparsed.as_deref() != Some(std::slice::from_ref(&statement)) {
        return Err(QueryError::Unsupported(
            "the statement cannot be planned from unambiguous SQL text; \
             check quoted names and values for a backslash before a quote"
                .to_owned(),
        ));
    }
    Ok(sql)
}

/// Rewrites string literals that hold a quote as `E'...'` literals.
struct QuoteSafeLiterals;

impl VisitorMut for QuoteSafeLiterals {
    type Break = QueryError;

    fn post_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
        match &mut value.value {
            SqlValue::SingleQuotedString(text) | SqlValue::NationalStringLiteral(text)
                if text.contains('\'') =>
            {
                let text = std::mem::take(text);
                value.value = SqlValue::EscapedStringLiteral(text);
            }
            SqlValue::HexStringLiteral(text)
            | SqlValue::SingleQuotedByteStringLiteral(text)
            | SqlValue::DoubleQuotedByteStringLiteral(text)
                if text.contains(['\'', '"', '\\']) =>
            {
                return ControlFlow::Break(QueryError::Unsupported(
                    "hex or bit-string literal containing a quote or backslash".to_owned(),
                ));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn count_statement_values_rows(statement: &Statement) -> usize {
    struct Counter(usize);
    impl Visitor for Counter {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            self.0 = self
                .0
                .saturating_add(count_set_expr_values_rows(query.body.as_ref()));
            ControlFlow::Continue(())
        }
    }
    let mut counter = Counter(0);
    let _ = statement.visit(&mut counter);
    counter.0
}

fn count_set_expr_values_rows(expr: &SetExpr) -> usize {
    match expr {
        SetExpr::SetOperation { left, right, .. } => {
            count_set_expr_values_rows(left).saturating_add(count_set_expr_values_rows(right))
        }
        SetExpr::Values(values) => values.rows.len(),
        // The AST visitor finds nested queries/CTEs/subqueries independently.
        _ => 0,
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

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[test]
    fn values_row_limit_covers_nested_selects_and_all_statements() {
        let limits = QueryExecutionConfig {
            max_values_rows: 1,
            ..QueryExecutionConfig::default()
        };
        for sql in [
            "SELECT * FROM (VALUES (1), (2)) AS v(x)",
            "SELECT (SELECT x FROM (VALUES (1), (2)) AS v(x) LIMIT 1)",
            "WITH a AS (VALUES (1)) SELECT * FROM a, (VALUES (2)) AS b(x)",
            "VALUES (1); SELECT * FROM (VALUES (2)) AS v(x)",
            "EXPLAIN SELECT * FROM (VALUES (1), (2)) AS v(x)",
        ] {
            let result = parse_with_limits(sql, &limits);
            assert!(
                matches!(&result, Err(QueryError::InputLimit(_))),
                "{sql}: {result:?}"
            );
        }
        assert!(parse_with_limits("WITH a AS (VALUES (1)) SELECT * FROM a", &limits).is_ok());
    }
}
