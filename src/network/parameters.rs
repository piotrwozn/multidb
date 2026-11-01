//! Bind only tokenizer-recognized parameter tokens, preserving all other SQL bytes.

use std::ops::ControlFlow;

use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::{
    ast::{
        Expr as SqlExpr, Function, FunctionArgumentClause, FunctionArguments, GroupByExpr, OrderBy,
        OrderByKind, Query, Select, Statement, UnaryOperator, Value as SqlValue, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::{Location, Token, Tokenizer},
};

use super::{db_error_to_pg, pg_error};
use crate::{
    db::DbError,
    performance::QueryExecutionConfig,
    query::{QueryError, parse_with_limits},
};

const MAX_EXPANDED_SQL_BYTES: usize = 16 * 1024 * 1024;

/// The highest parameter number. `Bind` and `ParameterDescription` carry the
/// parameter count as an Int16, so `PostgreSQL` allows at most 65535.
const MAX_PARAMETER_NUMBER: usize = 65_535;

/// The placeholders of one SQL text and the number of statements it holds.
struct Scan {
    /// Byte range and parameter number of each `$n` placeholder.
    placeholders: Vec<(usize, usize, usize)>,
    /// The `;`-separated token groups that are not empty.
    statements: usize,
}

fn scan(sql: &str) -> PgWireResult<Scan> {
    if sql.len() > MAX_EXPANDED_SQL_BYTES {
        return Err(pg_error(
            "ERROR",
            "54000",
            "SQL parameter expansion exceeds size limit",
        ));
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .map_err(|_| pg_error("ERROR", "42601", "invalid SQL parameter syntax"))?;
    let mut characters = sql.char_indices().peekable();
    let (mut line, mut column) = (1_u64, 1_u64);
    let mut placeholders = Vec::new();
    let (mut statements, mut open_statement) = (0_usize, false);
    for token in tokens {
        match token.token {
            Token::Whitespace(_) => continue,
            Token::SemiColon => {
                statements += usize::from(open_statement);
                open_statement = false;
                continue;
            }
            _ => open_statement = true,
        }
        let Token::Placeholder(raw) = token.token else {
            continue;
        };
        let digits = raw
            .strip_prefix('$')
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or_else(|| pg_error("ERROR", "22023", "invalid SQL parameter reference"))?;
        let parameter = digits
            .parse::<usize>()
            .ok()
            .filter(|index| (1..=MAX_PARAMETER_NUMBER).contains(index))
            .ok_or_else(|| {
                pg_error(
                    "ERROR",
                    "42P02",
                    format!(
                        "there is no parameter {raw}: parameters are $1 to ${MAX_PARAMETER_NUMBER}"
                    ),
                )
            })?;
        while (line, column) < (token.span.start.line, token.span.start.column) {
            let (_, character) = characters
                .next()
                .ok_or_else(|| pg_error("ERROR", "42601", "invalid SQL token position"))?;
            if character == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        let start = characters
            .peek()
            .map(|(index, _)| *index)
            .ok_or_else(|| pg_error("ERROR", "42601", "invalid SQL token position"))?;
        let end = start
            .checked_add(raw.len())
            .filter(|end| sql.get(start..*end) == Some(raw.as_str()))
            .ok_or_else(|| pg_error("ERROR", "42601", "invalid SQL token position"))?;
        placeholders.push((start, end, parameter));
    }
    Ok(Scan {
        placeholders,
        statements: statements + usize::from(open_statement),
    })
}

fn positions(sql: &str) -> PgWireResult<Vec<(usize, usize, usize)>> {
    Ok(scan(sql)?.placeholders)
}

pub(super) fn contains(sql: &str) -> PgWireResult<bool> {
    Ok(!positions(sql)?.is_empty())
}

pub(super) fn count(sql: &str) -> PgWireResult<usize> {
    Ok(positions(sql)?
        .into_iter()
        .map(|(_, _, index)| index)
        .max()
        .unwrap_or(0))
}

/// Checks the SQL text of a prepared statement before it is stored: it holds
/// at most one statement, as in `PostgreSQL`, and only `$1` to `$65535`.
pub(super) fn check_prepared(sql: &str) -> PgWireResult<()> {
    if scan(sql)?.statements > 1 {
        return Err(pg_error(
            "ERROR",
            "42601",
            "cannot insert multiple commands into a prepared statement",
        ));
    }
    Ok(())
}

pub(super) fn substitute(sql: &str, literals: &[String]) -> PgWireResult<String> {
    let positions = positions(sql)?;
    let mut output = String::with_capacity(sql.len());
    let mut previous = 0;
    for (start, end, parameter) in positions {
        let literal = literals
            .get(parameter - 1)
            .ok_or_else(|| PgWireError::ParameterIndexOutOfBound(parameter))?;
        // A literal written against a neighbouring token could merge with it,
        // as `1` after `1` or `'x'` after `U&` would; a space keeps it apart.
        let before = separator(sql[..start].chars().next_back());
        let after = separator(sql[end..].chars().next());
        let next_len = output
            .len()
            .checked_add(start - previous)
            .and_then(|len| len.checked_add(literal.len()))
            .and_then(|len| len.checked_add(before.len() + after.len()));
        if next_len.is_none_or(|len| len > MAX_EXPANDED_SQL_BYTES) {
            return Err(pg_error(
                "ERROR",
                "54000",
                "SQL parameter expansion exceeds size limit",
            ));
        }
        output.push_str(&sql[previous..start]);
        output.push_str(before);
        output.push_str(literal);
        output.push_str(after);
        previous = end;
    }
    if output.len().saturating_add(sql.len() - previous) > MAX_EXPANDED_SQL_BYTES {
        return Err(pg_error(
            "ERROR",
            "54000",
            "SQL parameter expansion exceeds size limit",
        ));
    }
    output.push_str(&sql[previous..]);
    Ok(output)
}

/// The text that separates a spliced literal from the character next to it.
fn separator(neighbour: Option<char>) -> &'static str {
    match neighbour {
        Some(character)
            if !character.is_whitespace()
                && !matches!(character, '(' | ')' | '[' | ']' | ',' | ';') =>
        {
            " "
        }
        _ => "",
    }
}

/// Rejects a prepared statement that uses a placeholder as a whole `ORDER BY`
/// or `GROUP BY` item. The engine reads an integer there as a column
/// position, so a bound `2` would sort or group by the second column; in
/// `PostgreSQL` it is a constant.
///
/// The statement is checked as Execute plans it, without an `AS OF` suffix,
/// whose point may be a placeholder too. Where sqlparser takes a quoted value
/// but not a placeholder, as for an alias, a placeholder is read as the
/// quoted value it is spliced as. A statement that parses neither way is
/// refused, as `PostgreSQL` refuses it at `Parse`, unless the database answers
/// it from its text without planning it, as it does `CALL`.
pub(super) fn reject_positional_placeholders(
    sql: &str,
    limits: &QueryExecutionConfig,
) -> PgWireResult<()> {
    let statement = match crate::db::as_of_statement(sql) {
        Some(statement) => statement,
        None if crate::db::answered_from_text(sql) => return Ok(()),
        None => sql,
    };
    let (statements, quoted) = match parse_with_limits(statement, limits) {
        Ok(statements) => (statements, Vec::new()),
        Err(error) => matches!(error, QueryError::Parse(_))
            .then(|| parse_with_quoted_placeholders(statement, limits))
            .flatten()
            .ok_or_else(|| db_error_to_pg(&DbError::from(error)))?,
    };
    let mut visitor = PositionalPlaceholder { quoted: &quoted };
    for statement in &statements {
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
    }
    Ok(())
}

/// The start and text of each placeholder read as a quoted value.
type QuotedPlaceholders = Vec<(Location, String)>;

/// Parses `sql` under the parser's recursion limit with each placeholder read
/// as a quoted value. Returns the statements and those placeholders.
fn parse_with_quoted_placeholders(
    sql: &str,
    limits: &QueryExecutionConfig,
) -> Option<(Vec<Statement>, QuotedPlaceholders)> {
    let mut quoted = Vec::new();
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .ok()?
        .into_iter()
        .map(|mut token| {
            if let Token::Placeholder(placeholder) = &mut token.token {
                quoted.push((token.span.start, std::mem::take(placeholder)));
                token.token = Token::SingleQuotedString(String::new());
            }
            token
        })
        .collect();
    let statements = Parser::new(&PostgreSqlDialect {})
        .with_recursion_limit(limits.parser_recursion_limit)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .ok()?;
    Some((statements, quoted))
}

struct PositionalPlaceholder<'a> {
    /// The start and text of each placeholder read as a quoted value.
    quoted: &'a [(Location, String)],
}

impl Visitor for PositionalPlaceholder<'_> {
    type Break = PgWireError;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        match &query.order_by {
            Some(OrderBy {
                kind: OrderByKind::Expressions(items),
                ..
            }) => self.reject_items(items.iter().map(|item| &item.expr), "ORDER BY"),
            _ => ControlFlow::Continue(()),
        }
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        match &select.group_by {
            GroupByExpr::Expressions(items, _) => self.reject_items(items, "GROUP BY"),
            GroupByExpr::All(_) => ControlFlow::Continue(()),
        }
    }

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
        // An aggregate's `ORDER BY` reads a number as a column position too.
        let SqlExpr::Function(Function {
            args, within_group, ..
        }) = expr
        else {
            return ControlFlow::Continue(());
        };
        let clauses = match args {
            FunctionArguments::List(list) => list.clauses.as_slice(),
            _ => &[],
        };
        let items = clauses
            .iter()
            .flat_map(|clause| match clause {
                FunctionArgumentClause::OrderBy(items) => items.as_slice(),
                _ => &[],
            })
            .chain(within_group);
        self.reject_items(items.map(|item| &item.expr), "ORDER BY")
    }
}

impl PositionalPlaceholder<'_> {
    fn reject_items<'e>(
        &self,
        items: impl IntoIterator<Item = &'e SqlExpr>,
        clause: &str,
    ) -> ControlFlow<PgWireError> {
        for mut expr in items {
            // Parentheses and a unary plus leave a numeric literal unchanged in the planner.
            while let SqlExpr::Nested(inner)
            | SqlExpr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: inner,
            } = expr
            {
                expr = inner;
            }
            let SqlExpr::Value(value) = expr else {
                continue;
            };
            let placeholder = match &value.value {
                SqlValue::Placeholder(placeholder) => Some(placeholder),
                _ => self
                    .quoted
                    .iter()
                    .find(|(start, _)| *start == value.span.start)
                    .map(|(_, placeholder)| placeholder),
            };
            if let Some(placeholder) = placeholder {
                return ControlFlow::Break(pg_error(
                    "ERROR",
                    "0A000",
                    format!(
                        "parameter {placeholder} cannot be a whole {clause} item, \
                         where a number is a column position"
                    ),
                ));
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::{check_prepared, contains, count, substitute};

    #[test]
    fn quoted_comments_and_dollar_strings_are_not_parameter_contexts()
    -> Result<(), Box<dyn std::error::Error>> {
        let sql = "select '$1', \"$1\", $$ $1 $$, $tag$ $1 $tag$, E'\\\'$1', $1 /* $1 /* $2 */ */ -- $2\n, $2";
        let expected = "select '$1', \"$1\", $$ $1 $$, $tag$ $1 $tag$, E'\\\'$1', 7 /* $1 /* $2 */ */ -- $2\n, 'ok'";
        assert_eq!(
            substitute(sql, &["7".to_owned(), "'ok'".to_owned()])?,
            expected
        );
        assert!(!contains("select '$1', \"$1\", $$ $1 $$ /* $2 */")?);
        let literal = "'$tag$; insert into secrets values (1); --'".to_owned();
        assert_eq!(
            substitute("select $tag$ $1 $tag$", &[literal])?,
            "select $tag$ $1 $tag$"
        );
        Ok(())
    }

    #[test]
    fn unicode_and_newlines_preserve_exact_nonparameter_text()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            substitute(
                "select 'ą🦀'\r\n, $2, $1, $2",
                &["1".to_owned(), "2".to_owned()]
            )?,
            "select 'ą🦀'\r\n, 2, 1, 2"
        );
        assert!(substitute("select $0", &["1".to_owned()]).is_err());
        assert!(substitute("select $999999999999999999999999", &[]).is_err());
        assert!(substitute("select $2", &["1".to_owned()]).is_err());
        Ok(())
    }

    #[test]
    fn repeated_parameters_cannot_expand_without_a_size_bound() {
        let literal = "x".repeat(super::MAX_EXPANDED_SQL_BYTES / 2 + 1);
        assert!(substitute("select $1, $1", &[literal]).is_err());
    }

    #[test]
    fn prepared_statements_hold_at_most_one_statement() -> Result<(), Box<dyn std::error::Error>> {
        for sql in [
            "",
            "select 1",
            "select 1;",
            " ; select ';' ;; ",
            "select $$;$$, \";\", $tag$ ; $tag$ /* ; */ -- ;",
        ] {
            check_prepared(sql)?;
        }
        for sql in [
            "select 1; select 2",
            "insert into t values (1);select 1",
            "select 1; /* ; */ select 2;",
            "select ';'; select $1",
        ] {
            let Err(pgwire::error::PgWireError::UserError(error)) = check_prepared(sql) else {
                return Err(format!("{sql} was accepted").into());
            };
            assert_eq!(error.code, "42601", "{sql}");
        }
        Ok(())
    }

    #[test]
    fn placeholder_numbers_are_bounded() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(count("select $65535, $2")?, 65_535);
        for sql in [
            "select $0",
            "select $65536",
            "select $4000000000",
            "select $999999999999999999999999",
        ] {
            let Err(pgwire::error::PgWireError::UserError(error)) = count(sql) else {
                return Err(format!("{sql} was accepted").into());
            };
            assert_eq!(error.code, "42P02", "{sql}");
        }
        Ok(())
    }

    #[test]
    fn spliced_literals_cannot_merge_with_neighbouring_tokens()
    -> Result<(), Box<dyn std::error::Error>> {
        let sql = "select 0-$1, u&$2, x||$2, f($1), $1 from t where a = $1 and b=$2";
        let substituted = substitute(sql, &["(-5)".to_owned(), "'x'".to_owned()])?;
        assert_eq!(
            substituted,
            "select 0- (-5), u& 'x', x|| 'x', f((-5)), (-5) from t where a = (-5) and b= 'x'"
        );
        let [statement] = <[_; 1]>::try_from(crate::query::parse(&substituted)?)
            .map_err(|_| std::io::Error::other("expected one statement"))?;
        assert!(statement.to_string().contains("b = 'x'"), "{statement}");
        Ok(())
    }
}
