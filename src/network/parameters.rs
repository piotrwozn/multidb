//! Bind only tokenizer-recognized parameter tokens, preserving all other SQL bytes.

use pgwire::error::PgWireResult;
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer},
};

use super::pg_error;

const MAX_EXPANDED_SQL_BYTES: usize = 16 * 1024 * 1024;

fn positions(sql: &str) -> PgWireResult<Vec<(usize, usize, usize)>> {
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
    let mut result = Vec::new();
    for token in tokens {
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
            .filter(|index| *index > 0)
            .ok_or_else(|| pg_error("ERROR", "22023", "invalid SQL parameter index"))?;
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
        result.push((start, end, parameter));
    }
    Ok(result)
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

pub(super) fn substitute(sql: &str, literals: &[String]) -> PgWireResult<String> {
    let positions = positions(sql)?;
    let mut output = String::with_capacity(sql.len());
    let mut previous = 0;
    for (start, end, parameter) in positions {
        let literal = literals
            .get(parameter - 1)
            .ok_or_else(|| pgwire::error::PgWireError::ParameterIndexOutOfBound(parameter))?;
        let next_len = output
            .len()
            .checked_add(start - previous)
            .and_then(|len| len.checked_add(literal.len()));
        if next_len.is_none_or(|len| len > MAX_EXPANDED_SQL_BYTES) {
            return Err(pg_error(
                "ERROR",
                "54000",
                "SQL parameter expansion exceeds size limit",
            ));
        }
        output.push_str(&sql[previous..start]);
        output.push_str(literal);
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

#[cfg(test)]
mod tests {
    use super::{contains, substitute};

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
}
