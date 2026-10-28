//! Bound retained prepared-statement and portal state on each connection.

use std::{collections::BTreeMap, io};

use pgwire::{
    api::{DEFAULT_NAME, store::PortalStore},
    messages::PgWireFrontendMessage,
};

const MAX_ENTRIES: usize = 128;
const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(super) const AVAILABLE_RESULT_BYTES: &str = "__multidb_available_portal_bytes";
pub(super) const NEW_RESULT_BYTES: &str = "__multidb_new_portal_bytes";

#[derive(Default)]
pub(super) struct Budget {
    statements: BTreeMap<String, usize>,
    portals: BTreeMap<String, usize>,
    results: BTreeMap<String, usize>,
    bytes: usize,
}

pub(super) enum Update {
    Statement(String, Option<usize>),
    Portal(String, Option<usize>),
}

fn limit() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "pgwire retained session state limit exceeded",
    )
}

impl Budget {
    pub(super) fn prepare<S: PortalStore<Statement = String>>(
        &self,
        message: &PgWireFrontendMessage,
        store: &S,
    ) -> io::Result<Option<Update>> {
        let update = match message {
            PgWireFrontendMessage::Parse(parse) => {
                let name = parse.name.as_deref().unwrap_or(DEFAULT_NAME);
                let bytes = name
                    .len()
                    .saturating_mul(2)
                    .saturating_add(parse.query.len())
                    .saturating_add(parse.type_oids.len().saturating_mul(128))
                    .saturating_add(256);
                Update::Statement(name.to_owned(), Some(bytes))
            }
            PgWireFrontendMessage::Bind(bind) => {
                let name = bind.portal_name.as_deref().unwrap_or(DEFAULT_NAME);
                let statement_name = bind.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
                // A portal retains its original Arc even if the statement name is
                // replaced or closed, so account for that SQL in each portal too.
                let statement_bytes = store.get_statement(statement_name).map_or(0, |statement| {
                    statement
                        .statement
                        .len()
                        .saturating_add(statement.parameter_types.len().saturating_mul(128))
                        .saturating_add(statement.id.len())
                });
                let bytes = bind
                    .parameters
                    .iter()
                    .fold(statement_bytes, |bytes, parameter| {
                        bytes
                            .saturating_add(parameter.as_ref().map_or(0, bytes::Bytes::len))
                            .saturating_add(64)
                    })
                    .saturating_add(name.len().saturating_mul(2))
                    .saturating_add(statement_name.len())
                    .saturating_add(bind.parameter_format_codes.len().saturating_mul(2))
                    .saturating_add(bind.result_column_format_codes.len().saturating_mul(2))
                    .saturating_add(256);
                Update::Portal(name.to_owned(), Some(bytes))
            }
            PgWireFrontendMessage::Close(close) => {
                let name = close.name.as_deref().unwrap_or(DEFAULT_NAME).to_owned();
                match close.target_type {
                    b'S' => Update::Statement(name, None),
                    b'P' => Update::Portal(name, None),
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        let (entries, name, bytes) = match &update {
            Update::Statement(name, bytes) => (&self.statements, name, *bytes),
            Update::Portal(name, bytes) => (&self.portals, name, *bytes),
        };
        if let Some(bytes) = bytes {
            if !entries.contains_key(name) && entries.len() >= MAX_ENTRIES {
                return Err(limit());
            }
            let retained = self
                .bytes
                .saturating_sub(entries.get(name).copied().unwrap_or(0))
                .saturating_sub(if matches!(&update, Update::Portal(_, _)) {
                    self.results.get(name).copied().unwrap_or(0)
                } else {
                    0
                });
            if bytes > MAX_BYTES || retained.saturating_add(bytes) > MAX_BYTES {
                return Err(limit());
            }
        }
        Ok(Some(update))
    }

    pub(super) fn commit(&mut self, update: Update) {
        if let Update::Portal(name, _) = &update {
            self.bytes = self
                .bytes
                .saturating_sub(self.results.remove(name).unwrap_or(0));
        }
        let (entries, name, bytes) = match update {
            Update::Statement(name, bytes) => (&mut self.statements, name, bytes),
            Update::Portal(name, bytes) => (&mut self.portals, name, bytes),
        };
        self.bytes = self
            .bytes
            .saturating_sub(entries.remove(&name).unwrap_or(0));
        if let Some(bytes) = bytes {
            self.bytes = self.bytes.saturating_add(bytes);
            entries.insert(name, bytes);
        }
    }

    pub(super) fn available_result_bytes(&self) -> usize {
        MAX_BYTES.saturating_sub(self.bytes)
    }

    pub(super) fn record_result(&mut self, name: &str, bytes: Option<usize>) -> io::Result<()> {
        let old = self.results.get(name).copied().unwrap_or(0);
        let retained = self.bytes.saturating_sub(old);
        if let Some(bytes) = bytes {
            if retained.saturating_add(bytes) > MAX_BYTES {
                return Err(limit());
            }
            self.results.insert(name.to_owned(), bytes);
            self.bytes = retained.saturating_add(bytes);
        } else {
            self.results.remove(name);
            self.bytes = retained;
        }
        Ok(())
    }
}

pub(super) fn result_bytes(rows: &crate::query::SqlRows, limit: usize) -> io::Result<usize> {
    fn value_bytes(value: &crate::model::Value, depth: usize, limit: usize) -> io::Result<usize> {
        use crate::model::Value;
        if depth > 128 {
            return Err(super::session_state::limit());
        }
        let bytes = match value {
            Value::Str(value) => value.len(),
            Value::Bytes(value) => value.len(),
            Value::Vector(value) => value.len().saturating_mul(4),
            Value::Array(values) => {
                let mut bytes = values.len().saturating_mul(std::mem::size_of::<Value>());
                for value in values {
                    bytes = bytes.saturating_add(value_bytes(value, depth + 1, limit)?);
                    if bytes > limit {
                        return Err(super::session_state::limit());
                    }
                }
                bytes
            }
            Value::Object(values) => {
                let mut bytes = values
                    .len()
                    .saturating_mul(std::mem::size_of::<Value>() + 64);
                for (key, value) in values {
                    bytes = bytes.saturating_add(key.len()).saturating_add(value_bytes(
                        value,
                        depth + 1,
                        limit,
                    )?);
                    if bytes > limit {
                        return Err(super::session_state::limit());
                    }
                }
                bytes
            }
            _ => 0,
        };
        if bytes > limit {
            return Err(super::session_state::limit());
        }
        Ok(bytes)
    }
    let mut bytes = rows
        .rows
        .len()
        .saturating_mul(std::mem::size_of::<crate::query::Row>());
    bytes = rows.columns.iter().fold(bytes, |bytes, column| {
        bytes.saturating_add(128).saturating_add(column.len())
    });
    for row in &rows.rows {
        bytes = bytes.saturating_add(
            row.len()
                .saturating_mul(std::mem::size_of::<crate::model::Value>()),
        );
        for value in row {
            bytes = bytes.saturating_add(value_bytes(value, 0, limit)?);
            if bytes > limit {
                return Err(self::limit());
            }
        }
    }
    if bytes > limit {
        return Err(self::limit());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::{
        api::store::MemPortalStore,
        messages::extendedquery::{Bind, Close, Parse},
    };

    fn parse(name: &str, sql: &str) -> PgWireFrontendMessage {
        PgWireFrontendMessage::Parse(Parse::new(
            Some(name.to_owned()),
            sql.to_owned(),
            Vec::new(),
        ))
    }

    #[test]
    fn retained_names_are_bounded_and_close_releases_the_reservation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = Budget::default();
        let store = MemPortalStore::<String>::default();
        for index in 0..MAX_ENTRIES {
            let update = budget
                .prepare(&parse(&index.to_string(), "select 1"), &store)?
                .ok_or("missing update")?;
            budget.commit(update);
        }
        assert!(
            budget
                .prepare(&parse("over-limit", "select 1"), &store)
                .is_err()
        );
        let same = budget
            .prepare(&parse("0", "select 2"), &store)?
            .ok_or("missing update")?;
        budget.commit(same);
        let close = PgWireFrontendMessage::Close(Close::new(b'S', Some("0".to_owned())));
        let update = budget.prepare(&close, &store)?.ok_or("missing close")?;
        budget.commit(update);
        assert!(budget.prepare(&parse("new", "select 1"), &store).is_ok());
        Ok(())
    }

    #[test]
    fn byte_budget_covers_statements_and_bound_parameter_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = Budget::default();
        let store = MemPortalStore::<String>::default();
        assert!(
            budget
                .prepare(&parse("big", &"x".repeat(MAX_BYTES)), &store)
                .is_err()
        );
        let first = budget
            .prepare(&parse("small", "select 1"), &store)?
            .ok_or("missing update")?;
        budget.commit(first);
        let bind = PgWireFrontendMessage::Bind(Bind::new(
            Some("portal".to_owned()),
            Some("small".to_owned()),
            Vec::new(),
            vec![Some(bytes::Bytes::from(vec![0; MAX_BYTES]))],
            Vec::new(),
        ));
        assert!(budget.prepare(&bind, &store).is_err());
        assert!(budget.bytes < MAX_BYTES);
        Ok(())
    }

    #[test]
    fn suspended_results_share_the_retained_budget_and_finished_results_release_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut budget = Budget::default();
        budget.record_result("a", Some(MAX_BYTES / 2))?;
        assert_eq!(budget.available_result_bytes(), MAX_BYTES / 2);
        assert!(budget.record_result("b", Some(MAX_BYTES / 2 + 1)).is_err());
        budget.record_result("a", None)?;
        assert_eq!(budget.available_result_bytes(), MAX_BYTES);
        let rows = crate::query::SqlRows {
            columns: vec!["data".to_owned()],
            rows: vec![vec![crate::model::Value::Str("x".repeat(1_024))]],
        };
        assert!(result_bytes(&rows, 1_000).is_err());
        assert!(result_bytes(&rows, 2_000).is_ok());
        Ok(())
    }
}
