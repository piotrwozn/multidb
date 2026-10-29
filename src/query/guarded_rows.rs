//! Owned adapter output whose allocations remain admitted until Arrow consumes it.
use crate::storage::{ReadBudget, ReadReservation, StorageError};

use super::{Row, Value};

pub(crate) struct GuardedRows {
    // Field order matters: values drop before their reservation tokens.
    rows: Vec<Row>,
    reservations: Vec<Box<dyn ReadReservation>>,
}

struct SortRow {
    key: Vec<u8>,
    row: Row,
    reservation: Box<dyn ReadReservation>,
}

impl GuardedRows {
    pub(crate) const fn new() -> Self {
        Self {
            rows: Vec::new(),
            reservations: Vec::new(),
        }
    }

    pub(crate) fn as_rows(&self) -> &[Row] {
        &self.rows
    }

    pub(crate) fn push(&mut self, row: Row, reservation: Box<dyn ReadReservation>) {
        self.rows.push(row);
        self.reservations.push(reservation);
    }

    pub(crate) fn retain_reservation(&mut self, reservation: Box<dyn ReadReservation>) {
        self.reservations.push(reservation);
    }

    pub(crate) fn truncate(&mut self, limit: usize) {
        self.rows.truncate(limit);
    }

    pub(crate) fn sort_debug(&mut self, budget: &dyn ReadBudget) -> Result<(), StorageError> {
        self.sort(
            budget,
            |row| row_bound(row, false),
            |row, limit| debug_key(row, limit, budget),
        )
    }

    pub(crate) fn sort_json(&mut self, budget: &dyn ReadBudget) -> Result<(), StorageError> {
        self.sort(
            budget,
            |row| row_bound(row, true),
            |row, limit| json_key(row, limit, budget),
        )
    }

    fn sort(
        &mut self,
        budget: &dyn ReadBudget,
        bound: impl Fn(&Row) -> usize,
        key: impl Fn(&Row, usize) -> Result<Vec<u8>, StorageError>,
    ) -> Result<(), StorageError> {
        budget.check()?;
        if self.rows.len() < 2 {
            return Ok(());
        }
        let _metadata = budget.reserve(
            self.rows
                .len()
                .saturating_mul(4 * (std::mem::size_of::<SortRow>() + std::mem::size_of::<Row>()))
                .saturating_add(512),
        )?;
        let mut keyed = Vec::with_capacity(self.rows.len());
        for row in std::mem::take(&mut self.rows) {
            budget.check()?;
            let limit = bound(&row);
            let reservation = budget.reserve(limit.saturating_mul(2).saturating_add(128))?;
            let key = key(&row, limit)?;
            keyed.push(SortRow {
                key,
                row,
                reservation,
            });
        }
        // Cache one admitted key per row instead of formatting complete payloads
        // in every comparison (the established ordering is preserved).
        keyed.sort_by(|left, right| left.key.cmp(&right.key));
        budget.check()?;
        self.rows = Vec::with_capacity(keyed.len());
        for SortRow {
            key,
            row,
            reservation,
        } in keyed
        {
            self.rows.push(row);
            drop(key);
            drop(reservation);
        }
        Ok(())
    }
}

struct KeyWriter<'a> {
    bytes: Vec<u8>,
    budget: &'a dyn ReadBudget,
    limit: usize,
    error: Option<StorageError>,
}

impl KeyWriter<'_> {
    fn append(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(StorageError::ResourceLimit(
                "sort key exceeds its allocation bound".to_owned(),
            ));
        }
        for chunk in bytes.chunks(4_096) {
            self.budget.check()?;
            self.bytes.extend_from_slice(chunk);
        }
        Ok(())
    }
}

impl std::fmt::Write for KeyWriter<'_> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.append(text.as_bytes()).map_err(|error| {
            self.error = Some(error);
            std::fmt::Error
        })
    }
}

impl std::io::Write for KeyWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.append(bytes).map(|()| bytes.len()).map_err(|error| {
            self.error = Some(error);
            std::io::Error::other("guarded sort key writer stopped")
        })
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn debug_key(row: &Row, limit: usize, budget: &dyn ReadBudget) -> Result<Vec<u8>, StorageError> {
    use std::fmt::Write;
    let mut writer = KeyWriter {
        bytes: Vec::new(),
        budget,
        limit,
        error: None,
    };
    let result = write!(&mut writer, "{row:?}");
    if let Some(error) = writer.error {
        return Err(error);
    }
    result.map_err(|_| StorageError::Corruption("cannot format historical row".to_owned()))?;
    budget.check()?;
    Ok(writer.bytes)
}

fn json_key(row: &Row, limit: usize, budget: &dyn ReadBudget) -> Result<Vec<u8>, StorageError> {
    let mut writer = KeyWriter {
        bytes: Vec::new(),
        budget,
        limit,
        error: None,
    };
    let result = serde_json::to_writer(&mut writer, row);
    if let Some(error) = writer.error {
        return Err(error);
    }
    result.map_err(|error| StorageError::Corruption(error.to_string()))?;
    budget.check()?;
    Ok(writer.bytes)
}

/// `serde_json` borrows unescaped strings. Its reusable scratch buffer is needed
/// only for escaped strings, not for large numeric byte arrays around them.
pub(crate) fn json_unescape_working_bytes(bytes: &[u8]) -> usize {
    let mut start = None;
    let mut escaped = false;
    let mut has_escape = false;
    let mut longest = 0_usize;
    for (index, byte) in bytes.iter().enumerate() {
        if let Some(begin) = start {
            if escaped {
                escaped = false;
                continue;
            }
            if *byte == b'\\' {
                escaped = true;
                has_escape = true;
            } else if *byte == b'"' {
                if has_escape {
                    longest = longest.max(index - begin);
                }
                start = None;
            }
        } else if *byte == b'"' {
            start = Some(index + 1);
            has_escape = false;
        }
    }
    if let Some(begin) = start
        && has_escape
    {
        longest = longest.max(bytes.len() - begin);
    }
    longest.saturating_mul(2).saturating_add(128)
}

fn row_bound(row: &Row, json: bool) -> usize {
    row.iter().fold(2_usize, |bytes, value| {
        bytes
            .saturating_add(2)
            .saturating_add(value_bound(value, json))
    })
}

pub(crate) fn json_value_upper_bytes(value: &Value) -> usize {
    value_bound(value, true)
}

pub(crate) fn owned_value_memory_bound(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(owned_heap(value))
}

fn owned_heap(value: &Value) -> usize {
    match value {
        Value::Str(value) => value.capacity(),
        Value::Bytes(value) => value.capacity(),
        Value::Vector(value) => value.capacity().saturating_mul(std::mem::size_of::<f32>()),
        Value::Array(values) => values.iter().fold(
            values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>()),
            |bytes, value| bytes.saturating_add(owned_heap(value)),
        ),
        Value::Object(values) => values.iter().fold(
            if values.is_empty() { 0 } else { 1_024 },
            |bytes: usize, (key, value)| {
                bytes
                    .saturating_add(256 + std::mem::size_of::<Value>())
                    .saturating_add(key.capacity())
                    .saturating_add(owned_heap(value))
            },
        ),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::GeoPoint { .. } => {
            0
        }
    }
}

fn text_bound(text: &str, json: bool) -> usize {
    if json {
        text.chars().fold(2_usize, |bytes, character| {
            bytes.saturating_add(match character {
                '"' | '\\' => 2,
                character if character.is_ascii_control() => 6,
                character => character.len_utf8(),
            })
        })
    } else {
        text.chars().fold(2_usize, |bytes, character| {
            bytes.saturating_add(character.escape_debug().map(char::len_utf8).sum::<usize>())
        })
    }
}

fn value_bound(value: &Value, json: bool) -> usize {
    match value {
        Value::Null => 6,
        Value::Bool(_) => 16,
        Value::Int(_) => 32,
        Value::Float(_) => 64,
        Value::Str(value) => text_bound(value, json).saturating_add(16),
        Value::Bytes(value) => value.len().saturating_mul(5).saturating_add(16),
        Value::Vector(value) => value.len().saturating_mul(64).saturating_add(16),
        Value::Array(values) => values.iter().fold(16_usize, |bytes, value| {
            bytes
                .saturating_add(2)
                .saturating_add(value_bound(value, json))
        }),
        Value::Object(values) => values.iter().fold(16_usize, |bytes, (key, value)| {
            bytes
                .saturating_add(4)
                .saturating_add(text_bound(key, json))
                .saturating_add(value_bound(value, json))
        }),
        Value::GeoPoint { .. } => 128,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn formatting_bounds_cover_control_unicode_binary_and_nested_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let row = vec![
            Value::Str("\0\n\"\\'é\u{200b}".to_owned()),
            Value::Bytes(vec![0, 255]),
            Value::Int(i64::MIN),
            Value::Float(f64::MAX),
            Value::Array(vec![Value::Null, Value::Bool(true)]),
        ];
        assert!(format!("{row:?}").len() <= row_bound(&row, false));
        assert!(serde_json::to_vec(&row)?.len() <= row_bound(&row, true));
        Ok(())
    }

    #[test]
    fn json_scratch_tracks_escaped_strings_without_counting_numeric_byte_arrays() {
        let mut payload = b"{\"Bytes\":[".to_vec();
        for _ in 0..65_536 {
            payload.extend_from_slice(b"255,");
        }
        payload.extend_from_slice(b"0]}");
        assert_eq!(json_unescape_working_bytes(&payload), 128);
        assert_eq!(json_unescape_working_bytes(br#"["hello\nworld"]"#), 152);
        assert_eq!(json_unescape_working_bytes(br#"["escaped\"quote"]"#), 156);
    }

    struct Checkpoints(AtomicUsize);

    impl ReadBudget for Checkpoints {
        fn check(&self) -> Result<(), StorageError> {
            if self.0.fetch_sub(1, Ordering::SeqCst) == 1 {
                Err(StorageError::Cancelled("checkpoint stopped".to_owned()))
            } else {
                Ok(())
            }
        }

        fn reserve(&self, _bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
            Ok(Box::new(()))
        }

        fn remaining_bytes(&self) -> usize {
            usize::MAX
        }
    }

    #[test]
    fn historical_sort_key_checks_cancellation_while_formatting_a_large_row() {
        let row = vec![Value::Bytes(vec![255; 65_536])];
        let budget = Checkpoints(AtomicUsize::new(32));
        let result = debug_key(&row, row_bound(&row, false), &budget);
        assert!(
            matches!(result, Err(StorageError::Cancelled(_))),
            "{result:?}"
        );
        assert_eq!(budget.0.load(Ordering::SeqCst), 0);
        let budget = Checkpoints(AtomicUsize::new(32));
        let result = json_key(&row, row_bound(&row, true), &budget);
        assert!(
            matches!(result, Err(StorageError::Cancelled(_))),
            "{result:?}"
        );
        assert_eq!(budget.0.load(Ordering::SeqCst), 0);
    }
}
