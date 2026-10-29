//! Commit timestamps with borrowed/streamed write validation and no payload Vecs.

use std::{cell::RefCell, fmt, time::SystemTime};

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::{
    phase30::HlcTimestamp,
    storage::{ReadBudget, StorageError},
    txn::TxnId,
};

const MAGIC: &[u8] = b"MDBCLOG2";
const FIELDS: &[&str] = &["txn_id", "committed_at", "hlc", "writes"];

pub(super) fn decode_commit_metadata(
    bytes: &[u8],
    budget: &dyn ReadBudget,
) -> Result<(TxnId, SystemTime), StorageError> {
    budget.check()?;
    let context = Checked {
        budget,
        failure: RefCell::new(None),
    };
    let metadata = if let Some(payload) = bytes.strip_prefix(MAGIC) {
        let mut decoder = postcard::Deserializer::from_bytes(payload);
        let result = HeaderSeed {
            context: &context,
            binary: true,
        }
        .deserialize(&mut decoder);
        let metadata = result.map_err(|error| context.error(error))?;
        let remainder = decoder.finalize().map_err(|error| context.error(error))?;
        if !remainder.is_empty() {
            return Err(StorageError::Corruption(
                "commit metadata has trailing binary bytes".to_owned(),
            ));
        }
        metadata
    } else {
        // Numeric byte arrays need no Vec or string scratch. Escaped table/key
        // names can grow serde_json's reusable string buffer and are admitted.
        let _scratch = budget.reserve(crate::query::json_unescape_working_bytes(bytes))?;
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        let metadata = HeaderSeed {
            context: &context,
            binary: false,
        }
        .deserialize(&mut decoder)
        .map_err(|error| context.error(error))?;
        decoder.end().map_err(|error| context.error(error))?;
        metadata
    };
    budget.check()?;
    Ok(metadata)
}

struct Checked<'a> {
    budget: &'a dyn ReadBudget,
    failure: RefCell<Option<StorageError>>,
}
impl Checked<'_> {
    fn check<E: serde::de::Error>(&self) -> Result<(), E> {
        self.budget.check().map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            E::custom("commit metadata read interrupted")
        })
    }
    fn error(&self, error: impl fmt::Display) -> StorageError {
        self.failure
            .borrow_mut()
            .take()
            .unwrap_or_else(|| StorageError::Corruption(format!("commit metadata: {error}")))
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum HeaderField {
    TxnId,
    CommittedAt,
    Hlc,
    Writes,
    #[serde(other)]
    Other,
}

struct HeaderSeed<'a, 'b> {
    context: &'a Checked<'b>,
    binary: bool,
}
impl<'de> DeserializeSeed<'de> for HeaderSeed<'_, '_> {
    type Value = (TxnId, SystemTime);
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        self.context.check()?;
        decoder.deserialize_struct("CommitLogRecord", FIELDS, self)
    }
}
impl<'de> Visitor<'de> for HeaderSeed<'_, '_> {
    type Value = (TxnId, SystemTime);
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a commit log record")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
        let txn_id = fields
            .next_element::<TxnId>()?
            .ok_or_else(|| serde::de::Error::missing_field("txn_id"))?;
        let committed_at = fields
            .next_element::<SystemTime>()?
            .ok_or_else(|| serde::de::Error::missing_field("committed_at"))?;
        let _ = fields
            .next_element::<Option<HlcTimestamp>>()?
            .ok_or_else(|| serde::de::Error::missing_field("hlc"))?;
        fields
            .next_element_seed(WritesSeed {
                context: self.context,
                binary: self.binary,
            })?
            .ok_or_else(|| serde::de::Error::missing_field("writes"))?;
        self.context.check()?;
        Ok((txn_id, committed_at))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut fields: A) -> Result<Self::Value, A::Error> {
        let (mut txn_id, mut committed_at, mut hlc, mut writes) = (None, None, false, false);
        while let Some(field) = fields.next_key::<HeaderField>()? {
            self.context.check()?;
            match field {
                HeaderField::TxnId => {
                    if txn_id.is_some() {
                        return Err(serde::de::Error::duplicate_field("txn_id"));
                    }
                    txn_id = Some(fields.next_value::<TxnId>()?);
                }
                HeaderField::CommittedAt => {
                    if committed_at.is_some() {
                        return Err(serde::de::Error::duplicate_field("committed_at"));
                    }
                    committed_at = Some(fields.next_value::<SystemTime>()?);
                }
                HeaderField::Hlc => {
                    if hlc {
                        return Err(serde::de::Error::duplicate_field("hlc"));
                    }
                    let _ = fields.next_value::<Option<HlcTimestamp>>()?;
                    hlc = true;
                }
                HeaderField::Writes => {
                    if writes {
                        return Err(serde::de::Error::duplicate_field("writes"));
                    }
                    fields.next_value_seed(WritesSeed {
                        context: self.context,
                        binary: self.binary,
                    })?;
                    writes = true;
                }
                HeaderField::Other => {
                    fields.next_value_seed(SkipAny(self.context))?;
                }
            }
        }
        if !writes {
            return Err(serde::de::Error::missing_field("writes"));
        }
        Ok((
            txn_id.ok_or_else(|| serde::de::Error::missing_field("txn_id"))?,
            committed_at.ok_or_else(|| serde::de::Error::missing_field("committed_at"))?,
        ))
    }
}

struct WritesSeed<'a, 'b> {
    context: &'a Checked<'b>,
    binary: bool,
}
impl<'de> DeserializeSeed<'de> for WritesSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for WritesSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("commit writes")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut writes: A) -> Result<(), A::Error> {
        while writes
            .next_element_seed(WriteSeed {
                context: self.context,
                binary: self.binary,
            })?
            .is_some()
        {
            self.context.check()?;
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum WriteField {
    Table,
    Key,
    Value,
    #[serde(other)]
    Other,
}

struct WriteSeed<'a, 'b> {
    context: &'a Checked<'b>,
    binary: bool,
}
impl<'de> DeserializeSeed<'de> for WriteSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.context.check()?;
        decoder.deserialize_struct("CommitLogWrite", &["table", "key", "value"], self)
    }
}
impl<'de> Visitor<'de> for WriteSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a typed commit write")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut fields: A) -> Result<(), A::Error> {
        fields
            .next_element_seed(Text(self.context))?
            .ok_or_else(|| serde::de::Error::missing_field("table"))?;
        fields
            .next_element_seed(Blob {
                context: self.context,
                binary: self.binary,
            })?
            .ok_or_else(|| serde::de::Error::missing_field("key"))?;
        fields
            .next_element_seed(OptionalBlob {
                context: self.context,
                binary: self.binary,
            })?
            .ok_or_else(|| serde::de::Error::missing_field("value"))?;
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut fields: A) -> Result<(), A::Error> {
        let (mut table, mut key, mut value) = (false, false, false);
        while let Some(field) = fields.next_key::<WriteField>()? {
            self.context.check()?;
            match field {
                WriteField::Table => {
                    if table {
                        return Err(serde::de::Error::duplicate_field("table"));
                    }
                    fields.next_value_seed(Text(self.context))?;
                    table = true;
                }
                WriteField::Key => {
                    if key {
                        return Err(serde::de::Error::duplicate_field("key"));
                    }
                    fields.next_value_seed(Blob {
                        context: self.context,
                        binary: self.binary,
                    })?;
                    key = true;
                }
                WriteField::Value => {
                    if value {
                        return Err(serde::de::Error::duplicate_field("value"));
                    }
                    fields.next_value_seed(OptionalBlob {
                        context: self.context,
                        binary: self.binary,
                    })?;
                    value = true;
                }
                WriteField::Other => {
                    fields.next_value_seed(SkipAny(self.context))?;
                }
            }
        }
        if !table {
            return Err(serde::de::Error::missing_field("table"));
        }
        if !key {
            return Err(serde::de::Error::missing_field("key"));
        }
        // Missing Option<Bytes> has the same default None as serde's derived struct.
        Ok(())
    }
}

struct Text<'a, 'b>(&'a Checked<'b>);
impl<'de> DeserializeSeed<'de> for Text<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_str(self)
    }
}
impl Visitor<'_> for Text<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        self.0.check()?;
        for _ in value.as_bytes().chunks(4096) {
            self.0.check()?;
        }
        Ok(())
    }
}

struct Blob<'a, 'b> {
    context: &'a Checked<'b>,
    binary: bool,
}
impl<'de> DeserializeSeed<'de> for Blob<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        if self.binary {
            decoder.deserialize_bytes(self)
        } else {
            decoder.deserialize_seq(self)
        }
    }
}
impl<'de> Visitor<'de> for Blob<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a byte array")
    }
    fn visit_borrowed_bytes<E: serde::de::Error>(self, bytes: &'de [u8]) -> Result<(), E> {
        self.visit_bytes(bytes)
    }
    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<(), E> {
        self.context.check()?;
        for _ in bytes.chunks(4096) {
            self.context.check()?;
        }
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut bytes: A) -> Result<(), A::Error> {
        self.context.check()?;
        let mut count = 0_usize;
        while bytes.next_element::<u8>()?.is_some() {
            count += 1;
            if count.is_multiple_of(4096) {
                self.context.check()?;
            }
        }
        self.context.check()
    }
}

struct OptionalBlob<'a, 'b> {
    context: &'a Checked<'b>,
    binary: bool,
}
impl<'de> DeserializeSeed<'de> for OptionalBlob<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_option(self)
    }
}
impl<'de> Visitor<'de> for OptionalBlob<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("optional byte array")
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<(), E> {
        self.context.check()
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        self.context.check()
    }
    fn visit_some<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        Blob {
            context: self.context,
            binary: self.binary,
        }
        .deserialize(decoder)
    }
}

struct SkipAny<'a, 'b>(&'a Checked<'b>);
impl<'de> DeserializeSeed<'de> for SkipAny<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0.check()?;
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for SkipAny<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        self.0.check()
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        self.0.check()
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        self.0.check()
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        self.0.check()
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        self.0.check()
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        Text(self.0).visit_str(value)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        while values.next_element_seed(SkipAny(self.0))?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut values: A) -> Result<(), A::Error> {
        while values.next_key_seed(Text(self.0))?.is_some() {
            values.next_value_seed(SkipAny(self.0))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::ReadReservation,
        txn::{CommitLogRecord, CommitLogWrite, encode_commit_log_record},
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, UNIX_EPOCH},
    };

    struct Budget {
        limit: usize,
        checks: AtomicUsize,
        timeout_after: usize,
    }
    impl ReadBudget for Budget {
        fn check(&self) -> Result<(), StorageError> {
            if self.checks.fetch_add(1, Ordering::Relaxed) >= self.timeout_after {
                return Err(StorageError::Timeout("test cancellation".to_owned()));
            }
            Ok(())
        }
        fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
            self.check()?;
            if bytes > self.limit {
                return Err(StorageError::ResourceLimit("test scratch limit".to_owned()));
            }
            Ok(Box::new(()))
        }
        fn remaining_bytes(&self) -> usize {
            self.limit
        }
    }
    fn budget(limit: usize) -> Budget {
        Budget {
            limit,
            checks: AtomicUsize::new(0),
            timeout_after: usize::MAX,
        }
    }
    fn record() -> CommitLogRecord {
        CommitLogRecord {
            txn_id: 42,
            committed_at: UNIX_EPOCH + Duration::from_secs(123),
            hlc: Some(HlcTimestamp {
                physical_ms: 123_000,
                logical: 1,
            }),
            writes: vec![CommitLogWrite {
                table: "rows".to_owned(),
                key: vec![1, 2],
                value: Some(vec![7; 100_000]),
            }],
        }
    }

    #[test]
    fn binary_and_json_headers_do_not_need_write_payload_allocations()
    -> Result<(), Box<dyn std::error::Error>> {
        let record = record();
        for bytes in [
            encode_commit_log_record(&record)?,
            serde_json::to_vec(&record)?,
        ] {
            assert_eq!(
                decode_commit_metadata(&bytes, &budget(256))?,
                (record.txn_id, record.committed_at)
            );
        }
        let mut legacy = serde_json::to_value(&record)?;
        legacy.as_object_mut().ok_or("not an object")?.remove("hlc");
        legacy["writes"][0]
            .as_object_mut()
            .ok_or("not a write")?
            .remove("value");
        assert_eq!(
            decode_commit_metadata(&serde_json::to_vec(&legacy)?, &budget(256))?,
            (record.txn_id, record.committed_at)
        );
        Ok(())
    }

    #[test]
    fn malformed_writes_duplicates_missing_fields_and_trailing_bytes_still_fail()
    -> Result<(), Box<dyn std::error::Error>> {
        let record = record();
        let mut binary = encode_commit_log_record(&record)?;
        binary.pop();
        assert!(matches!(
            decode_commit_metadata(&binary, &budget(256)),
            Err(StorageError::Corruption(_))
        ));
        let mut binary = encode_commit_log_record(&record)?;
        binary.push(0);
        assert!(matches!(
            decode_commit_metadata(&binary, &budget(256)),
            Err(StorageError::Corruption(_))
        ));
        for value in [
            r#"{"txn_id":1,"txn_id":2,"committed_at":{"secs_since_epoch":1,"nanos_since_epoch":0},"writes":[]}"#,
            r#"{"txn_id":1,"committed_at":{"secs_since_epoch":1,"nanos_since_epoch":0}}"#,
            r#"{"txn_id":1,"committed_at":{"secs_since_epoch":1,"nanos_since_epoch":0},"writes":[{"table":"t","key":[256],"value":null}]}"#,
            r#"{"txn_id":1,"committed_at":{"secs_since_epoch":1,"nanos_since_epoch":0},"writes":[]} false"#,
        ] {
            assert!(matches!(
                decode_commit_metadata(value.as_bytes(), &budget(256)),
                Err(StorageError::Corruption(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn byte_streams_preserve_timeout_and_escaped_strings_admit_scratch_first()
    -> Result<(), Box<dyn std::error::Error>> {
        let record = record();
        for bytes in [
            encode_commit_log_record(&record)?,
            serde_json::to_vec(&record)?,
        ] {
            let budget = Budget {
                limit: 256,
                checks: AtomicUsize::new(0),
                timeout_after: 12,
            };
            assert!(matches!(
                decode_commit_metadata(&bytes, &budget),
                Err(StorageError::Timeout(_))
            ));
        }
        let mut escaped = record;
        escaped.writes[0].table = "\n".repeat(10_000);
        assert!(matches!(
            decode_commit_metadata(&serde_json::to_vec(&escaped)?, &budget(256)),
            Err(StorageError::ResourceLimit(_))
        ));
        Ok(())
    }
}
