use std::{collections::BTreeMap, str};

use uuid::Uuid;

use crate::keyenc;
use crate::repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition};
use crate::storage::{Bytes, StorageError};

pub const DOCUMENT_TABLE: &str = "documents";
pub const INDEX_TABLE: &str = "document_indexes";
const EMPTY_INDEX_VALUE: &[u8] = b"";
const DOCUMENT_KEY_LEN: usize = 20;
const INDEX_PREFIX_LEN: usize = 8;
const INDEX_KEY_DOC_ID_LEN: usize = 16;
const VALUE_MAGIC: &[u8; 4] = b"MDBV";
pub const VALUE_FORMAT_V1: u8 = 1;
const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_INT: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_STR: u8 = 0x04;
const TAG_BYTES: u8 = 0x05;
const TAG_ARRAY: u8 = 0x06;
const TAG_OBJECT: u8 = 0x07;
const TAG_VECTOR: u8 = 0x08;
const TAG_GEO_POINT: u8 = 0x09;

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Bytes),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
    Vector(Vec<f32>),
    GeoPoint { lon: f64, lat: f64 },
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct CollectionId(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentId([u8; 16]);

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct IndexId(u32);

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FieldPath(Vec<String>);

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IndexSpec {
    pub id: IndexId,
    pub path: FieldPath,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Eq {
        path: FieldPath,
        value: Value,
    },
    Range {
        path: FieldPath,
        start: Value,
        end: Value,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanKind {
    IndexScan(IndexId),
    FullScan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    pub plan: PlanKind,
    pub examined_documents: usize,
    pub documents: Vec<(DocumentId, Value)>,
}

pub struct DocumentCollection<'repl, R: Replication + ?Sized> {
    repl: &'repl R,
    collection_id: CollectionId,
    indexes: Vec<IndexSpec>,
}

#[derive(thiserror::Error, Debug)]
pub enum ModelError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("unsupported index value")]
    UnsupportedIndexValue,

    #[error("invalid float for index")]
    InvalidFloat,

    #[error("invalid range")]
    InvalidRange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueCodecLimits {
    pub max_document_bytes: usize,
    pub max_nesting_depth: usize,
}

impl Default for ValueCodecLimits {
    fn default() -> Self {
        Self {
            max_document_bytes: 16 * 1_024 * 1_024,
            max_nesting_depth: 128,
        }
    }
}

/// Serializes a `Value` to storage bytes.
/// # Errors
/// Fails if the value violates the canonical binary codec limits.
pub fn encode_value(value: &Value) -> Result<Bytes, StorageError> {
    encode_value_with_limits(value, ValueCodecLimits::default())
}

/// Serializes a `Value` with explicit codec limits.
/// # Errors
/// Fails if the value is too large, too deep, or contains non-finite numbers.
pub fn encode_value_with_limits(
    value: &Value,
    limits: ValueCodecLimits,
) -> Result<Bytes, StorageError> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(VALUE_MAGIC);
    out.push(VALUE_FORMAT_V1);
    encode_value_payload(value, &mut out, 0, limits)?;
    ensure_encoded_size(&out, limits)?;
    Ok(out)
}

/// Deserializes storage bytes into a `Value`.
/// # Errors
/// Fails with corruption when bytes are not a valid encoded `Value`.
pub fn decode_value(bytes: &[u8]) -> Result<Value, StorageError> {
    decode_value_with_limits(bytes, ValueCodecLimits::default())
}

/// Deserializes storage bytes with explicit codec limits.
/// # Errors
/// Fails with corruption when bytes are neither `MDBV/1` nor legacy JSON.
pub fn decode_value_with_limits(
    bytes: &[u8],
    limits: ValueCodecLimits,
) -> Result<Value, StorageError> {
    if bytes.len() > limits.max_document_bytes {
        return Err(value_too_large_error(limits.max_document_bytes));
    }
    if bytes.starts_with(VALUE_MAGIC) {
        return decode_binary_value(bytes, limits);
    }

    let value = serde_json::from_slice(bytes)
        .map_err(|error| StorageError::Corruption(error.to_string()))?;
    canonicalize_legacy_value(value, 0, limits)
}

#[must_use]
pub fn value_is_binary_encoded(bytes: &[u8]) -> bool {
    bytes.starts_with(VALUE_MAGIC)
}

fn encode_value_payload(
    value: &Value,
    out: &mut Bytes,
    depth: usize,
    limits: ValueCodecLimits,
) -> Result<(), StorageError> {
    if depth > limits.max_nesting_depth {
        return Err(value_too_deep_error(limits.max_nesting_depth));
    }
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Bool(value) => {
            out.push(TAG_BOOL);
            out.push(u8::from(*value));
        }
        Value::Int(value) => {
            out.push(TAG_INT);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(non_finite_value_error());
            }
            out.push(TAG_FLOAT);
            out.extend_from_slice(&keyenc::normalize_f64(*value).to_bits().to_be_bytes());
        }
        Value::Str(value) => {
            out.push(TAG_STR);
            encode_len_bytes(value.as_bytes(), out)?;
        }
        Value::Bytes(value) => {
            out.push(TAG_BYTES);
            encode_len_bytes(value, out)?;
        }
        Value::Array(values) => {
            out.push(TAG_ARRAY);
            encode_len(values.len(), out)?;
            for value in values {
                encode_value_payload(value, out, depth.saturating_add(1), limits)?;
            }
        }
        Value::Object(values) => {
            out.push(TAG_OBJECT);
            encode_len(values.len(), out)?;
            for (key, value) in values {
                encode_len_bytes(key.as_bytes(), out)?;
                encode_value_payload(value, out, depth.saturating_add(1), limits)?;
            }
        }
        Value::Vector(values) => {
            out.push(TAG_VECTOR);
            encode_len(values.len(), out)?;
            for value in values {
                if !value.is_finite() {
                    return Err(non_finite_value_error());
                }
                out.extend_from_slice(&value.to_bits().to_be_bytes());
            }
        }
        Value::GeoPoint { lon, lat } => {
            if !lon.is_finite() || !lat.is_finite() {
                return Err(non_finite_value_error());
            }
            out.push(TAG_GEO_POINT);
            out.extend_from_slice(&keyenc::normalize_f64(*lon).to_bits().to_be_bytes());
            out.extend_from_slice(&keyenc::normalize_f64(*lat).to_bits().to_be_bytes());
        }
    }
    ensure_encoded_size(out, limits)
}

fn decode_binary_value(bytes: &[u8], limits: ValueCodecLimits) -> Result<Value, StorageError> {
    if bytes.len() < VALUE_MAGIC.len() + 1 {
        return Err(StorageError::Corruption(
            "truncated value header".to_owned(),
        ));
    }
    let version = bytes[VALUE_MAGIC.len()];
    if version != VALUE_FORMAT_V1 {
        return Err(StorageError::Corruption(format!(
            "unsupported value codec version {version}"
        )));
    }
    let mut cursor = VALUE_MAGIC.len() + 1;
    let value = decode_value_payload(bytes, &mut cursor, 0, limits)?;
    if cursor != bytes.len() {
        return Err(StorageError::Corruption(
            "trailing bytes after value".to_owned(),
        ));
    }
    Ok(value)
}

fn decode_value_payload(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
    limits: ValueCodecLimits,
) -> Result<Value, StorageError> {
    if depth > limits.max_nesting_depth {
        return Err(StorageError::Corruption(format!(
            "value nesting depth exceeds {}",
            limits.max_nesting_depth
        )));
    }
    let tag = read_u8(bytes, cursor)?;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_BOOL => match read_u8(bytes, cursor)? {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            other => Err(StorageError::Corruption(format!(
                "invalid bool byte {other}"
            ))),
        },
        TAG_INT => Ok(Value::Int(read_i64(bytes, cursor)?)),
        TAG_FLOAT => Ok(Value::Float(read_f64(bytes, cursor)?)),
        TAG_STR => Ok(Value::Str(read_string(bytes, cursor)?)),
        TAG_BYTES => Ok(Value::Bytes(read_len_bytes(bytes, cursor)?.to_vec())),
        TAG_ARRAY => {
            let count = read_len(bytes, cursor)?;
            if count > bytes.len().saturating_sub(*cursor) {
                return Err(StorageError::Corruption(
                    "array item count exceeds remaining bytes".to_owned(),
                ));
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_value_payload(
                    bytes,
                    cursor,
                    depth.saturating_add(1),
                    limits,
                )?);
            }
            Ok(Value::Array(values))
        }
        TAG_OBJECT => {
            let count = read_len(bytes, cursor)?;
            let min_remaining = count.checked_mul(9).ok_or_else(|| {
                StorageError::Corruption("object item count overflows".to_owned())
            })?;
            if min_remaining > bytes.len().saturating_sub(*cursor) {
                return Err(StorageError::Corruption(
                    "object item count exceeds remaining bytes".to_owned(),
                ));
            }
            let mut values = BTreeMap::new();
            for _ in 0..count {
                let key = read_string(bytes, cursor)?;
                let value = decode_value_payload(bytes, cursor, depth.saturating_add(1), limits)?;
                values.insert(key, value);
            }
            Ok(Value::Object(values))
        }
        TAG_VECTOR => {
            let count = read_len(bytes, cursor)?;
            let byte_len = count.checked_mul(4).ok_or_else(|| {
                StorageError::Corruption("vector byte length overflows".to_owned())
            })?;
            if byte_len > bytes.len().saturating_sub(*cursor) {
                return Err(StorageError::Corruption("truncated vector".to_owned()));
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let value = read_f32(bytes, cursor)?;
                if !value.is_finite() {
                    return Err(StorageError::Corruption(
                        "non-finite vector value".to_owned(),
                    ));
                }
                values.push(value);
            }
            Ok(Value::Vector(values))
        }
        TAG_GEO_POINT => {
            let lon = read_f64(bytes, cursor)?;
            let lat = read_f64(bytes, cursor)?;
            Ok(Value::GeoPoint { lon, lat })
        }
        other => Err(StorageError::Corruption(format!("bad value tag {other}"))),
    }
}

fn canonicalize_legacy_value(
    value: Value,
    depth: usize,
    limits: ValueCodecLimits,
) -> Result<Value, StorageError> {
    if depth > limits.max_nesting_depth {
        return Err(StorageError::Corruption(format!(
            "value nesting depth exceeds {}",
            limits.max_nesting_depth
        )));
    }
    match value {
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(StorageError::Corruption(
                    "non-finite numeric value".to_owned(),
                ));
            }
            Ok(Value::Float(keyenc::normalize_f64(value)))
        }
        Value::Array(values) => values
            .into_iter()
            .map(|value| canonicalize_legacy_value(value, depth.saturating_add(1), limits))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    key,
                    canonicalize_legacy_value(value, depth.saturating_add(1), limits)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(Value::Object),
        Value::Vector(values) => {
            if values.iter().any(|value| !value.is_finite()) {
                return Err(StorageError::Corruption(
                    "non-finite vector value".to_owned(),
                ));
            }
            Ok(Value::Vector(values))
        }
        Value::GeoPoint { lon, lat } => {
            if !lon.is_finite() || !lat.is_finite() {
                return Err(StorageError::Corruption("non-finite geo value".to_owned()));
            }
            Ok(Value::GeoPoint {
                lon: keyenc::normalize_f64(lon),
                lat: keyenc::normalize_f64(lat),
            })
        }
        other => Ok(other),
    }
}

fn encode_len(value: usize, out: &mut Bytes) -> Result<(), StorageError> {
    let len = u64::try_from(value).map_err(|error| StorageError::Backend(error.to_string()))?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn encode_len_bytes(value: &[u8], out: &mut Bytes) -> Result<(), StorageError> {
    encode_len(value.len(), out)?;
    out.extend_from_slice(value);
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, StorageError> {
    let Some(value) = bytes.get(*cursor).copied() else {
        return Err(StorageError::Corruption("truncated value".to_owned()));
    };
    *cursor = (*cursor).saturating_add(1);
    Ok(value)
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], StorageError> {
    let end = (*cursor).saturating_add(N);
    if end > bytes.len() {
        return Err(StorageError::Corruption("truncated value".to_owned()));
    }
    let mut raw = [0_u8; N];
    raw.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(raw)
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> Result<usize, StorageError> {
    usize::try_from(u64::from_be_bytes(read_array::<8>(bytes, cursor)?))
        .map_err(|error| StorageError::Corruption(error.to_string()))
}

fn read_len_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], StorageError> {
    let len = read_len(bytes, cursor)?;
    let end = (*cursor).saturating_add(len);
    if end > bytes.len() {
        return Err(StorageError::Corruption("truncated bytes".to_owned()));
    }
    let value = &bytes[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> Result<String, StorageError> {
    let value = read_len_bytes(bytes, cursor)?;
    str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|error| StorageError::Corruption(error.to_string()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, StorageError> {
    Ok(i64::from_be_bytes(read_array::<8>(bytes, cursor)?))
}

fn read_f64(bytes: &[u8], cursor: &mut usize) -> Result<f64, StorageError> {
    let value = f64::from_bits(u64::from_be_bytes(read_array::<8>(bytes, cursor)?));
    if !value.is_finite() {
        return Err(StorageError::Corruption(
            "non-finite numeric value".to_owned(),
        ));
    }
    Ok(keyenc::normalize_f64(value))
}

fn read_f32(bytes: &[u8], cursor: &mut usize) -> Result<f32, StorageError> {
    Ok(f32::from_bits(u32::from_be_bytes(read_array::<4>(
        bytes, cursor,
    )?)))
}

fn ensure_encoded_size(out: &[u8], limits: ValueCodecLimits) -> Result<(), StorageError> {
    if out.len() > limits.max_document_bytes {
        return Err(value_too_large_error(limits.max_document_bytes));
    }
    Ok(())
}

fn non_finite_value_error() -> StorageError {
    StorageError::Backend("non-finite numeric values cannot be encoded".to_owned())
}

fn value_too_deep_error(limit: usize) -> StorageError {
    StorageError::Backend(format!("value nesting depth exceeds {limit}"))
}

fn value_too_large_error(limit: usize) -> StorageError {
    StorageError::Backend(format!("encoded value exceeds {limit} bytes"))
}

impl CollectionId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl DocumentId {
    #[must_use]
    pub fn generate() -> Self {
        Self(*Uuid::now_v7().as_bytes())
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl IndexId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl FieldPath {
    #[must_use]
    pub fn new(segments: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(segments.into_iter().map(Into::into).collect())
    }

    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }
}

impl IndexSpec {
    #[must_use]
    pub const fn new(id: IndexId, path: FieldPath) -> Self {
        Self { id, path }
    }
}

impl<'repl, R: Replication + ?Sized> DocumentCollection<'repl, R> {
    #[must_use]
    pub const fn new(repl: &'repl R, collection_id: CollectionId) -> Self {
        Self {
            repl,
            collection_id,
            indexes: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_indexes(
        repl: &'repl R,
        collection_id: CollectionId,
        indexes: Vec<IndexSpec>,
    ) -> Self {
        Self {
            repl,
            collection_id,
            indexes,
        }
    }

    /// Inserts a document and returns its generated id.
    /// # Errors
    /// Fails when serialization, replication, or storage fails.
    pub fn insert(&self, doc: &Value) -> Result<DocumentId, ModelError> {
        let (id, ops) = self.insert_ops(doc)?;
        self.repl.propose_batch(ops)?;
        Ok(id)
    }

    /// Reads a document with strong consistency.
    /// # Errors
    /// Fails when replication, storage, or deserialization fails.
    pub fn get(&self, id: DocumentId) -> Result<Option<Value>, ModelError> {
        self.get_with_consistency(id, ReadConsistency::Strong)
    }

    /// Reads a document with the requested consistency level.
    /// # Errors
    /// Fails when replication, storage, or deserialization fails.
    pub fn get_with_consistency(
        &self,
        id: DocumentId,
        consistency: ReadConsistency,
    ) -> Result<Option<Value>, ModelError> {
        let key = make_document_key(self.collection_id, id);

        let Some(bytes) = self.repl.read(DOCUMENT_TABLE, &key, consistency)? else {
            return Ok(None);
        };

        Ok(Some(decode_value(&bytes)?))
    }

    /// Replaces a document or creates it if it does not exist.
    /// # Errors
    /// Fails when serialization, replication, or storage fails.
    pub fn update(&self, id: DocumentId, doc: &Value) -> Result<(), ModelError> {
        let key = make_document_key(self.collection_id, id);
        let old_bytes = self
            .repl
            .read(DOCUMENT_TABLE, &key, ReadConsistency::Strong)?;
        let old = old_bytes.as_deref().map(decode_value).transpose()?;
        let ops = self.replace_ops_with_old(id, old.as_ref(), doc)?;
        let condition = WriteCondition::ValueEquals {
            table: DOCUMENT_TABLE.to_owned(),
            key,
            expected: old_bytes,
        };
        match self
            .repl
            .propose_conditional_batch(ConditionalBatch::new(vec![condition], ops.clone()))
        {
            Ok(()) => {}
            Err(ReplError::Unsupported(_)) => self.repl.propose_batch(ops)?,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Deletes a document. Missing documents are ignored.
    /// # Errors
    /// Fails when replication or storage fails.
    pub fn delete(&self, id: DocumentId) -> Result<(), ModelError> {
        let key = make_document_key(self.collection_id, id);
        let Some(old_bytes) = self
            .repl
            .read(DOCUMENT_TABLE, &key, ReadConsistency::Strong)?
        else {
            return Ok(());
        };
        let old = decode_value(&old_bytes)?;
        let ops = self.delete_ops_with_old(id, Some(&old))?;
        let condition = WriteCondition::ValueEquals {
            table: DOCUMENT_TABLE.to_owned(),
            key,
            expected: Some(old_bytes),
        };
        match self
            .repl
            .propose_conditional_batch(ConditionalBatch::new(vec![condition], ops.clone()))
        {
            Ok(()) => {}
            Err(ReplError::Unsupported(_)) => self.repl.propose_batch(ops)?,
            Err(error) => return Err(error.into()),
        }

        Ok(())
    }

    /// Builds all operations needed to insert a document.
    /// # Errors
    /// Fails when the document or its indexed values cannot be encoded.
    pub fn insert_ops(&self, doc: &Value) -> Result<(DocumentId, Vec<Op>), ModelError> {
        let id = DocumentId::generate();
        let mut ops = Vec::new();
        ops.push(put_document_op(self.collection_id, id, doc)?);
        append_new_index_ops(&mut ops, self.collection_id, &self.indexes, id, doc)?;
        Ok((id, ops))
    }

    /// Builds all operations needed to update a document.
    /// # Errors
    /// Fails when reads, serialization, or indexed value encoding fails.
    pub fn update_ops(&self, id: DocumentId, doc: &Value) -> Result<Vec<Op>, ModelError> {
        let mut ops = Vec::new();
        if let Some(old) = self.get(id)? {
            append_old_index_delete_ops(&mut ops, self.collection_id, &self.indexes, id, &old)?;
        }
        ops.push(put_document_op(self.collection_id, id, doc)?);
        append_new_index_ops(&mut ops, self.collection_id, &self.indexes, id, doc)?;
        Ok(ops)
    }

    /// Builds all operations needed to delete a document.
    /// # Errors
    /// Fails when reads or indexed value encoding fails.
    pub fn delete_ops(&self, id: DocumentId) -> Result<Vec<Op>, ModelError> {
        let mut ops = Vec::new();
        if let Some(old) = self.get(id)? {
            append_old_index_delete_ops(&mut ops, self.collection_id, &self.indexes, id, &old)?;
            ops.push(delete_document_op(self.collection_id, id));
        }
        Ok(ops)
    }

    /// Builds put operations for a known document id without reading current storage.
    /// # Errors
    /// Fails when the document or indexed values cannot be encoded.
    pub fn put_ops_for_id(&self, id: DocumentId, doc: &Value) -> Result<Vec<Op>, ModelError> {
        let mut ops = Vec::new();
        ops.push(put_document_op(self.collection_id, id, doc)?);
        append_new_index_ops(&mut ops, self.collection_id, &self.indexes, id, doc)?;
        Ok(ops)
    }

    /// Builds replacement operations using a caller-provided old snapshot value.
    /// # Errors
    /// Fails when the document or indexed values cannot be encoded.
    pub fn replace_ops_with_old(
        &self,
        id: DocumentId,
        old: Option<&Value>,
        doc: &Value,
    ) -> Result<Vec<Op>, ModelError> {
        let mut ops = Vec::new();
        if let Some(old) = old {
            append_old_index_delete_ops(&mut ops, self.collection_id, &self.indexes, id, old)?;
        }
        ops.extend(self.put_ops_for_id(id, doc)?);
        Ok(ops)
    }

    /// Builds delete operations using a caller-provided old snapshot value.
    /// # Errors
    /// Fails when indexed values cannot be encoded.
    pub fn delete_ops_with_old(
        &self,
        id: DocumentId,
        old: Option<&Value>,
    ) -> Result<Vec<Op>, ModelError> {
        let mut ops = Vec::new();
        if let Some(old) = old {
            append_old_index_delete_ops(&mut ops, self.collection_id, &self.indexes, id, old)?;
            ops.push(delete_document_op(self.collection_id, id));
        }
        Ok(ops)
    }

    #[must_use]
    pub fn document_key(&self, id: DocumentId) -> Bytes {
        make_document_key(self.collection_id, id)
    }

    /// Scans all documents from this collection.
    /// # Errors
    /// Fails when storage or deserialization fails.
    pub fn scan(&self) -> Result<Vec<(DocumentId, Value)>, ModelError> {
        let (start, end) = collection_range_bounds(self.collection_id);
        self.repl
            .range(DOCUMENT_TABLE, &start, &end, ReadConsistency::Strong)?
            .into_iter()
            .filter_map(|(key, value)| document_id_from_key(&key).map(|id| (id, value)))
            .map(|(id, value)| Ok((id, decode_value(&value)?)))
            .collect()
    }

    /// Scans a bounded page of documents from this collection.
    /// # Errors
    /// Fails when storage or deserialization fails.
    pub fn scan_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(DocumentId, Value)>, ModelError> {
        const BATCH_ROWS: usize = 256;

        if limit == 0 {
            return Ok(Vec::new());
        }

        let (start, end) = collection_range_bounds(self.collection_id);
        let stop_after = offset.saturating_add(limit);
        let mut seen = 0usize;
        let mut documents = Vec::new();
        let mut decode_error = None;
        self.repl.scan_range_batches(
            DOCUMENT_TABLE,
            &start,
            &end,
            ReadConsistency::Strong,
            BATCH_ROWS,
            &|| false,
            &mut |batch| {
                for (key, value) in batch {
                    let Some(id) = document_id_from_key(key) else {
                        continue;
                    };
                    if seen >= offset {
                        match decode_value(value) {
                            Ok(document) => documents.push((id, document)),
                            Err(error) => {
                                decode_error = Some(error);
                                return Ok(false);
                            }
                        }
                    }
                    seen = seen.saturating_add(1);
                    if seen >= stop_after {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        if let Some(error) = decode_error {
            return Err(error.into());
        }
        Ok(documents)
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub fn indexes(&self) -> &[IndexSpec] {
        &self.indexes
    }

    /// Runs a simple document query.
    /// # Errors
    /// Fails when index encoding, replication, storage, or deserialization fails.
    pub fn query(&self, predicate: &Predicate) -> Result<QueryResult, ModelError> {
        if let Some(index) = self.find_index(predicate.path()) {
            self.query_with_index(index, predicate)
        } else {
            self.query_full_scan(predicate)
        }
    }

    fn find_index(&self, path: &FieldPath) -> Option<&IndexSpec> {
        self.indexes.iter().find(|index| index.path == *path)
    }

    fn query_with_index(
        &self,
        index: &IndexSpec,
        predicate: &Predicate,
    ) -> Result<QueryResult, ModelError> {
        let ids = match predicate {
            Predicate::Eq { value, .. } => {
                let prefix = make_index_prefix(self.collection_id, index.id, value)?;
                range_index_ids(self.repl, &prefix)?
            }
            Predicate::Range { start, end, .. } => {
                let start_key = make_index_prefix(self.collection_id, index.id, start)?;
                let end_key = make_index_prefix(self.collection_id, index.id, end)?;
                if start_key >= end_key {
                    return Err(ModelError::InvalidRange);
                }
                range_index_ids_between(self.repl, &start_key, &end_key)?
            }
        };

        let mut documents = Vec::new();
        for id in ids {
            if let Some(document) = self.get(id)?
                && predicate.matches(&document)
            {
                documents.push((id, document));
            }
        }

        Ok(QueryResult {
            plan: PlanKind::IndexScan(index.id),
            examined_documents: documents.len(),
            documents,
        })
    }

    fn query_full_scan(&self, predicate: &Predicate) -> Result<QueryResult, ModelError> {
        let (start, end) = collection_range_bounds(self.collection_id);
        let rows = self
            .repl
            .range(DOCUMENT_TABLE, &start, &end, ReadConsistency::Strong)?;
        let mut documents = Vec::new();
        let mut examined_documents = 0;

        for (key, value) in rows {
            if let Some(id) = document_id_from_key(&key) {
                examined_documents += 1;
                let document = decode_value(&value)?;
                if predicate.matches(&document) {
                    documents.push((id, document));
                }
            }
        }

        Ok(QueryResult {
            plan: PlanKind::FullScan,
            examined_documents,
            documents,
        })
    }
}

impl Predicate {
    #[must_use]
    pub const fn path(&self) -> &FieldPath {
        match self {
            Self::Eq { path, .. } | Self::Range { path, .. } => path,
        }
    }

    fn matches(&self, doc: &Value) -> bool {
        match self {
            Self::Eq { path, value } => extract_path(doc, path) == Some(value),
            Self::Range { path, start, end } => extract_path(doc, path)
                .and_then(|value| encode_index_value(value).ok())
                .is_some_and(|encoded| {
                    let start = encode_index_value(start);
                    let end = encode_index_value(end);
                    matches!((start, end), (Ok(start), Ok(end)) if encoded >= start && encoded < end)
                }),
        }
    }
}

pub(crate) fn make_document_key(collection_id: CollectionId, document_id: DocumentId) -> Bytes {
    let mut key = Vec::with_capacity(20);
    key.extend_from_slice(&collection_id.as_u32().to_be_bytes());
    key.extend_from_slice(&document_id.as_bytes());
    key
}

pub(crate) fn collection_range_bounds(collection_id: CollectionId) -> (Bytes, Bytes) {
    keyenc::u32_prefix_range(collection_id.as_u32())
}

pub(crate) fn document_id_from_key(key: &[u8]) -> Option<DocumentId> {
    if key.len() != DOCUMENT_KEY_LEN {
        return None;
    }

    let mut id = [0; INDEX_KEY_DOC_ID_LEN];
    id.copy_from_slice(&key[4..]);
    Some(DocumentId::from_bytes(id))
}

fn put_document_op(
    collection_id: CollectionId,
    id: DocumentId,
    doc: &Value,
) -> Result<Op, ModelError> {
    Ok(Op::Put {
        table: DOCUMENT_TABLE.to_owned(),
        key: make_document_key(collection_id, id),
        value: encode_value(doc)?,
    })
}

fn delete_document_op(collection_id: CollectionId, id: DocumentId) -> Op {
    Op::Delete {
        table: DOCUMENT_TABLE.to_owned(),
        key: make_document_key(collection_id, id),
    }
}

fn append_new_index_ops(
    ops: &mut Vec<Op>,
    collection_id: CollectionId,
    indexes: &[IndexSpec],
    id: DocumentId,
    doc: &Value,
) -> Result<(), ModelError> {
    for index in indexes {
        if let Some(value) = extract_path(doc, &index.path) {
            ops.push(Op::Put {
                table: INDEX_TABLE.to_owned(),
                key: make_index_key(collection_id, index.id, value, id)?,
                value: EMPTY_INDEX_VALUE.to_vec(),
            });
        }
    }

    Ok(())
}

fn append_old_index_delete_ops(
    ops: &mut Vec<Op>,
    collection_id: CollectionId,
    indexes: &[IndexSpec],
    id: DocumentId,
    doc: &Value,
) -> Result<(), ModelError> {
    for index in indexes {
        if let Some(value) = extract_path(doc, &index.path) {
            ops.push(Op::Delete {
                table: INDEX_TABLE.to_owned(),
                key: make_index_key(collection_id, index.id, value, id)?,
            });
        }
    }

    Ok(())
}

#[must_use]
pub fn extract_path<'value>(value: &'value Value, path: &FieldPath) -> Option<&'value Value> {
    let mut current = value;

    for segment in path.segments() {
        match current {
            Value::Object(map) => current = map.get(segment)?,
            _ => return None,
        }
    }

    Some(current)
}

fn make_index_key(
    collection_id: CollectionId,
    index_id: IndexId,
    value: &Value,
    document_id: DocumentId,
) -> Result<Bytes, ModelError> {
    let mut key = make_index_prefix(collection_id, index_id, value)?;
    key.extend_from_slice(&document_id.as_bytes());
    Ok(key)
}

fn make_index_prefix(
    collection_id: CollectionId,
    index_id: IndexId,
    value: &Value,
) -> Result<Bytes, ModelError> {
    let encoded_value = encode_index_value(value)?;
    let mut key = Vec::with_capacity(INDEX_PREFIX_LEN + encoded_value.len());
    key.extend_from_slice(&collection_id.as_u32().to_be_bytes());
    key.extend_from_slice(&index_id.as_u32().to_be_bytes());
    key.extend_from_slice(&encoded_value);
    Ok(key)
}

fn range_index_ids<R: Replication + ?Sized>(
    repl: &R,
    prefix: &[u8],
) -> Result<Vec<DocumentId>, ModelError> {
    let end = keyenc::range_end(prefix);
    range_index_ids_between(repl, prefix, &end)
}

fn range_index_ids_between<R: Replication + ?Sized>(
    repl: &R,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<DocumentId>, ModelError> {
    repl.range(INDEX_TABLE, start, end, ReadConsistency::Strong)?
        .into_iter()
        .map(|(key, _)| document_id_from_index_key(&key))
        .collect()
}

fn document_id_from_index_key(key: &[u8]) -> Result<DocumentId, ModelError> {
    if key.len() < INDEX_PREFIX_LEN + INDEX_KEY_DOC_ID_LEN {
        return Err(ModelError::Storage(StorageError::Corruption(
            "index key is too short".to_owned(),
        )));
    }

    let mut id = [0; INDEX_KEY_DOC_ID_LEN];
    id.copy_from_slice(&key[key.len() - INDEX_KEY_DOC_ID_LEN..]);
    Ok(DocumentId::from_bytes(id))
}

pub(crate) fn encode_index_value(value: &Value) -> Result<Bytes, ModelError> {
    let mut bytes = Vec::new();

    match value {
        Value::Null => bytes.push(0x00),
        Value::Bool(value) => {
            bytes.push(0x01);
            bytes.push(u8::from(*value));
        }
        Value::Int(value) => {
            bytes.push(0x02);
            bytes.extend_from_slice(&keyenc::encode_i64_ordered(*value));
        }
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(ModelError::InvalidFloat);
            }

            bytes.push(0x03);
            bytes.extend_from_slice(&keyenc::encode_f64_ordered(*value));
        }
        Value::Str(value) => {
            bytes.push(0x04);
            keyenc::encode_terminated_bytes(value.as_bytes(), &mut bytes);
        }
        Value::Bytes(_)
        | Value::Array(_)
        | Value::Object(_)
        | Value::Vector(_)
        | Value::GeoPoint { .. } => {
            return Err(ModelError::UnsupportedIndexValue);
        }
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        CollectionId, DocumentCollection, DocumentId, FieldPath, IndexId, IndexSpec, ModelError,
        PlanKind, Predicate, Value, ValueCodecLimits, decode_value, decode_value_with_limits,
        encode_index_value, encode_value, encode_value_with_limits, extract_path,
        value_is_binary_encoded,
    };
    use crate::db::{DbConfig, Profile, create_database};
    use crate::storage::StorageError;

    #[test]
    fn value_round_trips_through_codec() -> Result<(), StorageError> {
        let value = Value::Array(vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Float(-0.0),
            Value::Str("Ada".to_owned()),
            Value::Bytes(vec![0, 255]),
            sample_document("Ada", 37),
            Value::Vector(vec![0.1, -2.5]),
            Value::GeoPoint {
                lon: 21.0122,
                lat: 52.2297,
            },
        ]);
        let encoded = encode_value(&value)?;

        assert!(value_is_binary_encoded(&encoded));
        assert_eq!(&encoded[..5], b"MDBV\x01");
        assert_eq!(decode_value(&encoded)?, value);

        Ok(())
    }

    #[test]
    fn legacy_json_decodes_without_rewrite() -> Result<(), StorageError> {
        let value = sample_document("Ada", 37);
        let legacy = serde_json::to_vec(&value)
            .map_err(|error| StorageError::Corruption(error.to_string()))?;

        assert!(!value_is_binary_encoded(&legacy));
        assert_eq!(decode_value(&legacy)?, value);

        Ok(())
    }

    #[test]
    fn codec_canonicalizes_object_order_and_negative_zero() -> Result<(), StorageError> {
        let mut first = BTreeMap::new();
        first.insert("a".to_owned(), Value::Float(-0.0));
        first.insert("b".to_owned(), Value::Int(2));

        let mut second = BTreeMap::new();
        second.insert("b".to_owned(), Value::Int(2));
        second.insert("a".to_owned(), Value::Float(0.0));

        let first_bytes = encode_value(&Value::Object(first))?;
        let second_bytes = encode_value(&Value::Object(second))?;
        assert_eq!(first_bytes, second_bytes);
        let Value::Object(decoded) = decode_value(&first_bytes)? else {
            panic!("expected object");
        };
        let Some(Value::Float(value)) = decoded.get("a") else {
            panic!("expected float");
        };
        assert_eq!(value.to_bits(), 0.0_f64.to_bits());

        Ok(())
    }

    #[test]
    fn codec_rejects_non_finite_numbers() {
        assert!(matches!(
            encode_value(&Value::Float(f64::NAN)),
            Err(StorageError::Backend(_))
        ));
        assert!(matches!(
            encode_value(&Value::Vector(vec![0.1, f32::INFINITY])),
            Err(StorageError::Backend(_))
        ));
        assert!(matches!(
            encode_value(&Value::GeoPoint {
                lon: f64::INFINITY,
                lat: 52.0,
            }),
            Err(StorageError::Backend(_))
        ));
    }

    #[test]
    fn invalid_value_bytes_are_corruption() {
        assert!(matches!(
            decode_value(b"not json"),
            Err(StorageError::Corruption(_))
        ));
    }

    #[test]
    fn codec_rejects_too_deep_too_large_and_unknown_version() -> Result<(), StorageError> {
        let limits = ValueCodecLimits {
            max_document_bytes: 8,
            max_nesting_depth: 0,
        };
        assert!(matches!(
            encode_value_with_limits(&Value::Array(vec![Value::Null]), limits),
            Err(StorageError::Backend(_))
        ));
        assert!(matches!(
            encode_value_with_limits(&Value::Bytes(vec![0; 16]), limits),
            Err(StorageError::Backend(_))
        ));

        let mut encoded = encode_value(&Value::Null)?;
        encoded[4] = 99;
        assert!(matches!(
            decode_value(&encoded),
            Err(StorageError::Corruption(message)) if message.contains("unsupported value codec version")
        ));

        let encoded = encode_value(&Value::Bytes(vec![0; 16]))?;
        assert!(matches!(
            decode_value_with_limits(&encoded, limits),
            Err(StorageError::Backend(_))
        ));

        Ok(())
    }

    #[test]
    fn object_order_is_deterministic() {
        let mut first = BTreeMap::new();
        first.insert("a".to_owned(), Value::Int(1));
        first.insert("b".to_owned(), Value::Int(2));

        let mut second = BTreeMap::new();
        second.insert("b".to_owned(), Value::Int(2));
        second.insert("a".to_owned(), Value::Int(1));

        assert_eq!(Value::Object(first), Value::Object(second));
    }

    #[test]
    fn document_id_generation_is_unique_and_fixed_size() {
        let first = DocumentId::generate();
        let second = DocumentId::generate();

        assert_ne!(first, second);
        assert_eq!(first.as_bytes().len(), 16);
    }

    #[test]
    fn index_encoding_preserves_numeric_and_string_order() -> Result<(), ModelError> {
        assert_index_order(&[Value::Int(-10), Value::Int(0), Value::Int(10)])?;
        assert_index_order(&[
            Value::Float(-1.5),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(1.5),
        ])?;
        assert_eq!(
            encode_index_value(&Value::Float(-0.0))?,
            encode_index_value(&Value::Float(0.0))?
        );

        assert!(
            encode_index_value(&Value::Str("ab".to_owned()))?
                < encode_index_value(&Value::Str("abc".to_owned()))?
        );
        assert!(matches!(
            encode_index_value(&Value::Float(f64::NAN)),
            Err(ModelError::InvalidFloat)
        ));

        Ok(())
    }

    #[test]
    fn json_path_extracts_nested_values() {
        let doc = city_document("Ada", "Warsaw", 37);

        assert_eq!(
            extract_path(&doc, &FieldPath::new(["user", "address", "city"])),
            Some(&Value::Str("Warsaw".to_owned()))
        );
        assert_eq!(
            extract_path(&doc, &FieldPath::new(["user", "address", "missing"])),
            None
        );
        assert_eq!(extract_path(&doc, &FieldPath::new(["age", "bad"])), None);
    }

    #[test]
    fn collection_flow_on_in_memory_database() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        run_collection_flow(&database)
    }

    #[test]
    fn collection_flow_on_redb_database() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("documents.redb");
        let database = create_database(DbConfig::on_disk(Profile::Document, path))?;

        run_collection_flow(&database)
    }

    #[test]
    fn collections_do_not_collide() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let users = DocumentCollection::new(&database, CollectionId::new(1));
        let products = DocumentCollection::new(&database, CollectionId::new(2));
        let id = DocumentId::from_bytes([7; 16]);

        let user_doc = sample_document("User", 1);
        let product_doc = sample_document("Product", 2);

        users.update(id, &user_doc)?;
        products.update(id, &product_doc)?;

        assert_eq!(users.get(id)?, Some(user_doc));
        assert_eq!(products.get(id)?, Some(product_doc));

        Ok(())
    }

    #[test]
    fn indexed_query_uses_index_scan() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(77),
            vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["age"]))],
        );

        collection.insert(&sample_document("Ada", 37))?;
        collection.insert(&sample_document("Grace", 85))?;
        collection.insert(&sample_document("Other", 37))?;

        let result = collection.query(&Predicate::Eq {
            path: FieldPath::new(["age"]),
            value: Value::Int(37),
        })?;

        assert_eq!(result.plan, PlanKind::IndexScan(IndexId::new(1)));
        assert_eq!(result.examined_documents, 2);
        assert_eq!(names(result.documents), vec!["Ada", "Other"]);

        Ok(())
    }

    #[test]
    fn collection_max_id_range_includes_documents() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::new(&database, CollectionId::new(u32::MAX));
        let id = DocumentId::from_bytes([0xFF; 16]);
        let doc = sample_document("Max", 1);

        collection.update(id, &doc)?;

        assert_eq!(collection.get(id)?, Some(doc.clone()));
        assert_eq!(collection.scan()?, vec![(id, doc)]);

        Ok(())
    }

    #[test]
    fn indexed_query_matches_full_scan_for_ff_document_id() -> Result<(), Box<dyn std::error::Error>>
    {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection_id = CollectionId::new(84);
        let indexed = DocumentCollection::with_indexes(
            &database,
            collection_id,
            vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["age"]))],
        );
        let full_scan = DocumentCollection::new(&database, collection_id);
        let id = DocumentId::from_bytes([0xFF; 16]);
        let doc = sample_document("Edge", 37);
        let predicate = Predicate::Eq {
            path: FieldPath::new(["age"]),
            value: Value::Int(37),
        };

        indexed.update(id, &doc)?;

        let index_result = indexed.query(&predicate)?;
        let scan_result = full_scan.query(&predicate)?;
        assert_eq!(index_result.plan, PlanKind::IndexScan(IndexId::new(1)));
        assert_eq!(scan_result.plan, PlanKind::FullScan);
        assert_eq!(index_result.documents, scan_result.documents);
        assert_eq!(index_result.documents, vec![(id, doc)]);

        Ok(())
    }

    #[test]
    fn unindexed_query_uses_full_scan() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(78),
            vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["age"]))],
        );

        collection.insert(&sample_document("Ada", 37))?;
        collection.insert(&sample_document("Grace", 85))?;
        collection.insert(&sample_document("Other", 37))?;

        let result = collection.query(&Predicate::Eq {
            path: FieldPath::new(["name"]),
            value: Value::Str("Grace".to_owned()),
        })?;

        assert_eq!(result.plan, PlanKind::FullScan);
        assert_eq!(result.examined_documents, 3);
        assert_eq!(names(result.documents), vec!["Grace"]);

        Ok(())
    }

    #[test]
    fn json_path_index_queries_nested_values() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(79),
            vec![IndexSpec::new(
                IndexId::new(4),
                FieldPath::new(["user", "address", "city"]),
            )],
        );

        collection.insert(&city_document("Ada", "Warsaw", 37))?;
        collection.insert(&city_document("Grace", "London", 85))?;
        collection.insert(&sample_document("NoCity", 1))?;

        let result = collection.query(&Predicate::Eq {
            path: FieldPath::new(["user", "address", "city"]),
            value: Value::Str("London".to_owned()),
        })?;

        assert_eq!(result.plan, PlanKind::IndexScan(IndexId::new(4)));
        assert_eq!(result.examined_documents, 1);
        assert_eq!(names(result.documents), vec!["Grace"]);

        Ok(())
    }

    #[test]
    fn range_query_uses_index_order() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(80),
            vec![IndexSpec::new(IndexId::new(2), FieldPath::new(["age"]))],
        );

        collection.insert(&sample_document("Minus", -10))?;
        collection.insert(&sample_document("Zero", 0))?;
        collection.insert(&sample_document("Ten", 10))?;

        let result = collection.query(&Predicate::Range {
            path: FieldPath::new(["age"]),
            start: Value::Int(-10),
            end: Value::Int(10),
        })?;

        assert_eq!(result.plan, PlanKind::IndexScan(IndexId::new(2)));
        assert_eq!(names(result.documents), vec!["Minus", "Zero"]);

        Ok(())
    }

    #[test]
    fn update_removes_old_index_entries() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(81),
            vec![IndexSpec::new(IndexId::new(3), FieldPath::new(["age"]))],
        );

        let id = collection.insert(&sample_document("Ada", 37))?;
        collection.update(id, &sample_document("Ada", 38))?;

        let old = collection.query(&Predicate::Eq {
            path: FieldPath::new(["age"]),
            value: Value::Int(37),
        })?;
        let new = collection.query(&Predicate::Eq {
            path: FieldPath::new(["age"]),
            value: Value::Int(38),
        })?;

        assert!(old.documents.is_empty());
        assert_eq!(names(new.documents), vec!["Ada"]);

        Ok(())
    }

    #[test]
    fn delete_removes_index_entries() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(82),
            vec![IndexSpec::new(IndexId::new(5), FieldPath::new(["age"]))],
        );

        let id = collection.insert(&sample_document("Ada", 37))?;
        collection.delete(id)?;

        let result = collection.query(&Predicate::Eq {
            path: FieldPath::new(["age"]),
            value: Value::Int(37),
        })?;

        assert!(result.documents.is_empty());

        Ok(())
    }

    #[test]
    fn failed_index_encoding_does_not_write_document() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let collection = DocumentCollection::with_indexes(
            &database,
            CollectionId::new(83),
            vec![IndexSpec::new(IndexId::new(6), FieldPath::new(["score"]))],
        );
        let mut doc = BTreeMap::new();
        doc.insert("name".to_owned(), Value::Str("Bad".to_owned()));
        doc.insert("score".to_owned(), Value::Float(f64::NAN));

        assert!(matches!(
            collection.insert(&Value::Object(doc)),
            Err(ModelError::InvalidFloat | ModelError::Storage(_))
        ));

        let result = collection.query(&Predicate::Eq {
            path: FieldPath::new(["name"]),
            value: Value::Str("Bad".to_owned()),
        })?;

        assert!(result.documents.is_empty());

        Ok(())
    }

    fn run_collection_flow<R>(repl: &R) -> Result<(), Box<dyn std::error::Error>>
    where
        R: crate::repl::Replication,
    {
        let collection = DocumentCollection::new(repl, CollectionId::new(42));
        let original = sample_document("Ada", 37);
        let updated = sample_document("Grace", 85);

        let id = collection.insert(&original)?;
        assert_eq!(collection.get(id)?, Some(original));

        collection.update(id, &updated)?;
        assert_eq!(collection.get(id)?, Some(updated));

        collection.delete(id)?;
        assert_eq!(collection.get(id)?, None);

        Ok(())
    }

    fn sample_document(name: &str, age: i64) -> Value {
        let mut nested = BTreeMap::new();
        nested.insert("active".to_owned(), Value::Bool(true));
        nested.insert("score".to_owned(), Value::Float(4.5));

        let mut doc = BTreeMap::new();
        doc.insert("name".to_owned(), Value::Str(name.to_owned()));
        doc.insert("age".to_owned(), Value::Int(age));
        doc.insert("raw".to_owned(), Value::Bytes(vec![1, 2, 3]));
        doc.insert("embedding".to_owned(), Value::Vector(vec![0.1, 0.2, 0.3]));
        doc.insert(
            "tags".to_owned(),
            Value::Array(vec![Value::Str("database".to_owned()), Value::Null]),
        );
        doc.insert("meta".to_owned(), Value::Object(nested));

        Value::Object(doc)
    }

    fn city_document(name: &str, city: &str, age: i64) -> Value {
        let mut address = BTreeMap::new();
        address.insert("city".to_owned(), Value::Str(city.to_owned()));

        let mut user = BTreeMap::new();
        user.insert("address".to_owned(), Value::Object(address));

        let mut doc = BTreeMap::new();
        doc.insert("name".to_owned(), Value::Str(name.to_owned()));
        doc.insert("age".to_owned(), Value::Int(age));
        doc.insert("user".to_owned(), Value::Object(user));

        Value::Object(doc)
    }

    fn assert_index_order(values: &[Value]) -> Result<(), ModelError> {
        let encoded = values
            .iter()
            .map(encode_index_value)
            .collect::<Result<Vec<_>, _>>()?;
        let mut sorted = encoded.clone();
        sorted.sort();

        assert_eq!(encoded, sorted);
        Ok(())
    }

    fn names(documents: Vec<(DocumentId, Value)>) -> Vec<String> {
        let mut values = documents
            .into_iter()
            .filter_map(|(_, doc)| match doc {
                Value::Object(map) => match map.get("name") {
                    Some(Value::Str(name)) => Some(name.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        values.sort();
        values
    }
}
