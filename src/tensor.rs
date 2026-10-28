//! Typed tensor datasets with immutable binary chunks and append revisions.
//!
//! Dataset revisions are append snapshots. Sample indexes are immutable, so a selected
//! column read opens only the chunks containing the requested sample positions.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    keyenc,
    repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition},
    storage::{Bytes, StorageError},
    vector::VectorMetric,
};

pub const TENSOR_CHUNKS_TABLE: &str = "tensor_chunks";
pub const TENSOR_META_TABLE: &str = "tensor_meta";
pub const TENSOR_SAMPLE_INDEX_TABLE: &str = "tensor_sample_index";
pub const TENSOR_REVISIONS_TABLE: &str = "tensor_revisions";
const MAGIC: &[u8; 5] = b"MDBT1";
const FRAME_BYTES: usize = 13;
const MAX_ELEMENTS: usize = 4_000_000;
const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;
const APPEND_RETRIES: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorDType {
    U8,
    I64,
    F32,
    F64,
    Bool,
    Utf8,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorData {
    U8(Vec<u8>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Utf8(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tensor {
    pub shape: Vec<usize>,
    pub data: TensorData,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TensorColumn {
    pub name: String,
    pub dtype: TensorDType,
    pub shape: Vec<Option<usize>>,
}

impl TensorColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, dtype: TensorDType, shape: Vec<Option<usize>>) -> Self {
        Self {
            name: name.into(),
            dtype,
            shape,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TensorDatasetConfig {
    pub name: String,
    pub tensors: Vec<TensorColumn>,
    #[serde(default = "default_chunk_samples")]
    pub chunk_samples: usize,
    #[serde(default = "default_chunk_bytes")]
    pub max_chunk_bytes: usize,
}

impl TensorDatasetConfig {
    #[must_use]
    pub fn new(name: impl Into<String>, tensors: Vec<TensorColumn>) -> Self {
        Self {
            name: name.into(),
            tensors,
            chunk_samples: default_chunk_samples(),
            max_chunk_bytes: default_chunk_bytes(),
        }
    }

    /// Validates the schema and physical chunk limits without reading storage.
    /// # Errors
    /// Fails for invalid names, duplicate columns, shapes or chunk limits.
    pub fn validate(&self) -> Result<(), TensorError> {
        valid_name(&self.name)?;
        if !(1..=64).contains(&self.tensors.len()) {
            return config_error("datasets need 1 to 64 tensor columns");
        }
        if !(1..=1024).contains(&self.chunk_samples) {
            return config_error("chunk_samples must be between 1 and 1024");
        }
        if !(1024..=MAX_ENCODED_BYTES).contains(&self.max_chunk_bytes) {
            return config_error("max_chunk_bytes must be between 1024 and 16777216");
        }
        let mut names = BTreeSet::new();
        for column in &self.tensors {
            valid_name(&column.name)?;
            if !names.insert(&column.name) {
                return config_error("duplicate tensor column");
            }
            if column.shape.len() > 8 {
                return config_error("tensor rank cannot exceed 8");
            }
            shape_elements(column.shape.iter().map(|value| value.unwrap_or(1)))?;
        }
        Ok(())
    }
}

pub type TensorBatch = BTreeMap<String, Vec<Tensor>>;

/// A one-use capability issued only after this module validates and frames a typed append.
/// The digest binds it to the exact conditions and operations; forwarding wrappers cannot
/// reuse it to authorize another payload. No public constructor or cloning is provided.
#[doc(hidden)]
#[derive(Debug)]
pub struct TensorWriteAuthorization {
    fingerprint: blake3::Hash,
}

impl TensorWriteAuthorization {
    pub(crate) fn validate(&self, batch: &ConditionalBatch) -> Result<(), ReplError> {
        if self.fingerprint != typed_batch_fingerprint(batch) {
            return Err(ReplError::Rejected(
                "typed tensor write authorization does not match its batch".to_owned(),
            ));
        }
        Ok(())
    }
}

fn tensor_write_authorization(batch: &ConditionalBatch) -> TensorWriteAuthorization {
    TensorWriteAuthorization {
        fingerprint: typed_batch_fingerprint(batch),
    }
}

fn typed_batch_fingerprint(batch: &ConditionalBatch) -> blake3::Hash {
    fn bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
        hasher.update(&value.len().to_le_bytes());
        hasher.update(value);
    }
    let mut hasher = blake3::Hasher::new_derive_key("MultiDB validated tensor append v1");
    hasher.update(&batch.conditions.len().to_le_bytes());
    for condition in &batch.conditions {
        match condition {
            WriteCondition::KeyMissing { table, key } => {
                hasher.update(&[0]);
                bytes(&mut hasher, table.as_bytes());
                bytes(&mut hasher, key);
            }
            WriteCondition::ValueEquals {
                table,
                key,
                expected,
            } => {
                hasher.update(&[1]);
                bytes(&mut hasher, table.as_bytes());
                bytes(&mut hasher, key);
                match expected {
                    Some(value) => {
                        hasher.update(&[1]);
                        bytes(&mut hasher, value);
                    }
                    None => {
                        hasher.update(&[0]);
                    }
                }
            }
        }
    }
    hasher.update(&batch.ops.len().to_le_bytes());
    for operation in &batch.ops {
        match operation {
            Op::Put { table, key, value } => {
                hasher.update(&[0]);
                bytes(&mut hasher, table.as_bytes());
                bytes(&mut hasher, key);
                bytes(&mut hasher, value);
            }
            Op::Delete { table, key } => {
                hasher.update(&[1]);
                bytes(&mut hasher, table.as_bytes());
                bytes(&mut hasher, key);
            }
        }
    }
    hasher.finalize()
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TensorDatasetInfo {
    pub config: TensorDatasetConfig,
    pub samples: u64,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TensorAppendResult {
    pub start: u64,
    pub end: u64,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TensorReadRequest {
    #[serde(default)]
    pub start: u64,
    #[serde(default = "default_read_limit")]
    pub limit: usize,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default)]
    pub revision: Option<u64>,
}

impl Default for TensorReadRequest {
    fn default() -> Self {
        Self {
            start: 0,
            limit: default_read_limit(),
            columns: None,
            revision: None,
        }
    }
}

impl TensorReadRequest {
    #[must_use]
    pub fn new(start: u64, limit: usize) -> Self {
        Self {
            start,
            limit,
            ..Self::default()
        }
    }

    /// # Errors
    /// Fails when a read limit or selected column list is invalid.
    pub fn validate(&self) -> Result<(), TensorError> {
        if !(1..=1024).contains(&self.limit) {
            return invalid("read limit must be between 1 and 1024");
        }
        if let Some(columns) = &self.columns {
            if !(1..=64).contains(&columns.len()) {
                return invalid("columns must contain 1 to 64 names");
            }
            let mut seen = BTreeSet::new();
            for name in columns {
                valid_name(name)?;
                if !seen.insert(name) {
                    return invalid("duplicate selected column");
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TensorReadBatch {
    pub start: u64,
    pub end: u64,
    pub total_samples: u64,
    pub revision: u64,
    pub tensors: TensorBatch,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorDevice {
    #[default]
    Cpu,
    Gpu,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TensorExecution {
    pub device: TensorDevice,
    pub adapter: Option<String>,
    pub backend: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TensorSearchRequest {
    pub tensor: String,
    pub query: Vec<f64>,
    #[serde(default = "default_search_k")]
    pub k: usize,
    #[serde(default)]
    pub metric: VectorMetric,
    #[serde(default)]
    pub start: u64,
    #[serde(default = "default_max_samples")]
    pub max_samples: usize,
    #[serde(default)]
    pub revision: Option<u64>,
}

impl TensorSearchRequest {
    #[must_use]
    pub fn new(tensor: impl Into<String>, query: Vec<f64>) -> Self {
        Self {
            tensor: tensor.into(),
            query,
            k: default_search_k(),
            metric: VectorMetric::default(),
            start: 0,
            max_samples: default_max_samples(),
            revision: None,
        }
    }
    /// # Errors
    /// Fails for invalid names, limits, or non-finite query values.
    pub fn validate(&self) -> Result<(), TensorError> {
        valid_name(&self.tensor)?;
        if !(1..=100).contains(&self.k) {
            return invalid("search k must be between 1 and 100");
        }
        if !(1..=100_000).contains(&self.max_samples) {
            return invalid("max_samples must be between 1 and 100000");
        }
        if self.query.is_empty()
            || self.query.len() > MAX_ELEMENTS
            || self.query.iter().any(|value| !value.is_finite())
        {
            return invalid("query must contain 1 to 4000000 finite values");
        }
        if self.metric == VectorMetric::Cosine && self.query.iter().all(|value| *value == 0.0) {
            return invalid("cosine query cannot be zero");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TensorSearchHit {
    pub index: u64,
    pub distance: f64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TensorSearchResult {
    pub hits: Vec<TensorSearchHit>,
    pub scanned_samples: u64,
    pub next_start: Option<u64>,
    pub total_samples: u64,
    pub revision: u64,
    pub skipped_samples: u64,
    pub execution: TensorExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorGpuOperation {
    Sum,
    Mean,
    L2Norm,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TensorComputeRequest {
    pub tensor: String,
    pub operation: TensorGpuOperation,
    #[serde(default)]
    pub start: u64,
    #[serde(default = "default_read_limit")]
    pub limit: usize,
    #[serde(default)]
    pub revision: Option<u64>,
}

impl TensorComputeRequest {
    /// # Errors
    /// Fails when the tensor name or batch limit is invalid.
    pub fn validate(&self) -> Result<(), TensorError> {
        valid_name(&self.tensor)?;
        if !(1..=1024).contains(&self.limit) {
            return invalid("compute limit must be between 1 and 1024");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TensorComputeResult {
    pub start: u64,
    pub end: u64,
    pub revision: u64,
    pub values: Vec<f32>,
    pub execution: TensorExecution,
}

#[derive(thiserror::Error, Debug)]
pub enum TensorError {
    #[error("invalid tensor config: {0}")]
    Config(String),
    #[error("invalid tensor value: {0}")]
    InvalidValue(String),
    #[error("missing tensor dataset: {0}")]
    MissingDataset(String),
    #[error("missing tensor revision: {0}")]
    MissingRevision(u64),
    #[error("tensor data is corrupt: {0}")]
    Corruption(String),
    #[error("replication: {0}")]
    Repl(#[from] ReplError),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("GPU unavailable: {0}")]
    GpuUnavailable(String),
    #[error("GPU operation unsupported: {0}")]
    GpuUnsupported(String),
    #[error("GPU computation failed: {0}")]
    Gpu(String),
}

pub struct TensorDataset<'a, R: Replication + ?Sized> {
    repl: &'a R,
    config: TensorDatasetConfig,
    require_gpu: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SampleIndex {
    chunk_start: u64,
    position: u32,
}

#[derive(serde::Deserialize)]
struct StoredChunk {
    start: u64,
    samples: Vec<Tensor>,
}

#[derive(serde::Serialize)]
struct ChunkWrite<'a> {
    start: u64,
    samples: &'a [Tensor],
}

impl<'a, R: Replication + ?Sized> TensorDataset<'a, R> {
    /// Opens a typed dataset handle; this does not create metadata.
    /// # Errors
    /// Fails for invalid config or a backend without native bounded scans and commit logs.
    pub fn new(repl: &'a R, config: TensorDatasetConfig) -> Result<Self, TensorError> {
        config.validate()?;
        if !repl.supports_bounded_scans() || !repl.supports_commit_log() {
            return config_error(
                "tensor datasets require a native bounded, conditional commit-log backend",
            );
        }
        Ok(Self {
            repl,
            config,
            require_gpu: false,
        })
    }

    /// Requires physical GPU execution for the default search API. Dataset
    /// storage remains portable; the database profile selects this runtime flag.
    #[must_use]
    pub(crate) const fn with_gpu_required(mut self, required: bool) -> Self {
        self.require_gpu = required;
        self
    }

    #[must_use]
    pub const fn config(&self) -> &TensorDatasetConfig {
        &self.config
    }

    pub(crate) fn create_ops(config: &TensorDatasetConfig) -> Result<Vec<Op>, TensorError> {
        config.validate()?;
        let info = TensorDatasetInfo {
            config: config.clone(),
            samples: 0,
            revision: 0,
        };
        let value = frame(&info)?;
        Ok(vec![
            put(
                TENSOR_META_TABLE,
                config.name.as_bytes().to_vec(),
                value.clone(),
            ),
            put(TENSOR_REVISIONS_TABLE, revision_key(&config.name, 0), value),
        ])
    }

    /// Returns the current manifest or an immutable append revision.
    /// # Errors
    /// Fails for missing manifests, corrupt binary framing or a mismatched schema.
    pub fn info(&self, revision: Option<u64>) -> Result<TensorDatasetInfo, TensorError> {
        let (current, _) = self.current()?;
        let Some(revision) = revision else {
            return Ok(current);
        };
        if revision > current.revision {
            return Err(TensorError::MissingRevision(revision));
        }
        let bytes = self
            .repl
            .read(
                TENSOR_REVISIONS_TABLE,
                &revision_key(&self.config.name, revision),
                ReadConsistency::Strong,
            )?
            .ok_or(TensorError::MissingRevision(revision))?;
        let info: TensorDatasetInfo = unframe(&bytes)?;
        self.check_info(&info)?;
        if info.revision != revision || info.samples > current.samples {
            return corrupt("revision manifest does not match its key");
        }
        Ok(info)
    }

    /// Atomically appends an aligned batch. Every chunk and sample index is immutable.
    /// # Errors
    /// Fails for invalid tensors, oversized chunks, corruption, or repeated write conflicts.
    pub fn append(&self, batch: &TensorBatch) -> Result<TensorAppendResult, TensorError> {
        let (rows, sizes) = self.validate_batch(batch)?;
        for _ in 0..APPEND_RETRIES {
            let (current, expected) = self.current()?;
            let end = current
                .samples
                .checked_add(u64::try_from(rows).map_err(|_| {
                    TensorError::InvalidValue("batch size overflows u64".to_owned())
                })?)
                .ok_or_else(|| {
                    TensorError::InvalidValue("sample index overflows u64".to_owned())
                })?;
            let revision = current
                .revision
                .checked_add(1)
                .ok_or_else(|| TensorError::InvalidValue("revision overflows u64".to_owned()))?;
            let mut ops = self.append_ops(batch, &sizes, current.samples, rows)?;
            let next = TensorDatasetInfo {
                config: self.config.clone(),
                samples: end,
                revision,
            };
            let manifest = frame(&next)?;
            ops.push(put(
                TENSOR_REVISIONS_TABLE,
                revision_key(&self.config.name, revision),
                manifest.clone(),
            ));
            let mut conditions = ops
                .iter()
                .map(|op| match op {
                    Op::Put { table, key, .. } => Ok(WriteCondition::KeyMissing {
                        table: table.clone(),
                        key: key.clone(),
                    }),
                    Op::Delete { .. } => corrupt("tensor append produced a non-append operation"),
                })
                .collect::<Result<Vec<_>, TensorError>>()?;
            conditions.push(WriteCondition::ValueEquals {
                table: TENSOR_META_TABLE.to_owned(),
                key: self.config.name.as_bytes().to_vec(),
                expected: Some(expected),
            });
            ops.push(put(
                TENSOR_META_TABLE,
                self.config.name.as_bytes().to_vec(),
                manifest,
            ));
            let write_batch = ConditionalBatch::new(conditions, ops);
            let authorization = tensor_write_authorization(&write_batch);
            match self.repl.propose_tensor_batch(write_batch, authorization) {
                Ok(()) => {
                    return Ok(TensorAppendResult {
                        start: current.samples,
                        end,
                        revision,
                    });
                }
                Err(ReplError::Conflict | ReplError::Storage(StorageError::Conflict)) => {
                    std::thread::yield_now();
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(ReplError::Conflict.into())
    }

    /// Reads selected columns using the immutable sample index, never preceding chunks.
    /// # Errors
    /// Fails for invalid requests, missing or corrupt records, or more than 64 MiB decoded data.
    pub fn read(&self, request: &TensorReadRequest) -> Result<TensorReadBatch, TensorError> {
        request.validate()?;
        let info = self.info(request.revision)?;
        if request.start > info.samples {
            return invalid("read start is beyond the revision");
        }
        let columns = self.selected_columns(request.columns.as_deref())?;
        let end = request
            .start
            .saturating_add(u64::try_from(request.limit).unwrap_or(u64::MAX))
            .min(info.samples);
        let mut output = columns
            .iter()
            .map(|column| (column.name.clone(), Vec::new()))
            .collect::<TensorBatch>();
        let mut chunks = BTreeMap::<(String, u64), StoredChunk>::new();
        let mut decoded_bytes = 0_usize;
        let mut cached_decoded_bytes = 0_usize;
        for sample in request.start..end {
            let raw = self
                .repl
                .read(
                    TENSOR_SAMPLE_INDEX_TABLE,
                    &sample_key(&self.config.name, sample),
                    ReadConsistency::Strong,
                )?
                .ok_or_else(|| TensorError::Corruption("sample index is missing".to_owned()))?;
            let index: SampleIndex = unframe(&raw)?;
            if index.chunk_start.checked_add(u64::from(index.position)) != Some(sample) {
                return corrupt("sample index points to another sample");
            }
            for column in &columns {
                let cache_key = (column.name.clone(), index.chunk_start);
                if !chunks.contains_key(&cache_key) {
                    let bytes = self
                        .repl
                        .read(
                            TENSOR_CHUNKS_TABLE,
                            &chunk_key(&self.config.name, &column.name, index.chunk_start),
                            ReadConsistency::Strong,
                        )?
                        .ok_or_else(|| {
                            TensorError::Corruption("tensor chunk is missing".to_owned())
                        })?;
                    if bytes.len() > self.config.max_chunk_bytes {
                        return corrupt("chunk exceeds configured byte limit");
                    }
                    let chunk: StoredChunk = unframe(&bytes)?;
                    if chunk.start != index.chunk_start
                        || chunk.samples.is_empty()
                        || chunk.samples.len() > self.config.chunk_samples
                    {
                        return corrupt("chunk header is invalid");
                    }
                    let mut chunk_bytes = 0_usize;
                    for tensor in &chunk.samples {
                        chunk_bytes = checked_bytes(
                            chunk_bytes,
                            validate_tensor(column, tensor)
                                .map_err(|error| TensorError::Corruption(error.to_string()))?,
                        )?;
                    }
                    cached_decoded_bytes = checked_bytes(cached_decoded_bytes, chunk_bytes)?;
                    chunks.insert(cache_key.clone(), chunk);
                }
                let chunk = chunks.get(&cache_key).ok_or_else(|| {
                    TensorError::Corruption("chunk cache entry missing".to_owned())
                })?;
                let tensor = chunk
                    .samples
                    .get(usize::try_from(index.position).map_err(|_| {
                        TensorError::Corruption("chunk position overflows usize".to_owned())
                    })?)
                    .ok_or_else(|| {
                        TensorError::Corruption("chunk position is outside stored data".to_owned())
                    })?;
                decoded_bytes = checked_bytes(decoded_bytes, tensor_decoded_bytes(tensor)?)?;
                output
                    .get_mut(&column.name)
                    .ok_or_else(|| {
                        TensorError::Corruption("selected tensor is missing".to_owned())
                    })?
                    .push(tensor.clone());
            }
        }
        Ok(TensorReadBatch {
            start: request.start,
            end,
            total_samples: info.samples,
            revision: info.revision,
            tensors: output,
        })
    }

    /// Searches with the execution backend selected by the database profile.
    /// # Errors
    /// Fails for invalid requests, missing data, or a required GPU being unavailable.
    pub fn search(&self, request: &TensorSearchRequest) -> Result<TensorSearchResult, TensorError> {
        if self.require_gpu {
            crate::tensor_gpu::search_dataset(self, request)
        } else {
            self.search_cpu(request)
        }
    }

    /// Exact CPU reference search for callers explicitly selecting this backend.
    /// # Errors
    /// Fails for non-floating or non-fixed-vector columns, invalid queries, or corrupt data.
    pub fn search_cpu(
        &self,
        request: &TensorSearchRequest,
    ) -> Result<TensorSearchResult, TensorError> {
        request.validate()?;
        let column = self
            .config
            .tensors
            .iter()
            .find(|column| column.name == request.tensor)
            .ok_or_else(|| TensorError::InvalidValue("search tensor does not exist".to_owned()))?;
        if !matches!(column.dtype, TensorDType::F32 | TensorDType::F64)
            || column.shape.len() != 1
            || column.shape[0].is_none()
            || column.shape[0] != Some(request.query.len())
        {
            return invalid(
                "search requires a fixed-rank-1 F32/F64 tensor matching query dimension",
            );
        }
        let info = self.info(request.revision)?;
        if request.start > info.samples {
            return invalid("search start is beyond the revision");
        }
        let end = request
            .start
            .saturating_add(u64::try_from(request.max_samples).unwrap_or(u64::MAX))
            .min(info.samples);
        let mut hits = Vec::new();
        let mut skipped = 0;
        let mut cursor = request.start;
        while cursor < end {
            // A single sample can occupy most of the decoded-read budget. Keep
            // reference execution streaming even for such large embeddings.
            let limit = 1;
            let batch = self.read(&TensorReadRequest {
                start: cursor,
                limit,
                columns: Some(vec![request.tensor.clone()]),
                revision: Some(info.revision),
            })?;
            let tensors = batch
                .tensors
                .get(&request.tensor)
                .ok_or_else(|| TensorError::Corruption("search column missing".to_owned()))?;
            for (offset, tensor) in tensors.iter().enumerate() {
                let values = match &tensor.data {
                    TensorData::F32(values) => values
                        .iter()
                        .map(|value| f64::from(*value))
                        .collect::<Vec<_>>(),
                    TensorData::F64(values) => values.clone(),
                    _ => return corrupt("search tensor dtype changed"),
                };
                let Some(distance) = exact_distance(&request.query, &values, request.metric)?
                else {
                    skipped += 1;
                    continue;
                };
                hits.push(TensorSearchHit {
                    index: cursor + u64::try_from(offset).unwrap_or(u64::MAX),
                    distance,
                });
                hits.sort_by(|left, right| {
                    left.distance
                        .total_cmp(&right.distance)
                        .then_with(|| left.index.cmp(&right.index))
                });
                hits.truncate(request.k);
            }
            cursor = batch.end;
        }
        Ok(TensorSearchResult {
            hits,
            scanned_samples: end - request.start,
            next_start: (end < info.samples).then_some(end),
            total_samples: info.samples,
            revision: info.revision,
            skipped_samples: skipped,
            execution: TensorExecution::default(),
        })
    }

    fn current(&self) -> Result<(TensorDatasetInfo, Bytes), TensorError> {
        let bytes = self
            .repl
            .read(
                TENSOR_META_TABLE,
                self.config.name.as_bytes(),
                ReadConsistency::Strong,
            )?
            .ok_or_else(|| TensorError::MissingDataset(self.config.name.clone()))?;
        let info: TensorDatasetInfo = unframe(&bytes)?;
        self.check_info(&info)?;
        Ok((info, bytes))
    }

    fn check_info(&self, info: &TensorDatasetInfo) -> Result<(), TensorError> {
        info.config
            .validate()
            .map_err(|error| TensorError::Corruption(error.to_string()))?;
        if info.config != self.config {
            return corrupt("persisted dataset schema differs from its catalog");
        }
        Ok(())
    }

    fn selected_columns(
        &self,
        names: Option<&[String]>,
    ) -> Result<Vec<&TensorColumn>, TensorError> {
        match names {
            None => Ok(self.config.tensors.iter().collect()),
            Some(names) => names
                .iter()
                .map(|name| {
                    self.config
                        .tensors
                        .iter()
                        .find(|column| column.name == *name)
                        .ok_or_else(|| {
                            TensorError::InvalidValue(format!("unknown tensor column: {name}"))
                        })
                })
                .collect(),
        }
    }

    fn validate_batch(
        &self,
        batch: &TensorBatch,
    ) -> Result<(usize, BTreeMap<String, Vec<usize>>), TensorError> {
        if batch.len() != self.config.tensors.len() {
            return invalid("every dataset tensor must be present in an append");
        }
        let rows = batch.values().next().map_or(0, Vec::len);
        if !(1..=1024).contains(&rows) {
            return invalid("append must contain 1 to 1024 samples");
        }
        let mut decoded = 0;
        let mut sizes = BTreeMap::new();
        for column in &self.config.tensors {
            let tensors = batch.get(&column.name).ok_or_else(|| {
                TensorError::InvalidValue(format!("missing tensor column: {}", column.name))
            })?;
            if tensors.len() != rows {
                return invalid("tensor columns must have equal sample counts");
            }
            let mut encoded = Vec::with_capacity(rows);
            for tensor in tensors {
                decoded = checked_bytes(decoded, validate_tensor(column, tensor)?)?;
                let bytes = postcard::to_stdvec(tensor)
                    .map_err(|error| TensorError::InvalidValue(error.to_string()))?;
                if bytes.len().saturating_add(FRAME_BYTES) > self.config.max_chunk_bytes {
                    return invalid("one tensor sample exceeds max_chunk_bytes");
                }
                encoded.push(bytes.len());
            }
            sizes.insert(column.name.clone(), encoded);
        }
        Ok((rows, sizes))
    }

    fn append_ops(
        &self,
        batch: &TensorBatch,
        sizes: &BTreeMap<String, Vec<usize>>,
        start: u64,
        rows: usize,
    ) -> Result<Vec<Op>, TensorError> {
        let mut ops = Vec::new();
        let mut offset = 0;
        while offset < rows {
            let chunk_start = start
                .checked_add(u64::try_from(offset).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    TensorError::InvalidValue("chunk sample index overflow".to_owned())
                })?;
            let start_bytes = postcard::to_stdvec(&chunk_start)
                .map_err(|error| TensorError::InvalidValue(error.to_string()))?
                .len();
            let mut totals = BTreeMap::<&str, usize>::new();
            let mut count = 0;
            let mut candidate = offset;
            while candidate < rows.min(offset + self.config.chunk_samples) {
                let next_count = candidate - offset + 1;
                let count_bytes = postcard::to_stdvec(&next_count)
                    .map_err(|error| TensorError::InvalidValue(error.to_string()))?
                    .len();
                let fits = self.config.tensors.iter().all(|column| {
                    totals
                        .get(column.name.as_str())
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(sizes[&column.name][candidate])
                        .saturating_add(start_bytes + count_bytes + FRAME_BYTES)
                        <= self.config.max_chunk_bytes
                });
                if !fits {
                    break;
                }
                for column in &self.config.tensors {
                    *totals.entry(&column.name).or_default() += sizes[&column.name][candidate];
                }
                count = next_count;
                candidate += 1;
            }
            if count == 0 {
                return invalid("one tensor sample plus its chunk header exceeds max_chunk_bytes");
            }
            for column in &self.config.tensors {
                let value = frame(&ChunkWrite {
                    start: chunk_start,
                    samples: &batch[&column.name][offset..offset + count],
                })?;
                if value.len() > self.config.max_chunk_bytes {
                    return invalid("encoded chunk exceeds max_chunk_bytes");
                }
                ops.push(put(
                    TENSOR_CHUNKS_TABLE,
                    chunk_key(&self.config.name, &column.name, chunk_start),
                    value,
                ));
            }
            for position in 0..count {
                let sample = chunk_start + u64::try_from(position).unwrap_or(u64::MAX);
                ops.push(put(
                    TENSOR_SAMPLE_INDEX_TABLE,
                    sample_key(&self.config.name, sample),
                    frame(&SampleIndex {
                        chunk_start,
                        position: u32::try_from(position).map_err(|_| {
                            TensorError::InvalidValue("chunk position overflow".to_owned())
                        })?,
                    })?,
                ));
            }
            offset += count;
        }
        Ok(ops)
    }
}

fn validate_tensor(column: &TensorColumn, tensor: &Tensor) -> Result<usize, TensorError> {
    if tensor.shape.len() != column.shape.len()
        || tensor.shape.len() > 8
        || tensor
            .shape
            .iter()
            .zip(&column.shape)
            .any(|(actual, expected)| expected.is_some_and(|expected| *actual != expected))
    {
        return invalid("tensor shape does not match its column");
    }
    let elements = shape_elements(tensor.shape.iter().copied())?;
    let (dtype, len, finite) = match &tensor.data {
        TensorData::U8(values) => (TensorDType::U8, values.len(), true),
        TensorData::I64(values) => (TensorDType::I64, values.len(), true),
        TensorData::F32(values) => (
            TensorDType::F32,
            values.len(),
            values.iter().all(|value| value.is_finite()),
        ),
        TensorData::F64(values) => (
            TensorDType::F64,
            values.len(),
            values.iter().all(|value| value.is_finite()),
        ),
        TensorData::Bool(values) => (TensorDType::Bool, values.len(), true),
        TensorData::Utf8(values) => (TensorDType::Utf8, values.len(), true),
    };
    if dtype != column.dtype || len != elements || !finite {
        return invalid(
            "tensor dtype, element count or finite-value constraint does not match its schema",
        );
    }
    tensor_decoded_bytes(tensor)
}

fn tensor_decoded_bytes(tensor: &Tensor) -> Result<usize, TensorError> {
    let data = match &tensor.data {
        TensorData::U8(values) => Some(values.len()),
        TensorData::Bool(values) => Some(values.len()),
        TensorData::I64(values) => values.len().checked_mul(8),
        TensorData::F32(values) => values.len().checked_mul(4),
        TensorData::F64(values) => values.len().checked_mul(8),
        TensorData::Utf8(values) => values
            .len()
            .checked_mul(std::mem::size_of::<String>())
            .and_then(|base| {
                values
                    .iter()
                    .try_fold(base, |total, value| total.checked_add(value.len()))
            }),
    }
    .ok_or_else(|| TensorError::InvalidValue("decoded tensor size overflows usize".to_owned()))?;
    data.checked_add(
        tensor.shape.len() * std::mem::size_of::<usize>() + std::mem::size_of::<Tensor>(),
    )
    .ok_or_else(|| TensorError::InvalidValue("decoded tensor size overflows usize".to_owned()))
}

fn shape_elements(shape: impl IntoIterator<Item = usize>) -> Result<usize, TensorError> {
    shape.into_iter().try_fold(1_usize, |total, dimension| {
        if dimension > MAX_ELEMENTS {
            return invalid("tensor dimension exceeds 4000000");
        }
        let total = total.checked_mul(dimension).ok_or_else(|| {
            TensorError::InvalidValue("tensor shape product overflows usize".to_owned())
        })?;
        if total > MAX_ELEMENTS {
            return invalid("tensor shape exceeds 4000000 elements");
        }
        Ok(total)
    })
}

fn checked_bytes(total: usize, next: usize) -> Result<usize, TensorError> {
    let total = total.checked_add(next).ok_or_else(|| {
        TensorError::InvalidValue("decoded byte count overflows usize".to_owned())
    })?;
    if total > MAX_DECODED_BYTES {
        return invalid("decoded tensor batch exceeds 64 MiB");
    }
    Ok(total)
}

fn frame<T: Serialize>(value: &T) -> Result<Bytes, TensorError> {
    let payload =
        postcard::to_stdvec(value).map_err(|error| TensorError::Corruption(error.to_string()))?;
    let len = u32::try_from(payload.len())
        .map_err(|_| TensorError::InvalidValue("binary payload is too large".to_owned()))?;
    let mut bytes = Vec::with_capacity(FRAME_BYTES + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn unframe<T: DeserializeOwned + BinaryRecord>(bytes: &[u8]) -> Result<T, TensorError> {
    if bytes.len() < FRAME_BYTES || &bytes[..5] != MAGIC || bytes.len() > MAX_ENCODED_BYTES {
        return corrupt("invalid binary tensor frame");
    }
    let len = u32::from_le_bytes(
        bytes[5..9]
            .try_into()
            .map_err(|_| TensorError::Corruption("missing binary length".to_owned()))?,
    );
    let checksum = u32::from_le_bytes(
        bytes[9..13]
            .try_into()
            .map_err(|_| TensorError::Corruption("missing checksum".to_owned()))?,
    );
    let payload = &bytes[FRAME_BYTES..];
    if usize::try_from(len).ok() != Some(payload.len()) || checksum != crc32fast::hash(payload) {
        return corrupt("binary tensor length or checksum mismatch");
    }
    // Validate every length and the decoded footprint without allocating. Derived
    // serde Vec visitors must never see an unchecked count from a stored frame.
    let mut cursor = WireCursor {
        bytes: payload,
        offset: 0,
        decoded_bytes: 0,
    };
    T::preflight(&mut cursor)?;
    if cursor.offset != payload.len() {
        return corrupt("binary tensor record has trailing bytes");
    }
    let (value, remainder) = postcard::take_from_bytes(payload)
        .map_err(|error| TensorError::Corruption(error.to_string()))?;
    if !remainder.is_empty() {
        return corrupt("binary tensor record has trailing bytes");
    }
    Ok(value)
}

/// The postcard wire grammar used by the four persisted record types. The
/// preflight parser borrows input only and checks counts before any allocations.
trait BinaryRecord {
    fn preflight(cursor: &mut WireCursor<'_>) -> Result<(), TensorError>;
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
    decoded_bytes: usize,
}

impl<'a> WireCursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], TensorError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| TensorError::Corruption("binary length overflow".to_owned()))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| TensorError::Corruption("truncated binary tensor record".to_owned()))?;
        self.offset = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, TensorError> {
        Ok(self.take(1)?[0])
    }

    fn varint(&mut self) -> Result<u64, TensorError> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = self.byte()?;
            if index == 9 && byte > 1 {
                return corrupt("binary integer overflow");
            }
            value |= u64::from(byte & 127) << (index * 7);
            if byte & 128 == 0 {
                if index > 0 && byte == 0 {
                    return corrupt("noncanonical binary integer");
                }
                return Ok(value);
            }
        }
        corrupt("binary integer overflow")
    }

    fn length(&mut self, max: usize) -> Result<usize, TensorError> {
        let length = usize::try_from(self.varint()?)
            .map_err(|_| TensorError::Corruption("binary length exceeds usize".to_owned()))?;
        if length > max {
            return corrupt("binary collection length exceeds its limit");
        }
        Ok(length)
    }

    fn charge(&mut self, count: usize, width: usize) -> Result<(), TensorError> {
        let bytes = count
            .checked_mul(width)
            .and_then(|bytes| self.decoded_bytes.checked_add(bytes))
            .ok_or_else(|| TensorError::Corruption("decoded binary size overflow".to_owned()))?;
        if bytes > MAX_DECODED_BYTES {
            return corrupt("decoded binary tensor footprint exceeds 64 MiB");
        }
        self.decoded_bytes = bytes;
        Ok(())
    }

    fn string(&mut self, max: usize) -> Result<&'a str, TensorError> {
        let length = self.length(max)?;
        self.charge(length, 1)?;
        std::str::from_utf8(self.take(length)?)
            .map_err(|error| TensorError::Corruption(error.to_string()))
    }

    fn name(&mut self) -> Result<(), TensorError> {
        valid_name(self.string(128)?).map_err(|error| TensorError::Corruption(error.to_string()))
    }

    fn tensor(&mut self) -> Result<(), TensorError> {
        self.charge(1, std::mem::size_of::<Tensor>())?;
        let rank = self.length(8)?;
        self.charge(rank, std::mem::size_of::<usize>())?;
        let mut elements = 1_usize;
        for _ in 0..rank {
            let dimension = self.length(MAX_ELEMENTS)?;
            elements = elements
                .checked_mul(dimension)
                .filter(|elements| *elements <= MAX_ELEMENTS)
                .ok_or_else(|| {
                    TensorError::Corruption("binary tensor shape exceeds its limit".to_owned())
                })?;
        }
        let dtype = self.length(5)?;
        let count = self.length(MAX_ELEMENTS)?;
        if count != elements {
            return corrupt("binary tensor element count differs from its shape");
        }
        match dtype {
            0 => {
                self.charge(count, 1)?;
                self.take(count)?;
            }
            1 => {
                self.charge(count, std::mem::size_of::<i64>())?;
                for _ in 0..count {
                    self.varint()?;
                }
            }
            2 => {
                self.charge(count, std::mem::size_of::<f32>())?;
                let bytes = self.take(count.checked_mul(4).ok_or_else(|| {
                    TensorError::Corruption("binary float count overflow".to_owned())
                })?)?;
                if bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .any(|bytes| !f32::from_le_bytes(*bytes).is_finite())
                {
                    return corrupt("non-finite binary tensor value");
                }
            }
            3 => {
                self.charge(count, std::mem::size_of::<f64>())?;
                let bytes = self.take(count.checked_mul(8).ok_or_else(|| {
                    TensorError::Corruption("binary float count overflow".to_owned())
                })?)?;
                if bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .any(|bytes| !f64::from_le_bytes(*bytes).is_finite())
                {
                    return corrupt("non-finite binary tensor value");
                }
            }
            4 => {
                self.charge(count, 1)?;
                if self.take(count)?.iter().any(|byte| *byte > 1) {
                    return corrupt("invalid binary boolean");
                }
            }
            5 => {
                // Empty strings are still String objects. Check this multiplication
                // before iterating or letting serde reserve the vector capacity.
                self.charge(count, std::mem::size_of::<String>())?;
                for _ in 0..count {
                    self.string(MAX_ENCODED_BYTES)?;
                }
            }
            _ => return corrupt("unknown binary tensor dtype"),
        }
        Ok(())
    }

    fn config(&mut self) -> Result<(), TensorError> {
        self.charge(1, std::mem::size_of::<TensorDatasetInfo>())?;
        self.name()?;
        let columns = self.length(64)?;
        if columns == 0 {
            return corrupt("binary tensor config has no columns");
        }
        self.charge(columns, std::mem::size_of::<TensorColumn>())?;
        for _ in 0..columns {
            self.name()?;
            self.length(5)?;
            let rank = self.length(8)?;
            self.charge(rank, std::mem::size_of::<Option<usize>>())?;
            let mut elements = 1_usize;
            for _ in 0..rank {
                let dimension = match self.byte()? {
                    0 => 1,
                    1 => self.length(MAX_ELEMENTS)?,
                    _ => return corrupt("invalid binary shape option"),
                };
                elements = elements
                    .checked_mul(dimension)
                    .filter(|elements| *elements <= MAX_ELEMENTS)
                    .ok_or_else(|| {
                        TensorError::Corruption("binary schema shape exceeds its limit".to_owned())
                    })?;
            }
        }
        let chunk_samples = self.length(1024)?;
        let chunk_bytes = self.length(MAX_ENCODED_BYTES)?;
        if chunk_samples == 0 || chunk_bytes < 1024 {
            return corrupt("invalid binary chunk limits");
        }
        Ok(())
    }
}

impl BinaryRecord for Tensor {
    fn preflight(cursor: &mut WireCursor<'_>) -> Result<(), TensorError> {
        cursor.tensor()
    }
}

impl BinaryRecord for StoredChunk {
    fn preflight(cursor: &mut WireCursor<'_>) -> Result<(), TensorError> {
        cursor.varint()?;
        let count = cursor.length(1024)?;
        if count == 0 {
            return corrupt("binary chunk has no samples");
        }
        for _ in 0..count {
            cursor.tensor()?;
        }
        Ok(())
    }
}

impl BinaryRecord for SampleIndex {
    fn preflight(cursor: &mut WireCursor<'_>) -> Result<(), TensorError> {
        cursor.varint()?;
        cursor.length(1023)?;
        Ok(())
    }
}

impl BinaryRecord for TensorDatasetInfo {
    fn preflight(cursor: &mut WireCursor<'_>) -> Result<(), TensorError> {
        cursor.config()?;
        cursor.varint()?;
        cursor.varint()?;
        Ok(())
    }
}

fn prefix(dataset: &str) -> Bytes {
    let mut key = Vec::new();
    keyenc::push_len_bytes(&mut key, dataset.as_bytes());
    key
}
fn sample_key(dataset: &str, sample: u64) -> Bytes {
    let mut key = prefix(dataset);
    key.extend_from_slice(&sample.to_be_bytes());
    key
}
fn revision_key(dataset: &str, revision: u64) -> Bytes {
    sample_key(dataset, revision)
}
fn chunk_key(dataset: &str, tensor: &str, sample: u64) -> Bytes {
    let mut key = prefix(dataset);
    keyenc::push_len_bytes(&mut key, tensor.as_bytes());
    key.extend_from_slice(&sample.to_be_bytes());
    key
}
fn put(table: &str, key: Bytes, value: Bytes) -> Op {
    Op::Put {
        table: table.to_owned(),
        key,
        value,
    }
}

fn exact_distance(
    query: &[f64],
    values: &[f64],
    metric: VectorMetric,
) -> Result<Option<f64>, TensorError> {
    let distance = match metric {
        VectorMetric::L2 => query
            .iter()
            .zip(values)
            .fold(0.0_f64, |distance, (left, right)| {
                distance.hypot(left - right)
            }),
        VectorMetric::Cosine => {
            let q_scale = query
                .iter()
                .fold(0.0_f64, |max, value| max.max(value.abs()));
            let v_scale = values
                .iter()
                .fold(0.0_f64, |max, value| max.max(value.abs()));
            if v_scale == 0.0 {
                return Ok(None);
            }
            let (dot, q_norm, v_norm) = query.iter().zip(values).fold(
                (0.0, 0.0, 0.0),
                |(dot, q_norm, v_norm), (left, right)| {
                    let left = left / q_scale;
                    let right = right / v_scale;
                    (
                        dot + left * right,
                        q_norm + left * left,
                        v_norm + right * right,
                    )
                },
            );
            (1.0 - dot / (q_norm.sqrt() * v_norm.sqrt())).clamp(0.0, 2.0)
        }
    };
    if !distance.is_finite() {
        return invalid("distance cannot be represented as a finite f64");
    }
    Ok(Some(distance))
}

fn valid_name(name: &str) -> Result<(), TensorError> {
    if name.trim().is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
        return config_error(
            "tensor names must be nonempty, at most 128 bytes and have no control characters",
        );
    }
    Ok(())
}
fn config_error<T>(message: impl Into<String>) -> Result<T, TensorError> {
    Err(TensorError::Config(message.into()))
}
fn invalid<T>(message: impl Into<String>) -> Result<T, TensorError> {
    Err(TensorError::InvalidValue(message.into()))
}
fn corrupt<T>(message: impl Into<String>) -> Result<T, TensorError> {
    Err(TensorError::Corruption(message.into()))
}
const fn default_chunk_samples() -> usize {
    64
}
const fn default_chunk_bytes() -> usize {
    4 * 1024 * 1024
}
const fn default_read_limit() -> usize {
    100
}
const fn default_search_k() -> usize {
    10
}
const fn default_max_samples() -> usize {
    10_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, DbConfig, Profile, create_database, open_database};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn config(name: &str) -> TensorDatasetConfig {
        let mut config = TensorDatasetConfig::new(
            name,
            vec![
                TensorColumn::new("image", TensorDType::U8, vec![None, Some(2)]),
                TensorColumn::new("count", TensorDType::I64, vec![]),
                TensorColumn::new("embedding", TensorDType::F32, vec![Some(2)]),
                TensorColumn::new("precise", TensorDType::F64, vec![Some(1)]),
                TensorColumn::new("flags", TensorDType::Bool, vec![Some(2)]),
                TensorColumn::new("text", TensorDType::Utf8, vec![None]),
            ],
        );
        config.chunk_samples = 1;
        config
    }

    fn tensor(shape: &[usize], data: TensorData) -> Tensor {
        Tensor {
            shape: shape.to_vec(),
            data,
        }
    }

    fn batch() -> TensorBatch {
        BTreeMap::from([
            (
                "image".to_owned(),
                vec![
                    tensor(&[1, 2], TensorData::U8(vec![1, 2])),
                    tensor(&[2, 2], TensorData::U8(vec![3, 4, 5, 6])),
                ],
            ),
            (
                "count".to_owned(),
                vec![
                    tensor(&[], TensorData::I64(vec![10])),
                    tensor(&[], TensorData::I64(vec![20])),
                ],
            ),
            (
                "embedding".to_owned(),
                vec![
                    tensor(&[2], TensorData::F32(vec![1.0, 0.0])),
                    tensor(&[2], TensorData::F32(vec![0.0, 1.0])),
                ],
            ),
            (
                "precise".to_owned(),
                vec![
                    tensor(&[1], TensorData::F64(vec![f64::MAX])),
                    tensor(&[1], TensorData::F64(vec![f64::MIN_POSITIVE])),
                ],
            ),
            (
                "flags".to_owned(),
                vec![
                    tensor(&[2], TensorData::Bool(vec![true, false])),
                    tensor(&[2], TensorData::Bool(vec![false, true])),
                ],
            ),
            (
                "text".to_owned(),
                vec![
                    tensor(&[1], TensorData::Utf8(vec!["hello".to_owned()])),
                    tensor(
                        &[2],
                        TensorData::Utf8(vec!["żółw".to_owned(), "😀".to_owned()]),
                    ),
                ],
            ),
        ])
    }

    fn create(
        database: &Database,
        config: TensorDatasetConfig,
    ) -> Result<TensorDataset<'_, Database>, TensorError> {
        database.propose_batch(TensorDataset::<Database>::create_ops(&config)?)?;
        TensorDataset::new(database, config)
    }

    #[test]
    fn typed_ragged_tensors_round_trip_and_revisions_keep_immutable_chunks() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let dataset = create(&database, config("training"))?;
        let values = batch();
        assert_eq!(dataset.info(None)?.samples, 0);
        assert_eq!(
            dataset.append(&values)?,
            TensorAppendResult {
                start: 0,
                end: 2,
                revision: 1
            }
        );
        assert_eq!(dataset.read(&TensorReadRequest::default())?.tensors, values);
        let chunks = database.range(TENSOR_CHUNKS_TABLE, &[], &[], ReadConsistency::Strong)?;
        assert_eq!(chunks.len(), 12);
        assert!(chunks.iter().all(|(_, value)| value.starts_with(MAGIC)));
        let next = values
            .iter()
            .map(|(name, rows)| (name.clone(), vec![rows[0].clone()]))
            .collect();
        dataset.append(&next)?;
        assert_eq!(dataset.info(None)?.samples, 3);
        assert_eq!(dataset.info(None)?.revision, 2);
        for (key, bytes) in chunks {
            assert_eq!(
                database.read(TENSOR_CHUNKS_TABLE, &key, ReadConsistency::Strong)?,
                Some(bytes)
            );
        }
        let old = dataset.read(&TensorReadRequest {
            revision: Some(1),
            ..TensorReadRequest::default()
        })?;
        assert_eq!(old.total_samples, 2);
        assert_eq!(old.tensors, values);
        assert_eq!(dataset.info(Some(0))?.samples, 0);
        assert!(matches!(
            dataset.info(Some(99)),
            Err(TensorError::MissingRevision(99))
        ));
        Ok(())
    }

    #[test]
    fn selected_column_reads_skip_unselected_and_preceding_corrupt_chunks() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let dataset = create(&database, config("training"))?;
        dataset.append(&batch())?;
        let selected = TensorReadRequest {
            start: 1,
            limit: 1,
            columns: Some(vec!["embedding".to_owned()]),
            revision: Some(1),
        };
        let expected = dataset.read(&selected)?;
        database.propose_batch(vec![
            put(
                TENSOR_CHUNKS_TABLE,
                chunk_key("training", "image", 1),
                b"broken unselected column".to_vec(),
            ),
            put(
                TENSOR_CHUNKS_TABLE,
                chunk_key("training", "embedding", 0),
                b"broken earlier chunk".to_vec(),
            ),
            put(
                TENSOR_SAMPLE_INDEX_TABLE,
                sample_key("training", 0),
                b"broken earlier index".to_vec(),
            ),
        ])?;
        assert_eq!(dataset.read(&selected)?, expected);
        assert!(matches!(
            dataset.read(&TensorReadRequest::new(0, 1)),
            Err(TensorError::Corruption(_))
        ));
        assert!(matches!(
            dataset.read(&TensorReadRequest {
                columns: Some(vec!["image".to_owned()]),
                ..selected
            }),
            Err(TensorError::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn append_validation_is_atomic_for_alignment_shapes_types_and_nonfinite_values() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let dataset = create(&database, config("training"))?;
        let mut invalid_batches = Vec::new();
        let mut value = batch();
        value.remove("image");
        invalid_batches.push(value);
        let mut value = batch();
        value.get_mut("count").ok_or("missing count")?.pop();
        invalid_batches.push(value);
        let mut value = batch();
        value.insert(
            "embedding".to_owned(),
            vec![tensor(&[2], TensorData::F32(vec![f32::NAN, 1.0])); 2],
        );
        invalid_batches.push(value);
        let mut value = batch();
        value.insert(
            "embedding".to_owned(),
            vec![tensor(&[3], TensorData::F32(vec![1.0, 2.0, 3.0])); 2],
        );
        invalid_batches.push(value);
        let mut value = batch();
        value.insert(
            "count".to_owned(),
            vec![tensor(&[], TensorData::F64(vec![1.0])); 2],
        );
        invalid_batches.push(value);
        for value in invalid_batches {
            assert!(dataset.append(&value).is_err());
            assert_eq!(dataset.info(None)?.samples, 0);
            assert_eq!(dataset.info(None)?.revision, 0);
            assert_eq!(
                database.range(TENSOR_CHUNKS_TABLE, &[], &[], ReadConsistency::Strong)?,
                Vec::new()
            );
            assert_eq!(
                database.range(TENSOR_SAMPLE_INDEX_TABLE, &[], &[], ReadConsistency::Strong)?,
                Vec::new()
            );
        }
        dataset.append(&batch())?;
        assert_eq!(dataset.info(None)?.samples, 2);
        Ok(())
    }

    #[test]
    fn concurrent_appends_do_not_overwrite_samples_or_revisions() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let config = TensorDatasetConfig::new(
            "events",
            vec![TensorColumn::new("event", TensorDType::I64, vec![])],
        );
        let dataset = create(&database, config.clone())?;
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                barrier.wait();
                dataset.append(&BTreeMap::from([(
                    "event".to_owned(),
                    vec![tensor(&[], TensorData::I64(vec![1]))],
                )]))
            });
            let second = scope.spawn(|| {
                barrier.wait();
                let handle = TensorDataset::new(&database, config)?;
                handle.append(&BTreeMap::from([(
                    "event".to_owned(),
                    vec![tensor(&[], TensorData::I64(vec![2]))],
                )]))
            });
            first.join().map_err(|_| "first append panicked")??;
            second.join().map_err(|_| "second append panicked")??;
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        assert_eq!(dataset.info(None)?.samples, 2);
        assert_eq!(dataset.info(None)?.revision, 2);
        assert_eq!(dataset.info(Some(1))?.samples, 1);
        let mut values = dataset.read(&TensorReadRequest::default())?.tensors["event"]
            .iter()
            .map(|value| match &value.data {
                TensorData::I64(values) => values[0],
                _ => 0,
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn chunks_respect_both_sample_and_encoded_byte_limits() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let mut config = TensorDatasetConfig::new(
            "images",
            vec![TensorColumn::new("pixels", TensorDType::U8, vec![None])],
        );
        config.max_chunk_bytes = 1024;
        config.chunk_samples = 64;
        let dataset = create(&database, config)?;
        dataset.append(&BTreeMap::from([(
            "pixels".to_owned(),
            vec![tensor(&[600], TensorData::U8(vec![7; 600])); 3],
        )]))?;
        let chunks = database.range(TENSOR_CHUNKS_TABLE, &[], &[], ReadConsistency::Strong)?;
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|(_, bytes)| bytes.len() <= 1024));
        assert!(
            dataset
                .append(&BTreeMap::from([(
                    "pixels".to_owned(),
                    vec![tensor(&[1024], TensorData::U8(vec![7; 1024]))]
                )]))
                .is_err()
        );
        assert_eq!(dataset.info(None)?.samples, 3);
        Ok(())
    }

    #[test]
    fn binary_framing_rejects_corruption_and_shapes_use_checked_products() -> TestResult {
        let value = tensor(&[2], TensorData::F32(vec![1.0, 2.0]));
        let mut bytes = frame(&value)?;
        assert_eq!(unframe::<Tensor>(&bytes)?, value);
        let last = bytes.last_mut().ok_or("empty tensor frame")?;
        *last ^= 1;
        assert!(matches!(
            unframe::<Tensor>(&bytes),
            Err(TensorError::Corruption(_))
        ));
        assert!(matches!(
            unframe::<Tensor>(b"{}"),
            Err(TensorError::Corruption(_))
        ));
        let mut config = config("invalid");
        config.tensors[0].shape = vec![Some(usize::MAX), Some(2)];
        assert!(config.validate().is_err());
        config.tensors[0].shape = vec![None, None];
        assert!(
            validate_tensor(
                &config.tensors[0],
                &tensor(&[0, usize::MAX], TensorData::U8(Vec::new()))
            )
            .is_err()
        );
        config.tensors[0].shape = vec![Some(2); 9];
        assert!(config.validate().is_err());
        Ok(())
    }

    fn raw_frame(payload: &[u8]) -> Result<Bytes, TensorError> {
        let mut frame = MAGIC.to_vec();
        frame.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| TensorError::InvalidValue("test payload too large".to_owned()))?
                .to_le_bytes(),
        );
        frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        frame.extend_from_slice(payload);
        Ok(frame)
    }

    #[test]
    fn binary_preflight_rejects_allocation_bombs_before_serde_decoding() -> TestResult {
        // This is a complete, valid postcard Tensor: four million empty Utf8
        // strings occupy about 4 MiB on wire but require at least 96 MiB of
        // String objects. CRC and frame length are valid. Reject the count before
        // serde can reserve or grow that vector, rather than checking afterwards.
        let mut payload = postcard::to_stdvec(&vec![MAX_ELEMENTS])?;
        payload.extend_from_slice(&postcard::to_stdvec(&5_u32)?);
        payload.extend_from_slice(&postcard::to_stdvec(&MAX_ELEMENTS)?);
        payload.resize(payload.len() + MAX_ELEMENTS, 0);
        let bytes = raw_frame(&payload)?;
        let error = unframe::<Tensor>(&bytes)
            .err()
            .ok_or("allocation bomb was accepted")?;
        assert!(
            matches!(error, TensorError::Corruption(ref message) if message.contains("footprint"))
        );

        // A chunk's row count is checked before looking at any Tensor records.
        let mut payload = postcard::to_stdvec(&0_u64)?;
        payload.extend_from_slice(&postcard::to_stdvec(&MAX_ELEMENTS)?);
        assert!(
            matches!(unframe::<StoredChunk>(&raw_frame(&payload)?), Err(TensorError::Corruption(message)) if message.contains("collection length"))
        );

        // Manifests use their own small schema/name/rank limits as well.
        let mut payload = postcard::to_stdvec("dataset")?;
        payload.extend_from_slice(&postcard::to_stdvec(&usize::MAX)?);
        assert!(
            matches!(unframe::<TensorDatasetInfo>(&raw_frame(&payload)?), Err(TensorError::Corruption(message)) if message.contains("collection length"))
        );
        Ok(())
    }

    #[test]
    fn binary_frames_reject_valid_crc_records_with_trailing_payload_bytes() -> TestResult {
        let tensor = tensor(&[1], TensorData::I64(vec![-1]));
        let mut payload = postcard::to_stdvec(&tensor)?;
        payload.push(0);
        assert!(
            matches!(unframe::<Tensor>(&raw_frame(&payload)?), Err(TensorError::Corruption(message)) if message.contains("trailing bytes"))
        );
        Ok(())
    }

    #[test]
    fn exact_reference_search_is_bounded_and_reports_skipped_zero_vectors() -> TestResult {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let dataset = create(
            &database,
            TensorDatasetConfig::new(
                "vectors",
                vec![TensorColumn::new(
                    "embedding",
                    TensorDType::F32,
                    vec![Some(2)],
                )],
            ),
        )?;
        dataset.append(&BTreeMap::from([(
            "embedding".to_owned(),
            vec![
                tensor(&[2], TensorData::F32(vec![1.0, 0.0])),
                tensor(&[2], TensorData::F32(vec![0.0, 1.0])),
                tensor(&[2], TensorData::F32(vec![0.0, 0.0])),
                tensor(&[2], TensorData::F32(vec![-1.0, 0.0])),
            ],
        )]))?;
        let mut request = TensorSearchRequest::new("embedding", vec![1.0, 0.0]);
        request.max_samples = 2;
        let result = dataset.search(&request)?;
        assert_eq!(
            result.hits.iter().map(|hit| hit.index).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(result.scanned_samples, 2);
        assert_eq!(result.next_start, Some(2));
        assert_eq!(result.execution.device, TensorDevice::Cpu);
        request.start = 2;
        let result = dataset.search(&request)?;
        assert_eq!(result.skipped_samples, 1);
        assert_eq!(
            result.hits,
            vec![TensorSearchHit {
                index: 3,
                distance: 2.0
            }]
        );
        assert_eq!(result.next_start, None);
        request.query = vec![0.0, 0.0];
        assert!(dataset.search(&request).is_err());
        request.metric = VectorMetric::L2;
        assert_eq!(dataset.search(&request)?.hits[0].index, 2);
        assert_eq!(
            exact_distance(&[f64::MAX, 0.0], &[f64::MAX, 0.0], VectorMetric::Cosine)?,
            Some(0.0)
        );
        assert_eq!(
            exact_distance(
                &[f64::from_bits(1), 0.0],
                &[10.0, 0.0],
                VectorMetric::Cosine
            )?,
            Some(0.0)
        );
        Ok(())
    }

    #[test]
    fn typed_append_capability_is_bound_to_values_keys_and_conditions() {
        let original = ConditionalBatch::new(
            vec![WriteCondition::KeyMissing {
                table: TENSOR_CHUNKS_TABLE.to_owned(),
                key: b"immutable".to_vec(),
            }],
            vec![Op::Put {
                table: TENSOR_CHUNKS_TABLE.to_owned(),
                key: b"immutable".to_vec(),
                value: vec![1, 2],
            }],
        );
        let authorization = super::tensor_write_authorization(&original);
        assert!(authorization.validate(&original).is_ok());
        let mut forged = original.clone();
        if let Op::Put { value, .. } = &mut forged.ops[0] {
            value.push(3);
        }
        assert!(authorization.validate(&forged).is_err());
        let mut forged = original.clone();
        if let Op::Put { key, .. } = &mut forged.ops[0] {
            key.push(0);
        }
        assert!(authorization.validate(&forged).is_err());
        let mut forged = original.clone();
        forged.conditions.clear();
        assert!(authorization.validate(&forged).is_err());
    }

    #[test]
    fn typed_binary_datasets_reopen_with_revision_snapshots() -> TestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("tensor-storage.redb");
        {
            let database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
            let dataset = create(&database, config("training"))?;
            dataset.append(&batch())?;
        }
        let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
        let dataset = TensorDataset::new(&database, config("training"))?;
        assert_eq!(dataset.info(None)?.samples, 2);
        assert_eq!(dataset.info(None)?.revision, 1);
        assert_eq!(
            dataset
                .read(&TensorReadRequest {
                    revision: Some(1),
                    ..TensorReadRequest::default()
                })?
                .tensors,
            batch()
        );
        assert_eq!(dataset.info(Some(0))?.samples, 0);
        Ok(())
    }
}
