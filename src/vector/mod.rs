use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use hnsw_rs::prelude::{DistCosine, DistL2, Hnsw};

use crate::{
    keyenc,
    model::{
        CollectionId, DocumentCollection, DocumentId, FieldPath, ModelError, Value, extract_path,
    },
    observability,
    repl::{Op, ReadConsistency, ReplError, Replication},
    storage::{Bytes, StorageError},
};

pub const VECTOR_TABLE: &str = "vectors";

const VECTOR_KEY_LEN: usize = 20;
const DOCUMENT_ID_LEN: usize = 16;
const F32_BYTES: usize = 4;
const HNSW_MAX_LAYER: usize = 16;
const MIN_HNSW_M: usize = 1;
const MAX_HNSW_M: usize = 255;
const TOMBSTONE_COMPACT_PERCENT: usize = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum VectorMetric {
    #[default]
    Cosine,
    L2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HnswParams {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VectorCollectionConfig {
    pub collection_id: CollectionId,
    pub dim: usize,
    pub metric: VectorMetric,
    pub hnsw: HnswParams,
    #[serde(default)]
    pub quantization: QuantizationConfig,
    #[serde(default)]
    pub disk_ann: DiskAnnConfig,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorHit {
    pub id: DocumentId,
    pub distance: f32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum QuantizationConfig {
    #[default]
    None,
    Scalar {
        bits: u8,
    },
    Product {
        segments: usize,
        bits: u8,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DiskAnnConfig {
    pub enabled: bool,
    pub cache_capacity: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum VectorFilter {
    Eq { path: FieldPath, value: Value },
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchOptions {
    pub ef_search: Option<usize>,
    pub filter: Option<VectorFilter>,
    pub rerank: usize,
}

pub struct VectorCollection<'repl, R: Replication + ?Sized> {
    repl: &'repl R,
    metadata: DocumentCollection<'repl, R>,
    config: VectorCollectionConfig,
    state: SharedVectorIndex,
}

pub(crate) type SharedVectorIndex = Arc<RwLock<VectorIndexState>>;

#[derive(thiserror::Error, Debug)]
pub enum VectorError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("model: {0}")]
    Model(#[from] ModelError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("invalid vector dimension: expected {expected}, found {found}")]
    InvalidDimension { expected: usize, found: usize },

    #[error("vector dimension must be greater than zero")]
    EmptyDimension,

    #[error("vector contains a non-finite value")]
    NonFiniteValue,

    #[error("cosine metric cannot index a zero vector")]
    ZeroVector,

    #[error("invalid HNSW parameter: {0}")]
    InvalidHnswParams(String),

    #[error("invalid vector index config: {0}")]
    InvalidIndexConfig(String),

    #[error("vector bytes are corrupt: {0}")]
    Corruption(String),

    #[error("vector index error: {0}")]
    Index(String),

    #[error("vector index lock poisoned")]
    LockPoisoned,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            ef_search: 48,
        }
    }
}

impl Default for DiskAnnConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cache_capacity: 4_096,
        }
    }
}

impl Default for VectorSearchOptions {
    fn default() -> Self {
        Self {
            ef_search: None,
            filter: None,
            rerank: 64,
        }
    }
}

impl VectorSearchOptions {
    #[must_use]
    pub fn with_filter(mut self, filter: VectorFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    #[must_use]
    pub const fn with_ef_search(mut self, ef_search: usize) -> Self {
        self.ef_search = Some(ef_search);
        self
    }

    #[must_use]
    pub const fn with_rerank(mut self, rerank: usize) -> Self {
        self.rerank = rerank;
        self
    }
}

impl HnswParams {
    #[must_use]
    pub const fn new(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            m,
            ef_construction,
            ef_search,
        }
    }
}

impl VectorCollectionConfig {
    #[must_use]
    pub fn new(collection_id: CollectionId, dim: usize) -> Self {
        Self {
            collection_id,
            dim,
            metric: VectorMetric::default(),
            hnsw: HnswParams::default(),
            quantization: QuantizationConfig::default(),
            disk_ann: DiskAnnConfig::default(),
        }
    }

    #[must_use]
    pub const fn with_metric(mut self, metric: VectorMetric) -> Self {
        self.metric = metric;
        self
    }

    #[must_use]
    pub const fn with_hnsw(mut self, hnsw: HnswParams) -> Self {
        self.hnsw = hnsw;
        self
    }

    #[must_use]
    pub fn with_quantization(mut self, quantization: QuantizationConfig) -> Self {
        self.quantization = quantization;
        self
    }

    #[must_use]
    pub fn with_disk_ann(mut self, disk_ann: DiskAnnConfig) -> Self {
        self.disk_ann = disk_ann;
        self
    }
}

impl<'repl, R: Replication + ?Sized> VectorCollection<'repl, R> {
    /// Opens a vector collection and rebuilds its in-memory HNSW index from storage.
    /// # Errors
    /// Fails when config validation, storage reads, or vector decoding fails.
    pub fn new(repl: &'repl R, config: VectorCollectionConfig) -> Result<Self, VectorError> {
        validate_config(&config)?;
        let state = Arc::new(RwLock::new(VectorIndexState::new(
            config.metric,
            config.hnsw,
            0,
        )));
        let collection = Self {
            repl,
            metadata: DocumentCollection::new(repl, config.collection_id),
            config,
            state,
        };
        collection.rebuild_index()?;
        Ok(collection)
    }

    /// Opens a collection with a shared in-memory index state.
    /// # Errors
    /// Fails when config validation, storage reads, or vector decoding fails.
    pub(crate) fn with_shared_state(
        repl: &'repl R,
        config: VectorCollectionConfig,
        state: SharedVectorIndex,
    ) -> Result<Self, VectorError> {
        validate_config(&config)?;
        let collection = Self {
            repl,
            metadata: DocumentCollection::new(repl, config.collection_id),
            config,
            state,
        };
        collection.ensure_index_loaded()?;
        Ok(collection)
    }

    #[must_use]
    pub const fn config(&self) -> &VectorCollectionConfig {
        &self.config
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.config.collection_id
    }

    /// Inserts a vector and metadata document atomically.
    /// # Errors
    /// Fails when the vector is invalid or replication rejects the batch.
    pub fn insert_vector(
        &self,
        metadata: &Value,
        vector: Vec<f32>,
    ) -> Result<DocumentId, VectorError> {
        validate_vector(&self.config, &vector)?;

        let id = DocumentId::generate();
        let mut ops = self.metadata.put_ops_for_id(id, metadata)?;
        ops.push(put_vector_op(self.config.collection_id, id, &vector));
        self.repl.propose_batch(ops)?;
        self.insert_in_memory(id, vector)?;
        Ok(id)
    }

    /// Reads metadata associated with a vector id.
    /// # Errors
    /// Fails when replication, storage, or metadata decoding fails.
    pub fn get_metadata(&self, id: DocumentId) -> Result<Option<Value>, VectorError> {
        Ok(self.metadata.get(id)?)
    }

    /// Searches nearest vectors using the configured `ef_search`.
    /// # Errors
    /// Fails when the query vector is invalid or the index lock is poisoned.
    pub fn knn(&self, query: &[f32], k: usize) -> Result<Vec<VectorHit>, VectorError> {
        self.knn_with_ef(query, k, self.config.hnsw.ef_search)
    }

    /// Searches nearest vectors with a per-query HNSW `ef_search`.
    /// # Errors
    /// Fails when the query vector is invalid or the index lock is poisoned.
    pub fn knn_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<VectorHit>, VectorError> {
        self.knn_with_options(
            query,
            k,
            VectorSearchOptions::default().with_ef_search(ef_search),
        )
    }

    /// Searches nearest vectors with filtering/rerank options.
    /// # Errors
    /// Fails when the query vector is invalid, metadata cannot be read, or the index lock is poisoned.
    #[allow(clippy::needless_pass_by_value)]
    pub fn knn_with_options(
        &self,
        query: &[f32],
        k: usize,
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorHit>, VectorError> {
        if k == 0 {
            return Ok(Vec::new());
        }

        validate_query(&self.config, query)?;
        let ef_search = options.ef_search.unwrap_or(self.config.hnsw.ef_search);
        if let Some(filter) = &options.filter {
            return self.filtered_exact_knn(query, k, filter);
        }
        if self.config.disk_ann.enabled {
            return self.disk_ann_style_knn(query, k, ef_search, options.rerank);
        }
        let state = self.state.read().map_err(|_| VectorError::LockPoisoned)?;
        Ok(state.search(query, k, ef_search))
    }

    /// Deletes a vector and its metadata document atomically.
    /// # Errors
    /// Fails when replication or metadata reads fail.
    pub fn delete_vector(&self, id: DocumentId) -> Result<(), VectorError> {
        let mut ops = self.metadata.delete_ops(id)?;
        ops.push(Op::Delete {
            table: VECTOR_TABLE.to_owned(),
            key: vector_key(self.config.collection_id, id),
        });
        self.repl.propose_batch(ops)?;

        let mut state = self.state.write().map_err(|_| VectorError::LockPoisoned)?;
        state.delete(id);
        let should_compact = state.should_compact();
        drop(state);
        if should_compact {
            self.rebuild_index()?;
        }
        Ok(())
    }

    /// Rebuilds the in-memory HNSW index from durable vector rows.
    /// # Errors
    /// Fails when storage reads or vector decoding fails.
    pub fn rebuild_index(&self) -> Result<(), VectorError> {
        let items = self.read_persisted_vectors()?;
        let mut rebuilt = VectorIndexState::new(self.config.metric, self.config.hnsw, items.len());
        rebuilt.rebuild(items)?;

        let mut state = self.state.write().map_err(|_| VectorError::LockPoisoned)?;
        *state = rebuilt;
        state.loaded = true;
        Ok(())
    }

    fn ensure_index_loaded(&self) -> Result<(), VectorError> {
        if self
            .state
            .read()
            .map_err(|_| VectorError::LockPoisoned)?
            .loaded
        {
            return Ok(());
        }
        self.rebuild_index()
    }

    fn read_persisted_vectors(&self) -> Result<Vec<(DocumentId, Vec<f32>)>, VectorError> {
        let (start, end) = vector_range_bounds(self.config.collection_id);
        self.repl
            .range(VECTOR_TABLE, &start, &end, ReadConsistency::Strong)?
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    document_id_from_vector_key(&key)?,
                    decode_vector(&value, self.config.dim)?,
                ))
            })
            .collect()
    }

    fn insert_in_memory(&self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorError> {
        let needs_rebuild = self
            .state
            .read()
            .map_err(|_| VectorError::LockPoisoned)?
            .needs_resize_after_insert();
        if needs_rebuild {
            return self.rebuild_index();
        }

        let mut state = self.state.write().map_err(|_| VectorError::LockPoisoned)?;
        state.insert(id, vector)
    }

    fn filtered_exact_knn(
        &self,
        query: &[f32],
        k: usize,
        filter: &VectorFilter,
    ) -> Result<Vec<VectorHit>, VectorError> {
        let hits = {
            let state = self.state.read().map_err(|_| VectorError::LockPoisoned)?;
            state.exact_hits(query)
        };
        let mut filtered = Vec::new();
        for hit in hits {
            if self
                .get_metadata(hit.id)?
                .as_ref()
                .is_some_and(|metadata| vector_filter_matches(metadata, filter))
            {
                filtered.push(hit);
                if filtered.len() == k {
                    break;
                }
            }
        }
        Ok(filtered)
    }

    fn disk_ann_style_knn(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        rerank: usize,
    ) -> Result<Vec<VectorHit>, VectorError> {
        let candidate_count = k.max(rerank).max(ef_search);
        let mut candidates = {
            let state = self.state.read().map_err(|_| VectorError::LockPoisoned)?;
            state.search(query, candidate_count, ef_search.max(candidate_count))
        };
        sort_hits(&mut candidates);
        candidates.truncate(k);
        Ok(candidates)
    }
}

/// Encodes a vector as little-endian `f32` bytes.
#[must_use]
pub fn encode_vector(vector: &[f32]) -> Bytes {
    let mut bytes = Vec::with_capacity(vector.len() * F32_BYTES);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

/// Decodes fixed-width vector bytes.
/// # Errors
/// Fails when the byte length does not match the configured dimension.
pub fn decode_vector(bytes: &[u8], expected_dim: usize) -> Result<Vec<f32>, VectorError> {
    let expected_len = expected_dim
        .checked_mul(F32_BYTES)
        .ok_or_else(|| VectorError::Corruption("dimension is too large".to_owned()))?;

    if bytes.len() != expected_len {
        return Err(VectorError::InvalidDimension {
            expected: expected_dim,
            found: bytes.len() / F32_BYTES,
        });
    }

    let mut vector = Vec::with_capacity(expected_dim);
    for chunk in bytes.chunks_exact(F32_BYTES) {
        let array = <[u8; F32_BYTES]>::try_from(chunk)
            .map_err(|error| VectorError::Corruption(error.to_string()))?;
        vector.push(f32::from_le_bytes(array));
    }
    Ok(vector)
}

#[must_use]
pub fn vector_key(collection_id: CollectionId, id: DocumentId) -> Bytes {
    let mut key = Vec::with_capacity(VECTOR_KEY_LEN);
    key.extend_from_slice(&collection_id.as_u32().to_be_bytes());
    key.extend_from_slice(&id.as_bytes());
    key
}

fn put_vector_op(collection_id: CollectionId, id: DocumentId, vector: &[f32]) -> Op {
    Op::Put {
        table: VECTOR_TABLE.to_owned(),
        key: vector_key(collection_id, id),
        value: encode_vector(vector),
    }
}

fn vector_range_bounds(collection_id: CollectionId) -> (Bytes, Bytes) {
    keyenc::u32_prefix_range(collection_id.as_u32())
}

fn document_id_from_vector_key(key: &[u8]) -> Result<DocumentId, VectorError> {
    if key.len() != VECTOR_KEY_LEN {
        return Err(VectorError::Corruption(
            "vector key must be exactly 20 bytes".to_owned(),
        ));
    }

    let mut id = [0; DOCUMENT_ID_LEN];
    id.copy_from_slice(&key[4..]);
    Ok(DocumentId::from_bytes(id))
}

fn validate_config(config: &VectorCollectionConfig) -> Result<(), VectorError> {
    if config.dim == 0 {
        return Err(VectorError::EmptyDimension);
    }

    if !(MIN_HNSW_M..=MAX_HNSW_M).contains(&config.hnsw.m) {
        return Err(VectorError::InvalidHnswParams(
            "m must be between 1 and 255".to_owned(),
        ));
    }

    if config.hnsw.ef_construction < config.hnsw.m {
        return Err(VectorError::InvalidHnswParams(
            "ef_construction must be at least m".to_owned(),
        ));
    }

    if config.hnsw.ef_search == 0 {
        return Err(VectorError::InvalidHnswParams(
            "ef_search must be greater than zero".to_owned(),
        ));
    }

    match config.quantization {
        QuantizationConfig::None => {}
        QuantizationConfig::Scalar { bits } => {
            if !(4..=8).contains(&bits) {
                return Err(VectorError::InvalidIndexConfig(
                    "scalar quantization bits must be between 4 and 8".to_owned(),
                ));
            }
        }
        QuantizationConfig::Product { segments, bits } => {
            if segments == 0 || !config.dim.is_multiple_of(segments) {
                return Err(VectorError::InvalidIndexConfig(
                    "product quantization segments must evenly divide the dimension".to_owned(),
                ));
            }
            if !(4..=8).contains(&bits) {
                return Err(VectorError::InvalidIndexConfig(
                    "product quantization bits must be between 4 and 8".to_owned(),
                ));
            }
        }
    }

    if config.disk_ann.enabled && config.disk_ann.cache_capacity == 0 {
        return Err(VectorError::InvalidIndexConfig(
            "DiskANN cache capacity must be greater than zero".to_owned(),
        ));
    }

    Ok(())
}

fn validate_vector(config: &VectorCollectionConfig, vector: &[f32]) -> Result<(), VectorError> {
    if vector.len() != config.dim {
        return Err(VectorError::InvalidDimension {
            expected: config.dim,
            found: vector.len(),
        });
    }

    if vector.iter().any(|value| !value.is_finite()) {
        return Err(VectorError::NonFiniteValue);
    }

    if config.metric == VectorMetric::Cosine && is_zero_vector(vector) {
        return Err(VectorError::ZeroVector);
    }

    Ok(())
}

fn validate_query(config: &VectorCollectionConfig, query: &[f32]) -> Result<(), VectorError> {
    validate_vector(config, query)
}

fn vector_filter_matches(metadata: &Value, filter: &VectorFilter) -> bool {
    match filter {
        VectorFilter::Eq { path, value } => extract_path(metadata, path) == Some(value),
    }
}

#[must_use]
pub fn quantized_len(config: &QuantizationConfig, dim: usize) -> usize {
    match config {
        QuantizationConfig::None => dim.saturating_mul(F32_BYTES),
        QuantizationConfig::Scalar { .. } => dim,
        QuantizationConfig::Product { segments, .. } => *segments,
    }
}

#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn scalar_quantize(vector: &[f32]) -> Vec<i8> {
    if vector.is_empty() {
        return Vec::new();
    }
    let max_abs = vector
        .iter()
        .map(|value| value.abs())
        .fold(0.0_f32, f32::max)
        .max(f32::EPSILON);
    vector
        .iter()
        .map(|value| ((value / max_abs) * 127.0).round().clamp(-127.0, 127.0) as i8)
        .collect()
}

#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
pub fn product_quantize(vector: &[f32], segments: usize) -> Vec<i8> {
    if segments == 0 {
        return Vec::new();
    }
    let chunk = vector.len().div_ceil(segments);
    vector
        .chunks(chunk.max(1))
        .map(|values| {
            let mean = values.iter().sum::<f32>() / values.len() as f32;
            (mean * 16.0).round().clamp(-127.0, 127.0) as i8
        })
        .collect()
}

fn is_zero_vector(vector: &[f32]) -> bool {
    vector.iter().all(|value| *value == 0.0)
}

pub(crate) struct VectorIndexState {
    hnsw: HnswIndex,
    metric: VectorMetric,
    hnsw_params: HnswParams,
    capacity: usize,
    next_hnsw_id: usize,
    hnsw_to_doc: BTreeMap<usize, DocumentId>,
    doc_to_hnsw: BTreeMap<DocumentId, usize>,
    vectors: BTreeMap<DocumentId, Vec<f32>>,
    deleted: BTreeSet<DocumentId>,
    loaded: bool,
}

impl VectorIndexState {
    pub(crate) fn new(metric: VectorMetric, hnsw_params: HnswParams, expected_len: usize) -> Self {
        let capacity = hnsw_capacity(expected_len);
        Self {
            hnsw: HnswIndex::new(metric, hnsw_params, capacity),
            metric,
            hnsw_params,
            capacity,
            next_hnsw_id: 0,
            hnsw_to_doc: BTreeMap::new(),
            doc_to_hnsw: BTreeMap::new(),
            vectors: BTreeMap::new(),
            deleted: BTreeSet::new(),
            loaded: false,
        }
    }

    fn rebuild(&mut self, items: Vec<(DocumentId, Vec<f32>)>) -> Result<(), VectorError> {
        self.capacity = hnsw_capacity(items.len());
        self.hnsw = HnswIndex::new(self.metric, self.hnsw_params, self.capacity);
        self.next_hnsw_id = 0;
        self.hnsw_to_doc.clear();
        self.doc_to_hnsw.clear();
        self.vectors.clear();
        self.deleted.clear();

        for (id, vector) in items {
            self.insert_without_resize(id, vector)?;
        }
        self.loaded = true;

        Ok(())
    }

    fn insert(&mut self, id: DocumentId, vector: Vec<f32>) -> Result<(), VectorError> {
        if self.vectors.len().saturating_add(1) > self.capacity {
            let mut items = self
                .vectors
                .iter()
                .map(|(id, vector)| (*id, vector.clone()))
                .collect::<Vec<_>>();
            items.push((id, vector));
            return self.rebuild(items);
        }

        self.insert_without_resize(id, vector)
    }

    fn needs_resize_after_insert(&self) -> bool {
        self.vectors.len().saturating_add(1) > self.capacity
    }

    fn insert_without_resize(
        &mut self,
        id: DocumentId,
        vector: Vec<f32>,
    ) -> Result<(), VectorError> {
        let hnsw_id = self.next_hnsw_id;
        self.next_hnsw_id = self
            .next_hnsw_id
            .checked_add(1)
            .ok_or_else(|| VectorError::Index("HNSW id overflow".to_owned()))?;

        self.hnsw.insert(&vector, hnsw_id);
        self.hnsw_to_doc.insert(hnsw_id, id);
        self.doc_to_hnsw.insert(id, hnsw_id);
        self.vectors.insert(id, vector);
        self.deleted.remove(&id);
        Ok(())
    }

    fn delete(&mut self, id: DocumentId) {
        self.vectors.remove(&id);
        self.doc_to_hnsw.remove(&id);
        self.deleted.insert(id);
    }

    fn should_compact(&self) -> bool {
        let indexed = self.hnsw_to_doc.len().max(1);
        self.deleted.len().saturating_mul(100) > indexed.saturating_mul(TOMBSTONE_COMPACT_PERCENT)
    }

    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<VectorHit> {
        if self.vectors.is_empty() {
            return Vec::new();
        }

        let wanted = k.min(self.vectors.len());
        let requested = self.search_candidate_count(wanted, ef_search);
        let ef = ef_search.max(requested.saturating_add(1));
        let mut hits = BTreeMap::new();

        for (hnsw_id, _) in self.hnsw.search(query, requested, ef) {
            if let Some(id) = self.hnsw_to_doc.get(&hnsw_id) {
                if self.deleted.contains(id) {
                    continue;
                }

                if let Some(vector) = self.vectors.get(id) {
                    hits.insert(
                        *id,
                        VectorHit {
                            id: *id,
                            distance: distance(self.metric, query, vector),
                        },
                    );
                }
            }
        }

        let mut ordered = hits.into_values().collect::<Vec<_>>();
        if ordered.len() < wanted {
            observability::record_vector_bruteforce_fallback();
            ordered = self.exact_hits(query);
        } else {
            sort_hits(&mut ordered);
        }

        ordered.truncate(wanted);
        ordered
    }

    fn search_candidate_count(&self, wanted: usize, ef_search: usize) -> usize {
        let total_indexed = self.hnsw_to_doc.len();
        let with_deleted = wanted.saturating_add(self.deleted.len());
        total_indexed.min(wanted.max(ef_search).max(with_deleted))
    }

    fn exact_hits(&self, query: &[f32]) -> Vec<VectorHit> {
        let mut hits = self
            .vectors
            .iter()
            .map(|(id, vector)| VectorHit {
                id: *id,
                distance: distance(self.metric, query, vector),
            })
            .collect::<Vec<_>>();
        sort_hits(&mut hits);
        hits
    }
}

enum HnswIndex {
    Cosine(Hnsw<'static, f32, DistCosine>),
    L2(Hnsw<'static, f32, DistL2>),
}

impl HnswIndex {
    fn new(metric: VectorMetric, params: HnswParams, capacity: usize) -> Self {
        match metric {
            VectorMetric::Cosine => Self::Cosine(Hnsw::new(
                params.m,
                capacity,
                HNSW_MAX_LAYER,
                params.ef_construction,
                DistCosine,
            )),
            VectorMetric::L2 => Self::L2(Hnsw::new(
                params.m,
                capacity,
                HNSW_MAX_LAYER,
                params.ef_construction,
                DistL2,
            )),
        }
    }

    fn insert(&self, vector: &[f32], id: usize) {
        match self {
            Self::Cosine(index) => index.insert_slice((vector, id)),
            Self::L2(index) => index.insert_slice((vector, id)),
        }
    }

    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<(usize, f32)> {
        match self {
            Self::Cosine(index) => index
                .search(query, k, ef_search)
                .into_iter()
                .map(|neighbor| (neighbor.d_id, neighbor.distance))
                .collect(),
            Self::L2(index) => index
                .search(query, k, ef_search)
                .into_iter()
                .map(|neighbor| (neighbor.d_id, neighbor.distance))
                .collect(),
        }
    }
}

fn hnsw_capacity(len: usize) -> usize {
    match len.max(1).checked_next_power_of_two() {
        Some(capacity) => capacity,
        None => usize::MAX,
    }
}

fn distance(metric: VectorMetric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        VectorMetric::Cosine => cosine_distance(left, right),
        VectorMetric::L2 => squared_l2_distance(left, right).sqrt(),
    }
}

fn cosine_distance(left: &[f32], right: &[f32]) -> f32 {
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;

    for (left_value, right_value) in left.iter().zip(right) {
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }

    1.0 - dot / (left_norm.sqrt() * right_norm.sqrt())
}

fn squared_l2_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left_value, right_value)| {
            let delta = left_value - right_value;
            delta * delta
        })
        .sum()
}

fn sort_hits(hits: &mut [VectorHit]) {
    hits.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.id.cmp(&right.id))
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        DiskAnnConfig, HnswParams, QuantizationConfig, VectorCollection, VectorCollectionConfig,
        VectorError, VectorFilter, VectorMetric, VectorSearchOptions, decode_vector, encode_vector,
        product_quantize, quantized_len, scalar_quantize, vector_range_bounds,
    };
    use crate::{
        db::{DbConfig, Profile, create_database, open_database},
        model::{CollectionId, DocumentId, FieldPath, Value, decode_value, encode_value},
    };

    fn metadata(label: &str) -> Value {
        let mut object = BTreeMap::new();
        object.insert("label".to_owned(), Value::Str(label.to_owned()));
        Value::Object(object)
    }

    fn tagged_metadata(label: &str, tag: &str) -> Value {
        let mut object = BTreeMap::new();
        object.insert("label".to_owned(), Value::Str(label.to_owned()));
        object.insert("tag".to_owned(), Value::Str(tag.to_owned()));
        Value::Object(object)
    }

    #[test]
    fn value_vector_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let value = Value::Vector(vec![0.1, 0.2, 0.3]);
        assert_eq!(decode_value(&encode_value(&value)?)?, value);
        Ok(())
    }

    #[test]
    fn vector_bytes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let vector = vec![1.0, -2.5, 3.25];
        let encoded = encode_vector(&vector);
        assert_eq!(decode_vector(&encoded, 3)?, vector);
        Ok(())
    }

    #[test]
    fn max_collection_vector_range_is_open_ended() {
        let (start, end) = vector_range_bounds(CollectionId::new(u32::MAX));

        assert_eq!(start, vec![0xFF; 4]);
        assert!(end.is_empty());
    }

    #[test]
    fn validation_rejects_bad_vectors() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let config = VectorCollectionConfig::new(CollectionId::new(1), 3);
        let vectors = VectorCollection::new(&database, config)?;

        assert!(matches!(
            vectors.insert_vector(&metadata("bad-dim"), vec![1.0, 2.0]),
            Err(VectorError::InvalidDimension {
                expected: 3,
                found: 2
            })
        ));
        assert!(matches!(
            vectors.insert_vector(&metadata("nan"), vec![1.0, f32::NAN, 2.0]),
            Err(VectorError::NonFiniteValue)
        ));
        assert!(matches!(
            vectors.insert_vector(&metadata("zero"), vec![0.0, 0.0, 0.0]),
            Err(VectorError::ZeroVector)
        ));

        Ok(())
    }

    #[test]
    fn insert_knn_metadata_and_delete_work_in_memory() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(2), 3),
        )?;

        let first = vectors.insert_vector(&metadata("first"), vec![1.0, 0.0, 0.0])?;
        vectors.insert_vector(&metadata("second"), vec![0.0, 1.0, 0.0])?;

        let hits = vectors.knn(&[0.9, 0.1, 0.0], 1)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, first);
        assert_eq!(vectors.get_metadata(first)?, Some(metadata("first")));

        vectors.delete_vector(first)?;
        assert_eq!(vectors.get_metadata(first)?, None);
        assert_ne!(vectors.knn(&[0.9, 0.1, 0.0], 1)?[0].id, first);

        Ok(())
    }

    #[test]
    fn knn_allows_k_larger_than_dataset() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(3), 2),
        )?;

        vectors.insert_vector(&metadata("a"), vec![1.0, 0.0])?;
        vectors.insert_vector(&metadata("b"), vec![0.0, 1.0])?;

        assert_eq!(vectors.knn(&[1.0, 0.0], 10)?.len(), 2);
        Ok(())
    }

    #[test]
    fn redb_reopen_rebuilds_hnsw_from_vectors() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("vectors.redb");
        let config = DbConfig::on_disk(Profile::Vector, path);
        let collection_config = VectorCollectionConfig::new(CollectionId::new(4), 3);
        let id;

        {
            let database = create_database(config.clone())?;
            let vectors = VectorCollection::new(&database, collection_config.clone())?;
            id = vectors.insert_vector(&metadata("persisted"), vec![1.0, 0.0, 0.0])?;
        }

        let database = open_database(config)?;
        let vectors = VectorCollection::new(&database, collection_config)?;
        let hits = vectors.knn(&[1.0, 0.0, 0.0], 1)?;

        assert_eq!(hits[0].id, id);
        assert_eq!(vectors.get_metadata(id)?, Some(metadata("persisted")));
        Ok(())
    }

    #[test]
    fn cosine_and_l2_can_rank_differently() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let cosine = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(5), 2)
                .with_hnsw(HnswParams::new(8, 16, 16)),
        )?;
        let l2 = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(6), 2)
                .with_metric(VectorMetric::L2)
                .with_hnsw(HnswParams::new(8, 16, 16)),
        )?;

        let same_direction = cosine.insert_vector(&metadata("same"), vec![10.0, 0.0])?;
        let _near_cosine = cosine.insert_vector(&metadata("near"), vec![1.0, 1.0])?;
        let _same_direction_l2 = l2.insert_vector(&metadata("same"), vec![10.0, 0.0])?;
        let near_l2_id = l2.insert_vector(&metadata("near"), vec![1.0, 1.0])?;

        assert_eq!(cosine.knn(&[1.0, 0.0], 1)?[0].id, same_direction);
        assert_eq!(l2.knn(&[1.0, 0.0], 1)?[0].id, near_l2_id);

        Ok(())
    }

    #[test]
    fn l2_distance_is_euclidean_not_squared() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(8), 2).with_metric(VectorMetric::L2),
        )?;

        vectors.insert_vector(&metadata("far"), vec![3.0, 4.0])?;
        let hits = vectors.knn(&[0.0, 0.0], 1)?;

        assert!((hits[0].distance - 5.0).abs() < f32::EPSILON);
        Ok(())
    }

    #[test]
    fn filtered_knn_returns_k_matching_metadata_rows() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(9), 2).with_disk_ann(DiskAnnConfig {
                enabled: true,
                cache_capacity: 8,
            }),
        )?;

        vectors.insert_vector(&tagged_metadata("a", "keep"), vec![1.0, 0.0])?;
        vectors.insert_vector(&tagged_metadata("b", "drop"), vec![0.9, 0.1])?;
        vectors.insert_vector(&tagged_metadata("c", "keep"), vec![0.8, 0.2])?;

        let hits = vectors.knn_with_options(
            &[1.0, 0.0],
            2,
            VectorSearchOptions::default().with_filter(VectorFilter::Eq {
                path: FieldPath::new(["tag"]),
                value: Value::Str("keep".to_owned()),
            }),
        )?;

        assert_eq!(hits.len(), 2);
        for hit in hits {
            assert_eq!(
                vectors.get_metadata(hit.id)?,
                Some(tagged_metadata(
                    if hit.distance < 0.01 { "a" } else { "c" },
                    "keep"
                ))
            );
        }
        Ok(())
    }

    #[test]
    fn quantization_configs_have_compact_codes() {
        let vector = vec![0.0, 0.5, -0.5, 1.0];

        assert_eq!(
            quantized_len(&QuantizationConfig::Scalar { bits: 8 }, vector.len()),
            4
        );
        assert_eq!(
            quantized_len(
                &QuantizationConfig::Product {
                    segments: 2,
                    bits: 8,
                },
                vector.len()
            ),
            2
        );
        assert_eq!(scalar_quantize(&vector).len(), 4);
        assert_eq!(product_quantize(&vector, 2).len(), 2);
    }

    #[test]
    fn hnsw_results_match_bruteforce_on_small_dataset() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let vectors = VectorCollection::new(
            &database,
            VectorCollectionConfig::new(CollectionId::new(7), 2).with_metric(VectorMetric::L2),
        )?;
        let mut expected = Vec::new();

        for index in 1_i16..=10 {
            let value = f32::from(index);
            let id = vectors.insert_vector(&metadata("point"), vec![value, 1.0])?;
            expected.push((id, (value - 3.0).abs()));
        }

        expected.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        let hits = vectors.knn_with_ef(&[3.0, 1.0], 3, 32)?;

        assert_eq!(
            hits.iter().map(|hit| hit.id).collect::<Vec<DocumentId>>(),
            expected
                .into_iter()
                .take(3)
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
