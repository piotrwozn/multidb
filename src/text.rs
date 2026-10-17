use std::collections::{BTreeMap, BTreeSet};

use crate::{
    cdc::{self, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedTarget, ResumeToken},
    db::CatalogEntry,
    keyenc,
    model::{
        CollectionId, DOCUMENT_TABLE, DocumentId, FieldPath, Value, decode_value, extract_path,
    },
    repl::{Op, ReadConsistency, ReplError, Replication, propose_system_batch},
    storage::{Bytes, StorageError},
};

pub const FULL_TEXT_POSTINGS_TABLE: &str = "full_text_postings";
pub const FULL_TEXT_DOCS_TABLE: &str = "full_text_docs";
pub const FULL_TEXT_META_TABLE: &str = "__full_text_meta";

const EMPTY: &[u8] = b"";
/// Source documents read per page, and indexed documents buffered per batch, while
/// rebuilding an index.
const REFRESH_PAGE_ROWS: usize = 256;
/// Index writes buffered before a rebuild flushes a batch.
const REFRESH_PAGE_OPS: usize = 4_096;
/// Index keys deleted per batch while clearing an index.
const CLEAR_PAGE_KEYS: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TextSource {
    Collection {
        collection_id: CollectionId,
        path: FieldPath,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FullTextIndexConfig {
    pub name: String,
    pub source: TextSource,
    pub language: String,
    /// Retained for compatibility: index entries are maintained synchronously with
    /// document writes made through `Database::collection` handles.
    pub refresh_lag_target: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TextHit {
    pub id: DocumentId,
    pub score: f64,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FullTextIndexState {
    pub refreshed_to: ResumeToken,
    /// Number of indexed documents at the last rebuild, backfill or incremental refresh.
    pub indexed_documents: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct IndexedDoc {
    text: String,
    terms: Vec<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum TextError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("cdc: {0}")]
    Cdc(#[from] cdc::FeedError),

    #[error("unsupported text source")]
    UnsupportedSource,

    #[error("invalid text config: {0}")]
    InvalidConfig(String),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

pub struct FullTextIndex<'repl, R: Replication + ?Sized> {
    repl: &'repl R,
    config: FullTextIndexConfig,
}

impl FullTextIndexConfig {
    #[must_use]
    pub fn collection(
        name: impl Into<String>,
        collection_id: CollectionId,
        path: FieldPath,
    ) -> Self {
        Self {
            name: name.into(),
            source: TextSource::Collection {
                collection_id,
                path,
            },
            language: "simple".to_owned(),
            refresh_lag_target: 1,
        }
    }
}

impl<'repl, R: Replication + ?Sized> FullTextIndex<'repl, R> {
    /// Opens an index handle and validates its config.
    /// # Errors
    /// Fails when the index name is empty.
    pub fn new(repl: &'repl R, config: FullTextIndexConfig) -> Result<Self, TextError> {
        validate_config(&config)?;
        Ok(Self { repl, config })
    }

    #[must_use]
    pub const fn config(&self) -> &FullTextIndexConfig {
        &self.config
    }

    /// Persists index metadata.
    /// # Errors
    /// Fails when replication or serialization fails.
    pub fn create_metadata(&self) -> Result<(), TextError> {
        propose_system_batch(self.repl, metadata_ops(&self.config)?)?;
        Ok(())
    }

    /// Builds system metadata writes for atomic catalog DDL.
    /// # Errors
    /// Fails when metadata cannot be serialized.
    pub fn metadata_ops(config: &FullTextIndexConfig) -> Result<Vec<Op>, TextError> {
        metadata_ops(config)
    }

    /// Builds system metadata writes that publish `state` together with the config.
    /// # Errors
    /// Fails when metadata cannot be serialized.
    pub(crate) fn metadata_ops_with_state(
        config: &FullTextIndexConfig,
        state: &FullTextIndexState,
    ) -> Result<Vec<Op>, TextError> {
        metadata_ops_with_state(config, state)
    }

    /// Rebuilds the derived index from the source documents and records the refresh state.
    ///
    /// This is a repair tool: document writes made through `Database::collection` handles
    /// already maintain the index in their own batch. The rebuild clears and rewrites the
    /// index in bounded batches, so readers can see partial results while it runs. The
    /// resume token is taken before the scan, so `refresh_incremental` replays writes that
    /// land during the rebuild.
    /// # Errors
    /// Fails when source documents cannot be read or decoded, or writes fail.
    pub fn refresh_full(&self) -> Result<FullTextIndexState, TextError> {
        let state = self.rebuild_entries()?;
        propose_system_batch(
            self.repl,
            vec![put_json_op(
                FULL_TEXT_META_TABLE,
                meta_state_key(&self.config.name),
                &state,
            )?],
        )?;
        Ok(state)
    }

    /// Clears and rebuilds the index entries in bounded batches without writing the
    /// refresh state.
    /// # Errors
    /// Fails when source documents cannot be read or decoded, or writes fail.
    pub(crate) fn rebuild_entries(&self) -> Result<FullTextIndexState, TextError> {
        let TextSource::Collection {
            collection_id,
            path,
        } = &self.config.source;
        // Take the token before scanning: a write that commits during the scan is then
        // replayed by `refresh_incremental` instead of being lost.
        let refreshed_to = cdc::current_resume_token(self.repl)?;
        clear_index_entries(self.repl, &self.config.name)?;

        let (mut cursor, end) = keyenc::u32_prefix_range(collection_id.as_u32());
        let mut ops = Vec::new();
        let mut buffered_docs = 0_usize;
        let mut indexed = 0_usize;
        loop {
            let page = scan_page(self.repl, DOCUMENT_TABLE, &cursor, &end, REFRESH_PAGE_ROWS)?;
            for (key, value) in &page {
                let Some(id) = document_id_from_key(key) else {
                    continue;
                };
                let Some(text) = document_text(value, path)? else {
                    continue;
                };
                append_index_doc_ops(&self.config.name, id, &text, &mut ops)?;
                indexed += 1;
                buffered_docs += 1;
                if buffered_docs >= REFRESH_PAGE_ROWS || ops.len() >= REFRESH_PAGE_OPS {
                    propose_system_batch(self.repl, std::mem::take(&mut ops))?;
                    buffered_docs = 0;
                }
            }
            let Some(next) = next_page_start(&page, REFRESH_PAGE_ROWS, &end) else {
                break;
            };
            cursor = next;
        }
        if !ops.is_empty() {
            propose_system_batch(self.repl, ops)?;
        }
        Ok(FullTextIndexState {
            refreshed_to,
            indexed_documents: indexed,
        })
    }

    /// Replays committed changes after the stored LSN and updates the derived index.
    ///
    /// This is a catch-up/repair tool for raw-key writes that bypass `Database::collection`
    /// handles (stored procedures, raw batches). Changes to the same document within one
    /// changefeed page are coalesced, so only its final version is indexed.
    /// # Errors
    /// Fails when CDC or storage fails.
    pub fn refresh_incremental(
        &self,
        catalog: &BTreeMap<String, CatalogEntry>,
    ) -> Result<FullTextIndexState, TextError> {
        let TextSource::Collection {
            collection_id,
            path,
        } = &self.config.source;
        let Some(collection_name) = collection_name_for(catalog, *collection_id) else {
            return self.refresh_full();
        };

        let mut state = self.state()?.unwrap_or(FullTextIndexState {
            refreshed_to: ResumeToken::default(),
            indexed_documents: 0,
        });
        let target_lsn = cdc::current_resume_token(self.repl)?.lsn;
        while state.refreshed_to.lsn < target_lsn {
            let filter = ChangefeedFilter {
                target: ChangefeedTarget::Collection(collection_name.clone()),
            };
            let (events, next) = cdc::poll_changefeed(
                self.repl,
                catalog,
                &state.refreshed_to,
                &filter,
                &ChangefeedOptions::default(),
                1_024,
            )?;
            if next.lsn == state.refreshed_to.lsn {
                break;
            }

            let mut ops = Vec::new();
            for (id, value_after) in coalesce_document_changes(events) {
                remove_existing_doc_ops(self.repl, &self.config.name, id, &mut ops)?;
                if let Some(value_after) = value_after
                    && let Some(text) = document_text(&value_after, path)?
                {
                    append_index_doc_ops(&self.config.name, id, &text, &mut ops)?;
                }
            }
            state.refreshed_to = next;
            state.indexed_documents = count_indexed_after_ops(self.repl, &self.config.name, &ops)?;
            ops.push(put_json_op(
                FULL_TEXT_META_TABLE,
                meta_state_key(&self.config.name),
                &state,
            )?);
            propose_system_batch(self.repl, ops)?;
        }
        Ok(state)
    }

    /// Searches with a small BM25-style scorer.
    /// # Errors
    /// Fails when index entries cannot be read.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<TextHit>, TextError> {
        let terms = tokenize(query);
        if terms.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let doc_count = count_docs(self.repl, &self.config.name)?;
        let avg_len = avg_doc_len(self.repl, &self.config.name)?;
        let mut scores: BTreeMap<DocumentId, (f64, String)> = BTreeMap::new();

        for term in terms {
            let posting_prefix = posting_prefix(&self.config.name, &term);
            let posting_end = keyenc::range_end(&posting_prefix);
            let postings = self.repl.range(
                FULL_TEXT_POSTINGS_TABLE,
                &posting_prefix,
                &posting_end,
                ReadConsistency::Strong,
            )?;
            let df = postings.len();
            if df == 0 {
                continue;
            }
            let idf = bm25_idf(doc_count, df);
            for (key, _) in postings {
                let Some(id) = posting_doc_id(&key) else {
                    continue;
                };
                let Some(doc) = self.read_doc(id)? else {
                    continue;
                };
                let tf = doc
                    .terms
                    .iter()
                    .filter(|candidate| *candidate == &term)
                    .count();
                if tf == 0 {
                    // A posting whose document no longer contains the term is stale.
                    continue;
                }
                let score = bm25_score(tf, doc.terms.len(), avg_len, idf);
                let entry = scores.entry(id).or_insert((0.0, doc.text));
                entry.0 += score;
            }
        }

        let mut hits = scores
            .into_iter()
            .map(|(id, (score, text))| TextHit { id, score, text })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    /// Reads persisted refresh state.
    /// # Errors
    /// Fails when metadata cannot be decoded.
    pub fn state(&self) -> Result<Option<FullTextIndexState>, TextError> {
        read_json(
            self.repl,
            FULL_TEXT_META_TABLE,
            &meta_state_key(&self.config.name),
        )
    }

    fn read_doc(&self, id: DocumentId) -> Result<Option<IndexedDoc>, TextError> {
        read_json(
            self.repl,
            FULL_TEXT_DOCS_TABLE,
            &doc_key(&self.config.name, id),
        )
    }
}

fn validate_config(config: &FullTextIndexConfig) -> Result<(), TextError> {
    if config.name.is_empty() {
        return Err(TextError::InvalidConfig(
            "index name cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn metadata_ops(config: &FullTextIndexConfig) -> Result<Vec<Op>, TextError> {
    let state = FullTextIndexState {
        refreshed_to: ResumeToken::default(),
        indexed_documents: 0,
    };
    metadata_ops_with_state(config, &state)
}

fn metadata_ops_with_state(
    config: &FullTextIndexConfig,
    state: &FullTextIndexState,
) -> Result<Vec<Op>, TextError> {
    Ok(vec![
        put_json_op(FULL_TEXT_META_TABLE, meta_config_key(&config.name), config)?,
        put_json_op(FULL_TEXT_META_TABLE, meta_state_key(&config.name), state)?,
    ])
}

/// Appends the index writes for one document change.
///
/// The writes are a diff between the `old` and `new` document values, so they can be
/// queued in the same batch as the document write itself. Nothing is emitted when the
/// indexed text is unchanged.
/// # Errors
/// Fails when the indexed document entry cannot be serialized.
pub(crate) fn append_document_change_ops(
    config: &FullTextIndexConfig,
    id: DocumentId,
    old: Option<&Value>,
    new: Option<&Value>,
    ops: &mut Vec<Op>,
) -> Result<(), TextError> {
    let TextSource::Collection { path, .. } = &config.source;
    let old_text = old.and_then(|doc| value_text(doc, path));
    let new_text = new.and_then(|doc| value_text(doc, path));
    if old_text == new_text {
        return Ok(());
    }

    let new_terms = new_text.map_or_else(BTreeSet::new, unique_terms);
    if let Some(old_text) = old_text {
        for term in unique_terms(old_text).difference(&new_terms) {
            ops.push(Op::Delete {
                table: FULL_TEXT_POSTINGS_TABLE.to_owned(),
                key: posting_key(&config.name, term, id),
            });
        }
    }
    match new_text {
        Some(text) => append_index_doc_ops(&config.name, id, text, ops)?,
        None => ops.push(Op::Delete {
            table: FULL_TEXT_DOCS_TABLE.to_owned(),
            key: doc_key(&config.name, id),
        }),
    }
    Ok(())
}

/// Keeps the final version of each document changed in one changefeed page; `None`
/// marks a delete.
fn coalesce_document_changes(
    events: impl IntoIterator<Item = cdc::ChangeEvent>,
) -> BTreeMap<DocumentId, Option<Bytes>> {
    let mut changes = BTreeMap::new();
    for event in events {
        match event.op {
            ChangeOp::Upsert { key, value_after } => {
                if let Some(id) = document_id_from_key(&key) {
                    changes.insert(id, Some(value_after));
                }
            }
            ChangeOp::Delete { key } => {
                if let Some(id) = document_id_from_key(&key) {
                    changes.insert(id, None);
                }
            }
            ChangeOp::TxBegin | ChangeOp::TxCommit | ChangeOp::Ddl { .. } => {}
        }
    }
    changes
}

/// Deletes every entry of `index` in bounded batches.
fn clear_index_entries<R: Replication + ?Sized>(repl: &R, index: &str) -> Result<(), TextError> {
    let prefix = index_prefix(index);
    let end = keyenc::range_end(&prefix);
    for table in [FULL_TEXT_DOCS_TABLE, FULL_TEXT_POSTINGS_TABLE] {
        let mut cursor = prefix.clone();
        loop {
            let page = scan_page(repl, table, &cursor, &end, CLEAR_PAGE_KEYS)?;
            let next = next_page_start(&page, CLEAR_PAGE_KEYS, &end);
            let ops = page
                .into_iter()
                .map(|(key, _)| Op::Delete {
                    table: table.to_owned(),
                    key,
                })
                .collect::<Vec<_>>();
            if !ops.is_empty() {
                propose_system_batch(repl, ops)?;
            }
            let Some(next) = next else {
                break;
            };
            cursor = next;
        }
    }
    Ok(())
}

/// Reads at most `rows` entries starting at `start`.
fn scan_page<R: Replication + ?Sized>(
    repl: &R,
    table: &str,
    start: &[u8],
    end: &[u8],
    rows: usize,
) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    let mut page = Vec::new();
    repl.scan_range_batches(
        table,
        start,
        end,
        ReadConsistency::Strong,
        rows,
        &|| false,
        &mut |batch| {
            page.extend_from_slice(batch);
            Ok(false)
        },
    )?;
    Ok(page)
}

/// Returns the first key after a full page, or `None` when the range is exhausted.
fn next_page_start(page: &[(Bytes, Bytes)], rows: usize, end: &[u8]) -> Option<Bytes> {
    if page.len() < rows {
        return None;
    }
    let (last, _) = page.last()?;
    let mut next = last.clone();
    next.push(0);
    (end.is_empty() || next.as_slice() < end).then_some(next)
}

fn count_indexed_after_ops<R: Replication + ?Sized>(
    repl: &R,
    index: &str,
    ops: &[Op],
) -> Result<usize, TextError> {
    let prefix = index_prefix(index);
    let end = keyenc::range_end(&prefix);
    let mut docs = repl
        .range(FULL_TEXT_DOCS_TABLE, &prefix, &end, ReadConsistency::Strong)?
        .into_iter()
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    for op in ops {
        match op {
            Op::Put { table, key, .. } if table == FULL_TEXT_DOCS_TABLE => {
                docs.insert(key.clone());
            }
            Op::Delete { table, key } if table == FULL_TEXT_DOCS_TABLE => {
                docs.remove(key);
            }
            _ => {}
        }
    }
    Ok(docs.len())
}

fn collection_name_for(
    catalog: &BTreeMap<String, CatalogEntry>,
    collection_id: CollectionId,
) -> Option<String> {
    catalog.iter().find_map(|(name, entry)| match entry {
        CatalogEntry::Collection {
            collection_id: candidate,
            ..
        } if *candidate == collection_id => Some(name.clone()),
        _ => None,
    })
}

fn document_text(bytes: &[u8], path: &FieldPath) -> Result<Option<String>, TextError> {
    let value = decode_value(bytes)?;
    Ok(value_text(&value, path).map(str::to_owned))
}

fn value_text<'doc>(doc: &'doc Value, path: &FieldPath) -> Option<&'doc str> {
    match extract_path(doc, path) {
        Some(Value::Str(text)) if !text.trim().is_empty() => Some(text),
        _ => None,
    }
}

fn unique_terms(text: &str) -> BTreeSet<String> {
    tokenize(text).into_iter().collect()
}

fn append_index_doc_ops(
    index: &str,
    id: DocumentId,
    text: &str,
    ops: &mut Vec<Op>,
) -> Result<(), TextError> {
    let mut unique = BTreeSet::new();
    let terms = tokenize(text);
    for term in &terms {
        unique.insert(term.clone());
    }
    let doc = IndexedDoc {
        text: text.to_owned(),
        terms,
    };
    ops.push(put_json_op(FULL_TEXT_DOCS_TABLE, doc_key(index, id), &doc)?);
    for term in unique {
        ops.push(Op::Put {
            table: FULL_TEXT_POSTINGS_TABLE.to_owned(),
            key: posting_key(index, &term, id),
            value: EMPTY.to_vec(),
        });
    }
    Ok(())
}

fn remove_existing_doc_ops<R: Replication + ?Sized>(
    repl: &R,
    index: &str,
    id: DocumentId,
    ops: &mut Vec<Op>,
) -> Result<(), TextError> {
    let Some(doc) = read_json::<IndexedDoc, _>(repl, FULL_TEXT_DOCS_TABLE, &doc_key(index, id))?
    else {
        return Ok(());
    };
    let mut unique = BTreeSet::new();
    for term in doc.terms {
        unique.insert(term);
    }
    for term in unique {
        ops.push(Op::Delete {
            table: FULL_TEXT_POSTINGS_TABLE.to_owned(),
            key: posting_key(index, &term, id),
        });
    }
    ops.push(Op::Delete {
        table: FULL_TEXT_DOCS_TABLE.to_owned(),
        key: doc_key(index, id),
    });
    Ok(())
}

fn count_docs<R: Replication + ?Sized>(repl: &R, index: &str) -> Result<usize, TextError> {
    let prefix = index_prefix(index);
    let end = keyenc::range_end(&prefix);
    Ok(repl
        .range(FULL_TEXT_DOCS_TABLE, &prefix, &end, ReadConsistency::Strong)?
        .len())
}

fn avg_doc_len<R: Replication + ?Sized>(repl: &R, index: &str) -> Result<f64, TextError> {
    let prefix = index_prefix(index);
    let end = keyenc::range_end(&prefix);
    let rows = repl.range(FULL_TEXT_DOCS_TABLE, &prefix, &end, ReadConsistency::Strong)?;
    if rows.is_empty() {
        return Ok(1.0);
    }
    let mut total = 0_usize;
    let mut count = 0_usize;
    for (_, bytes) in rows {
        let doc: IndexedDoc =
            serde_json::from_slice(&bytes).map_err(|error| TextError::Serde(error.to_string()))?;
        total += doc.terms.len();
        count += 1;
    }
    let total = total.to_string().parse::<f64>().unwrap_or(f64::INFINITY);
    let count = count.to_string().parse::<f64>().unwrap_or(1.0).max(1.0);
    Ok((total / count).max(1.0))
}

fn bm25_idf(doc_count: usize, df: usize) -> f64 {
    let n = doc_count.to_string().parse::<f64>().unwrap_or(0.0);
    let df = df.to_string().parse::<f64>().unwrap_or(1.0);
    (((n - df + 0.5) / (df + 0.5)) + 1.0).ln().max(0.0)
}

fn bm25_score(tf: usize, doc_len: usize, avg_len: f64, idf: f64) -> f64 {
    let tf = tf.to_string().parse::<f64>().unwrap_or(0.0);
    let doc_len = doc_len.to_string().parse::<f64>().unwrap_or(0.0);
    let k1 = 1.2;
    let b = 0.75;
    if tf <= 0.0 {
        return 0.0;
    }
    idf * ((tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avg_len)))
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter_map(|part| {
            let token = part.trim().to_lowercase();
            (!token.is_empty()).then_some(token)
        })
        .collect()
}

fn put_json_op<T: serde::Serialize>(table: &str, key: Bytes, value: &T) -> Result<Op, TextError> {
    Ok(Op::Put {
        table: table.to_owned(),
        key,
        value: serde_json::to_vec(value).map_err(|error| TextError::Serde(error.to_string()))?,
    })
}

fn read_json<T: serde::de::DeserializeOwned, R: Replication + ?Sized>(
    repl: &R,
    table: &str,
    key: &[u8],
) -> Result<Option<T>, TextError> {
    let Some(bytes) = repl.read(table, key, ReadConsistency::Strong)? else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::from_slice(&bytes).map_err(|error| TextError::Serde(error.to_string()))?,
    ))
}

fn index_prefix(index: &str) -> Bytes {
    let mut key = Vec::new();
    keyenc::push_len_bytes(&mut key, index.as_bytes());
    key
}

fn doc_key(index: &str, id: DocumentId) -> Bytes {
    let mut key = index_prefix(index);
    key.extend_from_slice(&id.as_bytes());
    key
}

fn posting_prefix(index: &str, term: &str) -> Bytes {
    let mut key = index_prefix(index);
    keyenc::push_len_bytes(&mut key, term.as_bytes());
    key
}

fn posting_key(index: &str, term: &str, id: DocumentId) -> Bytes {
    let mut key = posting_prefix(index, term);
    key.extend_from_slice(&id.as_bytes());
    key
}

fn posting_doc_id(key: &[u8]) -> Option<DocumentId> {
    if key.len() < 16 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&key[key.len() - 16..]);
    Some(DocumentId::from_bytes(bytes))
}

fn document_id_from_key(key: &[u8]) -> Option<DocumentId> {
    if key.len() != 20 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&key[4..20]);
    Some(DocumentId::from_bytes(bytes))
}

fn meta_config_key(index: &str) -> Bytes {
    let mut key = b"config:".to_vec();
    key.extend_from_slice(index.as_bytes());
    key
}

fn meta_state_key(index: &str) -> Bytes {
    let mut key = b"state:".to_vec();
    key.extend_from_slice(index.as_bytes());
    key
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    use crate::{
        db::{Database, DbConfig, Profile, create_database},
        geo::{GEO_INDEX_TABLE, GEO_POINTS_TABLE, GeoIndexConfig},
        keyenc,
        model::{CollectionId, DocumentCollection, DocumentId, FieldPath, Value},
        query::{ColumnType, DocField},
        repl::{Op, ReadConsistency, Replication},
    };

    use super::{
        FULL_TEXT_DOCS_TABLE, FULL_TEXT_POSTINGS_TABLE, FullTextIndex, FullTextIndexConfig,
        IndexedDoc, TextHit, doc_key, index_prefix, posting_key,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn full_text_refresh_and_bm25_search() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let docs = DocumentCollection::new(repl.as_ref(), crate::model::CollectionId::new(7));
        let rust = docs.insert(&doc("Rust database storage engine"))?;
        docs.insert(&doc("Cooking with herbs"))?;
        let index = FullTextIndex::new(
            repl.as_ref(),
            FullTextIndexConfig::collection(
                "posts_text",
                crate::model::CollectionId::new(7),
                FieldPath::new(["body"]),
            ),
        )?;
        index.create_metadata()?;
        let state = index.refresh_full()?;
        assert_eq!(state.indexed_documents, 2);
        let hits = index.search("rust database", 5)?;
        assert_eq!(hits.first().map(|hit| hit.id), Some(rust));
        docs.delete(rust)?;
        let state = index.refresh_full()?;
        assert_eq!(state.indexed_documents, 1);
        assert!(
            index.search("rust database", 5)?.is_empty(),
            "expected an empty value, got {:?}",
            index.search("rust database", 5)?
        );
        Ok(())
    }

    #[test]
    fn full_text_index_tracks_document_writes_without_refresh() -> TestResult {
        let mut database = posts_database()?;
        let a = database
            .collection("posts")?
            .insert(&doc("rust database"))?;
        database.create_full_text_index(posts_text_config())?;
        let index = database.full_text_index("posts_text")?;
        assert_eq!(hit_ids(&index.search("rust", 5)?), BTreeSet::from([a]));

        let posts = database.collection("posts")?;
        let b = posts.insert(&doc("rust tokio"))?;
        for body in ["go compiler", "zig allocator", "rust parser"] {
            posts.update(a, &doc(body))?;
        }
        for term in ["database", "go", "zig"] {
            assert!(index.search(term, 5)?.is_empty(), "{term}");
        }
        assert_eq!(hit_ids(&index.search("parser", 5)?), BTreeSet::from([a]));
        assert_eq!(hit_ids(&index.search("rust", 5)?), BTreeSet::from([a, b]));
        assert_eq!(postings_for(&database, a)?, 2);

        posts.delete(a)?;
        assert_eq!(postings_for(&database, a)?, 0);
        assert!(!has_doc_entry(&database, a)?);
        assert_eq!(hit_ids(&index.search("rust", 5)?), BTreeSet::from([b]));
        Ok(())
    }

    #[test]
    fn transaction_rewrites_do_not_leave_ghost_postings() -> TestResult {
        let mut database = posts_database()?;
        let a = database.collection("posts")?.insert(&doc("seed text"))?;
        database.create_full_text_index(posts_text_config())?;

        database.transaction(|txn| {
            txn.update_document("posts", a, &doc("alpha beta"))?;
            txn.update_document("posts", a, &doc("gamma"))
        })?;
        let index = database.full_text_index("posts_text")?;
        for term in ["seed", "alpha", "beta"] {
            assert!(index.search(term, 5)?.is_empty(), "{term}");
        }
        assert_eq!(hit_ids(&index.search("gamma", 5)?), BTreeSet::from([a]));
        assert_eq!(postings_for(&database, a)?, 1);

        let c = database.transaction(|txn| {
            let c = txn.insert_document("posts", &doc("delta epsilon"))?;
            txn.update_document("posts", c, &doc("delta epsilon"))?;
            txn.delete_document("posts", c)?;
            Ok(c)
        })?;
        assert_eq!(postings_for(&database, c)?, 0);
        assert!(!has_doc_entry(&database, c)?);
        assert!(index.search("delta", 5)?.is_empty());

        database.transaction(|txn| txn.delete_document("posts", a))?;
        assert_eq!(postings_for(&database, a)?, 0);
        assert!(!has_doc_entry(&database, a)?);
        assert!(index.search("gamma", 5)?.is_empty());
        Ok(())
    }

    #[test]
    fn update_without_indexed_change_emits_no_derived_ops() -> TestResult {
        let mut database = posts_database()?;
        database.create_full_text_index(posts_text_config())?;
        database.create_geo_index(GeoIndexConfig::new(
            "posts_geo",
            CollectionId::new(7),
            FieldPath::new(["point"]),
        ))?;
        let posts = database.collection("posts")?;
        let id = DocumentId::generate();
        let old = post("rust database", 1);
        let ops = posts.replace_ops_with_old(id, Some(&old), &post("rust database", 2))?;
        assert!(!ops.iter().any(is_derived_op), "{ops:?}");

        let ops = posts.replace_ops_with_old(id, Some(&old), &post("rust compiler", 2))?;
        assert!(ops.iter().any(is_derived_op));
        assert!(!ops.iter().any(is_geo_op));
        assert!(!ops.iter().any(|op| is_posting_delete(op, "rust", id)));
        assert!(ops.iter().any(|op| is_posting_delete(op, "database", id)));
        Ok(())
    }

    #[test]
    fn refresh_incremental_coalesces_repeated_changes() -> TestResult {
        let mut database = posts_database()?;
        database.create_full_text_index(posts_text_config())?;
        // A bare handle has no derived indexes attached, like a raw-key writer.
        let raw = DocumentCollection::new(&database, CollectionId::new(7));
        let id = raw.insert(&doc("one two"))?;
        raw.update(id, &doc("three"))?;
        raw.update(id, &doc("four"))?;

        let index = database.full_text_index("posts_text")?;
        index.refresh_incremental(database.catalog())?;
        for term in ["one", "two", "three"] {
            assert!(index.search(term, 5)?.is_empty(), "{term}");
        }
        assert_eq!(hit_ids(&index.search("four", 5)?), BTreeSet::from([id]));
        assert_eq!(postings_for(&database, id)?, 1);

        raw.delete(id)?;
        index.refresh_incremental(database.catalog())?;
        assert_eq!(postings_for(&database, id)?, 0);
        assert!(!has_doc_entry(&database, id)?);
        Ok(())
    }

    #[test]
    fn search_ignores_postings_without_term_frequency() -> TestResult {
        let mut database = posts_database()?;
        let id = database
            .collection("posts")?
            .insert(&doc("rust database"))?;
        database.create_full_text_index(posts_text_config())?;
        Replication::propose_batch(
            &database,
            vec![Op::Put {
                table: FULL_TEXT_POSTINGS_TABLE.to_owned(),
                key: posting_key("posts_text", "ghost", id),
                value: Vec::new(),
            }],
        )?;
        let index = database.full_text_index("posts_text")?;
        assert!(index.search("ghost", 5)?.is_empty());
        assert_eq!(hit_ids(&index.search("rust", 5)?), BTreeSet::from([id]));
        Ok(())
    }

    #[test]
    fn refresh_full_rebuilds_large_collections_in_pages() -> TestResult {
        let mut database = posts_database()?;
        database.create_full_text_index(posts_text_config())?;
        {
            let raw = DocumentCollection::new(&database, CollectionId::new(7));
            for n in 0..300 {
                raw.insert(&doc(&format!("shared alpha{n} beta{n} gamma{n}")))?;
            }
        }
        let stale = DocumentId::generate();
        let stale_doc = IndexedDoc {
            text: "shared".to_owned(),
            terms: vec!["shared".to_owned()],
        };
        Replication::propose_batch(
            &database,
            vec![
                Op::Put {
                    table: FULL_TEXT_POSTINGS_TABLE.to_owned(),
                    key: posting_key("posts_text", "shared", stale),
                    value: Vec::new(),
                },
                Op::Put {
                    table: FULL_TEXT_DOCS_TABLE.to_owned(),
                    key: doc_key("posts_text", stale),
                    value: serde_json::to_vec(&stale_doc)?,
                },
            ],
        )?;

        let index = database.full_text_index("posts_text")?;
        assert_eq!(index.refresh_full()?.indexed_documents, 300);
        assert_eq!(index.search("shared", 300)?.len(), 300);
        assert_eq!(postings_for(&database, stale)?, 0);
        assert!(!has_doc_entry(&database, stale)?);

        // A second rebuild clears more postings than one clear batch holds.
        assert_eq!(index.refresh_full()?.indexed_documents, 300);
        assert_eq!(index.search("shared", 300)?.len(), 300);
        assert_eq!(index.search("gamma299", 5)?.len(), 1);
        Ok(())
    }

    fn posts_database() -> Result<Database, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_collection(
            "posts",
            CollectionId::new(7),
            vec![DocField::path(
                "body",
                FieldPath::new(["body"]),
                ColumnType::Str,
            )],
            Vec::new(),
        )?;
        Ok(database)
    }

    fn posts_text_config() -> FullTextIndexConfig {
        FullTextIndexConfig::collection(
            "posts_text",
            CollectionId::new(7),
            FieldPath::new(["body"]),
        )
    }

    fn hit_ids(hits: &[TextHit]) -> BTreeSet<DocumentId> {
        hits.iter().map(|hit| hit.id).collect()
    }

    fn postings_for(
        database: &Database,
        id: DocumentId,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let prefix = index_prefix("posts_text");
        let end = keyenc::range_end(&prefix);
        Ok(Replication::range(
            database,
            FULL_TEXT_POSTINGS_TABLE,
            &prefix,
            &end,
            ReadConsistency::Strong,
        )?
        .into_iter()
        .filter(|(key, _)| key.ends_with(&id.as_bytes()))
        .count())
    }

    fn has_doc_entry(
        database: &Database,
        id: DocumentId,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(Replication::read(
            database,
            FULL_TEXT_DOCS_TABLE,
            &doc_key("posts_text", id),
            ReadConsistency::Strong,
        )?
        .is_some())
    }

    fn is_geo_op(op: &Op) -> bool {
        let (Op::Put { table, .. } | Op::Delete { table, .. }) = op;
        table == GEO_POINTS_TABLE || table == GEO_INDEX_TABLE
    }

    fn is_derived_op(op: &Op) -> bool {
        let (Op::Put { table, .. } | Op::Delete { table, .. }) = op;
        table == FULL_TEXT_POSTINGS_TABLE || table == FULL_TEXT_DOCS_TABLE || is_geo_op(op)
    }

    fn is_posting_delete(op: &Op, term: &str, id: DocumentId) -> bool {
        matches!(
            op,
            Op::Delete { table, key }
                if table == FULL_TEXT_POSTINGS_TABLE && *key == posting_key("posts_text", term, id)
        )
    }

    fn post(body: &str, views: i64) -> Value {
        Value::Object(BTreeMap::from([
            ("body".to_owned(), Value::Str(body.to_owned())),
            (
                "point".to_owned(),
                Value::GeoPoint {
                    lon: 21.0122,
                    lat: 52.2297,
                },
            ),
            ("views".to_owned(), Value::Int(views)),
        ]))
    }

    fn doc(body: &str) -> Value {
        Value::Object(BTreeMap::from([(
            "body".to_owned(),
            Value::Str(body.to_owned()),
        )]))
    }
}
