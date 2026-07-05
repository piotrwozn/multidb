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

    /// Rebuilds the derived index from the source documents.
    /// # Errors
    /// Fails when source documents cannot be read or writes fail.
    pub fn refresh_full(&self) -> Result<FullTextIndexState, TextError> {
        let TextSource::Collection {
            collection_id,
            path,
        } = &self.config.source;
        let (prefix, end) = keyenc::u32_prefix_range(collection_id.as_u32());
        let rows = self
            .repl
            .range(DOCUMENT_TABLE, &prefix, &end, ReadConsistency::Strong)?;

        let refreshed_to = cdc::current_resume_token(self.repl)?;
        let mut ops = clear_index_ops(self.repl, &self.config.name)?;
        let mut indexed = 0_usize;
        for (key, value) in rows {
            let Some(id) = document_id_from_key(&key) else {
                continue;
            };
            if let Some(text) = document_text(&value, path)? {
                append_index_doc_ops(&self.config.name, id, &text, &mut ops)?;
                indexed += 1;
            }
        }

        let state = FullTextIndexState {
            refreshed_to,
            indexed_documents: indexed,
        };
        ops.push(put_json_op(
            FULL_TEXT_META_TABLE,
            meta_state_key(&self.config.name),
            &state,
        )?);
        if !ops.is_empty() {
            propose_system_batch(self.repl, ops)?;
        }
        Ok(state)
    }

    /// Replays committed changes after the stored LSN and updates the derived index.
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
            for event in events {
                match event.op {
                    ChangeOp::Upsert { key, value_after } => {
                        let Some(id) = document_id_from_key(&key) else {
                            continue;
                        };
                        remove_existing_doc_ops(self.repl, &self.config.name, id, &mut ops)?;
                        if let Some(text) = document_text(&value_after, path)? {
                            append_index_doc_ops(&self.config.name, id, &text, &mut ops)?;
                        }
                    }
                    ChangeOp::Delete { key } => {
                        if let Some(id) = document_id_from_key(&key) {
                            remove_existing_doc_ops(self.repl, &self.config.name, id, &mut ops)?;
                        }
                    }
                    ChangeOp::TxBegin | ChangeOp::TxCommit | ChangeOp::Ddl { .. } => {}
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
    Ok(vec![
        put_json_op(FULL_TEXT_META_TABLE, meta_config_key(&config.name), config)?,
        put_json_op(FULL_TEXT_META_TABLE, meta_state_key(&config.name), &state)?,
    ])
}

fn clear_index_ops<R: Replication + ?Sized>(repl: &R, index: &str) -> Result<Vec<Op>, TextError> {
    let mut ops = Vec::new();
    for table in [FULL_TEXT_DOCS_TABLE, FULL_TEXT_POSTINGS_TABLE] {
        let prefix = index_prefix(index);
        let end = keyenc::range_end(&prefix);
        for (key, _) in repl.range(table, &prefix, &end, ReadConsistency::Strong)? {
            ops.push(Op::Delete {
                table: table.to_owned(),
                key,
            });
        }
    }
    Ok(ops)
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
    Ok(match extract_path(&value, path) {
        Some(Value::Str(text)) if !text.trim().is_empty() => Some(text.clone()),
        _ => None,
    })
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
    use std::{collections::BTreeMap, sync::Arc};

    use crate::{
        db::{DbConfig, Profile, create_database},
        model::{DocumentCollection, FieldPath, Value},
        repl::Replication,
    };

    use super::{FullTextIndex, FullTextIndexConfig};

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
        assert!(index.search("rust database", 5)?.is_empty());
        Ok(())
    }

    fn doc(body: &str) -> Value {
        Value::Object(BTreeMap::from([(
            "body".to_owned(),
            Value::Str(body.to_owned()),
        )]))
    }
}
