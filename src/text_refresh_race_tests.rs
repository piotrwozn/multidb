use super::*;
use crate::{
    db::{Database, DbConfig, Profile, create_database},
    model::{DocumentCollection, encode_value},
    query::{ColumnType, DocField},
    txn::{COMMIT_LOG_TABLE, WriteAuthorization},
};
use std::sync::{Arc, Mutex};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
enum At {
    PutDoc,
    DeleteDoc,
    DeletePosting,
    LogRead,
}

#[derive(Clone, Copy)]
enum Mutation {
    Update,
    Delete,
    RawUpdate,
}

struct Race {
    database: Arc<Database>,
    id: DocumentId,
    mutation: Mutex<Option<(At, Mutation)>>,
}

impl Race {
    fn inject(&self, at: At) -> Result<(), ReplError> {
        let mutation = {
            let mut pending = self
                .mutation
                .lock()
                .map_err(|_| ReplError::Rejected("test race lock poisoned".to_owned()))?;
            if pending.as_ref().is_some_and(|(expected, _)| {
                std::mem::discriminant(expected) == std::mem::discriminant(&at)
            }) {
                pending.take().map(|(_, mutation)| mutation)
            } else {
                None
            }
        };
        let result = match mutation {
            Some(Mutation::Update) => self.database.collection("posts").and_then(|documents| {
                documents
                    .update(self.id, &doc("latest concurrent text"))
                    .map_err(Into::into)
            }),
            Some(Mutation::Delete) => self
                .database
                .collection("posts")
                .and_then(|documents| documents.delete(self.id).map_err(Into::into)),
            Some(Mutation::RawUpdate) => self
                .database
                .propose(Op::Put {
                    table: DOCUMENT_TABLE.to_owned(),
                    key: make_document_key(CollectionId::new(7), self.id),
                    value: encode_value(&doc("latest concurrent text")).map_err(ReplError::from)?,
                })
                .map_err(crate::db::DbError::from),
            None => Ok(()),
        };
        result.map_err(|error| ReplError::Rejected(error.to_string()))
    }
}

impl Replication for Race {
    fn supports_bounded_scans(&self) -> bool {
        true
    }
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.database.propose(op)
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.database.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.database.propose_authorized_batch(ops, authorization)
    }
    fn propose_authorized_conditional_batch(
        &self,
        batch: ConditionalBatch,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        if batch
            .ops
            .iter()
            .any(|op| matches!(op, Op::Put {table, ..} if table == FULL_TEXT_DOCS_TABLE))
        {
            self.inject(At::PutDoc)?;
        }
        if batch
            .ops
            .iter()
            .any(|op| matches!(op, Op::Delete {table, ..} if table == FULL_TEXT_DOCS_TABLE))
        {
            self.inject(At::DeleteDoc)?;
        }
        if batch
            .ops
            .iter()
            .any(|op| matches!(op, Op::Delete {table, ..} if table == FULL_TEXT_POSTINGS_TABLE))
        {
            self.inject(At::DeletePosting)?;
        }
        self.database
            .propose_authorized_conditional_batch(batch, authorization)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.database.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        let rows = self.database.range(table, start, end, consistency)?;
        if table == COMMIT_LOG_TABLE {
            self.inject(At::LogRead)?;
        }
        Ok(rows)
    }
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        self.database.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
    }
}

fn doc(text: &str) -> Value {
    Value::Object(BTreeMap::from([(
        "body".to_owned(),
        Value::Str(text.to_owned()),
    )]))
}

fn fixture(at: At, mutation: Mutation) -> Result<Race, Box<dyn std::error::Error>> {
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
    let id = database
        .collection("posts")?
        .insert(&doc("original indexed text"))?;
    database.create_full_text_index(config())?;
    Ok(Race {
        database: Arc::new(database),
        id,
        mutation: Mutex::new(Some((at, mutation))),
    })
}

fn config() -> FullTextIndexConfig {
    FullTextIndexConfig::collection("posts_text", CollectionId::new(7), FieldPath::new(["body"]))
}

#[test]
fn incremental_refresh_conflicts_with_concurrent_update_or_delete_without_advancing_frontier()
-> TestResult {
    for mutation in [Mutation::Update, Mutation::Delete] {
        let race = fixture(At::PutDoc, mutation)?;
        let raw = DocumentCollection::new(race.database.as_ref(), CollectionId::new(7));
        raw.update(race.id, &doc("historical replay text"))?;
        let index = FullTextIndex::new(&race, config())?;
        let original_state = index.state()?;
        assert!(matches!(
            index.refresh_incremental(race.database.catalog()),
            Err(TextError::Repl(ReplError::Conflict))
        ));
        assert_eq!(index.state()?, original_state);
        assert_eq!(
            index.search("historical original", 10)?,
            Vec::<TextHit>::new()
        );
        let expected = usize::from(matches!(mutation, Mutation::Update));
        assert_eq!(index.search("latest", 10)?.len(), expected);
        let repaired = race.database.full_text_index("posts_text")?;
        repaired.refresh_incremental(race.database.catalog())?;
        assert_eq!(repaired.search("latest", 10)?.len(), expected);
    }
    Ok(())
}

#[test]
fn incremental_refresh_reconciles_live_source_instead_of_old_event_payloads() -> TestResult {
    let race = fixture(At::LogRead, Mutation::RawUpdate)?;
    DocumentCollection::new(race.database.as_ref(), CollectionId::new(7))
        .update(race.id, &doc("historical replay text"))?;
    let index = FullTextIndex::new(&race, config())?;
    index.refresh_incremental(race.database.catalog())?;
    assert_eq!(
        index.search("latest", 10)?.first().map(|hit| hit.id),
        Some(race.id)
    );
    assert_eq!(
        index.search("historical original", 10)?,
        Vec::<TextHit>::new()
    );
    Ok(())
}

#[test]
fn full_refresh_does_not_overwrite_a_writer_during_clear_or_source_flush() -> TestResult {
    for at in [At::DeleteDoc, At::DeletePosting, At::PutDoc] {
        let race = fixture(at, Mutation::Update)?;
        let index = FullTextIndex::new(&race, config())?;
        let original_state = index.state()?;
        assert!(matches!(
            index.refresh_full(),
            Err(TextError::Repl(ReplError::Conflict))
        ));
        assert_eq!(index.state()?, original_state);
        assert_eq!(
            index.search("latest", 10)?.first().map(|hit| hit.id),
            Some(race.id)
        );
        assert_eq!(index.search("original", 10)?, Vec::<TextHit>::new());
    }
    Ok(())
}

#[test]
fn search_suppresses_stale_or_deleted_raw_source_documents_until_repair() -> TestResult {
    let race = fixture(At::PutDoc, Mutation::Update)?;
    let raw = DocumentCollection::new(race.database.as_ref(), CollectionId::new(7));
    let index = race.database.full_text_index("posts_text")?;
    raw.update(race.id, &doc("new raw text"))?;
    assert_eq!(index.search("original", 10)?, Vec::<TextHit>::new());
    raw.delete(race.id)?;
    assert_eq!(index.search("original", 10)?, Vec::<TextHit>::new());
    Ok(())
}
