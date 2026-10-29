use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use super::*;
use crate::{
    repl::{ApDynamo, SingleNode},
    storage::MemEngine,
    text::FullTextIndex,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Interleaved {
    inner: SingleNode<MemEngine>,
    conditional: bool,
    competing: Mutex<Option<Vec<Op>>>,
    batches: AtomicUsize,
}

impl Interleaved {
    fn new(conditional: bool) -> Self {
        Self {
            inner: SingleNode::new(MemEngine::new()),
            conditional,
            competing: Mutex::new(None),
            batches: AtomicUsize::new(0),
        }
    }
}

impl Replication for Interleaved {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.batches.fetch_add(1, Ordering::SeqCst);
        self.inner.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: crate::txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.inner.propose_authorized_batch(ops, authorization)
    }
    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        if !self.conditional {
            return Err(ReplError::Unsupported(
                "external backend has no CAS".to_owned(),
            ));
        }
        if let Some(ops) = self
            .competing
            .lock()
            .map_err(|_| ReplError::Transport("test lock".to_owned()))?
            .take()
        {
            self.inner.propose_batch(ops)?;
        }
        self.inner.propose_conditional_batch(batch)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.inner.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.inner.range(table, start, end, consistency)
    }
}

fn document(text: &str) -> Value {
    Value::Object(BTreeMap::from([(
        "text".to_owned(),
        Value::Str(text.to_owned()),
    )]))
}

fn collection(repl: &Interleaved) -> DocumentCollection<'_, Interleaved> {
    DocumentCollection::with_indexes(
        repl,
        CollectionId::new(1),
        vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["text"]))],
    )
    .with_derived_indexes(DerivedIndexes {
        full_text: vec![FullTextIndexConfig::collection(
            "text",
            CollectionId::new(1),
            FieldPath::new(["text"]),
        )],
        geo: Vec::new(),
    })
}

#[test]
fn concurrent_update_prevents_stale_update_or_delete_index_deltas() -> TestResult {
    for delete in [false, true] {
        let repl = Interleaved::new(true);
        let collection = collection(&repl);
        let original = document("before");
        let winner = document("winner");
        let id = collection.insert(&original)?;
        *repl.competing.lock().map_err(|_| "test lock")? =
            Some(collection.replace_ops_with_old(id, Some(&original), &winner)?);
        let result = if delete {
            collection.delete(id)
        } else {
            collection.update(id, &document("loser"))
        };
        assert!(matches!(result, Err(ModelError::Repl(ReplError::Conflict))));
        assert_eq!(collection.get(id)?, Some(winner.clone()));
        assert_eq!(
            collection
                .query(&Predicate::Eq {
                    path: FieldPath::new(["text"]),
                    value: Value::Str("winner".to_owned())
                })?
                .documents,
            vec![(id, winner)]
        );
        assert_eq!(
            repl.range(INDEX_TABLE, &[], &[], ReadConsistency::Strong)?
                .len(),
            1
        );
        let index = FullTextIndex::new(
            &repl,
            FullTextIndexConfig::collection("text", CollectionId::new(1), FieldPath::new(["text"])),
        )?;
        assert_eq!(index.search("winner", 10)?.len(), 1);
        assert_eq!(index.search("before", 10)?.len(), 0);
        assert_eq!(index.search("loser", 10)?.len(), 0);
    }
    Ok(())
}

#[test]
fn unsupported_conditional_writes_never_fall_back_to_unconditional_mutation() -> TestResult {
    let repl = Interleaved::new(false);
    let collection = collection(&repl);
    let original = document("before");
    let id = collection.insert(&original)?;
    repl.batches.store(0, Ordering::SeqCst);
    assert!(matches!(
        collection.update(id, &document("after")),
        Err(ModelError::Repl(ReplError::Unsupported(_)))
    ));
    assert!(matches!(
        collection.delete(id),
        Err(ModelError::Repl(ReplError::Unsupported(_)))
    ));
    assert_eq!(repl.batches.load(Ordering::SeqCst), 0);
    assert_eq!(collection.get(id)?, Some(original));
    Ok(())
}

#[test]
fn native_ap_document_updates_and_deletes_keep_their_secondary_index() -> TestResult {
    let repl = ApDynamo::new_for_test(MemEngine::new(), 1);
    let collection = DocumentCollection::with_indexes(
        &repl,
        CollectionId::new(1),
        vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["text"]))],
    );
    let id = collection.insert(&document("before"))?;
    collection.update(id, &document("after"))?;
    assert_eq!(
        repl.range(INDEX_TABLE, &[], &[], ReadConsistency::Strong)?
            .len(),
        1
    );
    collection.delete(id)?;
    assert!(collection.get(id)?.is_none());
    assert_eq!(
        repl.range(INDEX_TABLE, &[], &[], ReadConsistency::Strong)?
            .len(),
        0
    );
    Ok(())
}

#[test]
fn multi_node_ap_preserves_only_explicit_index_free_document_lww() -> TestResult {
    use crate::repl::{ApClusterConfig, ApNode, ConsistencyLevel, ap::InProcessApTransport};
    use std::sync::Arc;
    for indexed in 0..3 {
        let nodes = vec![
            ApNode::new(1, "in-process-one"),
            ApNode::new(2, "in-process-two"),
        ];
        let config = |node| {
            ApClusterConfig::new(node, format!("in-process-{node}"), nodes.clone(), 2)
                .with_default_write(ConsistencyLevel::All)
        };
        let one = Arc::new(ApDynamo::new_unchecked(MemEngine::new(), config(1)));
        let two = Arc::new(ApDynamo::new_unchecked(MemEngine::new(), config(2)));
        let transport = Arc::new(InProcessApTransport::new());
        transport.register(1, one.clone());
        transport.register(2, two.clone());
        one.set_transport(transport.clone())?;
        two.set_transport(transport)?;
        let indexes = if indexed == 1 {
            vec![IndexSpec::new(IndexId::new(1), FieldPath::new(["text"]))]
        } else {
            Vec::new()
        };
        let mut collection =
            DocumentCollection::with_indexes(one.as_ref(), CollectionId::new(1), indexes);
        if indexed == 2 {
            collection = collection.with_derived_indexes(DerivedIndexes {
                full_text: vec![FullTextIndexConfig::collection(
                    "text",
                    CollectionId::new(1),
                    FieldPath::new(["text"]),
                )],
                geo: Vec::new(),
            });
        }
        let id = collection.insert(&document("before"))?;
        if indexed == 0 {
            collection.update(id, &document("after"))?;
            assert_eq!(collection.get(id)?, Some(document("after")));
            assert_eq!(
                DocumentCollection::new(two.as_ref(), CollectionId::new(1)).get(id)?,
                Some(document("after"))
            );
            collection.delete(id)?;
            assert!(collection.get(id)?.is_none());
        } else {
            assert!(matches!(
                collection.update(id, &document("after")),
                Err(ModelError::Repl(ReplError::Unsupported(_)))
            ));
            assert!(matches!(
                collection.delete(id),
                Err(ModelError::Repl(ReplError::Unsupported(_)))
            ));
            assert_eq!(collection.get(id)?, Some(document("before")));
        }
    }
    Ok(())
}

#[test]
fn unknown_backend_cannot_opt_into_plain_document_fallback_by_returning_unsupported() -> TestResult
{
    let repl = Interleaved::new(false);
    let collection = DocumentCollection::new(&repl, CollectionId::new(1));
    let id = collection.insert(&document("before"))?;
    repl.batches.store(0, Ordering::SeqCst);
    assert!(matches!(
        collection.update(id, &document("after")),
        Err(ModelError::Repl(ReplError::Unsupported(_)))
    ));
    assert!(matches!(
        collection.delete(id),
        Err(ModelError::Repl(ReplError::Unsupported(_)))
    ));
    assert_eq!(repl.batches.load(Ordering::SeqCst), 0);
    assert_eq!(collection.get(id)?, Some(document("before")));
    Ok(())
}
