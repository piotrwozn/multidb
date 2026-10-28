use std::sync::{Arc, Barrier};

use super::*;
use crate::{
    repl::ap::ApReplicaEndpoint,
    repl::{
        ApClusterConfig, ApNode, Op, ReadConsistency, Replication, VectorClock, VersionedBytes,
        VersionedWrite, WriteCondition, propose_system_conditional_batch,
    },
    storage::{MemEngine, ReadTransaction},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn put(table: &str, key: &[u8], value: &[u8]) -> Op {
    Op::Put {
        table: table.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn compare(expected: Option<&[u8]>, replacement: &[u8]) -> ConditionalBatch {
    ConditionalBatch::new(
        vec![WriteCondition::ValueEquals {
            table: "rows".to_owned(),
            key: b"k".to_vec(),
            expected: expected.map(<[u8]>::to_vec),
        }],
        vec![
            put("rows", b"k", replacement),
            put("indexes", replacement, b"k"),
        ],
    )
}

#[test]
fn ap_conditional_failure_changes_no_data_versions_or_merkle_hashes() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    node.propose(put("rows", b"k", b"old"))?;
    let read_tables = || -> Result<_, StorageError> {
        let read = node.storage().begin_read()?;
        [
            "rows",
            "indexes",
            crate::repl::AP_VERSIONS_TABLE,
            crate::repl::AP_MERKLE_TABLE,
        ]
        .into_iter()
        .map(|table| read.range(table, &[], &[])?.collect::<Result<Vec<_>, _>>())
        .collect::<Result<Vec<_>, _>>()
    };
    let before = read_tables()?;
    assert!(matches!(
        node.propose_conditional_batch(compare(Some(b"wrong"), b"new")),
        Err(ReplError::Conflict)
    ));
    assert_eq!(read_tables()?, before);
    node.propose_conditional_batch(compare(Some(b"old"), b"new"))?;
    assert_eq!(
        node.read("rows", b"k", ReadConsistency::Strong)?,
        Some(b"new".to_vec())
    );
    assert_eq!(
        node.read("indexes", b"new", ReadConsistency::Strong)?,
        Some(b"k".to_vec())
    );
    Ok(())
}

#[test]
fn concurrent_single_node_ap_cas_has_one_winner_and_no_losing_index() -> TestResult {
    let node = Arc::new(ApDynamo::new_for_test(MemEngine::new(), 1));
    node.propose(put("rows", b"k", b"old"))?;
    let start = Arc::new(Barrier::new(3));
    let writers = [b"left".as_slice(), b"right".as_slice()].map(|replacement| {
        let node = Arc::clone(&node);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            node.propose_conditional_batch(compare(Some(b"old"), replacement))
        })
    });
    start.wait();
    let mut successes = 0;
    let mut conflicts = 0;
    for writer in writers {
        match writer.join().map_err(|_| "CAS writer panicked")? {
            Ok(()) => successes += 1,
            Err(ReplError::Conflict) => conflicts += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!((successes, conflicts), (1, 1));
    let winner = node
        .read("rows", b"k", ReadConsistency::Strong)?
        .ok_or("missing winner")?;
    assert_eq!(
        node.range("indexes", &[], &[], ReadConsistency::Strong)?,
        vec![(winner, b"k".to_vec())]
    );
    Ok(())
}

#[test]
fn local_plain_writes_advance_unique_clocks_under_concurrency() -> TestResult {
    let node = Arc::new(ApDynamo::new_for_test(MemEngine::new(), 1));
    let start = Arc::new(Barrier::new(9));
    let writers = (0_u8..8)
        .map(|value| {
            let node = Arc::clone(&node);
            let start = Arc::clone(&start);
            std::thread::spawn(move || -> Result<(), ReplError> {
                start.wait();
                for _ in 0..32 {
                    node.propose(put("rows", b"k", &[value]))?;
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    for writer in writers {
        writer.join().map_err(|_| "writer panicked")??;
    }
    let versions = node.read_conflict_versions("rows", b"k")?;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].clock.entries().get(&1), Some(&256));
    Ok(())
}

#[test]
fn siblings_are_not_mistaken_for_missing_keys_and_overflow_fails_closed() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    for origin in [2, 3] {
        node.receive_batch(&[VersionedWrite {
            table: "rows".to_owned(),
            key: b"k".to_vec(),
            version: VersionedBytes {
                value: Some(vec![u8::try_from(origin)?]),
                clock: VectorClock::new([(origin, 1)]),
                origin,
            },
        }])?;
    }
    assert!(matches!(
        node.propose_conditional_batch(compare(None, b"new")),
        Err(ReplError::Conflict)
    ));
    let full = ApDynamo::new_for_test(MemEngine::new(), 1);
    full.receive_batch(&[VersionedWrite {
        table: "rows".to_owned(),
        key: b"k".to_vec(),
        version: VersionedBytes {
            value: Some(b"old".to_vec()),
            clock: VectorClock::new([(1, u64::MAX)]),
            origin: 1,
        },
    }])?;
    assert!(full.propose(put("rows", b"k", b"new")).is_err());
    assert_eq!(
        full.read("rows", b"k", ReadConsistency::Strong)?,
        Some(b"old".to_vec())
    );
    Ok(())
}

#[test]
fn local_authorized_cas_is_atomic_but_multinode_ap_refuses_global_cas() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    let batch = ConditionalBatch::new(
        vec![WriteCondition::KeyMissing {
            table: "__meta".to_owned(),
            key: b"k".to_vec(),
        }],
        vec![put("__meta", b"k", b"value")],
    );
    assert!(node.propose_conditional_batch(batch.clone()).is_err());
    propose_system_conditional_batch(&node, batch.clone())?;
    assert!(matches!(
        propose_system_conditional_batch(&node, batch),
        Err(ReplError::Conflict)
    ));
    let multi = ApDynamo::new_unchecked(
        MemEngine::new(),
        ApClusterConfig::new(
            1,
            "local",
            vec![ApNode::new(1, "one"), ApNode::new(2, "two")],
            2,
        ),
    );
    assert!(matches!(
        multi.propose_conditional_batch(compare(None, b"new")),
        Err(ReplError::Unsupported(_))
    ));
    Ok(())
}

#[test]
fn non_replica_coordinator_reserves_new_clocks_before_each_publication_and_restart() -> TestResult {
    use crate::repl::ap::InProcessApTransport;
    let config = |node| {
        ApClusterConfig::new(
            node,
            "local",
            vec![ApNode::new(1, "one"), ApNode::new(2, "two")],
            1,
        )
    };
    let receiver = Arc::new(ApDynamo::new_unchecked(MemEngine::new(), config(2)));
    let transport = Arc::new(InProcessApTransport::new());
    transport.register(2, receiver.clone());
    let coordinator = ApDynamo::new_unchecked(MemEngine::new(), config(1));
    coordinator.set_transport(transport.clone())?;
    let key = (0_u64..1000)
        .map(u64::to_be_bytes)
        .find(|key| coordinator.preference_list("rows", key) == vec![2])
        .ok_or("missing remote key")?;
    for value in [b"first".as_slice(), b"second".as_slice()] {
        coordinator.propose(put("rows", &key, value))?;
        assert_eq!(
            coordinator.read("rows", &key, ReadConsistency::Strong)?,
            Some(value.to_vec())
        );
    }
    assert_eq!(
        coordinator.local_versions("rows", &key)?,
        Vec::<VersionedBytes>::new()
    );
    let stored = coordinator.storage;
    let reopened = ApDynamo::new_unchecked(stored, config(1));
    reopened.set_transport(transport)?;
    reopened.propose(put("rows", &key, b"third"))?;
    assert_eq!(
        reopened.read("rows", &key, ReadConsistency::Strong)?,
        Some(b"third".to_vec())
    );
    assert_eq!(
        receiver.local_versions("rows", &key)?[0]
            .clock
            .entries()
            .get(&1),
        Some(&3)
    );
    let read = reopened.storage().begin_read()?;
    assert_eq!(
        read.get(
            crate::txn::KEYSPACE_REGISTRY_TABLE,
            crate::repl::ap::AP_NODE_CLOCK_TABLE.as_bytes()
        )?,
        Some(b"1".to_vec())
    );
    Ok(())
}

#[test]
fn explicit_ap_plain_document_capability_rejects_wrong_keys_and_tables() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    let mut key = 7_u32.to_be_bytes().to_vec();
    key.extend_from_slice(&[0; 16]);
    assert!(
        node.propose_plain_document_lww(8, put(crate::model::DOCUMENT_TABLE, &key, b"bad"))
            .is_err()
    );
    assert!(
        node.propose_plain_document_lww(7, put("indexes", &key, b"bad"))
            .is_err()
    );
    assert!(
        node.propose_plain_document_lww(7, put(crate::model::DOCUMENT_TABLE, b"short", b"bad"))
            .is_err()
    );
    node.propose_plain_document_lww(7, put(crate::model::DOCUMENT_TABLE, &key, b"value"))?;
    assert_eq!(
        node.read(crate::model::DOCUMENT_TABLE, &key, ReadConsistency::Strong)?,
        Some(b"value".to_vec())
    );
    Ok(())
}
