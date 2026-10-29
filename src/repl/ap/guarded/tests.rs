use std::{sync::Arc, time::Duration};

use crate::{
    internal_transport::TestReadBudget,
    phase30::{InternalTransportConfig, InternalTransportSecurity},
    repl::{
        Op, ReadConsistency, ReplError, Replication, VectorClock, VersionedBytes, VersionedWrite,
        ap::{
            ApClusterConfig, ApDynamo, ApNode, ApReplicaEndpoint, ConsistencyLevel,
            InProcessApTransport,
        },
    },
    storage::{MemEngine, ReadBudget, StorageError},
};

type Node = Arc<ApDynamo<MemEngine>>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn cluster(replication_factor: usize, write: ConsistencyLevel) -> TestResult<Vec<Node>> {
    let peers = (1..=3)
        .map(|node| ApNode::new(node, format!("127.0.0.1:{}", 7300 + node)))
        .collect::<Vec<_>>();
    let nodes = peers
        .iter()
        .map(|node| {
            Arc::new(ApDynamo::new_unchecked(
                MemEngine::new(),
                ApClusterConfig::new(
                    node.id,
                    node.addr.clone(),
                    peers.clone(),
                    replication_factor,
                )
                .with_default_write(write)
                .with_transport(InternalTransportConfig::new(
                    node.addr.clone(),
                    InternalTransportSecurity::PlaintextForTests,
                )),
            ))
        })
        .collect::<Vec<_>>();
    let transport = Arc::new(InProcessApTransport::new());
    for node in &nodes {
        transport.register(node.cluster.node_id, node.clone());
    }
    for node in &nodes {
        node.set_transport(transport.clone())?;
    }
    Ok(nodes)
}

fn budget() -> TestResult<TestReadBudget> {
    Ok(TestReadBudget::new(
        8 * 1024 * 1024,
        Duration::from_secs(5),
    )?)
}

fn guarded_rows(node: &Node, budget: &dyn ReadBudget) -> Result<Rows, ReplError> {
    let mut rows = Vec::new();
    node.visit_range(
        "t",
        &[],
        &[],
        ReadConsistency::Strong,
        budget,
        &mut |key, value| {
            // Test-owned result copies are deliberately outside the scan itself;
            // retain their explicit reservation alongside the returned fixture rows.
            rows.push((key.to_vec(), value.to_vec()));
            Ok(true)
        },
    )?;
    Ok(rows)
}

#[test]
fn rf_one_range_requires_every_owner_and_does_not_repair_non_replicas() -> TestResult {
    let nodes = cluster(1, ConsistencyLevel::All)?;
    let key = (0..1000)
        .map(|index| format!("k{index}").into_bytes())
        .find(|key| nodes[0].preference_list("t", key) == [2])
        .ok_or("no owner-2 key")?;
    nodes[0].propose(Op::Put {
        table: "t".to_owned(),
        key: key.clone(),
        value: b"owner".to_vec(),
    })?;
    assert_eq!(
        nodes[0].range("t", &[], &[], ReadConsistency::Strong)?,
        vec![(key.clone(), b"owner".to_vec())]
    );
    assert_eq!(
        guarded_rows(&nodes[0], &budget()?)?,
        vec![(key.clone(), b"owner".to_vec())]
    );
    for node in [&nodes[0], &nodes[2]] {
        assert_eq!(node.local_versions("t", &key)?, Vec::new());
    }
    nodes[0].set_available_nodes_for_tests([1, 3])?;
    assert!(matches!(
        nodes[0].range("t", &[], &[], ReadConsistency::Strong),
        Err(ReplError::NoQuorum)
    ));
    assert!(matches!(
        guarded_rows(&nodes[0], &budget()?),
        Err(ReplError::NoQuorum)
    ));
    Ok(())
}

#[test]
fn rf_two_write_all_ranges_allow_one_down_and_preserve_missing_and_tombstones() -> TestResult {
    let nodes = cluster(2, ConsistencyLevel::All)?;
    let key = b"present".to_vec();
    nodes[0].propose(Op::Put {
        table: "t".to_owned(),
        key: key.clone(),
        value: b"v".to_vec(),
    })?;
    nodes[0].set_available_nodes_for_tests([1, 2])?;
    let read_budget = budget()?;
    assert_eq!(
        guarded_rows(&nodes[0], &read_budget)?,
        vec![(key.clone(), b"v".to_vec())]
    );
    assert_eq!(
        nodes[0].range("t", &[], &[], ReadConsistency::Strong)?,
        vec![(key.clone(), b"v".to_vec())]
    );
    let mut missing = 0;
    nodes[0].visit_get(
        "t",
        b"missing",
        ReadConsistency::Strong,
        &read_budget,
        &mut |value| {
            assert_eq!(value, None);
            missing += 1;
            Ok(true)
        },
    )?;
    assert_eq!(missing, 1);
    nodes[0].set_available_nodes_for_tests([1, 2, 3])?;
    nodes[0].propose(Op::Delete {
        table: "t".to_owned(),
        key: key.clone(),
    })?;
    let replicas = nodes[0].preference_list("t", &key);
    let ghost = nodes
        .iter()
        .find(|node| !replicas.contains(&node.cluster.node_id))
        .ok_or("no nonreplica")?;
    ghost.receive_batch(&[VersionedWrite {
        table: "t".to_owned(),
        key: key.clone(),
        version: VersionedBytes {
            value: Some(b"stale-nonreplica".to_vec()),
            clock: VectorClock::new([(99, 1)]),
            origin: 99,
        },
    }])?;
    assert_eq!(guarded_rows(&nodes[0], &read_budget)?, Vec::new());
    assert_eq!(
        nodes[0].range("t", &[], &[], ReadConsistency::Strong)?,
        Vec::new()
    );
    let ghost_before = ghost.local_versions("t", &key)?;
    assert_eq!(ghost_before.len(), 1); // no repair was sent outside the replica set
    assert_eq!(ghost_before[0].origin, 99);
    Ok(())
}

#[test]
fn rf_two_write_one_strong_range_cannot_count_a_nonowner_as_quorum() -> TestResult {
    let nodes = cluster(2, ConsistencyLevel::One)?;
    nodes[0].set_available_nodes_for_tests([1, 2])?;
    assert!(matches!(
        guarded_rows(&nodes[0], &budget()?),
        Err(ReplError::NoQuorum)
    ));
    assert!(matches!(
        nodes[0].range("t", &[], &[], ReadConsistency::Strong),
        Err(ReplError::NoQuorum)
    ));
    Ok(())
}

#[test]
fn local_guarded_page_reserves_before_json_decode_and_releases_with_the_page() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    node.propose(Op::Put {
        table: "t".to_owned(),
        key: b"k".to_vec(),
        value: vec![7; 1024],
    })?;
    let read_budget = budget()?;
    let before = read_budget.remaining_bytes();
    let page = node.read_range_page_guarded("t", &[], &[], 1, &read_budget)?;
    assert_eq!(page.records.len(), 1);
    assert!(read_budget.remaining_bytes() < before);
    drop(page);
    assert_eq!(read_budget.remaining_bytes(), before);
    let small = TestReadBudget::new(2048, Duration::from_secs(1))?;
    assert!(matches!(
        node.read_range_page_guarded("t", &[], &[], 1, &small),
        Err(ReplError::Storage(StorageError::ResourceLimit(_)))
    ));
    assert_eq!(small.remaining_bytes(), 2048);
    Ok(())
}

#[test]
fn numeric_sibling_arrays_fit_16k_with_prior_results_still_reserved() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    for key in 0..64_u8 {
        node.propose(Op::Put {
            table: "t".to_owned(),
            key: vec![key],
            value: vec![255; 1024],
        })?;
    }
    let read_budget = TestReadBudget::new(16 * 1024, Duration::from_secs(5))?;
    let retained = read_budget.reserve(6 * 1024)?;
    let mut rows = 0;
    node.visit_range(
        "t",
        &[],
        &[],
        ReadConsistency::Strong,
        &read_budget,
        &mut |_, value| {
            assert_eq!(value, [255; 1024]);
            rows += 1;
            Ok(true)
        },
    )?;
    assert_eq!(rows, 64);
    assert_eq!(read_budget.remaining_bytes(), 10 * 1024);
    drop(retained);
    assert_eq!(read_budget.remaining_bytes(), 16 * 1024);
    Ok(())
}

#[test]
fn json_workspace_tracks_tokens_and_ignored_nesting_not_byte_array_length() -> TestResult {
    let read_budget = budget()?;
    let working = |bytes: &[u8]| super::value_bound::working_bytes(bytes, &read_budget);
    let raw = |count| {
        format!(
            "[{{\"value\":[{}0],\"clock\":{{\"1\":1}},\"origin\":1}}]",
            "255,".repeat(count)
        )
    };
    let short = raw(8);
    let long = raw(65_536);
    assert_eq!(working(short.as_bytes())?, working(long.as_bytes())?);
    assert!(working(long.as_bytes())? < 1024);
    let escaped = br#"[{"valu\u0065":[1],"cl\u006fck":{"1":1},"or\u0069gin":1}]"#;
    assert!(super::value_bound::versions_bound(escaped, 8192)? > 1024);
    assert!(working(escaped)? > working(short.as_bytes())?);
    let nested = format!(
        "[{{\"ignored\":{}null{}}}]",
        "[".repeat(4096),
        "]".repeat(4096)
    );
    assert!(working(nested.as_bytes())? > 8192);
    let malformed = format!("[{{\"value\":\"{}\"}}]", "\u{7f}".repeat(4096));
    // serde formats unexpected typed strings into errors, even without escapes.
    assert!(working(malformed.as_bytes())? > 24 * 4096);
    assert!(matches!(
        super::value_bound::versions_bound(malformed.as_bytes(), 8192),
        Err(StorageError::Corruption(_))
    ));
    Ok(())
}

#[test]
fn json_workspace_checks_cancellation_before_scanning_the_second_chunk() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CancelOnSecondCheck(AtomicUsize);
    impl ReadBudget for CancelOnSecondCheck {
        fn check(&self) -> Result<(), StorageError> {
            if self.0.fetch_add(1, Ordering::Relaxed) >= 1 {
                Err(StorageError::Cancelled("test cancellation".to_owned()))
            } else {
                Ok(())
            }
        }

        fn reserve(
            &self,
            _: usize,
        ) -> Result<Box<dyn crate::storage::ReadReservation>, StorageError> {
            panic!("workspace preflight must not allocate owned input");
        }

        fn remaining_bytes(&self) -> usize {
            usize::MAX
        }
    }

    let source = [b'0'; 16 * 1024];
    let budget = CancelOnSecondCheck(AtomicUsize::new(0));
    assert!(matches!(
        super::value_bound::working_bytes(&source, &budget),
        Err(StorageError::Cancelled(_))
    ));
    assert_eq!(budget.0.load(Ordering::Relaxed), 2);
}

#[test]
fn guarded_point_and_many_preserve_missing_duplicates_tombstones_and_stop() -> TestResult {
    let node = ApDynamo::new_for_test(MemEngine::new(), 1);
    node.propose(Op::Put {
        table: "t".to_owned(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })?;
    let read_budget = budget()?;
    let before = read_budget.remaining_bytes();
    let mut seen = Vec::new();
    node.visit_many(
        "t",
        &[
            b"missing".to_vec(),
            b"k".to_vec(),
            b"k".to_vec(),
            b"ignored".to_vec(),
        ],
        ReadConsistency::Strong,
        &read_budget,
        &mut |index, value| {
            seen.push((index, value.map(<[u8]>::to_vec)));
            Ok(index < 2)
        },
    )?;
    assert_eq!(
        seen,
        vec![
            (0, None),
            (1, Some(b"v".to_vec())),
            (2, Some(b"v".to_vec()))
        ]
    );
    assert_eq!(read_budget.remaining_bytes(), before);
    node.propose(Op::Delete {
        table: "t".to_owned(),
        key: b"k".to_vec(),
    })?;
    node.visit_get(
        "t",
        b"k",
        ReadConsistency::Strong,
        &read_budget,
        &mut |value| {
            assert_eq!(value, None);
            Ok(true)
        },
    )?;
    Ok(())
}

#[test]
fn stored_sibling_preflight_bounds_json_containers_without_constructing_them() -> TestResult {
    let raw = format!(
        "[{}]",
        std::iter::repeat_n(r#"{"value":null,"clock":{},"origin":1}"#, 1000)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(matches!(
        super::value_bound::versions_bound(raw.as_bytes(), 1024),
        Err(StorageError::ResourceLimit(_))
    ));
    let raw = br#"[{"value":[1,2,3],"clock":{"2":4},"origin":2}]"#;
    assert!(super::value_bound::versions_bound(raw, 8192)? > 1024);
    assert!(matches!(
        super::value_bound::versions_bound(b"[]junk", 8192),
        Err(StorageError::Corruption(_))
    ));
    Ok(())
}

#[test]
fn legacy_custom_storage_keeps_point_many_and_range_visitors_without_claiming_guards() -> TestResult
{
    struct LegacyStorage(MemEngine);
    impl crate::storage::StorageEngine for LegacyStorage {
        type ReadTxn<'a> = <MemEngine as crate::storage::StorageEngine>::ReadTxn<'a>;
        type WriteTxn<'a> = <MemEngine as crate::storage::StorageEngine>::WriteTxn<'a>;
        fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
            <MemEngine as crate::storage::StorageEngine>::begin_read(&self.0)
        }
        fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
            <MemEngine as crate::storage::StorageEngine>::begin_write(&self.0)
        }
    }
    let node = ApDynamo::new_for_test(LegacyStorage(MemEngine::new()), 1);
    assert!(!node.supports_guarded_reads());
    node.propose(Op::Put {
        table: "t".to_owned(),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    })?;
    let budget = budget()?;
    let mut seen = Vec::new();
    node.visit_get("t", b"k", ReadConsistency::Strong, &budget, &mut |value| {
        assert_eq!(value, Some(b"v".as_slice()));
        Ok(true)
    })?;
    node.visit_many(
        "t",
        &[b"k".to_vec(), b"missing".to_vec()],
        ReadConsistency::Strong,
        &budget,
        &mut |index, value| {
            seen.push((index, value.map(<[u8]>::to_vec)));
            Ok(true)
        },
    )?;
    assert_eq!(seen, vec![(0, Some(b"v".to_vec())), (1, None)]);
    let mut rows = Vec::new();
    node.visit_range(
        "t",
        &[],
        &[],
        ReadConsistency::Strong,
        &budget,
        &mut |key, value| {
            rows.push((key.to_vec(), value.to_vec()));
            Ok(true)
        },
    )?;
    assert_eq!(rows, vec![(b"k".to_vec(), b"v".to_vec())]);
    Ok(())
}
