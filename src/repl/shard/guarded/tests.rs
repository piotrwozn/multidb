use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{
    internal_transport::TestReadBudget,
    repl::{
        Op, ReadConsistency, ReplError, Replication, SingleNode,
        shard::{PartitionStrategy, ShardMap, ShardedReplication},
    },
    storage::{MemEngine, ReadBudget, StorageError},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn router() -> TestResult<ShardedReplication> {
    let shards = BTreeMap::from([
        (
            1,
            Arc::new(SingleNode::new(MemEngine::new())) as Arc<dyn Replication>,
        ),
        (
            2,
            Arc::new(SingleNode::new(MemEngine::new())) as Arc<dyn Replication>,
        ),
    ]);
    Ok(ShardedReplication::new(
        ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 64 }, [1, 2])?,
        shards,
        1,
    )?)
}

fn separate_keys(router: &ShardedReplication) -> TestResult<(Vec<u8>, Vec<u8>)> {
    let mut keys = [None, None];
    for index in 0..1000 {
        let key = format!("k{index}").into_bytes();
        match router.shard_for("t", &key)? {
            1 if keys[0].is_none() => keys[0] = Some(key),
            2 if keys[1].is_none() => keys[1] = Some(key),
            _ => {}
        }
    }
    Ok((
        keys[0].take().ok_or("missing shard1 key")?,
        keys[1].take().ok_or("missing shard2 key")?,
    ))
}

#[test]
fn shard_merge_keeps_every_owned_head_reserved_until_callback_and_releases_on_stop() -> TestResult {
    let router = router()?;
    assert!(router.supports_guarded_reads());
    let (left, right) = separate_keys(&router)?;
    for key in [&left, &right] {
        router.propose(Op::Put {
            table: "t".to_owned(),
            key: key.clone(),
            value: vec![7; 4096],
        })?;
    }
    let budget = TestReadBudget::new(32 * 1024, Duration::from_secs(1))?;
    let mut called = 0;
    router.visit_range(
        "t",
        &[],
        &[],
        ReadConsistency::Strong,
        &budget,
        &mut |_, value| {
            assert_eq!(value.len(), 4096);
            assert!(budget.remaining_bytes() <= 32 * 1024 - 2 * 4096);
            called += 1;
            Ok(false)
        },
    )?;
    assert_eq!(called, 1);
    assert_eq!(budget.remaining_bytes(), 32 * 1024);
    Ok(())
}

#[test]
fn oversized_shard_head_fails_before_owned_clone_and_does_not_leave_a_lease() -> TestResult {
    let router = router()?;
    router.propose(Op::Put {
        table: "t".to_owned(),
        key: b"k".to_vec(),
        value: vec![7; 16 * 1024],
    })?;
    let budget = TestReadBudget::new(2048, Duration::from_secs(1))?;
    let mut called = false;
    let result = router.visit_range(
        "t",
        &[],
        &[],
        ReadConsistency::Strong,
        &budget,
        &mut |_, _| {
            called = true;
            Ok(true)
        },
    );
    assert!(matches!(
        result,
        Err(ReplError::Storage(StorageError::ResourceLimit(_)))
    ));
    assert!(!called);
    assert_eq!(budget.remaining_bytes(), 2048);
    Ok(())
}

#[test]
fn shard_guarded_points_and_many_keep_order_missing_duplicates_and_early_stop() -> TestResult {
    let router = router()?;
    let (left, right) = separate_keys(&router)?;
    for (key, value) in [(&left, b"left".to_vec()), (&right, b"right".to_vec())] {
        router.propose(Op::Put {
            table: "t".to_owned(),
            key: key.clone(),
            value,
        })?;
    }
    let budget = TestReadBudget::new(8192, Duration::from_secs(1))?;
    let keys = [
        right.clone(),
        b"missing".to_vec(),
        left.clone(),
        right.clone(),
        b"never".to_vec(),
    ];
    let mut seen = Vec::new();
    router.visit_many(
        "t",
        &keys,
        ReadConsistency::Strong,
        &budget,
        &mut |index, value| {
            seen.push((index, value.map(<[u8]>::to_vec)));
            Ok(index < 3)
        },
    )?;
    assert_eq!(
        seen,
        vec![
            (0, Some(b"right".to_vec())),
            (1, None),
            (2, Some(b"left".to_vec())),
            (3, Some(b"right".to_vec()))
        ]
    );
    assert_eq!(budget.remaining_bytes(), 8192);
    Ok(())
}

#[test]
fn guarded_read_deadline_interrupts_waiting_for_a_migration_barrier() -> TestResult {
    let router = router()?;
    let _migration = router
        .routing_barrier
        .write()
        .map_err(|_| "poisoned test routing lock")?;
    let budget = TestReadBudget::new(8192, Duration::from_millis(2))?;
    let started = std::time::Instant::now();
    let result = router.visit_get("t", b"k", ReadConsistency::Strong, &budget, &mut |_| {
        Ok(true)
    });
    assert!(matches!(
        result,
        Err(ReplError::Storage(StorageError::Timeout(_)))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    Ok(())
}
