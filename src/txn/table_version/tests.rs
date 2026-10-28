use std::{collections::BTreeMap, sync::Arc};

use super::TABLE_DATA_VERSION_TABLE;
use crate::{
    backup::{BackupConfig, RestoreTarget, full_backup, incremental_backup, restore_backup},
    db::{DbConfig, Profile, create_database, open_database},
    model::Value,
    query::{ColumnDef, ColumnType, REL_ROWS_TABLE, TableSchema},
    repl::{
        ApDynamo, ConditionalBatch, CpRaft, Op, PartitionStrategy, ReadConsistency, ReplError,
        Replication, ShardMap, ShardedReplication, SingleNode, WriteCondition, propose_system,
    },
    storage::{Bytes, MemEngine, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
    txn::{self, COMMIT_LOG_TABLE, KEYSPACE_REGISTRY_TABLE},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn put(table: &str, key: &[u8], value: &[u8]) -> Op {
    Op::Put {
        table: table.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

#[test]
fn tokens_track_only_public_tables_and_cover_deletes() -> TestResult {
    let node = SingleNode::new(MemEngine::new());
    assert_eq!(node.table_data_version("rows")?, Some(0));
    node.propose_batch(vec![
        put("rows", b"a", b"1"),
        put("rows", b"b", b"2"),
        put("other", b"x", b"3"),
    ])?;
    let first = node.current_txn_id()?;
    assert_eq!(node.table_data_version("rows")?, Some(first));
    assert_eq!(node.table_data_version("other")?, Some(first));
    let tokens = node.range(TABLE_DATA_VERSION_TABLE, &[], &[], ReadConsistency::Strong)?;
    for internal in ["__audit", "__workload", "__planner_feedback"] {
        propose_system(&node, put(internal, b"event", b"payload"))?;
        assert_eq!(node.table_data_version(internal)?, None);
    }
    assert!(node.current_txn_id()? > first);
    assert_eq!(
        node.range(TABLE_DATA_VERSION_TABLE, &[], &[], ReadConsistency::Strong)?,
        tokens
    );
    node.propose(Op::Delete {
        table: "rows".to_owned(),
        key: b"a".to_vec(),
    })?;
    assert_eq!(
        node.table_data_version("rows")?,
        Some(node.current_txn_id()?)
    );
    assert_eq!(node.table_data_version("other")?, Some(first));
    assert_eq!(
        node.read(
            KEYSPACE_REGISTRY_TABLE,
            TABLE_DATA_VERSION_TABLE.as_bytes(),
            ReadConsistency::Strong
        )?,
        Some(b"1".to_vec())
    );
    Ok(())
}

#[test]
fn internal_only_commits_do_not_create_token_metadata() -> TestResult {
    let node = SingleNode::new(MemEngine::new());
    propose_system(&node, put("__audit", b"event", b"payload"))?;
    assert_eq!(
        node.range(TABLE_DATA_VERSION_TABLE, &[], &[], ReadConsistency::Strong)?,
        Vec::<(Bytes, Bytes)>::new()
    );
    assert_eq!(
        node.read(
            KEYSPACE_REGISTRY_TABLE,
            TABLE_DATA_VERSION_TABLE.as_bytes(),
            ReadConsistency::Strong
        )?,
        None
    );
    Ok(())
}

#[test]
fn conflicting_and_empty_commits_keep_existing_tokens() -> TestResult {
    let node = SingleNode::new(MemEngine::new());
    node.propose(put("rows", b"a", b"old"))?;
    let version = node.table_data_version("rows")?;
    assert!(matches!(
        node.propose_conditional_batch(ConditionalBatch::new(
            vec![WriteCondition::KeyMissing {
                table: "rows".to_owned(),
                key: b"a".to_vec(),
            }],
            vec![put("rows", b"b", b"uncommitted")],
        )),
        Err(ReplError::Conflict)
    ));
    node.propose_batch(Vec::new())?;
    assert_eq!(node.table_data_version("rows")?, version);
    assert_eq!(node.read("rows", b"b", ReadConsistency::Strong)?, None);
    Ok(())
}

#[test]
fn data_and_token_appear_together_in_a_storage_snapshot() -> TestResult {
    let node = SingleNode::new(MemEngine::new());
    node.propose(put("rows", b"a", b"old"))?;
    let first = node.current_txn_id()?;
    let before = node.storage().begin_read()?;
    node.propose(put("rows", b"a", b"new"))?;
    let second = node.current_txn_id()?;
    let after = node.storage().begin_read()?;
    for (snapshot, expected_data, expected_version) in
        [(before, b"old", first), (after, b"new", second)]
    {
        assert_eq!(snapshot.get("rows", b"a")?, Some(expected_data.to_vec()));
        assert_eq!(
            snapshot.get(TABLE_DATA_VERSION_TABLE, b"rows")?,
            Some(txn::encode_txn_id(expected_version).to_vec())
        );
    }
    Ok(())
}

#[test]
fn failure_after_staging_tokens_rolls_back_data_token_and_log() -> TestResult {
    let node = SingleNode::new(MemEngine::new());
    node.propose(put("rows", b"a", b"old"))?;
    let first = node.current_txn_id()?;
    let result = txn::commit_write_set_in_txn_with_extra(
        node.storage().begin_write()?,
        first,
        txn::ops_to_write_set(vec![put("rows", b"a", b"uncommitted")]),
        |write, version| {
            assert_eq!(write.get("rows", b"a")?, Some(b"uncommitted".to_vec()));
            assert_eq!(
                write.get(TABLE_DATA_VERSION_TABLE, b"rows")?,
                Some(txn::encode_txn_id(version).to_vec())
            );
            Err(StorageError::Backend("injected failure".to_owned()))
        },
    );
    assert!(matches!(result, Err(StorageError::Backend(_))));
    assert_eq!(node.current_txn_id()?, first);
    assert_eq!(node.table_data_version("rows")?, Some(first));
    assert_eq!(
        node.read("rows", b"a", ReadConsistency::Strong)?,
        Some(b"old".to_vec())
    );
    assert_eq!(
        node.read(
            COMMIT_LOG_TABLE,
            &txn::commit_log_key(first + 1),
            ReadConsistency::Strong
        )?,
        None
    );
    Ok(())
}

#[test]
fn legacy_native_data_starts_at_zero_and_corrupt_tokens_fail_closed() -> TestResult {
    let node = SingleNode::new(MemEngine::new());
    let mut legacy = node.storage().begin_write()?;
    legacy.put("rows", b"a", b"legacy")?;
    legacy.put(
        txn::TXN_META_TABLE,
        txn::CURRENT_TXN_ID_KEY,
        &txn::encode_txn_id(24),
    )?;
    legacy.commit()?;
    assert_eq!(node.table_data_version("rows")?, Some(0));
    node.propose(put("rows", b"a", b"new"))?;
    assert_eq!(node.table_data_version("rows")?, Some(25));
    let mut corrupt = node.storage().begin_write()?;
    corrupt.put(TABLE_DATA_VERSION_TABLE, b"rows", b"bad")?;
    corrupt.commit()?;
    assert!(matches!(
        node.table_data_version("rows"),
        Err(ReplError::Storage(StorageError::Corruption(_)))
    ));
    Ok(())
}

#[test]
fn replay_reconstructs_tokens_including_deleted_tables() -> TestResult {
    let live = SingleNode::new(MemEngine::new());
    live.propose(put("rows", b"a", b"1"))?;
    live.propose_batch(vec![
        Op::Delete {
            table: "rows".to_owned(),
            key: b"a".to_vec(),
        },
        put("other", b"b", b"2"),
    ])?;
    propose_system(&live, put("__audit", b"event", b"ignored"))?;
    let replay = SingleNode::new(MemEngine::new());
    let mut write = replay.storage().begin_write()?;
    for (_, bytes) in live.range(COMMIT_LOG_TABLE, &[], &[], ReadConsistency::Strong)? {
        txn::apply_commit_record_in_txn(&mut write, &txn::decode_commit_log_record(&bytes)?)?;
    }
    write.commit()?;
    assert_eq!(
        replay.table_data_version("rows")?,
        live.table_data_version("rows")?
    );
    assert_eq!(
        replay.table_data_version("other")?,
        live.table_data_version("other")?
    );
    assert_eq!(replay.read("rows", b"a", ReadConsistency::Strong)?, None);
    assert_eq!(
        replay.range(TABLE_DATA_VERSION_TABLE, &[], &[], ReadConsistency::Strong)?,
        live.range(TABLE_DATA_VERSION_TABLE, &[], &[], ReadConsistency::Strong)?
    );
    Ok(())
}

#[test]
fn cp_exposes_native_tokens_and_ap_and_shards_do_not() -> TestResult {
    let cp = CpRaft::new_for_test(MemEngine::new(), 1);
    assert_eq!(cp.table_data_version("rows")?, Some(0));
    cp.propose(put("rows", b"a", b"1"))?;
    assert!(
        cp.table_data_version("rows")?
            .is_some_and(|version| version > 0)
    );
    let ap = ApDynamo::new_for_test(MemEngine::new(), 1);
    assert_eq!(ap.table_data_version("rows")?, None);
    let native: Arc<dyn Replication> = Arc::new(SingleNode::new(MemEngine::new()));
    let map = ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 16 }, [1])?;
    let sharded = ShardedReplication::new(map, BTreeMap::from([(1, native)]), 1)?;
    assert_eq!(sharded.table_data_version("rows")?, None);
    Ok(())
}

#[test]
fn database_handles_and_transactions_advance_the_same_public_token() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    assert_eq!(database.table_data_version(REL_ROWS_TABLE)?, Some(0));
    let table = database.create_table(
        "items",
        Some(TableSchema::new(
            vec![ColumnDef::new("id", ColumnType::Int, false)],
            0,
        )),
        Vec::new(),
    )?;
    // Creating a schema writes only internal keyspaces.
    assert_eq!(database.table_data_version(REL_ROWS_TABLE)?, Some(0));
    table.insert(vec![Value::Int(1)])?;
    let first = database
        .table_data_version(REL_ROWS_TABLE)?
        .ok_or("missing token")?;
    let mut transaction = database.begin_transaction()?;
    transaction.insert_row("items", vec![Value::Int(2)])?;
    transaction.commit()?;
    let second = database
        .table_data_version(REL_ROWS_TABLE)?
        .ok_or("missing token")?;
    assert!(second > first);
    table.delete(&Value::Int(1))?;
    assert!(
        database
            .table_data_version(REL_ROWS_TABLE)?
            .is_some_and(|version| version > second)
    );
    assert_eq!(table.scan()?, vec![vec![Value::Int(2)]]);
    Ok(())
}

#[test]
fn full_and_incremental_backups_preserve_tokens_across_reopen() -> TestResult {
    let temp = tempfile::tempdir()?;
    let config = DbConfig::on_disk(Profile::Balanced, temp.path().join("source.redb"));
    let database = create_database(config)?;
    database.propose(put("rows", b"a", b"full"))?;
    let first = database.table_data_version("rows")?;
    let full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    database.propose(Op::Delete {
        table: "rows".to_owned(),
        key: b"a".to_vec(),
    })?;
    let second = database.table_data_version("rows")?;
    assert!(second > first);
    let incremental = incremental_backup(
        &database,
        temp.path().join("backups"),
        &full.path,
        &BackupConfig::default(),
    )?;
    for (name, backup, expected, value) in [
        ("full", &full.path, first, Some(b"full".to_vec())),
        ("incremental", &incremental.path, second, None),
    ] {
        let restored_config =
            DbConfig::on_disk(Profile::Balanced, temp.path().join(format!("{name}.redb")));
        restore_backup(backup, restored_config.clone(), RestoreTarget::Latest)?;
        let restored = open_database(restored_config.clone())?;
        assert_eq!(restored.table_data_version("rows")?, expected);
        assert_eq!(restored.read("rows", b"a", ReadConsistency::Strong)?, value);
        drop(restored);
        assert_eq!(
            open_database(restored_config)?.table_data_version("rows")?,
            expected
        );
    }
    Ok(())
}
