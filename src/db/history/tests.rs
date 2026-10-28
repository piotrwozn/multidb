use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};

use crate::backup::{
    BackupConfig, BackupError, RestoreTarget, full_backup, incremental_backup, restore_backup,
};
use crate::cdc::{
    self, AggregateKind, AggregateSpec, ChangefeedFilter, ChangefeedOptions, FeedError,
    MaterializedViewSpec, ResumeToken, SubscriptionConfig,
};
use crate::continuous::ContinuousQuerySpec;
use crate::db::{
    CatalogEntry, ConfigError, Database, DbConfig, DbError, Profile, ReplicationKind,
    ShardBackendConfig, ShardSpec, ShardedDatabaseConfig, create_ap_database, create_database,
    create_sharded_database, open_database,
};
use crate::geo::{GeoIndexConfig, GeoPoint};
use crate::model::{CollectionId, FieldPath, Value};
use crate::query::{ColumnDef, ColumnType, DocField, RelTable, SqlOutput, TableSchema};
use crate::repl::{
    ApClusterConfig, Op, PartitionStrategy, ReadConsistency, ReplError, Replication, propose_system,
};
use crate::storage::StorageError;
use crate::temporal::{TemporalError, TemporalPoint, TemporalRetention};
use crate::text::FullTextIndexConfig;
use crate::txn::{
    self, TxnId,
    history::{HistoryGcOptions, HistoryWatermarkSource},
};

type TestResult = Result<(), Box<dyn Error>>;

fn counter_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("value", ColumnType::Int, false),
        ],
        0,
    )
}

fn seed_counter(database: &mut Database, updates: i64) -> Result<RelTable, DbError> {
    let counters = database.create_table("counters", Some(counter_schema()), Vec::new())?;
    counters.insert(vec![Value::Int(1), Value::Int(0)])?;
    for value in 1..=updates {
        counters.update(vec![Value::Int(1), Value::Int(value)])?;
    }
    Ok(counters)
}

fn current_lsn(database: &Database) -> Result<TxnId, Box<dyn Error>> {
    Ok(cdc::current_resume_token(database)?.lsn)
}

fn row_count(database: &Database, table: &str) -> Result<usize, ReplError> {
    Ok(database
        .range(table, &[], &[], ReadConsistency::Strong)?
        .len())
}

fn commit_time(database: &Database, lsn: TxnId) -> Result<SystemTime, Box<dyn Error>> {
    let bytes = database
        .read(
            txn::COMMIT_LOG_TABLE,
            &txn::commit_log_key(lsn),
            ReadConsistency::Strong,
        )?
        .ok_or("missing commit record")?;
    Ok(txn::decode_commit_log_record(&bytes)?.committed_at)
}

fn dry_run() -> HistoryGcOptions {
    HistoryGcOptions {
        dry_run: true,
        ..HistoryGcOptions::default()
    }
}

fn retain_lsns(retain: u64) -> HistoryGcOptions {
    HistoryGcOptions {
        commit_log_retain_lsns: Some(retain),
        ..HistoryGcOptions::default()
    }
}

fn counter_row(value: i64) -> Vec<Value> {
    vec![Value::Int(1), Value::Int(value)]
}

fn by_value_view() -> MaterializedViewSpec {
    MaterializedViewSpec {
        name: "by_value".to_owned(),
        source_table: "counters".to_owned(),
        filter: None,
        group_by: 1,
        aggregates: vec![AggregateSpec {
            output_name: "count".to_owned(),
            kind: AggregateKind::Count,
            column: None,
        }],
    }
}

fn post(body: &str, lon: f64, lat: f64) -> Value {
    Value::Object(BTreeMap::from([
        ("body".to_owned(), Value::Str(body.to_owned())),
        ("point".to_owned(), Value::GeoPoint { lon, lat }),
    ]))
}

fn is_retention_expired(result: &Result<SqlOutput, DbError>) -> bool {
    matches!(
        result,
        Err(DbError::Temporal(TemporalError::RetentionExpired { .. }))
    )
}

#[tokio::test]
async fn gc_preserves_as_of_results_at_and_above_watermark() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = database.create_table("counters", Some(counter_schema()), Vec::new())?;
    let plain = database.create_table("plain", Some(counter_schema()), Vec::new())?;
    for table in [&counters, &plain] {
        table.insert(vec![Value::Int(1), Value::Int(0)])?;
        table.insert(vec![Value::Int(2), Value::Int(0)])?;
    }
    let mut mid = 0;
    for step in 1..=30 {
        for table in [&counters, &plain] {
            table.update(vec![Value::Int(1), Value::Int(step)])?;
        }
        if step == 10 {
            for table in [&counters, &plain] {
                table.delete(&Value::Int(2))?;
            }
        }
        if step == 15 {
            mid = current_lsn(&database)?;
        }
    }
    database.enable_system_versioning(
        "counters",
        TemporalRetention {
            min_lsn: mid,
            keep_history: true,
        },
    )?;
    let current = current_lsn(&database)?;

    let mut before = BTreeMap::new();
    for lsn in mid..=current {
        for table in ["counters", "plain"] {
            let sql = format!("SELECT id, value FROM {table} ORDER BY id");
            let output = database.query_as_of(&sql, TemporalPoint::Lsn(lsn)).await?;
            before.insert((table, lsn), output);
        }
    }

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.watermark, mid);
    assert_eq!(
        report.limited_by,
        HistoryWatermarkSource::TemporalRetention {
            table: "counters".to_owned()
        }
    );
    assert!(report.mvcc_versions_pruned > 0);
    assert_eq!(report.history_horizon, mid);
    assert!(report.complete);

    for ((table, lsn), expected) in &before {
        let sql = format!("SELECT id, value FROM {table} ORDER BY id");
        let output = database.query_as_of(&sql, TemporalPoint::Lsn(*lsn)).await?;
        assert_eq!(&output, expected, "{table} AS OF {lsn}");
    }

    let plain_below = database
        .query_as_of("SELECT id, value FROM plain", TemporalPoint::Lsn(mid - 1))
        .await;
    assert!(
        matches!(
            plain_below,
            Err(DbError::Temporal(TemporalError::RetentionExpired { earliest, .. })) if earliest == mid
        ),
        "{plain_below:?}"
    );
    let counters_below = database
        .query_as_of(
            "SELECT id, value FROM counters",
            TemporalPoint::Lsn(mid - 1),
        )
        .await;
    assert!(is_retention_expired(&counters_below), "{counters_below:?}");
    assert!(matches!(
        crate::temporal::rows_as_of_lsn(&database, "plain", &counter_schema(), mid - 1),
        Err(TemporalError::RetentionExpired { .. })
    ));
    Ok(())
}

fn assert_gc_keeps_open_transaction_snapshot(mut database: Database) -> TestResult {
    let counters = seed_counter(&mut database, 0)?;
    let mut open = database.begin_transaction()?;
    for value in 1..=5 {
        counters.update(vec![Value::Int(1), Value::Int(value)])?;
    }

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.watermark, report.current_lsn);
    assert_eq!(report.limited_by, HistoryWatermarkSource::CurrentLsn);
    assert!(report.mvcc_versions_pruned > 0);

    assert_eq!(
        open.get_row("counters", &Value::Int(1))?,
        Some(counter_row(0))
    );
    open.update_row("counters", vec![Value::Int(1), Value::Int(99)])?;
    assert!(open.commit().is_err_and(|error| error.is_conflict()));

    let fresh = database.begin_transaction()?;
    assert_eq!(
        fresh.get_row("counters", &Value::Int(1))?,
        Some(counter_row(5))
    );
    Ok(())
}

#[test]
fn gc_does_not_disturb_open_transactions() -> TestResult {
    assert_gc_keeps_open_transaction_snapshot(create_database(DbConfig::new(Profile::InMemory))?)?;

    let temp = tempfile::tempdir()?;
    let path = temp.path().join("open-txn.redb");
    assert_gc_keeps_open_transaction_snapshot(create_database(DbConfig::on_disk(
        Profile::Balanced,
        &path,
    ))?)
}

#[tokio::test]
async fn gc_watermark_respects_as_of_pins_and_releases_them() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = seed_counter(&mut database, 0)?;
    let pinned = current_lsn(&database)?;
    for value in 1..=3 {
        counters.update(vec![Value::Int(1), Value::Int(value)])?;
    }
    let current = current_lsn(&database)?;

    {
        let _pin = database.pin_history_lsn(pinned)?;
        let report = database.gc_history(&dry_run())?;
        assert_eq!(report.watermark, pinned);
        assert_eq!(report.limited_by, HistoryWatermarkSource::AsOfPin);
        assert!(report.dry_run);
        assert_eq!(report.mvcc_versions_pruned, 0);
        assert_eq!(report.pending_commits, pinned);
    }
    assert_eq!(database.gc_history(&dry_run())?.watermark, current);

    database
        .query_as_of("SELECT id, value FROM counters", TemporalPoint::Lsn(pinned))
        .await?;
    assert_eq!(database.gc_history(&dry_run())?.watermark, current);
    assert!(
        database
            .query_as_of("SELECT * FROM missing_table", TemporalPoint::Lsn(pinned))
            .await
            .is_err()
    );
    assert_eq!(database.gc_history(&dry_run())?.watermark, current);

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.history_horizon, current);
    assert!(matches!(
        database.pin_history_lsn(pinned),
        Err(DbError::Temporal(TemporalError::RetentionExpired { requested, earliest }))
            if requested == pinned && earliest == current
    ));
    let report = database.gc_history(&dry_run())?;
    assert_eq!(report.watermark, current);
    assert_eq!(report.limited_by, HistoryWatermarkSource::CurrentLsn);
    Ok(())
}

#[test]
fn gc_watermark_respects_temporal_retention_only() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 6)?;
    let min_lsn = current_lsn(&database)? - 3;
    database.enable_system_versioning(
        "counters",
        TemporalRetention {
            min_lsn,
            keep_history: true,
        },
    )?;
    let report = database.gc_history(&dry_run())?;
    assert_eq!(report.watermark, min_lsn);
    assert_eq!(
        report.limited_by,
        HistoryWatermarkSource::TemporalRetention {
            table: "counters".to_owned()
        }
    );

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 6)?;
    database.enable_system_versioning("counters", TemporalRetention::default())?;
    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.watermark, 0);
    assert_eq!(report.mvcc_versions_pruned, 0);
    assert_eq!(report.history_horizon, 0);
    assert!(report.complete);

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 6)?;
    database.enable_system_versioning(
        "counters",
        TemporalRetention {
            min_lsn: 1,
            keep_history: false,
        },
    )?;
    let report = database.gc_history(&dry_run())?;
    assert_eq!(report.watermark, current_lsn(&database)?);
    assert_eq!(report.limited_by, HistoryWatermarkSource::CurrentLsn);

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 6)?;
    database.create_subscription(SubscriptionConfig::new("stale", ResumeToken::default()))?;
    let report = database.gc_history(&dry_run())?;
    assert_eq!(report.watermark, current_lsn(&database)?);
    assert_eq!(report.limited_by, HistoryWatermarkSource::CurrentLsn);
    Ok(())
}

#[test]
fn gc_truncation_respects_durable_consumers() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = seed_counter(&mut database, 5)?;
    database.create_subscription(SubscriptionConfig::new("sub", ResumeToken::default()))?;
    let acked = current_lsn(&database)? - 3;
    database.ack_subscription("sub", ResumeToken::new("default", acked))?;
    counters.update(vec![Value::Int(1), Value::Int(6)])?;
    let report = database.gc_history(&retain_lsns(1))?;
    assert_eq!(report.commit_log_floor_target, Some(acked + 1));
    assert_eq!(
        report.commit_log_limited_by,
        Some(HistoryWatermarkSource::Subscription {
            name: "sub".to_owned()
        })
    );
    assert_eq!(report.commit_log_floor, acked + 1);
    assert_eq!(report.watermark, report.current_lsn);

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = seed_counter(&mut database, 5)?;
    let acked = current_lsn(&database)? - 2;
    database.create_continuous_query(&ContinuousQuerySpec {
        name: "cq".to_owned(),
        sql: "SELECT * FROM counters".to_owned(),
        filter: ChangefeedFilter::default(),
        start: ResumeToken::new("default", acked),
        buffer_limit: 8,
    })?;
    counters.update(vec![Value::Int(1), Value::Int(6)])?;
    // Advance only the underlying subscription so the continuous query is the slowest consumer.
    database.ack_subscription("cq", ResumeToken::new("default", current_lsn(&database)?))?;
    let report = database.gc_history(&retain_lsns(1))?;
    assert_eq!(report.commit_log_floor_target, Some(acked + 1));
    assert_eq!(
        report.commit_log_limited_by,
        Some(HistoryWatermarkSource::ContinuousQuery {
            name: "cq".to_owned()
        })
    );
    assert_eq!(report.watermark, report.current_lsn);

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = seed_counter(&mut database, 3)?;
    let view = database.create_materialized_view(&by_value_view())?;
    for value in 4..=5 {
        counters.update(vec![Value::Int(1), Value::Int(value)])?;
    }
    let report = database.gc_history(&retain_lsns(1))?;
    assert_eq!(report.commit_log_floor_target, Some(view.fresh_to_lsn + 1));
    assert_eq!(
        report.commit_log_limited_by,
        Some(HistoryWatermarkSource::MaterializedView {
            name: "by_value".to_owned()
        })
    );
    assert_eq!(report.watermark, report.current_lsn);
    let refreshed = database.refresh_materialized_view_to_current("by_value")?;
    assert!(refreshed.fresh_to_lsn > view.fresh_to_lsn);

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 3)?;
    // A cataloged index without a stored refresh position counts as LSN 0.
    database.catalog.insert(
        "fts".to_owned(),
        CatalogEntry::FullTextIndex {
            config: FullTextIndexConfig::collection(
                "fts",
                CollectionId::new(7),
                FieldPath::new(["body"]),
            ),
        },
    );
    let report = database.gc_history(&retain_lsns(1))?;
    assert_eq!(report.commit_log_floor_target, Some(1));
    assert_eq!(
        report.commit_log_limited_by,
        Some(HistoryWatermarkSource::FullTextIndex {
            name: "fts".to_owned()
        })
    );
    assert_eq!(report.commit_log_records_pruned, 0);
    assert_eq!(report.watermark, report.current_lsn);
    Ok(())
}

#[tokio::test]
async fn gc_truncates_commit_log_only_with_explicit_retention() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = seed_counter(&mut database, 3)?;
    for value in 4..=6 {
        thread::sleep(Duration::from_millis(2));
        counters.update(vec![Value::Int(1), Value::Int(value)])?;
    }
    let current = current_lsn(&database)?;
    let log_rows = row_count(&database, txn::COMMIT_LOG_TABLE)?;

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.commit_log_floor_target, None);
    assert_eq!(report.commit_log_records_pruned, 0);
    assert_eq!(row_count(&database, txn::COMMIT_LOG_TABLE)?, log_rows);
    database.poll_changefeed(
        &ResumeToken::default(),
        &ChangefeedFilter::default(),
        &ChangefeedOptions::default(),
        1_000,
    )?;

    assert!(matches!(
        database.gc_history(&retain_lsns(0)),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    let truncated_at = commit_time(&database, current - 2)?;
    assert!(truncated_at < commit_time(&database, current - 1)?);
    let report = database.gc_history(&retain_lsns(2))?;
    assert_eq!(report.commit_log_floor_target, Some(current - 1));
    assert_eq!(
        report.commit_log_limited_by,
        Some(HistoryWatermarkSource::RetainLsns)
    );
    assert_eq!(report.commit_log_floor, current - 1);
    assert!(report.complete);
    assert_eq!(txn::history::earliest_commit_lsn(&database)?, current - 1);
    assert_eq!(row_count(&database, txn::COMMIT_LOG_TABLE)?, 2);

    assert!(matches!(
        database.poll_changefeed(
            &ResumeToken::default(),
            &ChangefeedFilter::default(),
            &ChangefeedOptions::default(),
            1_000,
        ),
        Err(DbError::Cdc(FeedError::TokenExpired { requested: 0, .. }))
    ));
    let (_, next) = database.poll_changefeed(
        &ResumeToken::new("default", current - 2),
        &ChangefeedFilter::default(),
        &ChangefeedOptions::default(),
        1_000,
    )?;
    assert_eq!(next.lsn, current);

    let as_of_truncated = database
        .query_as_of(
            "SELECT id, value FROM counters",
            TemporalPoint::Timestamp(truncated_at),
        )
        .await;
    assert!(
        matches!(
            as_of_truncated,
            Err(DbError::Temporal(TemporalError::MissingPoint(_)))
        ),
        "{as_of_truncated:?}"
    );
    Ok(())
}

#[tokio::test]
async fn gc_fails_closed_on_unreadable_inputs() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 5)?;
    propose_system(
        &database,
        Op::Put {
            table: cdc::CDC_SUBSCRIPTIONS_TABLE.to_owned(),
            key: b"broken".to_vec(),
            value: b"not json".to_vec(),
        },
    )?;
    let mvcc_rows = row_count(&database, txn::TXN_MVCC_TABLE)?;
    let log_rows = row_count(&database, txn::COMMIT_LOG_TABLE)?;

    assert!(matches!(
        database.gc_history(&retain_lsns(1)),
        Err(DbError::Serde(_))
    ));
    assert_eq!(row_count(&database, txn::TXN_MVCC_TABLE)?, mvcc_rows);
    assert_eq!(row_count(&database, txn::COMMIT_LOG_TABLE)?, log_rows);

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert!(report.mvcc_versions_pruned > 0);

    let pins = Arc::clone(&database.history_pins);
    let poisoner = thread::spawn(move || {
        let _guard = pins.lock();
        panic!("poisoning the history retention registry for this test");
    });
    assert!(poisoner.join().is_err());
    assert!(database.history_pins.is_poisoned());

    assert!(matches!(
        database.gc_history(&dry_run()),
        Err(DbError::Storage(StorageError::Backend(_)))
    ));
    let open = database.begin_transaction()?;
    assert_eq!(
        open.get_row("counters", &Value::Int(1))?,
        Some(counter_row(5))
    );
    database
        .query_as_of(
            "SELECT id, value FROM counters",
            TemporalPoint::Lsn(current_lsn(&database)?),
        )
        .await?;
    Ok(())
}

#[test]
fn gc_is_unsupported_for_replicated_databases() -> TestResult {
    let ap = create_ap_database(
        DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
        ApClusterConfig::single_node_for_tests(1),
    )?;
    assert!(matches!(
        ap.gc_history(&HistoryGcOptions::default()),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    let sharded = create_sharded_database(
        DbConfig::new(Profile::InMemory),
        ShardedDatabaseConfig::new(
            PartitionStrategy::HashSlots { partitions: 16 },
            1,
            vec![
                ShardSpec::new(
                    1,
                    ShardBackendConfig::Local(DbConfig::new(Profile::InMemory)),
                ),
                ShardSpec::new(
                    2,
                    ShardBackendConfig::Local(DbConfig::new(Profile::InMemory)),
                ),
            ],
        ),
    )?;
    assert!(matches!(
        sharded.gc_history(&dry_run()),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));
    Ok(())
}

#[test]
fn gc_runs_concurrently_with_writers() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 0)?;
    let database = Arc::new(database);

    let writers = (0..4)
        .map(|_| {
            let database = Arc::clone(&database);
            thread::spawn(move || -> Result<(), DbError> {
                for _ in 0..50 {
                    database.transaction_with_retry(10_000, |txn| {
                        let value = match txn.get_row("counters", &Value::Int(1))? {
                            Some(row) => match row.get(1) {
                                Some(Value::Int(value)) => *value,
                                other => {
                                    return Err(DbError::TransactionAborted(format!(
                                        "unexpected counter value {other:?}"
                                    )));
                                }
                            },
                            None => 0,
                        };
                        txn.update_row("counters", vec![Value::Int(1), Value::Int(value + 1)])
                    })?;
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();

    let bounded = HistoryGcOptions {
        batch_size: 1,
        max_batches: Some(2),
        ..HistoryGcOptions::default()
    };
    while !writers.iter().all(thread::JoinHandle::is_finished) {
        database.gc_history(&bounded)?;
        thread::yield_now();
    }
    for writer in writers {
        match writer.join() {
            Ok(result) => result?,
            Err(_) => return Err("counter writer panicked".into()),
        }
    }

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert!(report.complete);
    let fresh = database.begin_transaction()?;
    assert_eq!(
        fresh.get_row("counters", &Value::Int(1))?,
        Some(counter_row(200))
    );
    Ok(())
}

#[test]
fn online_gc_requires_retention_and_bounded_work_and_keeps_the_commit_log() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 20)?;
    let before = row_count(&database, txn::COMMIT_LOG_TABLE)?;
    let options = HistoryGcOptions {
        batch_size: 2,
        max_batches: Some(1),
        mvcc_retain_lsns: Some(5),
        ..HistoryGcOptions::default()
    };
    for invalid in [
        HistoryGcOptions {
            mvcc_retain_lsns: None,
            ..options
        },
        HistoryGcOptions {
            mvcc_retain_lsns: Some(0),
            ..options
        },
        HistoryGcOptions {
            max_batches: None,
            ..options
        },
        HistoryGcOptions {
            max_batches: Some(0),
            ..options
        },
        HistoryGcOptions {
            batch_size: 0,
            ..options
        },
        HistoryGcOptions {
            commit_log_retain_lsns: Some(5),
            ..options
        },
    ] {
        assert!(matches!(
            database.gc_history_online(&invalid),
            Err(DbError::Config(ConfigError::Unsupported(_)))
        ));
    }
    let report = database.gc_history_online(&options)?;
    assert!(report.batches <= 1);
    assert!(!report.complete);
    assert_eq!(report.commit_log_records_pruned, 0);
    assert_eq!(row_count(&database, txn::COMMIT_LOG_TABLE)?, before);
    assert!(report.watermark <= report.current_lsn.saturating_sub(4));
    Ok(())
}

#[test]
fn as_of_pin_cannot_slip_between_gc_watermark_and_horizon_advancement() -> TestResult {
    use std::sync::{TryLockError, mpsc};
    use std::time::Instant;

    use crate::storage::{StorageEngine, WriteTransaction};

    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = seed_counter(&mut database, 0)?;
    let old_lsn = current_lsn(&database)?;
    for value in 1..=10 {
        counters.update(counter_row(value))?;
    }
    let database = Arc::new(database);
    let super::DatabaseRepl::SingleNode(node) = &database.repl else {
        return Err("expected single-node replication".into());
    };
    // Hold the backend writer so maintenance pauses after computing its
    // watermark, before it can advance the horizon or delete any history.
    let writer = node.storage().begin_write()?;
    let gc_database = Arc::clone(&database);
    let gc = thread::spawn(move || {
        gc_database.gc_history_online(&HistoryGcOptions {
            mvcc_retain_lsns: Some(1),
            max_batches: Some(1),
            ..HistoryGcOptions::default()
        })
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !matches!(
        database.history_pins.try_lock(),
        Err(TryLockError::WouldBlock)
    ) {
        if Instant::now() >= deadline {
            writer.rollback();
            let _ = gc.join();
            return Err("GC did not serialize pin registration with its watermark".into());
        }
        thread::yield_now();
    }
    let (sent, received) = mpsc::channel();
    let reader_database = Arc::clone(&database);
    let reader = thread::spawn(move || {
        let result = reader_database.pin_history_lsn(old_lsn).map(|_| ());
        let _ = sent.send(result);
    });
    let early = received.recv_timeout(Duration::from_millis(50));
    writer.rollback();
    let report = gc.join().map_err(|_| "GC thread panicked")??;
    reader.join().map_err(|_| "reader thread panicked")?;
    assert!(
        matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
        "a reader registered below the pending horizon before GC committed"
    );
    assert!(matches!(
        received.recv_timeout(Duration::from_secs(5))?,
        Err(DbError::Temporal(TemporalError::RetentionExpired { requested, earliest }))
            if requested == old_lsn && earliest == report.history_horizon
    ));
    Ok(())
}

#[tokio::test]
async fn gc_state_persists_across_redb_reopen() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("history.redb");
    let (horizon, gc_lsn) = {
        let mut database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
        seed_counter(&mut database, 5)?;
        let report = database.gc_history(&HistoryGcOptions::default())?;
        assert!(report.complete);
        assert!(report.mvcc_versions_pruned > 0);
        (report.history_horizon, current_lsn(&database)?)
    };

    let database = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    let dry = database.gc_history(&dry_run())?;
    assert_eq!(dry.history_horizon, horizon);
    assert_eq!(dry.mvcc_cursor, horizon);
    assert_eq!(current_lsn(&database)?, gc_lsn);
    assert_eq!(dry.pending_commits, 0);

    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.pending_commits, 0);
    assert_eq!(report.mvcc_versions_pruned, 0);
    assert_eq!(report.batches, 0);

    let below = database
        .query_as_of(
            "SELECT id, value FROM counters",
            TemporalPoint::Lsn(horizon - 1),
        )
        .await;
    assert!(is_retention_expired(&below), "{below:?}");
    Ok(())
}

#[test]
fn incremental_backup_fails_closed_after_commit_log_truncation() -> TestResult {
    let temp = tempfile::tempdir()?;
    let mut database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source.redb"),
    ))?;
    let counters = seed_counter(&mut database, 1)?;
    let root = temp.path().join("backups");
    let full = full_backup(&database, &root, &BackupConfig::default())?;
    for value in 2..=4 {
        counters.update(vec![Value::Int(1), Value::Int(value)])?;
    }

    let report = database.gc_history(&retain_lsns(1))?;
    assert!(report.commit_log_floor > full.manifest.end_lsn + 1);

    let result = incremental_backup(&database, &root, &full.path, &BackupConfig::default());
    assert!(
        matches!(
            result,
            Err(BackupError::CommitLogTruncated { parent_end_lsn, earliest_retained_lsn })
                if parent_end_lsn == full.manifest.end_lsn
                    && earliest_retained_lsn == report.commit_log_floor
        ),
        "{result:?}"
    );
    assert_eq!(std::fs::read_dir(&root)?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn full_backup_after_gc_restores_latest_state_and_horizon() -> TestResult {
    let temp = tempfile::tempdir()?;
    let mut database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source.redb"),
    ))?;
    seed_counter(&mut database, 5)?;
    let report = database.gc_history(&HistoryGcOptions::default())?;
    let horizon = report.history_horizon;
    assert!(horizon > 1);

    let backup = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    let restore_path = temp.path().join("restored.redb");
    restore_backup(
        &backup.path,
        DbConfig::on_disk(Profile::Balanced, &restore_path),
        RestoreTarget::Latest,
    )?;
    let restored = open_database(DbConfig::on_disk(Profile::Balanced, &restore_path))?;

    let open = restored.begin_transaction()?;
    assert_eq!(
        open.get_row("counters", &Value::Int(1))?,
        Some(counter_row(5))
    );
    drop(open);
    let below = restored
        .query_as_of(
            "SELECT id, value FROM counters",
            TemporalPoint::Lsn(horizon - 1),
        )
        .await;
    assert!(is_retention_expired(&below), "{below:?}");
    Ok(())
}

#[tokio::test]
async fn gc_mvcc_retain_lsns_keeps_recent_as_of_history() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 6)?;
    let current = current_lsn(&database)?;
    let keep_from = current - 2;
    let sql = "SELECT id, value FROM counters";
    let mut before = BTreeMap::new();
    for lsn in keep_from..=current {
        before.insert(
            lsn,
            database.query_as_of(sql, TemporalPoint::Lsn(lsn)).await?,
        );
    }
    let retain = |lsns| HistoryGcOptions {
        mvcc_retain_lsns: Some(lsns),
        ..HistoryGcOptions::default()
    };

    assert!(matches!(
        database.gc_history(&retain(0)),
        Err(DbError::Config(ConfigError::Unsupported(_)))
    ));

    let report = database.gc_history(&retain(3))?;
    assert_eq!(report.watermark, keep_from);
    assert_eq!(report.limited_by, HistoryWatermarkSource::MvccRetainLsns);
    assert_eq!(report.history_horizon, keep_from);
    assert!(report.mvcc_versions_pruned > 0);
    assert!(report.complete);
    assert_eq!(report.note, None);
    for (lsn, expected) in &before {
        let output = database.query_as_of(sql, TemporalPoint::Lsn(*lsn)).await?;
        assert_eq!(&output, expected, "AS OF {lsn}");
    }
    let expired = database
        .query_as_of(sql, TemporalPoint::Lsn(keep_from - 1))
        .await;
    assert!(is_retention_expired(&expired), "{expired:?}");

    // A retention longer than the history keeps all of it.
    let report = database.gc_history(&HistoryGcOptions {
        dry_run: true,
        ..retain(current + 10)
    })?;
    assert_eq!(report.watermark, 0);
    assert_eq!(report.limited_by, HistoryWatermarkSource::MvccRetainLsns);

    // Lower inputs still win over the retention.
    {
        let _pin = database.pin_history_lsn(keep_from)?;
        let report = database.gc_history(&HistoryGcOptions {
            dry_run: true,
            ..retain(1)
        })?;
        assert_eq!(report.watermark, keep_from);
        assert_eq!(report.limited_by, HistoryWatermarkSource::AsOfPin);
    }

    // Without it the watermark is the current LSN, and the report says what that removes.
    let report = database.gc_history(&HistoryGcOptions::default())?;
    assert_eq!(report.watermark, current);
    assert_eq!(report.limited_by, HistoryWatermarkSource::CurrentLsn);
    assert!(
        report
            .note
            .as_deref()
            .is_some_and(|note| note.contains("--history-retain-lsns")),
        "{:?}",
        report.note
    );
    assert_eq!(report.history_horizon, current);
    Ok(())
}

#[tokio::test]
async fn system_versioned_history_fails_closed_below_gc_horizon() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    seed_counter(&mut database, 5)?;
    let report = database.gc_history(&HistoryGcOptions::default())?;
    let horizon = report.history_horizon;
    assert!(horizon > 1);

    // The history provider must not silently return only the surviving versions.
    let below = crate::temporal::history_rows_for_table(
        &database,
        "counters",
        &counter_schema(),
        TemporalRetention::default(),
    );
    assert!(
        matches!(
            below,
            Err(TemporalError::RetentionExpired { requested: 0, earliest }) if earliest == horizon
        ),
        "{below:?}"
    );

    let rejected = database.enable_system_versioning("counters", TemporalRetention::default());
    assert!(
        matches!(
            rejected,
            Err(DbError::Temporal(TemporalError::RetentionExpired { requested: 0, earliest }))
                if earliest == horizon
        ),
        "{rejected:?}"
    );
    assert!(!database.catalog.contains_key("counters_history"));

    let retention = TemporalRetention {
        min_lsn: horizon,
        keep_history: true,
    };
    database.enable_system_versioning("counters", retention)?;
    let rows = crate::temporal::history_rows_for_table(
        &database,
        "counters",
        &counter_schema(),
        retention,
    )?;
    assert_eq!(rows.len(), 1);
    let SqlOutput::Rows(output) = database.query("SELECT * FROM counters_history").await? else {
        return Err("history query should return rows".into());
    };
    assert_eq!(output.rows.len(), 1);

    // A catalog entry that predates the horizon check fails closed through SQL too.
    database.catalog.insert(
        "legacy_history".to_owned(),
        CatalogEntry::TemporalTable {
            base_table: "counters".to_owned(),
            schema: counter_schema(),
            retention: TemporalRetention::default(),
        },
    );
    // Match the invalidation performed by catalog loading/mutation APIs.
    database.invalidate_plan_cache();
    let legacy = database.query("SELECT * FROM legacy_history").await;
    assert!(
        legacy
            .as_ref()
            .is_err_and(|error| error.to_string().contains("retention expired")),
        "{legacy:?}"
    );
    Ok(())
}

#[test]
fn derived_objects_created_after_truncation_start_at_current_lsn() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    let counters = database.create_table("counters", Some(counter_schema()), Vec::new())?;
    for id in 1..=4 {
        counters.insert(vec![Value::Int(id), Value::Int(id % 2)])?;
    }
    counters.update(vec![Value::Int(4), Value::Int(1)])?;
    counters.delete(&Value::Int(2))?;
    let collection_id = CollectionId::new(42);
    let posts = database.create_collection(
        "posts",
        collection_id,
        vec![DocField::path(
            "body",
            FieldPath::new(["body"]),
            ColumnType::Str,
        )],
        Vec::new(),
    )?;
    posts.insert(&post("rust storage engine", 21.01, 52.23))?;
    posts.insert(&post("garbage collection notes", 2.35, 48.86))?;

    let report = database.gc_history(&retain_lsns(1))?;
    assert!(report.commit_log_floor > 1, "{report:?}");
    assert!(database.commit_log_truncated()?);

    // Rows (1, 1), (3, 1) and (4, 1) survive; replaying from LSN 0 would fail.
    let view = database.create_materialized_view(&by_value_view())?;
    assert_eq!(view.rows, vec![vec![Value::Int(1), Value::Int(3)]]);
    assert!(view.fresh_to_lsn >= report.current_lsn);
    counters.insert(vec![Value::Int(5), Value::Int(0)])?;
    counters.update(vec![Value::Int(1), Value::Int(0)])?;
    let refreshed = database.refresh_materialized_view_to_current("by_value")?;
    assert!(refreshed.fresh_to_lsn > view.fresh_to_lsn);
    assert_eq!(
        refreshed.rows,
        vec![
            vec![Value::Int(0), Value::Int(2)],
            vec![Value::Int(1), Value::Int(2)],
        ]
    );

    let text_config =
        FullTextIndexConfig::collection("posts_text", collection_id, FieldPath::new(["body"]));
    let text = database.create_full_text_index(text_config)?;
    let state = text.state()?.ok_or("missing full-text state")?;
    assert!(state.refreshed_to.lsn >= report.current_lsn);
    assert_eq!(state.indexed_documents, 2);
    assert_eq!(text.search("rust", 10)?.len(), 1);

    let geo_config = GeoIndexConfig::new("posts_geo", collection_id, FieldPath::new(["point"]));
    let geo = database.create_geo_index(geo_config)?;
    let geo_state = geo.state()?.ok_or("missing geo state")?;
    assert!(geo_state.refreshed_to.lsn >= report.current_lsn);
    assert_eq!(geo_state.indexed_points, 2);
    let warsaw = GeoPoint::new(21.0, 52.2)?;
    assert_eq!(geo.within_radius(warsaw, 10_000.0)?.len(), 1);

    database
        .collection("posts")?
        .insert(&post("rust query planner", 21.02, 52.24))?;
    let text = database.full_text_index("posts_text")?;
    text.refresh_incremental(database.catalog())?;
    assert_eq!(text.search("rust", 10)?.len(), 2);
    let geo = database.geo_index("posts_geo")?;
    geo.refresh_incremental(database.catalog())?;
    assert_eq!(geo.within_radius(warsaw, 10_000.0)?.len(), 2);

    // The new consumers do not pin the commit-log floor back to LSN 1.
    let report = database.gc_history(&HistoryGcOptions {
        dry_run: true,
        ..retain_lsns(1)
    })?;
    assert!(
        report
            .commit_log_floor_target
            .is_some_and(|target| target > view.fresh_to_lsn),
        "{report:?}"
    );
    Ok(())
}
