use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

use super::{DatabaseMap, MemEngine, Table, read_inner};
use crate::storage::{ReadTransaction, StorageEngine, WriteTransaction};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, PartialEq, Eq)]
struct Pointers {
    root: *const DatabaseMap,
    table: *const Table,
    key: *const u8,
    value: *const u8,
}

fn pointers(engine: &MemEngine, table: &str, key: &[u8]) -> TestResult<Pointers> {
    // Observe addresses without retaining an extra Arc that would itself force COW.
    let root = read_inner(&engine.inner)?;
    let data = root.get(table).ok_or("missing fixture table")?;
    let (stored_key, value) = data.get_key_value(key).ok_or("missing fixture key")?;
    Ok(Pointers {
        root: Arc::as_ptr(&root),
        table: Arc::as_ptr(data),
        key: stored_key.as_ptr(),
        value: value.as_ptr(),
    })
}

fn fixture() -> TestResult<MemEngine> {
    let engine = MemEngine::new();
    let mut write = engine.begin_write()?;
    write.put("changed", b"key", b"before")?;
    write.put("changed", b"stable", &vec![7; 256 * 1024])?;
    write.put("untouched", b"blob", &vec![8; 256 * 1024])?;
    write.commit()?;
    Ok(engine)
}

#[test]
fn commit_without_readers_reuses_root_tables_and_unchanged_payloads() -> TestResult {
    let engine = fixture()?;
    let stable = pointers(&engine, "changed", b"stable")?;
    let changed = pointers(&engine, "changed", b"key")?;
    let untouched = pointers(&engine, "untouched", b"blob")?;
    let mut write = engine.begin_write()?;
    write.put("changed", b"key", b"after")?;
    let payload = vec![9; 256 * 1024];
    write.put("changed", b"new", &payload)?;
    let pending_payload = write
        .pending
        .get(&("changed".to_owned(), b"new".to_vec()))
        .and_then(Option::as_ref)
        .ok_or("missing pending payload")?
        .as_ptr();
    let overlay = write.materialized_table("changed");
    assert_eq!(overlay[b"new".as_slice()].as_ptr(), pending_payload);
    drop(overlay);
    write.delete("changed", b"absent")?;
    write.commit()?;
    assert_eq!(pointers(&engine, "changed", b"stable")?, stable);
    assert_eq!(pointers(&engine, "untouched", b"blob")?, untouched);
    assert_eq!(pointers(&engine, "changed", b"new")?.value, pending_payload);
    let after = pointers(&engine, "changed", b"key")?;
    assert_eq!(after.root, changed.root);
    assert_eq!(after.table, changed.table);
    assert_eq!(after.key, changed.key);
    assert_eq!(
        engine.begin_read()?.get("changed", b"key")?,
        Some(b"after".to_vec())
    );
    Ok(())
}

#[test]
fn active_reader_forks_only_touched_structure_and_shares_all_unchanged_bytes() -> TestResult {
    let engine = fixture()?;
    let old = engine.begin_read()?;
    let mut write = engine.begin_write()?;
    write.put("changed", b"key", b"after")?;
    write.commit()?;
    {
        let current = read_inner(&engine.inner)?;
        assert!(!Arc::ptr_eq(&old.snapshot, &current));
        assert!(!Arc::ptr_eq(&old.snapshot["changed"], &current["changed"]));
        assert!(Arc::ptr_eq(
            &old.snapshot["untouched"],
            &current["untouched"]
        ));
        let (old_key, old_value) = old.snapshot["changed"]
            .get_key_value(b"stable".as_slice())
            .ok_or("missing stable")?;
        let (new_key, new_value) = current["changed"]
            .get_key_value(b"stable".as_slice())
            .ok_or("missing stable")?;
        assert!(Arc::ptr_eq(old_key, new_key));
        assert!(Arc::ptr_eq(old_value, new_value));
    }
    assert_eq!(old.get("changed", b"key")?, Some(b"before".to_vec()));
    let current_pointers = pointers(&engine, "changed", b"stable")?;
    // The old reader holds an earlier root, so it does not force another fork
    // of the current root/table on every later commit.
    let mut write = engine.begin_write()?;
    write.put("changed", b"key", b"third")?;
    write.commit()?;
    assert_eq!(pointers(&engine, "changed", b"stable")?, current_pointers);
    assert_eq!(old.get("changed", b"key")?, Some(b"before".to_vec()));
    assert_eq!(
        engine.begin_read()?.get("changed", b"key")?,
        Some(b"third".to_vec())
    );
    Ok(())
}

#[test]
fn multiple_snapshot_generations_keep_atomic_updates_across_tables() -> TestResult {
    let engine = MemEngine::new();
    let mut initial = engine.begin_write()?;
    for table in ["left", "right"] {
        initial.put(table, b"k", &0_u64.to_be_bytes())?;
    }
    initial.commit()?;
    let first = engine.begin_read()?;
    let mut middle = None;
    for version in 1..=20_u64 {
        let mut write = engine.begin_write()?;
        for table in ["left", "right"] {
            write.put(table, b"k", &version.to_be_bytes())?;
        }
        write.commit()?;
        if version == 5 {
            middle = Some(engine.begin_read()?);
        }
        for table in ["left", "right"] {
            assert_eq!(first.get(table, b"k")?, Some(0_u64.to_be_bytes().to_vec()));
        }
    }
    let middle = middle.ok_or("missing middle snapshot")?;
    let latest = engine.begin_read()?;
    for table in ["left", "right"] {
        assert_eq!(middle.get(table, b"k")?, Some(5_u64.to_be_bytes().to_vec()));
        assert_eq!(
            latest.get(table, b"k")?,
            Some(20_u64.to_be_bytes().to_vec())
        );
    }
    Ok(())
}

#[test]
fn pending_overlay_and_rollback_preserve_the_root_and_share_untouched_values() -> TestResult {
    let engine = fixture()?;
    let before = pointers(&engine, "changed", b"stable")?;
    let mut write = engine.begin_write()?;
    write.put("changed", b"key", b"pending")?;
    write.delete("changed", b"stable")?;
    write.put("changed", b"new", b"new")?;
    assert_eq!(write.get("changed", b"key")?, Some(b"pending".to_vec()));
    assert_eq!(write.get("changed", b"stable")?, None);
    assert_eq!(
        write
            .range("changed", &[], &[])?
            .collect::<Result<Vec<_>, _>>()?,
        vec![
            (b"key".to_vec(), b"pending".to_vec()),
            (b"new".to_vec(), b"new".to_vec())
        ]
    );
    let overlay = write.materialized_table("untouched");
    assert!(Arc::ptr_eq(
        &overlay[b"blob".as_slice()],
        &write.snapshot["untouched"][b"blob".as_slice()]
    ));
    drop(overlay);
    write.rollback();
    assert_eq!(pointers(&engine, "changed", b"stable")?, before);
    assert_eq!(
        engine.begin_read()?.get("changed", b"key")?,
        Some(b"before".to_vec())
    );
    assert_eq!(engine.begin_read()?.get("changed", b"new")?, None);
    Ok(())
}

#[test]
fn a_waiting_writer_observes_the_previous_commit_and_old_readers_stay_isolated() -> TestResult {
    let engine = Arc::new(fixture()?);
    let old = engine.begin_read()?;
    let mut first = engine.begin_write()?;
    first.put("changed", b"key", b"first")?;
    let (ready_tx, ready_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let worker_engine = Arc::clone(&engine);
    let worker = std::thread::spawn(move || -> TestResult {
        ready_tx.send(())?;
        let mut second = worker_engine.begin_write()?;
        acquired_tx.send(second.get("changed", b"key")?)?;
        second.put("changed", b"key", b"second")?;
        second.commit()?;
        Ok(())
    });
    ready_rx.recv_timeout(Duration::from_secs(1))?;
    assert_eq!(
        acquired_rx.recv_timeout(Duration::from_millis(10)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    first.commit()?;
    assert_eq!(
        acquired_rx.recv_timeout(Duration::from_secs(1))?,
        Some(b"first".to_vec())
    );
    worker.join().map_err(|_| "writer worker panicked")??;
    assert_eq!(old.get("changed", b"key")?, Some(b"before".to_vec()));
    assert_eq!(
        engine.begin_read()?.get("changed", b"key")?,
        Some(b"second".to_vec())
    );
    Ok(())
}

#[test]
fn concurrent_writer_read_modify_write_loses_no_updates_with_a_long_reader() -> TestResult {
    let engine = Arc::new(MemEngine::new());
    let mut initial = engine.begin_write()?;
    initial.put("counter", b"k", &0_u64.to_be_bytes())?;
    initial.commit()?;
    let old = engine.begin_read()?;
    let mut workers = Vec::new();
    for _ in 0..8 {
        let engine = Arc::clone(&engine);
        workers.push(std::thread::spawn(move || -> TestResult {
            for _ in 0..16 {
                let mut write = engine.begin_write()?;
                let bytes = write.get("counter", b"k")?.ok_or("missing counter")?;
                let value = u64::from_be_bytes(bytes.as_slice().try_into()?);
                write.put("counter", b"k", &(value + 1).to_be_bytes())?;
                write.commit()?;
            }
            Ok(())
        }));
    }
    for worker in workers {
        worker.join().map_err(|_| "counter writer panicked")??;
    }
    assert_eq!(
        old.get("counter", b"k")?,
        Some(0_u64.to_be_bytes().to_vec())
    );
    assert_eq!(
        engine.begin_read()?.get("counter", b"k")?,
        Some(128_u64.to_be_bytes().to_vec())
    );
    Ok(())
}
