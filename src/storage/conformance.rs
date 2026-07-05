use crate::performance::{CompressionAlgorithm, CompressionConfig};
use crate::txn::{self, WriteSet};

use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use super::{
    AnyEngine, Bytes, CompressedEngine, EncryptedEngine, Fault, FaultPlan, FaultPoint, MemEngine,
    RangeIter, ReadTransaction, RedbEngine, SimStorage, StaticKeyProvider, StorageEngine,
    StorageError, WriteTransaction,
};

fn run_conformance<S, F>(mut make: F) -> Result<(), StorageError>
where
    S: StorageEngine,
    F: FnMut() -> Result<S, StorageError>,
{
    put_get_overwrite_and_missing(&make()?)?;
    delete_removes_key(&make()?)?;
    range_is_half_open_and_sorted(&make()?)?;
    rollback_discards_changes(&make()?)?;
    write_transaction_reads_own_changes(&make()?)?;
    read_transaction_keeps_snapshot(&make()?)?;
    committed_changes_are_visible_to_new_reads(&make()?)?;
    write_range_uses_pending_overlay(&make()?)?;
    stale_snapshot_write_conflict_is_detected(&make()?)?;

    Ok(())
}

fn put_get_overwrite_and_missing<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"k", b"v1")?;
    write.put("t", b"k", b"v2")?;
    write.commit()?;

    let read = db.begin_read()?;
    assert_eq!(read.get("t", b"k")?, Some(bytes(b"v2")));
    assert_eq!(read.get("t", b"missing")?, None);

    Ok(())
}

fn delete_removes_key<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"k", b"v")?;
    write.commit()?;

    let mut write = db.begin_write()?;
    write.delete("t", b"k")?;
    write.commit()?;

    let read = db.begin_read()?;
    assert_eq!(read.get("t", b"k")?, None);

    Ok(())
}

fn range_is_half_open_and_sorted<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"c", b"3")?;
    write.put("t", b"a", b"1")?;
    write.put("t", b"b", b"2")?;
    write.commit()?;

    let read = db.begin_read()?;
    assert_eq!(
        keys(read.range("t", b"a", b"c")?)?,
        vec![bytes(b"a"), bytes(b"b")]
    );
    assert_eq!(keys(read.range("t", b"b", b"b")?)?, Vec::<Bytes>::new());
    assert_eq!(
        keys(read.range("t", b"b", &[])?)?,
        vec![bytes(b"b"), bytes(b"c")]
    );

    Ok(())
}

fn rollback_discards_changes<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"k", b"v")?;
    write.rollback();

    let read = db.begin_read()?;
    assert_eq!(read.get("t", b"k")?, None);

    Ok(())
}

fn write_transaction_reads_own_changes<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"k", b"v")?;
    assert_eq!(write.get("t", b"k")?, Some(bytes(b"v")));
    write.delete("t", b"k")?;
    assert_eq!(write.get("t", b"k")?, None);
    write.rollback();

    Ok(())
}

fn read_transaction_keeps_snapshot<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let before = db.begin_read()?;

    let mut write = db.begin_write()?;
    write.put("t", b"k", b"v")?;
    write.commit()?;

    assert_eq!(before.get("t", b"k")?, None);

    Ok(())
}

fn committed_changes_are_visible_to_new_reads<S: StorageEngine>(
    db: &S,
) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"k", b"v")?;
    write.commit()?;

    let after = db.begin_read()?;
    assert_eq!(after.get("t", b"k")?, Some(bytes(b"v")));

    Ok(())
}

fn write_range_uses_pending_overlay<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let mut write = db.begin_write()?;
    write.put("t", b"a", b"1")?;
    write.put("t", b"b", b"2")?;
    write.put("t", b"c", b"3")?;
    write.commit()?;

    let mut write = db.begin_write()?;
    write.delete("t", b"b")?;
    write.put("t", b"d", b"4")?;

    assert_eq!(
        keys(write.range("t", b"a", b"e")?)?,
        vec![bytes(b"a"), bytes(b"c"), bytes(b"d")]
    );

    write.rollback();

    Ok(())
}

fn stale_snapshot_write_conflict_is_detected<S: StorageEngine>(db: &S) -> Result<(), StorageError> {
    let snapshot_id = txn::current_txn_id(db)?;
    let mut first = WriteSet::new();
    first.insert(("t".to_owned(), bytes(b"k")), Some(bytes(b"first")));
    assert_eq!(txn::commit_write_set(db, snapshot_id, first)?, 1);

    let mut second = WriteSet::new();
    second.insert(("t".to_owned(), bytes(b"k")), Some(bytes(b"second")));
    assert!(matches!(
        txn::commit_write_set(db, snapshot_id, second),
        Err(StorageError::Conflict)
    ));

    let read = db.begin_read()?;
    assert_eq!(read.get("t", b"k")?, Some(bytes(b"first")));

    Ok(())
}

fn keys(iter: RangeIter<'_>) -> Result<Vec<Bytes>, StorageError> {
    iter.map(|item| item.map(|(key, _)| key)).collect()
}

fn bytes(value: &[u8]) -> Bytes {
    value.to_vec()
}

#[test]
fn mem_conformance() -> Result<(), StorageError> {
    run_conformance(|| Ok(MemEngine::new()))
}

#[test]
fn redb_conformance() -> Result<(), StorageError> {
    let mut temp_dirs = Vec::new();

    run_conformance(|| {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.redb");
        let engine = RedbEngine::open(path)?;
        temp_dirs.push(temp_dir);
        Ok(engine)
    })
}

#[test]
fn sim_conformance_without_faults() -> Result<(), StorageError> {
    run_conformance(|| Ok(SimStorage::without_faults()))
}

#[test]
fn compressed_memory_conformance() -> Result<(), StorageError> {
    run_conformance(|| {
        Ok(CompressedEngine::new(
            MemEngine::new(),
            compression_config(),
        ))
    })
}

#[test]
fn compressed_redb_conformance() -> Result<(), StorageError> {
    let mut temp_dirs = Vec::new();

    run_conformance(|| {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("compressed.redb");
        let engine = CompressedEngine::new(RedbEngine::open(path)?, compression_config());
        temp_dirs.push(temp_dir);
        Ok(engine)
    })
}

#[test]
fn encrypted_memory_conformance() -> Result<(), StorageError> {
    run_conformance(|| {
        Ok(EncryptedEngine::new(
            MemEngine::new(),
            StaticKeyProvider::new([7; 32]),
        ))
    })
}

#[test]
fn encrypted_redb_conformance() -> Result<(), StorageError> {
    let mut temp_dirs = Vec::new();

    run_conformance(|| {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("encrypted.redb");
        let engine = EncryptedEngine::new(RedbEngine::open(path)?, StaticKeyProvider::new([8; 32]));
        temp_dirs.push(temp_dir);
        Ok(engine)
    })
}

#[test]
fn compressed_encrypted_memory_conformance() -> Result<(), StorageError> {
    run_conformance(|| {
        let encrypted = EncryptedEngine::new(MemEngine::new(), StaticKeyProvider::new([9; 32]));
        Ok(CompressedEngine::new(encrypted, compression_config()))
    })
}

#[test]
fn compressed_encrypted_redb_conformance() -> Result<(), StorageError> {
    let mut temp_dirs = Vec::new();

    run_conformance(|| {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("compressed-encrypted.redb");
        let encrypted =
            EncryptedEngine::new(RedbEngine::open(path)?, StaticKeyProvider::new([10; 32]));
        let engine = CompressedEngine::new(encrypted, compression_config());
        temp_dirs.push(temp_dir);
        Ok(engine)
    })
}

#[test]
fn any_engine_memory_conformance() -> Result<(), StorageError> {
    run_conformance(|| Ok(AnyEngine::memory()))
}

#[test]
fn any_engine_redb_conformance() -> Result<(), StorageError> {
    let mut temp_dirs = Vec::new();

    run_conformance(|| {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("any.redb");
        let engine = AnyEngine::redb(path)?;
        temp_dirs.push(temp_dir);
        Ok(engine)
    })
}

#[test]
fn any_engine_wrappers_conformance() -> Result<(), StorageError> {
    let mut temp_dirs = Vec::new();

    run_conformance(|| {
        let temp_dir = tempfile::tempdir()?;
        let key_path = temp_dir.path().join("key.bin");
        std::fs::write(&key_path, [11; 32])?;
        let engine = AnyEngine::compressed_encrypted_memory(key_path, compression_config())?;
        temp_dirs.push(temp_dir);
        Ok(engine)
    })
}

#[test]
fn mem_writers_are_serialized() -> Result<(), StorageError> {
    writers_are_serialized(&Arc::new(MemEngine::new()))
}

#[test]
fn redb_writers_are_serialized() -> Result<(), StorageError> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("writers.redb");
    writers_are_serialized(&Arc::new(RedbEngine::open(path)?))
}

#[test]
fn sim_writers_are_serialized() -> Result<(), StorageError> {
    writers_are_serialized(&Arc::new(SimStorage::without_faults()))
}

#[test]
fn sim_read_transaction_faults_are_injected() -> Result<(), StorageError> {
    let engine = SimStorage::new(FaultPlan::new(1).with_fault(
        FaultPoint::ReadGet,
        5,
        Fault::CorruptRead { xor: 0xff },
    ));
    let mut write = engine.begin_write()?;
    write.put("t", b"k", b"v")?;
    write.commit()?;

    let read = engine.begin_read()?;
    assert_ne!(read.get("t", b"k")?, Some(bytes(b"v")));

    Ok(())
}

fn writers_are_serialized<S>(db: &Arc<S>) -> Result<(), StorageError>
where
    S: StorageEngine + Send + Sync + 'static,
{
    let (first_ready_tx, first_ready_rx) = mpsc::channel();
    let (release_first_tx, release_first_rx) = mpsc::channel();
    let (second_attempted_tx, second_attempted_rx) = mpsc::channel();
    let (second_acquired_tx, second_acquired_rx) = mpsc::channel();

    let first_db = Arc::clone(db);
    let first = thread::spawn(move || -> Result<(), StorageError> {
        let mut write = first_db.begin_write()?;
        write.put("t", b"first", b"1")?;
        first_ready_tx
            .send(())
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        release_first_rx
            .recv()
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        write.commit()
    });

    first_ready_rx
        .recv()
        .map_err(|error| StorageError::Backend(error.to_string()))?;

    let second_db = Arc::clone(db);
    let second = thread::spawn(move || -> Result<(), StorageError> {
        second_attempted_tx
            .send(())
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let mut write = second_db.begin_write()?;
        second_acquired_tx
            .send(())
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        write.put("t", b"second", b"2")?;
        write.commit()
    });

    second_attempted_rx
        .recv()
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    thread::sleep(Duration::from_millis(50));
    assert!(second_acquired_rx.try_recv().is_err());

    release_first_tx
        .send(())
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    join_storage_thread(first)?;
    join_storage_thread(second)?;
    second_acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .map_err(|error| StorageError::Backend(error.to_string()))?;

    let read = db.begin_read()?;
    assert_eq!(read.get("t", b"first")?, Some(bytes(b"1")));
    assert_eq!(read.get("t", b"second")?, Some(bytes(b"2")));

    Ok(())
}

fn join_storage_thread(
    handle: thread::JoinHandle<Result<(), StorageError>>,
) -> Result<(), StorageError> {
    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(StorageError::Backend("writer thread panicked".to_owned())),
    }
}

fn compression_config() -> CompressionConfig {
    CompressionConfig {
        algorithm: CompressionAlgorithm::Lz4,
        min_bytes: 1,
        zstd_level: 0,
    }
}

#[test]
fn redb_persists_after_reopen() -> Result<(), StorageError> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("durable.redb");

    {
        let engine = RedbEngine::open(&path)?;
        let mut write = engine.begin_write()?;
        write.put("t", b"k", b"v")?;
        write.commit()?;
    }

    let engine = RedbEngine::open(&path)?;
    let read = engine.begin_read()?;
    assert_eq!(read.get("t", b"k")?, Some(bytes(b"v")));

    Ok(())
}
