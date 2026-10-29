use std::{error::Error, fs, path::Path};

use super::*;
use crate::{
    db::{create_database, open_database},
    repl::{Op, ReadConsistency, Replication},
};

fn source(root: &Path, name: &str) -> Result<Database, Box<dyn Error>> {
    let database = create_database(DbConfig::on_disk(Profile::Balanced, root.join(name)))?;
    database.propose(Op::Put {
        table: "data".to_owned(),
        key: b"key".to_vec(),
        value: name.as_bytes().to_vec(),
    })?;
    Ok(database)
}

#[test]
fn incremental_backup_rejects_a_parent_from_another_database() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let first = source(temp.path(), "first.redb")?;
    let second = source(temp.path(), "second.redb")?;
    let root = temp.path().join("backups");
    let full = full_backup(&first, &root, &BackupConfig::default())?;
    let other = full_backup(&second, &root, &BackupConfig::default())?;
    assert_ne!(full.manifest.timeline_id, other.manifest.timeline_id);
    assert!(matches!(
        incremental_backup(&second, &root, &full.path, &BackupConfig::default()),
        Err(BackupError::Unsupported(_))
    ));
    let repeated = full_backup(&first, &root, &BackupConfig::default())?;
    assert_eq!(full.manifest.timeline_id, repeated.manifest.timeline_id);
    Ok(())
}

#[test]
fn incremental_backup_rejects_divergent_restored_branch_with_same_timeline()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let first = source(temp.path(), "source.redb")?;
    let root = temp.path().join("backups");
    let full = full_backup(&first, &root, &BackupConfig::default())?;
    let fork_path = temp.path().join("fork.redb");
    restore_backup(
        &full.path,
        DbConfig::on_disk(Profile::Balanced, &fork_path),
        RestoreTarget::Latest,
    )?;
    let fork = open_database(DbConfig::on_disk(Profile::Balanced, fork_path))?;
    for (database, value) in [(&first, b"first".as_slice()), (&fork, b"fork".as_slice())] {
        database.propose(Op::Put {
            table: "data".to_owned(),
            key: b"branch".to_vec(),
            value: value.to_vec(),
        })?;
    }
    let parent = full_backup(&first, &root, &BackupConfig::default())?;
    assert_eq!(parent.manifest.timeline_id, full.manifest.timeline_id);
    assert!(
        matches!(incremental_backup(&fork, &root, &parent.path, &BackupConfig::default()), Err(BackupError::Unsupported(message)) if message.contains("divergent"))
    );
    Ok(())
}

#[test]
fn compressed_payload_budget_fails_before_target_and_allows_explicit_override()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = source(temp.path(), "source.redb")?;
    database.propose(Op::Put {
        table: "data".to_owned(),
        key: b"compressible".to_vec(),
        value: vec![b'x'; 128 * 1024],
    })?;
    let full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    let limit = full.manifest.files[0].bytes + 64;
    let config = RestoreConfig::default().with_max_payload_bytes(limit);
    let destination = temp.path().join("restore.redb");
    assert!(
        matches!(restore_backup_with_config(&full.path, DbConfig::on_disk(Profile::Balanced, &destination), RestoreTarget::Latest, &config), Err(BackupError::Unsupported(message)) if message.contains("decompressed backup payload"))
    );
    assert!(!destination.exists());
    assert!(verify_backup_with_config(&full.path, &config).is_err());
    let override_config = RestoreConfig::default().with_max_payload_bytes(16 * 1024 * 1024);
    restore_backup_with_config(
        &full.path,
        DbConfig::on_disk(Profile::Balanced, &destination),
        RestoreTarget::Latest,
        &override_config,
    )?;
    let restored = open_database(DbConfig::on_disk(Profile::Balanced, destination))?;
    assert_eq!(
        restored.read("data", b"compressible", ReadConsistency::Strong)?,
        Some(vec![b'x'; 128 * 1024])
    );
    Ok(())
}

#[test]
fn restore_rejects_zero_budget_and_oversized_manifest() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    assert!(ensure_payload_limit(0).is_err());
    fs::write(
        temp.path().join(MANIFEST_FILE),
        vec![b' '; usize::try_from(MAX_MANIFEST_BYTES)? + 1],
    )?;
    assert!(
        matches!(read_manifest(temp.path()), Err(BackupError::Unsupported(message)) if message.contains("manifest"))
    );
    Ok(())
}

#[test]
fn failed_restore_does_not_publish_an_initialized_empty_database() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = source(temp.path(), "source.redb")?;
    let mut full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    full.manifest.end_lsn += 1;
    write_manifest(&full.path, &full.manifest)?;
    let destination = temp.path().join("restore.redb");
    assert!(matches!(
        restore_backup(
            &full.path,
            DbConfig::on_disk(Profile::Balanced, &destination),
            RestoreTarget::Latest
        ),
        Err(BackupError::Corruption(_))
    ));
    assert!(!destination.exists());
    assert!(
        !fs::read_dir(temp.path())?.any(|entry| entry.is_ok_and(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with(".multidb-restore-")))
    );
    Ok(())
}

#[test]
fn restore_publication_preserves_a_destination_created_during_replay() -> Result<(), Box<dyn Error>>
{
    let temp = tempfile::tempdir()?;
    let destination = temp.path().join("restore.redb");
    let mut config = DbConfig::on_disk(Profile::Balanced, &destination);
    let staging = RestoreStaging::new(&mut config)?;
    fs::write(
        config.path.as_ref().ok_or("missing staging path")?,
        b"complete restored database",
    )?;
    fs::write(&destination, b"concurrent user file")?;
    assert!(staging.publish().is_err());
    assert_eq!(fs::read(destination)?, b"concurrent user file");
    Ok(())
}

#[test]
fn restore_rejects_empty_existing_and_volatile_targets() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let empty = temp.path().join("empty");
    fs::write(&empty, [])?;
    assert!(ensure_restore_target_empty(&DbConfig::on_disk(Profile::Balanced, &empty)).is_err());
    assert_eq!(fs::metadata(&empty)?.len(), 0);
    assert!(ensure_restore_target_empty(&DbConfig::new(Profile::InMemory)).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn restore_rejects_dangling_symlink_without_creating_its_target() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let destination = temp.path().join("restore.redb");
    let outside = temp.path().join("not-a-database");
    std::os::unix::fs::symlink(&outside, &destination)?;
    assert!(
        ensure_restore_target_empty(&DbConfig::on_disk(Profile::Balanced, &destination)).is_err()
    );
    assert!(!outside.exists());
    assert!(fs::symlink_metadata(destination)?.file_type().is_symlink());
    Ok(())
}

#[test]
fn backup_payload_paths_cannot_escape_the_backup_directory() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let base = temp.path().join("backup");
    fs::create_dir(&base)?;
    let bytes = zstd::stream::encode_all(std::io::Cursor::new(b"{}"), 3)?;
    fs::write(temp.path().join("outside.zst"), &bytes)?;
    let entry = BackupFileEntry {
        path: "../outside.zst".to_owned(),
        bytes: bytes.len() as u64,
        checksum: crc32fast::hash(&bytes),
        logical_checksum: crc32fast::hash(b"{}"),
        encryption: None,
    };
    assert!(matches!(
        read_backup_payload::<serde_json::Value>(&base, &entry, None),
        Err(BackupError::Corruption(_))
    ));
    Ok(())
}

#[test]
fn restoring_with_a_missing_envelope_keyring_does_not_create_a_replacement()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let kek = temp.path().join("kek");
    let keyring = temp.path().join("missing-keyring");
    fs::write(&kek, [7; 32])?;
    let config =
        RestoreConfig::default().with_encryption(EncryptionConfig::local_envelope(&keyring, &kek));
    assert!(restore_key_provider(&config).is_err());
    assert!(!keyring.exists());
    Ok(())
}

#[test]
fn duplicate_full_payload_keys_are_rejected_before_floor_checks_or_publication()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = source(temp.path(), "source.redb")?;
    let mut full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    let mut payload: BackupDataPayload = read_backup_payload(
        &full.path,
        required_file(&full.manifest, FULL_DATA_FILE)?,
        None,
    )?;
    payload.records.push(BackupDataRecord {
        table: "data".to_owned(),
        key: b"key".to_vec(),
        value: b"ambiguous replacement".to_vec(),
    });
    full.manifest.files[0] = write_compressed_json(
        &full.path,
        FULL_DATA_FILE,
        &payload,
        &BackupConfig::default(),
        None,
    )?;
    write_manifest(&full.path, &full.manifest)?;
    let destination = temp.path().join("restore.redb");
    assert!(
        matches!(restore_backup(&full.path, DbConfig::on_disk(Profile::Balanced, &destination), RestoreTarget::Latest), Err(BackupError::Corruption(message)) if message.contains("duplicate"))
    );
    assert!(!destination.exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn backups_and_restored_databases_are_private_by_default() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir()?;
    let database = source(temp.path(), "source.redb")?;
    let full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    assert_eq!(
        fs::metadata(&full.path)?.permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(full.path.join(FULL_DATA_FILE))?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let destination = temp.path().join("restored.redb");
    restore_backup(
        &full.path,
        DbConfig::on_disk(Profile::Balanced, &destination),
        RestoreTarget::Latest,
    )?;
    assert_eq!(
        fs::metadata(&destination)?.permissions().mode() & 0o777,
        0o600
    );
    let restored = open_database(DbConfig::on_disk(Profile::Balanced, destination))?;
    assert_eq!(
        restored.read("data", b"key", ReadConsistency::Strong)?,
        Some(b"source.redb".to_vec())
    );
    Ok(())
}
