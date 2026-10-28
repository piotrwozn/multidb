use std::{error::Error, fs, time::Duration};

use super::*;
use crate::{
    db::{OperationalConfig, create_database, open_database_with_ops},
    repl::{Op, ReadConsistency, Replication},
    storage::{EnvelopeKeyProvider, LocalFileKms},
};

fn put(database: &Database, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
    database.propose(Op::Put {
        table: "recovery".to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
    })?;
    Ok(())
}

#[test]
fn full_and_incremental_restore_all_source_target_encryption_combinations()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    let key = temp.path().join("source-key");
    let target_key = temp.path().join("target-key");
    fs::write(&key, [11; 32])?;
    fs::write(&target_key, [22; 32])?;
    let source_encryption = EncryptionConfig::file_key(&key);
    let target_encryption = EncryptionConfig::file_key(&target_key);
    put(&database, b"one", b"recovery-secret-marker-one")?;
    for encrypted_source in [false, true] {
        let backup_config = if encrypted_source {
            BackupConfig::default().with_encryption(source_encryption.clone())
        } else {
            BackupConfig::default()
        };
        let root = temp.path().join(format!("backups-{encrypted_source}"));
        let full = full_backup(&database, &root, &backup_config)?;
        put(&database, b"two", b"recovery-secret-marker-two")?;
        let incremental = incremental_backup(&database, &root, &full.path, &backup_config)?;
        for encrypted_target in [false, true] {
            let destination = temp
                .path()
                .join(format!("restored-{encrypted_source}-{encrypted_target}"));
            let mut restore = RestoreConfig::default();
            if encrypted_source {
                restore = restore.with_encryption(source_encryption.clone());
            }
            if encrypted_target {
                restore = restore.with_target_encryption(target_encryption.clone());
            }
            let report = restore_backup_with_config(
                &incremental.path,
                DbConfig::on_disk(Profile::Balanced, &destination),
                RestoreTarget::Latest,
                &restore,
            )?;
            assert_eq!(report.restored_lsn, incremental.manifest.end_lsn);
            let mut ops = OperationalConfig::default();
            ops.encryption.clone_from(&restore.target_encryption);
            let restored =
                open_database_with_ops(DbConfig::on_disk(Profile::Balanced, &destination), &ops)?;
            assert_eq!(
                restored.read("recovery", b"two", ReadConsistency::Strong)?,
                Some(b"recovery-secret-marker-two".to_vec())
            );
            drop(restored);
            if encrypted_target {
                assert!(
                    !fs::read(&destination)?
                        .windows(b"recovery-secret-marker".len())
                        .any(|window| window == b"recovery-secret-marker")
                );
                let wrong = OperationalConfig::new().with_encryption(source_encryption.clone());
                assert!(
                    open_database_with_ops(
                        DbConfig::on_disk(Profile::Balanced, &destination),
                        &wrong
                    )
                    .is_err()
                );
            }
        }
    }
    Ok(())
}

#[test]
fn target_envelope_keys_are_fail_closed_and_creation_is_explicit_and_exclusive()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    put(&database, b"one", b"secret")?;
    let full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    let destination = temp.path().join("target");
    let keyring = temp.path().join("target-keyring");
    let kek = temp.path().join("kek");
    fs::write(&kek, [33; 32])?;
    let encryption = EncryptionConfig::local_envelope(&keyring, &kek);
    let restore = RestoreConfig::default().with_target_encryption(encryption.clone());
    assert!(
        restore_backup_with_config(
            &full.path,
            DbConfig::on_disk(Profile::Balanced, &destination),
            RestoreTarget::Latest,
            &restore
        )
        .is_err()
    );
    assert!(!destination.exists());
    assert!(!keyring.exists());
    let missing_kek = RestoreConfig::default()
        .with_target_encryption(EncryptionConfig::local_envelope(
            &keyring,
            temp.path().join("missing-kek"),
        ))
        .with_target_keyring_creation(true);
    assert!(
        restore_backup_with_config(
            &full.path,
            DbConfig::on_disk(Profile::Balanced, &destination),
            RestoreTarget::Latest,
            &missing_kek
        )
        .is_err()
    );
    assert!(!keyring.exists());
    restore_backup_with_config(
        &full.path,
        DbConfig::on_disk(Profile::Balanced, &destination),
        RestoreTarget::Latest,
        &restore.clone().with_target_keyring_creation(true),
    )?;
    let saved = fs::read(&keyring)?;
    let another = temp.path().join("another-target");
    assert!(
        restore_backup_with_config(
            &full.path,
            DbConfig::on_disk(Profile::Balanced, &another),
            RestoreTarget::Latest,
            &restore.clone().with_target_keyring_creation(true)
        )
        .is_err()
    );
    assert_eq!(fs::read(&keyring)?, saved);
    assert!(!another.exists());
    fs::write(&kek, [44; 32])?;
    assert!(
        restore_backup_with_config(
            &full.path,
            DbConfig::on_disk(Profile::Balanced, &another),
            RestoreTarget::Latest,
            &restore
        )
        .is_err()
    );
    assert!(!another.exists());
    fs::write(&kek, [33; 32])?;
    restore_backup_with_config(
        &full.path,
        DbConfig::on_disk(Profile::Balanced, &another),
        RestoreTarget::Latest,
        &restore,
    )?;
    let ops = OperationalConfig::new().with_encryption(encryption);
    let restored = open_database_with_ops(DbConfig::on_disk(Profile::Balanced, &another), &ops)?;
    assert_eq!(
        restored.read("recovery", b"one", ReadConsistency::Strong)?,
        Some(b"secret".to_vec())
    );
    Ok(())
}

#[test]
fn new_target_keyring_cannot_occupy_the_database_destination_or_a_nested_path()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    let full = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    let destination = temp.path().join("restore");
    let kek = temp.path().join("kek");
    fs::write(&kek, [7; 32])?;
    for keyring in [&destination, &destination.join("keyring")] {
        let restore = RestoreConfig::default()
            .with_target_encryption(EncryptionConfig::local_envelope(keyring, &kek))
            .with_target_keyring_creation(true);
        assert!(
            matches!(restore_backup_with_config(&full.path, DbConfig::on_disk(Profile::Balanced, &destination), RestoreTarget::Latest, &restore), Err(BackupError::Unsupported(message)) if message.contains("non-nested"))
        );
        assert!(!destination.exists());
        assert!(!keyring.exists());
    }
    Ok(())
}

#[test]
fn rotated_backup_keys_recover_old_and_new_payloads_and_destroyed_key_fails_closed()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    let keyring = temp.path().join("backup-keyring");
    let old_kek = temp.path().join("old-kek");
    let new_kek = temp.path().join("new-kek");
    fs::write(&old_kek, [55; 32])?;
    fs::write(&new_kek, [66; 32])?;
    let provider = EnvelopeKeyProvider::create(&keyring, LocalFileKms::new(&old_kek))?;
    let first_key = provider.current_key()?.id();
    put(&database, b"one", b"before-rotation")?;
    let backup_config = BackupConfig::default()
        .with_encryption(EncryptionConfig::local_envelope(&keyring, &old_kek));
    let root = temp.path().join("backups");
    let full = full_backup(&database, &root, &backup_config)?;
    provider.rotate_dek()?;
    put(&database, b"two", b"after-rotation")?;
    let incremental = incremental_backup(&database, &root, &full.path, &backup_config)?;
    assert_ne!(
        full.manifest.files[0]
            .encryption
            .as_ref()
            .map(|metadata| metadata.key_id),
        incremental.manifest.files[0]
            .encryption
            .as_ref()
            .map(|metadata| metadata.key_id)
    );
    provider.rotate_kek_with(&LocalFileKms::new(&new_kek))?;
    let restore = RestoreConfig::default()
        .with_encryption(EncryptionConfig::local_envelope(&keyring, &new_kek));
    let report = verify_backup_with_config(&incremental.path, &restore)?;
    assert_eq!(report.restored_lsn, incremental.manifest.end_lsn);
    let wrong = RestoreConfig::default()
        .with_encryption(EncryptionConfig::local_envelope(&keyring, &old_kek));
    assert!(verify_backup_with_config(&incremental.path, &wrong).is_err());
    provider.destroy_dek(first_key)?;
    let destination = temp.path().join("cannot-recover");
    assert!(
        restore_backup_with_config(
            &incremental.path,
            DbConfig::on_disk(Profile::Balanced, &destination),
            RestoreTarget::Latest,
            &restore
        )
        .is_err()
    );
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn restore_validates_full_incremental_and_empty_incremental_commit_anchors()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    put(&database, b"one", b"first")?;
    let root = temp.path().join("backups");
    let mut full = full_backup(&database, &root, &BackupConfig::default())?;
    let original = full.manifest.end_commit_hash.clone();
    full.manifest.end_commit_hash = Some("incorrect".to_owned());
    write_manifest(&full.path, &full.manifest)?;
    assert!(
        matches!(verify_backup(&full.path), Err(BackupError::Corruption(message)) if message.contains("commit hash"))
    );
    full.manifest.end_commit_hash = original;
    write_manifest(&full.path, &full.manifest)?;
    let mut empty = incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;
    empty.manifest.end_commit_hash = Some("incorrect".to_owned());
    write_manifest(&empty.path, &empty.manifest)?;
    assert!(
        matches!(verify_backup(&empty.path), Err(BackupError::Corruption(message)) if message.contains("commit hash"))
    );
    put(&database, b"two", b"second")?;
    let mut incremental =
        incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;
    incremental.manifest.end_commit_hash = Some("incorrect".to_owned());
    write_manifest(&incremental.path, &incremental.manifest)?;
    assert!(
        matches!(verify_backup(&incremental.path), Err(BackupError::Corruption(message)) if message.contains("commit hash"))
    );
    // Historical manifests omitted this optional anchor. Their checksums, LSN and chain
    // checks still apply, and their supported restore path remains compatible.
    full.manifest.end_commit_hash = None;
    incremental.manifest.end_commit_hash = None;
    write_manifest(&full.path, &full.manifest)?;
    write_manifest(&incremental.path, &incremental.manifest)?;
    verify_backup(&incremental.path)?;
    Ok(())
}

#[test]
fn time_pitr_never_includes_a_future_commit_after_wall_clock_regression()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    put(&database, b"base", b"base")?;
    let root = temp.path().join("backups");
    let full = full_backup(&database, &root, &BackupConfig::default())?;
    put(&database, b"future", b"must-stay-out")?;
    put(&database, b"clock-regressed", b"cannot-skip-earlier-commit")?;
    let mut incremental =
        incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;
    let mut payload: CommitLogPayload = read_backup_payload(
        &incremental.path,
        required_file(&incremental.manifest, WAL_DATA_FILE)?,
        None,
    )?;
    assert_eq!(payload.records.len(), 2);
    let cutoff = full.manifest.taken_at + Duration::from_secs(1);
    payload.records[0].committed_at = cutoff + Duration::from_secs(1);
    payload.records[1].committed_at = cutoff;
    incremental.manifest.end_commit_hash = payload.records.last().map(commit_hash).transpose()?;
    incremental.manifest.files[0] = write_compressed_json(
        &incremental.path,
        WAL_DATA_FILE,
        &payload,
        &BackupConfig::default(),
        None,
    )?;
    write_manifest(&incremental.path, &incremental.manifest)?;
    let destination = temp.path().join("pitr");
    let report = restore_backup(
        &incremental.path,
        DbConfig::on_disk(Profile::Balanced, &destination),
        RestoreTarget::Time(cutoff),
    )?;
    assert_eq!(report.restored_lsn, full.manifest.end_lsn);
    let restored = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &destination),
        &OperationalConfig::default(),
    )?;
    assert_eq!(
        restored.read("recovery", b"future", ReadConsistency::Strong)?,
        None
    );
    assert_eq!(
        restored.read("recovery", b"clock-regressed", ReadConsistency::Strong)?,
        None
    );
    Ok(())
}

#[test]
fn restore_publication_io_faults_leave_either_no_target_or_the_complete_target()
-> Result<(), Box<dyn Error>> {
    for fault in ["file-sync", "link", "directory-sync"] {
        let temp = tempfile::tempdir()?;
        let destination = temp.path().join("restored");
        let mut config = DbConfig::on_disk(Profile::Balanced, &destination);
        let staging = RestoreStaging::new(&mut config)?;
        fs::write(
            config.path.as_ref().ok_or("missing staging path")?,
            b"complete-restored-state",
        )?;
        let result = staging.publish_with(
            |path| {
                if fault == "file-sync" {
                    Err(std::io::ErrorKind::Other.into())
                } else {
                    fs::File::open(path)?.sync_all()
                }
            },
            |source, target| {
                if fault == "link" {
                    Err(std::io::ErrorKind::StorageFull.into())
                } else {
                    fs::hard_link(source, target)
                }
            },
            |path| {
                if fault == "directory-sync" {
                    Err(std::io::ErrorKind::Other.into())
                } else {
                    fileio::sync_parent(path)
                }
            },
        );
        assert!(matches!(result, Err(BackupError::Io(_))));
        if fault == "directory-sync" {
            assert_eq!(fs::read(&destination)?, b"complete-restored-state");
        } else {
            assert!(!destination.exists());
        }
        assert!(
            !fs::read_dir(temp.path())?.any(|entry| entry.is_ok_and(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(".multidb-restore-")))
        );
    }
    Ok(())
}

#[test]
fn incomplete_staging_does_not_hide_missing_manifests_of_published_backups()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let database = create_database(DbConfig::on_disk(
        Profile::Balanced,
        temp.path().join("source"),
    ))?;
    put(&database, b"one", b"first")?;
    let root = temp.path().join("backups");
    let full = full_backup(&database, &root, &BackupConfig::default())?;
    let partial_path = root.join(format!("{PARTIAL_BACKUP_PREFIX}{}", new_id()));
    fs::create_dir(&partial_path)?;
    fs::write(
        partial_path.join("interrupted-payload"),
        b"partial bytes before ENOSPC or crash",
    )?;
    assert_eq!(list_backups(&root)?, vec![full.manifest.clone()]);
    // A failed live attempt is cleaned up rather than becoming a published directory.
    let staging = BackupStaging::new(&root, &new_id())?;
    let attempt_path = staging.path().to_path_buf();
    fs::write(attempt_path.join("partial"), b"short write")?;
    drop(staging);
    assert!(!attempt_path.exists());
    put(&database, b"two", b"second")?;
    let incremental = incremental_backup(&database, &root, &full.path, &BackupConfig::default())?;
    fs::remove_file(full.path.join(MANIFEST_FILE))?;
    assert!(list_backups(&root).is_err());
    let destination = temp.path().join("broken-chain-target");
    assert!(
        restore_backup(
            &incremental.path,
            DbConfig::on_disk(Profile::Balanced, &destination),
            RestoreTarget::Latest
        )
        .is_err()
    );
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn backup_publication_never_replaces_a_preexisting_directory() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let id = new_id();
    let staging = BackupStaging::new(temp.path(), &id)?;
    let path = staging.path().to_path_buf();
    fs::write(path.join("payload"), b"new backup")?;
    let occupied = temp.path().join(id);
    fs::create_dir(&occupied)?;
    assert!(staging.publish().is_err());
    assert!(occupied.is_dir());
    assert_eq!(fs::read_dir(&occupied)?.count(), 0);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn inherited_backup_encryption_never_recreates_a_lost_source_database_keyring()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let keyring = temp.path().join("source-keyring");
    let kek = temp.path().join("source-kek");
    fs::write(&kek, [8; 32])?;
    let database = crate::db::create_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, temp.path().join("source")),
        OperationalConfig::new().with_encryption(EncryptionConfig::local_envelope(&keyring, &kek)),
    )?;
    put(&database, b"cached", b"retained in live key provider")?;
    fs::remove_file(&keyring)?;
    let root = temp.path().join("backups");
    assert!(full_backup(&database, &root, &BackupConfig::default()).is_err());
    assert!(!keyring.exists());
    assert_eq!(list_backups(&root)?, [] as [BackupManifest; 0]);
    // An explicitly chosen alternate key can still save logical data while the live
    // source provider retains its old DEK. It never replaces the missing source ring.
    let alternate_keyring = temp.path().join("alternate-keyring");
    let alternate = BackupConfig::default()
        .with_encryption(EncryptionConfig::local_envelope(&alternate_keyring, &kek));
    let full = full_backup(&database, &root, &alternate)?;
    assert!(alternate_keyring.exists());
    assert!(!keyring.exists());
    verify_backup_with_config(
        &full.path,
        &RestoreConfig::default()
            .with_encryption(EncryptionConfig::local_envelope(&alternate_keyring, &kek)),
    )?;
    Ok(())
}
