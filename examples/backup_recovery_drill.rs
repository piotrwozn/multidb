//! Local recovery drill with real redb files and envelope encryption.
//! Run with a fresh private output directory; this is not a power-loss certification.

use std::{error::Error, fs, io::Write, path::Path, time::SystemTime};

use multidb::{
    backup::{
        BackupConfig, RestoreConfig, RestoreTarget, full_backup, incremental_backup,
        restore_backup_with_config, verify_backup_with_config,
    },
    db::{
        AuditConfig, Database, DbConfig, DbError, EncryptionConfig, OperationalConfig, Profile,
        create_database_with_ops, open_database_with_ops,
    },
    repl::{Op, ReadConsistency, Replication},
    security::Principal,
    storage::{EnvelopeKeyProvider, LocalFileKms},
};
use serde_json::json;

fn private_key(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut key = zeroize::Zeroizing::new([0_u8; 32]);
    getrandom::fill(key.as_mut()).map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(key.as_ref())?;
    file.sync_all()?;
    Ok(())
}

fn write_balance(database: &Database, balance: &[u8]) -> Result<(), Box<dyn Error>> {
    database.propose(Op::Put {
        table: "accounts".to_owned(),
        key: b"alice".to_vec(),
        value: balance.to_vec(),
    })?;
    let before = database.audit_events()?.len();
    if !matches!(
        database.audit_events_as(&Principal::new("recovery-drill-denied")),
        Err(DbError::AuthzDenied(_))
    ) || database.audit_events()?.len() != before + 1
    {
        return Err("expected denied request was not durably audited".into());
    }
    Ok(())
}

fn check_balance(
    path: &Path,
    encryption: &EncryptionConfig,
    audit_key: &Path,
    balance: &[u8],
) -> Result<(), Box<dyn Error>> {
    let mut ops = OperationalConfig::new().with_encryption(encryption.clone());
    ops.security.audit = AuditConfig {
        key_path: Some(audit_key.to_path_buf()),
        anchor_path: Some(path.with_extension("audit.anchor")),
    };
    let database = open_database_with_ops(DbConfig::on_disk(Profile::Balanced, path), &ops)?;
    if database
        .read("accounts", b"alice", ReadConsistency::Strong)?
        .as_deref()
        != Some(balance)
    {
        return Err("recovered balance differs from the expected committed value".into());
    }
    database.verify_audit_chain()?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run(output: &Path) -> Result<(), Box<dyn Error>> {
    let mut directory = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directory.mode(0o700);
    }
    directory.create(output)?;
    let old_kek = output.join("source-old-kek");
    let new_kek = output.join("source-new-kek");
    let target_kek = output.join("target-kek");
    let audit_key = output.join("audit-signing-key");
    for key in [&old_kek, &new_kek, &target_kek, &audit_key] {
        private_key(key)?;
    }
    let source_keyring = output.join("source-keyring.json");
    let target_keyring = output.join("target-keyring.json");
    let source_encryption = EncryptionConfig::local_envelope(&source_keyring, &old_kek);
    EnvelopeKeyProvider::create(&source_keyring, LocalFileKms::new(&old_kek))?;
    let mut operational = OperationalConfig::new().with_encryption(source_encryption);
    operational.security.audit = AuditConfig {
        key_path: Some(audit_key.clone()),
        anchor_path: Some(output.join("source.audit.anchor")),
    };
    let database = create_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, output.join("source.redb")),
        operational,
    )?;
    write_balance(&database, b"100")?;
    let full = full_backup(&database, output.join("backups"), &BackupConfig::default())?;
    let rotated_dek = database.rotate_dek()?;
    write_balance(&database, b"125")?;
    let first = incremental_backup(
        &database,
        output.join("backups"),
        &full.path,
        &BackupConfig::default(),
    )?;
    let time_cutoff = SystemTime::now();
    write_balance(&database, b"175")?;
    let latest = incremental_backup(
        &database,
        output.join("backups"),
        &first.path,
        &BackupConfig::default(),
    )?;
    drop(database);

    // Rotate KEK only with the database closed. All historical live DEKs are rewrapped.
    let provider = EnvelopeKeyProvider::open(&source_keyring, LocalFileKms::new(&old_kek))?;
    provider.rotate_kek_with(&LocalFileKms::new(&new_kek))?;
    drop(provider);
    let source_keys = EncryptionConfig::local_envelope(&source_keyring, &new_kek);
    let target_keys = EncryptionConfig::local_envelope(&target_keyring, &target_kek);
    let options = RestoreConfig::default()
        .with_encryption(source_keys)
        .with_target_encryption(target_keys.clone());
    let latest_path = output.join("restored-latest.redb");
    let report = restore_backup_with_config(
        &latest.path,
        DbConfig::on_disk(Profile::Balanced, &latest_path),
        RestoreTarget::Latest,
        &options.clone().with_target_keyring_creation(true),
    )?;
    check_balance(&latest_path, &target_keys, &audit_key, b"175")?;
    let lsn_path = output.join("restored-lsn.redb");
    let lsn_report = restore_backup_with_config(
        &latest.path,
        DbConfig::on_disk(Profile::Balanced, &lsn_path),
        RestoreTarget::Lsn(first.manifest.end_lsn),
        &options,
    )?;
    check_balance(&lsn_path, &target_keys, &audit_key, b"125")?;
    let time_path = output.join("restored-time.redb");
    let time_report = restore_backup_with_config(
        &latest.path,
        DbConfig::on_disk(Profile::Balanced, &time_path),
        RestoreTarget::Time(time_cutoff),
        &options,
    )?;
    check_balance(&time_path, &target_keys, &audit_key, b"125")?;
    let verified = verify_backup_with_config(&latest.path, &options)?;
    let mut wrong_audit = OperationalConfig::new().with_encryption(target_keys.clone());
    wrong_audit.security.audit.key_path = Some(old_kek.clone());
    let wrong_audit_refused = open_database_with_ops(
        DbConfig::on_disk(Profile::Balanced, &latest_path),
        &wrong_audit,
    )
    .is_err();
    if !wrong_audit_refused {
        return Err("wrong audit signing key unexpectedly opened recovered history".into());
    }

    let missing_keyring = output.join("missing-keyring.json");
    let missing_options = options
        .clone()
        .with_target_encryption(EncryptionConfig::local_envelope(
            &missing_keyring,
            &target_kek,
        ));
    let missing_target = output.join("must-not-exist-missing-key.redb");
    let missing_refused = restore_backup_with_config(
        &latest.path,
        DbConfig::on_disk(Profile::Balanced, &missing_target),
        RestoreTarget::Latest,
        &missing_options,
    )
    .is_err();
    if !missing_refused || missing_target.exists() || missing_keyring.exists() {
        return Err("missing-keyring restore did not fail closed".into());
    }
    let wrong_options = options
        .clone()
        .with_encryption(EncryptionConfig::local_envelope(&source_keyring, &old_kek));
    let wrong_key_refused = verify_backup_with_config(&latest.path, &wrong_options).is_err();
    if !wrong_key_refused {
        return Err("old KEK unexpectedly opened the rewrapped keyring".into());
    }

    // Corrupt the final payload only after successful recoveries, leaving an inspectable
    // damaged chain. Earlier full/incremental payloads remain intact.
    let payload_path = latest.path.join(&latest.manifest.files[0].path);
    let mut bytes = fs::read(&payload_path)?;
    let first_byte = bytes.first_mut().ok_or("empty backup payload")?;
    *first_byte ^= 1;
    fs::write(&payload_path, bytes)?;
    let damaged_target = output.join("must-not-exist-damaged.redb");
    let damaged_refused = restore_backup_with_config(
        &latest.path,
        DbConfig::on_disk(Profile::Balanced, &damaged_target),
        RestoreTarget::Latest,
        &options,
    )
    .is_err();
    if !damaged_refused || damaged_target.exists() {
        return Err("damaged chain restore did not fail before publication".into());
    }

    let evidence = json!({
        "schema_version": 1,
        "engine_version": env!("CARGO_PKG_VERSION"),
        "status": "passed",
        "scope": "local logical recovery; no power-loss or hardware durability certification",
        "source_encrypted": true,
        "target_encrypted": true,
        "private_audit_signatures_verified_for_all_recovered_targets": true,
        "wrong_audit_signing_key_refused": wrong_audit_refused,
        "full_lsn": full.manifest.end_lsn,
        "incremental_lsn": first.manifest.end_lsn,
        "latest_lsn": report.restored_lsn,
        "lsn_pitr_lsn": lsn_report.restored_lsn,
        "time_pitr_lsn": time_report.restored_lsn,
        "verified_files": verified.checked_files,
        "dek_rotation": rotated_dek,
        "kek_rotation_recovered_all_payloads": true,
        "missing_keyring_refused_without_creation": missing_refused,
        "old_kek_refused_after_rewrap": wrong_key_refused,
        "damaged_chain_refused_without_target": damaged_refused,
        "chain_final_payload_intentionally_damaged": true
    });
    let evidence_path = output.join("recovery-evidence.json");
    let evidence_bytes = serde_json::to_vec_pretty(&evidence)?;
    fs::write(&evidence_path, &evidence_bytes)?;
    println!(
        "evidence={} blake3={}",
        evidence_path.display(),
        blake3::hash(&evidence_bytes)
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let output = args
        .next()
        .ok_or("usage: backup_recovery_drill <absent-output-directory>")?;
    if args.next().is_some() {
        return Err("usage: backup_recovery_drill <absent-output-directory>".into());
    }
    run(Path::new(&output))
}
