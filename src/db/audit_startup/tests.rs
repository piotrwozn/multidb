use std::{error::Error, io::Write};

use super::*;
use crate::{
    db::{
        DbConfig, OperationalConfig, Profile, SecurityConfig, create_database,
        create_database_with_ops, open_database, open_database_with_ops,
    },
    repl::{Op, ReadConsistency, Replication},
    security::{
        AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role,
    },
};

type TestResult = Result<(), Box<dyn Error>>;

fn key_file(path: &Path, bytes: &[u8]) -> TestResult {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)?;
    Ok(())
}

fn event(action: &str) -> AuditEvent {
    AuditEvent::new(
        None,
        action,
        Resource::System,
        AuditOutcome::Succeeded,
        None,
    )
}

fn keyed_database(root: &Path) -> Result<(Database, PathBuf, Principal), Box<dyn Error>> {
    let key = root.join("audit.key");
    key_file(&key, &[71; 32])?;
    let admin = Principal::new("operator").with_role("operator");
    let database = create_database_with_ops(
        DbConfig::new(Profile::InMemory),
        OperationalConfig::new().with_security(SecurityConfig {
            authz_policy: AuthzPolicy::new([
                Role::new("operator").grant(Resource::System, Permission::Admin)
            ]),
            audit: AuditConfig {
                key_path: Some(key.clone()),
                anchor_path: None,
            },
            ..SecurityConfig::default()
        }),
    )?;
    Ok((database, key, admin))
}

#[test]
fn key_loader_is_bounded_accepts_raw_and_hex_and_redacts_bad_material() -> TestResult {
    let temp = tempfile::tempdir()?;
    let raw = temp.path().join("raw");
    key_file(&raw, &[71; 32])?;
    assert_eq!(read_key(&raw)?, [71; 32]);
    let hex = temp.path().join("hex");
    key_file(&hex, format!("{}\n", "47".repeat(32)).as_bytes())?;
    assert_eq!(read_key(&hex)?, [71; 32]);
    let huge = temp.path().join("huge");
    key_file(&huge, &vec![b'q'; 1024 * 1024])?;
    let error = read_key(&huge)
        .err()
        .ok_or("oversized key accepted")?
        .to_string();
    assert!(!error.contains("qqqqqq"));
    assert!(!error.contains(&"47".repeat(16)));
    assert!(read_key(&temp.path().join("missing")).is_err());
    Ok(())
}

#[test]
fn a_development_chain_cannot_be_opened_with_a_new_private_key() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("database.redb");
    let key = temp.path().join("private.key");
    key_file(&key, &[71; 32])?;
    let anchor = temp.path().join("new.anchor");
    let before;
    let lsn;
    {
        let database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
        database.record_audit(&event("development_history"))?;
        before = database.audit_events()?;
        lsn = crate::txn::current_txn_id_from(
            &database.local_storage().ok_or("no storage")?.begin_read()?,
        )?;
    }
    let audit = AuditConfig {
        key_path: Some(key),
        anchor_path: Some(anchor.clone()),
    };
    assert!(
        open_database_with_ops(
            DbConfig::on_disk(Profile::Balanced, &path),
            &OperationalConfig::new().with_security(SecurityConfig {
                audit,
                ..SecurityConfig::default()
            })
        )
        .is_err()
    );
    assert!(!anchor.exists());
    let original = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    assert_eq!(original.audit_events()?, before);
    assert_eq!(
        crate::txn::current_txn_id_from(
            &original.local_storage().ok_or("no storage")?.begin_read()?
        )?,
        lsn
    );
    original.verify_audit_chain()?;
    Ok(())
}

#[test]
fn startup_verifies_valid_clock_regression_without_resigning_history() -> TestResult {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let mut first = event("first");
    first.at_millis = 2000;
    let mut second = event("second");
    second.at_millis = 1000;
    database.record_audit(&first)?;
    database.record_audit(&second)?;
    let original = database.audit_events()?;
    database.verify_audit_startup()?;
    assert_eq!(database.audit_events()?, original);
    database.verify_audit_chain()?;
    Ok(())
}

#[test]
fn changed_key_before_admission_refuses_without_mutating_policy_or_audit() -> TestResult {
    let temp = tempfile::tempdir()?;
    let (mut database, key, admin) = keyed_database(temp.path())?;
    let policy = database.authz_policy().clone();
    let before = database.audit_events()?;
    let lsn = crate::txn::current_txn_id_from(
        &database.local_storage().ok_or("no storage")?.begin_read()?,
    )?;
    fs::write(&key, [72; 32])?;
    let requested = policy.clone().allow(Role::new("must_not_be_added"));
    assert!(
        matches!(database.set_security_state_as(&admin, requested, PrincipalRegistry::new()), Err(DbError::AuditIntegrity(message)) if message.contains("key changed"))
    );
    assert_eq!(database.authz_policy(), &policy);
    assert_eq!(database.audit_events()?, before);
    assert_eq!(
        crate::txn::current_txn_id_from(
            &database.local_storage().ok_or("no storage")?.begin_read()?
        )?,
        lsn
    );
    fs::write(&key, [71; 32])?;
    assert!(
        database
            .set_security_state_as(
                &admin,
                policy,
                PrincipalRegistry::new().with_principal("operator", admin.clone())
            )
            .is_err()
    );
    database.verify_audit_chain()?;
    Ok(())
}

#[test]
fn rotation_after_admission_has_an_unconfirmed_commit_and_seals_later_mutations() -> TestResult {
    let temp = tempfile::tempdir()?;
    let (mut database, key, admin) = keyed_database(temp.path())?;
    let replacement = temp.path().join("replacement.key");
    key_file(&replacement, &[72; 32])?;
    database.authorize_or_audit(
        &admin,
        &Resource::System,
        Permission::Admin,
        "admitted_operation",
    )?;
    // Deterministically place the rotation after admission and a complete durable data
    // write, before its audit append. An error is not proof of transaction rollback.
    database.propose(Op::Put {
        table: "user_data".to_owned(),
        key: b"committed".to_vec(),
        value: b"unconfirmed".to_vec(),
    })?;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let rotate_barrier = Arc::clone(&barrier);
    let rotate_path = key.clone();
    let rotation = std::thread::spawn(move || {
        rotate_barrier.wait();
        fs::rename(replacement, rotate_path)
    });
    barrier.wait();
    rotation
        .join()
        .map_err(|_| "key rotation worker panicked")??;
    assert!(
        database
            .record_audit(&event("must_not_acknowledge"))
            .is_err()
    );
    assert_eq!(
        database.read("user_data", b"committed", ReadConsistency::Strong)?,
        Some(b"unconfirmed".to_vec())
    );
    assert_eq!(database.audit_events()?, [] as [AuditEvent; 0]);
    fs::write(&key, [71; 32])?;
    let policy = database.authz_policy().clone();
    assert!(
        database
            .set_security_state_as(
                &admin,
                policy.clone().allow(Role::new("after_seal")),
                PrincipalRegistry::new()
            )
            .is_err()
    );
    assert_eq!(database.authz_policy(), &policy);
    assert!(database.record_audit(&event("still_sealed")).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn projected_secret_symlink_to_private_target_is_accepted_but_public_key_is_rejected() -> TestResult
{
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir()?;
    let actual = temp.path().join("key-data");
    key_file(&actual, &[71; 32])?;
    fs::set_permissions(&actual, fs::Permissions::from_mode(0o440))?;
    let projected = temp.path().join("key.bin");
    std::os::unix::fs::symlink(&actual, &projected)?;
    let config = AuditConfig {
        key_path: Some(projected),
        anchor_path: None,
    };
    config.validate_startup(None, None, true, false)?;
    fs::set_permissions(&actual, fs::Permissions::from_mode(0o644))?;
    assert!(config.validate_startup(None, None, true, false).is_err());
    Ok(())
}

#[test]
fn production_does_not_accept_the_public_development_key_from_a_private_file() -> TestResult {
    let temp = tempfile::tempdir()?;
    let key = temp.path().join("audit.key");
    key_file(&key, &configured_key(&AuditConfig::default())?)?;
    let config = AuditConfig {
        key_path: Some(key),
        anchor_path: None,
    };
    assert!(
        matches!(config.validate_startup(None, None, true, false), Err(DbError::AuditIntegrity(message)) if message.contains("public development key"))
    );
    Ok(())
}

#[test]
fn an_audit_key_collision_never_overwrites_a_previously_acknowledged_record() -> TestResult {
    let database = create_database(DbConfig::new(Profile::InMemory))?;
    let repeated = event("same_physical_key");
    database.record_audit(&repeated)?;
    let original = database.audit_events()?;
    assert!(
        matches!(database.record_audit(&repeated), Err(DbError::AuditIntegrity(message)) if message.contains("key collision"))
    );
    assert_eq!(database.audit_events()?, original);
    assert_eq!(database.audit_head()?.sequence, 1);
    database.verify_audit_chain()?;
    assert!(
        database
            .record_audit(&event("sealed_after_collision"))
            .is_err()
    );
    Ok(())
}
