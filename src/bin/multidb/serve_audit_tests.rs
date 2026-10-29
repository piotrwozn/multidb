use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    io::Write,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use super::*;
use multidb::{
    config_spec::DatabaseSpec,
    repl::{Op, ReadConsistency, Replication},
    security::{Permission, Principal, PrincipalRegistry, Resource, Role},
};

type TestResult = Result<(), Box<dyn Error>>;

fn private_key(path: &Path, bytes: &[u8]) -> TestResult {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)?;
    Ok(())
}

fn environment(root: &Path) -> BTreeMap<String, String> {
    [
        ("MULTIDB_RUNTIME_MODE", "production".to_owned()),
        (
            "MULTIDB_DB_PATH",
            root.join("database.redb").display().to_string(),
        ),
        ("MULTIDB_PROFILE", "balanced".to_owned()),
        ("MULTIDB_ADMIN_TOKEN", "test-admin-token".to_owned()),
        ("MULTIDB_PG_PASSWORD", "test-pg-password".to_owned()),
        // This helper tests the storage/audit startup stage before listeners/TLS open.
        ("MULTIDB_PG_TLS_CERT", "test-unused-cert.pem".to_owned()),
        ("MULTIDB_PG_TLS_KEY", "test-unused-key.pem".to_owned()),
        (
            "MULTIDB_AUDIT_KEY_PATH",
            root.join("audit.key").display().to_string(),
        ),
        (
            "MULTIDB_AUDIT_ANCHOR_PATH",
            root.join("audit.anchor").display().to_string(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect()
}

fn config(values: &BTreeMap<String, String>) -> Result<ServeConfig, CliError> {
    serve_config_from_sources(|name| values.get(name).cloned())
}

#[test]
fn production_audit_settings_fail_before_database_creation_and_do_not_leak_keys() -> TestResult {
    let temp = tempfile::tempdir()?;
    let values = environment(temp.path());
    let key = temp.path().join("audit.key");
    private_key(&key, &[77; 32])?;
    for missing in ["MULTIDB_AUDIT_KEY_PATH", "MULTIDB_AUDIT_ANCHOR_PATH"] {
        let mut invalid = values.clone();
        invalid.remove(missing);
        assert!(serve_database(&config(&invalid)?).is_err());
        assert!(!temp.path().join("database.redb").exists());
    }
    fs::write(&key, b"not-a-private-key-secret-material")?;
    let error = serve_database(&config(&values)?)
        .err()
        .ok_or("invalid key accepted")?
        .to_string();
    assert!(!error.contains("not-a-private-key-secret-material"));
    assert!(!temp.path().join("database.redb").exists());
    assert!(!temp.path().join("audit.anchor").exists());
    fs::write(&key, [77; 32])?;
    let mut missing_parent = values.clone();
    missing_parent.insert(
        "MULTIDB_AUDIT_ANCHOR_PATH".to_owned(),
        temp.path()
            .join("absent-parent")
            .join("audit.anchor")
            .display()
            .to_string(),
    );
    assert!(serve_database(&config(&missing_parent)?).is_err());
    assert!(!temp.path().join("absent-parent").exists());
    assert!(!temp.path().join("database.redb").exists());
    let mut invalid = values.clone();
    invalid.insert(
        "MULTIDB_AUDIT_ANCHOR_PATH".to_owned(),
        key.display().to_string(),
    );
    assert!(serve_database(&config(&invalid)?).is_err());
    assert_eq!(fs::read(&key)?, [77; 32]);
    #[cfg(unix)]
    {
        // A dangling database alias must not create redb at the anchor destination.
        let database = temp.path().join("database.redb");
        let anchor = temp.path().join("audit.anchor");
        std::os::unix::fs::symlink(&anchor, &database)?;
        assert!(serve_database(&config(&values)?).is_err());
        assert!(!anchor.exists());
        assert!(fs::symlink_metadata(database)?.file_type().is_symlink());
    }
    Ok(())
}

#[test]
fn production_restart_preserves_private_chain_anchor_and_persisted_rbac() -> TestResult {
    let temp = tempfile::tempdir()?;
    private_key(&temp.path().join("audit.key"), &[77; 32])?;
    let values = environment(temp.path());
    let config = config(&values)?;
    let original_events;
    let expected_principal = Principal::new("service").with_role("reader");
    {
        let mut database = serve_database(&config)?;
        database.ensure_bootstrap_admin_principal()?;
        let admin = database.principal_for_user("admin");
        let policy = database.authz_policy().clone().allow(
            Role::new("reader").grant(Resource::Table("orders".to_owned()), Permission::Read),
        );
        let registry = PrincipalRegistry::new()
            .with_principal("admin", admin.clone())
            .with_principal("service", expected_principal.clone());
        database.set_security_state_as(&admin, policy, registry)?;
        original_events = database.audit_events()?;
        assert!(!original_events.is_empty());
        database.verify_audit_chain()?;
    }
    let anchor = fs::read(temp.path().join("audit.anchor"))?;
    {
        let database = serve_database(&config)?;
        assert_eq!(database.principal_for_user("service"), expected_principal);
        assert_eq!(database.audit_events()?, original_events);
        database.verify_audit_chain()?;
    }
    fs::write(temp.path().join("audit.key"), [78; 32])?;
    assert!(serve_database(&config).is_err());
    assert_eq!(fs::read(temp.path().join("audit.anchor"))?, anchor);
    fs::write(temp.path().join("audit.key"), [77; 32])?;
    let database = serve_database(&config)?;
    assert_eq!(database.audit_events()?, original_events);
    assert_eq!(database.principal_for_user("service"), expected_principal);
    Ok(())
}

#[test]
fn spec_create_and_verify_receive_runtime_audit_paths_before_provisioning() -> TestResult {
    let temp = tempfile::tempdir()?;
    private_key(&temp.path().join("audit.key"), &[77; 32])?;
    let path = temp.path().join("database.redb");
    let spec = DatabaseSpec::from_db_config(
        "audit-startup",
        &DbConfig::on_disk(Profile::Balanced, &path),
    );
    let spec_path = temp.path().join("database.json");
    fs::write(&spec_path, serde_json::to_vec_pretty(&spec)?)?;
    let mut values = environment(temp.path());
    values.insert(
        "MULTIDB_SPEC_PATH".to_owned(),
        spec_path.display().to_string(),
    );
    values.insert("MULTIDB_SPEC_APPLY".to_owned(), "create".to_owned());
    let before;
    {
        let database = serve_database(&config(&values)?)?;
        database.verify_audit_chain()?;
        before = database.audit_events()?;
        assert!(!before.is_empty());
    }
    values.insert("MULTIDB_SPEC_APPLY".to_owned(), "verify".to_owned());
    let configured = config(&values)?;
    assert_eq!(
        configured.audit.key_path.as_deref(),
        Some(temp.path().join("audit.key").as_path())
    );
    let database = serve_database(&configured)?;
    assert_eq!(database.audit_events()?, before);
    database.verify_audit_chain()?;
    Ok(())
}

#[test]
fn explicit_inmemory_production_uses_real_key_and_has_volatile_audit_history() -> TestResult {
    let temp = tempfile::tempdir()?;
    private_key(&temp.path().join("audit.key"), &[77; 32])?;
    let mut values = environment(temp.path());
    values.insert("MULTIDB_PROFILE".to_owned(), "memory".to_owned());
    values.remove("MULTIDB_AUDIT_ANCHOR_PATH");
    let configured = config(&values)?;
    {
        let database = serve_database(&configured)?;
        assert!(database.audit_events_as(&Principal::new("denied")).is_err());
        assert_eq!(database.audit_events()?.len(), 1);
        database.verify_audit_chain()?;
    }
    assert!(serve_database(&configured)?.audit_events()?.is_empty());
    assert!(!temp.path().join("database.redb").exists());
    values.insert("MULTIDB_AUDIT_KEY_PATH".to_owned(), "  ".to_owned());
    assert!(config(&values)?.audit.key_path.is_none());
    assert!(serve_database(&config(&values)?).is_err());
    Ok(())
}

#[test]
#[ignore = "subprocess entrypoint; exercised by production_audit_restart_after_abrupt_process_kill"]
fn audit_crash_worker() -> TestResult {
    let Some(directory) = std::env::var_os("MULTIDB_AUDIT_CRASH_TEST_DIR") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    let database = serve_database(&config(&environment(&directory))?)?;
    assert!(
        database
            .audit_events_as(&Principal::new("denied-before-kill"))
            .is_err()
    );
    database.propose(Op::Put {
        table: "durable".to_owned(),
        key: b"acknowledged".to_vec(),
        value: b"survives".to_vec(),
    })?;
    fs::write(directory.join("ready"), b"committed")?;
    fs::File::open(directory.join("ready"))?.sync_all()?;
    loop {
        thread::park_timeout(Duration::from_millis(100));
    }
}

#[test]
fn production_audit_restart_after_abrupt_process_kill() -> TestResult {
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let temp = tempfile::tempdir()?;
    private_key(&temp.path().join("audit.key"), &[77; 32])?;
    let log = fs::File::create(temp.path().join("worker.log"))?;
    let mut process = Process(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "serve_audit_tests::audit_crash_worker",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MULTIDB_AUDIT_CRASH_TEST_DIR", temp.path())
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while !temp.path().join("ready").exists() {
        if let Some(status) = process.0.try_wait()? {
            return Err(format!(
                "audit worker exited early ({status}): {}",
                fs::read_to_string(temp.path().join("worker.log"))?
            )
            .into());
        }
        if Instant::now() >= deadline {
            return Err("audit worker readiness timed out".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    process.0.kill()?;
    process.0.wait()?;
    let database = serve_database(&config(&environment(temp.path()))?)?;
    assert_eq!(
        database.read("durable", b"acknowledged", ReadConsistency::Strong)?,
        Some(b"survives".to_vec())
    );
    assert_eq!(database.audit_events()?.len(), 1);
    database.verify_audit_chain()?;
    assert!(
        database
            .audit_events_as(&Principal::new("after-recovery"))
            .is_err()
    );
    assert_eq!(database.audit_events()?.len(), 2);
    Ok(())
}
