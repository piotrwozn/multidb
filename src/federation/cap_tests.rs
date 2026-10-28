//! A child test process supplies connection secrets without unsafe environment
//! mutation in a multithreaded Rust test process.
use super::*;
use crate::{
    db::{DbConfig, DbError, Profile, create_database},
    network::{AuthConfig, NetworkConfig, PgServer, ScramCredential, TlsMode},
    query::{ColumnDef, SqlOutput},
    repl::Replication,
    security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role},
};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::oneshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const CHILD: &str = "MULTIDB_FOREIGN_CAP_TEST_CHILD";
const DSN: &str = "MULTIDB_FOREIGN_CAP_TEST_DSN";
const TEST: &str =
    "federation::cap_tests::guarded_sql_cap_is_an_error_instead_of_a_truncated_relation";

fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("payload", ColumnType::Str, false),
        ],
        0,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_sql_cap_is_an_error_instead_of_a_truncated_relation() -> TestResult {
    if std::env::var_os(CHILD).is_some() {
        return child().await;
    }
    let mut remote = create_database(DbConfig::new(Profile::InMemory))?;
    for (name, count) in [("over", 10001), ("exact", 10000)] {
        let table = remote.create_table(name, Some(schema()), Vec::new())?;
        for first in (0..count).step_by(256) {
            let mut ops = Vec::new();
            for index in first..(first + 256).min(count) {
                let payload = if index == 10000 { "TARGET" } else { "noise" };
                ops.extend(table.insert_ops(vec![
                    Value::Int(i64::from(index)),
                    Value::Str(payload.to_owned()),
                ])?);
            }
            remote.propose_batch(ops)?;
        }
    }
    let role = Role::new("reader")
        .grant(Resource::Table("over".to_owned()), Permission::Read)
        .grant(Resource::Table("exact".to_owned()), Permission::Read);
    remote.set_authz_policy(AuthzPolicy::new([role]));
    let mut registry = PrincipalRegistry::new();
    registry.insert("remote", Principal::new("remote").with_role("reader"));
    remote.set_principal_registry(registry);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (stop, stopped) = oneshot::channel();
    let auth = AuthConfig::single_user(
        "remote",
        4096,
        ScramCredential::from_password_for_tests("test-only-password", b"cap-fixture-salt", 4096),
    );
    let server = tokio::spawn(PgServer::serve_until_shutdown(
        listener,
        Arc::new(remote),
        NetworkConfig::new(TlsMode::DisabledForTests, auth),
        async {
            let _ = stopped.await;
        },
    ));
    let executable = std::env::current_exe()?;
    let dsn = format!(
        "host=127.0.0.1 port={port} user=remote password=test-only-password sslmode=disable"
    );
    let child = tokio::task::spawn_blocking(move || {
        std::process::Command::new(executable)
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD, "1")
            .env(DSN, dsn)
            .output()
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), child).await???;
    let _ = stop.send(());
    server.await??;
    assert!(
        result.status.success(),
        "child cap regression failed: {}",
        String::from_utf8_lossy(&result.stdout)
    );
    Ok(())
}

async fn child() -> TestResult {
    let mut database = create_database(DbConfig::new(Profile::InMemory))?;
    for name in ["over", "exact"] {
        database.create_foreign_table_default(
            format!("remote_{name}"),
            schema(),
            ForeignSource::MultidbPgWire {
                connection: SecretRef::EnvVar(DSN.to_owned()),
                table: name.to_owned(),
            },
        )?;
    }
    for sql in [
        "SELECT COUNT(*) FROM remote_over",
        "SELECT id FROM remote_over WHERE LOWER(payload) = 'target'",
        "SELECT COUNT(*) FROM remote_over WHERE LOWER(payload) = 'target'",
    ] {
        let error = database
            .query(sql)
            .await
            .err()
            .ok_or_else(|| format!("returned partial relation for {sql}"))?;
        assert!(
            matches!(error, DbError::Query(QueryError::ResourceLimit(_))),
            "{sql}: {error}"
        );
    }
    for sql in [
        "SELECT COUNT(*) FROM remote_exact",
        "SELECT COUNT(*) FROM remote_exact WHERE LOWER(payload) = 'noise'",
    ] {
        let SqlOutput::Rows(rows) = database.query(sql).await? else {
            return Err("count returned no rows".into());
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(10000)]], "{sql}");
    }
    let SqlOutput::Rows(rows) = database.query("SELECT id FROM remote_over LIMIT 1").await? else {
        return Err("limit returned no rows".into());
    };
    assert_eq!(rows.rows.len(), 1);
    Ok(())
}

#[test]
fn guarded_plan_requests_a_sentinel_only_for_implicit_operator_caps() -> TestResult {
    let options = ForeignTableOptions::default();
    let schema = schema();
    let all = ForeignScanRequest::new(Some(Vec::new()), Vec::new(), None);
    let guarded = remote_plan_with_cap(&schema, "data", &options, &all, true)?;
    assert_eq!(guarded.overflow_limit, Some(10000));
    assert!(guarded.sql.ends_with(" LIMIT 10001"));
    let legacy = remote_plan_with_cap(&schema, "data", &options, &all, false)?;
    assert_eq!(legacy.overflow_limit, None);
    assert!(legacy.sql.ends_with(" LIMIT 10000"));
    let local = ForeignScanRequest::new(
        None,
        vec![ForeignFilter {
            column: 1,
            op: ForeignFilterOp::Eq,
            value: Value::Str("target".to_owned()),
        }],
        None,
    );
    let guarded = remote_plan_with_cap(&schema, "data", &options, &local, true)?;
    assert!(!guarded.sql.contains("LIMIT"));
    assert_eq!(guarded.limit, Some(10001));
    let explicit = ForeignScanRequest::new(None, Vec::new(), Some(1));
    let guarded = remote_plan_with_cap(&schema, "data", &options, &explicit, true)?;
    assert_eq!(guarded.overflow_limit, None);
    assert!(guarded.sql.ends_with(" LIMIT 1"));
    Ok(())
}
