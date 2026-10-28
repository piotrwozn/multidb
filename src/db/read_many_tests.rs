use super::{
    DbConfig, OperationalConfig, Profile, ReplError, Replication, create_database_with_ops,
};
use crate::{
    cloud::{TenantConfig, TenantQuota},
    repl::{Op, ReadConsistency},
};

#[test]
fn batch_reads_through_database_and_handles_keep_tenant_query_permits()
-> Result<(), Box<dyn std::error::Error>> {
    let mut quota = TenantQuota::storage_bytes(1 << 20);
    quota.max_concurrent_queries = 1;
    let database = create_database_with_ops(
        DbConfig::new(Profile::InMemory),
        OperationalConfig::new().with_tenant(TenantConfig::new("reader", quota)),
    )?;
    database.propose(Op::Put {
        table: "data".to_owned(),
        key: b"a".to_vec(),
        value: b"value".to_vec(),
    })?;
    let runtime = database.tenant.as_ref().ok_or("missing tenant")?;
    let handle = database.replication_handle();
    let keys = vec![b"a".to_vec(), b"missing".to_vec(), b"a".to_vec()];
    for reader in [&database as &dyn Replication, handle.as_ref()] {
        let held = runtime.try_begin_query()?;
        assert!(matches!(
            reader.read_many("data", &keys, ReadConsistency::Strong),
            Err(ReplError::QuotaExceeded {
                requested: 1,
                remaining: 0,
                ..
            })
        ));
        drop(held);
        assert_eq!(
            reader.read_many("data", &keys, ReadConsistency::Strong)?,
            vec![Some(b"value".to_vec()), None, Some(b"value".to_vec())]
        );
        // Both the rejected and successful batch release their permit.
        drop(runtime.try_begin_query()?);
    }
    Ok(())
}
