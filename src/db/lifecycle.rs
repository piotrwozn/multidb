//! Security initialization for create-or-open operational constructors.

use super::{
    ConfigError, Database, SecurityConfig, read_security_from_repl, write_security_to_repl,
};

impl Database {
    /// Initializes RBAC only when no policy has been persisted yet. Operational
    /// constructors also open existing files, so their defaults must never
    /// replace the operator's roles, principal mappings or audit flag.
    pub(super) fn initialize_security_if_missing(
        &mut self,
        requested: SecurityConfig,
    ) -> Result<(), ConfigError> {
        let security = if let Some(mut persisted) = read_security_from_repl(self.replication_ref())?
        {
            // Key and anchor paths belong to the process, not persisted RBAC.
            persisted.audit = requested.audit;
            persisted
        } else {
            write_security_to_repl(self.replication_ref(), &requested)?;
            requested
        };
        self.apply_security_config(security);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            AuditConfig, ConfigError, DbConfig, OperationalConfig, Profile, SecurityConfig,
            create_database_with_ops,
        },
        security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role},
    };

    #[test]
    fn create_with_ops_preserves_existing_security_instead_of_reinitializing_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("operational-security.redb");
        let alice = Principal::new("alice").with_role("reader");
        let initial = SecurityConfig {
            authz_policy: AuthzPolicy::new([
                Role::new("reader").grant(Resource::Table("orders".to_owned()), Permission::Read),
                Role::new("audit_admin").grant(Resource::System, Permission::Admin),
            ]),
            principals: PrincipalRegistry::new().with_principal("alice", alice.clone()),
            audit_enabled: true,
            audit: AuditConfig::default(),
        };
        {
            let mut database = create_database_with_ops(
                DbConfig::on_disk(Profile::Transactional, &path),
                OperationalConfig::new().with_security(initial.clone()),
            )?;
            database.set_audit_enabled_as(
                &Principal::new("operator").with_role("audit_admin"),
                false,
            )?;
        }

        // Even an explicit different bootstrap policy is not an update request.
        let requested = SecurityConfig {
            authz_policy: AuthzPolicy::new([
                Role::new("replacement").grant(Resource::Database, Permission::Admin)
            ]),
            audit: AuditConfig {
                key_path: Some(temp.path().join("audit.key")),
                anchor_path: None,
            },
            ..SecurityConfig::default()
        };
        let database = create_database_with_ops(
            DbConfig::on_disk(Profile::Transactional, &path),
            OperationalConfig::new().with_security(requested.clone()),
        )?;
        assert_eq!(database.authz_policy(), &initial.authz_policy);
        assert_eq!(database.principal_for_user("alice"), alice);
        assert!(!database.audit_enabled());
        assert_eq!(database.audit_config, requested.audit);
        Ok(())
    }

    #[test]
    fn create_with_ops_refuses_corrupt_persisted_security() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::{
            db::{AUTHZ_TABLE, KEY_AUTHZ_POLICY},
            repl::{Op, propose_system},
        };
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("operational-corrupt-security.redb");
        {
            let database = create_database_with_ops(
                DbConfig::on_disk(Profile::Transactional, &path),
                OperationalConfig::default(),
            )?;
            propose_system(
                &database,
                Op::Put {
                    table: AUTHZ_TABLE.to_owned(),
                    key: KEY_AUTHZ_POLICY.to_vec(),
                    value: b"corrupt policy".to_vec(),
                },
            )?;
        }
        assert!(matches!(
            create_database_with_ops(
                DbConfig::on_disk(Profile::Transactional, &path),
                OperationalConfig::default(),
            ),
            Err(ConfigError::Serde(_))
        ));
        Ok(())
    }
}
