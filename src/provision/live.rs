//! The live snapshot a provisioning plan is computed from, and its fingerprint.
//!
//! [`LiveState::capture`] reads everything a plan depends on from an open single-node database
//! without writing anything: metadata and profile manifest, catalog and table schemas, the
//! in-memory and the persisted RBAC state and audit flag, whether audit and encryption keys are
//! configured, the extension policy, the session performance and the provision record. The
//! fingerprint hashes all of it except document counts, so ordinary writes never invalidate a
//! plan while any change a plan could depend on does.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use super::ProvisionError;
use super::record::RecordState;
use crate::config_spec::hex_prefix;
use crate::db::{CatalogEntry, Database, ProbedMetadata};
use crate::extension::PolicyConfig;
use crate::performance::PerformanceConfig;
use crate::query::TableSchema;
use crate::security::{AuthzPolicy, Permission, PrincipalRegistry, Resource};

/// The fingerprint of the live state of `db` that `ProvisionPlan.live_fingerprint` reports:
/// `live_` and 32 hex characters. It changes whenever anything a plan depends on changes (the
/// catalog, table schemas, RBAC state, audit flag and key, encryption, extension policies,
/// session performance and query governor override, profile manifest or provision record), but
/// not when data is written.
/// Never writes.
/// # Errors
/// Fails for CP, AP and sharded databases, and when the live state cannot be read.
pub fn live_fingerprint(db: &Database) -> Result<String, ProvisionError> {
    Ok(LiveState::capture(db)?.fingerprint())
}

/// What a plan knows about an existing database.
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct LiveState {
    pub metadata: ProbedMetadata,
    pub catalog: BTreeMap<String, CatalogEntry>,
    /// The schema of every typed table, by name.
    pub table_schemas: BTreeMap<String, TableSchema>,
    /// The RBAC state this handle enforces.
    pub authz: AuthzPolicy,
    pub principals: PrincipalRegistry,
    pub audit_enabled: bool,
    pub audit_key_configured: bool,
    pub encryption_configured: bool,
    /// The RBAC state and audit flag persisted in `__authz_policy`; what the next open loads
    /// (every opener does).
    pub persisted_authz: AuthzPolicy,
    pub persisted_principals: PrincipalRegistry,
    pub persisted_audit_enabled: bool,
    pub policy_config: PolicyConfig,
    /// The performance this session runs with.
    pub session_performance: PerformanceConfig,
    /// The query governor of the session was set for this process (`set_query_governor`), so
    /// its two knobs are not compared with the profile manifest.
    pub query_governor_overridden: bool,
    pub record: RecordState,
    /// Document counts of the sources of new full-text and geo indexes. The planner fills it
    /// after projection; the fingerprint leaves it out.
    pub source_row_counts: BTreeMap<String, u64>,
}

impl LiveState {
    /// Reads the live state of `db`. Never writes.
    /// # Errors
    /// Fails for CP, AP and sharded databases, and when metadata, schemas, security state,
    /// policies or the record cannot be read.
    pub(crate) fn capture(db: &Database) -> Result<Self, ProvisionError> {
        let metadata = db.probed_metadata()?;
        let catalog = db.catalog().clone();
        let mut table_schemas = BTreeMap::new();
        for (name, entry) in &catalog {
            if matches!(entry, CatalogEntry::Table { .. })
                && let Some(schema) = db.table(name)?.schema()
            {
                table_schemas.insert(name.clone(), schema.clone());
            }
        }
        let persisted = db.persisted_security_state()?;
        Ok(Self {
            metadata,
            catalog,
            table_schemas,
            authz: db.authz_policy().clone(),
            principals: db.principal_registry().clone(),
            audit_enabled: db.audit_enabled(),
            audit_key_configured: db.audit_key_configured(),
            encryption_configured: db.encryption_configured(),
            persisted_authz: persisted.authz_policy,
            persisted_principals: persisted.principals,
            persisted_audit_enabled: persisted.audit_enabled,
            policy_config: db.policy_config()?,
            session_performance: db.performance_config().clone(),
            query_governor_overridden: db.query_governor_overridden(),
            record: db.read_provision_record()?,
            source_row_counts: BTreeMap::new(),
        })
    }

    /// The in-memory RBAC state or audit flag differs from what is persisted, so an apply would
    /// persist state this handle never loaded (or lose state it holds only in memory).
    pub(crate) fn security_state_stale(&self) -> bool {
        self.authz != self.persisted_authz
            || self.principals != self.persisted_principals
            || self.audit_enabled != self.persisted_audit_enabled
    }

    /// `live_` and 32 hex characters of blake3("multidb-live-state-v1" ‖ canonical JSON of the
    /// state without `source_row_counts`).
    pub(crate) fn fingerprint(&self) -> String {
        let view = FingerprintView {
            metadata: &self.metadata,
            catalog: &self.catalog,
            table_schemas: &self.table_schemas,
            authz: role_views(&self.authz),
            principals: &self.principals,
            audit_enabled: self.audit_enabled,
            audit_key_configured: self.audit_key_configured,
            encryption_configured: self.encryption_configured,
            persisted_authz: role_views(&self.persisted_authz),
            persisted_principals: &self.persisted_principals,
            persisted_audit_enabled: self.persisted_audit_enabled,
            policy_config: &self.policy_config,
            session_performance: &self.session_performance,
            query_governor_overridden: self.query_governor_overridden,
            record: &self.record,
        };
        let json = serde_json::to_string(&view)
            .unwrap_or_else(|error| format!("<unserializable live state: {error}>"));
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"multidb-live-state-v1");
        hasher.update(json.as_bytes());
        format!("live_{}", hex_prefix(hasher.finalize().as_bytes(), 16))
    }
}

/// The serialized form the fingerprint hashes. Roles are listed with their grants as pairs,
/// because grant maps are keyed by `Resource`, which JSON cannot use as an object key.
#[derive(Serialize)]
#[allow(clippy::struct_excessive_bools)]
struct FingerprintView<'a> {
    metadata: &'a ProbedMetadata,
    catalog: &'a BTreeMap<String, CatalogEntry>,
    table_schemas: &'a BTreeMap<String, TableSchema>,
    authz: Vec<RoleView<'a>>,
    principals: &'a PrincipalRegistry,
    audit_enabled: bool,
    audit_key_configured: bool,
    encryption_configured: bool,
    persisted_authz: Vec<RoleView<'a>>,
    persisted_principals: &'a PrincipalRegistry,
    persisted_audit_enabled: bool,
    policy_config: &'a PolicyConfig,
    session_performance: &'a PerformanceConfig,
    query_governor_overridden: bool,
    record: &'a RecordState,
}

#[derive(Serialize)]
struct RoleView<'a> {
    name: &'a str,
    grants: Vec<(&'a Resource, &'a BTreeSet<Permission>)>,
}

fn role_views(policy: &AuthzPolicy) -> Vec<RoleView<'_>> {
    policy
        .roles()
        .values()
        .map(|role| RoleView {
            name: role.name(),
            grants: role.grants().iter().collect(),
        })
        .collect()
}
