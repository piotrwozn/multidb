use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::storage::Bytes;

pub const AUDIT_TABLE: &str = "__audit_log";
pub const AUDIT_HEAD_TABLE: &str = "__audit_head";

static AUDIT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub enum Resource {
    Database,
    Table(String),
    Collection(String),
    VectorCollection(String),
    FullTextIndex(String),
    TimeSeries(String),
    Graph(String),
    GeoIndex(String),
    System,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Role {
    name: String,
    grants: BTreeMap<Resource, BTreeSet<Permission>>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Principal {
    name: String,
    roles: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PrincipalRegistry {
    principals: BTreeMap<String, Principal>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuthzPolicy {
    roles: BTreeMap<String, Role>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum AuditOutcome {
    Allowed,
    Denied,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuditEvent {
    pub id: u64,
    pub at_millis: u64,
    pub principal: Option<String>,
    pub action: String,
    pub resource: Resource,
    pub outcome: AuditOutcome,
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<AuditIntegrity>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuditIntegrity {
    #[serde(default)]
    pub sequence: u64,
    pub previous_hash: Option<String>,
    pub hash: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AuditHead {
    pub sequence: u64,
    pub hash: Option<String>,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum AuthzError {
    #[error("principal {principal} lacks {permission:?} on {resource:?}")]
    Denied {
        principal: String,
        resource: Resource,
        permission: Permission,
    },
}

pub trait AuditSink {
    /// Appends one sanitized audit event.
    /// # Errors
    /// Fails when the sink cannot persist or serialize the event.
    fn append(&self, event: &AuditEvent) -> Result<(), String>;
}

#[derive(Default)]
pub struct MemoryAuditSink {
    events: std::sync::Mutex<Vec<AuditEvent>>,
}

#[derive(Clone, Debug)]
pub struct FileAuditSink {
    path: PathBuf,
}

impl Role {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            grants: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn grant(mut self, resource: Resource, permission: Permission) -> Self {
        self.grants.entry(resource).or_default().insert(permission);
        self
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn allows(&self, resource: &Resource, permission: Permission) -> bool {
        self.grants
            .get(resource)
            .is_some_and(|permissions| permissions.contains(&permission))
            || self
                .grants
                .get(&Resource::Database)
                .is_some_and(|permissions| permissions.contains(&permission))
    }

    #[must_use]
    pub fn grants(&self) -> &BTreeMap<Resource, BTreeSet<Permission>> {
        &self.grants
    }
}

impl Principal {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            roles: BTreeSet::new(),
        }
    }

    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.insert(role.into());
        self
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn roles(&self) -> &BTreeSet<String> {
        &self.roles
    }
}

impl PrincipalRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, pg_user: impl Into<String>, principal: Principal) {
        self.principals.insert(pg_user.into(), principal);
    }

    #[must_use]
    pub fn with_principal(mut self, pg_user: impl Into<String>, principal: Principal) -> Self {
        self.insert(pg_user, principal);
        self
    }

    #[must_use]
    pub fn principal_for_user(&self, pg_user: &str) -> Principal {
        self.principals
            .get(pg_user)
            .cloned()
            .unwrap_or_else(|| Principal::new(pg_user))
    }

    #[must_use]
    pub fn principals(&self) -> &BTreeMap<String, Principal> {
        &self.principals
    }
}

impl AuthzPolicy {
    #[must_use]
    pub fn new(roles: impl IntoIterator<Item = Role>) -> Self {
        Self {
            roles: roles
                .into_iter()
                .map(|role| (role.name.clone(), role))
                .collect(),
        }
    }

    #[must_use]
    pub fn allow(mut self, role: Role) -> Self {
        self.roles.insert(role.name.clone(), role);
        self
    }

    /// Checks one permission using default-deny semantics.
    /// # Errors
    /// Returns denied when no role grants the exact permission.
    pub fn authorize(
        &self,
        principal: &Principal,
        resource: &Resource,
        permission: Permission,
    ) -> Result<(), AuthzError> {
        if principal.roles.iter().any(|role_name| {
            self.roles
                .get(role_name)
                .is_some_and(|role| role.allows(resource, permission))
        }) {
            return Ok(());
        }

        Err(AuthzError::Denied {
            principal: principal.name.clone(),
            resource: resource.clone(),
            permission,
        })
    }

    #[must_use]
    pub fn roles(&self) -> &BTreeMap<String, Role> {
        &self.roles
    }
}

impl AuditEvent {
    #[must_use]
    pub fn new(
        principal: Option<&Principal>,
        action: impl Into<String>,
        resource: Resource,
        outcome: AuditOutcome,
        detail: Option<&str>,
    ) -> Self {
        Self {
            id: AUDIT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            at_millis: now_millis(),
            principal: principal.map(|principal| principal.name().to_owned()),
            action: action.into(),
            resource,
            outcome,
            detail: detail.map(sanitize_detail),
            integrity: None,
        }
    }

    #[must_use]
    pub fn key(&self) -> Bytes {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&self.at_millis.to_be_bytes());
        key.extend_from_slice(&self.id.to_be_bytes());
        key
    }

    /// Returns a copy with tamper-evident hash-chain metadata.
    /// # Errors
    /// Fails only when the event cannot be serialized into the canonical hash payload.
    pub fn with_integrity(
        mut self,
        previous_hash: Option<String>,
        sequence: u64,
        key: &[u8; 32],
    ) -> Result<Self, String> {
        let hash = self.integrity_hash(previous_hash.as_deref(), sequence, key)?;
        self.integrity = Some(AuditIntegrity {
            sequence,
            previous_hash,
            hash,
        });
        Ok(self)
    }

    /// Verifies this event against the expected previous hash.
    /// # Errors
    /// Fails when the event is missing integrity metadata or cannot be hashed.
    pub fn verify_integrity_link(
        &self,
        expected_previous: Option<&str>,
        expected_sequence: u64,
        key: &[u8; 32],
    ) -> Result<(), String> {
        let integrity = self
            .integrity
            .as_ref()
            .ok_or_else(|| "audit event missing integrity metadata".to_owned())?;
        if integrity.sequence != expected_sequence {
            return Err("audit event sequence mismatch".to_owned());
        }
        if integrity.previous_hash.as_deref() != expected_previous {
            return Err("audit event previous hash mismatch".to_owned());
        }

        let expected_hash = self.integrity_hash(expected_previous, expected_sequence, key)?;
        if integrity.hash != expected_hash {
            return Err("audit event hash mismatch".to_owned());
        }

        Ok(())
    }

    fn integrity_hash(
        &self,
        previous_hash: Option<&str>,
        sequence: u64,
        key: &[u8; 32],
    ) -> Result<String, String> {
        #[derive(serde::Serialize)]
        struct HashPayload<'event> {
            sequence: u64,
            id: u64,
            at_millis: u64,
            principal: &'event Option<String>,
            action: &'event str,
            resource: &'event Resource,
            outcome: AuditOutcome,
            detail: &'event Option<String>,
            previous_hash: Option<&'event str>,
        }

        let payload = HashPayload {
            sequence,
            id: self.id,
            at_millis: self.at_millis,
            principal: &self.principal,
            action: &self.action,
            resource: &self.resource,
            outcome: self.outcome,
            detail: &self.detail,
            previous_hash,
        };
        let bytes = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
        Ok(to_hex(blake3::keyed_hash(key, &bytes).as_bytes()))
    }
}

impl AuditSink for MemoryAuditSink {
    fn append(&self, event: &AuditEvent) -> Result<(), String> {
        self.events
            .lock()
            .map_err(|_| "audit sink lock poisoned".to_owned())?
            .push(event.clone());
        Ok(())
    }
}

impl MemoryAuditSink {
    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .map_or_else(|_| Vec::new(), |events| events.clone())
    }
}

impl FileAuditSink {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AuditSink for FileAuditSink {
    fn append(&self, event: &AuditEvent) -> Result<(), String> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| error.to_string())?;
        let bytes = serde_json::to_vec(event).map_err(|error| error.to_string())?;
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.write_all(b"\n").map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn sanitize_detail(detail: &str) -> String {
    detail
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '_' | '-' | ':' | '.'))
        .take(160)
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        AuditEvent, AuditOutcome, AuditSink, AuthzPolicy, FileAuditSink, Permission, Principal,
        Resource, Role,
    };

    #[test]
    fn default_denies_without_a_matching_role() {
        let policy = AuthzPolicy::default();
        let principal = Principal::new("alice");

        assert!(
            policy
                .authorize(
                    &principal,
                    &Resource::Table("users".to_owned()),
                    Permission::Read
                )
                .is_err()
        );
    }

    #[test]
    fn role_grants_are_resource_specific() {
        let policy = AuthzPolicy::new([
            Role::new("reader").grant(Resource::Table("users".to_owned()), Permission::Read)
        ]);
        let principal = Principal::new("alice").with_role("reader");

        assert!(
            policy
                .authorize(
                    &principal,
                    &Resource::Table("users".to_owned()),
                    Permission::Read
                )
                .is_ok()
        );
        assert!(
            policy
                .authorize(
                    &principal,
                    &Resource::Table("users".to_owned()),
                    Permission::Write
                )
                .is_err()
        );
        assert!(
            policy
                .authorize(
                    &principal,
                    &Resource::Table("orders".to_owned()),
                    Permission::Read
                )
                .is_err()
        );
    }

    #[test]
    fn audit_details_are_sanitized_and_bounded() {
        let principal = Principal::new("alice");
        let event = AuditEvent::new(
            Some(&principal),
            "query",
            Resource::Database,
            AuditOutcome::Denied,
            Some(&"secret value\nwith tabs\tand a very long tail".repeat(20)),
        );

        let Some(detail) = event.detail else {
            panic!("detail should be present");
        };
        assert!(!detail.contains('\n'));
        assert!(!detail.contains('\t'));
        assert!(detail.len() <= 160);
    }

    #[test]
    fn audit_integrity_detects_tampering() -> Result<(), Box<dyn std::error::Error>> {
        let key = [9_u8; 32];
        let event = AuditEvent::new(
            None,
            "set_authz_policy",
            Resource::System,
            AuditOutcome::Succeeded,
            Some("policy changed"),
        )
        .with_integrity(None, 1, &key)?;

        event.verify_integrity_link(None, 1, &key)?;

        let mut tampered = event.clone();
        tampered.detail = Some("policy changed again".to_owned());
        assert!(tampered.verify_integrity_link(None, 1, &key).is_err());
        Ok(())
    }

    #[test]
    fn file_audit_sink_appends_json_lines() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("audit.jsonl");
        let sink = FileAuditSink::new(&path);
        let key = [9_u8; 32];
        let event = AuditEvent::new(
            None,
            "login",
            Resource::System,
            AuditOutcome::Succeeded,
            Some("ok"),
        )
        .with_integrity(None, 1, &key)?;

        sink.append(&event)?;

        let content = std::fs::read_to_string(path)?;
        assert!(content.contains("\"action\":\"login\""));
        assert!(content.ends_with('\n'));
        Ok(())
    }
}
