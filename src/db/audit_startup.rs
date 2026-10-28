//! Process-local audit credentials and bounded verification before serve admits writes.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{
    AuditConfig, Database, DbError, EncryptionConfig, EncryptionMode, parse_audit_key_file,
};
use crate::{
    repl::Replication,
    security::{AUDIT_HEAD_TABLE, AUDIT_TABLE, AuditEvent, AuditHead},
    storage::{Bytes, ReadBudget, ReadReservation, ReadTransaction, StorageEngine, StorageError},
};

const MAX_KEY_BYTES: u64 = 66;
const MAX_EVENT_BYTES: usize = 1_048_576;
const VERIFY_BUDGET: usize = 32 * 1_048_576;

fn integrity(message: impl Into<String>) -> DbError {
    DbError::AuditIntegrity(message.into())
}

pub(super) fn configured_key(config: &AuditConfig) -> Result<[u8; 32], DbError> {
    match &config.key_path {
        Some(path) => read_key(path),
        None => Ok(blake3::derive_key(
            "multidb default audit integrity key",
            b"development default audit key",
        )),
    }
}

fn read_key(path: &Path) -> Result<[u8; 32], DbError> {
    if !fs::metadata(path).map_err(StorageError::Io)?.is_file() {
        return Err(integrity("audit key must be an existing regular file"));
    }
    let file = File::open(path).map_err(StorageError::Io)?;
    let metadata = file.metadata().map_err(StorageError::Io)?;
    if !metadata.is_file() || metadata.len() > MAX_KEY_BYTES {
        return Err(integrity(
            "audit key must contain 32 raw bytes or 64 hex characters",
        ));
    }
    let mut bytes = zeroize::Zeroizing::new(Vec::new());
    file.take(MAX_KEY_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(StorageError::Io)?;
    if bytes.len() as u64 > MAX_KEY_BYTES {
        return Err(integrity("audit key exceeds the bounded key-file limit"));
    }
    parse_audit_key_file(&bytes).map_err(integrity)
}

fn private_file(path: &Path) -> Result<(), DbError> {
    let metadata = fs::metadata(path).map_err(StorageError::Io)?;
    if !metadata.is_file() {
        return Err(integrity("audit key/anchor must be a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // A read-only fsGroup is how non-root Kubernetes containers read projected Secrets.
        if metadata.permissions().mode() & 0o027 != 0 {
            return Err(integrity(
                "audit key/anchor must have no world access or group/other write permission (0400, 0600 or trusted-group 0440/0640)",
            ));
        }
    }
    Ok(())
}

impl AuditConfig {
    /// Validates serve's process-local audit settings without changing the database.
    /// Production requires an existing private key and, for durable profiles, an anchor.
    /// Key files are never generated. Anchor parents must already exist; a missing anchor
    /// itself is created only after the stored chain has been verified.
    /// # Errors
    /// Fails for missing/invalid credentials, unsafe aliases, an unusable anchor, or an
    /// invalid signed anchor prefix. It never re-signs historical events.
    pub fn validate_startup(
        &self,
        database_path: Option<&Path>,
        encryption: Option<&EncryptionConfig>,
        production: bool,
        durable: bool,
    ) -> Result<(), DbError> {
        if production && self.key_path.is_none() {
            return Err(integrity(
                "production serve requires MULTIDB_AUDIT_KEY_PATH",
            ));
        }
        if production && durable && self.anchor_path.is_none() {
            return Err(integrity(
                "durable production serve requires MULTIDB_AUDIT_ANCHOR_PATH",
            ));
        }
        let key = configured_key(self)?;
        if production && key == configured_key(&AuditConfig::default())? {
            return Err(integrity(
                "production audit key must differ from the public development key",
            ));
        }
        if production && let Some(path) = &self.key_path {
            private_file(path)?;
        }
        if production
            && let Some(path) = database_path
            && fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink())
            && fs::canonicalize(path).is_err()
        {
            return Err(integrity(
                "production database symlinks must resolve to an existing file; use an absent regular path for a new database",
            ));
        }
        if let Some(anchor) = &self.anchor_path {
            if let Ok(metadata) = fs::symlink_metadata(anchor)
                && (metadata.file_type().is_symlink() || !metadata.is_file())
            {
                return Err(integrity(
                    "audit anchor must be a regular file, not a symlink or directory",
                ));
            }
            let parent = anchor
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            validate_anchor_parent(parent, production)?;
            let anchor_path = resolved_path(anchor)?;
            let mut protected = vec![database_path, self.key_path.as_deref()];
            if let Some(encryption) = encryption {
                protected.push(Some(encryption.key_path.as_path()));
                if let EncryptionMode::LocalEnvelope {
                    keyring_path,
                    kek_path,
                } = &encryption.mode
                {
                    protected.extend([Some(keyring_path.as_path()), Some(kek_path.as_path())]);
                }
            }
            for path in protected.into_iter().flatten() {
                let path = resolved_path(path)?;
                if path.starts_with(&anchor_path) || anchor_path.starts_with(&path) {
                    return Err(integrity(
                        "audit anchor must be separate from the database and cryptographic key files",
                    ));
                }
            }
            let existing_anchor = anchor_head(self, &key)?;
            let fresh_database = match database_path {
                None => true,
                Some(path) => match fs::metadata(path) {
                    Ok(metadata) => metadata.len() == 0,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                    Err(error) => return Err(StorageError::Io(error).into()),
                },
            };
            if fresh_database && existing_anchor != AuditHead::default() {
                return Err(integrity(
                    "a new or volatile database requires an empty/new audit anchor; preserve the existing history",
                ));
            }
            if anchor.try_exists().map_err(StorageError::Io)? {
                if production {
                    private_file(anchor)?;
                }
                // Check append access without modifying evidence.
                OpenOptions::new()
                    .append(true)
                    .open(anchor)
                    .map_err(StorageError::Io)?;
            } else {
                probe_anchor_parent(parent)?;
            }
        }
        Ok(())
    }
}

fn validate_anchor_parent(parent: &Path, production: bool) -> Result<(), DbError> {
    let metadata = fs::metadata(parent).map_err(StorageError::Io)?;
    if !metadata.is_dir() {
        return Err(integrity(
            "audit anchor parent must be an existing directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if production && metadata.permissions().mode() & 0o002 != 0 {
            return Err(integrity(
                "production audit anchor parent must not be world-writable",
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = production;
    Ok(())
}

fn resolved_path(path: &Path) -> Result<PathBuf, DbError> {
    let absolute = std::path::absolute(path).map_err(StorageError::Io)?;
    let mut current = absolute.as_path();
    let mut suffix = Vec::new();
    let mut result = loop {
        match fs::canonicalize(current) {
            Ok(path) => break path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = current
                    .file_name()
                    .ok_or_else(|| integrity("audit output path cannot be resolved"))?;
                suffix.push(name);
                current = current
                    .parent()
                    .ok_or_else(|| integrity("audit output path has no parent"))?;
            }
            Err(error) => return Err(StorageError::Io(error).into()),
        }
    };
    for name in suffix.into_iter().rev() {
        result.push(name);
    }
    Ok(result)
}

fn probe_anchor_parent(parent: &Path) -> Result<(), DbError> {
    struct Probe(PathBuf);
    impl Drop for Probe {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
    let path = parent.join(format!(".multidb-audit-probe-{}", uuid::Uuid::now_v7()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path).map_err(StorageError::Io)?;
    let _cleanup = Probe(path.clone());
    file.sync_all().map_err(StorageError::Io)?;
    drop(file);
    fs::remove_file(&path).map_err(StorageError::Io)?;
    crate::fileio::sync_parent(&path).map_err(StorageError::Io)?;
    Ok(())
}

fn anchor_head(config: &AuditConfig, key: &[u8; 32]) -> Result<AuditHead, DbError> {
    let Some(path) = &config.anchor_path else {
        return Ok(AuditHead::default());
    };
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AuditHead::default());
        }
        Err(error) => return Err(StorageError::Io(error).into()),
    };
    let mut reader = BufReader::new(file);
    let mut head = AuditHead::default();
    loop {
        let mut line = Vec::new();
        (&mut reader)
            .take((MAX_EVENT_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(StorageError::Io)?;
        if line.is_empty() {
            break;
        }
        if line.len() > MAX_EVENT_BYTES || line.last() != Some(&b'\n') {
            return Err(integrity(
                "audit anchor has an oversized or incomplete signed record; preserve it for investigation",
            ));
        }
        let event: AuditEvent = serde_json::from_slice(&line)
            .map_err(|_| integrity("audit anchor contains an invalid record"))?;
        advance(&mut head, &event, key)?;
    }
    Ok(head)
}

fn advance(head: &mut AuditHead, event: &AuditEvent, key: &[u8; 32]) -> Result<(), DbError> {
    let next = head
        .sequence
        .checked_add(1)
        .ok_or_else(|| integrity("audit sequence overflow"))?;
    event
        .verify_integrity_link(head.hash.as_deref(), next, key)
        .map_err(integrity)?;
    head.sequence = next;
    head.hash = event.integrity.as_ref().map(|link| link.hash.clone());
    Ok(())
}

struct Budget {
    used: Arc<AtomicUsize>,
}
struct Reservation {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl ReadBudget for Budget {
    fn check(&self) -> Result<(), StorageError> {
        Ok(())
    }
    fn remaining_bytes(&self) -> usize {
        VERIFY_BUDGET.saturating_sub(self.used.load(Ordering::Acquire))
    }
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|next| *next <= VERIFY_BUDGET)
                .ok_or_else(|| {
                    StorageError::ResourceLimit(
                        "audit startup verification budget exceeded".to_owned(),
                    )
                })?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(Box::new(Reservation {
                        used: Arc::clone(&self.used),
                        bytes,
                    }));
                }
                Err(current) => used = current,
            }
        }
    }
}

fn event_from(bytes: &[u8]) -> Result<AuditEvent, StorageError> {
    if bytes.len() >= MAX_EVENT_BYTES {
        return Err(StorageError::Corruption(
            "audit startup record exceeds 1 MiB".to_owned(),
        ));
    }
    serde_json::from_slice(bytes)
        .map_err(|_| StorageError::Corruption("invalid stored audit event".to_owned()))
}

// Keep the ordinary streaming pass and the clock-regression fallback together so the
// shared sequence/head/anchor invariants remain visible when reviewing this verifier.
#[allow(clippy::too_many_lines)]
fn verify_snapshot(
    read: &impl ReadTransaction,
    key: &[u8; 32],
    anchor: &AuditHead,
) -> Result<AuditHead, DbError> {
    if !read.supports_guarded_reads() {
        return Err(integrity(
            "audit startup requires a guarded local storage snapshot",
        ));
    }
    let budget = Budget {
        used: Arc::new(AtomicUsize::new(0)),
    };
    let mut expected = AuditHead::default();
    read.visit_get(AUDIT_HEAD_TABLE, b"head", &budget, &mut |bytes| {
        if let Some(bytes) = bytes {
            if bytes.len() > 4096 {
                return Err(StorageError::Corruption(
                    "audit head exceeds its metadata limit".to_owned(),
                ));
            }
            let _parse = budget.reserve(bytes.len().saturating_mul(8).saturating_add(4096))?;
            expected = serde_json::from_slice(bytes)
                .map_err(|_| StorageError::Corruption("invalid stored audit head".to_owned()))?;
        }
        Ok(false)
    })?;
    if anchor.sequence > expected.sequence {
        return Err(integrity(
            "audit anchor is ahead of the database; preserve both copies",
        ));
    }
    let mut head = AuditHead::default();
    let mut ordered = true;
    let mut anchor_matches = anchor.sequence == 0 && anchor.hash.is_none();
    read.visit_range(
        AUDIT_TABLE,
        &[],
        &[],
        &budget,
        &mut |physical_key, bytes| {
            let _parse = budget.reserve(bytes.len().saturating_mul(8).saturating_add(4096))?;
            let event = event_from(bytes)?;
            let sequence = event.integrity.as_ref().map_or(0, |link| link.sequence);
            if sequence == 0 || sequence > expected.sequence || physical_key != event.key() {
                return Err(StorageError::Corruption(
                    "stored audit key or sequence is invalid".to_owned(),
                ));
            }
            if sequence != head.sequence.saturating_add(1) {
                ordered = false;
                return Ok(false);
            }
            advance(&mut head, &event, key).map_err(|_| {
                StorageError::Corruption(
                    "stored audit signature does not match the configured key".to_owned(),
                )
            })?;
            if head.sequence == anchor.sequence {
                anchor_matches = head.hash == anchor.hash;
            }
            Ok(true)
        },
    )?;
    if !ordered {
        head = AuditHead::default();
        while head.sequence < expected.sequence {
            let start = head.sequence + 1;
            let mut end = start.saturating_add(255).min(expected.sequence);
            let selected = loop {
                let mut selected = BTreeMap::new();
                let result = read.visit_range(
                    AUDIT_TABLE,
                    &[],
                    &[],
                    &budget,
                    &mut |physical_key, bytes| {
                        let reservation =
                            budget.reserve(bytes.len().saturating_mul(8).saturating_add(4096))?;
                        let event = event_from(bytes)?;
                        let sequence = event.integrity.as_ref().map_or(0, |link| link.sequence);
                        if sequence == 0
                            || sequence > expected.sequence
                            || physical_key != event.key()
                        {
                            return Err(StorageError::Corruption(
                                "stored audit key or sequence is invalid".to_owned(),
                            ));
                        }
                        if (start..=end).contains(&sequence)
                            && selected.insert(sequence, (event, reservation)).is_some()
                        {
                            return Err(StorageError::Corruption(
                                "duplicate stored audit sequence".to_owned(),
                            ));
                        }
                        Ok(true)
                    },
                );
                match result {
                    Ok(()) => break selected,
                    Err(StorageError::ResourceLimit(_)) if end > start => {
                        drop(selected);
                        end = start + (end - start) / 2;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            if u64::try_from(selected.len()).ok() != Some(end - start + 1) {
                return Err(integrity("stored audit sequence gap"));
            }
            for (_, (event, _reservation)) in selected {
                advance(&mut head, &event, key)?;
                if head.sequence == anchor.sequence {
                    anchor_matches = head.hash == anchor.hash;
                }
            }
        }
    }
    if head != expected || !anchor_matches {
        return Err(integrity(
            "audit head or signed anchor prefix differs from the stored chain",
        ));
    }
    Ok(head)
}

impl Database {
    pub(super) fn ensure_unused_audit_keys(&self, keys: &[Bytes]) -> Result<(), DbError> {
        let mut collision = false;
        if let Some(storage) = self.local_storage() {
            let read = storage.begin_read()?;
            let budget = Budget {
                used: Arc::new(AtomicUsize::new(0)),
            };
            read.visit_many(AUDIT_TABLE, keys, &budget, &mut |_, value| {
                collision = value.is_some();
                Ok(!collision)
            })?;
        } else {
            collision = self
                .repl
                .read_many(AUDIT_TABLE, keys, crate::repl::ReadConsistency::Strong)?
                .into_iter()
                .any(|value| value.is_some());
        }
        if collision {
            return Err(integrity(
                "audit event key collision; historical records were not replaced",
            ));
        }
        Ok(())
    }

    pub(super) fn verify_local_audit_chain(&self, key: &[u8; 32]) -> Result<bool, DbError> {
        let Some(storage) = self.local_storage() else {
            return Ok(false);
        };
        let read = storage.begin_read()?;
        verify_snapshot(&read, key, &AuditHead::default())?;
        Ok(true)
    }

    /// Called under `audit_lock` before the first append on a local handle, including
    /// trusted Rust/CLI callers that did not run serve's startup path.
    pub(super) fn verify_audit_prefix_before_append(&self, key: &[u8; 32]) -> Result<(), DbError> {
        if self
            .audit_batch
            .key_fingerprint
            .lock()
            .map_err(|_| integrity("audit key binding lock poisoned"))?
            .is_some()
        {
            return Ok(());
        }
        if let Some(storage) = self.local_storage() {
            let read = storage.begin_read()?;
            let anchor = anchor_head(&self.audit_config, key)?;
            verify_snapshot(&read, key, &anchor)?;
        }
        Ok(())
    }

    /// Verifies the complete stored signed chain and anchor before serve admits mutations.
    /// Clean anchor prefixes are recovered using the verified database history. A different
    /// key never silently re-signs history. Database crash repair may precede this check.
    /// # Errors
    /// Fails on signature, prefix, storage, anchor I/O or bounded verification errors.
    pub fn verify_audit_startup(&self) -> Result<(), DbError> {
        let _lock = self
            .audit_lock
            .lock()
            .map_err(|_| integrity("audit lock poisoned"))?;
        let key = configured_key(&self.audit_config)?;
        let anchor = anchor_head(&self.audit_config, &key)?;
        let storage = self
            .local_storage()
            .ok_or_else(|| integrity("audit startup requires local storage"))?;
        let read = storage.begin_read()?;
        let head = verify_snapshot(&read, &key, &anchor)?;
        drop(read);
        self.recover_verified_audit_anchor(&key, &head)?;
        self.bind_audit_key(&key)
    }

    pub(super) fn check_audit_key_binding(&self, key: &[u8; 32]) -> Result<(), DbError> {
        let fingerprint = self
            .audit_batch
            .key_fingerprint
            .lock()
            .map_err(|_| integrity("audit key binding lock poisoned"))?;
        if fingerprint.is_some_and(|expected| expected != *blake3::hash(key).as_bytes()) {
            return Err(integrity(
                "audit key changed while the database was open; stop writes and restore the original key; automatic audit rekey is not supported",
            ));
        }
        Ok(())
    }

    pub(super) fn bind_audit_key(&self, key: &[u8; 32]) -> Result<(), DbError> {
        *self
            .audit_batch
            .key_fingerprint
            .lock()
            .map_err(|_| integrity("audit key binding lock poisoned"))? =
            Some(*blake3::hash(key).as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests;
