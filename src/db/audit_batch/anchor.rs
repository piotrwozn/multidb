//! Validate and recover a configured anchor before acknowledging later audit events.

use std::{
    collections::BTreeMap,
    fs::{File, Metadata, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::SystemTime,
};

use super::{AuditEvent, AuditHead, Database, DbError, poisoned};
use crate::{
    repl::{ReadConsistency, Replication},
    security::AUDIT_TABLE,
};

const MAX_LINE_BYTES: usize = 1_048_576;
const MAX_RECOVERY_BYTES: usize = 16 * 1_048_576;
const RECOVERY_EVENTS: u64 = 256;

pub(super) fn validate_record_size(bytes: &[u8]) -> Result<(), DbError> {
    if bytes.len() >= MAX_LINE_BYTES {
        return Err(integrity(
            "event exceeds 1048576 bytes including its newline",
        ));
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
struct Stamp {
    length: u64,
    modified: Option<SystemTime>,
    identity: Option<(u64, u64)>,
    changed: Option<(i64, i64)>,
}

impl Stamp {
    fn new(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            Some((metadata.dev(), metadata.ino()))
        };
        #[cfg(not(unix))]
        let identity = None;
        #[cfg(unix)]
        let changed = {
            use std::os::unix::fs::MetadataExt;
            Some((metadata.ctime(), metadata.ctime_nsec()))
        };
        #[cfg(not(unix))]
        let changed = None;
        Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
            identity,
            changed,
        }
    }
}

pub(super) struct Checkpoint {
    path: PathBuf,
    stamp: Stamp,
    tail: Vec<u8>,
    head: AuditHead,
    key_fingerprint: [u8; 32],
}

pub(super) struct Prepared {
    path: PathBuf,
    file: File,
    stamp: Stamp,
    created: bool,
}

fn integrity(message: impl std::fmt::Display) -> DbError {
    DbError::AuditIntegrity(format!(
        "audit anchor: {message}; preserve a damaged file, move it aside and reopen to rebuild from the verified database chain"
    ))
}

impl Database {
    /// Called only while holding `audit_lock`; no queue mutex is held during I/O.
    pub(super) fn prepare_audit_anchor(
        &self,
        key: &[u8; 32],
        head: &AuditHead,
    ) -> Result<Option<Prepared>, DbError> {
        let checkpoint = self
            .audit_batch
            .anchor_checkpoint
            .lock()
            .map_err(|_| poisoned())?
            .take();
        let Some(path) = self.audit_config.anchor_path.clone() else {
            return Ok(None);
        };
        let mut options = OpenOptions::new();
        options.read(true).append(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let (mut file, created) = match options.open(&path) {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&path)
                    .map_err(integrity)?,
                false,
            ),
            Err(error) => return Err(integrity(error)),
        };
        let mut stamp = Stamp::new(&file.metadata().map_err(integrity)?);
        let fingerprint = *blake3::hash(key).as_bytes();
        let cached = if let Some(cached) = checkpoint {
            cached.path == path
                && cached.stamp == stamp
                && stamp.modified.is_some()
                && stamp.changed.is_some()
                && cached.head == *head
                && cached.key_fingerprint == fingerprint
                && tail_matches(&mut file, &cached.tail)?
        } else {
            false
        };
        if !cached {
            let (mut sequence, mut previous_hash) =
                self.validate_anchor_prefix(&mut file, key, head)?;
            if Stamp::new(&file.metadata().map_err(integrity)?) != stamp {
                return Err(integrity("anchor changed during prefix validation"));
            }
            while sequence < head.sequence {
                let start = sequence
                    .checked_add(1)
                    .ok_or_else(|| integrity("sequence overflow"))?;
                let mut end = start.saturating_add(RECOVERY_EVENTS - 1).min(head.sequence);
                let events = loop {
                    if let Some(events) = self.audit_recovery_window(start, end)? {
                        break events;
                    }
                    if start == end {
                        return Err(integrity("one stored event exceeds the recovery budget"));
                    }
                    end = start + (end - start) / 2;
                };
                for event in &events {
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| integrity("sequence overflow"))?;
                    event
                        .verify_integrity_link(previous_hash.as_deref(), sequence, key)
                        .map_err(integrity)?;
                    previous_hash = event.integrity.as_ref().map(|link| link.hash.clone());
                }
                if Stamp::new(&file.metadata().map_err(integrity)?) != stamp {
                    return Err(integrity("anchor changed during recovery"));
                }
                let written = append_events(&mut file, &events)?;
                file.sync_all().map_err(integrity)?;
                let recovered_stamp = Stamp::new(&file.metadata().map_err(integrity)?);
                if recovered_stamp.length != stamp.length.saturating_add(written) {
                    return Err(integrity("anchor changed during recovery synchronization"));
                }
                stamp = recovered_stamp;
            }
            if previous_hash != head.hash {
                return Err(integrity(
                    "stored head does not match the recovered signed chain",
                ));
            }
        }
        if Stamp::new(&file.metadata().map_err(integrity)?) != stamp
            || Stamp::new(&std::fs::metadata(&path).map_err(integrity)?) != stamp
        {
            return Err(integrity("anchor changed or rotated during validation"));
        }
        Ok(Some(Prepared {
            path,
            file,
            stamp,
            created,
        }))
    }

    pub(super) fn finish_audit_anchor(
        &self,
        prepared: Option<Prepared>,
        events: &[AuditEvent],
        key: &[u8; 32],
        head: &AuditHead,
    ) -> Result<(), DbError> {
        let Some(mut prepared) = prepared else {
            return Ok(());
        };
        if Stamp::new(&prepared.file.metadata().map_err(integrity)?) != prepared.stamp
            || Stamp::new(&std::fs::metadata(&prepared.path).map_err(integrity)?) != prepared.stamp
        {
            return Err(integrity(
                "anchor changed or rotated during the audit commit",
            ));
        }
        let written = append_events(&mut prepared.file, events)?;
        prepared.file.sync_all().map_err(integrity)?;
        if prepared.created {
            crate::fileio::sync_parent(&prepared.path).map_err(integrity)?;
        }
        let stamp = Stamp::new(&prepared.file.metadata().map_err(integrity)?);
        if stamp.length != prepared.stamp.length.saturating_add(written)
            || Stamp::new(&std::fs::metadata(&prepared.path).map_err(integrity)?) != stamp
        {
            return Err(integrity(
                "anchor changed or rotated during synchronization",
            ));
        }
        let mut tail = events
            .last()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(integrity)?
            .unwrap_or_default();
        if !tail.is_empty() {
            tail.push(b'\n');
        }
        if !tail_matches(&mut prepared.file, &tail)? {
            return Err(integrity("anchor tail changed during synchronization"));
        }
        *self
            .audit_batch
            .anchor_checkpoint
            .lock()
            .map_err(|_| poisoned())? = Some(Checkpoint {
            path: prepared.path,
            stamp,
            tail,
            head: head.clone(),
            key_fingerprint: *blake3::hash(key).as_bytes(),
        });
        Ok(())
    }

    fn validate_anchor_prefix(
        &self,
        file: &mut File,
        key: &[u8; 32],
        head: &AuditHead,
    ) -> Result<(u64, Option<String>), DbError> {
        file.seek(SeekFrom::Start(0)).map_err(integrity)?;
        let mut reader = BufReader::new(file);
        let mut sequence = 0_u64;
        let mut previous_hash = None;
        loop {
            let mut line = Vec::new();
            (&mut reader)
                .take((MAX_LINE_BYTES + 1) as u64)
                .read_until(b'\n', &mut line)
                .map_err(integrity)?;
            if line.is_empty() {
                break;
            }
            if line.len() > MAX_LINE_BYTES {
                return Err(integrity("line exceeds 1048576 bytes"));
            }
            if line.last() != Some(&b'\n') {
                return Err(integrity("incomplete final JSONL record"));
            }
            let event: AuditEvent = serde_json::from_slice(&line).map_err(integrity)?;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| integrity("sequence overflow"))?;
            if sequence > head.sequence {
                return Err(integrity("anchor is ahead of the database head"));
            }
            event
                .verify_integrity_link(previous_hash.as_deref(), sequence, key)
                .map_err(integrity)?;
            let stored = self
                .repl
                .read(AUDIT_TABLE, &event.key(), ReadConsistency::Strong)?
                .ok_or_else(|| integrity("anchored event is missing from the database"))?;
            let stored: AuditEvent = serde_json::from_slice(&stored).map_err(integrity)?;
            if stored != event {
                return Err(integrity("anchored event differs from the stored event"));
            }
            previous_hash = event.integrity.map(|link| link.hash);
        }
        Ok((sequence, previous_hash))
    }

    // Storage order follows timestamps, not chain sequence. Recovery selects one
    // bounded sequence window per scan; even a very large missing tail never requires
    // retaining the complete audit history or an unbounded JSONL line in memory.
    fn audit_recovery_window(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Option<Vec<AuditEvent>>, DbError> {
        if !self.repl.supports_bounded_scans() {
            return Err(integrity(
                "this backend cannot recover an anchor with bounded scans",
            ));
        }
        let mut selected = BTreeMap::new();
        let mut retained = 0_usize;
        let mut overflow = false;
        let mut corruption = None;
        self.repl
            .scan_range_batches(
                AUDIT_TABLE,
                &[],
                &[],
                ReadConsistency::Strong,
                64,
                &|| false,
                &mut |rows| {
                    for (physical_key, value) in rows {
                        if value.len() >= MAX_LINE_BYTES {
                            corruption = Some("stored audit line length is invalid".to_owned());
                            return Ok(false);
                        }
                        let event: AuditEvent = match serde_json::from_slice(value) {
                            Ok(event) => event,
                            Err(error) => {
                                corruption = Some(error.to_string());
                                return Ok(false);
                            }
                        };
                        let Some(sequence) = event.integrity.as_ref().map(|link| link.sequence)
                        else {
                            corruption = Some("stored event has no integrity metadata".to_owned());
                            return Ok(false);
                        };
                        if physical_key != &event.key() || sequence == 0 {
                            corruption = Some("stored audit key or sequence is invalid".to_owned());
                            return Ok(false);
                        }
                        if !(start..=end).contains(&sequence) {
                            continue;
                        }
                        retained = retained.saturating_add(value.len());
                        if retained > MAX_RECOVERY_BYTES {
                            overflow = true;
                            return Ok(false);
                        }
                        if selected.insert(sequence, event).is_some() {
                            corruption = Some("duplicate stored audit sequence".to_owned());
                            return Ok(false);
                        }
                    }
                    Ok(true)
                },
            )
            .map_err(integrity)?;
        if let Some(corruption) = corruption {
            return Err(integrity(corruption));
        }
        if overflow {
            return Ok(None);
        }
        if u64::try_from(selected.len()).ok() != Some(end - start + 1) {
            return Err(integrity("stored audit chain has a sequence gap"));
        }
        Ok(Some(selected.into_values().collect()))
    }
}

fn append_events(file: &mut File, events: &[AuditEvent]) -> Result<u64, DbError> {
    let mut written = 0_u64;
    for event in events {
        let mut bytes = serde_json::to_vec(event).map_err(integrity)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_LINE_BYTES {
            return Err(integrity("event exceeds 1048576 bytes"));
        }
        file.write_all(&bytes).map_err(integrity)?;
        written = written.saturating_add(u64::try_from(bytes.len()).map_err(integrity)?);
    }
    Ok(written)
}

fn tail_matches(file: &mut File, tail: &[u8]) -> Result<bool, DbError> {
    let length = file.metadata().map_err(integrity)?.len();
    let tail_length = u64::try_from(tail.len()).map_err(integrity)?;
    if tail_length > length {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(length - tail_length))
        .map_err(integrity)?;
    let mut bytes = vec![0; tail.len()];
    file.read_exact(&mut bytes).map_err(integrity)?;
    Ok(bytes == tail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{DbConfig, Profile, create_database, open_database},
        security::{AuditOutcome, Resource},
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn event(action: &str) -> AuditEvent {
        AuditEvent::new(
            None,
            action,
            Resource::System,
            AuditOutcome::Succeeded,
            None,
        )
    }

    fn anchored_events(
        path: &std::path::Path,
    ) -> Result<Vec<AuditEvent>, Box<dyn std::error::Error>> {
        Ok(std::fs::read_to_string(path)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?)
    }

    #[test]
    fn reopening_recovers_a_clean_signed_prefix_before_the_next_event() -> TestResult {
        let temp = tempfile::tempdir()?;
        let database_path = temp.path().join("audit.redb");
        let anchor_path = temp.path().join("audit.jsonl");
        {
            let mut database =
                create_database(DbConfig::on_disk(Profile::Balanced, &database_path))?;
            database.record_audit(&event("before_anchor_enabled"))?;
            database.audit_config.anchor_path = Some(anchor_path.clone());
            database.record_audit(&event("second"))?;
            let lines = std::fs::read_to_string(&anchor_path)?;
            let first = lines
                .split_inclusive('\n')
                .next()
                .ok_or("missing first anchor")?;
            std::fs::write(&anchor_path, first)?;
        }
        let mut database = open_database(DbConfig::on_disk(Profile::Balanced, &database_path))?;
        database.audit_config.anchor_path = Some(anchor_path.clone());
        database.record_audit(&event("after_reopen"))?;
        let events = anchored_events(&anchor_path)?;
        assert_eq!(
            events
                .iter()
                .map(|event| event.action.as_str())
                .collect::<Vec<_>>(),
            vec!["before_anchor_enabled", "second", "after_reopen"]
        );
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.integrity.as_ref().map(|link| link.sequence))
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        database.verify_audit_chain()?;
        Ok(())
    }

    #[test]
    fn partial_anchor_is_preserved_and_operator_can_move_it_aside_then_reopen() -> TestResult {
        let temp = tempfile::tempdir()?;
        let database_path = temp.path().join("audit.redb");
        let anchor_path = temp.path().join("audit.jsonl");
        {
            let mut database =
                create_database(DbConfig::on_disk(Profile::Balanced, &database_path))?;
            database.audit_config.anchor_path = Some(anchor_path.clone());
            database.record_audit(&event("first"))?;
        }
        let mut bytes = std::fs::read(&anchor_path)?;
        bytes.extend_from_slice(b"{\"partial\":");
        std::fs::write(&anchor_path, &bytes)?;
        {
            let mut database = open_database(DbConfig::on_disk(Profile::Balanced, &database_path))?;
            database.audit_config.anchor_path = Some(anchor_path.clone());
            assert!(
                matches!(database.record_audit(&event("must_refuse")), Err(DbError::AuditIntegrity(message)) if message.contains("incomplete final"))
            );
            assert_eq!(database.audit_head()?.sequence, 1);
            assert_eq!(std::fs::read(&anchor_path)?, bytes);
        }
        let preserved = temp.path().join("audit.damaged.jsonl");
        std::fs::rename(&anchor_path, &preserved)?;
        let mut database = open_database(DbConfig::on_disk(Profile::Balanced, &database_path))?;
        database.audit_config.anchor_path = Some(anchor_path.clone());
        database.record_audit(&event("recovered"))?;
        assert_eq!(std::fs::read(preserved)?, bytes);
        assert_eq!(anchored_events(&anchor_path)?.len(), 2);
        database.verify_audit_chain()?;
        Ok(())
    }

    #[test]
    fn checkpoint_detects_rotation_and_revalidates_the_replacement() -> TestResult {
        let temp = tempfile::tempdir()?;
        let anchor_path = temp.path().join("audit.jsonl");
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.audit_config.anchor_path = Some(anchor_path.clone());
        database.record_audit(&event("first"))?;
        std::fs::rename(&anchor_path, temp.path().join("audit.rotated.jsonl"))?;
        std::fs::write(&anchor_path, b"")?;
        database.record_audit(&event("second"))?;
        assert_eq!(anchored_events(&anchor_path)?.len(), 2);
        let corrupt = b"foreign or corrupted evidence\n";
        std::fs::write(&anchor_path, corrupt)?;
        assert!(matches!(
            database.record_audit(&event("third")),
            Err(DbError::AuditIntegrity(_))
        ));
        assert_eq!(database.audit_head()?.sequence, 2);
        assert_eq!(std::fs::read(&anchor_path)?, corrupt);
        assert!(
            database
                .audit_batch
                .anchor_checkpoint
                .lock()
                .map_err(|_| "poisoned checkpoint")?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn oversized_anchor_lines_fail_without_advancing_the_database_head() -> TestResult {
        let temp = tempfile::tempdir()?;
        let anchor_path = temp.path().join("audit.jsonl");
        std::fs::write(&anchor_path, vec![b'x'; MAX_LINE_BYTES + 1])?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.audit_config.anchor_path = Some(anchor_path.clone());
        assert!(
            matches!(database.record_audit(&event("must_refuse")), Err(DbError::AuditIntegrity(message)) if message.contains("line exceeds"))
        );
        assert_eq!(database.audit_head()?.sequence, 0);
        assert_eq!(
            std::fs::metadata(anchor_path)?.len(),
            (MAX_LINE_BYTES + 1) as u64
        );
        Ok(())
    }

    #[test]
    fn oversized_new_events_are_rejected_before_the_database_commit() -> TestResult {
        let temp = tempfile::tempdir()?;
        let anchor_path = temp.path().join("audit.jsonl");
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.audit_config.anchor_path = Some(anchor_path.clone());
        assert!(
            matches!(database.record_audit(&event(&"x".repeat(MAX_LINE_BYTES))), Err(DbError::AuditIntegrity(message)) if message.contains("event exceeds"))
        );
        assert_eq!(database.audit_head()?.sequence, 0);
        assert_eq!(std::fs::metadata(anchor_path)?.len(), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn editing_an_earlier_record_and_restoring_mtime_invalidates_the_checkpoint() -> TestResult {
        let temp = tempfile::tempdir()?;
        let anchor_path = temp.path().join("audit.jsonl");
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.audit_config.anchor_path = Some(anchor_path.clone());
        database.record_audit(&event("first"))?;
        database.record_audit(&event("second"))?;
        let modified = std::fs::metadata(&anchor_path)?.modified()?;
        let original = std::fs::read_to_string(&anchor_path)?;
        let tampered = original.replacen("first", "other", 1);
        assert_eq!(tampered.len(), original.len());
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&anchor_path, &tampered)?;
        File::options()
            .write(true)
            .open(&anchor_path)?
            .set_times(std::fs::FileTimes::new().set_modified(modified))?;
        assert_eq!(std::fs::metadata(&anchor_path)?.modified()?, modified);
        assert!(matches!(
            database.record_audit(&event("third")),
            Err(DbError::AuditIntegrity(_))
        ));
        assert_eq!(database.audit_head()?.sequence, 2);
        assert_eq!(std::fs::read_to_string(anchor_path)?, tampered);
        Ok(())
    }
}
