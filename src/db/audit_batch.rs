//! Durable audit coalescing. A caller returns only after its complete batch and anchor
//! are durable; concurrent requests share commits without delaying an isolated request.

use std::{
    collections::{BTreeMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, Condvar, Mutex},
};

mod anchor;

use super::{Database, DbError};
use crate::{
    observability,
    repl::{Op, ReplError, propose_system_batch},
    security::{AUDIT_HEAD_TABLE, AUDIT_TABLE, AuditEvent, AuditHead},
    storage::StorageError,
};

const MAX_BATCH_EVENTS: usize = 256;
type Completion = Result<(), Arc<DbError>>;

#[derive(Default)]
pub(super) struct AuditBatchCoordinator {
    state: Mutex<BatchState>,
    changed: Condvar,
    anchor_checkpoint: Mutex<Option<anchor::Checkpoint>>,
}

#[derive(Default)]
struct BatchState {
    leader: bool,
    next_ticket: u64,
    pending: VecDeque<(u64, AuditEvent)>,
    completed: BTreeMap<u64, Completion>,
    failed: Option<Arc<DbError>>,
}

impl AuditBatchCoordinator {
    pub(super) fn record(
        &self,
        event: &AuditEvent,
        mut persist: impl FnMut(&[AuditEvent]) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        let mut state = self.state.lock().map_err(|_| poisoned())?;
        if let Some(error) = &state.failed {
            return Err(copy_error(error));
        }
        let ticket = state.next_ticket;
        state.next_ticket = ticket
            .checked_add(1)
            .ok_or_else(|| DbError::AuditIntegrity("audit request sequence overflow".to_owned()))?;
        state.pending.push_back((ticket, event.clone()));
        loop {
            if let Some(result) = state.completed.remove(&ticket) {
                return result.map_err(|error| copy_error(&error));
            }
            if let Some(error) = &state.failed {
                return Err(copy_error(error));
            }
            if state.leader {
                state = self.changed.wait(state).map_err(|_| poisoned())?;
                continue;
            }
            state.leader = true;
            let count = state.pending.len().min(MAX_BATCH_EVENTS);
            let batch = state.pending.drain(..count).collect::<Vec<_>>();
            let (tickets, events): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
            // Neither storage nor anchor work may hold the queue mutex. The leader does
            // all work itself, so its progress needs no blocked follower or Tokio task.
            drop(state);
            let outcome = catch_unwind(AssertUnwindSafe(|| persist(&events)));
            state = self.state.lock().map_err(|_| {
                self.changed.notify_all();
                poisoned()
            })?;
            state.leader = false;
            match outcome {
                Ok(result) => {
                    let completion = result.map_err(Arc::new);
                    if let Err(error) = &completion {
                        // A failed sink may have accepted only part of the batch. Never
                        // acknowledge later events past that unknown durable prefix.
                        // Pending callers also fail without attempting another commit.
                        state.failed = Some(Arc::clone(error));
                        state.pending.clear();
                    }
                    for done in tickets {
                        state.completed.insert(done, completion.clone());
                    }
                }
                Err(panic) => {
                    let error = Arc::new(DbError::AuditIntegrity(
                        "audit batch leader panicked; reopen the database before retrying"
                            .to_owned(),
                    ));
                    for done in tickets {
                        state.completed.insert(done, Err(Arc::clone(&error)));
                    }
                    state.failed = Some(error);
                    state.completed.remove(&ticket);
                    state.pending.clear();
                    self.changed.notify_all();
                    drop(state);
                    resume_unwind(panic);
                }
            }
            self.changed.notify_all();
            // A leader processes only one bounded batch before its own completion is
            // checked. Busy traffic cannot keep an already-durable caller behind a drain.
        }
    }
}

impl Database {
    pub(super) fn persist_audit_batch(&self, events: &[AuditEvent]) -> Result<(), DbError> {
        self.persist_audit_batch_with_anchor(events, |_| Ok(()))
    }

    fn persist_audit_batch_with_anchor(
        &self,
        events: &[AuditEvent],
        anchor: impl FnOnce(&[AuditEvent]) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        if events.is_empty() {
            return Ok(());
        }
        let _audit_guard = self
            .audit_lock
            .lock()
            .map_err(|_| DbError::AuditIntegrity("audit lock poisoned".to_owned()))?;
        // Read both anew for every batch. A failed commit never leaves a cached head,
        // and replacing an audit key/config does not retain stale signing material.
        let key = self.audit_key()?;
        let mut head = self.audit_head()?;
        let prepared_anchor = self.prepare_audit_anchor(&key, &head)?;
        let mut signed = Vec::with_capacity(events.len());
        let mut operations = Vec::with_capacity(events.len() + 1);
        for event in events {
            let sequence = head
                .sequence
                .checked_add(1)
                .ok_or_else(|| DbError::AuditIntegrity("audit sequence overflow".to_owned()))?;
            let event = event
                .clone()
                .with_integrity(head.hash.clone(), sequence, &key)
                .map_err(DbError::AuditIntegrity)?;
            head = AuditHead {
                sequence,
                hash: event
                    .integrity
                    .as_ref()
                    .map(|integrity| integrity.hash.clone()),
            };
            let value =
                serde_json::to_vec(&event).map_err(|error| DbError::Serde(error.to_string()))?;
            if prepared_anchor.is_some() {
                anchor::validate_record_size(&value)?;
            }
            operations.push(Op::Put {
                table: AUDIT_TABLE.to_owned(),
                key: event.key(),
                value,
            });
            signed.push(event);
        }
        operations.push(Op::Put {
            table: AUDIT_HEAD_TABLE.to_owned(),
            key: b"head".to_vec(),
            value: serde_json::to_vec(&head).map_err(|error| DbError::Serde(error.to_string()))?,
        });
        let result = (|| {
            let committed = propose_system_batch(&self.repl, operations);
            observability::record_audit_write(if committed.is_ok() { "ok" } else { "error" });
            committed?;
            // Retain the audit lock until the ordered anchor prefix is durable too.
            self.finish_audit_anchor(prepared_anchor, &signed, &key, &head)?;
            anchor(&signed)
        })();
        if result.is_err() {
            self.audit_batch
                .anchor_checkpoint
                .lock()
                .map_err(|_| poisoned())?
                .take();
        }
        result
    }
}

#[cfg(test)]
fn append_anchor(path: &std::path::Path, events: &[AuditEvent]) -> Result<(), DbError> {
    use std::{fs::OpenOptions, io::Write};
    let (mut file, created) = match OpenOptions::new().create_new(true).append(true).open(path) {
        Ok(file) => (file, true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (
            OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(|error| DbError::AuditIntegrity(error.to_string()))?,
            false,
        ),
        Err(error) => return Err(DbError::AuditIntegrity(error.to_string())),
    };
    let mut bytes = Vec::new();
    for event in events {
        serde_json::to_writer(&mut bytes, event)
            .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
        bytes.push(b'\n');
    }
    file.write_all(&bytes)
        .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
    file.sync_all()
        .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
    if created {
        crate::fileio::sync_parent(path)
            .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
    }
    Ok(())
}

fn poisoned() -> DbError {
    DbError::AuditIntegrity("audit batch queue poisoned".to_owned())
}

// Each waiter must own its error while preserving the existing failure category.
// Only these variants arise from key/head reads, audit serialization, commit or anchor I/O.
fn copy_error(error: &DbError) -> DbError {
    match error {
        DbError::AuditIntegrity(message) => DbError::AuditIntegrity(message.clone()),
        DbError::Serde(message) => DbError::Serde(message.clone()),
        DbError::Storage(error) => DbError::Storage(copy_storage_error(error)),
        DbError::Repl(error) => DbError::Repl(copy_repl_error(error)),
        error => DbError::AuditIntegrity(error.to_string()),
    }
}

fn copy_storage_error(error: &StorageError) -> StorageError {
    match error {
        StorageError::NotFound => StorageError::NotFound,
        StorageError::Io(error) => StorageError::Io(error.raw_os_error().map_or_else(
            || std::io::Error::new(error.kind(), error.to_string()),
            std::io::Error::from_raw_os_error,
        )),
        StorageError::Corruption(message) => StorageError::Corruption(message.clone()),
        StorageError::Conflict => StorageError::Conflict,
        StorageError::Backend(message) => StorageError::Backend(message.clone()),
    }
}

fn copy_repl_error(error: &ReplError) -> ReplError {
    match error {
        ReplError::NoQuorum => ReplError::NoQuorum,
        ReplError::Conflict => ReplError::Conflict,
        ReplError::Storage(error) => ReplError::Storage(copy_storage_error(error)),
        ReplError::Transport(message) => ReplError::Transport(message.clone()),
        ReplError::Unsupported(message) => ReplError::Unsupported(message.clone()),
        ReplError::Rejected(message) => ReplError::Rejected(message.clone()),
        ReplError::QuotaExceeded {
            tenant,
            requested,
            remaining,
        } => ReplError::QuotaExceeded {
            tenant: tenant.clone(),
            requested: *requested,
            remaining: *remaining,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, Instant},
    };

    use super::{AuditBatchCoordinator, append_anchor, copy_error};
    use crate::{
        db::{DbConfig, DbError, Profile, create_database},
        repl::{Op, ReplError, propose_system_batch},
        security::{AUDIT_TABLE, AuditEvent, AuditOutcome, Resource},
        storage::StorageError,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    type GroupResult = (Vec<usize>, Vec<Result<(), DbError>>);

    fn event(action: &str) -> AuditEvent {
        AuditEvent::new(
            None,
            action,
            Resource::Database,
            AuditOutcome::Succeeded,
            None,
        )
    }

    struct DurableProbe {
        sizes: Mutex<Vec<usize>>,
        first_entered: Mutex<Option<mpsc::Sender<()>>>,
        release_first: Mutex<mpsc::Receiver<()>>,
        failure: Option<DbError>,
    }

    impl DurableProbe {
        fn persist(&self, events: &[AuditEvent]) -> Result<(), DbError> {
            self.sizes
                .lock()
                .map_err(|_| super::poisoned())?
                .push(events.len());
            let first = self
                .first_entered
                .lock()
                .map_err(|_| super::poisoned())?
                .take();
            if let Some(first) = first {
                first
                    .send(())
                    .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
                self.release_first
                    .lock()
                    .map_err(|_| super::poisoned())?
                    .recv()
                    .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
            }
            self.failure
                .as_ref()
                .map_or(Ok(()), |error| Err(copy_error(error)))
        }
    }

    fn wait_for_pending(coordinator: &AuditBatchCoordinator, count: usize) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if coordinator
                .state
                .lock()
                .map_err(|_| "poisoned test queue")?
                .pending
                .len()
                >= count
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("audit followers did not queue".into());
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn queued_group(failure: Option<DbError>) -> Result<GroupResult, Box<dyn std::error::Error>> {
        let coordinator = Arc::new(AuditBatchCoordinator::default());
        let (entered, first_entered) = mpsc::channel();
        let (release, release_first) = mpsc::channel();
        let probe = Arc::new(DurableProbe {
            sizes: Mutex::new(Vec::new()),
            first_entered: Mutex::new(Some(entered)),
            release_first: Mutex::new(release_first),
            failure,
        });
        let mut workers = Vec::new();
        let first_queue = Arc::clone(&coordinator);
        let first_probe = Arc::clone(&probe);
        let (finished, completion) = mpsc::channel();
        workers.push(thread::spawn(move || {
            let result = first_queue.record(&event("first"), |events| first_probe.persist(events));
            let _ = finished.send(());
            result
        }));
        first_entered.recv_timeout(Duration::from_secs(5))?;
        for index in 0..8 {
            let queue = Arc::clone(&coordinator);
            let probe = Arc::clone(&probe);
            workers.push(thread::spawn(move || {
                queue.record(&event(&format!("follower_{index}")), |events| {
                    probe.persist(events)
                })
            }));
        }
        wait_for_pending(&coordinator, 8)?;
        assert!(matches!(
            completion.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release.send(())?;
        let results = workers
            .into_iter()
            .map(|worker| worker.join().map_err(|_| "audit worker panicked"))
            .collect::<Result<Vec<_>, _>>()?;
        let sizes = probe.sizes.lock().map_err(|_| "poisoned probe")?.clone();
        Ok((sizes, results))
    }

    #[test]
    fn concurrent_audits_coalesce_without_acknowledging_uncommitted_requests() -> TestResult {
        let (sizes, results) = queued_group(None)?;
        assert_eq!(sizes, vec![1, 8]);
        assert!(results.iter().all(Result::is_ok));
        Ok(())
    }

    #[test]
    fn failed_anchor_seals_auditing_even_if_the_path_is_repaired() -> TestResult {
        let temp = tempfile::tempdir()?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let missing = temp.path().join("missing");
        database.audit_config.anchor_path = Some(missing.join("audit.jsonl"));
        assert!(matches!(
            database.record_audit(&event("failed")),
            Err(DbError::AuditIntegrity(_))
        ));
        assert_eq!(database.audit_head()?.sequence, 0);
        std::fs::create_dir(&missing)?;
        assert!(matches!(
            database.record_audit(&event("later")),
            Err(DbError::AuditIntegrity(_))
        ));
        assert_eq!(database.audit_head()?.sequence, 0);
        assert!(!missing.join("audit.jsonl").exists());
        database.verify_audit_chain()?;
        Ok(())
    }

    #[test]
    fn batch_commit_failure_reaches_every_waiter_with_original_error_category() -> TestResult {
        let (sizes, results) = queued_group(Some(DbError::Repl(ReplError::Storage(
            StorageError::Io(std::io::Error::from_raw_os_error(5)),
        ))))?;
        assert_eq!(sizes, vec![1]);
        for result in results {
            assert!(
                matches!(result, Err(DbError::Repl(ReplError::Storage(StorageError::Io(error)))) if error.raw_os_error() == Some(5))
            );
        }
        Ok(())
    }

    #[test]
    fn successful_audit_waits_for_database_commit_and_anchor_sync() -> TestResult {
        let temp = tempfile::tempdir()?;
        let anchor = temp.path().join("audit.jsonl");
        let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
        let (database_committed, committed) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let (returned, response) = mpsc::channel();
        let writer = Arc::clone(&database);
        let anchor_for_writer = anchor.clone();
        let worker = thread::spawn(move || {
            let result = writer.audit_batch.record(&event("durability"), |events| {
                writer.persist_audit_batch_with_anchor(events, |signed| {
                    database_committed
                        .send(())
                        .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
                    wait.recv()
                        .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
                    append_anchor(&anchor_for_writer, signed)
                })
            });
            let _ = returned.send(result);
        });
        committed.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(
            database.audit_events()?.len(),
            1,
            "database commit must precede anchor acknowledgement"
        );
        assert!(!anchor.exists());
        assert!(matches!(
            response.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release.send(())?;
        response.recv_timeout(Duration::from_secs(5))??;
        worker.join().map_err(|_| "audit writer panicked")?;
        let lines = std::fs::read_to_string(&anchor)?;
        let anchored: AuditEvent = serde_json::from_str(lines.trim())?;
        assert_eq!(anchored.action, "durability");
        assert_eq!(
            anchored.integrity.ok_or("anchor lacks integrity")?.sequence,
            1
        );
        database.verify_audit_chain()?;
        Ok(())
    }

    #[test]
    fn anchor_failure_is_not_reported_as_success_after_database_commit() -> TestResult {
        let temp = tempfile::tempdir()?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.audit_config.anchor_path = Some(temp.path().join("absent").join("audit.jsonl"));
        let events = [
            event("anchor_first"),
            event("anchor_second"),
            event("anchor_third"),
        ];
        assert!(matches!(
            database.persist_audit_batch(&events),
            Err(DbError::AuditIntegrity(_))
        ));
        assert_eq!(
            database.audit_events()?.len(),
            0,
            "an unusable anchor is rejected before committing new database events"
        );
        database.verify_audit_chain()?;
        let (_, outcomes) = queued_group(Some(DbError::AuditIntegrity(
            "injected anchor failure".to_owned(),
        )))?;
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome, Err(DbError::AuditIntegrity(_))))
        );
        Ok(())
    }

    #[test]
    fn anchor_batch_preserves_hash_sequence_when_event_timestamps_are_out_of_order() -> TestResult {
        let temp = tempfile::tempdir()?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let path = temp.path().join("audit.jsonl");
        database.audit_config.anchor_path = Some(path.clone());
        let events = [300, 100, 200]
            .into_iter()
            .map(|timestamp| {
                let mut event = event("anchor_order");
                event.at_millis = timestamp;
                event
            })
            .collect::<Vec<_>>();
        database.persist_audit_batch(&events)?;
        let anchored = std::fs::read_to_string(path)?
            .lines()
            .map(serde_json::from_str::<AuditEvent>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            anchored
                .iter()
                .map(|event| event.integrity.as_ref().map(|integrity| integrity.sequence))
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
        database.verify_audit_chain()?;
        Ok(())
    }

    #[test]
    fn concurrent_database_audits_keep_all_events_and_a_verifiable_chain() -> TestResult {
        let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
        let mut workers = Vec::new();
        for worker in 0..8 {
            let database = Arc::clone(&database);
            workers.push(thread::spawn(move || -> Result<(), DbError> {
                for index in 0..32 {
                    database.record_audit(&event(&format!("worker_{worker}_{index}")))?;
                }
                Ok(())
            }));
        }
        for worker in workers {
            worker.join().map_err(|_| "audit worker panicked")??;
        }
        assert_eq!(database.audit_events()?.len(), 256);
        assert_eq!(database.audit_head()?.sequence, 256);
        database.verify_audit_chain()?;
        Ok(())
    }

    #[test]
    fn audit_verification_uses_sequence_and_still_rejects_duplicate_or_missing_links() -> TestResult
    {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        for (index, timestamp) in [300, 100, 200].into_iter().enumerate() {
            let mut audit = event(&format!("ordered_{index}"));
            audit.at_millis = timestamp;
            database.record_audit(&audit)?;
        }
        let events = database.audit_events()?;
        assert_eq!(
            events[0].at_millis, 100,
            "audit_events must retain its existing storage order"
        );
        database.verify_audit_chain()?;
        let mut duplicate = events[0].clone();
        duplicate
            .integrity
            .as_mut()
            .ok_or("missing integrity")?
            .sequence = 1;
        propose_system_batch(
            &database.repl,
            vec![Op::Put {
                table: AUDIT_TABLE.to_owned(),
                key: duplicate.key(),
                value: serde_json::to_vec(&duplicate)?,
            }],
        )?;
        assert!(matches!(
            database.verify_audit_chain(),
            Err(DbError::AuditIntegrity(_))
        ));
        propose_system_batch(
            &database.repl,
            vec![Op::Delete {
                table: AUDIT_TABLE.to_owned(),
                key: duplicate.key(),
            }],
        )?;
        assert!(matches!(
            database.verify_audit_chain(),
            Err(DbError::AuditIntegrity(_))
        ));
        Ok(())
    }

    #[test]
    fn panicking_batch_releases_waiters_and_fails_closed() -> TestResult {
        let coordinator = Arc::new(AuditBatchCoordinator::default());
        let (entered, started) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let leader_queue = Arc::clone(&coordinator);
        let leader = thread::spawn(move || {
            leader_queue.record(&event("panic"), |_| {
                entered
                    .send(())
                    .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
                wait.recv()
                    .map_err(|error| DbError::AuditIntegrity(error.to_string()))?;
                panic!("injected audit sink panic");
            })
        });
        started.recv_timeout(Duration::from_secs(5))?;
        let (finished, outcomes) = mpsc::channel();
        let mut followers = Vec::new();
        for _ in 0..4 {
            let coordinator = Arc::clone(&coordinator);
            let finished = finished.clone();
            followers.push(thread::spawn(move || {
                let result = coordinator.record(&event("follower"), |_| Ok(()));
                let _ = finished.send(result);
            }));
        }
        wait_for_pending(&coordinator, 4)?;
        release.send(())?;
        assert!(leader.join().is_err());
        for _ in 0..4 {
            assert!(matches!(
                outcomes.recv_timeout(Duration::from_secs(5))?,
                Err(DbError::AuditIntegrity(_))
            ));
        }
        for follower in followers {
            follower.join().map_err(|_| "audit follower panicked")?;
        }
        assert!(matches!(
            coordinator.record(&event("after_panic"), |_| Ok(())),
            Err(DbError::AuditIntegrity(_))
        ));
        Ok(())
    }
}
