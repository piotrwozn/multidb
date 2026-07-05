use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::storage::{Bytes, StorageError};

use super::{Op, ReplError};

pub type DistTxnId = u64;

pub const DIST_TXN_PARTICIPANT_TABLE: &str = "__dist_txn_participants";
pub const DIST_TXN_COORDINATOR_TABLE: &str = "__dist_txn_coordinator";
pub const DIST_TXN_FINISHED_TABLE: &str = "__dist_txn_finished";
pub const DEFAULT_DIST_TXN_DEADLINE_MS: u64 = 30_000;

const DIST_TXN_RECORD_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Vote {
    Yes,
    No,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Decision {
    Commit,
    Abort,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PreparedTxnRecord {
    pub version: u16,
    pub txn_id: DistTxnId,
    pub ops: Vec<Op>,
    pub prepared_at_ms: u64,
    pub deadline_ms: u64,
    pub retry_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CoordinatorDecisionRecord {
    pub version: u16,
    pub txn_id: DistTxnId,
    pub decision: Decision,
    pub decided_at_ms: u64,
    pub deadline_ms: u64,
    pub retry_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FinishedTxnRecord {
    pub version: u16,
    pub txn_id: DistTxnId,
    pub decision: Decision,
    pub finished_at_ms: u64,
}

pub trait Participant {
    /// Persists the participant's prepared state and returns a vote.
    /// # Errors
    /// Fails when the participant cannot prepare.
    fn prepare(&self, txn_id: DistTxnId) -> Result<Vote, ReplError>;

    /// Applies the coordinator decision and releases local locks.
    /// # Errors
    /// Fails when the participant cannot finish the transaction.
    fn finish(&self, txn_id: DistTxnId, decision: Decision) -> Result<(), ReplError>;
}

pub trait CoordinatorLog {
    /// Persists the final decision before participants are notified.
    /// # Errors
    /// Fails when the coordinator decision cannot be persisted.
    fn record_decision(&mut self, txn_id: DistTxnId, decision: Decision) -> Result<(), ReplError>;

    fn decision(&self, txn_id: DistTxnId) -> Option<Decision>;
}

#[derive(Default)]
pub struct InMemoryCoordinatorLog {
    decisions: BTreeMap<DistTxnId, Decision>,
}

/// Executes a minimal two-phase commit over provided participants.
/// # Errors
/// Fails when prepare, decision logging, or finish fails.
pub fn two_phase_commit<L>(
    txn_id: DistTxnId,
    participants: &[&dyn Participant],
    log: &mut L,
) -> Result<Decision, ReplError>
where
    L: CoordinatorLog,
{
    let mut decision = Decision::Commit;

    for participant in participants {
        if participant.prepare(txn_id)? != Vote::Yes {
            decision = Decision::Abort;
            break;
        }
    }

    log.record_decision(txn_id, decision)?;

    for participant in participants {
        participant.finish(txn_id, decision)?;
    }

    Ok(decision)
}

impl InMemoryCoordinatorLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[must_use]
pub fn txn_key(txn_id: DistTxnId) -> [u8; 8] {
    txn_id.to_be_bytes()
}

#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

impl PreparedTxnRecord {
    #[must_use]
    pub fn new(txn_id: DistTxnId, ops: Vec<Op>, deadline_ms: u64) -> Self {
        Self {
            version: DIST_TXN_RECORD_VERSION,
            txn_id,
            ops,
            prepared_at_ms: now_ms(),
            deadline_ms,
            retry_count: 0,
        }
    }
}

impl CoordinatorDecisionRecord {
    #[must_use]
    pub fn new(txn_id: DistTxnId, decision: Decision, deadline_ms: u64) -> Self {
        Self {
            version: DIST_TXN_RECORD_VERSION,
            txn_id,
            decision,
            decided_at_ms: now_ms(),
            deadline_ms,
            retry_count: 0,
        }
    }
}

impl FinishedTxnRecord {
    #[must_use]
    pub fn new(txn_id: DistTxnId, decision: Decision) -> Self {
        Self {
            version: DIST_TXN_RECORD_VERSION,
            txn_id,
            decision,
            finished_at_ms: now_ms(),
        }
    }
}

/// Encodes a durable participant prepare record.
/// # Errors
/// Fails when JSON serialization fails.
pub fn encode_prepared(record: &PreparedTxnRecord) -> Result<Bytes, StorageError> {
    serde_json::to_vec(record).map_err(|error| StorageError::Backend(error.to_string()))
}

/// Decodes a durable participant prepare record.
/// # Errors
/// Fails when the record bytes are corrupt or use an unsupported shape.
pub fn decode_prepared(bytes: &[u8]) -> Result<PreparedTxnRecord, StorageError> {
    serde_json::from_slice(bytes)
        .map_err(|error| StorageError::Corruption(format!("dist txn prepared record: {error}")))
}

/// Encodes a durable coordinator decision record.
/// # Errors
/// Fails when JSON serialization fails.
pub fn encode_decision(record: &CoordinatorDecisionRecord) -> Result<Bytes, StorageError> {
    serde_json::to_vec(record).map_err(|error| StorageError::Backend(error.to_string()))
}

/// Decodes a durable coordinator decision record.
/// # Errors
/// Fails when the record bytes are corrupt or use an unsupported shape.
pub fn decode_decision(bytes: &[u8]) -> Result<CoordinatorDecisionRecord, StorageError> {
    serde_json::from_slice(bytes)
        .map_err(|error| StorageError::Corruption(format!("dist txn decision record: {error}")))
}

/// Encodes a durable finished participant record.
/// # Errors
/// Fails when JSON serialization fails.
pub fn encode_finished(record: &FinishedTxnRecord) -> Result<Bytes, StorageError> {
    serde_json::to_vec(record).map_err(|error| StorageError::Backend(error.to_string()))
}

impl CoordinatorLog for InMemoryCoordinatorLog {
    fn record_decision(&mut self, txn_id: DistTxnId, decision: Decision) -> Result<(), ReplError> {
        self.decisions.insert(txn_id, decision);
        Ok(())
    }

    fn decision(&self, txn_id: DistTxnId) -> Option<Decision> {
        self.decisions.get(&txn_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::{
        CoordinatorLog, Decision, DistTxnId, InMemoryCoordinatorLog, Participant, Vote,
        two_phase_commit,
    };
    use crate::repl::ReplError;

    struct FakeParticipant {
        vote: Vote,
        events: RefCell<Vec<String>>,
    }

    struct RecordingLog {
        inner: InMemoryCoordinatorLog,
        events: RefCell<Vec<String>>,
    }

    impl FakeParticipant {
        fn new(vote: Vote) -> Self {
            Self {
                vote,
                events: RefCell::new(Vec::new()),
            }
        }
    }

    impl Participant for FakeParticipant {
        fn prepare(&self, txn_id: DistTxnId) -> Result<Vote, ReplError> {
            self.events.borrow_mut().push(format!("prepare:{txn_id}"));
            Ok(self.vote)
        }

        fn finish(&self, txn_id: DistTxnId, decision: Decision) -> Result<(), ReplError> {
            self.events
                .borrow_mut()
                .push(format!("finish:{txn_id}:{decision:?}"));
            Ok(())
        }
    }

    impl RecordingLog {
        fn new() -> Self {
            Self {
                inner: InMemoryCoordinatorLog::new(),
                events: RefCell::new(Vec::new()),
            }
        }
    }

    impl CoordinatorLog for RecordingLog {
        fn record_decision(
            &mut self,
            txn_id: DistTxnId,
            decision: Decision,
        ) -> Result<(), ReplError> {
            self.events
                .borrow_mut()
                .push(format!("decision:{txn_id}:{decision:?}"));
            self.inner.record_decision(txn_id, decision)
        }

        fn decision(&self, txn_id: DistTxnId) -> Option<Decision> {
            self.inner.decision(txn_id)
        }
    }

    #[test]
    fn all_yes_commits() -> Result<(), Box<dyn std::error::Error>> {
        let first = FakeParticipant::new(Vote::Yes);
        let second = FakeParticipant::new(Vote::Yes);
        let mut log = RecordingLog::new();

        let decision = two_phase_commit(7, &[&first, &second], &mut log)?;

        assert_eq!(decision, Decision::Commit);
        assert_eq!(log.decision(7), Some(Decision::Commit));
        assert_eq!(log.events.borrow().as_slice(), ["decision:7:Commit"]);
        assert_eq!(
            first.events.borrow().as_slice(),
            ["prepare:7", "finish:7:Commit"]
        );
        Ok(())
    }

    #[test]
    fn one_no_aborts() -> Result<(), Box<dyn std::error::Error>> {
        let first = FakeParticipant::new(Vote::Yes);
        let second = FakeParticipant::new(Vote::No);
        let mut log = InMemoryCoordinatorLog::new();

        let decision = two_phase_commit(8, &[&first, &second], &mut log)?;

        assert_eq!(decision, Decision::Abort);
        assert_eq!(log.decision(8), Some(Decision::Abort));
        assert_eq!(
            second.events.borrow().as_slice(),
            ["prepare:8", "finish:8:Abort"]
        );
        Ok(())
    }
}
