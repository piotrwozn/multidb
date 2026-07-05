use std::collections::{BTreeMap, BTreeSet};

use crate::{
    hardening::TraceDigest,
    migration::{self, MigrationError},
    query::Row,
    repl::{ReadConsistency, ReplError, Replication},
    storage::{
        Bytes, FaultPlan, ReadTransaction, SimStorage, StorageEngine, StorageError,
        WriteTransaction,
    },
    txn::{self, CommitLogRecord},
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PhaseContract {
    pub id: String,
    pub phase: u32,
    pub invariant: String,
    pub oracle: String,
    pub gate: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ContractRegistry {
    contracts: Vec<PhaseContract>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VerificationReport {
    checked: usize,
    violations: Vec<ConsistencyViolation>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ConsistencyViolation {
    pub contract: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct StorageModel {
    tables: BTreeMap<String, BTreeMap<Bytes, Bytes>>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum StorageAction {
    Put {
        table: String,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        table: String,
        key: Bytes,
    },
    Get {
        table: String,
        key: Bytes,
    },
    Range {
        table: String,
        start: Bytes,
        end: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeterministicScenario {
    pub name: String,
    pub seed: u64,
    pub actions: Vec<StorageAction>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeterministicRun {
    pub trace_digest: TraceDigest,
    pub report: VerificationReport,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct History {
    operations: Vec<OperationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct OperationRecord {
    pub actor: String,
    pub invoke_step: u64,
    pub response_step: u64,
    pub table: String,
    pub key: Bytes,
    pub operation: Operation,
    pub outcome: OperationOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Operation {
    Read,
    Write { value: Option<Bytes> },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum OperationOutcome {
    Read { value: Option<Bytes> },
    WriteOk,
    Failed { error: String },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinearizabilityChecker;

impl PhaseContract {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        phase: u32,
        invariant: impl Into<String>,
        oracle: impl Into<String>,
        gate: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            phase,
            invariant: invariant.into(),
            oracle: oracle.into(),
            gate: gate.into(),
        }
    }
}

impl ContractRegistry {
    #[must_use]
    pub fn new(contracts: Vec<PhaseContract>) -> Self {
        Self { contracts }
    }

    #[must_use]
    pub fn phase33_default() -> Self {
        Self::new(vec![
            PhaseContract::new(
                "storage-model",
                33,
                "storage contents match a naive BTreeMap model",
                "StorageModel",
                "cargo test --lib phase33",
            ),
            PhaseContract::new(
                "index-scan-full-scan",
                33,
                "index scan returns the same logical rows as full scan",
                "sorted row-set equality",
                "cargo test --lib verification",
            ),
            PhaseContract::new(
                "temporal-mvcc",
                33,
                "temporal AS OF rows match MVCC oracle rows",
                "row-set equality at resolved LSN",
                "cargo test --lib phase32 phase33",
            ),
            PhaseContract::new(
                "materialized-view-recompute",
                33,
                "materialized view state matches recomputation from source",
                "row-set equality",
                "cargo test --lib cdc verification",
            ),
            PhaseContract::new(
                "codec-round-trip",
                33,
                "persistent codecs round-trip canonical bytes",
                "encode/decode equality",
                "scripts/fuzz-smoke.ps1",
            ),
            PhaseContract::new(
                "commit-log-round-trip",
                33,
                "commit log binary format decodes to the original logical record",
                "txn encode/decode equality",
                "cargo test --lib txn verification",
            ),
            PhaseContract::new(
                "pg-copy-text",
                33,
                "pg_dump COPY text rows preserve tabs, nulls, and escapes",
                "PostgreSQL COPY text parser",
                "cargo fuzz run pg_copy_text",
            ),
            PhaseContract::new(
                "linearizable-register",
                33,
                "completed writes are not lost under reads after failover",
                "History + LinearizabilityChecker",
                "cargo test --lib phase33",
            ),
        ])
    }

    #[must_use]
    pub fn contracts(&self) -> &[PhaseContract] {
        &self.contracts
    }

    #[must_use]
    pub fn ids(&self) -> BTreeSet<&str> {
        self.contracts
            .iter()
            .map(|contract| contract.id.as_str())
            .collect()
    }
}

impl VerificationReport {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            checked: 0,
            violations: Vec::new(),
        }
    }

    pub fn record_ok(&mut self) {
        self.checked = self.checked.saturating_add(1);
    }

    pub fn push(&mut self, violation: ConsistencyViolation) {
        self.checked = self.checked.saturating_add(1);
        self.violations.push(violation);
    }

    pub fn merge(&mut self, other: Self) {
        self.checked = self.checked.saturating_add(other.checked);
        self.violations.extend(other.violations);
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }

    #[must_use]
    pub const fn checked(&self) -> usize {
        self.checked
    }

    #[must_use]
    pub fn violations(&self) -> &[ConsistencyViolation] {
        &self.violations
    }
}

impl ConsistencyViolation {
    #[must_use]
    pub fn new(contract: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            contract: contract.into(),
            detail: detail.into(),
        }
    }
}

impl StorageModel {
    #[must_use]
    pub fn tables(&self) -> &BTreeMap<String, BTreeMap<Bytes, Bytes>> {
        &self.tables
    }

    pub fn apply(&mut self, action: &StorageAction) {
        match action {
            StorageAction::Put { table, key, value } => {
                self.tables
                    .entry(table.clone())
                    .or_default()
                    .insert(key.clone(), value.clone());
            }
            StorageAction::Delete { table, key } => {
                self.tables.entry(table.clone()).or_default().remove(key);
            }
            StorageAction::Get { .. } | StorageAction::Range { .. } => {}
        }
    }

    #[must_use]
    pub fn get(&self, table: &str, key: &[u8]) -> Option<Bytes> {
        self.tables
            .get(table)
            .and_then(|rows| rows.get(key))
            .cloned()
    }

    #[must_use]
    pub fn range(&self, table: &str, start: &[u8], end: &[u8]) -> Vec<(Bytes, Bytes)> {
        let Some(rows) = self.tables.get(table) else {
            return Vec::new();
        };
        if end.is_empty() {
            return rows
                .range(start.to_vec()..)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
        }
        if start >= end {
            return Vec::new();
        }
        rows.range(start.to_vec()..end.to_vec())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl DeterministicScenario {
    #[must_use]
    pub fn new(name: impl Into<String>, seed: u64, actions: Vec<StorageAction>) -> Self {
        Self {
            name: name.into(),
            seed,
            actions,
        }
    }

    /// Runs the scenario on `SimStorage` and returns its deterministic digest.
    /// # Errors
    /// Fails when a simulated storage operation fails.
    pub fn run(&self) -> Result<DeterministicRun, StorageError> {
        let plan = FaultPlan::seeded(self.seed);
        self.run_with_plan(plan)
    }

    /// Runs the scenario with an explicit fault plan.
    /// # Errors
    /// Fails when a simulated storage operation fails.
    pub fn run_with_plan(&self, plan: FaultPlan) -> Result<DeterministicRun, StorageError> {
        let storage = SimStorage::new(plan);
        let report = verify_storage_actions(&storage, &self.actions)?;
        Ok(DeterministicRun {
            trace_digest: storage.trace_digest()?,
            report,
        })
    }
}

impl History {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_operations(operations: Vec<OperationRecord>) -> Self {
        Self { operations }
    }

    pub fn push(&mut self, operation: OperationRecord) {
        self.operations.push(operation);
    }

    #[must_use]
    pub fn operations(&self) -> &[OperationRecord] {
        &self.operations
    }
}

impl OperationRecord {
    #[must_use]
    pub fn write_ok(
        actor: impl Into<String>,
        invoke_step: u64,
        response_step: u64,
        table: impl Into<String>,
        key: Bytes,
        value: Option<Bytes>,
    ) -> Self {
        Self {
            actor: actor.into(),
            invoke_step,
            response_step,
            table: table.into(),
            key,
            operation: Operation::Write { value },
            outcome: OperationOutcome::WriteOk,
        }
    }

    #[must_use]
    pub fn read_ok(
        actor: impl Into<String>,
        invoke_step: u64,
        response_step: u64,
        table: impl Into<String>,
        key: Bytes,
        value: Option<Bytes>,
    ) -> Self {
        Self {
            actor: actor.into(),
            invoke_step,
            response_step,
            table: table.into(),
            key,
            operation: Operation::Read,
            outcome: OperationOutcome::Read { value },
        }
    }

    #[must_use]
    pub fn failed(
        actor: impl Into<String>,
        invoke_step: u64,
        response_step: u64,
        table: impl Into<String>,
        key: Bytes,
        operation: Operation,
        error: impl Into<String>,
    ) -> Self {
        Self {
            actor: actor.into(),
            invoke_step,
            response_step,
            table: table.into(),
            key,
            operation,
            outcome: OperationOutcome::Failed {
                error: error.into(),
            },
        }
    }
}

impl LinearizabilityChecker {
    #[must_use]
    pub fn check_register(history: &History, initial: Option<&[u8]>) -> VerificationReport {
        let mut report = VerificationReport::new();
        let mut by_key = BTreeMap::<(String, Bytes), Vec<&OperationRecord>>::new();
        for operation in history.operations() {
            by_key
                .entry((operation.table.clone(), operation.key.clone()))
                .or_default()
                .push(operation);
        }

        for ((table, key), operations) in by_key {
            check_register_key(&mut report, &table, &key, &operations, initial);
        }
        if history.operations().is_empty() {
            report.record_ok();
        }
        report
    }
}

/// Applies storage actions to a real engine and compares every read with the model.
/// # Errors
/// Fails when the storage engine rejects an operation.
pub fn verify_storage_actions<S: StorageEngine>(
    storage: &S,
    actions: &[StorageAction],
) -> Result<VerificationReport, StorageError> {
    let mut model = StorageModel::default();
    let mut report = VerificationReport::new();
    for action in actions {
        match action {
            StorageAction::Put { table, key, value } => {
                let mut write = storage.begin_write()?;
                write.put(table, key, value)?;
                write.commit()?;
                model.apply(action);
                report.record_ok();
            }
            StorageAction::Delete { table, key } => {
                let mut write = storage.begin_write()?;
                write.delete(table, key)?;
                write.commit()?;
                model.apply(action);
                report.record_ok();
            }
            StorageAction::Get { table, key } => {
                let read = storage.begin_read()?;
                let actual = read.get(table, key)?;
                let expected = model.get(table, key);
                if actual == expected {
                    report.record_ok();
                } else {
                    report.push(ConsistencyViolation::new(
                        "storage-model",
                        format!(
                            "get {table}/{} returned {actual:?}, expected {expected:?}",
                            hex_key(key)
                        ),
                    ));
                }
            }
            StorageAction::Range { table, start, end } => {
                let read = storage.begin_read()?;
                let actual = read
                    .range(table, start, end)?
                    .collect::<Result<Vec<_>, _>>()?;
                let expected = model.range(table, start, end);
                if actual == expected {
                    report.record_ok();
                } else {
                    report.push(ConsistencyViolation::new(
                        "storage-model",
                        format!(
                            "range {table}/{}..{} returned {actual:?}, expected {expected:?}",
                            hex_key(start),
                            hex_key(end)
                        ),
                    ));
                }
            }
        }
    }

    for (table, expected) in model.tables() {
        let read = storage.begin_read()?;
        let actual = read
            .range(table, &[], &[])?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if actual == *expected {
            report.record_ok();
        } else {
            report.push(ConsistencyViolation::new(
                "storage-model-final",
                format!(
                    "table {table} ended with {} rows, expected {} rows",
                    actual.len(),
                    expected.len()
                ),
            ));
        }
    }

    Ok(report)
}

#[must_use]
pub fn check_index_scan_matches_full_scan(
    index_scan: &[(Bytes, Bytes)],
    full_scan: &[(Bytes, Bytes)],
) -> VerificationReport {
    check_row_bytes_match("index-scan-full-scan", index_scan, full_scan)
}

#[must_use]
pub fn check_temporal_rows_match_mvcc(as_of_rows: &[Row], mvcc_rows: &[Row]) -> VerificationReport {
    check_rows_match("temporal-mvcc", as_of_rows, mvcc_rows)
}

#[must_use]
pub fn check_materialized_view_matches_recompute(
    materialized_rows: &[Row],
    recomputed_rows: &[Row],
) -> VerificationReport {
    check_rows_match(
        "materialized-view-recompute",
        materialized_rows,
        recomputed_rows,
    )
}

#[must_use]
pub fn check_codec_round_trip(
    contract: &str,
    original: &[u8],
    decoded: &[u8],
) -> VerificationReport {
    let mut report = VerificationReport::new();
    if original == decoded {
        report.record_ok();
    } else {
        report.push(ConsistencyViolation::new(
            contract,
            format!(
                "round trip changed {} bytes into {} bytes",
                original.len(),
                decoded.len()
            ),
        ));
    }
    report
}

/// Verifies the commit-log binary round trip.
/// # Errors
/// Fails when commit-log encoding or decoding fails.
pub fn check_commit_log_round_trip(
    record: &CommitLogRecord,
) -> Result<VerificationReport, StorageError> {
    let encoded = txn::encode_commit_log_record(record)?;
    let decoded = txn::decode_commit_log_record(&encoded)?;
    let mut report = VerificationReport::new();
    if decoded == *record {
        report.record_ok();
    } else {
        report.push(ConsistencyViolation::new(
            "commit-log-round-trip",
            "decoded commit log record differed from original",
        ));
    }
    Ok(report)
}

/// Verifies that a `PostgreSQL` COPY text row parses without data-loss shortcuts.
/// # Errors
/// Fails when the COPY parser rejects the row.
pub fn parse_pg_copy_text_for_verification(
    line: &str,
) -> Result<Vec<Option<String>>, MigrationError> {
    migration::parse_pg_copy_text_values(line)
}

/// Runs a write through `Replication`, records it, and keeps the history format uniform.
/// # Errors
/// Fails when replication rejects the write.
pub fn record_replicated_write(
    repl: &dyn Replication,
    history: &mut History,
    actor: &str,
    step: &mut u64,
    table: &str,
    key: Bytes,
    value: Bytes,
) -> Result<(), ReplError> {
    let invoke = next_step(step);
    repl.propose(crate::repl::Op::Put {
        table: table.to_owned(),
        key: key.clone(),
        value: value.clone(),
    })?;
    let response = next_step(step);
    history.push(OperationRecord::write_ok(
        actor,
        invoke,
        response,
        table,
        key,
        Some(value),
    ));
    Ok(())
}

/// Runs a read through `Replication` and records the observed value.
/// # Errors
/// Fails when replication rejects the read.
pub fn record_replicated_read(
    repl: &dyn Replication,
    history: &mut History,
    actor: &str,
    step: &mut u64,
    table: &str,
    key: Bytes,
    consistency: ReadConsistency,
) -> Result<Option<Bytes>, ReplError> {
    let invoke = next_step(step);
    let value = repl.read(table, &key, consistency)?;
    let response = next_step(step);
    history.push(OperationRecord::read_ok(
        actor,
        invoke,
        response,
        table,
        key,
        value.clone(),
    ));
    Ok(value)
}

fn check_register_key(
    report: &mut VerificationReport,
    table: &str,
    key: &[u8],
    operations: &[&OperationRecord],
    initial: Option<&[u8]>,
) {
    for operation in operations {
        if operation.invoke_step > operation.response_step {
            report.push(ConsistencyViolation::new(
                "linearizable-register",
                format!("{} returned before invocation", operation.actor),
            ));
            continue;
        }
        report.record_ok();
    }

    for read in operations
        .iter()
        .filter(|operation| matches!(operation.operation, Operation::Read))
    {
        let OperationOutcome::Read { value } = &read.outcome else {
            report.push(ConsistencyViolation::new(
                "linearizable-register",
                "read operation did not record a read result",
            ));
            continue;
        };
        let expected = latest_completed_write_before(operations, read.invoke_step)
            .map_or_else(|| initial.map(<[u8]>::to_vec), |write| write.value);
        if value == &expected
            || read_can_observe_overlapping_write(operations, read, value.as_ref())
        {
            report.record_ok();
            continue;
        }
        report.push(ConsistencyViolation::new(
            "linearizable-register",
            format!(
                "read {table}/{} at {}..{} observed {value:?}, expected {expected:?}",
                hex_key(key),
                read.invoke_step,
                read.response_step
            ),
        ));
    }
}

fn latest_completed_write_before(
    operations: &[&OperationRecord],
    read_invoke: u64,
) -> Option<CompletedWrite> {
    operations
        .iter()
        .filter_map(|operation| {
            if operation.response_step >= read_invoke {
                return None;
            }
            let Operation::Write { value } = &operation.operation else {
                return None;
            };
            if !matches!(operation.outcome, OperationOutcome::WriteOk) {
                return None;
            }
            Some((
                operation.response_step,
                CompletedWrite {
                    value: value.clone(),
                },
            ))
        })
        .max_by_key(|(response, _)| *response)
        .map(|(_, write)| write)
}

fn read_can_observe_overlapping_write(
    operations: &[&OperationRecord],
    read: &OperationRecord,
    observed: Option<&Bytes>,
) -> bool {
    operations.iter().any(|operation| {
        let Operation::Write { value } = &operation.operation else {
            return false;
        };
        matches!(operation.outcome, OperationOutcome::WriteOk)
            && value.as_ref() == observed
            && operation.invoke_step <= read.response_step
            && operation.response_step >= read.invoke_step
    })
}

struct CompletedWrite {
    value: Option<Bytes>,
}

fn check_row_bytes_match(
    contract: &str,
    left: &[(Bytes, Bytes)],
    right: &[(Bytes, Bytes)],
) -> VerificationReport {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort();
    right.sort();
    let mut report = VerificationReport::new();
    if left == right {
        report.record_ok();
    } else {
        report.push(ConsistencyViolation::new(
            contract,
            format!("left rows {left:?} did not match right rows {right:?}"),
        ));
    }
    report
}

fn check_rows_match(contract: &str, left: &[Row], right: &[Row]) -> VerificationReport {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by_key(|row| format!("{row:?}"));
    right.sort_by_key(|row| format!("{row:?}"));
    let mut report = VerificationReport::new();
    if left == right {
        report.record_ok();
    } else {
        report.push(ConsistencyViolation::new(
            contract,
            format!("left rows {left:?} did not match right rows {right:?}"),
        ));
    }
    report
}

fn next_step(step: &mut u64) -> u64 {
    *step = step.saturating_add(1);
    *step
}

fn hex_key(key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len().saturating_mul(2));
    for byte in key {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod phase33_tests {
    use std::time::{Duration, UNIX_EPOCH};

    use stateright::{Checker, Model, Property};

    use super::{
        ContractRegistry, DeterministicScenario, History, LinearizabilityChecker, OperationRecord,
        StorageAction, check_codec_round_trip, check_commit_log_round_trip,
        check_index_scan_matches_full_scan, check_materialized_view_matches_recompute,
        check_temporal_rows_match_mvcc, parse_pg_copy_text_for_verification,
        record_replicated_read, record_replicated_write, verify_storage_actions,
    };
    use crate::{
        model::Value,
        phase30::{InternalTransportConfig, InternalTransportSecurity},
        repl::{CpClusterConfig, CpRaft, RaftNode, ReadConsistency, ReplError, Replication},
        storage::{FaultPlan, MemEngine},
        txn::{CommitLogRecord, CommitLogWrite},
    };

    #[test]
    fn phase33_contract_registry_covers_required_oracles() {
        let registry = ContractRegistry::phase33_default();
        let ids = registry.ids();
        for required in [
            "storage-model",
            "index-scan-full-scan",
            "temporal-mvcc",
            "materialized-view-recompute",
            "codec-round-trip",
            "commit-log-round-trip",
            "pg-copy-text",
            "linearizable-register",
        ] {
            assert!(ids.contains(required));
        }
    }

    #[test]
    fn phase33_storage_actions_match_naive_model() -> Result<(), Box<dyn std::error::Error>> {
        let storage = MemEngine::new();
        let actions = vec![
            put("t", b"a", b"1"),
            put("t", b"b", b"2"),
            get("t", b"a"),
            range("t", b"a", b"c"),
            delete("t", b"a"),
            get("t", b"a"),
        ];

        let report = verify_storage_actions(&storage, &actions)?;

        assert!(report.is_ok());
        assert!(report.checked() >= actions.len());
        Ok(())
    }

    #[test]
    fn phase33_deterministic_scenario_replays_to_same_trace()
    -> Result<(), Box<dyn std::error::Error>> {
        let scenario = DeterministicScenario::new(
            "storage-seed",
            33,
            vec![put("t", b"a", b"1"), get("t", b"a"), range("t", &[], &[])],
        );
        let plan = FaultPlan::seeded_with_budget(33, 6, 2);

        let first = scenario.run_with_plan(plan.clone())?;
        let second = scenario.run_with_plan(plan)?;

        assert!(first.report.is_ok());
        assert_eq!(first.trace_digest, second.trace_digest);
        Ok(())
    }

    #[test]
    fn phase33_row_oracles_detect_mismatches() {
        let left = vec![(b"a".to_vec(), b"1".to_vec())];
        let right = vec![(b"a".to_vec(), b"1".to_vec())];
        assert!(check_index_scan_matches_full_scan(&left, &right).is_ok());
        assert!(!check_index_scan_matches_full_scan(&left, &[]).is_ok());

        let row = vec![Value::Int(1), Value::Str("Ada".to_owned())];
        assert!(
            check_temporal_rows_match_mvcc(std::slice::from_ref(&row), std::slice::from_ref(&row))
                .is_ok()
        );
        assert!(
            check_materialized_view_matches_recompute(
                std::slice::from_ref(&row),
                std::slice::from_ref(&row)
            )
            .is_ok()
        );
        assert!(check_codec_round_trip("codec-round-trip", b"abc", b"abc").is_ok());
        assert!(!check_codec_round_trip("codec-round-trip", b"abc", b"ab").is_ok());
    }

    #[test]
    fn phase33_commit_log_and_pg_copy_contracts() -> Result<(), Box<dyn std::error::Error>> {
        let record = CommitLogRecord {
            txn_id: 7,
            committed_at: UNIX_EPOCH + Duration::from_millis(33),
            hlc: None,
            writes: vec![CommitLogWrite {
                table: "t".to_owned(),
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
            }],
        };
        assert!(check_commit_log_round_trip(&record)?.is_ok());

        let fields = parse_pg_copy_text_for_verification("Ada\\tLovelace\t\\N\tline\\nnext")?;
        assert_eq!(
            fields,
            vec![
                Some("Ada\tLovelace".to_owned()),
                None,
                Some("line\nnext".to_owned())
            ]
        );
        Ok(())
    }

    #[test]
    fn phase33_linearizability_checker_catches_split_brain_history() {
        let valid = History::from_operations(vec![
            OperationRecord::write_ok("a", 1, 2, "t", b"k".to_vec(), Some(b"1".to_vec())),
            OperationRecord::write_ok("b", 3, 4, "t", b"k".to_vec(), Some(b"2".to_vec())),
            OperationRecord::read_ok("c", 5, 6, "t", b"k".to_vec(), Some(b"2".to_vec())),
        ]);
        assert!(LinearizabilityChecker::check_register(&valid, None).is_ok());

        let split_brain = History::from_operations(vec![
            OperationRecord::write_ok("a", 1, 2, "t", b"k".to_vec(), Some(b"1".to_vec())),
            OperationRecord::write_ok("b", 3, 4, "t", b"k".to_vec(), Some(b"2".to_vec())),
            OperationRecord::read_ok("c", 5, 6, "t", b"k".to_vec(), Some(b"1".to_vec())),
        ]);
        let report = LinearizabilityChecker::check_register(&split_brain, None);
        assert!(!report.is_ok());
        assert_eq!(report.violations().len(), 1);
    }

    #[test]
    fn phase33_cp_quorum_history_is_linearizable() -> Result<(), Box<dyn std::error::Error>> {
        let raft = CpRaft::new(MemEngine::new(), three_voter_config())?;
        let mut history = History::new();
        let mut step = 0;

        raft.set_available_voters_for_tests([1])?;
        let no_quorum = raft.propose(crate::repl::Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"lost".to_vec(),
        });
        assert!(matches!(no_quorum, Err(ReplError::NoQuorum)));

        raft.set_available_voters_for_tests([1, 2])?;
        record_replicated_write(
            &raft,
            &mut history,
            "writer",
            &mut step,
            "t",
            b"k".to_vec(),
            b"committed".to_vec(),
        )?;
        let value = record_replicated_read(
            &raft,
            &mut history,
            "reader",
            &mut step,
            "t",
            b"k".to_vec(),
            ReadConsistency::Strong,
        )?;

        assert_eq!(value, Some(b"committed".to_vec()));
        assert!(LinearizabilityChecker::check_register(&history, None).is_ok());
        Ok(())
    }

    #[test]
    fn phase33_stateright_checks_cp_single_slot_safety() {
        CpSlotModel {
            max_step: 3,
            values: 2,
        }
        .checker()
        .target_max_depth(4)
        .spawn_dfs()
        .join()
        .assert_properties();
    }

    #[test]
    fn phase33_stateright_checks_2pc_decision_safety() {
        TwoPcModel { participants: 2 }
            .checker()
            .target_max_depth(5)
            .spawn_dfs()
            .join()
            .assert_properties();
    }

    fn put(table: &str, key: &[u8], value: &[u8]) -> StorageAction {
        StorageAction::Put {
            table: table.to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn delete(table: &str, key: &[u8]) -> StorageAction {
        StorageAction::Delete {
            table: table.to_owned(),
            key: key.to_vec(),
        }
    }

    fn get(table: &str, key: &[u8]) -> StorageAction {
        StorageAction::Get {
            table: table.to_owned(),
            key: key.to_vec(),
        }
    }

    fn range(table: &str, start: &[u8], end: &[u8]) -> StorageAction {
        StorageAction::Range {
            table: table.to_owned(),
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    fn three_voter_config() -> CpClusterConfig {
        CpClusterConfig::new(
            1,
            "127.0.0.1:7101",
            vec![
                RaftNode::new(1, "127.0.0.1:7101"),
                RaftNode::new(2, "127.0.0.1:7102"),
                RaftNode::new(3, "127.0.0.1:7103"),
            ],
        )
        .with_transport(InternalTransportConfig::new(
            "127.0.0.1:7101",
            InternalTransportSecurity::PlaintextForTests,
        ))
    }

    #[derive(Clone)]
    struct CpSlotModel {
        max_step: u8,
        values: u8,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct CpSlotState {
        accepted: [Option<u8>; 3],
        committed: Option<u8>,
        step: u8,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    enum CpSlotAction {
        QuorumCommit { value: u8, voters: u8 },
        LocalAccept { value: u8, node: usize },
    }

    impl Model for CpSlotModel {
        type State = CpSlotState;
        type Action = CpSlotAction;

        fn init_states(&self) -> Vec<Self::State> {
            vec![CpSlotState {
                accepted: [None, None, None],
                committed: None,
                step: 0,
            }]
        }

        fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
            if state.step >= self.max_step {
                return;
            }
            for value in 1..=self.values {
                for voters in 1_u8..8 {
                    if voters.count_ones() >= 2 {
                        actions.push(CpSlotAction::QuorumCommit { value, voters });
                    }
                }
                for node in 0..3 {
                    actions.push(CpSlotAction::LocalAccept { value, node });
                }
            }
        }

        fn next_state(
            &self,
            last_state: &Self::State,
            action: Self::Action,
        ) -> Option<Self::State> {
            let mut next = last_state.clone();
            next.step = next.step.saturating_add(1);
            match action {
                CpSlotAction::QuorumCommit { value, voters } => {
                    if next.committed.is_some_and(|committed| committed != value) {
                        return None;
                    }
                    for node in 0..3 {
                        if voters & (1 << node) != 0 {
                            next.accepted[node] = Some(value);
                        }
                    }
                    next.committed = Some(value);
                }
                CpSlotAction::LocalAccept { value, node } => {
                    if next.committed.is_some_and(|committed| committed != value) {
                        return None;
                    }
                    next.accepted[node] = Some(value);
                }
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![
                Property::always("single committed value", |_, state: &CpSlotState| {
                    let committed = state
                        .accepted
                        .iter()
                        .flatten()
                        .filter(|value| Some(**value) == state.committed)
                        .count();
                    state.committed.is_none() || committed >= 2
                }),
                Property::sometimes("commit reachable", |_, state: &CpSlotState| {
                    state.committed.is_some()
                }),
            ]
        }
    }

    #[derive(Clone)]
    struct TwoPcModel {
        participants: usize,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct TwoPcState {
        votes: [Option<bool>; 2],
        decision: Option<bool>,
        step: u8,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    enum TwoPcAction {
        Vote { participant: usize, yes: bool },
        DecideCommit,
        DecideAbort,
    }

    impl Model for TwoPcModel {
        type State = TwoPcState;
        type Action = TwoPcAction;

        fn init_states(&self) -> Vec<Self::State> {
            vec![TwoPcState {
                votes: [None, None],
                decision: None,
                step: 0,
            }]
        }

        fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
            if state.step >= 4 || state.decision.is_some() {
                return;
            }
            for participant in 0..self.participants.min(2) {
                if state.votes[participant].is_none() {
                    actions.push(TwoPcAction::Vote {
                        participant,
                        yes: true,
                    });
                    actions.push(TwoPcAction::Vote {
                        participant,
                        yes: false,
                    });
                }
            }
            actions.push(TwoPcAction::DecideAbort);
            if state.votes.iter().all(|vote| *vote == Some(true)) {
                actions.push(TwoPcAction::DecideCommit);
            }
        }

        fn next_state(
            &self,
            last_state: &Self::State,
            action: Self::Action,
        ) -> Option<Self::State> {
            let mut next = last_state.clone();
            next.step = next.step.saturating_add(1);
            match action {
                TwoPcAction::Vote { participant, yes } => {
                    if participant >= self.participants.min(2) || next.votes[participant].is_some()
                    {
                        return None;
                    }
                    next.votes[participant] = Some(yes);
                }
                TwoPcAction::DecideCommit => {
                    if !next.votes.iter().all(|vote| *vote == Some(true)) {
                        return None;
                    }
                    next.decision = Some(true);
                }
                TwoPcAction::DecideAbort => next.decision = Some(false),
            }
            Some(next)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            vec![
                Property::always("commit only after yes votes", |_, state: &TwoPcState| {
                    state.decision != Some(true)
                        || state.votes.iter().all(|vote| *vote == Some(true))
                }),
                Property::sometimes("commit reachable", |_, state: &TwoPcState| {
                    state.decision == Some(true)
                }),
            ]
        }
    }
}
