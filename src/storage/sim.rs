use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use crate::{
    hardening::{TraceDigest, TraceEvent, trace_digest},
    storage::{Bytes, RangeIter, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
};

type Table = BTreeMap<Bytes, Bytes>;
type DatabaseMap = BTreeMap<String, Table>;
type PendingMap = BTreeMap<(String, Bytes), Option<Bytes>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum FaultPoint {
    BeginRead,
    BeginWrite,
    ReadGet,
    ReadRange,
    WritePut,
    WriteDelete,
    Commit,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Fault {
    IoError,
    NoSpace,
    DelayMs(u64),
    LostAfterCommit,
    CorruptRead { xor: u8 },
    TornWrite { max_ops: usize },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ScheduledFault {
    pub point: FaultPoint,
    pub at_step: u64,
    pub fault: Fault,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FaultPlan {
    pub seed: u64,
    pub faults: Vec<ScheduledFault>,
}

#[derive(Debug)]
pub struct SimStorage {
    inner: RwLock<Arc<DatabaseMap>>,
    writer: Mutex<()>,
    state: Mutex<SimState>,
}

#[derive(Debug, Default)]
struct SimState {
    plan: FaultPlan,
    step: u64,
    trace: Vec<TraceEvent>,
}

#[derive(Debug)]
pub struct SimReadTxn<'engine> {
    engine: &'engine SimStorage,
    snapshot: Arc<DatabaseMap>,
}

#[derive(Debug)]
pub struct SimWriteTxn<'engine> {
    engine: &'engine SimStorage,
    _writer: MutexGuard<'engine, ()>,
    snapshot: Arc<DatabaseMap>,
    pending: PendingMap,
}

impl ScheduledFault {
    #[must_use]
    pub const fn new(point: FaultPoint, at_step: u64, fault: Fault) -> Self {
        Self {
            point,
            at_step,
            fault,
        }
    }
}

impl FaultPlan {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            seed,
            faults: Vec::new(),
        }
    }

    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self::seeded_with_budget(seed, 12, 3)
    }

    #[must_use]
    pub fn seeded_with_budget(seed: u64, max_step: u64, max_faults: usize) -> Self {
        let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
        let mut plan = Self::new(seed);
        let step_limit = max_step.max(1);
        for _ in 0..max_faults {
            state = next_seed(state);
            let point = match state % 7 {
                0 => FaultPoint::BeginRead,
                1 => FaultPoint::BeginWrite,
                2 => FaultPoint::ReadGet,
                3 => FaultPoint::ReadRange,
                4 => FaultPoint::WritePut,
                5 => FaultPoint::WriteDelete,
                _ => FaultPoint::Commit,
            };
            state = next_seed(state);
            let at_step = state % step_limit + 1;
            plan.faults
                .push(ScheduledFault::new(point, at_step, Fault::DelayMs(0)));
        }
        plan.faults
            .sort_by_key(|fault| (fault.at_step, fault_point_order(fault.point)));
        plan
    }

    #[must_use]
    pub fn with_fault(mut self, point: FaultPoint, at_step: u64, fault: Fault) -> Self {
        self.faults.push(ScheduledFault::new(point, at_step, fault));
        self
    }
}

impl SimStorage {
    #[must_use]
    pub fn new(plan: FaultPlan) -> Self {
        Self {
            inner: RwLock::new(Arc::new(DatabaseMap::new())),
            writer: Mutex::new(()),
            state: Mutex::new(SimState {
                plan,
                step: 0,
                trace: Vec::new(),
            }),
        }
    }

    #[must_use]
    pub fn without_faults() -> Self {
        Self::new(FaultPlan::default())
    }

    /// Returns a stable digest for the observed simulated execution trace.
    /// # Errors
    /// Fails when the trace lock is poisoned.
    pub fn trace_digest(&self) -> Result<TraceDigest, StorageError> {
        let state = lock_state(&self.state)?;
        Ok(trace_digest(&state.trace))
    }

    /// Returns a copy of trace events recorded so far.
    /// # Errors
    /// Fails when the trace lock is poisoned.
    pub fn trace(&self) -> Result<Vec<TraceEvent>, StorageError> {
        Ok(lock_state(&self.state)?.trace.clone())
    }

    fn apply_fault(&self, point: FaultPoint, detail: &str) -> Result<Option<Fault>, StorageError> {
        let mut state = lock_state(&self.state)?;
        state.step = state.step.saturating_add(1);
        let step = state.step;
        let fault = state
            .plan
            .faults
            .iter()
            .find(|scheduled| scheduled.point == point && scheduled.at_step == step)
            .map(|scheduled| scheduled.fault.clone());
        let trace_detail = fault.as_ref().map_or_else(
            || detail.to_owned(),
            |fault| format!("{detail}; fault={fault:?}"),
        );
        state.trace.push(TraceEvent::new(
            step,
            "sim-storage",
            format!("{point:?}"),
            trace_detail,
        ));
        drop(state);

        match &fault {
            Some(Fault::IoError) => Err(StorageError::Io(std::io::Error::other(
                "simulated io error",
            ))),
            Some(Fault::NoSpace) => Err(StorageError::Backend("simulated no space".to_owned())),
            Some(Fault::DelayMs(ms)) => {
                std::thread::sleep(Duration::from_millis(*ms));
                Ok(fault)
            }
            _ => Ok(fault),
        }
    }
}

impl Default for SimStorage {
    fn default() -> Self {
        Self::without_faults()
    }
}

impl StorageEngine for SimStorage {
    type ReadTxn<'a> = SimReadTxn<'a>;
    type WriteTxn<'a> = SimWriteTxn<'a>;

    fn begin_read(&self) -> Result<Self::ReadTxn<'_>, StorageError> {
        self.apply_fault(FaultPoint::BeginRead, "begin read")?;
        let snapshot = Arc::clone(&*read_inner(&self.inner)?);
        Ok(SimReadTxn {
            engine: self,
            snapshot,
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTxn<'_>, StorageError> {
        self.apply_fault(FaultPoint::BeginWrite, "begin write")?;
        let writer = lock_writer(&self.writer)?;
        let snapshot = Arc::clone(&*read_inner(&self.inner)?);
        Ok(SimWriteTxn {
            engine: self,
            _writer: writer,
            snapshot,
            pending: PendingMap::new(),
        })
    }
}

impl ReadTransaction for SimReadTxn<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let fault = self
            .engine
            .apply_fault(FaultPoint::ReadGet, &format!("{table}/{}", hex_key(key)))?;
        let value = self
            .snapshot
            .get(table)
            .and_then(|data| data.get(key))
            .cloned();
        Ok(corrupt_if_requested(value, fault.as_ref()))
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let fault = self.engine.apply_fault(
            FaultPoint::ReadRange,
            &format!("{table}/{}..{}", hex_key(start), hex_key(end)),
        )?;
        let mut rows = self
            .snapshot
            .get(table)
            .map_or_else(Vec::new, |data| collect_range(data, start, end));
        if let Some(Fault::CorruptRead { xor }) = fault
            && let Some((_, value)) = rows.first_mut()
            && let Some(first) = value.first_mut()
        {
            *first ^= xor;
        }
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl ReadTransaction for SimWriteTxn<'_> {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let fault = self
            .engine
            .apply_fault(FaultPoint::ReadGet, &format!("{table}/{}", hex_key(key)))?;
        let pending_key = (table.to_owned(), key.to_vec());
        let value = if let Some(value) = self.pending.get(&pending_key) {
            value.clone()
        } else {
            self.snapshot
                .get(table)
                .and_then(|data| data.get(key))
                .cloned()
        };
        Ok(corrupt_if_requested(value, fault.as_ref()))
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let fault = self.engine.apply_fault(
            FaultPoint::ReadRange,
            &format!("{table}/{}..{}", hex_key(start), hex_key(end)),
        )?;
        let materialized = self.materialized_table(table);
        let mut rows = collect_range(&materialized, start, end);
        if let Some(Fault::CorruptRead { xor }) = fault
            && let Some((_, value)) = rows.first_mut()
            && let Some(first) = value.first_mut()
        {
            *first ^= xor;
        }
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl ReadTransaction for SimStorage {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let fault = self.apply_fault(FaultPoint::ReadGet, &format!("{table}/{}", hex_key(key)))?;
        let value = read_inner(&self.inner)?
            .get(table)
            .and_then(|data| data.get(key))
            .cloned();
        Ok(corrupt_if_requested(value, fault.as_ref()))
    }

    fn range<'txn>(
        &'txn self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RangeIter<'txn>, StorageError> {
        let fault = self.apply_fault(
            FaultPoint::ReadRange,
            &format!("{table}/{}..{}", hex_key(start), hex_key(end)),
        )?;
        let mut rows = read_inner(&self.inner)?
            .get(table)
            .map_or_else(Vec::new, |data| collect_range(data, start, end));
        if let Some(Fault::CorruptRead { xor }) = fault
            && let Some((_, value)) = rows.first_mut()
            && let Some(first) = value.first_mut()
        {
            *first ^= xor;
        }
        Ok(Box::new(rows.into_iter().map(Ok)))
    }
}

impl WriteTransaction for SimWriteTxn<'_> {
    fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.engine
            .apply_fault(FaultPoint::WritePut, &format!("{table}/{}", hex_key(key)))?;
        self.pending
            .insert((table.to_owned(), key.to_vec()), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8]) -> Result<(), StorageError> {
        self.engine.apply_fault(
            FaultPoint::WriteDelete,
            &format!("{table}/{}", hex_key(key)),
        )?;
        self.pending.insert((table.to_owned(), key.to_vec()), None);
        Ok(())
    }

    fn commit(self) -> Result<(), StorageError> {
        let fault = self.engine.apply_fault(FaultPoint::Commit, "commit")?;
        if matches!(fault, Some(Fault::LostAfterCommit)) {
            return Ok(());
        }

        let max_ops = match fault {
            Some(Fault::TornWrite { max_ops }) => Some(max_ops),
            _ => None,
        };
        let mut next = self.snapshot.as_ref().clone();
        for (index, ((table, key), value)) in self.pending.into_iter().enumerate() {
            if max_ops.is_some_and(|limit| index >= limit) {
                break;
            }
            let data = next.entry(table).or_default();
            match value {
                Some(bytes) => {
                    data.insert(key, bytes);
                }
                None => {
                    data.remove(&key);
                }
            }
        }
        *write_inner(&self.engine.inner)? = Arc::new(next);
        Ok(())
    }

    fn rollback(self) {}
}

impl SimWriteTxn<'_> {
    fn materialized_table(&self, table: &str) -> Table {
        let mut materialized = self.snapshot.get(table).cloned().unwrap_or_default();
        for ((pending_table, key), value) in &self.pending {
            if pending_table == table {
                match value {
                    Some(bytes) => {
                        materialized.insert(key.clone(), bytes.clone());
                    }
                    None => {
                        materialized.remove(key);
                    }
                }
            }
        }
        materialized
    }
}

fn corrupt_if_requested(value: Option<Bytes>, fault: Option<&Fault>) -> Option<Bytes> {
    let Some(Fault::CorruptRead { xor }) = fault else {
        return value;
    };
    value.map(|mut bytes| {
        if let Some(first) = bytes.first_mut() {
            *first ^= *xor;
        }
        bytes
    })
}

fn next_seed(value: u64) -> u64 {
    let mut next = value;
    next ^= next << 13;
    next ^= next >> 7;
    next ^ (next << 17)
}

const fn fault_point_order(point: FaultPoint) -> u8 {
    match point {
        FaultPoint::BeginRead => 0,
        FaultPoint::BeginWrite => 1,
        FaultPoint::ReadGet => 2,
        FaultPoint::ReadRange => 3,
        FaultPoint::WritePut => 4,
        FaultPoint::WriteDelete => 5,
        FaultPoint::Commit => 6,
    }
}

fn collect_range(table: &Table, start: &[u8], end: &[u8]) -> Vec<(Bytes, Bytes)> {
    if end.is_empty() {
        return table
            .range(start.to_vec()..)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
    }
    if start >= end {
        return Vec::new();
    }
    table
        .range(start.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn read_inner(
    lock: &RwLock<Arc<DatabaseMap>>,
) -> Result<RwLockReadGuard<'_, Arc<DatabaseMap>>, StorageError> {
    lock.read()
        .map_err(|_| StorageError::Backend("sim storage read lock poisoned".to_owned()))
}

fn write_inner(
    lock: &RwLock<Arc<DatabaseMap>>,
) -> Result<RwLockWriteGuard<'_, Arc<DatabaseMap>>, StorageError> {
    lock.write()
        .map_err(|_| StorageError::Backend("sim storage write lock poisoned".to_owned()))
}

fn lock_writer(lock: &Mutex<()>) -> Result<MutexGuard<'_, ()>, StorageError> {
    lock.lock()
        .map_err(|_| StorageError::Backend("sim storage writer lock poisoned".to_owned()))
}

fn lock_state(lock: &Mutex<SimState>) -> Result<MutexGuard<'_, SimState>, StorageError> {
    lock.lock()
        .map_err(|_| StorageError::Backend("sim storage state lock poisoned".to_owned()))
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
mod tests {
    use crate::storage::{
        Fault, FaultPlan, FaultPoint, ReadTransaction, SimStorage, StorageEngine, WriteTransaction,
    };

    #[test]
    fn same_seed_and_plan_have_same_trace_digest() -> Result<(), Box<dyn std::error::Error>> {
        let plan = FaultPlan::new(7).with_fault(FaultPoint::Commit, 3, Fault::LostAfterCommit);
        let first = run_trace(plan.clone())?;
        let second = run_trace(plan)?;
        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn lost_after_commit_returns_ok_but_drops_write() -> Result<(), Box<dyn std::error::Error>> {
        let storage = SimStorage::new(FaultPlan::new(1).with_fault(
            FaultPoint::Commit,
            3,
            Fault::LostAfterCommit,
        ));
        let mut write = storage.begin_write()?;
        write.put("t", b"k", b"v")?;
        write.commit()?;
        let read = storage.begin_read()?;
        assert_eq!(read.get("t", b"k")?, None);
        Ok(())
    }

    #[test]
    fn corrupt_read_changes_payload_deterministically() -> Result<(), Box<dyn std::error::Error>> {
        let storage = SimStorage::new(FaultPlan::new(1).with_fault(
            FaultPoint::ReadGet,
            4,
            Fault::CorruptRead { xor: 1 },
        ));
        let mut write = storage.begin_write()?;
        write.put("t", b"k", b"a")?;
        write.commit()?;
        assert_eq!(storage.get("t", b"k")?, Some(vec![b'`']));
        Ok(())
    }

    fn run_trace(
        plan: FaultPlan,
    ) -> Result<crate::hardening::TraceDigest, crate::storage::StorageError> {
        let storage = SimStorage::new(plan);
        let mut write = storage.begin_write()?;
        write.put("t", b"k", b"v")?;
        write.commit()?;
        storage.trace_digest()
    }
}
