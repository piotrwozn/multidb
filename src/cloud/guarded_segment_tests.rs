use super::*;
use crate::storage::{ReadBudget, ReadReservation};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

type TestResult = Result<(), Box<dyn std::error::Error>>;
struct Budget {
    used: Arc<AtomicUsize>,
    peak: AtomicUsize,
    limit: usize,
    cancel: AtomicBool,
}
struct Lease {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl Budget {
    fn new(limit: usize) -> Self {
        Self {
            used: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            limit,
            cancel: AtomicBool::new(false),
        }
    }
}
impl ReadBudget for Budget {
    fn check(&self) -> Result<(), StorageError> {
        if self.cancel.load(Ordering::Acquire) {
            Err(StorageError::Cancelled("test cancellation".to_owned()))
        } else {
            Ok(())
        }
    }
    #[allow(deprecated)] // AtomicUsize::try_update is newer than Rust 1.89.
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
        self.check()?;
        let before = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|n| *n <= self.limit)
            })
            .map_err(|_| StorageError::ResourceLimit("test shared segment budget".to_owned()))?;
        self.peak.fetch_max(before + bytes, Ordering::AcqRel);
        Ok(Box::new(Lease {
            used: Arc::clone(&self.used),
            bytes,
        }))
    }
    fn remaining_bytes(&self) -> usize {
        self.limit.saturating_sub(self.used.load(Ordering::Acquire))
    }
}
fn pointer(directory: &Path, bytes: &[u8]) -> TieredSegmentPointer {
    TieredSegmentPointer {
        uri: ObjectStoreUri::from_local_dir(directory),
        path: "segment.parquet".to_owned(),
        bytes: bytes.len() as u64,
        checksum: crc32fast::hash(bytes),
    }
}
#[test]
fn remote_payload_reserves_before_read_and_keeps_lease_until_drop() -> TestResult {
    let temp = tempfile::tempdir()?;
    let value = vec![23; 128 * 1024];
    fs::write(temp.path().join("segment.parquet"), &value)?;
    let encoded = encode_tiered_segment_pointer(&pointer(temp.path(), &value))?;
    let budget = Budget::new(256 * 1024);
    let payload = resolve_tiered_segment_payload_guarded(&encoded, &budget)?;
    assert_eq!(payload.as_ref(), value);
    assert!(budget.used.load(Ordering::Acquire) >= value.len());
    drop(payload);
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    Ok(())
}
#[test]
fn remote_payload_larger_than_remaining_budget_fails_before_payload_allocation() -> TestResult {
    let temp = tempfile::tempdir()?;
    let value = vec![17; 128 * 1024];
    fs::write(temp.path().join("segment.parquet"), &value)?;
    let encoded = encode_tiered_segment_pointer(&pointer(temp.path(), &value))?;
    let budget = Budget::new(8192);
    assert!(matches!(
        resolve_tiered_segment_payload_guarded(&encoded, &budget),
        Err(StorageError::ResourceLimit(_))
    ));
    assert!(budget.peak.load(Ordering::Acquire) <= 8192);
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    Ok(())
}
#[test]
fn forged_size_and_checksum_fail_without_returning_any_payload() -> TestResult {
    let temp = tempfile::tempdir()?;
    let value = b"real bytes";
    fs::write(temp.path().join("segment.parquet"), value)?;
    for size_mismatch in [true, false] {
        let mut descriptor = pointer(temp.path(), value);
        if size_mismatch {
            descriptor.bytes = u64::MAX;
        } else {
            descriptor.checksum ^= 1;
        }
        let budget = Budget::new(8192);
        assert!(matches!(
            resolve_tiered_segment_payload_guarded(
                &encode_tiered_segment_pointer(&descriptor)?,
                &budget
            ),
            Err(StorageError::Corruption(_))
        ));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
    Ok(())
}
#[test]
fn local_payload_is_admitted_before_copy_and_cancellation_precedes_io() -> TestResult {
    let value = vec![5; 4096];
    let budget = Budget::new(8192);
    let payload = resolve_tiered_segment_payload_guarded(&value, &budget)?;
    assert_eq!(payload.as_ref(), value);
    assert!(budget.remaining_bytes() < 4096);
    drop(payload);
    assert_eq!(budget.remaining_bytes(), 8192);
    budget.cancel.store(true, Ordering::Release);
    assert!(matches!(
        resolve_tiered_segment_payload_guarded(&value, &budget),
        Err(StorageError::Cancelled(_))
    ));
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    Ok(())
}
