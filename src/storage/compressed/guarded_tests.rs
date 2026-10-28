use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use super::*;
use crate::storage::{EncryptedEngine, MemEngine, ReadReservation, StaticKeyProvider};

struct Budget {
    cap: usize,
    used: Arc<AtomicUsize>,
    cancelled: AtomicBool,
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

impl Budget {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            used: Arc::new(AtomicUsize::new(0)),
            cancelled: AtomicBool::new(false),
        }
    }
}

impl ReadBudget for Budget {
    fn check(&self) -> Result<(), StorageError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StorageError::Cancelled("test cancelled".to_owned()));
        }
        Ok(())
    }

    fn reserve(&self, bytes: usize) -> Result<Box<dyn ReadReservation>, StorageError> {
        self.check()?;
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|next| *next <= self.cap)
                .ok_or_else(|| {
                    StorageError::ResourceLimit("test read budget exceeded".to_owned())
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

    fn remaining_bytes(&self) -> usize {
        self.cap.saturating_sub(self.used.load(Ordering::Acquire))
    }
}

fn exercise_guarded_reads(engine: &impl StorageEngine) -> Result<(), Box<dyn Error>> {
    assert!(engine.supports_guarded_reads());
    let mut write = engine.begin_write()?;
    write.put("t", b"a", b"small")?;
    write.put("t", b"b", &vec![b'x'; 128 * 1024])?;
    write.commit()?;
    let read = engine.begin_read()?;
    assert!(read.supports_guarded_reads());
    let budget = Budget::new(4096);
    let mut count = 0;
    read.visit_range("t", &[], &[], &budget, &mut |key, value| {
        assert_eq!(key, b"a");
        assert_eq!(value, b"small");
        assert!(budget.used.load(Ordering::Acquire) >= value.len());
        count += 1;
        Ok(false)
    })?;
    assert_eq!(count, 1);
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    let mut called = false;
    assert!(matches!(
        read.visit_get("t", b"b", &budget, &mut |_| {
            called = true;
            Ok(true)
        }),
        Err(StorageError::ResourceLimit(_))
    ));
    assert!(!called);
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    read.visit_many(
        "t",
        &[b"missing".to_vec(), b"a".to_vec(), b"b".to_vec()],
        &budget,
        &mut |index, value| {
            if index == 0 {
                assert_eq!(value, None);
                return Ok(true);
            }
            assert_eq!(index, 1);
            assert_eq!(value, Some(b"small".as_slice()));
            Ok(false)
        },
    )?;
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    assert!(matches!(
        read.visit_range("t", &[], &[], &budget, &mut |_, _| {
            budget.cancelled.store(true, Ordering::Release);
            Ok(true)
        }),
        Err(StorageError::Cancelled(_))
    ));
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn guarded_wrappers_reserve_before_decoding_stop_early_and_release_on_cancel()
-> Result<(), Box<dyn Error>> {
    exercise_guarded_reads(&EncryptedEngine::new(
        MemEngine::new(),
        StaticKeyProvider::new([7; 32]),
    ))?;
    for algorithm in [CompressionAlgorithm::Lz4, CompressionAlgorithm::Zstd] {
        let config = CompressionConfig {
            algorithm,
            min_bytes: 1,
            zstd_level: 1,
        };
        exercise_guarded_reads(&CompressedEngine::new(MemEngine::new(), config.clone()))?;
        exercise_guarded_reads(&CompressedEngine::new(
            EncryptedEngine::new(MemEngine::new(), StaticKeyProvider::new([7; 32])),
            config,
        ))?;
    }
    Ok(())
}

#[test]
fn guarded_raw_frame_budgets_actual_payload_before_copying_even_when_header_lies()
-> Result<(), Box<dyn Error>> {
    let engine = CompressedEngine::new(MemEngine::new(), CompressionConfig::disabled());
    let mut malformed = Vec::with_capacity(HEADER_LEN + 128 * 1024);
    malformed.extend_from_slice(MAGIC);
    malformed.push(ALGO_RAW);
    malformed.extend_from_slice(&1_u64.to_be_bytes());
    malformed.extend_from_slice(&0_u64.to_be_bytes());
    malformed.resize(HEADER_LEN + 128 * 1024, b'x');
    let mut raw = engine.inner().begin_write()?;
    raw.put("t", b"k", &malformed)?;
    raw.commit()?;
    let read = engine.begin_read()?;
    let budget = Budget::new(4096);
    assert!(matches!(
        read.visit_get("t", b"k", &budget, &mut |_| Ok(true)),
        Err(StorageError::ResourceLimit(_))
    ));
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn guarded_ciphertext_budget_denial_precedes_authentication_of_large_invalid_value()
-> Result<(), Box<dyn Error>> {
    let engine = EncryptedEngine::new(MemEngine::new(), StaticKeyProvider::new([7; 32]));
    let mut write = engine.begin_write()?;
    write.put("t", b"k", b"small")?;
    write.commit()?;
    let mut raw = engine.inner().begin_write()?;
    raw.put("t", b"k", &vec![0; 128 * 1024])?;
    raw.commit()?;
    let budget = Budget::new(4096);
    assert!(matches!(
        engine
            .begin_read()?
            .visit_get("t", b"k", &budget, &mut |_| Ok(true)),
        Err(StorageError::ResourceLimit(_))
    ));
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
    Ok(())
}
