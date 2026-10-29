//! Cheap process-local fatal-state snapshots for public probes. No probe opens
//! storage, reads keys, initializes a GPU, or waits for the database query lock.
use super::Database;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone)]
pub(crate) struct RuntimeHealth {
    audit_failed: Arc<AtomicBool>,
    requires_gpu: bool,
    gpu_failed: Option<Arc<AtomicBool>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeHealthCondition {
    Ready,
    AuditFailed,
    GpuUnavailable,
    GpuStopped,
}
impl RuntimeHealth {
    pub(crate) fn condition(&self) -> RuntimeHealthCondition {
        // Restarting a sealed audit does not repair durable/key corruption.
        // It takes priority over a simultaneous GPU stop to avoid restart loops.
        if self.audit_failed.load(Ordering::Acquire) {
            return RuntimeHealthCondition::AuditFailed;
        }
        if self.requires_gpu {
            return match &self.gpu_failed {
                Some(flag) if flag.load(Ordering::Acquire) => RuntimeHealthCondition::GpuStopped,
                Some(_) => RuntimeHealthCondition::Ready,
                None => RuntimeHealthCondition::GpuUnavailable,
            };
        }
        RuntimeHealthCondition::Ready
    }
    #[cfg(test)]
    pub(crate) fn with_gpu_for_tests(mut self, failed: Arc<AtomicBool>) -> Self {
        self.requires_gpu = true;
        self.gpu_failed = Some(failed);
        self
    }
}
impl Database {
    pub(crate) fn runtime_health(&self) -> RuntimeHealth {
        let requires_gpu = self.config.profile.requires_gpu();
        RuntimeHealth {
            audit_failed: self.audit_batch.failure_flag(),
            requires_gpu,
            gpu_failed: if requires_gpu {
                crate::tensor_gpu::cached_device_failure_flag()
            } else {
                None
            },
        }
    }
}
