use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UsageSample {
    pub disk_used_bytes: u64,
    pub disk_limit_bytes: u64,
    pub memory_used_bytes: u64,
    pub memory_limit_bytes: u64,
    pub local_recovery_attempted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResourcePolicy {
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub hard_limit: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceKind {
    Disk,
    Memory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceSpec {
    pub kind: ResourceKind,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceSignal {
    Ok,
    RecoverLocal { reason: String },
    NeedMore { reason: String, want: ResourceSpec },
    LimitReached { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorResourceEvent {
    pub signal: String,
    pub kind: Option<ResourceKind>,
    pub reason: Option<String>,
    pub requested_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct ResourceMonitor {
    policy: ResourcePolicy,
    active_pressure: bool,
}

pub trait ResourceSampler {
    /// Captures a local resource sample.
    /// # Errors
    /// Fails when the sampler cannot inspect the requested local source.
    fn sample(&self) -> Result<UsageSample, ResourceSampleError>;
}

#[derive(Clone, Debug)]
pub struct StaticResourceSampler {
    sample: UsageSample,
}

#[derive(Clone, Debug)]
pub struct LocalResourceSampler {
    path: PathBuf,
    disk_limit_bytes: u64,
    memory_used_bytes: u64,
    memory_limit_bytes: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum ResourceSampleError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Default for ResourcePolicy {
    fn default() -> Self {
        Self {
            high_watermark: 0.85,
            low_watermark: 0.70,
            hard_limit: 0.98,
        }
    }
}

impl UsageSample {
    #[must_use]
    pub const fn new(
        disk_used_bytes: u64,
        disk_limit_bytes: u64,
        memory_used_bytes: u64,
        memory_limit_bytes: u64,
    ) -> Self {
        Self {
            disk_used_bytes,
            disk_limit_bytes,
            memory_used_bytes,
            memory_limit_bytes,
            local_recovery_attempted: false,
        }
    }

    #[must_use]
    pub const fn with_local_recovery_attempted(mut self, attempted: bool) -> Self {
        self.local_recovery_attempted = attempted;
        self
    }
}

impl ResourceSignal {
    #[must_use]
    pub fn operator_event(&self) -> OperatorResourceEvent {
        OperatorResourceEvent::from_signal(self)
    }
}

impl OperatorResourceEvent {
    #[must_use]
    pub fn from_signal(signal: &ResourceSignal) -> Self {
        match signal {
            ResourceSignal::Ok => Self {
                signal: "ok".to_owned(),
                kind: None,
                reason: None,
                requested_bytes: 0,
            },
            ResourceSignal::RecoverLocal { reason } => Self {
                signal: "recover_local".to_owned(),
                kind: None,
                reason: Some(sanitize_manifest_value(reason)),
                requested_bytes: 0,
            },
            ResourceSignal::NeedMore { reason, want } => Self {
                signal: "need_more".to_owned(),
                kind: Some(want.kind),
                reason: Some(sanitize_manifest_value(reason)),
                requested_bytes: want.bytes,
            },
            ResourceSignal::LimitReached { reason } => Self {
                signal: "limit_reached".to_owned(),
                kind: None,
                reason: Some(sanitize_manifest_value(reason)),
                requested_bytes: 0,
            },
        }
    }
}

impl ResourcePolicy {
    #[must_use]
    pub const fn new(high_watermark: f64, low_watermark: f64, hard_limit: f64) -> Self {
        Self {
            high_watermark,
            low_watermark,
            hard_limit,
        }
    }
}

#[must_use]
pub fn render_kubernetes_signal_config_map(
    name: &str,
    namespace: &str,
    signal: &ResourceSignal,
) -> String {
    let event = signal.operator_event();
    let name = sanitize_kubernetes_name(name);
    let namespace = sanitize_kubernetes_name(namespace);
    let kind = event.kind.map_or("none", resource_kind_label);
    let reason = event.reason.as_deref().unwrap_or("");

    format!(
        "apiVersion: v1\n\
         kind: ConfigMap\n\
         metadata:\n\
           name: {name}\n\
           namespace: {namespace}\n\
           labels:\n\
             app.kubernetes.io/name: multidb\n\
             multidb.io/resource-signal: {signal}\n\
         data:\n\
           signal: \"{signal}\"\n\
           kind: \"{kind}\"\n\
           requested_bytes: \"{requested_bytes}\"\n\
           reason: \"{reason}\"\n",
        signal = event.signal,
        requested_bytes = event.requested_bytes
    )
}

impl ResourceMonitor {
    #[must_use]
    pub const fn new(policy: ResourcePolicy) -> Self {
        Self {
            policy,
            active_pressure: false,
        }
    }

    #[must_use]
    pub const fn policy(&self) -> ResourcePolicy {
        self.policy
    }

    #[must_use]
    pub const fn active_pressure(&self) -> bool {
        self.active_pressure
    }

    #[must_use]
    pub fn assess(&mut self, sample: UsageSample) -> ResourceSignal {
        let disk = ratio(sample.disk_used_bytes, sample.disk_limit_bytes);
        let memory = ratio(sample.memory_used_bytes, sample.memory_limit_bytes);
        let (kind, pressure) = if disk >= memory {
            (ResourceKind::Disk, disk)
        } else {
            (ResourceKind::Memory, memory)
        };

        if pressure >= self.policy.hard_limit {
            self.active_pressure = true;
            return ResourceSignal::LimitReached {
                reason: format!("{kind:?} usage reached hard limit"),
            };
        }

        if self.active_pressure && pressure <= self.policy.low_watermark {
            self.active_pressure = false;
            return ResourceSignal::Ok;
        }

        if pressure >= self.policy.high_watermark || self.active_pressure {
            self.active_pressure = true;
            if !sample.local_recovery_attempted {
                return ResourceSignal::RecoverLocal {
                    reason: format!("{kind:?} usage crossed high watermark"),
                };
            }

            return ResourceSignal::NeedMore {
                reason: format!("{kind:?} usage remains high after local recovery"),
                want: ResourceSpec {
                    kind,
                    bytes: grow_step(kind, sample),
                },
            };
        }

        ResourceSignal::Ok
    }

    pub fn assess_and_record(&mut self, sample: UsageSample) -> ResourceSignal {
        let signal = self.assess(sample);
        crate::observability::record_resource_signal(&signal);
        signal
    }
}

impl StaticResourceSampler {
    #[must_use]
    pub const fn new(sample: UsageSample) -> Self {
        Self { sample }
    }
}

impl ResourceSampler for StaticResourceSampler {
    fn sample(&self) -> Result<UsageSample, ResourceSampleError> {
        Ok(self.sample)
    }
}

impl LocalResourceSampler {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>, disk_limit_bytes: u64, memory_limit_bytes: u64) -> Self {
        Self {
            path: path.into(),
            disk_limit_bytes,
            memory_used_bytes: 0,
            memory_limit_bytes,
        }
    }

    #[must_use]
    pub const fn with_memory_used_bytes(mut self, memory_used_bytes: u64) -> Self {
        self.memory_used_bytes = memory_used_bytes;
        self
    }
}

impl ResourceSampler for LocalResourceSampler {
    fn sample(&self) -> Result<UsageSample, ResourceSampleError> {
        Ok(UsageSample::new(
            directory_size(&self.path)?,
            self.disk_limit_bytes,
            self.memory_used_bytes,
            self.memory_limit_bytes,
        ))
    }
}

#[allow(clippy::cast_precision_loss)]
fn ratio(used: u64, limit: u64) -> f64 {
    if limit == 0 {
        return 1.0;
    }

    used as f64 / limit as f64
}

fn grow_step(kind: ResourceKind, sample: UsageSample) -> u64 {
    let limit = match kind {
        ResourceKind::Disk => sample.disk_limit_bytes,
        ResourceKind::Memory => sample.memory_limit_bytes,
    };

    limit.saturating_div(4).max(1)
}

fn directory_size(path: &Path) -> Result<u64, std::io::Error> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        total = total.saturating_add(directory_size(&entry?.path())?);
    }
    Ok(total)
}

fn resource_kind_label(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Disk => "disk",
        ResourceKind::Memory => "memory",
    }
}

fn sanitize_kubernetes_name(value: &str) -> String {
    let mut output = value
        .chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else if ch == '-' || ch == '.' {
                Some(ch)
            } else {
                None
            }
        })
        .take(63)
        .collect::<String>();

    while output.ends_with('-') || output.ends_with('.') {
        output.pop();
    }

    if output.is_empty() {
        "multidb-signal".to_owned()
    } else {
        output
    }
}

fn sanitize_manifest_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '_' | '-' | ':' | '.'))
        .take(160)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        LocalResourceSampler, ResourceKind, ResourceMonitor, ResourcePolicy, ResourceSampler,
        ResourceSignal, ResourceSpec, UsageSample, render_kubernetes_signal_config_map,
    };

    #[test]
    fn high_watermark_requests_local_recovery_first() {
        let mut monitor = ResourceMonitor::new(ResourcePolicy::default());
        let signal = monitor.assess(UsageSample::new(90, 100, 10, 100));

        assert!(matches!(signal, ResourceSignal::RecoverLocal { .. }));
    }

    #[test]
    fn pressure_after_recovery_requests_more_resources() {
        let mut monitor = ResourceMonitor::new(ResourcePolicy::default());
        let signal =
            monitor.assess(UsageSample::new(90, 100, 10, 100).with_local_recovery_attempted(true));

        assert!(matches!(signal, ResourceSignal::NeedMore { .. }));
    }

    #[test]
    fn hard_limit_never_allocates_by_itself() {
        let mut monitor = ResourceMonitor::new(ResourcePolicy::default());
        let signal =
            monitor.assess(UsageSample::new(99, 100, 10, 100).with_local_recovery_attempted(true));

        assert!(matches!(signal, ResourceSignal::LimitReached { .. }));
    }

    #[test]
    fn hysteresis_prevents_threshold_flapping() {
        let mut monitor = ResourceMonitor::new(ResourcePolicy::new(0.85, 0.70, 0.98));

        assert!(matches!(
            monitor.assess(UsageSample::new(86, 100, 0, 100)),
            ResourceSignal::RecoverLocal { .. }
        ));
        assert!(matches!(
            monitor.assess(UsageSample::new(80, 100, 0, 100).with_local_recovery_attempted(true)),
            ResourceSignal::NeedMore { .. }
        ));
        assert!(matches!(
            monitor.assess(UsageSample::new(69, 100, 0, 100)),
            ResourceSignal::Ok
        ));
    }

    #[test]
    fn local_sampler_reports_usage_and_metric_signal() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        std::fs::write(temp_dir.path().join("data.bin"), [1_u8; 90])?;
        let sampler = LocalResourceSampler::new(temp_dir.path(), 100, 100);
        let sample = sampler.sample()?;

        assert_eq!(sample.disk_used_bytes, 90);

        let mut monitor = ResourceMonitor::new(ResourcePolicy::default());
        assert!(matches!(
            monitor.assess_and_record(sample),
            ResourceSignal::RecoverLocal { .. }
        ));

        let metrics = crate::observability::global_registry().render()?;
        assert!(metrics.contains("multidb_resource_signal_total"));
        Ok(())
    }

    #[test]
    fn operator_event_exports_need_more_without_allocating() {
        let signal = ResourceSignal::NeedMore {
            reason: "Disk usage remains high after local recovery; value=hidden".to_owned(),
            want: ResourceSpec {
                kind: ResourceKind::Disk,
                bytes: 256,
            },
        };

        let event = signal.operator_event();
        assert_eq!(event.signal, "need_more");
        assert_eq!(event.kind, Some(ResourceKind::Disk));
        assert_eq!(event.requested_bytes, 256);

        let manifest =
            render_kubernetes_signal_config_map("MultiDB Signals!", "Default_Namespace", &signal);
        assert!(manifest.contains("kind: ConfigMap"));
        assert!(manifest.contains("name: multidbsignals"));
        assert!(manifest.contains("namespace: defaultnamespace"));
        assert!(manifest.contains("multidb.io/resource-signal: need_more"));
        assert!(manifest.contains("requested_bytes: \"256\""));
    }

    #[test]
    fn operator_event_for_hard_limit_never_requests_bytes() {
        let signal = ResourceSignal::LimitReached {
            reason: "Disk usage reached hard limit".to_owned(),
        };

        let event = signal.operator_event();
        assert_eq!(event.signal, "limit_reached");
        assert_eq!(event.requested_bytes, 0);
    }
}
