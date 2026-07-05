use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::repl::NodeId;

pub const HLC_TABLE: &str = "__hlc_clock";
pub const HLC_LOCAL_KEY: &[u8] = b"local";

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HlcConfig {
    pub max_clock_drift_ms: u64,
    pub persist_every_ticks: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct InternalTransportConfig {
    pub bind_addr: String,
    pub security: InternalTransportSecurity,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub handshake_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub flow_control: FlowControlConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InternalTransportSecurity {
    Mtls(InternalTlsConfig),
    #[cfg(any(test, feature = "insecure-transport"))]
    PlaintextForTests,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct InternalTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_cert_path: PathBuf,
    pub server_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RaftRuntimeConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub snapshot_threshold: u64,
    pub max_payload_entries: u64,
    pub install_snapshot_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DistTxnConfig {
    pub prepare_timeout_ms: u64,
    pub finish_retry_backoff_ms: u64,
    pub max_finish_retries: u32,
    pub recovery_batch_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CdcWorkerConfig {
    pub page_size: usize,
    pub channel_capacity: usize,
    pub poll_interval_ms: u64,
    pub deliver_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RegionConfig {
    pub local_region: String,
    pub nodes: Vec<RegionNodePlacement>,
    pub bounded_staleness_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RegionNodePlacement {
    pub node_id: NodeId,
    pub region: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FlowControlConfig {
    pub max_inflight_frames_per_peer: usize,
    pub max_inflight_bytes_per_peer: usize,
    pub max_hint_backlog_bytes: usize,
    pub anti_entropy_batch_records: usize,
    pub retry_backoff_ms: u64,
}

impl Default for HlcConfig {
    fn default() -> Self {
        Self {
            max_clock_drift_ms: 1_000,
            persist_every_ticks: 1,
        }
    }
}

impl Default for RaftRuntimeConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_threshold: 10_000,
            max_payload_entries: 256,
            install_snapshot_timeout_ms: 30_000,
        }
    }
}

impl Default for DistTxnConfig {
    fn default() -> Self {
        Self {
            prepare_timeout_ms: 30_000,
            finish_retry_backoff_ms: 250,
            max_finish_retries: 16,
            recovery_batch_size: 128,
        }
    }
}

impl Default for CdcWorkerConfig {
    fn default() -> Self {
        Self {
            page_size: 1_024,
            channel_capacity: 1_024,
            poll_interval_ms: 100,
            deliver_timeout_ms: 30_000,
        }
    }
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            max_inflight_frames_per_peer: 256,
            max_inflight_bytes_per_peer: 16 * 1024 * 1024,
            max_hint_backlog_bytes: 64 * 1024 * 1024,
            anti_entropy_batch_records: 1_024,
            retry_backoff_ms: 100,
        }
    }
}

impl HlcTimestamp {
    #[must_use]
    pub fn now() -> Self {
        Self {
            physical_ms: unix_ms(),
            logical: 0,
        }
    }

    #[must_use]
    pub fn tick(self) -> Self {
        let now = unix_ms();
        if now > self.physical_ms {
            Self {
                physical_ms: now,
                logical: 0,
            }
        } else {
            Self {
                physical_ms: self.physical_ms,
                logical: self.logical.saturating_add(1),
            }
        }
    }

    #[must_use]
    pub fn observe(self, remote: Self) -> Self {
        let now = unix_ms();
        let physical_ms = now.max(self.physical_ms).max(remote.physical_ms);
        let logical = if physical_ms == self.physical_ms && physical_ms == remote.physical_ms {
            self.logical.max(remote.logical).saturating_add(1)
        } else if physical_ms == self.physical_ms {
            self.logical.saturating_add(1)
        } else if physical_ms == remote.physical_ms {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        Self {
            physical_ms,
            logical,
        }
    }
}

impl InternalTransportConfig {
    #[must_use]
    pub fn new(bind_addr: impl Into<String>, security: InternalTransportSecurity) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            security,
            connect_timeout_ms: 5_000,
            request_timeout_ms: 10_000,
            handshake_timeout_ms: 5_000,
            idle_timeout_ms: 60_000,
            max_frame_bytes: 16 * 1024 * 1024,
            max_connections: 512,
            flow_control: FlowControlConfig::default(),
        }
    }

    #[must_use]
    pub const fn with_flow_control(mut self, flow_control: FlowControlConfig) -> Self {
        self.flow_control = flow_control;
        self
    }

    #[must_use]
    pub const fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }
}

impl RegionConfig {
    #[must_use]
    pub fn new(local_region: impl Into<String>, nodes: Vec<RegionNodePlacement>) -> Self {
        Self {
            local_region: local_region.into(),
            nodes,
            bounded_staleness_ms: 5_000,
        }
    }

    #[must_use]
    pub fn region_for(&self, node_id: NodeId) -> Option<&str> {
        self.nodes
            .iter()
            .find(|placement| placement.node_id == node_id)
            .map(|placement| placement.region.as_str())
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{HlcTimestamp, RegionConfig, RegionNodePlacement};

    #[test]
    fn hlc_tick_is_monotonic_when_wall_clock_does_not_advance() {
        let base = HlcTimestamp {
            physical_ms: u64::MAX,
            logical: 0,
        };
        assert!(base.tick() > base);
    }

    #[test]
    fn region_lookup_returns_node_region() {
        let cfg = RegionConfig::new(
            "eu",
            vec![RegionNodePlacement {
                node_id: 7,
                region: "us".to_owned(),
            }],
        );
        assert_eq!(cfg.region_for(7), Some("us"));
        assert_eq!(cfg.region_for(8), None);
    }
}
