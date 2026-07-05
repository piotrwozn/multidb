use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    phase30::{FlowControlConfig, HlcConfig, InternalTransportConfig, RegionConfig},
    storage::{Bytes, ReadTransaction, StorageEngine, StorageError, WriteTransaction},
    txn,
};

use super::{NodeId, Op, ReadConsistency, ReplError, Replication};

pub const AP_VERSIONS_TABLE: &str = "__ap_versions";
pub const AP_HINTS_TABLE: &str = "__ap_hints";
pub const AP_MERKLE_TABLE: &str = "__ap_merkle";
pub const AP_HINT_SEQ_TABLE: &str = "__ap_hint_seq";

const DEFAULT_HINT_TTL_MS: u64 = 60 * 60 * 1_000;
const MAX_HINT_WRITES: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ApNode {
    pub id: NodeId,
    pub addr: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ConflictStrategy {
    Siblings,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ApClusterConfig {
    pub node_id: NodeId,
    pub bind_addr: String,
    pub nodes: Vec<ApNode>,
    pub replication_factor: usize,
    pub default_read: ConsistencyLevel,
    pub default_write: ConsistencyLevel,
    pub transport: Option<InternalTransportConfig>,
    pub hlc: HlcConfig,
    pub region: Option<RegionConfig>,
    pub flow_control: FlowControlConfig,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VectorClock(BTreeMap<NodeId, u64>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockOrdering {
    Before,
    After,
    Equal,
    Concurrent,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VersionedBytes {
    pub value: Option<Bytes>,
    pub clock: VectorClock,
    pub origin: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VersionedWrite {
    pub table: String,
    pub key: Bytes,
    pub version: VersionedBytes,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct VersionedRecord {
    pub table: String,
    pub key: Bytes,
    pub versions: Vec<VersionedBytes>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct Hint {
    #[serde(default)]
    seq: u64,
    target: NodeId,
    #[serde(default)]
    created_at_ms: u64,
    #[serde(default)]
    expires_at_ms: u64,
    #[serde(default)]
    attempts: u32,
    writes: Vec<VersionedWrite>,
}

impl Hint {
    fn is_expired(&self) -> bool {
        self.expires_at_ms != 0 && ap_now_ms() > self.expires_at_ms
    }
}

pub trait ApTransport: Send + Sync {
    /// Sends one AP batch to a replica.
    /// # Errors
    /// Fails when the target is unavailable or rejects the batch.
    fn send_batch(&self, target: NodeId, writes: &[VersionedWrite]) -> Result<(), ReplError>;

    /// Reads all AP siblings for one key from a replica.
    /// # Errors
    /// Fails when the target is unavailable or rejects the read.
    fn read_versions(
        &self,
        target: NodeId,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<VersionedBytes>, ReplError>;

    /// Reads every AP versioned record from a replica.
    /// # Errors
    /// Fails when the target is unavailable or rejects the scan.
    fn read_all_versions(&self, target: NodeId) -> Result<Vec<VersionedRecord>, ReplError>;

    /// Reads Merkle leaf hashes keyed by AP version key.
    /// # Errors
    /// Fails when the target is unavailable or rejects the scan.
    fn read_merkle(&self, target: NodeId) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        records_to_merkle(&self.read_all_versions(target)?)
    }

    /// Reads Merkle leaves for a bounded prefix/range.
    /// # Errors
    /// Fails when the target is unavailable or rejects the scan.
    fn read_merkle_range(
        &self,
        target: NodeId,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        Ok(self
            .read_merkle(target)?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .collect())
    }

    /// Reads only the records identified by AP version keys.
    /// # Errors
    /// Fails when the target is unavailable or rejects the fetch.
    fn read_records_by_version_keys(
        &self,
        target: NodeId,
        version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError> {
        let wanted = version_keys.iter().collect::<BTreeSet<_>>();
        self.read_all_versions(target).map(|records| {
            records
                .into_iter()
                .filter(|record| {
                    version_key(&record.table, &record.key).is_ok_and(|key| wanted.contains(&key))
                })
                .collect()
        })
    }
}

pub struct ApDynamo<S: StorageEngine> {
    storage: S,
    cluster: ApClusterConfig,
    conflict_strategy: ConflictStrategy,
    commit_lock: Mutex<()>,
    available_nodes: Mutex<BTreeSet<NodeId>>,
    transport: Mutex<Option<Arc<dyn ApTransport>>>,
}

impl ApNode {
    #[must_use]
    pub fn new(id: NodeId, addr: impl Into<String>) -> Self {
        Self {
            id,
            addr: addr.into(),
        }
    }
}

impl ConsistencyLevel {
    #[must_use]
    pub const fn required(self, replication_factor: usize) -> usize {
        match self {
            Self::One => 1,
            Self::Quorum => replication_factor / 2 + 1,
            Self::All => replication_factor,
        }
    }
}

impl ApClusterConfig {
    #[must_use]
    pub fn new(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        nodes: Vec<ApNode>,
        replication_factor: usize,
    ) -> Self {
        Self {
            node_id,
            bind_addr: bind_addr.into(),
            nodes,
            replication_factor,
            default_read: ConsistencyLevel::One,
            default_write: ConsistencyLevel::One,
            transport: None,
            hlc: HlcConfig::default(),
            region: None,
            flow_control: FlowControlConfig::default(),
        }
    }

    #[must_use]
    pub const fn with_default_read(mut self, level: ConsistencyLevel) -> Self {
        self.default_read = level;
        self
    }

    #[must_use]
    pub const fn with_default_write(mut self, level: ConsistencyLevel) -> Self {
        self.default_write = level;
        self
    }

    #[must_use]
    pub fn with_transport(mut self, transport: InternalTransportConfig) -> Self {
        self.transport = Some(transport);
        self
    }

    #[must_use]
    pub const fn with_hlc(mut self, hlc: HlcConfig) -> Self {
        self.hlc = hlc;
        self
    }

    #[must_use]
    pub fn with_region(mut self, region: RegionConfig) -> Self {
        self.region = Some(region);
        self
    }

    #[must_use]
    pub const fn with_flow_control(mut self, flow_control: FlowControlConfig) -> Self {
        self.flow_control = flow_control;
        self
    }

    #[must_use]
    pub fn single_node_for_tests(node_id: NodeId) -> Self {
        let addr = format!("ap-in-process-{node_id}");
        Self::new(node_id, addr.clone(), vec![ApNode::new(node_id, addr)], 1)
    }

    #[must_use]
    pub fn contains_node(&self, node_id: NodeId) -> bool {
        self.nodes.iter().any(|node| node.id == node_id)
    }
}

impl VectorClock {
    #[must_use]
    pub fn new(entries: impl IntoIterator<Item = (NodeId, u64)>) -> Self {
        Self(entries.into_iter().collect())
    }

    #[must_use]
    pub fn entries(&self) -> &BTreeMap<NodeId, u64> {
        &self.0
    }

    pub fn increment(&mut self, node_id: NodeId) {
        *self.0.entry(node_id).or_insert(0) += 1;
    }

    pub fn merge_from(&mut self, other: &Self) {
        for (node_id, value) in &other.0 {
            let entry = self.0.entry(*node_id).or_insert(0);
            *entry = (*entry).max(*value);
        }
    }

    #[must_use]
    pub fn merged<'a>(clocks: impl IntoIterator<Item = &'a Self>) -> Self {
        let mut merged = Self::default();
        for clock in clocks {
            merged.merge_from(clock);
        }
        merged
    }

    #[must_use]
    pub fn compare(left: &Self, right: &Self) -> ClockOrdering {
        let mut left_gt = false;
        let mut right_gt = false;

        for node_id in left.0.keys().chain(right.0.keys()) {
            let left_value = left.0.get(node_id).copied().unwrap_or(0);
            let right_value = right.0.get(node_id).copied().unwrap_or(0);
            if left_value > right_value {
                left_gt = true;
            } else if right_value > left_value {
                right_gt = true;
            }
        }

        match (left_gt, right_gt) {
            (false, false) => ClockOrdering::Equal,
            (true, false) => ClockOrdering::After,
            (false, true) => ClockOrdering::Before,
            (true, true) => ClockOrdering::Concurrent,
        }
    }
}

/// Validates AP cluster topology.
/// # Errors
/// Returns a message when nodes, local membership, or RF are invalid.
pub fn validate_ap_cluster_config(config: &ApClusterConfig) -> Result<(), String> {
    if config.bind_addr.trim().is_empty() {
        return Err("AP cluster bind address cannot be empty".to_owned());
    }

    if config.node_id == 0 {
        return Err("AP local node id cannot be zero".to_owned());
    }

    if config.nodes.is_empty() {
        return Err("AP cluster must contain at least one node".to_owned());
    }

    if config.replication_factor == 0 || config.replication_factor > config.nodes.len() {
        return Err(format!(
            "AP replication factor must be between 1 and node count {}; got {}",
            config.nodes.len(),
            config.replication_factor
        ));
    }

    if config.replication_factor > 1 && config.transport.is_none() {
        return Err(
            "AP multi-node replication requires InternalTransportConfig at startup".to_owned(),
        );
    }

    let mut ids = BTreeSet::new();
    for node in &config.nodes {
        if node.id == 0 {
            return Err("AP node id cannot be zero".to_owned());
        }

        if node.addr.trim().is_empty() {
            return Err(format!("AP node {} address cannot be empty", node.id));
        }

        if !ids.insert(node.id) {
            return Err(format!("duplicate AP node id {}", node.id));
        }
    }

    if !ids.contains(&config.node_id) {
        return Err(format!(
            "AP local node {} must be present in cluster nodes",
            config.node_id
        ));
    }

    Ok(())
}

impl<S: StorageEngine> ApDynamo<S> {
    /// Creates an AP/Dynamo-style replication backend.
    /// # Errors
    /// Fails when cluster topology is invalid.
    pub fn new(storage: S, cluster: ApClusterConfig) -> Result<Self, ReplError> {
        validate_ap_cluster_config(&cluster).map_err(ReplError::Transport)?;
        Ok(Self::new_unchecked(storage, cluster))
    }

    #[must_use]
    pub(crate) fn new_unchecked(storage: S, cluster: ApClusterConfig) -> Self {
        let available_nodes = cluster.nodes.iter().map(|node| node.id).collect();
        Self {
            storage,
            cluster,
            conflict_strategy: ConflictStrategy::Siblings,
            commit_lock: Mutex::new(()),
            available_nodes: Mutex::new(available_nodes),
            transport: Mutex::new(None),
        }
    }

    #[must_use]
    pub fn new_for_test(storage: S, node_id: NodeId) -> Self {
        Self::new_unchecked(storage, ApClusterConfig::single_node_for_tests(node_id))
    }

    #[must_use]
    pub const fn storage(&self) -> &S {
        &self.storage
    }

    #[must_use]
    pub const fn cluster_config(&self) -> &ApClusterConfig {
        &self.cluster
    }

    #[must_use]
    pub const fn conflict_strategy(&self) -> ConflictStrategy {
        self.conflict_strategy
    }

    /// Installs a node-to-node transport.
    /// # Errors
    /// Fails when the transport lock is poisoned.
    pub fn set_transport(&self, transport: Arc<dyn ApTransport>) -> Result<(), ReplError> {
        *self.transport()? = Some(transport);
        Ok(())
    }

    /// Replaces the set of nodes considered reachable by this coordinator.
    /// # Errors
    /// Fails when the set contains an unknown node id or the lock is poisoned.
    pub fn set_available_nodes_for_tests(
        &self,
        available: impl IntoIterator<Item = NodeId>,
    ) -> Result<(), ReplError> {
        let node_ids = self.node_ids();
        let mut next = BTreeSet::new();

        for node_id in available {
            if !node_ids.contains(&node_id) {
                return Err(ReplError::Transport(format!(
                    "unknown AP node id {node_id}"
                )));
            }
            next.insert(node_id);
        }

        *self.available_nodes()? = next;
        Ok(())
    }

    /// Reads all AP siblings visible for a key.
    /// # Errors
    /// Fails when the read quorum cannot be reached or storage rejects the read.
    pub fn read_conflict_versions(
        &self,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<VersionedBytes>, ReplError> {
        let replies = self.read_versions_from_replicas(table, key, self.ap_read_level())?;
        Ok(merge_reply_versions(&replies))
    }

    /// Resolves siblings by writing one version that dominates all parent clocks.
    /// # Errors
    /// Fails when the write level cannot be reached or storage rejects the write.
    pub fn resolve_conflict(
        &self,
        table: &str,
        key: &[u8],
        value: Bytes,
        parents: Vec<VectorClock>,
    ) -> Result<(), ReplError> {
        let mut clock = VectorClock::default();
        for parent in parents {
            clock.merge_from(&parent);
        }
        clock.increment(self.cluster.node_id);
        let write = VersionedWrite {
            table: table.to_owned(),
            key: key.to_vec(),
            version: VersionedBytes {
                value: Some(value),
                clock,
                origin: self.cluster.node_id,
            },
        };
        self.write_versioned_batch(&[write], self.cluster.default_write)
    }

    /// Attempts to deliver all currently stored hinted handoff records.
    /// # Errors
    /// Fails when storage rejects hint reads or deletes.
    pub fn deliver_hints(&self) -> Result<(), ReplError> {
        self.ensure_transport_for_multi_node()?;
        let mut hints = self.local_hints()?;
        hints.sort_by_key(|(_, hint)| (hint.target, hint.seq));
        for (hint_key, hint) in hints {
            if hint.is_expired() {
                self.delete_hint(&hint_key)?;
                continue;
            }

            if !self.is_available(hint.target)? {
                continue;
            }

            if self.send_to_node(hint.target, &hint.writes).is_ok() {
                self.delete_hint(&hint_key)?;
            } else {
                self.bump_hint_attempts(&hint_key, hint)?;
            }
        }

        Ok(())
    }

    /// Runs one anti-entropy round against a peer.
    /// # Errors
    /// Fails when storage or transport rejects the synchronization.
    pub fn anti_entropy_with(&self, peer: NodeId) -> Result<(), ReplError> {
        self.ensure_transport_for_multi_node()?;
        let transport = self.transport_arc()?;
        let local_merkle = self.local_merkle()?;
        let remote_merkle = transport
            .read_merkle_range(
                peer,
                &[],
                self.cluster.flow_control.anti_entropy_batch_records.max(1),
            )?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let mut divergent_keys = divergent_merkle_keys(&local_merkle, &remote_merkle);
        if divergent_keys.len() > self.cluster.flow_control.anti_entropy_batch_records.max(1) {
            divergent_keys = divergent_keys
                .into_iter()
                .take(self.cluster.flow_control.anti_entropy_batch_records.max(1))
                .collect();
        }
        if divergent_keys.is_empty() {
            return Ok(());
        }

        let divergent_keys = divergent_keys.into_iter().collect::<Vec<_>>();
        let local_records = self.local_records_by_version_keys(&divergent_keys)?;
        let remote_records = transport.read_records_by_version_keys(peer, &divergent_keys)?;
        let merged = merge_record_sets(local_records, remote_records);

        self.apply_records(&merged)?;
        let writes = records_to_writes(&merged);
        if !writes.is_empty() {
            transport.send_batch(peer, &writes)?;
        }

        Ok(())
    }

    fn node_ids(&self) -> BTreeSet<NodeId> {
        self.cluster.nodes.iter().map(|node| node.id).collect()
    }

    fn available_nodes(&self) -> Result<MutexGuard<'_, BTreeSet<NodeId>>, ReplError> {
        self.available_nodes
            .lock()
            .map_err(|_| ReplError::Transport("AP availability lock poisoned".to_owned()))
    }

    fn transport(&self) -> Result<MutexGuard<'_, Option<Arc<dyn ApTransport>>>, ReplError> {
        self.transport
            .lock()
            .map_err(|_| ReplError::Transport("AP transport lock poisoned".to_owned()))
    }

    fn transport_arc(&self) -> Result<Arc<dyn ApTransport>, ReplError> {
        self.transport()?
            .clone()
            .ok_or_else(|| ReplError::Transport("AP transport is not configured".to_owned()))
    }

    fn ensure_transport_for_multi_node(&self) -> Result<(), ReplError> {
        if self.cluster.replication_factor <= 1 {
            return Ok(());
        }

        if self.transport()?.is_some() {
            Ok(())
        } else {
            Err(ReplError::Transport(
                "AP multi-node replication requires a configured transport".to_owned(),
            ))
        }
    }

    fn is_available(&self, node_id: NodeId) -> Result<bool, ReplError> {
        Ok(self.available_nodes()?.contains(&node_id))
    }

    fn ap_read_level(&self) -> ConsistencyLevel {
        self.cluster.default_read
    }

    fn read_level_for(&self, consistency: ReadConsistency) -> ConsistencyLevel {
        match consistency {
            ReadConsistency::Eventual | ReadConsistency::Bounded => ConsistencyLevel::One,
            ReadConsistency::Strong => [
                ConsistencyLevel::One,
                ConsistencyLevel::Quorum,
                ConsistencyLevel::All,
            ]
            .into_iter()
            .find(|level| {
                level.required(self.cluster.replication_factor)
                    + self
                        .cluster
                        .default_write
                        .required(self.cluster.replication_factor)
                    > self.cluster.replication_factor
            })
            .unwrap_or(ConsistencyLevel::All),
        }
    }

    fn build_writes(&self, ops: Vec<Op>) -> Result<Vec<VersionedWrite>, StorageError> {
        let mut collapsed = BTreeMap::new();
        for op in ops {
            match op {
                Op::Put { table, key, value } => {
                    collapsed.insert((table, key), Some(value));
                }
                Op::Delete { table, key } => {
                    collapsed.insert((table, key), None);
                }
            }
        }

        collapsed
            .into_iter()
            .map(|((table, key), value)| {
                let existing = self.local_versions(&table, &key)?;
                let mut clock = VectorClock::merged(existing.iter().map(|version| &version.clock));
                clock.increment(self.cluster.node_id);
                Ok(VersionedWrite {
                    table,
                    key,
                    version: VersionedBytes {
                        value,
                        clock,
                        origin: self.cluster.node_id,
                    },
                })
            })
            .collect()
    }

    fn write_versioned_batch(
        &self,
        writes: &[VersionedWrite],
        level: ConsistencyLevel,
    ) -> Result<(), ReplError> {
        self.ensure_transport_for_multi_node()?;
        if writes.is_empty() {
            return Ok(());
        }

        let required = level.required(self.cluster.replication_factor);
        let targets = self.target_nodes_for_writes(writes);
        let mut acks = 0;

        for target in targets {
            if !self.is_available(target)? {
                self.store_hint(target, writes)?;
                continue;
            }

            let result = if target == self.cluster.node_id {
                self.apply_versioned_writes(writes).map_err(Into::into)
            } else {
                self.send_to_node(target, writes)
            };

            match result {
                Ok(()) => acks += 1,
                Err(_) => self.store_hint(target, writes)?,
            }
        }

        if acks >= required {
            Ok(())
        } else {
            Err(ReplError::NoQuorum)
        }
    }

    fn send_to_node(&self, target: NodeId, writes: &[VersionedWrite]) -> Result<(), ReplError> {
        self.transport_arc()?.send_batch(target, writes)
    }

    fn target_nodes_for_writes(&self, writes: &[VersionedWrite]) -> Vec<NodeId> {
        let mut nodes = BTreeSet::new();
        for write in writes {
            nodes.extend(self.preference_list(&write.table, &write.key));
        }
        nodes.into_iter().collect()
    }

    fn preference_list(&self, table: &str, key: &[u8]) -> Vec<NodeId> {
        let mut scored = self
            .cluster
            .nodes
            .iter()
            .map(|node| {
                (
                    self.region_preference_rank(node.id),
                    rendezvous_score(table, key, node.id),
                    node.id,
                )
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| right.1.cmp(&left.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        scored
            .into_iter()
            .take(self.cluster.replication_factor)
            .map(|(_, _, node_id)| node_id)
            .collect()
    }

    fn region_preference_rank(&self, node_id: NodeId) -> u8 {
        let Some(region) = &self.cluster.region else {
            return 0;
        };
        u8::from(region.region_for(node_id) != Some(region.local_region.as_str()))
    }

    fn read_versions_from_replicas(
        &self,
        table: &str,
        key: &[u8],
        level: ConsistencyLevel,
    ) -> Result<Vec<(NodeId, Vec<VersionedBytes>)>, ReplError> {
        self.ensure_transport_for_multi_node()?;
        let required = level.required(self.cluster.replication_factor);
        let targets = self.preference_list(table, key);
        let mut replies = Vec::new();

        for target in targets {
            if !self.is_available(target)? {
                continue;
            }

            let result = if target == self.cluster.node_id {
                self.local_versions(table, key).map_err(Into::into)
            } else {
                self.transport_arc()?.read_versions(target, table, key)
            };

            if let Ok(versions) = result {
                replies.push((target, versions));
            }
        }

        if replies.len() >= required {
            Ok(replies)
        } else {
            Err(ReplError::NoQuorum)
        }
    }

    fn range_records_from_replicas(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(NodeId, Vec<VersionedRecord>)>, ReplError> {
        self.ensure_transport_for_multi_node()?;
        let required = self
            .read_level_for(ReadConsistency::Strong)
            .required(self.cluster.replication_factor);
        let mut replies = Vec::new();

        for target in self.cluster.nodes.iter().map(|node| node.id) {
            if !self.is_available(target)? {
                continue;
            }

            let result = if target == self.cluster.node_id {
                self.local_all_versions().map_err(Into::into)
            } else {
                self.transport_arc()?.read_all_versions(target)
            };

            if let Ok(records) = result {
                replies.push((target, filter_records_for_range(records, table, start, end)));
            }
        }

        if replies.len() >= required {
            Ok(replies)
        } else {
            Err(ReplError::NoQuorum)
        }
    }

    fn read_repair(
        &self,
        table: &str,
        key: &[u8],
        replies: &[(NodeId, Vec<VersionedBytes>)],
        merged: &[VersionedBytes],
    ) {
        if merged.is_empty() {
            return;
        }

        let writes = merged
            .iter()
            .cloned()
            .map(|version| VersionedWrite {
                table: table.to_owned(),
                key: key.to_vec(),
                version,
            })
            .collect::<Vec<_>>();

        for (node_id, versions) in replies {
            if versions != merged {
                let _ = if *node_id == self.cluster.node_id {
                    self.apply_versioned_writes(&writes).map_err(Into::into)
                } else {
                    self.send_to_node(*node_id, &writes)
                };
            }
        }
    }

    fn repair_range_replicas(
        &self,
        replies: &[(NodeId, Vec<VersionedRecord>)],
        merged: &[VersionedRecord],
    ) {
        if merged.is_empty() {
            return;
        }

        let writes = records_to_writes(merged);
        for (node_id, records) in replies {
            let local_view = merge_many_record_sets(std::iter::once(records.clone()));
            if local_view == merged {
                continue;
            }

            let _ = if *node_id == self.cluster.node_id {
                self.apply_records(merged)
            } else {
                self.send_to_node(*node_id, &writes)
            };
        }
    }

    fn local_versions(&self, table: &str, key: &[u8]) -> Result<Vec<VersionedBytes>, StorageError> {
        let txn = self.storage.begin_read()?;
        read_versions_from_txn(&txn, &version_key(table, key)?)
    }

    fn local_hints(&self) -> Result<Vec<(Bytes, Hint)>, StorageError> {
        let txn = self.storage.begin_read()?;
        txn.range(AP_HINTS_TABLE, &[], &[0xFF])?
            .map(|entry| {
                let (key, value) = entry?;
                let mut hint: Hint = serde_json::from_slice(&value)
                    .map_err(|error| StorageError::Corruption(error.to_string()))?;
                if hint.seq == 0 {
                    hint.seq = hint_seq_from_key(&key).unwrap_or(0);
                }
                Ok((key, hint))
            })
            .collect()
    }

    fn local_merkle(&self) -> Result<BTreeMap<Bytes, Bytes>, StorageError> {
        let txn = self.storage.begin_read()?;
        txn.range(AP_MERKLE_TABLE, &[], &[0xFF])?
            .collect::<Result<BTreeMap<_, _>, _>>()
    }

    fn local_all_versions(&self) -> Result<Vec<VersionedRecord>, StorageError> {
        let txn = self.storage.begin_read()?;
        txn.range(AP_VERSIONS_TABLE, &[], &[0xFF])?
            .map(|entry| {
                let (encoded_key, value) = entry?;
                let (table, key) = decode_version_key(&encoded_key)?;
                let versions = decode_versions(&value)?;
                Ok(VersionedRecord {
                    table,
                    key,
                    versions,
                })
            })
            .collect()
    }

    fn local_records_by_version_keys(
        &self,
        version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, StorageError> {
        let txn = self.storage.begin_read()?;
        version_keys
            .iter()
            .filter_map(
                |encoded_key| match txn.get(AP_VERSIONS_TABLE, encoded_key) {
                    Ok(Some(value)) => Some(read_record_from_encoded_key(encoded_key, &value)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect()
    }

    fn apply_records(&self, records: &[VersionedRecord]) -> Result<(), ReplError> {
        self.apply_versioned_writes(&records_to_writes(records))?;
        Ok(())
    }

    fn apply_versioned_writes(&self, writes: &[VersionedWrite]) -> Result<(), StorageError> {
        if writes.is_empty() {
            return Ok(());
        }

        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("AP commit lock poisoned".to_owned()))?;
        let mut txn = self.storage.begin_write()?;

        for write in writes {
            let encoded_key = version_key(&write.table, &write.key)?;
            let existing = read_versions_from_txn(&txn, &encoded_key)?;
            let merged = merge_one_version(existing, write.version.clone());
            write_versions_to_txn(&mut txn, &write.table, &write.key, &encoded_key, &merged)?;
        }

        txn.commit()
    }

    fn store_hint(&self, target: NodeId, writes: &[VersionedWrite]) -> Result<(), ReplError> {
        if writes.is_empty() {
            return Ok(());
        }
        if writes.len() > MAX_HINT_WRITES {
            return Err(ReplError::Transport(format!(
                "AP hint batch exceeds limit of {MAX_HINT_WRITES} writes"
            )));
        }
        self.ensure_hint_backlog_capacity(writes)?;

        let mut txn = self.storage.begin_write()?;
        let seq_key = target.to_be_bytes();
        let seq = txn
            .get(AP_HINT_SEQ_TABLE, &seq_key)?
            .map(|bytes| decode_u64(&bytes))
            .transpose()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| ReplError::Transport("AP hint sequence overflow".to_owned()))?;
        let now = ap_now_ms();
        let hint = Hint {
            seq,
            target,
            created_at_ms: now,
            expires_at_ms: now.saturating_add(DEFAULT_HINT_TTL_MS),
            attempts: 0,
            writes: writes.to_vec(),
        };
        let bytes =
            serde_json::to_vec(&hint).map_err(|error| ReplError::Transport(error.to_string()))?;
        let mut key = target.to_be_bytes().to_vec();
        key.extend_from_slice(&seq.to_be_bytes());

        txn.put(AP_HINT_SEQ_TABLE, &seq_key, &seq.to_be_bytes())?;
        txn.put(AP_HINTS_TABLE, &key, &bytes)?;
        txn.commit()?;
        Ok(())
    }

    fn ensure_hint_backlog_capacity(&self, writes: &[VersionedWrite]) -> Result<(), ReplError> {
        let max_backlog = self.cluster.flow_control.max_hint_backlog_bytes;
        if max_backlog == 0 {
            return Err(ReplError::Transport(
                "AP hint backlog is disabled by flow-control".to_owned(),
            ));
        }

        let current = self.hint_backlog_bytes()?;
        let next = current.saturating_add(estimate_hint_bytes(writes));
        if next > max_backlog {
            return Err(ReplError::Transport(format!(
                "AP hint backlog {next} bytes exceeds limit {max_backlog}"
            )));
        }
        Ok(())
    }

    fn hint_backlog_bytes(&self) -> Result<usize, StorageError> {
        let txn = self.storage.begin_read()?;
        txn.range(AP_HINTS_TABLE, &[], &[0xFF])?
            .map(|entry| entry.map(|(key, value)| key.len().saturating_add(value.len())))
            .try_fold(0_usize, |sum, bytes| {
                bytes.map(|bytes| sum.saturating_add(bytes))
            })
    }

    fn delete_hint(&self, key: &[u8]) -> Result<(), StorageError> {
        let mut txn = self.storage.begin_write()?;
        txn.delete(AP_HINTS_TABLE, key)?;
        txn.commit()
    }

    fn bump_hint_attempts(&self, key: &[u8], mut hint: Hint) -> Result<(), ReplError> {
        hint.attempts = hint.attempts.saturating_add(1);
        let bytes =
            serde_json::to_vec(&hint).map_err(|error| ReplError::Transport(error.to_string()))?;
        let mut txn = self.storage.begin_write()?;
        txn.put(AP_HINTS_TABLE, key, &bytes)?;
        txn.commit()?;
        Ok(())
    }
}

impl<S: StorageEngine> Replication for ApDynamo<S> {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        txn::validate_public_ops(&ops)?;
        let writes = self.build_writes(ops)?;
        self.write_versioned_batch(&writes, self.cluster.default_write)
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        _authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let writes = self.build_writes(ops)?;
        self.write_versioned_batch(&writes, self.cluster.default_write)
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        let replies =
            self.read_versions_from_replicas(table, key, self.read_level_for(consistency))?;
        let merged = merge_reply_versions(&replies);
        self.read_repair(table, key, &replies, &merged);

        match merged.as_slice() {
            [] => Ok(None),
            [version] => Ok(version.value.clone()),
            _ => Err(ReplError::Conflict),
        }
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        if matches!(consistency, ReadConsistency::Strong) {
            let replies = self.range_records_from_replicas(table, start, end)?;
            let merged = merge_many_record_sets(replies.iter().map(|(_, records)| records.clone()));
            self.repair_range_replicas(&replies, &merged);
            return rows_from_records(merged, table, start, end);
        }

        let txn = self.storage.begin_read()?;
        let start_key = version_key(table, start)?;
        let end_key = version_key(table, end)?;
        txn.range(AP_VERSIONS_TABLE, &start_key, &end_key)?
            .map(|entry| {
                let (encoded_key, value) = entry?;
                let (_, key) = decode_version_key(&encoded_key)?;
                let versions = decode_versions(&value)?;
                match versions.as_slice() {
                    [version] => match &version.value {
                        Some(value) => Ok(Some((key, value.clone()))),
                        None => Ok(None),
                    },
                    [] => Ok(None),
                    _ => Err(StorageError::Conflict),
                }
            })
            .filter_map(std::result::Result::transpose)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let rows = self.range(table, start, end, consistency)?;
        let batch_rows = batch_rows.max(1);
        for chunk in rows.chunks(batch_rows) {
            if cancelled() {
                break;
            }
            if !on_batch(chunk)? {
                break;
            }
        }
        Ok(())
    }
}

fn rendezvous_score(table: &str, key: &[u8], node_id: NodeId) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(table.as_bytes());
    hasher.update(&[0]);
    hasher.update(key);
    hasher.update(&node_id.to_be_bytes());
    let hash = hasher.finalize();
    u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap_or([0; 8]))
}

fn version_key(table: &str, key: &[u8]) -> Result<Bytes, StorageError> {
    let table_len =
        u64::try_from(table.len()).map_err(|error| StorageError::Backend(error.to_string()))?;
    let mut out = Vec::with_capacity(8 + table.len() + key.len());
    out.extend_from_slice(&table_len.to_be_bytes());
    out.extend_from_slice(table.as_bytes());
    out.extend_from_slice(key);
    Ok(out)
}

fn decode_version_key(bytes: &[u8]) -> Result<(String, Bytes), StorageError> {
    if bytes.len() < 8 {
        return Err(StorageError::Corruption(
            "AP version key is shorter than table length".to_owned(),
        ));
    }

    let table_len_bytes: [u8; 8] = bytes[..8].try_into().map_err(|_| {
        StorageError::Corruption("AP table length must be exactly 8 bytes".to_owned())
    })?;
    let table_len = usize::try_from(u64::from_be_bytes(table_len_bytes))
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let table_end = 8 + table_len;
    if bytes.len() < table_end {
        return Err(StorageError::Corruption(
            "AP version key is shorter than table name".to_owned(),
        ));
    }

    let table = String::from_utf8(bytes[8..table_end].to_vec())
        .map_err(|error| StorageError::Corruption(error.to_string()))?;
    Ok((table, bytes[table_end..].to_vec()))
}

fn read_versions_from_txn(
    txn: &impl ReadTransaction,
    encoded_key: &[u8],
) -> Result<Vec<VersionedBytes>, StorageError> {
    match txn.get(AP_VERSIONS_TABLE, encoded_key)? {
        Some(bytes) => decode_versions(&bytes),
        None => Ok(Vec::new()),
    }
}

fn read_record_from_encoded_key(
    encoded_key: &[u8],
    value: &[u8],
) -> Result<VersionedRecord, StorageError> {
    let (table, key) = decode_version_key(encoded_key)?;
    let versions = decode_versions(value)?;
    Ok(VersionedRecord {
        table,
        key,
        versions,
    })
}

fn decode_versions(bytes: &[u8]) -> Result<Vec<VersionedBytes>, StorageError> {
    serde_json::from_slice(bytes).map_err(|error| StorageError::Corruption(error.to_string()))
}

fn write_versions_to_txn<T: WriteTransaction>(
    txn: &mut T,
    table: &str,
    key: &[u8],
    encoded_key: &[u8],
    versions: &[VersionedBytes],
) -> Result<(), StorageError> {
    let bytes =
        serde_json::to_vec(versions).map_err(|error| StorageError::Backend(error.to_string()))?;
    txn.put(AP_VERSIONS_TABLE, encoded_key, &bytes)?;
    txn.put(
        AP_MERKLE_TABLE,
        encoded_key,
        merkle_hash(encoded_key, &bytes).as_bytes(),
    )?;

    match versions {
        [version] => match &version.value {
            Some(value) => txn.put(table, key, value)?,
            None => txn.delete(table, key)?,
        },
        _ => txn.delete(table, key)?,
    }

    Ok(())
}

fn merkle_hash(encoded_key: &[u8], version_bytes: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(encoded_key);
    hasher.update(version_bytes);
    hasher.finalize()
}

fn merge_one_version(
    existing: Vec<VersionedBytes>,
    incoming: VersionedBytes,
) -> Vec<VersionedBytes> {
    let mut keep_incoming = true;
    let mut merged = Vec::new();

    for current in existing {
        match VectorClock::compare(&current.clock, &incoming.clock) {
            ClockOrdering::Before => {}
            ClockOrdering::After | ClockOrdering::Equal => {
                keep_incoming = false;
                merged.push(current);
            }
            ClockOrdering::Concurrent => merged.push(current),
        }
    }

    if keep_incoming {
        merged.push(incoming);
    }

    merged.sort_by(|left, right| {
        left.origin
            .cmp(&right.origin)
            .then_with(|| left.clock.entries().cmp(right.clock.entries()))
            .then_with(|| left.value.cmp(&right.value))
    });
    merged
}

fn merge_reply_versions(replies: &[(NodeId, Vec<VersionedBytes>)]) -> Vec<VersionedBytes> {
    replies
        .iter()
        .flat_map(|(_, versions)| versions.iter().cloned())
        .fold(Vec::new(), merge_one_version)
}

fn merge_record_sets(
    left: Vec<VersionedRecord>,
    right: Vec<VersionedRecord>,
) -> Vec<VersionedRecord> {
    let mut records: BTreeMap<(String, Bytes), Vec<VersionedBytes>> = BTreeMap::new();

    for record in left.into_iter().chain(right) {
        let entry = records.entry((record.table, record.key)).or_default();
        for version in record.versions {
            *entry = merge_one_version(std::mem::take(entry), version);
        }
    }

    records
        .into_iter()
        .map(|((table, key), versions)| VersionedRecord {
            table,
            key,
            versions,
        })
        .collect()
}

fn merge_many_record_sets(
    sets: impl IntoIterator<Item = Vec<VersionedRecord>>,
) -> Vec<VersionedRecord> {
    sets.into_iter().fold(Vec::new(), merge_record_sets)
}

fn records_to_writes(records: &[VersionedRecord]) -> Vec<VersionedWrite> {
    records
        .iter()
        .flat_map(|record| {
            record
                .versions
                .iter()
                .cloned()
                .map(|version| VersionedWrite {
                    table: record.table.clone(),
                    key: record.key.clone(),
                    version,
                })
        })
        .collect()
}

fn records_to_merkle(records: &[VersionedRecord]) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    records
        .iter()
        .map(|record| {
            let encoded_key = version_key(&record.table, &record.key)?;
            let bytes = serde_json::to_vec(&record.versions)
                .map_err(|error| ReplError::Transport(error.to_string()))?;
            Ok((
                encoded_key.clone(),
                merkle_hash(&encoded_key, &bytes).as_bytes().to_vec(),
            ))
        })
        .collect()
}

fn estimate_hint_bytes(writes: &[VersionedWrite]) -> usize {
    writes.iter().fold(0_usize, |sum, write| {
        let value_len = write
            .version
            .value
            .as_ref()
            .map_or(0_usize, std::vec::Vec::len);
        sum.saturating_add(write.table.len())
            .saturating_add(write.key.len())
            .saturating_add(value_len)
            .saturating_add(64)
    })
}

fn divergent_merkle_keys(
    left: &BTreeMap<Bytes, Bytes>,
    right: &BTreeMap<Bytes, Bytes>,
) -> BTreeSet<Bytes> {
    left.keys()
        .chain(right.keys())
        .filter(|key| left.get(*key) != right.get(*key))
        .cloned()
        .collect()
}

fn filter_records_for_range(
    records: Vec<VersionedRecord>,
    table: &str,
    start: &[u8],
    end: &[u8],
) -> Vec<VersionedRecord> {
    records
        .into_iter()
        .filter(|record| {
            record.table == table
                && record.key.as_slice() >= start
                && (end.is_empty() || record.key.as_slice() < end)
        })
        .collect()
}

fn rows_from_records(
    records: Vec<VersionedRecord>,
    table: &str,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    let mut rows = BTreeMap::new();
    for record in filter_records_for_range(records, table, start, end) {
        match record.versions.as_slice() {
            [version] => {
                if let Some(value) = &version.value {
                    rows.insert(record.key, value.clone());
                }
            }
            [] => {}
            _ => return Err(ReplError::Conflict),
        }
    }
    Ok(rows.into_iter().collect())
}

fn ap_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::Corruption("AP u64 value must be 8 bytes".to_owned()))?;
    Ok(u64::from_be_bytes(array))
}

fn hint_seq_from_key(key: &[u8]) -> Option<u64> {
    let start = std::mem::size_of::<NodeId>();
    let end = start.checked_add(8)?;
    let bytes: [u8; 8] = key.get(start..end)?.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
pub struct InProcessApTransport {
    nodes: Mutex<BTreeMap<NodeId, Arc<dyn ApReplicaEndpoint>>>,
}

pub trait ApReplicaEndpoint: Send + Sync {
    /// Receives one AP batch.
    /// # Errors
    /// Fails when the replica rejects the batch.
    fn receive_batch(&self, writes: &[VersionedWrite]) -> Result<(), ReplError>;

    /// Reads all versions for one key.
    /// # Errors
    /// Fails when the replica rejects the read.
    fn read_versions(&self, table: &str, key: &[u8]) -> Result<Vec<VersionedBytes>, ReplError>;

    /// Reads all versioned AP records.
    /// # Errors
    /// Fails when the replica rejects the scan.
    fn read_all_versions(&self) -> Result<Vec<VersionedRecord>, ReplError>;

    /// Reads AP records identified by encoded AP version keys.
    /// # Errors
    /// Fails when the replica rejects the fetch.
    fn read_records_by_version_keys(
        &self,
        version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError>;

    /// Reads a bounded Merkle leaf range.
    /// # Errors
    /// Fails when the replica rejects the scan.
    fn read_merkle_range(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError>;
}

#[cfg(test)]
impl InProcessApTransport {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn register(&self, node_id: NodeId, node: Arc<dyn ApReplicaEndpoint>) {
        if let Ok(mut nodes) = self.nodes.lock() {
            nodes.insert(node_id, node);
        }
    }
}

#[cfg(test)]
impl Default for InProcessApTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl ApTransport for InProcessApTransport {
    fn send_batch(&self, target: NodeId, writes: &[VersionedWrite]) -> Result<(), ReplError> {
        self.nodes
            .lock()
            .map_err(|_| ReplError::Transport("test AP transport lock poisoned".to_owned()))?
            .get(&target)
            .ok_or_else(|| ReplError::Transport(format!("missing AP node {target}")))?
            .receive_batch(writes)
    }

    fn read_versions(
        &self,
        target: NodeId,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<VersionedBytes>, ReplError> {
        self.nodes
            .lock()
            .map_err(|_| ReplError::Transport("test AP transport lock poisoned".to_owned()))?
            .get(&target)
            .ok_or_else(|| ReplError::Transport(format!("missing AP node {target}")))?
            .read_versions(table, key)
    }

    fn read_all_versions(&self, target: NodeId) -> Result<Vec<VersionedRecord>, ReplError> {
        self.nodes
            .lock()
            .map_err(|_| ReplError::Transport("test AP transport lock poisoned".to_owned()))?
            .get(&target)
            .ok_or_else(|| ReplError::Transport(format!("missing AP node {target}")))?
            .read_all_versions()
    }

    fn read_merkle_range(
        &self,
        target: NodeId,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.nodes
            .lock()
            .map_err(|_| ReplError::Transport("test AP transport lock poisoned".to_owned()))?
            .get(&target)
            .ok_or_else(|| ReplError::Transport(format!("missing AP node {target}")))?
            .read_merkle_range(prefix, limit)
    }

    fn read_records_by_version_keys(
        &self,
        target: NodeId,
        version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError> {
        self.nodes
            .lock()
            .map_err(|_| ReplError::Transport("test AP transport lock poisoned".to_owned()))?
            .get(&target)
            .ok_or_else(|| ReplError::Transport(format!("missing AP node {target}")))?
            .read_records_by_version_keys(version_keys)
    }
}

impl<S: StorageEngine> ApReplicaEndpoint for ApDynamo<S> {
    fn receive_batch(&self, writes: &[VersionedWrite]) -> Result<(), ReplError> {
        self.apply_versioned_writes(writes)?;
        Ok(())
    }

    fn read_versions(&self, table: &str, key: &[u8]) -> Result<Vec<VersionedBytes>, ReplError> {
        Ok(self.local_versions(table, key)?)
    }

    fn read_all_versions(&self) -> Result<Vec<VersionedRecord>, ReplError> {
        Ok(self.local_all_versions()?)
    }

    fn read_records_by_version_keys(
        &self,
        version_keys: &[Bytes],
    ) -> Result<Vec<VersionedRecord>, ReplError> {
        Ok(self.local_records_by_version_keys(version_keys)?)
    }

    fn read_merkle_range(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        Ok(self
            .local_merkle()?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::prelude::*;

    use super::{
        ApClusterConfig, ApDynamo, ApNode, ClockOrdering, ConsistencyLevel, InProcessApTransport,
        VectorClock, VersionedBytes, validate_ap_cluster_config,
    };
    use crate::{
        phase30::{InternalTransportConfig, InternalTransportSecurity},
        repl::{NodeId, Op, ReadConsistency, ReplError, Replication},
        storage::MemEngine,
    };

    type TestNode = Arc<ApDynamo<MemEngine>>;

    fn cluster_config(node_id: NodeId, write: ConsistencyLevel) -> ApClusterConfig {
        ApClusterConfig::new(
            node_id,
            format!("127.0.0.1:72{node_id:02}"),
            vec![
                ApNode::new(1, "127.0.0.1:7201"),
                ApNode::new(2, "127.0.0.1:7202"),
                ApNode::new(3, "127.0.0.1:7203"),
            ],
            3,
        )
        .with_default_write(write)
        .with_transport(InternalTransportConfig::new(
            format!("127.0.0.1:73{node_id:02}"),
            InternalTransportSecurity::PlaintextForTests,
        ))
    }

    fn test_cluster(write: ConsistencyLevel) -> Result<(TestNode, TestNode, TestNode), ReplError> {
        let one = Arc::new(ApDynamo::new_unchecked(
            MemEngine::new(),
            cluster_config(1, write),
        ));
        let two = Arc::new(ApDynamo::new_unchecked(
            MemEngine::new(),
            cluster_config(2, write),
        ));
        let three = Arc::new(ApDynamo::new_unchecked(
            MemEngine::new(),
            cluster_config(3, write),
        ));
        let transport = Arc::new(InProcessApTransport::new());
        transport.register(1, one.clone());
        transport.register(2, two.clone());
        transport.register(3, three.clone());
        one.set_transport(transport.clone())?;
        two.set_transport(transport.clone())?;
        three.set_transport(transport)?;
        Ok((one, two, three))
    }

    #[test]
    fn consistency_required_counts() {
        assert_eq!(ConsistencyLevel::One.required(3), 1);
        assert_eq!(ConsistencyLevel::Quorum.required(3), 2);
        assert_eq!(ConsistencyLevel::All.required(3), 3);
        assert_eq!(ConsistencyLevel::Quorum.required(5), 3);
    }

    #[test]
    fn vector_clock_compare_detects_causality_and_concurrency() {
        let a = VectorClock::new([(1, 1)]);
        let b = VectorClock::new([(1, 2)]);
        let c = VectorClock::new([(2, 1)]);

        assert_eq!(VectorClock::compare(&a, &a), ClockOrdering::Equal);
        assert_eq!(VectorClock::compare(&a, &b), ClockOrdering::Before);
        assert_eq!(VectorClock::compare(&b, &a), ClockOrdering::After);
        assert_eq!(VectorClock::compare(&a, &c), ClockOrdering::Concurrent);
    }

    proptest! {
        #[test]
        fn vector_clock_equal_to_itself(a in 0_u64..10, b in 0_u64..10) {
            let clock = VectorClock::new([(1, a), (2, b)]);
            prop_assert_eq!(VectorClock::compare(&clock, &clock), ClockOrdering::Equal);
        }
    }

    #[test]
    fn validates_ap_cluster_config() {
        assert!(validate_ap_cluster_config(&cluster_config(1, ConsistencyLevel::One)).is_ok());
        let invalid = ApClusterConfig::new(
            1,
            "127.0.0.1:7201",
            vec![ApNode::new(1, "127.0.0.1:7201")],
            2,
        );
        assert!(validate_ap_cluster_config(&invalid).is_err());
    }

    #[test]
    fn versioned_bytes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let version = VersionedBytes {
            value: Some(b"value".to_vec()),
            clock: VectorClock::new([(1, 2), (2, 1)]),
            origin: 1,
        };

        let bytes = serde_json::to_vec(&version)?;
        assert_eq!(serde_json::from_slice::<VersionedBytes>(&bytes)?, version);
        Ok(())
    }

    #[test]
    fn one_write_succeeds_with_one_visible_node() -> Result<(), Box<dyn std::error::Error>> {
        let (one, _, _) = test_cluster(ConsistencyLevel::One)?;
        one.set_available_nodes_for_tests([1])?;

        one.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;

        assert_eq!(
            one.read("t", b"k", ReadConsistency::Eventual)?,
            Some(b"v".to_vec())
        );
        Ok(())
    }

    #[test]
    fn quorum_and_all_require_enough_acks() -> Result<(), Box<dyn std::error::Error>> {
        let (one, _, _) = test_cluster(ConsistencyLevel::Quorum)?;
        one.set_available_nodes_for_tests([1])?;
        assert!(matches!(
            one.propose(Op::Put {
                table: "t".to_owned(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }),
            Err(ReplError::NoQuorum)
        ));

        one.set_available_nodes_for_tests([1, 2])?;
        one.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;

        let (all, _, _) = test_cluster(ConsistencyLevel::All)?;
        all.set_available_nodes_for_tests([1, 2])?;
        assert!(matches!(
            all.propose(Op::Put {
                table: "t".to_owned(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }),
            Err(ReplError::NoQuorum)
        ));
        Ok(())
    }

    #[test]
    fn concurrent_writes_create_siblings_and_resolve() -> Result<(), Box<dyn std::error::Error>> {
        let (one, two, three) = test_cluster(ConsistencyLevel::One)?;
        one.set_available_nodes_for_tests([1])?;
        two.set_available_nodes_for_tests([2])?;

        one.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"a".to_vec(),
        })?;
        two.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"b".to_vec(),
        })?;

        one.set_available_nodes_for_tests([1, 2, 3])?;
        two.set_available_nodes_for_tests([1, 2, 3])?;
        three.set_available_nodes_for_tests([1, 2, 3])?;
        one.anti_entropy_with(2)?;

        assert!(matches!(
            one.read("t", b"k", ReadConsistency::Eventual),
            Err(ReplError::Conflict)
        ));
        let siblings = one.read_conflict_versions("t", b"k")?;
        assert_eq!(siblings.len(), 2);
        let parents = siblings
            .iter()
            .map(|version| version.clock.clone())
            .collect::<Vec<_>>();

        one.resolve_conflict("t", b"k", b"resolved".to_vec(), parents)?;
        assert_eq!(
            one.read("t", b"k", ReadConsistency::Eventual)?,
            Some(b"resolved".to_vec())
        );
        Ok(())
    }

    #[test]
    fn read_repair_and_hinted_handoff_converge() -> Result<(), Box<dyn std::error::Error>> {
        let (one, two, _) = test_cluster(ConsistencyLevel::Quorum)?;
        one.set_available_nodes_for_tests([1, 2])?;

        one.propose(Op::Put {
            table: "t".to_owned(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        })?;

        assert_eq!(
            two.read("t", b"k", ReadConsistency::Eventual)?,
            Some(b"v".to_vec())
        );

        one.set_available_nodes_for_tests([1, 2, 3])?;
        one.deliver_hints()?;
        one.anti_entropy_with(2)?;

        assert_eq!(
            one.read("t", b"k", ReadConsistency::Strong)?,
            Some(b"v".to_vec())
        );
        Ok(())
    }
}
