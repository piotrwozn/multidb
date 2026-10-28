use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use crate::{observability, storage::Bytes, txn};

use super::{
    ConditionalBatch, CoordinatorDecisionRecord, DEFAULT_DIST_TXN_DEADLINE_MS,
    DIST_TXN_COORDINATOR_TABLE, Decision, DistTxnId, Op, ReadConsistency, ReplError, Replication,
    Vote, WriteCondition, dist_txn, propose_system, propose_system_conditional_batch,
};

pub type ShardId = u32;
pub type PartitionId = u32;

pub const DEFAULT_PARTITIONS: u32 = 4096;
pub const SHARD_MIGRATIONS_TABLE: &str = "__shard_migrations";

#[cfg(test)]
const REL_ROWS_TABLE: &str = "rel_rows";
const REL_COLUMNAR_SEGMENTS_TABLE: &str = "rel_columnar_segments";
const REL_INDEX_TABLE: &str = "rel_indexes";
const DOCUMENT_INDEX_TABLE: &str = "document_indexes";
const DOCUMENT_KEY_LEN: usize = 20;
const DOCUMENT_ID_LEN: usize = 16;
const DIST_TXN_SEQUENCE_TABLE: &str = "__dist_txn_sequence";
const DIST_TXN_SEQUENCE_KEY: &[u8] = b"last_allocated_id";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum PartitionStrategy {
    HashSlots { partitions: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ShardMap {
    pub version: u64,
    pub strategy: PartitionStrategy,
    pub placement: BTreeMap<PartitionId, ShardId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum MigrationState {
    Copying,
    CatchingUp,
    Switching,
    Done,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ShardMigration {
    pub partition: PartitionId,
    pub from: ShardId,
    pub to: ShardId,
    pub state: MigrationState,
}

pub struct ShardedReplication {
    shard_map: RwLock<ShardMap>,
    shards: BTreeMap<ShardId, Arc<dyn Replication>>,
    global_shard: ShardId,
    migrations: RwLock<BTreeMap<PartitionId, ShardMigration>>,
    // An operation retains a read guard from route selection through backend
    // completion. Placement changes take a write guard and drain every old route.
    routing_barrier: RwLock<()>,
}

enum Route {
    Global,
    Partition(PartitionId),
}

impl Default for PartitionStrategy {
    fn default() -> Self {
        Self::HashSlots {
            partitions: DEFAULT_PARTITIONS,
        }
    }
}

impl PartitionStrategy {
    /// Returns the logical partition for a canonical route key.
    /// # Errors
    /// Fails when the strategy is invalid.
    pub fn partition_of(&self, key: &[u8]) -> Result<PartitionId, ReplError> {
        match *self {
            Self::HashSlots { partitions } => {
                if partitions == 0 {
                    return Err(ReplError::Unsupported(
                        "hash-slot partition count must be greater than zero".to_owned(),
                    ));
                }

                let partition = stable_hash(key) % u64::from(partitions);
                PartitionId::try_from(partition)
                    .map_err(|error| ReplError::Unsupported(error.to_string()))
            }
        }
    }

    #[must_use]
    pub const fn partition_count(self) -> u32 {
        match self {
            Self::HashSlots { partitions } => partitions,
        }
    }
}

impl ShardMap {
    /// Builds a balanced hash-slot map over the supplied shards.
    /// # Errors
    /// Fails when partitions or shard ids are invalid.
    pub fn balanced(
        strategy: PartitionStrategy,
        shard_ids: impl IntoIterator<Item = ShardId>,
    ) -> Result<Self, ReplError> {
        let shard_ids = shard_ids.into_iter().collect::<Vec<_>>();
        if shard_ids.is_empty() {
            return Err(ReplError::Unsupported(
                "sharded replication needs at least one shard".to_owned(),
            ));
        }

        let mut seen = BTreeSet::new();
        for id in &shard_ids {
            if !seen.insert(*id) {
                return Err(ReplError::Unsupported(format!("duplicate shard id {id}")));
            }
        }

        let partitions = strategy.partition_count();
        if partitions == 0 {
            return Err(ReplError::Unsupported(
                "sharded replication needs at least one partition".to_owned(),
            ));
        }

        let mut placement = BTreeMap::new();
        for partition in 0..partitions {
            let index = usize::try_from(partition)
                .map_err(|error| ReplError::Unsupported(error.to_string()))?
                % shard_ids.len();
            placement.insert(partition, shard_ids[index]);
        }

        Ok(Self {
            version: 1,
            strategy,
            placement,
        })
    }

    /// Resolves a canonical route key to a shard.
    /// # Errors
    /// Fails when placement is missing.
    pub fn shard_for_key(&self, key: &[u8]) -> Result<ShardId, ReplError> {
        let partition = self.strategy.partition_of(key)?;
        self.shard_for_partition(partition)
    }

    /// Resolves a partition to a shard.
    /// # Errors
    /// Fails when placement is missing.
    pub fn shard_for_partition(&self, partition: PartitionId) -> Result<ShardId, ReplError> {
        self.placement.get(&partition).copied().ok_or_else(|| {
            ReplError::Unsupported(format!("partition {partition} has no shard placement"))
        })
    }

    /// Assigns a partition to a new shard and bumps the map version.
    /// # Errors
    /// Fails when the partition is unknown.
    pub fn reassign_partition(
        &mut self,
        partition: PartitionId,
        shard: ShardId,
    ) -> Result<(), ReplError> {
        let Some(current) = self.placement.get_mut(&partition) else {
            return Err(ReplError::Unsupported(format!(
                "partition {partition} has no shard placement"
            )));
        };

        if *current != shard {
            let version = self
                .version
                .checked_add(1)
                .ok_or_else(|| ReplError::Unsupported("shard map version overflow".to_owned()))?;
            *current = shard;
            self.version = version;
        }
        Ok(())
    }
}

impl ShardedReplication {
    /// Creates a sharded router over existing replication backends.
    /// # Errors
    /// Fails when the shard map references unknown shards.
    pub fn new(
        shard_map: ShardMap,
        shards: BTreeMap<ShardId, Arc<dyn Replication>>,
        global_shard: ShardId,
    ) -> Result<Self, ReplError> {
        if !shards.contains_key(&global_shard) {
            return Err(ReplError::Unsupported(format!(
                "global shard {global_shard} is not configured"
            )));
        }

        for shard in shard_map.placement.values() {
            if !shards.contains_key(shard) {
                return Err(ReplError::Unsupported(format!(
                    "shard map references unknown shard {shard}"
                )));
            }
        }

        Ok(Self {
            shard_map: RwLock::new(shard_map),
            shards,
            global_shard,
            migrations: RwLock::new(BTreeMap::new()),
            routing_barrier: RwLock::new(()),
        })
    }

    #[must_use]
    pub fn shard_map(&self) -> ShardMap {
        self.shard_map
            .read()
            .map_or_else(|_| empty_shard_map(), |map| map.clone())
    }

    #[must_use]
    pub const fn global_shard(&self) -> ShardId {
        self.global_shard
    }

    /// Returns the shard that owns a table/key pair.
    /// # Errors
    /// Fails when the key cannot be routed.
    pub fn shard_for(&self, table: &str, key: &[u8]) -> Result<ShardId, ReplError> {
        match self.route(table, key)? {
            Route::Global => Ok(self.global_shard),
            Route::Partition(partition) => self.shard_for_partition(partition),
        }
    }

    /// Returns the current owner of a partition.
    /// # Errors
    /// Fails when the partition is not placed.
    pub fn shard_for_partition(&self, partition: PartitionId) -> Result<ShardId, ReplError> {
        self.shard_map_read()?.shard_for_partition(partition)
    }

    /// Reassigns one partition without copying data.
    /// # Errors
    /// Fails when the target shard or partition is unknown.
    pub fn reassign_partition(
        &self,
        partition: PartitionId,
        to: ShardId,
    ) -> Result<ShardMigration, ReplError> {
        let _routing = self.routing_write()?;
        self.ensure_shard_exists(to)?;
        let from = self.shard_for_partition(partition)?;
        let migration = ShardMigration {
            partition,
            from,
            to,
            state: MigrationState::Done,
        };
        self.shard_map_write()?.reassign_partition(partition, to)?;
        self.record_migration(&migration)?;
        Ok(migration)
    }

    /// Migrates a partition while router operations are quiescent. Public model
    /// keyspaces come from both backends' complete tracked inventories; internal
    /// global metadata retains its global placement.
    /// # Errors
    /// Fails before ownership changes when inventory, copying or persistence fails.
    pub fn migrate_partition(
        &self,
        partition: PartitionId,
        to: ShardId,
    ) -> Result<ShardMigration, ReplError> {
        let _routing = self.routing_write()?;
        self.ensure_shard_exists(to)?;
        let from = self.shard_for_partition(partition)?;
        let mut migration = ShardMigration {
            partition,
            from,
            to,
            state: MigrationState::Copying,
        };
        if from == to {
            migration.state = MigrationState::Done;
            self.set_migration(&migration)?;
            return Ok(migration);
        }
        // Refuse a backend without a complete inventory before any copy/switch.
        let tables = self
            .shard(from)?
            .registered_keyspaces()?
            .into_iter()
            .chain(self.shard(to)?.registered_keyspaces()?)
            .filter(|table| !is_global_table(table))
            .collect::<BTreeSet<_>>();
        self.set_migration(&migration)?;
        let copying = self.copy_partition(partition, from, to, &tables);
        if let Err(error) = copying {
            migration.state = MigrationState::Aborted;
            let _ = self.set_migration(&migration);
            return Err(error);
        }
        migration.state = MigrationState::Switching;
        self.set_migration(&migration)?;
        if let Err(error) = self.shard_map_write()?.reassign_partition(partition, to) {
            migration.state = MigrationState::Aborted;
            let _ = self.set_migration(&migration);
            return Err(error);
        }
        migration.state = MigrationState::Done;
        self.set_migration(&migration)?;
        Ok(migration)
    }

    #[must_use]
    pub fn migration(&self, partition: PartitionId) -> Option<ShardMigration> {
        self.migrations
            .read()
            .ok()
            .and_then(|migrations| migrations.get(&partition).cloned())
    }

    fn route(&self, table: &str, key: &[u8]) -> Result<Route, ReplError> {
        if is_global_table(table) {
            return Ok(Route::Global);
        }

        let route_key = canonical_route_key(table, key);
        let partition = self.shard_map_read()?.strategy.partition_of(&route_key)?;
        Ok(Route::Partition(partition))
    }

    fn shard_for_route(&self, table: &str, key: &[u8]) -> Result<ShardId, ReplError> {
        self.shard_for(table, key)
    }

    fn shard(&self, shard: ShardId) -> Result<&Arc<dyn Replication>, ReplError> {
        self.shards
            .get(&shard)
            .ok_or_else(|| ReplError::Unsupported(format!("unknown shard {shard}")))
    }

    fn ensure_shard_exists(&self, shard: ShardId) -> Result<(), ReplError> {
        if self.shards.contains_key(&shard) {
            Ok(())
        } else {
            Err(ReplError::Unsupported(format!("unknown shard {shard}")))
        }
    }

    fn shard_map_read(&self) -> Result<std::sync::RwLockReadGuard<'_, ShardMap>, ReplError> {
        self.shard_map
            .read()
            .map_err(|_| ReplError::Transport("shard map lock poisoned".to_owned()))
    }

    fn shard_map_write(&self) -> Result<std::sync::RwLockWriteGuard<'_, ShardMap>, ReplError> {
        self.shard_map
            .write()
            .map_err(|_| ReplError::Transport("shard map lock poisoned".to_owned()))
    }

    fn set_migration(&self, migration: &ShardMigration) -> Result<(), ReplError> {
        self.migrations
            .write()
            .map_err(|_| ReplError::Transport("shard migration lock poisoned".to_owned()))?
            .insert(migration.partition, migration.clone());
        self.record_migration(migration)
    }

    fn record_migration(&self, migration: &ShardMigration) -> Result<(), ReplError> {
        let bytes = serde_json::to_vec(migration)
            .map_err(|error| ReplError::Transport(error.to_string()))?;
        propose_system(
            self.shard(self.global_shard)?.as_ref(),
            Op::Put {
                table: SHARD_MIGRATIONS_TABLE.to_owned(),
                key: migration.partition.to_be_bytes().to_vec(),
                value: bytes,
            },
        )
    }

    fn routing_read(&self) -> Result<std::sync::RwLockReadGuard<'_, ()>, ReplError> {
        self.routing_barrier
            .read()
            .map_err(|_| ReplError::Transport("shard routing barrier poisoned".to_owned()))
    }

    fn routing_write(&self) -> Result<std::sync::RwLockWriteGuard<'_, ()>, ReplError> {
        self.routing_barrier
            .write()
            .map_err(|_| ReplError::Transport("shard routing barrier poisoned".to_owned()))
    }

    fn copy_partition(
        &self,
        partition: PartitionId,
        from: ShardId,
        to: ShardId,
        tables: &BTreeSet<String>,
    ) -> Result<(), ReplError> {
        for table in tables {
            // Target copies from an earlier attempt must disappear, including tables
            // that no longer have rows on the source. Each bounded callback holds at
            // most 128 records; ownership stays with the source until all copies finish.
            self.shard(to)?.scan_range_batches(table, &[], &[], ReadConsistency::Strong, 128, &|| false, &mut |batch| {
                let mut ops = Vec::new();
                for (key, _) in batch {
                    if matches!(self.route(table, key)?, Route::Partition(owner) if owner == partition) {
                        ops.push(Op::Delete { table: table.clone(), key: key.clone() });
                    }
                }
                if !ops.is_empty() { self.shard(to)?.propose_batch(ops)?; }
                Ok(true)
            })?;
            self.shard(from)?.scan_range_batches(table, &[], &[], ReadConsistency::Strong, 128, &|| false, &mut |batch| {
                let mut ops = Vec::new();
                for (key, value) in batch {
                    if matches!(self.route(table, key)?, Route::Partition(owner) if owner == partition) {
                        ops.push(Op::Put { table: table.clone(), key: key.clone(), value: value.clone() });
                    }
                }
                if !ops.is_empty() { self.shard(to)?.propose_batch(ops)?; }
                Ok(true)
            })?;
        }
        Ok(())
    }

    fn route_public_ops(&self, ops: &[Op]) -> Result<BTreeMap<ShardId, Vec<Op>>, ReplError> {
        let mut groups = BTreeMap::<ShardId, Vec<Op>>::new();
        for op in ops {
            let (table, key) = op_parts(op);
            let shard = self.shard_for_route(table, key)?;
            groups.entry(shard).or_default().push(op.clone());
        }
        Ok(groups)
    }

    fn propose_distributed_batch(
        &self,
        groups: &BTreeMap<ShardId, Vec<Op>>,
    ) -> Result<(), ReplError> {
        let txn_id = self.allocate_dist_txn_id()?;
        let mut prepare_error = None;

        for (shard_id, ops) in groups {
            observability::record_shard_operation(*shard_id, "dist_txn_prepare");
            match self.shard(*shard_id)?.prepare_dist_txn(txn_id, ops.clone()) {
                Ok(Vote::Yes) => {}
                Ok(Vote::No) => {
                    prepare_error = Some(ReplError::Conflict);
                    break;
                }
                Err(error) => {
                    prepare_error = Some(error);
                    break;
                }
            }
        }

        let decision = if prepare_error.is_some() {
            Decision::Abort
        } else {
            Decision::Commit
        };
        self.record_dist_txn_decision(txn_id, decision)?;

        let mut finish_error = None;
        // A prepare RPC can persist its locks and then lose its acknowledgement.
        // Notify every possible participant, also after an ambiguous prepare error.
        for shard_id in groups.keys() {
            observability::record_shard_operation(*shard_id, "dist_txn_finish");
            if let Err(error) = self.shard(*shard_id)?.finish_dist_txn(txn_id, decision)
                && finish_error.is_none()
            {
                finish_error = Some(error);
            }
        }

        if let Some(error) = prepare_error {
            return Err(error);
        }
        if let Some(error) = finish_error {
            return Err(error);
        }
        Ok(())
    }

    fn record_dist_txn_decision(
        &self,
        txn_id: DistTxnId,
        decision: Decision,
    ) -> Result<(), ReplError> {
        let record = CoordinatorDecisionRecord::new(txn_id, decision, DEFAULT_DIST_TXN_DEADLINE_MS);
        let coordinator = self.shard(self.global_shard)?.as_ref();
        let key = dist_txn::txn_key(txn_id).to_vec();
        match propose_system_conditional_batch(
            coordinator,
            ConditionalBatch::new(
                vec![WriteCondition::KeyMissing {
                    table: DIST_TXN_COORDINATOR_TABLE.to_owned(),
                    key: key.clone(),
                }],
                vec![Op::Put {
                    table: DIST_TXN_COORDINATOR_TABLE.to_owned(),
                    key: key.clone(),
                    value: dist_txn::encode_decision(&record)?,
                }],
            ),
        ) {
            Ok(()) => Ok(()),
            Err(
                ReplError::Conflict | ReplError::Storage(crate::storage::StorageError::Conflict),
            ) => {
                let bytes = coordinator
                    .read(DIST_TXN_COORDINATOR_TABLE, &key, ReadConsistency::Strong)?
                    .ok_or(ReplError::Conflict)?;
                let existing = dist_txn::decode_decision(&bytes)?;
                if existing.txn_id != txn_id || existing.decision != decision {
                    return Err(ReplError::Conflict);
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn allocate_dist_txn_id(&self) -> Result<DistTxnId, ReplError> {
        let coordinator = self.shard(self.global_shard)?.as_ref();
        for _ in 0..64 {
            let previous = coordinator.read(
                DIST_TXN_SEQUENCE_TABLE,
                DIST_TXN_SEQUENCE_KEY,
                ReadConsistency::Strong,
            )?;
            let last = if let Some(bytes) = previous.as_deref() {
                txn::decode_txn_id(bytes)?
            } else {
                let mut maximum = self
                    .coordinator_decisions()?
                    .iter()
                    .map(|record| record.txn_id)
                    .max()
                    .unwrap_or(0);
                // Bootstrap a legacy cluster without reusing an in-doubt or
                // completed participant ID. Subsequent allocations are O(1).
                for shard in self.shards.values() {
                    for table in [
                        super::DIST_TXN_PARTICIPANT_TABLE,
                        super::DIST_TXN_FINISHED_TABLE,
                    ] {
                        for (key, _) in shard.range(table, &[], &[], ReadConsistency::Strong)? {
                            maximum = maximum.max(txn::decode_txn_id(&key)?);
                        }
                    }
                }
                maximum
            };
            let next = last.checked_add(1).ok_or_else(|| {
                ReplError::Unsupported("distributed transaction ID exhausted".to_owned())
            })?;
            let batch = ConditionalBatch::new(
                vec![WriteCondition::ValueEquals {
                    table: DIST_TXN_SEQUENCE_TABLE.to_owned(),
                    key: DIST_TXN_SEQUENCE_KEY.to_vec(),
                    expected: previous,
                }],
                vec![Op::Put {
                    table: DIST_TXN_SEQUENCE_TABLE.to_owned(),
                    key: DIST_TXN_SEQUENCE_KEY.to_vec(),
                    value: txn::encode_txn_id(next).to_vec(),
                }],
            );
            match propose_system_conditional_batch(coordinator, batch) {
                Ok(()) => return Ok(next),
                Err(
                    ReplError::Conflict
                    | ReplError::Storage(crate::storage::StorageError::Conflict),
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Err(ReplError::Conflict)
    }

    fn coordinator_decisions(&self) -> Result<Vec<CoordinatorDecisionRecord>, ReplError> {
        self.shard(self.global_shard)?
            .range(
                DIST_TXN_COORDINATOR_TABLE,
                &[],
                &[],
                ReadConsistency::Strong,
            )?
            .into_iter()
            .map(|(_, bytes)| dist_txn::decode_decision(&bytes).map_err(Into::into))
            .collect()
    }
}

impl Replication for ShardedReplication {
    fn propose_authorized_conditional_batch(
        &self,
        batch: ConditionalBatch,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let _routing = self.routing_read()?;
        let mut target = None;
        for (table, key) in batch
            .ops
            .iter()
            .map(op_parts)
            .chain(batch.conditions.iter().map(condition_parts))
        {
            let next = self.shard_for_route(table, key)?;
            if target.is_some_and(|current| current != next) {
                return Err(ReplError::Unsupported(
                    "cross-shard conditional batch needs distributed conditions".to_owned(),
                ));
            }
            target = Some(next);
        }
        self.shard(target.unwrap_or(self.global_shard))?
            .propose_authorized_conditional_batch(batch, authorization)
    }

    fn supports_commit_log(&self) -> bool {
        self.shards
            .values()
            .all(|shard| shard.supports_commit_log())
    }

    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![op])
    }

    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        if ops.is_empty() {
            return Ok(());
        }
        let _routing = self.routing_read()?;
        txn::validate_public_ops(&ops)?;

        let mut groups = self.route_public_ops(&ops)?;
        if groups.len() > 1 {
            return self.propose_distributed_batch(&groups);
        }

        let shard = groups.keys().next().copied().unwrap_or(self.global_shard);
        observability::record_shard_operation(shard, "propose_batch");
        self.shard(shard)?
            .propose_batch(groups.remove(&shard).unwrap_or_default())
    }

    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        let _routing = self.routing_read()?;
        if ops.is_empty() {
            return Ok(());
        }

        let mut target = None;
        for op in &ops {
            let (table, key) = op_parts(op);
            let next = self.shard_for_route(table, key)?;

            match target {
                Some(current) if current != next => {
                    return Err(ReplError::Unsupported(
                        "cross-shard atomic batch needs distributed transactions".to_owned(),
                    ));
                }
                Some(_) => {}
                None => target = Some(next),
            }
        }

        let shard = target.unwrap_or(self.global_shard);
        observability::record_shard_operation(shard, "propose_authorized_batch");
        self.shard(shard)?
            .propose_authorized_batch(ops, authorization)
    }

    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        if batch.conditions.is_empty() {
            return self.propose_batch(batch.ops);
        }

        let _routing = self.routing_read()?;
        txn::validate_public_conditions(&batch.conditions)?;
        txn::validate_public_ops(&batch.ops)?;
        let mut target = None;
        for (table, key) in batch
            .ops
            .iter()
            .map(op_parts)
            .chain(batch.conditions.iter().map(condition_parts))
        {
            let next = self.shard_for_route(table, key)?;

            match target {
                Some(current) if current != next => {
                    return Err(ReplError::Unsupported(
                        "cross-shard conditional batch needs distributed transactions".to_owned(),
                    ));
                }
                Some(_) => {}
                None => target = Some(next),
            }
        }

        let shard = target.unwrap_or(self.global_shard);
        observability::record_shard_operation(shard, "propose_conditional_batch");
        self.shard(shard)?.propose_conditional_batch(batch)
    }

    fn recover_dist_txns(&self) -> Result<(), ReplError> {
        let _routing = self.routing_read()?;
        for decision in self.coordinator_decisions()? {
            for (shard_id, shard) in &self.shards {
                observability::record_shard_operation(*shard_id, "dist_txn_recover_finish");
                shard.finish_dist_txn(decision.txn_id, decision.decision)?;
            }
        }

        for (shard_id, shard) in &self.shards {
            observability::record_shard_operation(*shard_id, "dist_txn_recover_participant");
            shard.recover_dist_txns()?;
        }
        Ok(())
    }

    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        let _routing = self.routing_read()?;
        let shard = self.shard_for_route(table, key)?;
        observability::record_shard_operation(shard, "read");
        self.shard(shard)?.read(table, key, consistency)
    }

    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        let _routing = self.routing_read()?;
        if is_global_table(table) {
            observability::record_shard_operation(self.global_shard, "range_global");
            return self
                .shard(self.global_shard)?
                .range(table, start, end, consistency);
        }

        let mut rows = Vec::new();
        for (shard_id, shard) in &self.shards {
            observability::record_shard_operation(*shard_id, "scatter_range");
            for (key, value) in shard.range(table, start, end, consistency)? {
                // Old owners retain physical copies after migration. Only the current
                // owner may serve a key, including after updates/deletes on its target.
                if self.shard_for_route(table, &key)? == *shard_id {
                    rows.push((key, value));
                }
            }
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows.dedup_by(|left, right| left.0 == right.0);
        Ok(rows)
    }
}

fn op_parts(op: &Op) -> (&str, &[u8]) {
    match op {
        Op::Put { table, key, .. } | Op::Delete { table, key } => (table, key),
    }
}

fn condition_parts(condition: &WriteCondition) -> (&str, &[u8]) {
    match condition {
        WriteCondition::KeyMissing { table, key }
        | WriteCondition::ValueEquals { table, key, .. } => (table, key),
    }
}

fn canonical_route_key(table: &str, key: &[u8]) -> Bytes {
    match table {
        REL_INDEX_TABLE => {
            row_key_from_rel_index(key).map_or_else(|| key.to_vec(), std::borrow::ToOwned::to_owned)
        }
        DOCUMENT_INDEX_TABLE => document_key_from_index_key(key).unwrap_or_else(|| key.to_vec()),
        REL_COLUMNAR_SEGMENTS_TABLE => table_prefix_from_rel_key(key)
            .map_or_else(|| key.to_vec(), std::borrow::ToOwned::to_owned),
        _ => key.to_vec(),
    }
}

fn row_key_from_rel_index(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 8 {
        return None;
    }

    let len_start = key.len().checked_sub(8)?;
    let row_len = u64::from_be_bytes(key[len_start..].try_into().ok()?);
    let row_len = usize::try_from(row_len).ok()?;
    let row_start = len_start.checked_sub(row_len)?;
    Some(&key[row_start..len_start])
}

fn document_key_from_index_key(key: &[u8]) -> Option<Bytes> {
    if key.len() < 4 + DOCUMENT_ID_LEN {
        return None;
    }

    let mut route = Vec::with_capacity(DOCUMENT_KEY_LEN);
    route.extend_from_slice(&key[..4]);
    route.extend_from_slice(&key[key.len() - DOCUMENT_ID_LEN..]);
    Some(route)
}

fn table_prefix_from_rel_key(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 8 {
        return None;
    }

    let table_len = u64::from_be_bytes(key[..8].try_into().ok()?);
    let table_len = usize::try_from(table_len).ok()?;
    let end = 8_usize.checked_add(table_len)?;
    (key.len() >= end).then_some(&key[..end])
}

fn is_global_table(table: &str) -> bool {
    table.starts_with("__")
}

fn stable_hash(key: &[u8]) -> u64 {
    let hash = blake3::hash(key);
    u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap_or([0; 8]))
}

fn empty_shard_map() -> ShardMap {
    ShardMap {
        version: 0,
        strategy: PartitionStrategy::default(),
        placement: BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::{
        DOCUMENT_INDEX_TABLE, MigrationState, PartitionStrategy, REL_INDEX_TABLE, REL_ROWS_TABLE,
        ShardMap, ShardedReplication,
    };
    use crate::{
        repl::{Decision, Op, ReadConsistency, ReplError, Replication, SingleNode, Vote},
        storage::{MemEngine, ReadTransaction, StorageEngine},
    };

    type Harness = (
        ShardedReplication,
        Arc<SingleNode<MemEngine>>,
        Arc<SingleNode<MemEngine>>,
    );

    fn sharded() -> Result<Harness, ReplError> {
        let one = Arc::new(SingleNode::new(MemEngine::new()));
        let two = Arc::new(SingleNode::new(MemEngine::new()));
        let mut shards: BTreeMap<u32, Arc<dyn Replication>> = BTreeMap::new();
        let one_repl: Arc<dyn Replication> = one.clone();
        let two_repl: Arc<dyn Replication> = two.clone();
        shards.insert(1, one_repl);
        shards.insert(2, two_repl);
        let map = ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 16 }, [1, 2])?;
        Ok((ShardedReplication::new(map, shards, 1)?, one, two))
    }

    #[test]
    fn commit_log_capability_includes_every_shard() -> Result<(), ReplError> {
        let (cp_only, _, _) = sharded()?;
        assert!(cp_only.supports_commit_log());

        let mut shards: BTreeMap<u32, Arc<dyn Replication>> = BTreeMap::new();
        shards.insert(1, Arc::new(SingleNode::new(MemEngine::new())));
        shards.insert(
            2,
            Arc::new(crate::repl::ApDynamo::new_for_test(MemEngine::new(), 2)),
        );
        let map = ShardMap::balanced(PartitionStrategy::HashSlots { partitions: 16 }, [1, 2])?;
        let mixed = ShardedReplication::new(map, shards, 1)?;
        assert!(!mixed.supports_commit_log());
        Ok(())
    }

    fn two_keys_on_different_shards(
        repl: &ShardedReplication,
    ) -> Result<(Vec<u8>, Vec<u8>), ReplError> {
        let mut first: Option<(Vec<u8>, u32)> = None;
        for value in 0_u8..=u8::MAX {
            let key = vec![value];
            let shard = repl.shard_for("t", &key)?;
            if let Some((first_key, first_shard)) = &first {
                if *first_shard != shard {
                    return Ok((first_key.clone(), key));
                }
            } else {
                first = Some((key, shard));
            }
        }
        Err(ReplError::Unsupported(
            "could not find split keys".to_owned(),
        ))
    }

    #[test]
    fn hash_slots_are_deterministic_and_balanced() -> Result<(), Box<dyn std::error::Error>> {
        let map = ShardMap::balanced(
            PartitionStrategy::HashSlots { partitions: 32 },
            [1, 2, 3, 4],
        )?;
        assert_eq!(
            map.strategy.partition_of(b"same")?,
            map.strategy.partition_of(b"same")?
        );
        assert_eq!(map.placement.len(), 32);

        let mut counts = BTreeMap::new();
        for shard in map.placement.values() {
            *counts.entry(*shard).or_insert(0_usize) += 1;
        }
        assert!(counts.values().all(|count| *count == 8));
        Ok(())
    }

    #[test]
    fn point_writes_go_to_one_shard_and_range_scatters() -> Result<(), Box<dyn std::error::Error>> {
        let (repl, one, two) = sharded()?;
        let (left, right) = two_keys_on_different_shards(&repl)?;

        repl.propose(Op::Put {
            table: "t".to_owned(),
            key: left.clone(),
            value: b"left".to_vec(),
        })?;
        repl.propose(Op::Put {
            table: "t".to_owned(),
            key: right.clone(),
            value: b"right".to_vec(),
        })?;

        assert_eq!(
            repl.read("t", &left, ReadConsistency::Strong)?,
            Some(b"left".to_vec())
        );
        let rows = repl.range("t", &[], &[0xFF], ReadConsistency::Strong)?;
        assert_eq!(rows.len(), 2);

        let one_rows = one
            .storage()
            .begin_read()?
            .range("t", &[], &[0xFF])?
            .count();
        let two_rows = two
            .storage()
            .begin_read()?
            .range("t", &[], &[0xFF])?
            .count();
        assert_eq!(one_rows + two_rows, 2);
        Ok(())
    }

    #[test]
    fn model_index_batches_route_with_their_primary_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, _, _) = sharded()?;
        let document_key = [7_u8; 20].to_vec();
        let mut doc_index_key = vec![7_u8; 4];
        doc_index_key.extend_from_slice(b"encoded-value");
        doc_index_key.extend_from_slice(&document_key[4..]);

        assert_eq!(
            repl.shard_for("documents", &document_key)?,
            repl.shard_for(DOCUMENT_INDEX_TABLE, &doc_index_key)?
        );

        let mut row_key = b"row-key".to_vec();
        row_key.push(0);
        let mut rel_index_key = b"index-prefix".to_vec();
        rel_index_key.extend_from_slice(&row_key);
        rel_index_key.extend_from_slice(&(row_key.len() as u64).to_be_bytes());
        assert_eq!(
            repl.shard_for(REL_ROWS_TABLE, &row_key)?,
            repl.shard_for(REL_INDEX_TABLE, &rel_index_key)?
        );

        repl.propose_batch(vec![
            Op::Put {
                table: "documents".to_owned(),
                key: document_key,
                value: b"doc".to_vec(),
            },
            Op::Put {
                table: DOCUMENT_INDEX_TABLE.to_owned(),
                key: doc_index_key,
                value: Vec::new(),
            },
        ])?;

        Ok(())
    }

    #[test]
    fn cross_shard_batch_commits_with_dist_txn() -> Result<(), Box<dyn std::error::Error>> {
        let (repl, _, _) = sharded()?;
        let (left, right) = two_keys_on_different_shards(&repl)?;

        repl.propose_batch(vec![
            Op::Put {
                table: "t".to_owned(),
                key: left.clone(),
                value: b"1".to_vec(),
            },
            Op::Put {
                table: "t".to_owned(),
                key: right.clone(),
                value: b"2".to_vec(),
            },
        ])?;

        assert_eq!(
            repl.read("t", &left, ReadConsistency::Strong)?,
            Some(b"1".to_vec())
        );
        assert_eq!(
            repl.read("t", &right, ReadConsistency::Strong)?,
            Some(b"2".to_vec())
        );

        Ok(())
    }

    #[test]
    fn recover_dist_txns_finishes_committed_in_doubt_participants()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, one, two) = sharded()?;
        let (left, right) = two_keys_on_different_shards(&repl)?;
        let left_op = Op::Put {
            table: "t".to_owned(),
            key: left.clone(),
            value: b"left".to_vec(),
        };
        let right_op = Op::Put {
            table: "t".to_owned(),
            key: right.clone(),
            value: b"right".to_vec(),
        };
        let txn_id = 9_001;
        let left_shard = repl.shard_for("t", &left)?;
        let right_shard = repl.shard_for("t", &right)?;

        repl.shard(left_shard)?
            .prepare_dist_txn(txn_id, vec![left_op])?;
        repl.shard(right_shard)?
            .prepare_dist_txn(txn_id, vec![right_op])?;
        repl.record_dist_txn_decision(txn_id, Decision::Commit)?;
        repl.shard(left_shard)?
            .finish_dist_txn(txn_id, Decision::Commit)?;

        assert_eq!(
            repl.read("t", &left, ReadConsistency::Strong)?,
            Some(b"left".to_vec())
        );
        assert!(matches!(
            repl.read("t", &right, ReadConsistency::Strong),
            Err(ReplError::Conflict | ReplError::Storage(crate::storage::StorageError::Conflict))
        ));

        let mut shards: BTreeMap<u32, Arc<dyn Replication>> = BTreeMap::new();
        let one_repl: Arc<dyn Replication> = one;
        let two_repl: Arc<dyn Replication> = two;
        shards.insert(1, one_repl);
        shards.insert(2, two_repl);
        let restarted = ShardedReplication::new(repl.shard_map(), shards, repl.global_shard())?;

        restarted.recover_dist_txns()?;
        restarted.recover_dist_txns()?;

        assert_eq!(
            restarted.read("t", &left, ReadConsistency::Strong)?,
            Some(b"left".to_vec())
        );
        assert_eq!(
            restarted.read("t", &right, ReadConsistency::Strong)?,
            Some(b"right".to_vec())
        );

        Ok(())
    }

    #[test]
    fn recover_dist_txns_keeps_prepared_without_coordinator_decision()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repl, _, _) = sharded()?;
        let (left, right) = two_keys_on_different_shards(&repl)?;
        let txn_id = 9_002;
        let left_shard = repl.shard_for("t", &left)?;
        let right_shard = repl.shard_for("t", &right)?;

        repl.shard(left_shard)?.prepare_dist_txn(
            txn_id,
            vec![Op::Put {
                table: "t".to_owned(),
                key: left.clone(),
                value: b"left".to_vec(),
            }],
        )?;
        repl.shard(right_shard)?.prepare_dist_txn(
            txn_id,
            vec![Op::Put {
                table: "t".to_owned(),
                key: right.clone(),
                value: b"right".to_vec(),
            }],
        )?;

        repl.recover_dist_txns()?;

        for key in [&left, &right] {
            assert!(matches!(
                repl.read("t", key, ReadConsistency::Strong),
                Err(ReplError::Conflict
                    | ReplError::Storage(crate::storage::StorageError::Conflict))
            ));
        }
        assert_eq!(
            repl.shard(left_shard)?
                .prepare_dist_txn(txn_id, Vec::new())?,
            Vote::No
        );
        assert_eq!(
            repl.shard(right_shard)?
                .prepare_dist_txn(txn_id, Vec::new())?,
            Vote::No
        );

        // Resolve only after an authoritative coordinator decision arrives.
        repl.record_dist_txn_decision(txn_id, Decision::Abort)?;
        repl.recover_dist_txns()?;
        assert_eq!(repl.read("t", &left, ReadConsistency::Strong)?, None);
        assert_eq!(repl.read("t", &right, ReadConsistency::Strong)?, None);

        Ok(())
    }

    #[test]
    fn distributed_ids_and_decisions_are_shared_across_router_instances()
    -> Result<(), Box<dyn std::error::Error>> {
        let (first, one, two) = sharded()?;
        let router = |map: ShardMap| {
            let left: Arc<dyn Replication> = one.clone();
            let right: Arc<dyn Replication> = two.clone();
            ShardedReplication::new(map, BTreeMap::from([(1, left), (2, right)]), 1)
        };
        let second = router(first.shard_map())?;
        let map = first.shard_map();
        let handles = [first, second].map(|router| {
            std::thread::spawn(move || {
                (0..32)
                    .map(|_| router.allocate_dist_txn_id())
                    .collect::<Result<Vec<_>, _>>()
            })
        });
        let mut ids = std::collections::BTreeSet::new();
        for handle in handles {
            for id in handle.join().map_err(|_| "allocator panicked")?? {
                assert!(ids.insert(id));
            }
        }
        assert_eq!(ids.len(), 64);
        let reopened = router(map)?;
        assert_eq!(reopened.allocate_dist_txn_id()?, 65);
        reopened.record_dist_txn_decision(65, Decision::Commit)?;
        reopened.record_dist_txn_decision(65, Decision::Commit)?;
        assert!(
            reopened
                .record_dist_txn_decision(65, Decision::Abort)
                .is_err()
        );
        assert_eq!(
            reopened.coordinator_decisions()?[0].decision,
            Decision::Commit
        );
        Ok(())
    }

    #[test]
    fn migration_copies_partition_and_updates_shard_map() -> Result<(), Box<dyn std::error::Error>>
    {
        let (repl, _, two) = sharded()?;
        let key = (0_u8..=u8::MAX)
            .map(|value| vec![value])
            .find(|key| {
                repl.shard_for("rel_rows", key)
                    .is_ok_and(|shard| shard == 1)
            })
            .ok_or("missing key for shard 1")?;
        let partition = repl.shard_map().strategy.partition_of(&key)?;

        repl.propose(Op::Put {
            table: REL_ROWS_TABLE.to_owned(),
            key: key.clone(),
            value: b"row".to_vec(),
        })?;
        let migration = repl.migrate_partition(partition, 2)?;

        assert_eq!(migration.state, MigrationState::Done);
        assert_eq!(repl.shard_for("rel_rows", &key)?, 2);
        assert_eq!(
            repl.read(REL_ROWS_TABLE, &key, ReadConsistency::Strong)?,
            Some(b"row".to_vec())
        );
        assert_eq!(
            two.storage().begin_read()?.get(REL_ROWS_TABLE, &key)?,
            Some(b"row".to_vec())
        );

        Ok(())
    }
}

#[cfg(test)]
mod migration_tests;
