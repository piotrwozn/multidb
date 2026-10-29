use super::{ApDynamo, read_versions_from_txn, version_key, write_versions_to_txn};
use crate::{
    repl::{ConditionalBatch, ReplError, WriteCondition},
    storage::{StorageEngine, StorageError, WriteTransaction},
};

impl<S: StorageEngine> ApDynamo<S> {
    pub(super) fn is_single_local_node(&self) -> bool {
        self.cluster.replication_factor == 1 && self.cluster.nodes.len() == 1
    }

    /// A multi-writer AP cluster cannot implement global compare-and-swap with
    /// a coordinator-local mutex. Only a genuinely single-node topology offers
    /// these atomic conditions; ordinary AP publication remains unchanged.
    pub(super) fn commit_local_conditional(
        &self,
        batch: ConditionalBatch,
    ) -> Result<(), ReplError> {
        if batch.conditions.is_empty() && batch.ops.is_empty() {
            return Ok(());
        }
        if !self.is_single_local_node() {
            return Err(ReplError::Unsupported(
                "atomic conditional writes require single-node AP; multi-node AP needs a consensus coordinator".to_owned(),
            ));
        }
        if !self.is_available(self.cluster.node_id)? {
            return Err(ReplError::NoQuorum);
        }
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| StorageError::Backend("AP commit lock poisoned".to_owned()))?;
        let mut txn = self.storage.begin_write()?;
        for condition in &batch.conditions {
            let (table, key) = match condition {
                WriteCondition::KeyMissing { table, key }
                | WriteCondition::ValueEquals { table, key, .. } => (table, key),
            };
            let versions = read_versions_from_txn(&txn, &version_key(table, key)?)?;
            let current = match versions.as_slice() {
                [] => None,
                [version] => version.value.as_deref(),
                _ => {
                    txn.rollback();
                    return Err(ReplError::Conflict);
                }
            };
            let matches = match condition {
                WriteCondition::KeyMissing { .. } => current.is_none(),
                WriteCondition::ValueEquals { expected, .. } => current == expected.as_deref(),
            };
            if !matches {
                txn.rollback();
                return Err(ReplError::Conflict);
            }
        }
        if batch.ops.is_empty() {
            txn.rollback();
            return Ok(());
        }
        for write in self.build_writes_in_txn(&mut txn, batch.ops)? {
            let encoded = version_key(&write.table, &write.key)?;
            write_versions_to_txn(
                &mut txn,
                &write.table,
                &write.key,
                &encoded,
                &[write.version],
            )?;
        }
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
