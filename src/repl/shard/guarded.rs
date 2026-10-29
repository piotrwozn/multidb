use crate::{
    repl::{ReadConsistency, ReplError},
    storage::{Bytes, ReadBudget, ReadReservation},
};

use super::{ShardId, ShardedReplication, is_global_table};

struct Head {
    key: Bytes,
    value: Bytes,
    _reservation: Box<dyn ReadReservation>,
}

#[cfg(test)]
mod tests;

impl ShardedReplication {
    pub(super) fn guarded_routing_read(
        &self,
        budget: &dyn ReadBudget,
    ) -> Result<std::sync::RwLockReadGuard<'_, ()>, ReplError> {
        loop {
            budget.check()?;
            match self.routing_barrier.try_read() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(ReplError::Transport(
                        "shard routing barrier poisoned".to_owned(),
                    ));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    }

    fn guarded_head(
        &self,
        shard: ShardId,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        budget: &dyn ReadBudget,
    ) -> Result<Option<Head>, ReplError> {
        let mut head = None;
        self.shard(shard)?.visit_range(
            table,
            start,
            end,
            consistency,
            budget,
            &mut |key, value| {
                budget.check()?;
                let _routing = budget.reserve(key.len().saturating_mul(4).saturating_add(256))?;
                if self.shard_for_route(table, key)? != shard {
                    return Ok(true);
                }
                let reservation =
                    budget.reserve(key.len().saturating_add(value.len()).saturating_add(256))?;
                head = Some(Head {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    _reservation: reservation,
                });
                Ok(false)
            },
        )?;
        Ok(head)
    }

    // Match the public replication visitor without changing callback lifetimes.
    #[allow(clippy::type_complexity)]
    pub(super) fn guarded_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        budget.check()?;
        if !end.is_empty() && start >= end {
            return Ok(());
        }
        let _routing = self.guarded_routing_read(budget)?;
        if is_global_table(table) {
            return self.shard(self.global_shard)?.visit_range(
                table,
                start,
                end,
                consistency,
                budget,
                visitor,
            );
        }
        let _metadata = budget.reserve(
            self.shards
                .len()
                .saturating_mul(2 * std::mem::size_of::<(ShardId, Option<Head>)>())
                .saturating_add(512),
        )?;
        let mut heads = Vec::with_capacity(self.shards.len());
        for shard in self.shards.keys().copied() {
            budget.check()?;
            heads.push((
                shard,
                self.guarded_head(shard, table, start, end, consistency, budget)?,
            ));
        }
        loop {
            budget.check()?;
            let Some(index) = heads
                .iter()
                .enumerate()
                .filter_map(|(index, (_, head))| head.as_ref().map(|head| (index, &head.key)))
                .min_by(|(_, left), (_, right)| left.cmp(right))
                .map(|(index, _)| index)
            else {
                break;
            };
            let head = heads[index]
                .1
                .take()
                .ok_or_else(|| ReplError::Transport("missing guarded shard head".to_owned()))?;
            if !visitor(&head.key, &head.value)? {
                return Ok(());
            }
            let _cursor = budget.reserve(
                head.key
                    .len()
                    .saturating_add(1)
                    .saturating_mul(2)
                    .saturating_add(128),
            )?;
            let mut cursor = head.key.clone();
            cursor.push(0);
            drop(head);
            heads[index].1 =
                self.guarded_head(heads[index].0, table, &cursor, end, consistency, budget)?;
        }
        budget.check().map_err(Into::into)
    }
}
