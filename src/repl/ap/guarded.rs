use crate::storage::{ReadBudget, ReadReservation, ReadTransaction, StorageEngine, StorageError};

use super::{
    AP_VERSIONS_TABLE, ApDynamo, NodeId, ReadConsistency, ReplError, VersionedRecord,
    decode_version_key, decode_versions, merge_reply_versions, version_range_bounds,
};

mod value_bound;

/// Owned page buffers and the reservation covering their decoded allocations.
pub struct GuardedApPage {
    pub records: Vec<VersionedRecord>,
    pub reservation: Box<dyn ReadReservation>,
}

#[cfg(test)]
mod tests;

fn budget_error(error: &ReplError) -> bool {
    matches!(
        error,
        ReplError::Storage(
            StorageError::ResourceLimit(_) | StorageError::Timeout(_) | StorageError::Cancelled(_)
        )
    )
}

fn record_cost(record: &VersionedRecord) -> usize {
    let mut cost = std::mem::size_of::<VersionedRecord>()
        .saturating_mul(2)
        .saturating_add(record.table.len().saturating_mul(2))
        .saturating_add(record.key.len().saturating_mul(2));
    for version in &record.versions {
        cost = cost
            .saturating_add(std::mem::size_of_val(version).saturating_mul(2))
            .saturating_add(version.value.as_ref().map_or(0, Vec::len).saturating_mul(2))
            .saturating_add(version.clock.entries().len().saturating_mul(1_024));
    }
    cost
}

fn reserve_bounds(
    table: &str,
    start: &[u8],
    end: &[u8],
    budget: &dyn ReadBudget,
) -> Result<Box<dyn ReadReservation>, StorageError> {
    budget.reserve(
        table
            .len()
            .saturating_add(start.len())
            .saturating_add(end.len())
            .saturating_add(8)
            .saturating_mul(4)
            .saturating_add(256),
    )
}

impl<S: StorageEngine> ApDynamo<S> {
    pub(super) fn guarded_local_page(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        budget: &dyn ReadBudget,
    ) -> Result<GuardedApPage, ReplError> {
        budget.check()?;
        if !(1..=1_024).contains(&limit) {
            return Err(ReplError::Rejected(
                "AP range page limit must be 1..=1024".to_owned(),
            ));
        }
        let mut reservations = Vec::new();
        // On every error path, records must drop before their reservations.
        let mut records = Vec::new();
        if !end.is_empty() && start >= end {
            return Ok(GuardedApPage {
                records,
                reservation: Box::new(reservations),
            });
        }
        reservations.push(budget.reserve(
            4 * std::mem::size_of::<VersionedRecord>()
                + 4 * std::mem::size_of::<Box<dyn ReadReservation>>()
                + 128,
        )?);
        let _bounds = reserve_bounds(table, start, end, budget)?;
        let (start, end) = version_range_bounds(table, start, end)?;
        let transaction = self.storage.begin_read()?;
        if !transaction.supports_guarded_reads() {
            return Err(ReplError::Unsupported(
                "AP storage does not support guarded reads".to_owned(),
            ));
        }
        transaction.visit_range(AP_VERSIONS_TABLE, &start, &end, budget, &mut |key, raw| {
            // Slice JSON borrows ordinary field names. Admit its actual token
            // workspace before both preflight and owned sibling decoding.
            let _scratch = budget.reserve(value_bound::working_bytes(raw))?;
            let decoded = value_bound::versions_bound(raw, budget.remaining_bytes())?;
            let cost = decoded
                .saturating_add(key.len().saturating_mul(2))
                .saturating_add(std::mem::size_of::<VersionedRecord>().saturating_mul(2))
                .saturating_add(4 * std::mem::size_of::<Box<dyn ReadReservation>>());
            let reservation = budget.reserve(cost)?;
            let (table, key) = decode_version_key(key)?;
            let versions = decode_versions(raw)?;
            records.push(VersionedRecord {
                table,
                key,
                versions,
            });
            reservations.push(reservation);
            Ok(records.len() < limit)
        })?;
        Ok(GuardedApPage {
            records,
            reservation: Box::new(reservations),
        })
    }

    fn guarded_head(
        &self,
        node: NodeId,
        table: &str,
        start: &[u8],
        end: &[u8],
        budget: &dyn ReadBudget,
    ) -> Result<GuardedApPage, ReplError> {
        budget.check()?;
        let page = if node == self.cluster.node_id {
            self.guarded_local_page(table, start, end, 1, budget)?
        } else {
            self.transport_arc()?
                .read_range_page_guarded(node, table, start, end, 1, budget)?
        };
        if page.records.len() > 1
            || page.records.iter().any(|record| {
                record.table != table
                    || record.key.as_slice() < start
                    || (!end.is_empty() && record.key.as_slice() >= end)
            })
        {
            return Err(
                StorageError::Corruption("invalid guarded AP range page".to_owned()).into(),
            );
        }
        Ok(page)
    }

    // Preserve the exact borrowed callback contract used by replication impls.
    #[allow(clippy::type_complexity)]
    pub(super) fn guarded_get(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        budget.check()?;
        let _metadata = budget.reserve(
            self.cluster
                .nodes
                .len()
                .saturating_mul(256)
                .saturating_add(key.len().saturating_mul(4))
                .saturating_add(512),
        )?;
        let mut end = key.to_vec();
        end.push(0);
        if self.is_single_local_node() {
            if !self.is_available(self.cluster.node_id)? {
                return Err(ReplError::NoQuorum);
            }
            let page = self.guarded_head(self.cluster.node_id, table, key, &end, budget)?;
            match page
                .records
                .first()
                .map(|record| record.versions.as_slice())
                .unwrap_or_default()
            {
                [] => {
                    let _ = visitor(None)?;
                }
                [version] => {
                    let _ = visitor(version.value.as_deref())?;
                }
                _ => return Err(ReplError::Conflict),
            }
            return budget.check().map_err(Into::into);
        }
        let required = self
            .read_level_for(consistency)
            .required(self.cluster.replication_factor);
        let mut pages = Vec::new();
        for node in self.preference_list(table, key) {
            budget.check()?;
            if !self.is_available(node)? {
                continue;
            }
            match self.guarded_head(node, table, key, &end, budget) {
                Ok(page) => pages.push((node, page)),
                Err(error) if budget_error(&error) => return Err(error),
                Err(_) => {}
            }
        }
        if pages.len() < required {
            return Err(ReplError::NoQuorum);
        }
        self.guarded_emit(table, key, &pages, budget, &mut |_, value| visitor(value))?;
        budget.check().map_err(Into::into)
    }

    // Keep callback borrows independent of the guard and owned page leases.
    #[allow(clippy::type_complexity)]
    fn guarded_emit(
        &self,
        table: &str,
        key: &[u8],
        heads: &[(NodeId, GuardedApPage)],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(&[u8], Option<&[u8]>) -> Result<bool, ReplError>,
    ) -> Result<bool, ReplError> {
        let _routing = budget.reserve(
            self.cluster
                .nodes
                .len()
                .saturating_mul(128)
                .saturating_add(512),
        )?;
        let replicas = self.preference_list(table, key);
        // Cover reply/merge copies and best-effort repair's write/JSON/wire
        // copies before constructing them. Head leases remain live throughout.
        let scratch = heads
            .iter()
            .filter(|(node, _)| replicas.contains(node))
            .flat_map(|(_, page)| &page.records)
            .filter(|record| record.key == key)
            .fold(512_usize, |sum, record| {
                sum.saturating_add(record_cost(record).saturating_mul(8))
            });
        let _scratch = budget.reserve(scratch)?;
        let replies = heads
            .iter()
            .filter(|(node, _)| replicas.contains(node))
            .map(|(node, page)| {
                (
                    *node,
                    page.records
                        .first()
                        .filter(|record| record.key == key)
                        .map_or_else(Vec::new, |record| record.versions.clone()),
                )
            })
            .collect::<Vec<_>>();
        let merged = merge_reply_versions(&replies);
        if !merged.is_empty() {
            let writes = merged
                .iter()
                .cloned()
                .map(|version| super::VersionedWrite {
                    table: table.to_owned(),
                    key: key.to_vec(),
                    version,
                })
                .collect::<Vec<_>>();
            for (node, versions) in &replies {
                if versions != &merged {
                    if *node == self.cluster.node_id {
                        let _ = self.apply_versioned_writes(&writes);
                    } else if let Ok(transport) = self.transport_arc() {
                        let _ = transport.send_batch_guarded(*node, &writes, budget);
                    }
                }
            }
        }
        match merged.as_slice() {
            [] => visitor(key, None),
            [version] => visitor(key, version.value.as_deref()),
            _ => Err(ReplError::Conflict),
        }
    }

    // Preserve the storage visitor's borrowed preallocation-guard contract.
    #[allow(clippy::type_complexity)]
    fn guarded_local_range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        budget: &dyn ReadBudget,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        let _bounds = reserve_bounds(table, start, end, budget)?;
        let (start, end) = version_range_bounds(table, start, end)?;
        let transaction = self.storage.begin_read()?;
        if !transaction.supports_guarded_reads() {
            return Err(ReplError::Unsupported(
                "AP storage does not support guarded reads".to_owned(),
            ));
        }
        let mut callback_error = None;
        transaction.visit_range(
            AP_VERSIONS_TABLE,
            &start,
            &end,
            budget,
            &mut |encoded_key, raw| {
                let _scratch = budget.reserve(value_bound::working_bytes(raw))?;
                let decoded = value_bound::versions_bound(raw, budget.remaining_bytes())?;
                let _decoded =
                    budget.reserve(decoded.saturating_add(encoded_key.len().saturating_mul(2)))?;
                let (_, key) = decode_version_key(encoded_key)?;
                let versions = decode_versions(raw)?;
                match versions.as_slice() {
                    [] => Ok(true),
                    [version] => match &version.value {
                        None => Ok(true),
                        Some(value) => match visitor(&key, value) {
                            Ok(keep) => Ok(keep),
                            Err(error) => {
                                callback_error = Some(error);
                                Ok(false)
                            }
                        },
                    },
                    _ => Err(StorageError::Conflict),
                }
            },
        )?;
        callback_error.map_or_else(|| budget.check().map_err(Into::into), Err)
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
        if self.is_single_local_node() || !matches!(consistency, ReadConsistency::Strong) {
            if self.is_single_local_node()
                && matches!(consistency, ReadConsistency::Strong)
                && !self.is_available(self.cluster.node_id)?
            {
                return Err(ReplError::NoQuorum);
            }
            return self.guarded_local_range(table, start, end, budget, visitor);
        }
        let _metadata = budget.reserve(
            self.cluster
                .nodes
                .len()
                .saturating_mul(256)
                .saturating_add(512),
        )?;
        let required = self.cluster.nodes.len() - self.cluster.replication_factor
            + self
                .read_level_for(consistency)
                .required(self.cluster.replication_factor);
        let mut heads = Vec::new();
        for node in &self.cluster.nodes {
            budget.check()?;
            if !self.is_available(node.id)? {
                continue;
            }
            match self.guarded_head(node.id, table, start, end, budget) {
                Ok(page) => heads.push((node.id, page)),
                Err(error) if budget_error(&error) => return Err(error),
                Err(_) => {}
            }
        }
        if heads.len() < required {
            return Err(ReplError::NoQuorum);
        }
        loop {
            budget.check()?;
            let Some(key) = heads
                .iter()
                .filter_map(|(_, page)| page.records.first())
                .map(|record| &record.key)
                .min()
            else {
                break;
            };
            let _cursor = budget.reserve(
                key.len()
                    .saturating_add(1)
                    .saturating_mul(4)
                    .saturating_add(128),
            )?;
            let key = key.clone();
            if !self.guarded_emit(table, &key, &heads, budget, &mut |key, value| {
                value.map_or(Ok(true), |value| visitor(key, value))
            })? {
                return Ok(());
            }
            let mut next = key.clone();
            next.push(0);
            for (node, page) in &mut heads {
                if page.records.first().is_some_and(|record| record.key == key) {
                    // Release the previous head before fetching its successor.
                    let old = std::mem::replace(
                        page,
                        GuardedApPage {
                            records: Vec::new(),
                            reservation: Box::new(()),
                        },
                    );
                    drop(old);
                    *page = self.guarded_head(*node, table, &next, end, budget)?;
                }
            }
        }
        budget.check().map_err(Into::into)
    }
}
