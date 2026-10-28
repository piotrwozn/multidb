//! Bounded, versioned raw Arrow batches shared by every provider in one SQL engine.
//! Authorization, policy wrappers, snapshots and filtered scans never enter this cache.

use std::{collections::HashMap, mem::size_of, sync::Mutex};

use super::{QueryError, REL_ROWS_TABLE, RecordBatch, RelTable, SchemaRef};
use crate::performance::PerformanceConfig;

const MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    table: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    batch_rows: usize,
    version: u64,
}

struct Entry {
    batches: Vec<RecordBatch>,
    bytes: usize,
    last_used: u64,
}

#[derive(Default)]
struct Entries {
    values: HashMap<Key, Entry>,
    bytes: usize,
    clock: u64,
}

pub(super) struct BatchCache {
    max_bytes: usize,
    max_entries: usize,
    entries: Mutex<Entries>,
}

impl BatchCache {
    pub(super) fn new(performance: &PerformanceConfig) -> Self {
        Self {
            max_bytes: if performance.cache.enabled {
                MAX_BYTES.min(performance.query.memory_limit_bytes / 8)
            } else {
                0
            },
            max_entries: usize::try_from(performance.cache.max_capacity).unwrap_or(usize::MAX),
            entries: Mutex::new(Entries::default()),
        }
    }

    /// Caller admits only current, unfiltered, unlimited row-store scans. Tokens are
    /// read again for every lookup; a concurrent write can never publish a mixed fill.
    pub(super) fn batches(
        &self,
        table: &RelTable,
        schema: &SchemaRef,
        projection: Option<&[usize]>,
        batch_rows: usize,
        budget: &super::budget::Budget,
        fill: impl FnOnce() -> Result<Vec<RecordBatch>, QueryError>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        if self.max_bytes == 0 || self.max_entries == 0 {
            return fill();
        }
        let Some(version) = table.repl.table_data_version(REL_ROWS_TABLE)? else {
            return fill();
        };
        let key = Key {
            table: table.name.clone(),
            schema: schema.clone(),
            projection: projection.map(<[usize]>::to_vec),
            batch_rows,
            version,
        };
        if let Some(batches) = self.get(&key, Some(budget))? {
            return Ok(batches);
        }
        let batches = fill()?;
        if table.repl.table_data_version(REL_ROWS_TABLE)? == Some(version) {
            self.insert(key, &batches);
        }
        Ok(batches)
    }

    fn get(
        &self,
        key: &Key,
        budget: Option<&super::budget::Budget>,
    ) -> Result<Option<Vec<RecordBatch>>, QueryError> {
        // Charge before cloning all per-batch Vec/Arc metadata. The data buffers
        // themselves remain shared, but a tiny-budget hit must not first clone
        // an entire large entry and only then discover that it cannot be admitted.
        let Ok(mut entries) = self.entries.lock() else {
            return Ok(None);
        };
        entries.clock = entries.clock.saturating_add(1);
        let clock = entries.clock;
        let Some(entry) = entries.values.get_mut(key) else {
            return Ok(None);
        };
        if let Some(budget) = budget {
            for batch in &entry.batches {
                budget.charge_cached_source(batch)?;
            }
        }
        entry.last_used = clock;
        Ok(Some(entry.batches.clone()))
    }

    fn insert(&self, key: Key, batches: &[RecordBatch]) {
        let bytes = weight(&key, batches);
        if bytes > self.max_bytes || self.max_entries == 0 {
            return;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if let Some(previous) = entries.values.remove(&key) {
            entries.bytes -= previous.bytes;
        }
        while entries.values.len() >= self.max_entries || entries.bytes > self.max_bytes - bytes {
            let Some(oldest) = entries
                .values
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(previous) = entries.values.remove(&oldest) {
                entries.bytes -= previous.bytes;
            }
        }
        entries.clock = entries.clock.saturating_add(1);
        let last_used = entries.clock;
        entries.values.insert(
            key,
            Entry {
                batches: batches.to_vec(),
                bytes,
                last_used,
            },
        );
        entries.bytes += bytes;
    }
}

fn schema_bytes(schema: &SchemaRef) -> usize {
    let fields = schema
        .fields()
        .iter()
        .fold(0_usize, |bytes, field| bytes.saturating_add(field.size()));
    schema.metadata().iter().fold(
        size_of::<datafusion::arrow::datatypes::Schema>().saturating_add(fields),
        |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(value.capacity())
        },
    )
}

fn weight(key: &Key, batches: &[RecordBatch]) -> usize {
    // Conservative structural/hash-bucket overhead also charges zero-column COUNT
    // batches, which retain row counts even though they have no Arrow arrays.
    let overhead = 4 * (size_of::<Key>() + size_of::<Entry>()) + key.table.capacity();
    let projection = key.projection.as_ref().map_or(0, |value| {
        value.capacity().saturating_mul(size_of::<usize>())
    });
    batches.iter().fold(
        overhead
            .saturating_add(projection)
            .saturating_add(schema_bytes(&key.schema)),
        |bytes, batch| {
            bytes
                .saturating_add(size_of::<RecordBatch>())
                .saturating_add(batch.get_array_memory_size())
                .saturating_add(schema_bytes(&batch.schema()))
        },
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod limits_tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn fixture(table: &str) -> Result<(Key, Vec<RecordBatch>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64; 128]))],
        )?;
        Ok((
            Key {
                table: table.to_owned(),
                schema,
                projection: None,
                batch_rows: 128,
                version: 1,
            },
            vec![batch],
        ))
    }

    #[test]
    fn all_tables_share_one_weighted_lru_budget() -> Result<(), Box<dyn std::error::Error>> {
        let (first, batches) = fixture("a")?;
        let (second, _) = fixture("b")?;
        let (third, _) = fixture("c")?;
        let max_bytes = weight(&first, &batches) * 2;
        let cache = BatchCache {
            max_bytes,
            max_entries: 10,
            entries: Mutex::new(Entries::default()),
        };
        cache.insert(first.clone(), &batches);
        cache.insert(second.clone(), &batches);
        assert!(cache.get(&first, None)?.is_some());
        cache.insert(third.clone(), &batches);
        assert!(cache.get(&second, None)?.is_none());
        assert!(cache.get(&first, None)?.is_some());
        assert!(cache.get(&third, None)?.is_some());
        let entries = cache.entries.lock().map_err(|_| "cache poisoned")?;
        assert_eq!(entries.values.len(), 2);
        assert!(entries.bytes <= max_bytes);
        Ok(())
    }

    #[test]
    fn oversized_entries_and_projection_aliases_are_not_admitted()
    -> Result<(), Box<dyn std::error::Error>> {
        let (key, batches) = fixture("a")?;
        let cache = BatchCache {
            max_bytes: weight(&key, &batches) - 1,
            max_entries: 10,
            entries: Mutex::new(Entries::default()),
        };
        cache.insert(key.clone(), &batches);
        assert!(cache.get(&key, None)?.is_none());
        let cache = BatchCache::new(&PerformanceConfig::default());
        cache.insert(key.clone(), &batches);
        let mut projected = key.clone();
        projected.projection = Some(Vec::new());
        assert!(cache.get(&projected, None)?.is_none());
        projected = key;
        projected.batch_rows += 1;
        assert!(cache.get(&projected, None)?.is_none());
        projected.batch_rows -= 1;
        projected.schema = Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int64,
            false,
        )]));
        assert!(cache.get(&projected, None)?.is_none());
        Ok(())
    }

    #[test]
    fn configured_entry_count_is_shared_across_tables() -> Result<(), Box<dyn std::error::Error>> {
        let (first, batches) = fixture("a")?;
        let (second, _) = fixture("b")?;
        let cache = BatchCache {
            max_bytes: MAX_BYTES,
            max_entries: 1,
            entries: Mutex::new(Entries::default()),
        };
        cache.insert(first.clone(), &batches);
        cache.insert(second.clone(), &batches);
        assert!(cache.get(&first, None)?.is_none());
        assert!(cache.get(&second, None)?.is_some());
        Ok(())
    }

    #[test]
    fn configuration_disables_or_bounds_the_shared_cache() {
        let mut performance = PerformanceConfig::default();
        performance.cache.enabled = false;
        assert_eq!(BatchCache::new(&performance).max_bytes, 0);
        performance.cache.enabled = true;
        performance.query.memory_limit_bytes = 8_000;
        assert_eq!(BatchCache::new(&performance).max_bytes, 1_000);
        performance.query.memory_limit_bytes = usize::MAX;
        assert_eq!(BatchCache::new(&performance).max_bytes, MAX_BYTES);
        performance.cache.max_capacity = 0;
        assert_eq!(BatchCache::new(&performance).max_entries, 0);
    }
}
