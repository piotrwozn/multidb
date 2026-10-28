//! Immutable SQL catalog templates. Authorization and statistics remain request-local.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use super::{Database, DbError, Profile};
use crate::{
    performance::PerformanceConfig,
    query::{QueryError, REL_SCHEMA_TABLE, SqlEngine},
    repl::Op,
    storage::StorageError,
};

struct CachedCatalog {
    epoch: u64,
    profile: Profile,
    performance: PerformanceConfig,
    engine: Arc<SqlEngine>,
}

#[derive(Default)]
pub(super) struct SqlCatalogCache {
    pub(super) schema_epoch: Arc<AtomicU64>,
    engine: Mutex<Option<CachedCatalog>>,
}

impl SqlCatalogCache {
    pub(super) fn invalidate(&self) {
        self.schema_epoch.fetch_add(1, Ordering::AcqRel);
    }
}

pub(super) fn schema_changed(ops: &[Op]) -> bool {
    ops.iter().any(|op| match op {
        Op::Put { table, .. } | Op::Delete { table, .. } => table == REL_SCHEMA_TABLE,
    })
}

impl Database {
    pub(super) fn sql_engine(&self) -> Result<Arc<SqlEngine>, DbError> {
        let mut cache = self.sql_catalog_cache.engine.lock().map_err(|_| {
            QueryError::Storage(StorageError::Backend(
                "SQL catalog cache lock poisoned".to_owned(),
            ))
        })?;
        for _ in 0..3 {
            let epoch = self.sql_catalog_cache.schema_epoch.load(Ordering::Acquire);
            if let Some(cached) = cache.as_ref()
                && cached.epoch == epoch
                && cached.profile == self.profile()
                && cached.performance == self.performance
            {
                return Ok(Arc::clone(&cached.engine));
            }
            let engine = Arc::new(self.build_sql_engine()?);
            if self.sql_catalog_cache.schema_epoch.load(Ordering::Acquire) == epoch {
                *cache = Some(CachedCatalog {
                    epoch,
                    profile: self.profile(),
                    performance: self.performance.clone(),
                    engine: Arc::clone(&engine),
                });
                return Ok(engine);
            }
        }
        // Concurrent trusted schema writers must not publish a stale template.
        Err(QueryError::Storage(StorageError::Conflict).into())
    }
}

#[cfg(test)]
mod tests;
