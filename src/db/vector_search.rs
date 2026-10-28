//! Fresh vector authorization and shared query-governor boundaries.

use std::time::{Duration, Instant};

use super::{Database, DbError};
use crate::{
    model::{Value, encode_value},
    query::QueryError,
    security::{AuditEvent, AuditOutcome, Permission, Principal, Resource},
    vector::{VectorHit, VectorSearchOptions, validate_search_options},
};

type VisibleHits = Vec<(VectorHit, Value)>;

impl Database {
    /// Searches a vector collection under current grants, row policies and masks.
    /// A shared query slot remains held until search, metadata and audit work finish.
    /// # Errors
    /// Fails for invalid limits, missing Read, saturation, timeout, or policy/storage errors.
    pub fn search_vectors_as(
        &self,
        principal: &Principal,
        name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<VisibleHits, DbError> {
        validate_search_options(k, &VectorSearchOptions::default())?;
        let resource = Resource::VectorCollection(name.to_owned());
        self.authorize_or_audit(principal, &resource, Permission::Read, "vector_search")?;
        let _permit = self
            .query_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                QueryError::Busy("vector search concurrent query limit reached".to_owned())
            })?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(self.performance.query.timeout_ms))
            .ok_or_else(|| {
                QueryError::ResourceLimit("vector timeout exceeds monotonic clock range".to_owned())
            })?;
        let check = || {
            if Instant::now() >= deadline {
                Err(QueryError::Timeout(
                    "vector search execution deadline exceeded".to_owned(),
                ))
            } else {
                Ok(())
            }
        };
        let result = (|| -> Result<VisibleHits, DbError> {
            check()?;
            let policy = self.policy_config()?;
            let vectors = self.vector_collection(name)?;
            check()?;
            let hits = vectors.knn(query, k)?;
            check()?;
            let mut visible = Vec::new();
            let mut retained = 0_usize;
            for hit in hits {
                check()?;
                let Some(metadata) = vectors.get_metadata(hit.id)? else {
                    continue;
                };
                if !policy.row_visible(principal, &resource, &metadata) {
                    continue;
                }
                let metadata = policy.mask_value(&resource, &metadata);
                retained = retained
                    .saturating_add(encode_value(&metadata)?.len().saturating_mul(4))
                    .saturating_add(256);
                if retained > self.performance.query.memory_limit_bytes {
                    return Err(QueryError::ResourceLimit(
                        "vector metadata result exceeds query memory limit".to_owned(),
                    )
                    .into());
                }
                visible.push((hit, metadata));
            }
            check()?;
            Ok(visible)
        })();
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "vector_search",
            resource,
            if result.is_ok() {
                AuditOutcome::Succeeded
            } else {
                AuditOutcome::Failed
            },
            None,
        ))?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{DbConfig, Profile, create_database},
        model::CollectionId,
        security::{AuthzPolicy, Role},
        vector::{VectorCollectionConfig, VectorMetric},
    };

    #[test]
    fn vector_search_shares_governor_and_zero_deadline_fails_before_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_vector_collection(
            "vectors",
            VectorCollectionConfig::new(CollectionId::new(1), 2).with_metric(VectorMetric::L2),
        )?;
        database
            .vector_collection("vectors")?
            .insert_vector(&Value::Int(7), vec![0.0, 0.0])?;
        database.set_authz_policy(AuthzPolicy::new([Role::new("reader").grant(
            Resource::VectorCollection("vectors".to_owned()),
            Permission::Read,
        )]));
        let reader = Principal::new("alice").with_role("reader");
        database.query_permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let occupied = database.query_permits.clone().try_acquire_owned()?;
        assert!(matches!(
            database.search_vectors_as(&Principal::new("denied"), "vectors", &[0.0, 0.0], 1),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.search_vectors_as(&reader, "vectors", &[0.0, 0.0], 1),
            Err(DbError::Query(QueryError::Busy(_)))
        ));
        drop(occupied);
        assert_eq!(
            database
                .search_vectors_as(&reader, "vectors", &[0.0, 0.0], 1)?
                .len(),
            1
        );
        database.performance.query.timeout_ms = 0;
        assert!(matches!(
            database.search_vectors_as(&reader, "missing", &[0.0, 0.0], 1),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.search_vectors_as(&reader, "vectors", &[0.0, 0.0], 1),
            Err(DbError::Query(QueryError::Timeout(_)))
        ));
        assert_eq!(database.query_permits.available_permits(), 1);
        Ok(())
    }
}
