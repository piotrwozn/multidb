//! Fresh vector authorization and shared query-governor boundaries.

use std::time::Duration;

use super::{Database, DbError};
use crate::{
    model::Value,
    query::QueryError,
    security::{AuditEvent, AuditOutcome, Permission, Principal, Resource},
    vector::{
        VectorError, VectorHit, VectorSearchOptions,
        execution::{ExecutionLimits, retained_value_bytes},
        validate_search_options,
    },
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
        let limits = ExecutionLimits::new(
            Duration::from_millis(self.performance.query.timeout_ms),
            self.performance.query.memory_limit_bytes,
        )?;
        let check = || limits.check();
        let result = (|| -> Result<VisibleHits, DbError> {
            check()?;
            let policy = self.policy_config()?;
            let vectors = self.vector_collection(name)?.with_execution_limits(limits);
            check()?;
            let hits = vectors.knn(query, k).map_err(vector_execution_error)?;
            check()?;
            let mut visible = Vec::new();
            let mut retained = 0_usize;
            for hit in hits {
                check()?;
                let Some(metadata) = vectors
                    .with_reserved_memory(retained)
                    .map_err(vector_execution_error)?
                    .get_metadata(hit.id)
                    .map_err(vector_execution_error)?
                else {
                    continue;
                };
                if !policy.row_visible(principal, &resource, &metadata) {
                    continue;
                }
                let metadata = policy.mask_value(&resource, &metadata);
                retained = retained
                    .saturating_add(retained_value_bytes(&metadata))
                    .saturating_add(256);
                vectors
                    .check_temporary_memory(retained)
                    .map_err(vector_execution_error)?;
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

pub(super) fn vector_execution_error(error: VectorError) -> DbError {
    match error {
        VectorError::Execution(error) => DbError::Query(error),
        other => DbError::Vector(other),
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

    #[tokio::test]
    async fn graph_sql_special_route_preserves_governor_limits_and_releases_its_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::graph::{GraphId, GraphNodeId};
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_graph("bounded", GraphId::new(17))?;
        database.graph("bounded")?.add_edge(
            GraphNodeId::from_str_id("root"),
            "next",
            GraphNodeId::from_str_id(&"x".repeat(64 * 1024)),
            &Value::Null,
        )?;
        let sql = "SELECT graph_neighbors('bounded', 'root', 'next', 1)";
        database.query_permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        database.performance.query.timeout_ms = 0;
        assert!(matches!(
            database.query(sql).await,
            Err(DbError::Query(QueryError::Timeout(_)))
        ));
        assert_eq!(database.query_permits.available_permits(), 1);
        database.performance.query.timeout_ms = 5_000;
        database.performance.query.memory_limit_bytes = 16 * 1024;
        assert!(matches!(
            database.query(sql).await,
            Err(DbError::Query(QueryError::ResourceLimit(_)))
        ));
        assert_eq!(database.query_permits.available_permits(), 1);
        database.performance.query.memory_limit_bytes = 2 * 1024 * 1024;
        assert!(
            matches!(database.query(sql).await?, crate::query::SqlOutput::Rows(rows) if rows.rows.len() == 1)
        );
        Ok(())
    }

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
