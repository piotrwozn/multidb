//! Database authorization and policy boundaries for bounded graph-assisted retrieval.

use std::time::Duration;

use super::{ConfigError, Database, DbError, policy_has_row_or_masking};
use crate::{
    graph::GraphNodeId,
    graphrag::{self, GraphRagError, GraphRagRequest, GraphRagResult},
    model::Value,
    query::QueryError,
    security::{AuditEvent, AuditOutcome, Permission, Principal, Resource},
    vector::{VectorError, execution::ExecutionLimits},
};

impl Database {
    /// Retrieves vector seeds and graph context for trusted embedded/operator callers.
    /// No RBAC or row/masking policies are applied; use `graph_rag_as` for user requests.
    /// # Errors
    /// Fails when the request is invalid, catalog objects are missing, or retrieval fails.
    pub fn graph_rag(&self, request: &GraphRagRequest) -> Result<GraphRagResult, DbError> {
        request.validate()?;
        self.ensure_graph_rag_backend()?;
        let _permit = self.try_graph_rag_permit()?;
        let limits = self.graph_rag_limits()?;
        limits.check()?;
        let vectors = self
            .vector_collection(&request.vector_collection)?
            .with_execution_limits(limits);
        let graph = self.graph(&request.graph)?.with_execution_limits(limits);
        graphrag::retrieve(&vectors, &graph, request, |_, metadata| Ok(Some(metadata)))
            .map_err(graph_rag_execution_error)
    }

    /// Retrieves graph context after authorizing both the vector collection and graph.
    /// Vector metadata is filtered and masked before it can become context or a traversal
    /// intermediate. Graph row/masking policies are unsupported and fail closed.
    /// # Errors
    /// Fails when either Read grant is missing, policies reject the surface, or retrieval fails.
    pub fn graph_rag_as(
        &self,
        principal: &Principal,
        request: &GraphRagRequest,
    ) -> Result<GraphRagResult, DbError> {
        request.validate()?;
        let vectors_resource = Resource::VectorCollection(request.vector_collection.clone());
        let graph_resource = Resource::Graph(request.graph.clone());
        // Both checks precede catalog lookup and vector metadata/edge access.
        self.authorize_or_audit(principal, &vectors_resource, Permission::Read, "graph_rag")?;
        self.authorize_or_audit(principal, &graph_resource, Permission::Read, "graph_rag")?;
        self.ensure_graph_rag_backend()?;
        let _permit = self.try_graph_rag_permit()?;
        let limits = self.graph_rag_limits()?;
        let policy = self.policy_config()?;
        let result: Result<GraphRagResult, DbError> =
            if policy_has_row_or_masking(&policy, &graph_resource) {
                Err(GraphRagError::Policy(
                    "GraphRAG does not support row or masking policies on graph edges".to_owned(),
                )
                .into())
            } else {
                (|| {
                    limits.check()?;
                    let vectors = self
                        .vector_collection(&request.vector_collection)?
                        .with_execution_limits(limits);
                    let graph = self.graph(&request.graph)?.with_execution_limits(limits);
                    graphrag::retrieve(&vectors, &graph, request, |_, metadata| {
                        if !policy.row_visible(principal, &vectors_resource, &metadata) {
                            return Ok(None);
                        }
                        Ok(Some(policy.mask_value(&vectors_resource, &metadata)))
                    })
                    .map_err(graph_rag_execution_error)
                })()
            };
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "graph_rag",
            graph_resource,
            if result.is_ok() {
                AuditOutcome::Succeeded
            } else {
                AuditOutcome::Failed
            },
            None,
        ))?;
        result
    }

    fn ensure_graph_rag_backend(&self) -> Result<(), DbError> {
        if self.repl.is_ap()
            || self.repl.is_sharded()
            || !self.replication_ref().supports_bounded_scans()
        {
            return Err(ConfigError::Unsupported(
                "GraphRAG requires bounded storage scans; AP and sharded backends are outside this preview"
                    .to_owned(),
            )
            .into());
        }
        Ok(())
    }

    fn try_graph_rag_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
        self.query_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryError::Busy("GraphRAG concurrent query limit reached".to_owned()))
    }

    fn graph_rag_limits(&self) -> Result<ExecutionLimits, QueryError> {
        ExecutionLimits::new(
            Duration::from_millis(self.performance.query.timeout_ms),
            self.performance.query.memory_limit_bytes,
        )
    }

    /// Adds or replaces a graph edge after checking Write on the named graph.
    /// The authorized attempt is audited before graph lookup and before the user write.
    /// The graph handle routes the write through triggers, hooks and tenant quotas.
    /// # Errors
    /// Fails when permission/audit fails, the graph is missing, or the write is rejected.
    pub fn add_graph_edge_as(
        &self,
        principal: &Principal,
        name: &str,
        source: GraphNodeId,
        label: &str,
        target: GraphNodeId,
        properties: &Value,
    ) -> Result<(), DbError> {
        let resource = Resource::Graph(name.to_owned());
        self.authorize_or_audit(principal, &resource, Permission::Write, "add_graph_edge")?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "add_graph_edge",
            resource,
            AuditOutcome::Allowed,
            None,
        ))?;
        self.graph(name)?
            .add_edge(source, label, target, properties)?;
        Ok(())
    }
}

fn graph_rag_execution_error(error: GraphRagError) -> DbError {
    match error {
        GraphRagError::ConcurrentMutation => DbError::Repl(crate::repl::ReplError::Conflict),
        GraphRagError::Vector(VectorError::Execution(error))
        | GraphRagError::Graph(crate::graph::GraphError::Execution(error)) => DbError::Query(error),
        other => DbError::GraphRag(other),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        db::{
            ConfigError, Database, DbConfig, DbError, Profile, ReplicationKind, create_ap_database,
            create_database, open_database,
        },
        extension::{MaskingPolicy, POLICIES_TABLE, PolicyConfig, RowPolicy},
        graph::{GraphId, GraphNodeId},
        graphrag::{GraphRagError, GraphRagRequest, document_node_id},
        model::{CollectionId, DocumentId, FieldPath, Value},
        query::QueryError,
        repl::{ApClusterConfig, Op, propose_system},
        security::{
            AuditOutcome, AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role,
        },
        vector::{VectorCollectionConfig, VectorMetric},
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn graph_rag_honors_the_model_deadline_and_memory_governor() -> TestResult {
        let (mut database, _) = fixture()?;
        database.performance.query.timeout_ms = 0;
        assert!(matches!(
            database.graph_rag_as(&reader(), &request()),
            Err(DbError::Query(QueryError::Timeout(_)))
        ));
        database.performance.query.timeout_ms = 5_000;
        database.performance.query.memory_limit_bytes = 1_024;
        assert!(matches!(
            database.graph_rag_as(&reader(), &request()),
            Err(DbError::Query(QueryError::ResourceLimit(_)))
        ));
        assert!(database.query_permits.available_permits() > 0);
        assert!(
            database
                .audit_events()?
                .iter()
                .any(|event| event.action == "graph_rag" && event.outcome == AuditOutcome::Failed)
        );
        Ok(())
    }

    fn authz() -> AuthzPolicy {
        AuthzPolicy::new([
            Role::new("reader")
                .grant(
                    Resource::VectorCollection("knowledge".to_owned()),
                    Permission::Read,
                )
                .grant(Resource::Graph("relations".to_owned()), Permission::Read),
            Role::new("vector_only").grant(
                Resource::VectorCollection("knowledge".to_owned()),
                Permission::Read,
            ),
            Role::new("graph_only")
                .grant(Resource::Graph("relations".to_owned()), Permission::Read),
            Role::new("writer").grant(Resource::Graph("relations".to_owned()), Permission::Write),
            Role::new("operator").grant(Resource::System, Permission::Admin),
        ])
    }

    fn reader() -> Principal {
        Principal::new("alice").with_role("reader")
    }

    fn metadata(text: &str, tenant: &str, source: &str) -> Value {
        Value::Object(BTreeMap::from([
            ("text".to_owned(), Value::Str(text.to_owned())),
            ("tenant".to_owned(), Value::Str(tenant.to_owned())),
            ("source".to_owned(), Value::Str(source.to_owned())),
        ]))
    }

    fn seed(database: &mut Database) -> Result<[DocumentId; 3], Box<dyn std::error::Error>> {
        database.create_vector_collection(
            "knowledge",
            VectorCollectionConfig::new(CollectionId::new(51), 2).with_metric(VectorMetric::L2),
        )?;
        database.create_graph("relations", GraphId::new(52))?;
        let vectors = database.vector_collection("knowledge")?;
        let seed = vectors.insert_vector(
            &metadata("seed document", "alice", "private-seed.txt"),
            vec![0.0, 0.0],
        )?;
        let bridge = vectors.insert_vector(
            &metadata("hidden bridge", "bob", "private-bridge.txt"),
            vec![10.0, 0.0],
        )?;
        let target = vectors.insert_vector(
            &metadata("reachable target", "alice", "private-target.txt"),
            vec![20.0, 0.0],
        )?;
        database.graph("relations")?.add_edge(
            document_node_id(seed),
            "related",
            document_node_id(bridge),
            &Value::Null,
        )?;
        database.graph("relations")?.add_edge(
            document_node_id(bridge),
            "related",
            document_node_id(target),
            &Value::Null,
        )?;
        database.set_authz_policy(authz());
        Ok([seed, bridge, target])
    }

    fn fixture() -> Result<(Database, [DocumentId; 3]), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let ids = seed(&mut database)?;
        Ok((database, ids))
    }

    fn request() -> GraphRagRequest {
        let mut request = GraphRagRequest::new("knowledge", "relations", vec![0.0, 0.0]);
        request.seed_k = 1;
        request
    }

    fn hex_id(id: DocumentId) -> String {
        // The graph helper is the canonical boundary shared by retrieval and edge writes.
        String::from_utf8_lossy(document_node_id(id).as_bytes()).into_owned()
    }

    #[test]
    fn graph_rag_requires_read_on_both_objects_before_catalog_lookup() -> TestResult {
        let (database, _) = fixture()?;
        for role in ["vector_only", "graph_only", "ungranted"] {
            let principal = Principal::new("alice").with_role(role);
            assert!(matches!(
                database.graph_rag_as(&principal, &request()),
                Err(DbError::AuthzDenied(_))
            ));
        }
        let mut missing = request();
        missing.vector_collection = "does_not_exist".to_owned();
        assert!(matches!(
            database.graph_rag_as(&reader(), &missing),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(
            database.audit_events()?.iter().any(|event| {
                event.action == "graph_rag" && event.outcome == AuditOutcome::Denied
            })
        );
        Ok(())
    }

    #[test]
    fn graph_rag_does_not_traverse_row_hidden_bridges() -> TestResult {
        let (database, ids) = fixture()?;
        database.set_policy_config(&PolicyConfig {
            row_policies: vec![RowPolicy {
                resource: Resource::VectorCollection("knowledge".to_owned()),
                path: FieldPath::new(["tenant"]),
                equals: Value::Str("public".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
        let result = database.graph_rag_as(&reader(), &request())?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].document_id, hex_id(ids[0]));
        assert!(!result.context.contains("hidden bridge"));
        assert!(!result.context.contains("reachable target"));
        assert_eq!(result.relationships, Vec::new());
        assert_eq!(database.graph_rag(&request())?.sources.len(), 3);
        Ok(())
    }

    #[test]
    fn graph_rag_filters_hidden_seed_metadata_before_text_extraction() -> TestResult {
        let (database, ids) = fixture()?;
        database.set_policy_config(&PolicyConfig {
            row_policies: vec![RowPolicy {
                resource: Resource::VectorCollection("knowledge".to_owned()),
                path: FieldPath::new(["tenant"]),
                equals: Value::Str("bob".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
        let mut query = request();
        query.max_depth = 0;
        let principal = Principal::new("carol").with_role("reader");
        let result = database.graph_rag_as(&principal, &query)?;
        assert!(
            result
                .sources
                .iter()
                .all(|source| source.document_id != hex_id(ids[0]))
        );
        assert!(!result.context.contains("seed document"));
        Ok(())
    }

    #[test]
    fn graph_rag_masks_text_and_source_before_context_and_citations() -> TestResult {
        let (database, _) = fixture()?;
        database.set_policy_config(&PolicyConfig {
            masking: vec![MaskingPolicy {
                resource: Resource::VectorCollection("knowledge".to_owned()),
                paths: vec![FieldPath::new(["text"]), FieldPath::new(["source"])],
                replacement: Value::Str("redacted".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
        let result = database.graph_rag_as(&reader(), &request())?;
        assert_ne!(result.sources, Vec::new());
        assert!(
            result
                .sources
                .iter()
                .all(|source| source.text == "redacted")
        );
        let serialized = serde_json::to_string(&result)?;
        for secret in [
            "seed document",
            "hidden bridge",
            "reachable target",
            "private-seed.txt",
        ] {
            assert!(!serialized.contains(secret), "leaked {secret}");
        }
        assert!(
            database
                .graph_rag(&request())?
                .context
                .contains("seed document")
        );
        Ok(())
    }

    #[test]
    fn graph_rag_refuses_graph_policies_and_corrupt_policy_metadata() -> TestResult {
        let (database, _) = fixture()?;
        for policy in [
            PolicyConfig {
                row_policies: vec![RowPolicy {
                    resource: Resource::Graph("relations".to_owned()),
                    path: FieldPath::new(["tenant"]),
                    equals: Value::Str("alice".to_owned()),
                }],
                ..PolicyConfig::default()
            },
            PolicyConfig {
                masking: vec![MaskingPolicy {
                    resource: Resource::Graph("relations".to_owned()),
                    paths: vec![FieldPath::new(["secret"])],
                    replacement: Value::Null,
                }],
                ..PolicyConfig::default()
            },
        ] {
            database.set_policy_config(&policy)?;
            assert!(matches!(
                database.graph_rag_as(&reader(), &request()),
                Err(DbError::GraphRag(GraphRagError::Policy(_)))
            ));
        }
        propose_system(
            &database,
            Op::Put {
                table: POLICIES_TABLE.to_owned(),
                key: b"policy".to_vec(),
                value: b"invalid policy metadata".to_vec(),
            },
        )?;
        assert!(database.graph_rag_as(&reader(), &request()).is_err());
        // Trusted retrieval is explicitly outside the principal policy surface.
        assert!(database.graph_rag(&request()).is_ok());
        Ok(())
    }

    #[test]
    fn graph_rag_rejects_unbounded_backends_before_catalog_or_knn() -> TestResult {
        let mut database = create_ap_database(
            DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
            ApClusterConfig::single_node_for_tests(1),
        )?;
        database.set_authz_policy(authz());
        assert!(matches!(
            database.graph_rag(&request()),
            Err(DbError::Config(ConfigError::Unsupported(_)))
        ));
        assert!(matches!(
            database.graph_rag_as(&reader(), &request()),
            Err(DbError::Config(ConfigError::Unsupported(_)))
        ));
        assert!(matches!(
            database.graph_rag_as(&Principal::new("nobody"), &request()),
            Err(DbError::AuthzDenied(_))
        ));
        Ok(())
    }

    #[test]
    fn graph_rag_shares_query_governor_and_releases_permits_on_all_paths() -> TestResult {
        let (mut database, _) = fixture()?;
        database.set_query_governor(1, 100)?;
        let slot = database.query_permits.clone().try_acquire_owned()?;
        assert!(matches!(
            database.graph_rag(&request()),
            Err(DbError::Query(QueryError::Busy(_)))
        ));
        assert!(matches!(
            database.graph_rag_as(&reader(), &request()),
            Err(DbError::Query(QueryError::Busy(_)))
        ));
        assert!(matches!(
            database.graph_rag_as(&Principal::new("ungranted"), &request()),
            Err(DbError::AuthzDenied(_))
        ));
        drop(slot);
        assert!(database.graph_rag(&request()).is_ok());
        assert_eq!(database.query_permits.available_permits(), 1);
        assert!(database.graph_rag_as(&reader(), &request()).is_ok());
        assert_eq!(database.query_permits.available_permits(), 1);
        let mut wrong_dimension = request();
        wrong_dimension.query_vector = vec![0.0];
        assert!(database.graph_rag_as(&reader(), &wrong_dimension).is_err());
        assert_eq!(database.query_permits.available_permits(), 1);
        Ok(())
    }

    #[test]
    fn graph_rag_rechecks_roles_and_registry_mapping_without_cached_authorization() -> TestResult {
        let (mut database, _) = fixture()?;
        database
            .set_principal_registry(PrincipalRegistry::new().with_principal("pg-alice", reader()));
        assert!(
            database
                .graph_rag_as(&database.principal_for_user("pg-alice"), &request())
                .is_ok()
        );
        database.set_principal_registry(PrincipalRegistry::new());
        assert!(matches!(
            database.graph_rag_as(&database.principal_for_user("pg-alice"), &request()),
            Err(DbError::AuthzDenied(_))
        ));
        database.set_authz_policy(AuthzPolicy::default());
        assert!(matches!(
            database.graph_rag_as(&reader(), &request()),
            Err(DbError::AuthzDenied(_))
        ));
        Ok(())
    }

    #[test]
    fn graph_edge_write_requires_graph_write_and_audits_before_write() -> TestResult {
        let (database, ids) = fixture()?;
        let source = document_node_id(ids[0]);
        let target = document_node_id(ids[2]);
        assert!(matches!(
            database.add_graph_edge_as(
                &reader(),
                "relations",
                source.clone(),
                "cites",
                target.clone(),
                &Value::Null
            ),
            Err(DbError::AuthzDenied(_))
        ));
        assert_eq!(
            database.graph("relations")?.neighbors(&source, "cites")?,
            Vec::new()
        );
        database.add_graph_edge_as(
            &Principal::new("writer").with_role("writer"),
            "relations",
            source.clone(),
            "cites",
            target.clone(),
            &Value::Null,
        )?;
        let edges = database.graph("relations")?.neighbors(&source, "cites")?;
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst, target);
        assert!(database.audit_events()?.iter().any(
            |event| event.action == "add_graph_edge" && event.outcome == AuditOutcome::Allowed
        ));
        // Generic edge writes keep supporting graph nodes unrelated to vector document ids.
        database.add_graph_edge_as(
            &Principal::new("writer").with_role("writer"),
            "relations",
            GraphNodeId::from_str_id("article"),
            "links",
            GraphNodeId::from_str_id("topic"),
            &Value::Null,
        )?;
        Ok(())
    }

    #[test]
    fn graph_rag_policies_and_graph_relationships_survive_reopen() -> TestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("knowledge.redb");
        let ids;
        {
            let mut database = create_database(DbConfig::on_disk(Profile::Transactional, &path))?;
            ids = seed(&mut database)?;
            database
                .set_authz_policy_as(&Principal::new("operator").with_role("operator"), authz())?;
            database.set_principal_registry_as(
                &Principal::new("operator").with_role("operator"),
                PrincipalRegistry::new().with_principal("pg-alice", reader()),
            )?;
            database.set_policy_config(&PolicyConfig {
                row_policies: vec![RowPolicy {
                    resource: Resource::VectorCollection("knowledge".to_owned()),
                    path: FieldPath::new(["tenant"]),
                    equals: Value::Str("public".to_owned()),
                }],
                masking: vec![MaskingPolicy {
                    resource: Resource::VectorCollection("knowledge".to_owned()),
                    paths: vec![FieldPath::new(["source"])],
                    replacement: Value::Str("redacted".to_owned()),
                }],
                ..PolicyConfig::default()
            })?;
        }
        let database = open_database(DbConfig::on_disk(Profile::Transactional, &path))?;
        let result = database.graph_rag_as(&database.principal_for_user("pg-alice"), &request())?;
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].document_id, hex_id(ids[0]));
        assert!(!serde_json::to_string(&result)?.contains("private-seed.txt"));
        assert_eq!(database.graph_rag(&request())?.sources.len(), 3);
        Ok(())
    }
}
