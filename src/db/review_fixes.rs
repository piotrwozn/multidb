//! Authorization shared by metadata-only SQL planning and derived relations.

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use super::{
    Arc, BTreeMap, BTreeSet, CatalogEntry, Database, DbError, PolicyConfig, Principal, QueryError,
    SqlOutput, TemporalError, TuningSystemView, Value, compat, extension, parse_as_of_query,
    parse_call_procedure_sql, parse_phase19_call, phase32_unsupported, policy_has_row_or_masking,
    resource_for_catalog_name, sql_requirements, tuning,
};

impl Database {
    /// Plans SQL metadata under the same authorization and policies as execution.
    /// Planning never executes the statement or evaluates user expressions.
    pub(crate) async fn describe_query_as(
        &self,
        principal: &Principal,
        sql: &str,
    ) -> Result<Option<SchemaRef>, DbError> {
        let requirements = sql_requirements(sql, &self.catalog, &self.performance.query)?;
        for (resource, permission) in &requirements {
            self.authorize_or_audit(principal, resource, *permission, "describe_query")?;
        }
        let mut guard = self.sql_read_authorizer(principal, "describe_query", &requirements)?;
        if let Some((rewritten, point)) = parse_as_of_query(sql)? {
            if self.repl.is_ap() || self.repl.is_sharded() {
                return Err(TemporalError::Unsupported(
                    "temporal AS OF requires single-node commit-log storage".to_owned(),
                )
                .into());
            }
            let lsn = crate::query::resolve_temporal_point_with_budget(
                self.replication_ref(),
                point,
                &self.performance.query,
            )?;
            self.validate_temporal_retention_for_sql(&rewritten, lsn)?;
            let _pin = self.pin_history_lsn(lsn)?;
            return Ok(self
                .sql_engine()?
                .as_ref()
                .clone()
                .with_snapshot_lsn(lsn)
                .describe_with_read_guard(&rewritten, &mut guard)
                .await?);
        }
        // The statements execution runs itself are described in the order it
        // dispatches them, without running any of them. A procedure's result
        // depends on the module it runs, so `CALL` is described as returning
        // no rows, like `CREATE FUNCTION`.
        if parse_call_procedure_sql(sql)?.is_some()
            || extension::parse_create_wasm_function(sql)?.is_some()
        {
            return Ok(None);
        }
        if let Some(kind) = super::phase32_declaration_kind(sql) {
            return Err(phase32_unsupported(kind));
        }
        // Compatibility statements have fixed engine-owned results. They do not
        // evaluate user SQL or read user tables.
        if let Some(output) = compat::execute_compat_sql(
            sql,
            &self.config,
            &self.compat_catalog_snapshot(),
            &self.performance.query,
        )? {
            return Ok(match output {
                SqlOutput::AffectedRows(_) => None,
                SqlOutput::Rows(rows) => Some(Arc::new(Schema::new(
                    rows.columns
                        .iter()
                        .enumerate()
                        .map(|(index, name)| {
                            let data_type = rows
                                .rows
                                .iter()
                                .filter_map(|row| row.get(index))
                                .find_map(|value| match value {
                                    Value::Null => None,
                                    Value::Bool(_) => Some(DataType::Boolean),
                                    Value::Int(_) => Some(DataType::Int64),
                                    Value::Float(_) => Some(DataType::Float64),
                                    Value::Bytes(_) => Some(DataType::Binary),
                                    _ => Some(DataType::Utf8),
                                })
                                .unwrap_or(DataType::Utf8);
                            Field::new(name, data_type, true)
                        })
                        .collect::<Vec<_>>(),
                ))),
            });
        }
        if let Some(view) = tuning::parse_system_view(sql)? {
            return Ok(Some(system_view_schema(view)));
        }
        if let Some(schema) = parse_phase19_call(sql)?.and_then(|call| phase19_schema(&call.name)) {
            return Ok(Some(schema));
        }
        if extension::parse_select_udf_call(sql)?.is_some() {
            return Err(QueryError::Unsupported(
                "a WASM UDF call cannot be described without running it; use the simple query protocol"
                    .to_owned(),
            )
            .into());
        }
        Ok(self
            .sql_engine()?
            .describe_with_read_guard(sql, &mut guard)
            .await?)
    }

    /// Sweeps one bounded page of expired time-series chunks and latest values.
    /// # Errors
    /// Fails when retention metadata is invalid or storage rejects maintenance.
    pub fn sweep_time_series_retention(
        &self,
        cursor: Option<crate::timeseries::RetentionCursor>,
        batch_rows: usize,
    ) -> Result<crate::timeseries::RetentionReport, DbError> {
        let configs = self
            .catalog
            .iter()
            .filter_map(|(name, entry)| match entry {
                CatalogEntry::TimeSeries { config } => Some((name.clone(), config.clone())),
                _ => None,
            })
            .collect();
        Ok(crate::timeseries::sweep_retention(
            self.replication_ref(),
            &configs,
            cursor,
            batch_rows,
        )?)
    }
}

/// The result columns the built-in system view `view` returns.
fn system_view_schema(view: TuningSystemView) -> SchemaRef {
    use DataType::{Float64, Int64, Utf8};
    let columns: &[(&str, DataType)] = match view {
        TuningSystemView::Workload => &[
            ("fingerprint", Utf8),
            ("executions", Int64),
            ("avg_latency_ms", Float64),
            ("returned_rows", Int64),
            ("examined_rows", Int64),
            ("access_pattern", Utf8),
            ("last_seen_millis", Int64),
        ],
        TuningSystemView::TuningLog => &[
            ("id", Utf8),
            ("at_millis", Int64),
            ("parameter", Utf8),
            ("old_value", Int64),
            ("new_value", Int64),
            ("status", Utf8),
            ("outcome", Utf8),
            ("rollback_of", Utf8),
        ],
        TuningSystemView::IndexAdvice => &[
            ("id", Utf8),
            ("action", Utf8),
            ("table_name", Utf8),
            ("column_name", Utf8),
            ("estimated_benefit", Float64),
            ("status", Utf8),
            ("reason", Utf8),
        ],
        TuningSystemView::ReprofileJobs => &[
            ("id", Utf8),
            ("object", Utf8),
            ("from_layout", Utf8),
            ("to_layout", Utf8),
            ("status", Utf8),
            ("detail", Utf8),
        ],
    };
    static_schema(columns)
}

/// The result columns of the built-in SQL helper `name`, or `None` for
/// another name.
fn phase19_schema(name: &str) -> Option<SchemaRef> {
    use DataType::{Binary, Float64, Int64, Utf8};
    let columns: &[(&str, DataType)] = match name {
        "match" => &[("id", Binary), ("score", Float64), ("text", Utf8)],
        "time_bucket" => &[("bucket", Int64)],
        "within_radius" => &[
            ("id", Binary),
            ("point", geo_point_type()),
            ("distance_meters", Float64),
        ],
        "graph_neighbors" => &[("node", Utf8)],
        _ => return None,
    };
    Some(static_schema(columns))
}

/// A geographic point, which pgwire sends as JSON like other structured values.
fn geo_point_type() -> DataType {
    DataType::Struct(
        vec![
            Field::new("lon", DataType::Float64, false),
            Field::new("lat", DataType::Float64, false),
        ]
        .into(),
    )
}

fn static_schema(columns: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Aggregated materialized data cannot safely inherit source row predicates or
/// masks after the fact. Refuse principal reads while any source is protected.
pub(super) fn protected_materialized_views(
    catalog: &BTreeMap<String, CatalogEntry>,
    policy: &PolicyConfig,
) -> BTreeSet<String> {
    catalog
        .iter()
        .filter_map(|(name, entry)| {
            let CatalogEntry::MaterializedView { spec, .. } = entry else {
                return None;
            };
            let mut seen = BTreeSet::new();
            let mut source = spec.source_table.as_str();
            loop {
                if !seen.insert(source.to_owned())
                    || policy_has_row_or_masking(
                        policy,
                        &resource_for_catalog_name(catalog, source),
                    )
                {
                    return Some(name.clone());
                }
                match catalog.get(source) {
                    Some(CatalogEntry::MaterializedView { spec, .. }) => {
                        source = &spec.source_table;
                    }
                    Some(_) => return None,
                    None => return Some(name.clone()),
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::CATALOG_TABLE;
    use super::*;
    use crate::{
        cdc::{self, AggregateKind, AggregateSpec, MaterializedViewSpec},
        db::{DbConfig, Profile, create_database},
        extension::{ExtensionError, MaskingPolicy, RowPolicy},
        geo::GeoIndexConfig,
        model::{CollectionId, FieldPath},
        query::{ColumnDef, ColumnType, DocField, TableLayout, TableSchema},
        repl::{ReadConsistency, Replication},
        security::{AuthzError, AuthzPolicy, Permission, Resource, Role},
        tuning::{
            ParameterEnvelope, ReprofilePlan, TunableParameter, TuningDecision, TuningPolicy,
            WorkloadSample, WorkloadWindow,
        },
    };

    #[tokio::test]
    async fn sql_graph_neighbors_fail_closed_for_graph_row_and_masking_policies()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_graph("social", crate::graph::GraphId::new(1))?;
        database.graph("social")?.add_edge(
            crate::graph::GraphNodeId::from_str_id("alice"),
            "knows",
            crate::graph::GraphNodeId::from_str_id("private-neighbor"),
            &Value::Null,
        )?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Graph("social".to_owned()), Permission::Read)
        ]));
        let reader = Principal::new("alice").with_role("reader");
        let sql = "SELECT graph_neighbors('social', 'alice', 'knows', 1)";
        assert!(
            matches!(database.query_as(&reader, sql).await?, SqlOutput::Rows(rows) if !rows.rows.is_empty())
        );
        for policy in [
            PolicyConfig {
                row_policies: vec![RowPolicy {
                    resource: Resource::Graph("social".to_owned()),
                    path: FieldPath::new(["owner"]),
                    equals: Value::Str("public".to_owned()),
                }],
                ..PolicyConfig::default()
            },
            PolicyConfig {
                masking: vec![MaskingPolicy {
                    resource: Resource::Graph("social".to_owned()),
                    paths: vec![FieldPath::new(["dst"])],
                    replacement: Value::Str("***".to_owned()),
                }],
                ..PolicyConfig::default()
            },
        ] {
            database.set_policy_config(&policy)?;
            assert!(matches!(
                database.query_as(&reader, sql).await,
                Err(DbError::Extension(ExtensionError::PolicyDenied(_)))
            ));
            assert!(matches!(
                database.describe_query_as(&reader, sql).await,
                Err(DbError::Extension(ExtensionError::PolicyDenied(_)))
            ));
        }
        database.set_policy_config(&PolicyConfig::default())?;
        assert!(
            matches!(database.query_as(&reader, sql).await?, SqlOutput::Rows(rows) if !rows.rows.is_empty())
        );
        Ok(())
    }

    #[test]
    fn plain_document_lww_catalog_guard_rejects_unknown_and_derived_collections() {
        use crate::model::{CollectionId, IndexId, IndexSpec};
        let id = CollectionId::new(1);
        let entry = |indexes| CatalogEntry::Collection {
            collection_id: id,
            fields: Vec::new(),
            indexes,
        };
        let mut catalog = BTreeMap::new();
        assert!(!super::super::plain_document_catalog_allowed(&catalog, 1));
        catalog.insert("docs".to_owned(), entry(Vec::new()));
        assert!(super::super::plain_document_catalog_allowed(&catalog, 1));
        catalog.insert(
            "docs".to_owned(),
            entry(vec![IndexSpec::new(
                IndexId::new(1),
                FieldPath::new(["text"]),
            )]),
        );
        assert!(!super::super::plain_document_catalog_allowed(&catalog, 1));
        catalog.insert("docs".to_owned(), entry(Vec::new()));
        catalog.insert(
            "fts".to_owned(),
            CatalogEntry::FullTextIndex {
                config: crate::text::FullTextIndexConfig::collection(
                    "fts",
                    id,
                    FieldPath::new(["text"]),
                ),
            },
        );
        assert!(!super::super::plain_document_catalog_allowed(&catalog, 1));
        catalog.remove("fts");
        catalog.insert(
            "geo".to_owned(),
            CatalogEntry::GeoIndex {
                config: crate::geo::GeoIndexConfig::new("geo", id, FieldPath::new(["point"])),
            },
        );
        assert!(!super::super::plain_document_catalog_allowed(&catalog, 1));
    }

    #[tokio::test]
    async fn custom_sql_dispatchers_cannot_bypass_text_size_limits()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut performance = crate::performance::PerformanceConfig::for_profile(Profile::InMemory);
        performance.query.max_sql_bytes = 64;
        let mut database = crate::db::create_database_with_ops(
            DbConfig::new(Profile::InMemory),
            crate::db::OperationalConfig::new().with_performance(performance),
        )?;
        database.create_table(
            "users",
            Some(TableSchema::new(
                vec![ColumnDef::new("id", ColumnType::Int, false)],
                0,
            )),
            Vec::new(),
        )?;
        database.set_authz_policy(AuthzPolicy::new([Role::new("admin")
            .grant(Resource::Database, Permission::Read)
            .grant(Resource::Database, Permission::Admin)
            .grant(Resource::Database, Permission::Write)]));
        let principal = Principal::new("root").with_role("admin");
        for prefix in [
            "ANALYZE FULL users",
            "CALL absent()",
            "SELECT time_bucket(1, 1)",
        ] {
            let sql = format!("{prefix}{}", " ".repeat(100));
            for result in [
                database.query(&sql).await,
                database.query_as(&principal, &sql).await,
                database
                    .query_as_of(&sql, crate::temporal::TemporalPoint::Lsn(0))
                    .await,
                database
                    .describe_query_as(&principal, &sql)
                    .await
                    .map(|_| SqlOutput::AffectedRows(0)),
            ] {
                assert!(
                    matches!(
                        result,
                        Err(DbError::Query(crate::query::QueryError::InputLimit(_)))
                    ),
                    "{result:?}"
                );
            }
        }
        assert!(
            crate::query::StatsCatalog::read_table(database.replication_ref(), "users")?.is_none()
        );
        Ok(())
    }

    #[test]
    fn admin_credential_compare_and_set_has_exactly_one_concurrent_winner()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
        let original = super::super::AdminCredentialRecord {
            username: "admin".to_owned(),
            password_hash: "old-hash".to_owned(),
            updated_at_millis: 1,
        };
        database.compare_and_set_admin_credential(None, &original)?;
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let attempts = std::thread::scope(|scope| {
            (0..2)
                .map(|index| {
                    let database = Arc::clone(&database);
                    let barrier = Arc::clone(&barrier);
                    let original = original.clone();
                    scope.spawn(move || {
                        let replacement = super::super::AdminCredentialRecord {
                            username: "admin".to_owned(),
                            password_hash: format!("new-hash-{index}"),
                            updated_at_millis: 2,
                        };
                        barrier.wait();
                        database.compare_and_set_admin_credential(Some(&original), &replacement)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(std::thread::ScopedJoinHandle::join)
                .collect::<Vec<_>>()
        });
        let mut winners = 0;
        let mut conflicts = 0;
        for attempt in attempts {
            match attempt.map_err(|_| "credential worker panicked")? {
                Ok(()) => winners += 1,
                Err(error) if error.is_conflict() => conflicts += 1,
                Err(error) => return Err(error.into()),
            }
        }
        assert_eq!((winners, conflicts), (1, 1));
        assert_ne!(database.admin_credential("admin")?, Some(original));
        Ok(())
    }

    #[tokio::test]
    async fn workload_and_tuning_metadata_require_system_admin()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Database, Permission::Read),
            Role::new("system_reader").grant(Resource::System, Permission::Read),
            Role::new("admin").grant(Resource::System, Permission::Admin),
        ]));
        for role in ["reader", "system_reader"] {
            let principal = Principal::new("alice").with_role(role);
            for sql in [
                "SELECT * FROM system.workload",
                "SELECT * FROM system.tuning_log",
                "SELECT * FROM system.index_advice",
                "SELECT * FROM system.reprofile_jobs",
            ] {
                assert!(
                    matches!(
                        database.query_as(&principal, sql).await,
                        Err(DbError::AuthzDenied(_))
                    ),
                    "{role}: {sql}"
                );
            }
            assert!(matches!(
                database.workload_report_as(&principal, WorkloadWindow::default()),
                Err(DbError::AuthzDenied(_))
            ));
        }
        let admin = Principal::new("admin").with_role("admin");
        assert!(
            database
                .query_as(&admin, "SELECT * FROM system.workload")
                .await
                .is_ok()
        );
        assert!(
            database
                .workload_report_as(&admin, WorkloadWindow::default())
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialized_reads_fail_closed_after_source_becomes_protected()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database
            .create_table(
                "sales",
                Some(TableSchema::new(
                    vec![
                        ColumnDef::new("id", ColumnType::Int, false),
                        ColumnDef::new("owner", ColumnType::Str, false),
                    ],
                    0,
                )),
                Vec::new(),
            )?
            .insert(vec![Value::Int(1), Value::Str("alice".into())])?;
        database.create_materialized_view_object(&MaterializedViewSpec {
            name: "totals".into(),
            source_table: "sales".into(),
            filter: None,
            group_by: 1,
            aggregates: vec![AggregateSpec {
                output_name: "n".into(),
                kind: AggregateKind::Count,
                column: None,
            }],
        })?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Table("totals".into()), Permission::Read)
        ]));
        let alice = Principal::new("alice").with_role("reader");
        assert!(
            database
                .query_as(&alice, "SELECT * FROM totals")
                .await
                .is_ok()
        );

        for policy in [
            PolicyConfig {
                row_policies: vec![RowPolicy {
                    resource: Resource::Table("sales".into()),
                    path: FieldPath::new(["owner"]),
                    equals: Value::Str("alice".into()),
                }],
                ..PolicyConfig::default()
            },
            PolicyConfig {
                masking: vec![MaskingPolicy {
                    resource: Resource::Table("sales".into()),
                    paths: vec![FieldPath::new(["owner"])],
                    replacement: Value::Str("***".into()),
                }],
                ..PolicyConfig::default()
            },
        ] {
            database.set_policy_config(&policy)?;
            for sql in [
                "SELECT * FROM totals",
                "SELECT (SELECT count(*) FROM totals)",
            ] {
                assert!(
                    matches!(
                        database.query_as(&alice, sql).await,
                        Err(DbError::AuthzDenied(_))
                    ),
                    "{sql}"
                );
            }
            assert!(matches!(
                database
                    .describe_query_as(&alice, "SELECT * FROM totals")
                    .await,
                Err(DbError::AuthzDenied(_))
            ));
        }
        // The trusted operator API still permits maintenance and inspection.
        assert!(database.query("SELECT * FROM totals").await.is_ok());
        Ok(())
    }

    #[test]
    fn materialized_creation_requires_both_source_admin_and_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = materialized_source_database()?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Table("sales".into()), Permission::Read),
            Role::new("administrator").grant(Resource::Table("sales".into()), Permission::Admin),
        ]));
        for (role, permission) in [
            ("reader", Permission::Admin),
            ("administrator", Permission::Read),
        ] {
            let principal = Principal::new("alice").with_role(role);
            let spec = totals_spec(&format!("denied_{role}"));
            assert!(matches!(
                database.create_materialized_view_object_as(&principal, &spec),
                Err(DbError::AuthzDenied(AuthzError::Denied {
                    resource: Resource::Table(source),
                    permission: missing,
                    ..
                })) if source == "sales" && missing == permission
            ));
            assert_view_metadata_absent(&database, &spec.name)?;
        }
        let principal = Principal::new("alice")
            .with_role("reader")
            .with_role("administrator");
        let spec = totals_spec("allowed_totals");
        database.create_materialized_view_object_as(&principal, &spec)?;
        assert!(matches!(
            database.catalog().get(&spec.name),
            Some(CatalogEntry::MaterializedView { .. })
        ));
        assert!(database.read_materialized_view(&spec.name).is_ok());
        Ok(())
    }

    #[test]
    fn materialized_creation_refuses_source_policies_before_writing_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut database = materialized_source_database()?;
        database.set_authz_policy(AuthzPolicy::new([Role::new("creator")
            .grant(Resource::Table("sales".into()), Permission::Admin)
            .grant(Resource::Table("sales".into()), Permission::Read)]));
        let principal = Principal::new("alice").with_role("creator");
        for (name, policy) in [
            (
                "row_protected_totals",
                PolicyConfig {
                    row_policies: vec![RowPolicy {
                        resource: Resource::Table("sales".into()),
                        path: FieldPath::new(["owner"]),
                        equals: Value::Str("alice".into()),
                    }],
                    ..PolicyConfig::default()
                },
            ),
            (
                "masked_totals",
                PolicyConfig {
                    masking: vec![MaskingPolicy {
                        resource: Resource::Table("sales".into()),
                        paths: vec![FieldPath::new(["owner"])],
                        replacement: Value::Str("***".into()),
                    }],
                    ..PolicyConfig::default()
                },
            ),
        ] {
            database.set_policy_config(&policy)?;
            let spec = totals_spec(name);
            assert!(matches!(
                database.create_materialized_view_object_as(&principal, &spec),
                Err(DbError::Extension(ExtensionError::PolicyDenied(_)))
            ));
            assert_view_metadata_absent(&database, name)?;
        }
        Ok(())
    }

    fn materialized_source_database() -> Result<Database, DbError> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database
            .create_table(
                "sales",
                Some(TableSchema::new(
                    vec![
                        ColumnDef::new("id", ColumnType::Int, false),
                        ColumnDef::new("owner", ColumnType::Str, false),
                    ],
                    0,
                )),
                Vec::new(),
            )?
            .insert(vec![Value::Int(1), Value::Str("alice".into())])?;
        Ok(database)
    }

    fn totals_spec(name: &str) -> MaterializedViewSpec {
        MaterializedViewSpec {
            name: name.into(),
            source_table: "sales".into(),
            filter: None,
            group_by: 1,
            aggregates: vec![AggregateSpec {
                output_name: "n".into(),
                kind: AggregateKind::Count,
                column: None,
            }],
        }
    }

    fn assert_view_metadata_absent(database: &Database, name: &str) -> Result<(), DbError> {
        assert!(!database.catalog().contains_key(name));
        for table in [
            CATALOG_TABLE,
            cdc::MATERIALIZED_VIEWS_TABLE,
            cdc::MATERIALIZED_VIEW_STATE_TABLE,
        ] {
            assert!(
                database
                    .read(table, name.as_bytes(), ReadConsistency::Strong)?
                    .is_none(),
                "denied view creation wrote {table}/{name}"
            );
        }
        Ok(())
    }

    fn describe_test_database()
    -> Result<(Database, Principal, Principal), Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let schema = || {
            TableSchema::new(
                vec![
                    ColumnDef::new("id", ColumnType::Int, false),
                    ColumnDef::new("name", ColumnType::Str, false),
                ],
                0,
            )
        };
        database.create_table("public_t", Some(schema()), Vec::new())?;
        database.create_table("secrets", Some(schema()), Vec::new())?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("admin")
                .grant(Resource::Database, Permission::Read)
                .grant(Resource::Database, Permission::Write)
                .grant(Resource::Database, Permission::Admin)
                .grant(Resource::System, Permission::Admin),
            Role::new("reader").grant(Resource::Table("public_t".to_owned()), Permission::Read),
        ]));
        Ok((
            database,
            Principal::new("root").with_role("admin"),
            Principal::new("alice").with_role("reader"),
        ))
    }

    #[tokio::test]
    async fn explain_requirements_come_from_the_statement_as_parsed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (database, _, reader) = describe_test_database()?;
        // The pre-check printed the explained statement back and parsed it
        // again; the quoted value ended early there and read `secrets`.
        let sql =
            "explain select id from public_t where name = '\\'' union select id from secrets --'";
        assert!(
            matches!(database.query_as(&reader, sql).await?, SqlOutput::Rows(_)),
            "{sql}"
        );
        assert!(
            database.describe_query_as(&reader, sql).await?.is_some(),
            "{sql}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn describe_answers_statements_without_rows() -> Result<(), Box<dyn std::error::Error>> {
        let (database, admin, _) = describe_test_database()?;
        // Neither statement runs: the procedure does not exist and the
        // module is not valid WASM.
        for sql in [
            "CALL absent(1, 'x')",
            "CREATE FUNCTION f LANGUAGE wasm AS HEX '00'",
        ] {
            let described = database.describe_query_as(&admin, sql).await;
            assert!(matches!(described, Ok(None)), "{sql}: {described:?}");
        }
        let udfs = database.udf_specs()?;
        assert!(udfs.is_empty(), "{udfs:?}");
        let described = database
            .describe_query_as(&admin, "CREATE TRIGGER t BEFORE INSERT ON public_t")
            .await;
        assert!(
            matches!(
                described,
                Err(DbError::Query(crate::query::QueryError::Unsupported(_)))
            ),
            "{described:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn describe_types_builtins_like_execution() -> Result<(), Box<dyn std::error::Error>> {
        let (mut database, admin, _) = describe_test_database()?;
        // A UDF call runs WASM, so it is not described.
        let described = database
            .describe_query_as(&admin, "SELECT absent_udf(1)")
            .await;
        assert!(
            matches!(
                described,
                Err(DbError::Query(crate::query::QueryError::Unsupported(_)))
            ),
            "{described:?}"
        );

        // Each view and helper below returns a row, so its values' types are compared.
        database.record_workload_sample(
            &WorkloadSample::new("SELECT * FROM public_t WHERE name = 'x'")
                .with_access("public_t", "name")
                .with_observed_rows(1, 3_000),
        )?;
        let mut policy = TuningPolicy {
            enabled: true,
            recommend_only: false,
            ..TuningPolicy::default()
        };
        policy.envelopes.insert(
            TunableParameter::CacheMaxCapacity,
            ParameterEnvelope::new(64, 4_096, 64),
        );
        database.set_tuning_policy(&policy)?;
        let capacity = database.performance_config().cache.max_capacity;
        database.apply_tuning(TuningDecision::new(
            TunableParameter::CacheMaxCapacity,
            capacity,
            1_024,
            "cache miss pressure",
        ))?;
        database.start_reprofile(ReprofilePlan {
            object: "public_t".to_owned(),
            from_layout: TableLayout::Row,
            to_layout: TableLayout::Columnar,
            reversible: true,
        })?;
        database
            .create_collection(
                "places",
                CollectionId::new(1),
                vec![DocField::document_id("id")],
                Vec::new(),
            )?
            .insert(&Value::Object(BTreeMap::from([(
                "point".to_owned(),
                Value::GeoPoint {
                    lon: 21.0,
                    lat: 52.0,
                },
            )])))?;
        database.create_geo_index(GeoIndexConfig::new(
            "places_geo",
            CollectionId::new(1),
            FieldPath::new(["point"]),
        ))?;

        for sql in [
            "SELECT * FROM system.workload",
            "SELECT * FROM system.tuning_log",
            "SELECT * FROM system.index_advice",
            "SELECT * FROM system.reprofile_jobs",
            "SELECT time_bucket(60000, 125000)",
            "SELECT * FROM within_radius('places_geo', 21.0, 52.0, 10)",
        ] {
            let schema = database
                .describe_query_as(&admin, sql)
                .await?
                .ok_or_else(|| format!("{sql} has no result columns"))?;
            let SqlOutput::Rows(rows) = database.query_as(&admin, sql).await? else {
                return Err(format!("{sql} returned no rows").into());
            };
            assert!(!rows.rows.is_empty(), "{sql} returned no rows to compare");
            let names = schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            assert_eq!(names, rows.columns, "{sql}");
            for row in &rows.rows {
                for (value, field) in row.iter().zip(schema.fields()) {
                    let expected = match value {
                        Value::Null => continue,
                        Value::Bool(_) => DataType::Boolean,
                        Value::Int(_) => DataType::Int64,
                        Value::Float(_) => DataType::Float64,
                        Value::Bytes(_) => DataType::Binary,
                        Value::GeoPoint { .. } => super::geo_point_type(),
                        _ => DataType::Utf8,
                    };
                    assert_eq!(field.data_type(), &expected, "{sql}: {}", field.name());
                }
            }
        }
        Ok(())
    }
}
