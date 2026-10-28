//! Authorization shared by metadata-only SQL planning and derived relations.

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use super::{
    Arc, BTreeMap, BTreeSet, CatalogEntry, Database, DbError, PolicyConfig, Principal, SqlOutput,
    TemporalError, Value, compat, parse_as_of_query, policy_has_row_or_masking,
    resource_for_catalog_name, sql_requirements,
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
        if let Some((rewritten, point)) = parse_as_of_query(sql)? {
            if self.repl.is_ap() || self.repl.is_sharded() {
                return Err(TemporalError::Unsupported(
                    "temporal AS OF requires single-node commit-log storage".to_owned(),
                )
                .into());
            }
            let lsn = crate::temporal::resolve_temporal_point(self.replication_ref(), point)?;
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
        model::FieldPath,
        query::{ColumnDef, ColumnType, TableSchema},
        repl::{ReadConsistency, Replication},
        security::{AuthzError, AuthzPolicy, Permission, Resource, Role},
        tuning::WorkloadWindow,
    };

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
}
