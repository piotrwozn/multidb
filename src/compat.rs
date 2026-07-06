#![allow(clippy::missing_errors_doc)]

use std::collections::BTreeMap;

use sqlparser::{
    ast::{Expr, ObjectName, Query, SelectItem, SetExpr, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::{
    db::{CatalogEntry, DbConfig, DbError},
    model::Value,
    query::{ColumnType, QueryError, SqlOutput, SqlRows, TableLayout, TableSchema},
    repl::ReplError,
    storage::StorageError,
};

pub const SERVER_VERSION: &str = "14.0-multidb";
pub const CURRENT_DATABASE: &str = "multidb";
pub const DEFAULT_SCHEMA: &str = "public";
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const INFORMATION_SCHEMA: &str = "information_schema";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompatibilityReport {
    pub server_version: String,
    pub supported_clients: Vec<ClientCompatibility>,
    pub known_gaps: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ClientCompatibility {
    pub client: String,
    pub required: bool,
    pub scenarios: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogObject {
    pub name: String,
    pub schema: Option<TableSchema>,
    pub kind: CatalogObjectKind,
    pub layout: Option<TableLayout>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogObjectKind {
    Table,
    Collection,
    VectorCollection,
    FullTextIndex,
    TimeSeries,
    Graph,
    GeoIndex,
    ForeignTable,
    MaterializedView,
    TemporalTable,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgCatalogSnapshot {
    pub objects: Vec<CatalogObject>,
}

impl CompatibilityReport {
    #[must_use]
    pub fn phase22_default() -> Self {
        Self {
            server_version: SERVER_VERSION.to_owned(),
            supported_clients: vec![
                ClientCompatibility::new("psql", true),
                ClientCompatibility::new("psycopg3", true),
                ClientCompatibility::new("node-postgres", true),
                ClientCompatibility::new("jdbc", true),
                ClientCompatibility::new("sqlx", true),
                ClientCompatibility::new("sqlalchemy-core", true),
            ],
            known_gaps: vec![
                "Mongo wire protocol is intentionally deferred".to_owned(),
                "Full PostgreSQL catalog/rules/triggers compatibility is outside the current scope"
                    .to_owned(),
                "Decimal/Numeric has no native engine type yet; Mongo Decimal128 imports as text"
                    .to_owned(),
            ],
        }
    }

    #[must_use]
    pub fn to_sql_output(&self) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "client".to_owned(),
                "required".to_owned(),
                "scenarios".to_owned(),
            ],
            rows: self
                .supported_clients
                .iter()
                .map(|client| {
                    vec![
                        Value::Str(client.client.clone()),
                        Value::Bool(client.required),
                        Value::Str(client.scenarios.join(",")),
                    ]
                })
                .collect(),
        })
    }
}

impl ClientCompatibility {
    #[must_use]
    pub fn new(client: impl Into<String>, required: bool) -> Self {
        Self {
            client: client.into(),
            required,
            scenarios: vec![
                "connect_tls_scram".to_owned(),
                "simple_query".to_owned(),
                "prepared_statement".to_owned(),
                "transactions".to_owned(),
                "catalog_introspection".to_owned(),
                "sqlstate_errors".to_owned(),
            ],
        }
    }
}

impl PgCatalogSnapshot {
    #[must_use]
    pub fn from_catalog(
        catalog: &BTreeMap<String, CatalogEntry>,
        schemas: &BTreeMap<String, TableSchema>,
    ) -> Self {
        let objects = catalog
            .iter()
            .map(|(name, entry)| {
                let (kind, layout) = match entry {
                    CatalogEntry::Table { layout, .. } => (CatalogObjectKind::Table, Some(*layout)),
                    CatalogEntry::Collection { .. } => (CatalogObjectKind::Collection, None),
                    CatalogEntry::Vector { .. } => (CatalogObjectKind::VectorCollection, None),
                    CatalogEntry::FullTextIndex { .. } => (CatalogObjectKind::FullTextIndex, None),
                    CatalogEntry::TimeSeries { .. } => (CatalogObjectKind::TimeSeries, None),
                    CatalogEntry::Graph { .. } => (CatalogObjectKind::Graph, None),
                    CatalogEntry::GeoIndex { .. } => (CatalogObjectKind::GeoIndex, None),
                    CatalogEntry::ForeignTable { .. } => (CatalogObjectKind::ForeignTable, None),
                    CatalogEntry::MaterializedView { .. } => {
                        (CatalogObjectKind::MaterializedView, None)
                    }
                    CatalogEntry::TemporalTable { .. } => (CatalogObjectKind::TemporalTable, None),
                };
                CatalogObject {
                    name: name.clone(),
                    schema: schemas.get(name).cloned(),
                    kind,
                    layout,
                }
            })
            .collect();
        Self { objects }
    }
}

pub fn execute_compat_sql(
    sql: &str,
    config: &DbConfig,
    snapshot: &PgCatalogSnapshot,
) -> Result<Option<SqlOutput>, QueryError> {
    let Some(request) = parse_compat_request(sql)? else {
        return Ok(None);
    };

    let output = match request {
        CompatRequest::Version => scalar(
            "version",
            Value::Str(format!("PostgreSQL {SERVER_VERSION}")),
        ),
        CompatRequest::CurrentSchema => {
            scalar("current_schema", Value::Str(DEFAULT_SCHEMA.to_owned()))
        }
        CompatRequest::CurrentDatabase => {
            scalar("current_database", Value::Str(CURRENT_DATABASE.to_owned()))
        }
        CompatRequest::CompatibilityReport => {
            CompatibilityReport::phase22_default().to_sql_output()
        }
        CompatRequest::PgType => pg_type_rows(),
        CompatRequest::PgNamespace => pg_namespace_rows(),
        CompatRequest::PgClass => pg_class_rows(snapshot),
        CompatRequest::InformationSchemaTables => information_schema_tables(snapshot),
        CompatRequest::InformationSchemaColumns => information_schema_columns(snapshot),
        CompatRequest::ServerStatus => server_status_rows(config, snapshot),
    };
    Ok(Some(output))
}

pub fn compat_sql_requirements(sql: &str) -> Result<bool, QueryError> {
    parse_compat_request(sql).map(|request| request.is_some())
}

#[must_use]
pub fn sqlstate_for_db_error(error: &DbError) -> &'static str {
    if error.is_conflict() {
        return "40001";
    }

    match error {
        DbError::AuthzDenied(_) => "42501",
        DbError::MissingCatalogObject(_) => "42P01",
        DbError::CatalogObjectExists(_) => "42710",
        DbError::CatalogKindMismatch { .. } => "42809",
        DbError::Storage(error) => sqlstate_for_storage(error),
        DbError::Repl(error) => sqlstate_for_repl(error),
        DbError::Query(error) => sqlstate_for_query(error),
        DbError::Config(_) => "08004",
        _ => "XX000",
    }
}

#[must_use]
pub fn sqlstate_for_query(error: &QueryError) -> &'static str {
    match error {
        QueryError::Parse(_) | QueryError::InputLimit(_) => "42601",
        QueryError::Timeout(_) | QueryError::Cancelled(_) => "57014",
        QueryError::ResourceLimit(_) => "53200",
        QueryError::Unsupported(_) => "0A000",
        QueryError::InvalidSchema(_) | QueryError::InvalidRow(_) | QueryError::InvalidValue(_) => {
            "22023"
        }
        QueryError::MissingTable(_) => "42P01",
        QueryError::MissingColumn(_) => "42703",
        QueryError::DuplicatePrimaryKey => "23505",
        QueryError::Storage(error) => sqlstate_for_storage(error),
        QueryError::Repl(error) => sqlstate_for_repl(error),
        QueryError::DataFusion(_) | QueryError::Model(_) | QueryError::Serde(_) => "XX000",
    }
}

#[must_use]
pub fn sqlstate_for_repl(error: &ReplError) -> &'static str {
    match error {
        ReplError::Conflict | ReplError::Storage(StorageError::Conflict) => "40001",
        ReplError::NoQuorum | ReplError::Transport(_) => "08006",
        ReplError::Unsupported(_) => "0A000",
        ReplError::QuotaExceeded { .. } => "53100",
        ReplError::Storage(error) => sqlstate_for_storage(error),
    }
}

#[must_use]
pub fn sqlstate_for_storage(error: &StorageError) -> &'static str {
    match error {
        StorageError::Conflict => "40001",
        StorageError::Corruption(_) => "XX001",
        StorageError::NotFound => "02000",
        StorageError::Io(_) => "58030",
        StorageError::Backend(_) => "XX000",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompatRequest {
    Version,
    CurrentSchema,
    CurrentDatabase,
    CompatibilityReport,
    PgType,
    PgNamespace,
    PgClass,
    InformationSchemaTables,
    InformationSchemaColumns,
    ServerStatus,
}

fn parse_compat_request(sql: &str) -> Result<Option<CompatRequest>, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed
        .get(..trimmed.len().min(6))
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
    {
        return Ok(None);
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, trimmed)
        .map_err(|error| QueryError::Parse(error.to_string()))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };

    if let Some(function) = single_function_projection(query) {
        return Ok(match function.as_str() {
            "version" => Some(CompatRequest::Version),
            "current_schema" => Some(CompatRequest::CurrentSchema),
            "current_database" => Some(CompatRequest::CurrentDatabase),
            _ => None,
        });
    }

    let Some(name) = single_from_name(query) else {
        return Ok(None);
    };
    Ok(match normalize_object_name(&name).as_str() {
        "pg_catalog.pg_type" | "pg_type" => Some(CompatRequest::PgType),
        "pg_catalog.pg_namespace" | "pg_namespace" => Some(CompatRequest::PgNamespace),
        "pg_catalog.pg_class" | "pg_class" => Some(CompatRequest::PgClass),
        "information_schema.tables" => Some(CompatRequest::InformationSchemaTables),
        "information_schema.columns" => Some(CompatRequest::InformationSchemaColumns),
        "system.compatibility" => Some(CompatRequest::CompatibilityReport),
        "system.status" => Some(CompatRequest::ServerStatus),
        _ => None,
    })
}

fn single_function_projection(query: &Query) -> Option<String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !select.from.is_empty() || select.projection.len() != 1 {
        return None;
    }
    let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
        return None;
    };
    if !matches_empty_args(&function.args) {
        return None;
    }
    Some(function.name.to_string().to_ascii_lowercase())
}

fn matches_empty_args(args: &sqlparser::ast::FunctionArguments) -> bool {
    match args {
        sqlparser::ast::FunctionArguments::None => true,
        sqlparser::ast::FunctionArguments::List(list) => list.args.is_empty(),
        sqlparser::ast::FunctionArguments::Subquery(_) => false,
    }
}

fn single_from_name(query: &Query) -> Option<String> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
    {
        return None;
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || select.selection.is_some()
        || select.having.is_some()
        || select.projection.len() != 1
        || !matches!(select.projection[0], SelectItem::Wildcard(_))
        || !matches!(
            select.group_by,
            sqlparser::ast::GroupByExpr::Expressions(ref expressions, ref modifiers)
                if expressions.is_empty() && modifiers.is_empty()
        )
    {
        return None;
    }
    let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation else {
        return None;
    };
    Some(object_name_to_string(name))
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.to_string().trim_matches('"').to_owned())
        .collect::<Vec<_>>()
        .join(".")
}

fn normalize_object_name(name: &str) -> String {
    name.trim_matches('"').to_ascii_lowercase()
}

fn scalar(column: &str, value: Value) -> SqlOutput {
    SqlOutput::Rows(SqlRows {
        columns: vec![column.to_owned()],
        rows: vec![vec![value]],
    })
}

fn pg_type_rows() -> SqlOutput {
    let types = [
        (16_i64, "bool", "B"),
        (17, "bytea", "U"),
        (20, "int8", "N"),
        (25, "text", "S"),
        (701, "float8", "N"),
        (1082, "date", "D"),
        (1114, "timestamp", "D"),
        (1184, "timestamptz", "D"),
        (2950, "uuid", "U"),
        (3802, "jsonb", "U"),
        (50_001, "vector", "U"),
    ];
    SqlOutput::Rows(SqlRows {
        columns: vec![
            "oid".to_owned(),
            "typname".to_owned(),
            "typcategory".to_owned(),
        ],
        rows: types
            .into_iter()
            .map(|(oid, name, category)| {
                vec![
                    Value::Int(oid),
                    Value::Str(name.to_owned()),
                    Value::Str(category.to_owned()),
                ]
            })
            .collect(),
    })
}

fn pg_namespace_rows() -> SqlOutput {
    SqlOutput::Rows(SqlRows {
        columns: vec!["oid".to_owned(), "nspname".to_owned()],
        rows: vec![
            vec![Value::Int(11), Value::Str(PG_CATALOG_SCHEMA.to_owned())],
            vec![Value::Int(2_200), Value::Str(DEFAULT_SCHEMA.to_owned())],
            vec![
                Value::Int(13_337),
                Value::Str(INFORMATION_SCHEMA.to_owned()),
            ],
        ],
    })
}

fn pg_class_rows(snapshot: &PgCatalogSnapshot) -> SqlOutput {
    SqlOutput::Rows(SqlRows {
        columns: vec![
            "oid".to_owned(),
            "relname".to_owned(),
            "relnamespace".to_owned(),
            "relkind".to_owned(),
            "reltuples".to_owned(),
        ],
        rows: snapshot
            .objects
            .iter()
            .map(|object| {
                vec![
                    Value::Int(stable_oid(&object.name)),
                    Value::Str(object.name.clone()),
                    Value::Int(2_200),
                    Value::Str(relkind(object.kind).to_owned()),
                    Value::Float(0.0),
                ]
            })
            .collect(),
    })
}

fn information_schema_tables(snapshot: &PgCatalogSnapshot) -> SqlOutput {
    SqlOutput::Rows(SqlRows {
        columns: vec![
            "table_schema".to_owned(),
            "table_name".to_owned(),
            "table_type".to_owned(),
        ],
        rows: snapshot
            .objects
            .iter()
            .map(|object| {
                vec![
                    Value::Str(DEFAULT_SCHEMA.to_owned()),
                    Value::Str(object.name.clone()),
                    Value::Str("BASE TABLE".to_owned()),
                ]
            })
            .collect(),
    })
}

fn information_schema_columns(snapshot: &PgCatalogSnapshot) -> SqlOutput {
    let mut rows = Vec::new();
    for object in &snapshot.objects {
        if let Some(schema) = &object.schema {
            for (index, column) in schema.columns.iter().enumerate() {
                rows.push(vec![
                    Value::Str(DEFAULT_SCHEMA.to_owned()),
                    Value::Str(object.name.clone()),
                    Value::Str(column.name.clone()),
                    Value::Int(i64::try_from(index + 1).unwrap_or(i64::MAX)),
                    Value::Str(if column.nullable { "YES" } else { "NO" }.to_owned()),
                    Value::Str(pg_data_type(&column.ty).to_owned()),
                ]);
            }
        } else {
            rows.push(vec![
                Value::Str(DEFAULT_SCHEMA.to_owned()),
                Value::Str(object.name.clone()),
                Value::Str("value".to_owned()),
                Value::Int(1),
                Value::Str("YES".to_owned()),
                Value::Str("jsonb".to_owned()),
            ]);
        }
    }
    SqlOutput::Rows(SqlRows {
        columns: vec![
            "table_schema".to_owned(),
            "table_name".to_owned(),
            "column_name".to_owned(),
            "ordinal_position".to_owned(),
            "is_nullable".to_owned(),
            "data_type".to_owned(),
        ],
        rows,
    })
}

fn server_status_rows(config: &DbConfig, snapshot: &PgCatalogSnapshot) -> SqlOutput {
    SqlOutput::Rows(SqlRows {
        columns: vec![
            "server_version".to_owned(),
            "profile".to_owned(),
            "replication".to_owned(),
            "objects".to_owned(),
        ],
        rows: vec![vec![
            Value::Str(SERVER_VERSION.to_owned()),
            Value::Str(format!("{:?}", config.profile)),
            Value::Str(format!("{:?}", config.replication)),
            Value::Int(i64::try_from(snapshot.objects.len()).unwrap_or(i64::MAX)),
        ]],
    })
}

fn relkind(kind: CatalogObjectKind) -> &'static str {
    match kind {
        CatalogObjectKind::Table | CatalogObjectKind::ForeignTable => "r",
        CatalogObjectKind::Collection
        | CatalogObjectKind::VectorCollection
        | CatalogObjectKind::FullTextIndex
        | CatalogObjectKind::TimeSeries
        | CatalogObjectKind::Graph
        | CatalogObjectKind::GeoIndex
        | CatalogObjectKind::MaterializedView
        | CatalogObjectKind::TemporalTable => "v",
    }
}

fn pg_data_type(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Int => "bigint",
        ColumnType::Float => "double precision",
        ColumnType::Str | ColumnType::Null => "text",
        ColumnType::Bool => "boolean",
        ColumnType::Bytes => "bytea",
    }
}

fn stable_oid(name: &str) -> i64 {
    let mut hash = 14_959_u64;
    for byte in name.as_bytes() {
        hash = hash.wrapping_mul(16_777_619) ^ u64::from(*byte);
    }
    i64::try_from(10_000 + (hash % 2_000_000)).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        CatalogObject, CatalogObjectKind, PgCatalogSnapshot, SERVER_VERSION,
        compat_sql_requirements, execute_compat_sql, sqlstate_for_query,
    };
    use crate::{
        db::{DbConfig, Profile},
        model::Value,
        query::{ColumnDef, ColumnType, QueryError, SqlOutput, TableSchema},
    };

    #[test]
    fn pg_catalog_views_are_generated_from_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = PgCatalogSnapshot {
            objects: vec![CatalogObject {
                name: "users".to_owned(),
                schema: Some(TableSchema::new(
                    vec![
                        ColumnDef::new("id", ColumnType::Int, false),
                        ColumnDef::new("name", ColumnType::Str, true),
                    ],
                    0,
                )),
                kind: CatalogObjectKind::Table,
                layout: None,
            }],
        };
        let output = execute_compat_sql(
            "SELECT * FROM information_schema.columns",
            &DbConfig::new(Profile::InMemory),
            &snapshot,
        )?
        .ok_or("missing compat output")?;
        assert!(matches!(output, SqlOutput::Rows(rows) if rows.rows.len() == 2));
        Ok(())
    }

    #[test]
    fn version_and_requirements_are_detected() -> Result<(), Box<dyn std::error::Error>> {
        assert!(compat_sql_requirements("SELECT version()")?);
        let output = execute_compat_sql(
            "SELECT version()",
            &DbConfig::new(Profile::InMemory),
            &PgCatalogSnapshot::default(),
        )?;
        assert!(matches!(
            output,
            Some(SqlOutput::Rows(rows))
                if matches!(&rows.rows[0][0], Value::Str(value) if value.contains(SERVER_VERSION))
        ));
        Ok(())
    }

    #[test]
    fn sqlstate_mapping_uses_standard_codes() {
        assert_eq!(
            sqlstate_for_query(&QueryError::DuplicatePrimaryKey),
            "23505"
        );
        assert_eq!(
            sqlstate_for_query(&QueryError::Unsupported("x".to_owned())),
            "0A000"
        );
        assert_eq!(
            sqlstate_for_query(&QueryError::MissingTable("users".to_owned())),
            "42P01"
        );
    }

    #[test]
    fn compatibility_report_lists_required_clients() {
        let report = super::CompatibilityReport::phase22_default();
        let clients = report
            .supported_clients
            .into_iter()
            .map(|client| client.client)
            .collect::<Vec<_>>();
        assert!(clients.contains(&"sqlalchemy-core".to_owned()));
    }
}
