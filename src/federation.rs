use std::{collections::BTreeMap, env};

use crate::{
    cloud::{ObjectStoreConfig, ObjectStoreUri, open_object_store},
    migration::{json_object_to_row, parse_csv_line},
    model::Value,
    query::{ColumnType, QueryError, Row, TableSchema, parquet_bytes_to_rows},
};

pub const FOREIGN_TABLES_TABLE: &str = "__foreign_tables";

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum SecretRef {
    EnvVar(String),
    Named(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForeignSource {
    Parquet {
        uri: ObjectStoreUri,
        path: String,
    },
    Csv {
        uri: ObjectStoreUri,
        path: String,
        has_header: bool,
    },
    JsonLines {
        uri: ObjectStoreUri,
        path: String,
    },
    Postgres {
        connection: SecretRef,
        table: String,
    },
    MultidbPgWire {
        connection: SecretRef,
        table: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ForeignTableOptions {
    pub batch_size: usize,
    pub remote_scan_limit: usize,
    pub allow_remote_without_limit: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ForeignTableStats {
    pub row_count: Option<u64>,
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
    pub pushed_filters: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ForeignScanRequest {
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<ForeignFilter>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ForeignFilter {
    pub column: usize,
    pub op: ForeignFilterOp,
    pub value: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForeignFilterOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ForeignScanResult {
    pub rows: Vec<Row>,
    pub stats: ForeignTableStats,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ForeignTableSpec {
    pub schema: TableSchema,
    pub source: ForeignSource,
    pub options: ForeignTableOptions,
    pub stats: ForeignTableStats,
}

#[derive(thiserror::Error, Debug)]
pub enum FederationError {
    #[error("query: {0}")]
    Query(#[from] QueryError),

    #[error("cloud: {0}")]
    Cloud(#[from] crate::cloud::CloudError),

    #[error("unsupported foreign source: {0}")]
    Unsupported(String),

    #[error("invalid foreign source: {0}")]
    InvalidSource(String),

    #[error("remote postgres: {0}")]
    Postgres(String),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

pub trait ForeignDataSource: Send + Sync {
    /// Scans a foreign source with a validated request.
    /// # Errors
    /// Fails when the source cannot be read, decoded, filtered, or projected.
    fn scan(&self, request: &ForeignScanRequest) -> Result<ForeignScanResult, FederationError>;
}

impl Default for ForeignTableOptions {
    fn default() -> Self {
        Self {
            batch_size: 1_024,
            remote_scan_limit: 10_000,
            allow_remote_without_limit: false,
        }
    }
}

/// Scans a foreign table through its local or remote source implementation.
/// # Errors
/// Fails when the source is unsupported, unreadable, corrupt, or rejects the remote query.
pub async fn scan_foreign_table(
    spec: &ForeignTableSpec,
    request: ForeignScanRequest,
) -> Result<ForeignScanResult, FederationError> {
    match &spec.source {
        ForeignSource::Parquet { .. }
        | ForeignSource::Csv { .. }
        | ForeignSource::JsonLines { .. } => {
            scan_local_source(&spec.schema, &spec.source, &request)
        }
        ForeignSource::Postgres { .. } | ForeignSource::MultidbPgWire { .. } => {
            scan_remote_postgres(&spec.schema, &spec.source, &spec.options, &request).await
        }
    }
}

impl ForeignScanRequest {
    #[must_use]
    pub fn new(
        projection: Option<Vec<usize>>,
        filters: Vec<ForeignFilter>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            projection,
            filters,
            limit,
        }
    }
}

/// Scans a local object-store backed foreign source.
/// # Errors
/// Fails when the object cannot be read, decoded, filtered, or projected.
pub fn scan_local_source(
    schema: &TableSchema,
    source: &ForeignSource,
    request: &ForeignScanRequest,
) -> Result<ForeignScanResult, FederationError> {
    let bytes = match source {
        ForeignSource::Parquet { uri, path }
        | ForeignSource::Csv { uri, path, .. }
        | ForeignSource::JsonLines { uri, path } => read_object_bytes(uri, path)?,
        ForeignSource::Postgres { .. } | ForeignSource::MultidbPgWire { .. } => {
            return Err(FederationError::Unsupported(
                "remote sources must be scanned asynchronously".to_owned(),
            ));
        }
    };
    let scanned_bytes = bytes.len() as u64;
    let rows = match source {
        ForeignSource::Parquet { .. } => parquet_bytes_to_rows(&bytes, schema)?,
        ForeignSource::Csv { has_header, .. } => csv_bytes_to_rows(schema, &bytes, *has_header)?,
        ForeignSource::JsonLines { .. } => jsonl_bytes_to_rows(schema, &bytes)?,
        ForeignSource::Postgres { .. } | ForeignSource::MultidbPgWire { .. } => unreachable!(),
    };
    let rows = apply_filters_and_projection(schema, rows, request)?;
    Ok(ForeignScanResult {
        stats: ForeignTableStats {
            row_count: Some(rows.len() as u64),
            scanned_rows: rows.len() as u64,
            scanned_bytes,
            pushed_filters: request.filters.len(),
        },
        rows,
    })
}

/// Scans a PG-compatible remote foreign source.
/// # Errors
/// Fails when the secret cannot be resolved, the connection/query fails, or rows cannot be decoded.
pub async fn scan_remote_postgres(
    schema: &TableSchema,
    source: &ForeignSource,
    options: &ForeignTableOptions,
    request: &ForeignScanRequest,
) -> Result<ForeignScanResult, FederationError> {
    let (connection, table) = match source {
        ForeignSource::Postgres { connection, table }
        | ForeignSource::MultidbPgWire { connection, table } => (connection, table),
        ForeignSource::Parquet { .. }
        | ForeignSource::Csv { .. }
        | ForeignSource::JsonLines { .. } => {
            return Err(FederationError::Unsupported(
                "local object sources are not remote postgres".to_owned(),
            ));
        }
    };
    let connection = resolve_secret(connection)?;
    let (client, connection_task) = tokio_postgres::connect(&connection, tokio_postgres::NoTls)
        .await
        .map_err(|error| FederationError::Postgres(error.to_string()))?;
    tokio::spawn(async move {
        if let Err(error) = connection_task.await {
            tracing::debug!("foreign postgres connection ended: {error}");
        }
    });

    let sql = remote_select_sql(schema, table, options, request)?;
    let rows = client
        .query(&sql, &[])
        .await
        .map_err(|error| FederationError::Postgres(error.to_string()))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(postgres_row_to_values(schema, &row)?);
    }
    let filtered = apply_filters_and_projection(schema, out, request)?;
    Ok(ForeignScanResult {
        stats: ForeignTableStats {
            row_count: Some(filtered.len() as u64),
            scanned_rows: filtered.len() as u64,
            scanned_bytes: 0,
            pushed_filters: request.filters.len(),
        },
        rows: filtered,
    })
}

#[must_use]
pub fn source_uses_secret(source: &ForeignSource) -> bool {
    matches!(
        source,
        ForeignSource::Postgres { .. } | ForeignSource::MultidbPgWire { .. }
    )
}

/// Validates a foreign source descriptor without exposing secret values.
/// # Errors
/// Fails when the descriptor is incomplete, unsupported, or references unsupported named secrets.
pub fn validate_foreign_source(source: &ForeignSource) -> Result<(), FederationError> {
    match source {
        ForeignSource::Parquet { uri, path }
        | ForeignSource::Csv { uri, path, .. }
        | ForeignSource::JsonLines { uri, path } => {
            if path.trim().is_empty() {
                return Err(FederationError::InvalidSource(
                    "foreign object path is required".to_owned(),
                ));
            }
            if !cfg!(feature = "cloud-object-store") && uri.scheme() != "file" {
                return Err(FederationError::Unsupported(format!(
                    "object store scheme {} requires the cloud-object-store feature",
                    uri.scheme()
                )));
            }
        }
        ForeignSource::Postgres { connection, table }
        | ForeignSource::MultidbPgWire { connection, table } => {
            validate_remote_identifier_path(table)?;
            match connection {
                SecretRef::EnvVar(name) => validate_secret_name(name)?,
                SecretRef::Named(name) => {
                    validate_secret_name(name)?;
                    return Err(FederationError::Unsupported(
                        "named foreign secrets require encrypted secret storage".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn read_object_bytes(uri: &ObjectStoreUri, path: &str) -> Result<Vec<u8>, FederationError> {
    let store = open_object_store(&ObjectStoreConfig { uri: uri.clone() })?;
    store.get(path).map_err(Into::into)
}

fn csv_bytes_to_rows(
    schema: &TableSchema,
    bytes: &[u8],
    has_header: bool,
) -> Result<Vec<Row>, FederationError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| FederationError::InvalidSource(error.to_string()))?;
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if index == 0 && has_header {
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let values = parse_csv_line(line);
        if values.len() != schema.columns.len() {
            return Err(FederationError::InvalidSource(format!(
                "CSV row has {} fields, expected {}",
                values.len(),
                schema.columns.len()
            )));
        }
        let mut row = Vec::with_capacity(values.len());
        for (raw, column) in values.iter().zip(&schema.columns) {
            row.push(csv_value_to_value(raw, &column.ty)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

fn jsonl_bytes_to_rows(schema: &TableSchema, bytes: &[u8]) -> Result<Vec<Row>, FederationError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| FederationError::InvalidSource(error.to_string()))?;
    let mut rows = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value = serde_json::from_str::<serde_json::Value>(line)
            .map_err(|error| FederationError::InvalidSource(error.to_string()))?;
        rows.push(
            json_object_to_row(Some(schema), &value)
                .map_err(|error| FederationError::InvalidSource(error.to_string()))?,
        );
    }
    Ok(rows)
}

fn csv_value_to_value(raw: &str, ty: &ColumnType) -> Result<Value, FederationError> {
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    match ty {
        ColumnType::Int => raw
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|error| FederationError::InvalidSource(error.to_string())),
        ColumnType::Float => raw
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|error| FederationError::InvalidSource(error.to_string())),
        ColumnType::Str => Ok(Value::Str(raw.to_owned())),
        ColumnType::Bool => raw
            .parse::<bool>()
            .map(Value::Bool)
            .map_err(|error| FederationError::InvalidSource(error.to_string())),
        ColumnType::Bytes => Ok(Value::Bytes(raw.as_bytes().to_vec())),
        ColumnType::Null => Ok(Value::Null),
    }
}

/// Applies validated simple filters and projection to decoded foreign rows.
/// # Errors
/// Fails when a requested filter or projection column is out of range.
pub fn apply_filters_and_projection(
    schema: &TableSchema,
    rows: Vec<Row>,
    request: &ForeignScanRequest,
) -> Result<Vec<Row>, FederationError> {
    let mut out = Vec::new();
    for row in rows {
        if request
            .filters
            .iter()
            .all(|filter| foreign_filter_matches(row.as_slice(), filter))
        {
            let row = if let Some(projection) = &request.projection {
                let mut projected = Vec::with_capacity(projection.len());
                for index in projection {
                    if *index >= schema.columns.len() {
                        return Err(FederationError::InvalidSource(format!(
                            "projection column {index} is out of range"
                        )));
                    }
                    projected.push(row.get(*index).cloned().unwrap_or(Value::Null));
                }
                projected
            } else {
                row
            };
            out.push(row);
            if request.limit.is_some_and(|limit| out.len() >= limit) {
                break;
            }
        }
    }
    Ok(out)
}

fn foreign_filter_matches(row: &[Value], filter: &ForeignFilter) -> bool {
    let Some(value) = row.get(filter.column) else {
        return false;
    };
    match filter.op {
        ForeignFilterOp::Eq => value == &filter.value,
        ForeignFilterOp::Lt | ForeignFilterOp::Le | ForeignFilterOp::Gt | ForeignFilterOp::Ge => {
            compare_values(value, &filter.value).is_some_and(|ord| {
                matches!(
                    (filter.op, ord),
                    (ForeignFilterOp::Lt, std::cmp::Ordering::Less)
                        | (
                            ForeignFilterOp::Le,
                            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                        )
                        | (ForeignFilterOp::Gt, std::cmp::Ordering::Greater)
                        | (
                            ForeignFilterOp::Ge,
                            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                        )
                )
            })
        }
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => left.partial_cmp(right),
        (Value::Str(left), Value::Str(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn remote_select_sql(
    schema: &TableSchema,
    table: &str,
    options: &ForeignTableOptions,
    request: &ForeignScanRequest,
) -> Result<String, FederationError> {
    validate_remote_identifier_path(table)?;
    let columns: Vec<String> = if let Some(projection) = request.projection.as_ref() {
        projection
            .iter()
            .map(|index| {
                schema
                    .columns
                    .get(*index)
                    .map(|column| quote_ident(&column.name))
                    .ok_or_else(|| {
                        FederationError::InvalidSource(format!(
                            "projection column {index} is out of range"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        schema
            .columns
            .iter()
            .map(|column| quote_ident(&column.name))
            .collect::<Vec<_>>()
    };
    let mut sql = format!("SELECT {} FROM {}", columns.join(", "), quote_path(table));
    let predicates = request
        .filters
        .iter()
        .map(|filter| remote_predicate_sql(schema, filter))
        .collect::<Result<Vec<_>, _>>()?;
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    let limit = request.limit.unwrap_or(options.remote_scan_limit);
    if limit == 0 && !options.allow_remote_without_limit {
        return Err(FederationError::InvalidSource(
            "remote foreign scan requires a non-zero limit".to_owned(),
        ));
    }
    if limit > 0 {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }
    Ok(sql)
}

fn remote_predicate_sql(
    schema: &TableSchema,
    filter: &ForeignFilter,
) -> Result<String, FederationError> {
    let column = schema
        .columns
        .get(filter.column)
        .ok_or_else(|| FederationError::InvalidSource("filter column out of range".to_owned()))?;
    Ok(format!(
        "{} {} {}",
        quote_ident(&column.name),
        match filter.op {
            ForeignFilterOp::Eq => "=",
            ForeignFilterOp::Lt => "<",
            ForeignFilterOp::Le => "<=",
            ForeignFilterOp::Gt => ">",
            ForeignFilterOp::Ge => ">=",
        },
        sql_literal(&filter.value)?
    ))
}

fn postgres_row_to_values(
    schema: &TableSchema,
    row: &tokio_postgres::Row,
) -> Result<Row, FederationError> {
    let mut values = Vec::with_capacity(schema.columns.len());
    for (index, column) in schema.columns.iter().enumerate() {
        let value = match column.ty {
            ColumnType::Int => row
                .try_get::<usize, Option<i64>>(index)
                .map_err(|error| FederationError::Postgres(error.to_string()))?
                .map_or(Value::Null, Value::Int),
            ColumnType::Float => row
                .try_get::<usize, Option<f64>>(index)
                .map_err(|error| FederationError::Postgres(error.to_string()))?
                .map_or(Value::Null, Value::Float),
            ColumnType::Str => row
                .try_get::<usize, Option<String>>(index)
                .map_err(|error| FederationError::Postgres(error.to_string()))?
                .map_or(Value::Null, Value::Str),
            ColumnType::Bool => row
                .try_get::<usize, Option<bool>>(index)
                .map_err(|error| FederationError::Postgres(error.to_string()))?
                .map_or(Value::Null, Value::Bool),
            ColumnType::Bytes => row
                .try_get::<usize, Option<Vec<u8>>>(index)
                .map_err(|error| FederationError::Postgres(error.to_string()))?
                .map_or(Value::Null, Value::Bytes),
            ColumnType::Null => Value::Null,
        };
        values.push(value);
    }
    Ok(values)
}

fn sql_literal(value: &Value) -> Result<String, FederationError> {
    match value {
        Value::Null => Ok("NULL".to_owned()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Int(value) => Ok(value.to_string()),
        Value::Float(value) if value.is_finite() => Ok(value.to_string()),
        Value::Str(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        Value::Bytes(value) => Ok(format!("'{}'", hex_bytes(value))),
        Value::Array(_)
        | Value::Object(_)
        | Value::Vector(_)
        | Value::GeoPoint { .. }
        | Value::Float(_) => Err(FederationError::Unsupported(
            "foreign pushdown supports scalar finite literals only".to_owned(),
        )),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0F)] as char);
    }
    out
}

fn resolve_secret(secret: &SecretRef) -> Result<String, FederationError> {
    match secret {
        SecretRef::EnvVar(name) => env::var(name).map_err(|_| {
            FederationError::InvalidSource(format!("missing environment secret {name}"))
        }),
        SecretRef::Named(name) => Err(FederationError::Unsupported(format!(
            "named secret {name} requires encrypted secret storage"
        ))),
    }
}

fn validate_secret_name(name: &str) -> Result<(), FederationError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .chars()
            .all(|ch| ch == '_' || ch == '-' || ch == '.' || ch.is_ascii_alphanumeric())
    {
        return Err(FederationError::InvalidSource(format!(
            "invalid secret reference {name}"
        )));
    }
    Ok(())
}

fn validate_remote_identifier_path(path: &str) -> Result<(), FederationError> {
    if path
        .split('.')
        .all(|part| !part.is_empty() && is_identifier(part))
    {
        Ok(())
    } else {
        Err(FederationError::InvalidSource(format!(
            "invalid remote table identifier {path}"
        )))
    }
}

fn quote_path(path: &str) -> String {
    path.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

#[must_use]
pub fn row_to_json_object(schema: &TableSchema, row: &[Value]) -> serde_json::Value {
    let fields = schema
        .columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| (column.name.clone(), value_to_json(value)))
        .collect::<BTreeMap<_, _>>();
    serde_json::Value::Object(fields.into_iter().collect())
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::Value::Number((*value).into()),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Str(value) => serde_json::Value::String(value.clone()),
        Value::Bytes(value) => serde_json::Value::String(hex_bytes(value)),
        Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(value_to_json).collect())
        }
        Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_to_json(value)))
                .collect(),
        ),
        Value::Vector(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|value| {
                    serde_json::Number::from_f64(f64::from(*value))
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                })
                .collect(),
        ),
        Value::GeoPoint { lon, lat } => serde_json::json!({ "lon": lon, "lat": lat }),
    }
}
