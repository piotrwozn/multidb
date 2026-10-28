use std::{collections::BTreeMap, env};

use crate::{
    cloud::ObjectStoreUri,
    model::Value,
    query::{ColumnType, QueryError, Row, TableSchema},
};

mod stream;
pub(crate) use stream::guarded_parquet_bytes_to_batches;
pub use stream::scan_foreign_table_guarded;

/// A statement's shared cancellation and memory accounting boundary.
pub trait ForeignScanGuard: Send + Sync {
    /// Checks the statement deadline and cancellation state.
    /// # Errors
    /// Fails after cancellation, timeout or an exhausted resource budget.
    fn check(&self) -> Result<(), QueryError>;
    /// Reserves retained result storage until the statement finishes.
    /// # Errors
    /// Fails when the statement cannot retain another result.
    fn charge(&self, retained_bytes: usize) -> Result<(), QueryError>;
    /// Reports the currently available shared statement memory.
    fn remaining_bytes(&self) -> usize;
    /// Reserves temporary decoder storage; dropping the token releases it.
    /// # Errors
    /// Fails before allocation when the shared budget cannot cover the decoder.
    fn reserve(&self, bytes: usize) -> Result<Box<dyn ForeignScanReservation>, QueryError>;
}

/// A type-erased reservation whose destructor releases temporary memory.
pub trait ForeignScanReservation: Send {}
impl<T: Send> ForeignScanReservation for T {}

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
    scan_foreign_table_guarded(spec, request, None).await
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
    stream::scan_local(schema, source, request, 256, &None)
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
    stream::scan_remote(schema, source, options, request, None).await
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
    validate_scan_request(schema, request)?;
    if request.limit == Some(0) {
        return Ok(Vec::new());
    }
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

fn validate_scan_request(
    schema: &TableSchema,
    request: &ForeignScanRequest,
) -> Result<(), FederationError> {
    for index in request
        .projection
        .iter()
        .flatten()
        .copied()
        .chain(request.filters.iter().map(|filter| filter.column))
    {
        if index >= schema.columns.len() {
            return Err(FederationError::InvalidSource(format!(
                "scan column {index} is out of range"
            )));
        }
    }
    Ok(())
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
        (Value::Bytes(left), Value::Bytes(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

struct RemotePlan<'a> {
    sql: String,
    needed: Vec<usize>,
    limit: Option<usize>,
    params: Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>,
    types: Vec<tokio_postgres::types::Type>,
    overflow_limit: Option<usize>,
}

#[cfg(test)]
fn remote_select_sql(
    schema: &TableSchema,
    table: &str,
    options: &ForeignTableOptions,
    request: &ForeignScanRequest,
) -> Result<String, FederationError> {
    remote_plan(schema, table, options, request).map(|plan| plan.sql)
}

#[cfg(test)]
fn remote_plan<'a>(
    schema: &TableSchema,
    table: &str,
    options: &ForeignTableOptions,
    request: &'a ForeignScanRequest,
) -> Result<RemotePlan<'a>, FederationError> {
    remote_plan_with_cap(schema, table, options, request, false)
}

fn remote_plan_with_cap<'a>(
    schema: &TableSchema,
    table: &str,
    options: &ForeignTableOptions,
    request: &'a ForeignScanRequest,
    guarded: bool,
) -> Result<RemotePlan<'a>, FederationError> {
    validate_remote_identifier_path(table)?;
    validate_scan_request(schema, request)?;
    let limit = request
        .limit
        .or_else(|| (options.remote_scan_limit != 0).then_some(options.remote_scan_limit));
    if limit.is_none() && !options.allow_remote_without_limit {
        return Err(FederationError::InvalidSource(
            "remote foreign scan requires a non-zero limit".to_owned(),
        ));
    }
    let overflow_limit = (guarded && request.limit.is_none() && options.remote_scan_limit != 0)
        .then_some(options.remote_scan_limit);
    let limit = if let Some(cap) = overflow_limit {
        Some(cap.checked_add(1).ok_or_else(|| {
            QueryError::ResourceLimit("foreign scan cap sentinel overflow".to_owned())
        })?)
    } else {
        limit
    };
    let mut needed = request
        .projection
        .clone()
        .unwrap_or_else(|| (0..schema.columns.len()).collect());
    needed.extend(request.filters.iter().map(|filter| filter.column));
    needed.sort_unstable();
    needed.dedup();
    let columns = if needed.is_empty() {
        "1".to_owned()
    } else {
        needed
            .iter()
            .map(|index| quote_ident(&schema.columns[*index].name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut plan = RemotePlan {
        sql: format!("SELECT {columns} FROM {}", quote_path(table)),
        needed,
        limit,
        params: Vec::new(),
        types: Vec::new(),
        overflow_limit,
    };
    let mut predicates = Vec::new();
    let mut exact = true;
    for filter in &request.filters {
        let (predicate, filter_exact) =
            remote_predicate(schema, filter, &mut plan.params, &mut plan.types)?;
        exact &= filter_exact;
        if let Some(predicate) = predicate {
            predicates.push(predicate);
        }
    }
    if !predicates.is_empty() {
        plan.sql.push_str(" WHERE ");
        plan.sql.push_str(&predicates.join(" AND "));
    }
    // Superset prefilters (locale text equality, float NaN ordering) and local
    // string ordering must not consume LIMIT before their residual is checked.
    if exact && let Some(limit) = limit {
        use std::fmt::Write;
        write!(&mut plan.sql, " LIMIT {limit}")
            .map_err(|error| FederationError::InvalidSource(error.to_string()))?;
    }
    Ok(plan)
}

fn remote_predicate<'a>(
    schema: &TableSchema,
    filter: &'a ForeignFilter,
    params: &mut Vec<&'a (dyn tokio_postgres::types::ToSql + Sync)>,
    types: &mut Vec<tokio_postgres::types::Type>,
) -> Result<(Option<String>, bool), FederationError> {
    use tokio_postgres::types::{ToSql, Type};
    let column = schema
        .columns
        .get(filter.column)
        .ok_or_else(|| FederationError::InvalidSource("filter column out of range".to_owned()))?;
    let name = quote_ident(&column.name);
    if filter.value == Value::Null {
        return Ok((
            Some(if filter.op == ForeignFilterOp::Eq {
                format!("{name} IS NULL")
            } else {
                "FALSE".to_owned()
            }),
            true,
        ));
    }
    if matches!(
        &filter.value,
        Value::Array(_) | Value::Object(_) | Value::Vector(_) | Value::GeoPoint { .. }
    ) || matches!(&filter.value, Value::Float(value) if !value.is_finite())
    {
        return Err(FederationError::Unsupported(
            "foreign pushdown supports scalar finite values only".to_owned(),
        ));
    }
    let (value, ty, exact): (&(dyn ToSql + Sync), Type, bool) = match (&column.ty, &filter.value) {
        (ColumnType::Int, Value::Int(value)) => (value, Type::INT8, true),
        (ColumnType::Bool, Value::Bool(value)) => (value, Type::BOOL, true),
        (ColumnType::Float, Value::Float(value)) => {
            (value, Type::FLOAT8, filter.op == ForeignFilterOp::Eq)
        }
        (ColumnType::Str, Value::Str(value)) if filter.op == ForeignFilterOp::Eq => {
            (value, Type::TEXT, false)
        }
        (ColumnType::Str, Value::Str(_)) => return Ok((None, false)),
        (ColumnType::Bytes, Value::Bytes(value)) => (value, Type::BYTEA, true),
        // Local foreign comparison does not coerce types.
        _ => return Ok((Some("FALSE".to_owned()), true)),
    };
    params.push(value);
    types.push(ty);
    let operator = match filter.op {
        ForeignFilterOp::Eq => "=",
        ForeignFilterOp::Lt => "<",
        ForeignFilterOp::Le => "<=",
        ForeignFilterOp::Gt => ">",
        ForeignFilterOp::Ge => ">=",
    };
    Ok((Some(format!("{name} {operator} ${}", params.len())), exact))
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

#[cfg(test)]
mod cap_tests;
#[cfg(test)]
mod tests;
