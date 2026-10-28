use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    thread,
};

use async_trait::async_trait;
use bytes::Bytes as ByteBuf;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Float64Array, Int64Array,
            NullArray, StringArray, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::Session,
    common::{
        Column as DfColumn, ScalarValue, Statistics, TableReference, stats::Precision,
        tree_node::TreeNodeRecursion,
    },
    datasource::{
        MemTable, TableProvider, TableType, cte_worktable::CteWorkTable, provider_as_source,
        source_as_provider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{disk_manager::DiskManagerBuilder, runtime_env::RuntimeEnvBuilder},
    logical_expr::{
        Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, lit,
    },
    physical_plan::ExecutionPlan,
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use futures::StreamExt;
use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use rayon::prelude::*;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, ConflictTarget, Expr as SqlExpr, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, ObjectNamePart, OnConflictAction,
    OnInsert, Query, SelectItem, SetExpr, Statement, TableObject as SqlTableObject, UnaryOperator,
    Value as SqlValue,
};

use crate::{
    cdc, cloud,
    federation::{self, ForeignScanRequest, ForeignTableSpec},
    keyenc,
    model::{
        CollectionId, DocumentCollection, DocumentId, FieldPath, IndexSpec, ModelError, Value,
        decode_value, encode_index_value, encode_value, extract_path,
    },
    observability,
    performance::{PerformanceConfig, split_into_partitions},
    repl::{
        ConditionalBatch, Op, ReadConsistency, ReplError, Replication, WriteCondition,
        propose_system,
    },
    security::AuthzError,
    storage::{Bytes, StorageError},
    temporal::{self, TemporalRetention},
};

mod batch_cache;
mod budget;
mod explain_fallback;
mod optimizer;
mod parser;
mod row_scan;
mod simple_grammar;
mod write_batch;

pub use optimizer::{
    AccessPath, AnalyzeMode, AnalyzeReport, AnalyzeTarget, Bucket, CachedPlan,
    CardinalityEstimator, ColumnStats, Cost, CostCoefficients, CostModel, CostProfile,
    EqPathRequest, EqPlan, ExplainNode, ExplainOptions, ExplainReport, FilterPathRequest,
    MostCommonValue, PLANNER_FEEDBACK_TABLE, PLANNER_META_TABLE, PlanCache, PlanCacheMetrics,
    PlanDependency, PlannerFeedback, QueryFingerprint, STATS_TABLE, SimpleSelectPlan, StatsCatalog,
    StatsObjectKind, TableStats,
};
pub(crate) use parser::parse_with_limits;
pub use parser::{parse, parse_analyze_for_authz};

use parser::{parse_analyze_command, parse_limit};

pub const REL_ROWS_TABLE: &str = "rel_rows";
pub const REL_COLUMNAR_SEGMENTS_TABLE: &str = "rel_columnar_segments";
pub const REL_COLUMNAR_SEGMENT_META_TABLE: &str = "rel_columnar_segment_meta";
pub const REL_INDEX_TABLE: &str = "rel_indexes";
pub const REL_SCHEMA_TABLE: &str = "__schema__";

const EMPTY_INDEX_VALUE: &[u8] = b"";
const COLUMNAR_SEGMENT_ROWS: usize = 1_024;
/// How many times a write that lost a race re-reads and tries again before it
/// reports a serialization conflict.
const WRITE_CONFLICT_RETRIES: usize = 64;
/// How many times a pruned columnar scan reads segments and zone maps before it
/// gives up on pruning because a commit landed between the reads every time.
/// Any commit counts, so a higher value mostly re-reads every segment again.
const SEGMENT_PRUNING_READ_ATTEMPTS: usize = 2;

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ColumnType {
    Int,
    Float,
    Str,
    Bool,
    Bytes,
    Null,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
    pub primary_key: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelIndexExpression {
    Column(usize),
    LowerAscii(usize),
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelPredicate {
    Eq {
        expression: RelIndexExpression,
        value: Value,
    },
    And(Vec<RelPredicate>),
    Or(Vec<RelPredicate>),
}

impl Eq for RelPredicate {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum BitmapOp {
    And,
    Or,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RelIndexSpec {
    pub id: u32,
    pub column: usize,
    pub expression: RelIndexExpression,
    pub predicate: Option<RelPredicate>,
    pub include: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TableLayout {
    #[default]
    Row,
    Columnar,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum DocFieldSource {
    DocumentId,
    Path(FieldPath),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DocField {
    pub name: String,
    pub source: DocFieldSource,
    pub ty: ColumnType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelPlanKind {
    IndexScan(u32),
    IndexOnlyScan(u32),
    BitmapIndex { op: BitmapOp, indexes: Vec<u32> },
    FullScan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelQueryResult {
    pub plan: RelPlanKind,
    pub examined_rows: usize,
    pub rows: Vec<Row>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SegmentColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ColumnarSegmentMeta {
    pub table: String,
    pub segment_id: u64,
    pub row_count: u64,
    pub bytes: u64,
    pub columns: Vec<SegmentColumnStats>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SegmentSkipReport {
    pub scanned_segments: usize,
    pub skipped_segments: usize,
    pub scanned_bytes: u64,
    pub skipped_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SqlRows {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlOutput {
    Rows(SqlRows),
    AffectedRows(usize),
}

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("model: {0}")]
    Model(#[from] ModelError),

    #[error("sql parse: {0}")]
    Parse(String),

    #[error("query input limit: {0}")]
    InputLimit(String),

    #[error("query resource limit: {0}")]
    ResourceLimit(String),

    /// The query governor had no free statement slot within its queue timeout.
    /// Nothing ran, so the statement can be retried.
    #[error("query governor busy: {0}")]
    Busy(String),

    #[error("query cancelled: {0}")]
    Cancelled(String),

    #[error("query timeout: {0}")]
    Timeout(String),

    #[error("unsupported SQL: {0}")]
    Unsupported(String),

    #[error("datafusion: {0}")]
    DataFusion(String),

    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    #[error("invalid row: {0}")]
    InvalidRow(String),

    #[error("missing table: {0}")]
    MissingTable(String),

    #[error("missing column: {0}")]
    MissingColumn(String),

    #[error("duplicate primary key")]
    DuplicatePrimaryKey,

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("metadata serialization: {0}")]
    Serde(String),

    #[error("authorization denied: {0}")]
    AuthzDenied(AuthzError),
}

/// A relation that a planned SQL statement reads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SqlReadTarget {
    /// A catalog object registered with the engine, named as it was registered.
    Catalog(String),
    /// A relation outside the catalog, such as `information_schema` or a table function.
    Builtin(String),
}

/// The row and masking policies a caller's SQL sees on one catalog relation.
///
/// Paths name document fields for collections and columns for every other relation. The
/// engine applies the policies where it scans the relation, so the rest of the query only
/// ever reads the rows they let through, with the masked values already replaced.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SqlRelationPolicy {
    /// A row is visible only if, for every list, its value at one of the list's paths equals
    /// the paired value. An empty list hides every row; no lists show every row. Collections
    /// decide this on the stored document.
    pub visible_when: Vec<Vec<(FieldPath, Value)>>,
    /// Values at or below each path are replaced with the paired value, later entries last.
    pub masks: Vec<(FieldPath, Value)>,
}

/// Authorizes the relations of each SQL statement before any statement runs.
pub trait SqlReadGuard: Send {
    /// Checks every relation one planned statement reads.
    /// An empty set means the statement reads no relation, for example `SELECT 1`.
    /// # Errors
    /// Fails when the caller may not read one of `reads`.
    fn authorize_reads(&mut self, reads: &BTreeSet<SqlReadTarget>) -> Result<(), AuthzError>;

    /// Returns the row and masking policies of the caller, keyed by catalog name. Relations
    /// without an entry are read as stored.
    fn relation_policies(&self) -> BTreeMap<String, SqlRelationPolicy> {
        BTreeMap::new()
    }
}

enum PreparedStatement {
    Select(PreparedSelect),
    Analyze(AnalyzeTarget),
    Explain(explain_fallback::PreparedExplain),
    Insert(Box<sqlparser::ast::Insert>),
}

enum PreparedSelect {
    RowRange(Box<row_scan::RowRangePlan>),
    Simple {
        sql: String,
        plan: Box<SimpleSelectPlan>,
    },
    DataFusion {
        ctx: SessionContext,
        plan: Box<LogicalPlan>,
        /// Time already spent planning; execution gets the rest of the query timeout.
        planning: std::time::Duration,
        budget: Arc<budget::Budget>,
    },
}

type RegisteredProviders = Vec<(String, Arc<dyn TableProvider>)>;

#[derive(Clone)]
pub struct RelTable {
    repl: Arc<dyn Replication>,
    name: String,
    schema: Option<TableSchema>,
    indexes: Vec<RelIndexSpec>,
    layout: TableLayout,
}

#[derive(Clone)]
pub struct SqlEngine {
    repl: Arc<dyn Replication>,
    tables: BTreeMap<String, RelTable>,
    collections: BTreeMap<String, DocCollectionProvider>,
    foreign_tables: BTreeMap<String, ForeignTableProvider>,
    materialized_views: BTreeMap<String, MaterializedViewProvider>,
    temporal_tables: BTreeMap<String, TemporalTableProvider>,
    layout: TableLayout,
    cost_model: CostModel,
    plan_cache: Arc<Mutex<PlanCache>>,
    batch_cache: Arc<batch_cache::BatchCache>,
    performance: PerformanceConfig,
    snapshot_lsn: Option<u64>,
}

#[derive(Clone)]
struct StorageTableProvider {
    table: RelTable,
    arrow_schema: SchemaRef,
    performance: PerformanceConfig,
    snapshot_lsn: Option<u64>,
    batch_cache: Arc<batch_cache::BatchCache>,
    budget: Arc<budget::Budget>,
}

#[derive(Clone)]
pub struct DocCollectionProvider {
    repl: Arc<dyn Replication>,
    collection_id: CollectionId,
    fields: Vec<DocField>,
    indexes: Vec<IndexSpec>,
    arrow_schema: SchemaRef,
    /// Row policies decided on each stored document, as in `SqlRelationPolicy::visible_when`.
    visible_when: Arc<[Vec<(FieldPath, Value)>]>,
}

#[derive(Clone)]
pub struct ForeignTableProvider {
    spec: ForeignTableSpec,
    arrow_schema: SchemaRef,
}

#[derive(Clone)]
pub struct MaterializedViewProvider {
    repl: Arc<dyn Replication>,
    spec: cdc::MaterializedViewSpec,
    arrow_schema: SchemaRef,
}

#[derive(Clone)]
pub struct TemporalTableProvider {
    repl: Arc<dyn Replication>,
    base_table: String,
    base_schema: TableSchema,
    retention: TemporalRetention,
    arrow_schema: SchemaRef,
}

impl ColumnDef {
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
        }
    }
}

impl TableSchema {
    #[must_use]
    pub fn new(columns: Vec<ColumnDef>, primary_key: usize) -> Self {
        Self {
            columns,
            primary_key,
        }
    }

    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    #[must_use]
    pub fn primary_key_column(&self) -> Option<&ColumnDef> {
        self.columns.get(self.primary_key)
    }
}

impl RelIndexSpec {
    #[must_use]
    pub fn new(id: u32, column: usize) -> Self {
        Self {
            id,
            column,
            expression: RelIndexExpression::Column(column),
            predicate: None,
            include: Vec::new(),
        }
    }

    #[must_use]
    pub fn lower_ascii(id: u32, column: usize) -> Self {
        Self {
            id,
            column,
            expression: RelIndexExpression::LowerAscii(column),
            predicate: None,
            include: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_predicate(mut self, predicate: RelPredicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    #[must_use]
    pub fn with_include(mut self, include: Vec<usize>) -> Self {
        self.include = include;
        self
    }

    #[must_use]
    pub fn covers_projection(&self, projection: &[usize]) -> bool {
        projection.iter().all(|column| self.covers_column(*column))
    }

    #[must_use]
    pub fn covers_column(&self, column: usize) -> bool {
        self.include.contains(&column)
            || matches!(self.expression, RelIndexExpression::Column(indexed) if indexed == column)
    }
}

impl<'de> serde::Deserialize<'de> for RelIndexSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RawRelIndexSpec {
            id: u32,
            column: usize,
            expression: Option<RelIndexExpression>,
            #[serde(default)]
            predicate: Option<RelPredicate>,
            #[serde(default)]
            include: Vec<usize>,
        }

        let raw = RawRelIndexSpec::deserialize(deserializer)?;
        Ok(Self {
            id: raw.id,
            column: raw.column,
            expression: raw
                .expression
                .unwrap_or(RelIndexExpression::Column(raw.column)),
            predicate: raw.predicate,
            include: raw.include,
        })
    }
}

impl RelIndexExpression {
    #[must_use]
    pub const fn column(self) -> usize {
        match self {
            Self::Column(column) | Self::LowerAscii(column) => column,
        }
    }
}

impl DocField {
    #[must_use]
    pub fn new(name: impl Into<String>, source: DocFieldSource, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            source,
            ty,
        }
    }

    #[must_use]
    pub fn path(name: impl Into<String>, path: FieldPath, ty: ColumnType) -> Self {
        Self::new(name, DocFieldSource::Path(path), ty)
    }

    #[must_use]
    pub fn document_id(name: impl Into<String>) -> Self {
        Self::new(name, DocFieldSource::DocumentId, ColumnType::Bytes)
    }
}

impl RelTable {
    /// Builds a table handle with an explicit physical layout.
    /// # Errors
    /// Fails when the table name, schema, indexes, or layout are invalid.
    pub(crate) fn handle_with_layout(
        repl: Arc<dyn Replication>,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
        layout: TableLayout,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        validate_table_name(&name)?;
        if let Some(schema) = &schema {
            validate_schema(schema)?;
        }
        validate_layout(schema.as_ref(), &indexes, layout)?;

        Ok(Self {
            repl,
            name,
            schema,
            indexes,
            layout,
        })
    }

    /// Creates a table handle and writes schema metadata when a schema exists.
    /// # Errors
    /// Fails when schema validation, serialization, or storage fails.
    pub fn create(
        repl: Arc<dyn Replication>,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
    ) -> Result<Self, QueryError> {
        Self::create_with_layout(repl, name, schema, indexes, TableLayout::Row)
    }

    /// Creates a table handle with an explicit layout and writes schema metadata.
    /// # Errors
    /// Fails when schema validation, layout validation, serialization, or storage fails.
    pub fn create_with_layout(
        repl: Arc<dyn Replication>,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
        layout: TableLayout,
    ) -> Result<Self, QueryError> {
        let table = Self::handle_with_layout(repl, name, schema, indexes, layout)?;

        if read_schema(table.repl.as_ref(), &table.name)?.is_some() {
            return Err(QueryError::InvalidSchema(format!(
                "table {} already exists",
                table.name
            )));
        }

        if let Some(schema) = &table.schema {
            propose_system(table.repl.as_ref(), schema_put_op(&table.name, schema)?)?;
        }

        Ok(table)
    }

    /// Opens a table handle and loads persisted schema metadata when present.
    /// # Errors
    /// Fails when metadata cannot be read or decoded.
    pub fn open(
        repl: Arc<dyn Replication>,
        name: impl Into<String>,
        indexes: Vec<RelIndexSpec>,
    ) -> Result<Self, QueryError> {
        Self::open_with_layout(repl, name, indexes, TableLayout::Row)
    }

    /// Opens a table handle with an explicit layout and loads persisted schema metadata.
    /// # Errors
    /// Fails when metadata cannot be read, decoded, or does not fit the layout.
    pub fn open_with_layout(
        repl: Arc<dyn Replication>,
        name: impl Into<String>,
        indexes: Vec<RelIndexSpec>,
        layout: TableLayout,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        validate_table_name(&name)?;
        let schema = read_schema(repl.as_ref(), &name)?;
        if let Some(schema) = &schema {
            validate_schema(schema)?;
        }
        validate_layout(schema.as_ref(), &indexes, layout)?;

        Ok(Self {
            repl,
            name,
            schema,
            indexes,
            layout,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    #[must_use]
    pub fn indexes(&self) -> &[RelIndexSpec] {
        &self.indexes
    }

    #[must_use]
    pub const fn layout(&self) -> TableLayout {
        self.layout
    }

    /// Inserts a new row.
    /// # Errors
    /// Fails if the row is invalid, the primary key exists, or storage fails.
    /// A columnar table also fails with a conflict when other writers keep changing it.
    pub fn insert(&self, row: Row) -> Result<(), QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.append_columnar_rows(&[row]);
        }

        let row_key = self.row_key(&row)?;
        if self.get_by_row_key(&row_key)?.is_some() {
            return Err(QueryError::DuplicatePrimaryKey);
        }
        let ops = self.put_ops(row, &row_key)?;
        let condition = WriteCondition::KeyMissing {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key,
        };
        match self
            .repl
            .propose_conditional_batch(ConditionalBatch::new(vec![condition], ops.clone()))
        {
            Ok(()) => {}
            Err(ReplError::Conflict) => return Err(QueryError::DuplicatePrimaryKey),
            Err(ReplError::Unsupported(_)) => self.repl.propose_batch(ops)?,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Builds all operations needed to insert a row.
    /// # Errors
    /// Fails if the row is invalid or the primary key already exists.
    pub fn insert_ops(&self, row: Row) -> Result<Vec<Op>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_insert_ops(row);
        }

        let row_key = self.row_key(&row)?;
        if self.get_by_row_key(&row_key)?.is_some() {
            return Err(QueryError::DuplicatePrimaryKey);
        }

        self.put_ops(row, &row_key)
    }

    /// Replaces a row or creates it if it does not exist.
    /// # Errors
    /// Fails when row validation, serialization, or storage fails.
    /// A columnar table also fails with a conflict when other writers keep changing it.
    pub fn update(&self, row: Row) -> Result<(), QueryError> {
        if self.layout == TableLayout::Columnar {
            let primary_key = self.primary_key_value_for_row(&row)?;
            return self.rewrite_columnar_rows(|rows| {
                if let Some(index) = row_with_primary_key(self.schema.as_ref(), rows, &primary_key)
                {
                    rows[index].clone_from(&row);
                } else {
                    rows.push(row.clone());
                }
                Ok(true)
            });
        }

        let row_key = self.row_key(&row)?;
        let old_bytes = self
            .repl
            .read(REL_ROWS_TABLE, &row_key, ReadConsistency::Strong)?;
        let ops = match old_bytes.as_deref().map(decode_row).transpose()?.flatten() {
            Some(old) => self.update_ops_for_key(&old, row, &row_key)?,
            None => self.put_ops(row, &row_key)?,
        };
        let condition = WriteCondition::ValueEquals {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key,
            expected: old_bytes,
        };
        match self
            .repl
            .propose_conditional_batch(ConditionalBatch::new(vec![condition], ops.clone()))
        {
            Ok(()) => {}
            Err(ReplError::Unsupported(_)) => self.repl.propose_batch(ops)?,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Builds all operations needed to update a row.
    /// # Errors
    /// Fails when row validation, serialization, or storage fails.
    pub fn update_ops(&self, row: Row) -> Result<Vec<Op>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_update_ops(row);
        }

        let row_key = self.row_key(&row)?;
        match self.get_by_row_key(&row_key)? {
            Some(old) => self.update_ops_for_key(&old, row, &row_key),
            None => self.put_ops(row, &row_key),
        }
    }

    /// Deletes a row by primary key.
    /// # Errors
    /// Fails when key encoding or storage fails.
    /// A columnar table also fails with a conflict when other writers keep changing it.
    pub fn delete(&self, primary_key: &Value) -> Result<(), QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.rewrite_columnar_rows(|rows| {
                let before = rows.len();
                rows.retain(|row| !row_primary_key_eq(self.schema.as_ref(), row, primary_key));
                Ok(rows.len() != before)
            });
        }

        let row_key = make_row_key(&self.name, primary_key)?;
        let Some(old_bytes) = self
            .repl
            .read(REL_ROWS_TABLE, &row_key, ReadConsistency::Strong)?
        else {
            return Ok(());
        };
        let old = decode_row(&old_bytes)?.ok_or_else(|| {
            QueryError::Storage(StorageError::Corruption(
                "relational row is missing".to_owned(),
            ))
        })?;
        let mut ops = Vec::new();
        self.append_index_delete_ops(&mut ops, &old, &row_key)?;
        ops.push(Op::Delete {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key.clone(),
        });
        let condition = WriteCondition::ValueEquals {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key,
            expected: Some(old_bytes),
        };
        match self
            .repl
            .propose_conditional_batch(ConditionalBatch::new(vec![condition], ops.clone()))
        {
            Ok(()) => {}
            Err(ReplError::Unsupported(_)) => self.repl.propose_batch(ops)?,
            Err(error) => return Err(error.into()),
        }

        Ok(())
    }

    /// Builds all operations needed to delete a row.
    /// # Errors
    /// Fails when key encoding or storage fails.
    pub fn delete_ops(&self, primary_key: &Value) -> Result<Vec<Op>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_delete_ops(primary_key);
        }

        let row_key = make_row_key(&self.name, primary_key)?;
        let mut ops = Vec::new();

        if let Some(old) = self.get_by_row_key(&row_key)? {
            self.append_index_delete_ops(&mut ops, &old, &row_key)?;
            ops.push(Op::Delete {
                table: REL_ROWS_TABLE.to_owned(),
                key: row_key,
            });
        }

        Ok(ops)
    }

    pub(crate) fn row_key_for_row(&self, row: &Row) -> Result<Bytes, QueryError> {
        self.row_key(row)
    }

    pub(crate) fn row_key_for_primary_key(&self, primary_key: &Value) -> Result<Bytes, QueryError> {
        make_row_key(&self.name, primary_key)
    }

    pub(crate) fn put_ops_for_key(&self, row: Row, row_key: &[u8]) -> Result<Vec<Op>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_update_ops(row);
        }

        self.put_ops(row, row_key)
    }

    pub(crate) fn update_ops_for_key(
        &self,
        old: &Row,
        row: Row,
        row_key: &[u8],
    ) -> Result<Vec<Op>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_update_ops(row);
        }
        write_batch::update_ops_for_key(self, old, row, row_key)
    }

    pub(crate) fn index_delete_ops_for_key(
        &self,
        row: &Row,
        row_key: &[u8],
    ) -> Result<Vec<Op>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return Ok(Vec::new());
        }

        let mut ops = Vec::new();
        self.append_index_delete_ops(&mut ops, row, row_key)?;
        Ok(ops)
    }

    /// Reads a row by primary key.
    /// # Errors
    /// Fails when key encoding, storage, or decoding fails.
    pub fn get(&self, primary_key: &Value) -> Result<Option<Row>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_get(primary_key);
        }

        let row_key = make_row_key(&self.name, primary_key)?;
        self.get_by_row_key(&row_key)
    }

    /// Scans all rows in primary-key order.
    /// # Errors
    /// Fails when storage or decoding fails.
    pub fn scan(&self) -> Result<Vec<Row>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_rows();
        }

        self.scan_with_keys()
            .map(|rows| rows.into_iter().map(|(_, row)| row).collect())
    }

    /// Counts stored rows without decoding every row when segment metadata is available.
    /// # Errors
    /// Fails when storage, segment metadata, or a legacy segment cannot be read.
    pub fn row_count(&self) -> Result<usize, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.columnar_segments_with_meta()?.into_iter().try_fold(
                0_usize,
                |count, (_, bytes, meta)| {
                    let rows = match meta {
                        Some(meta) => usize::try_from(meta.row_count).map_err(|_| {
                            QueryError::ResourceLimit("table row count exceeds usize".to_owned())
                        })?,
                        None => self.decode_columnar_rows(Some(&bytes))?.len(),
                    };
                    count.checked_add(rows).ok_or_else(|| {
                        QueryError::ResourceLimit("table row count exceeds usize".to_owned())
                    })
                },
            );
        }
        let (start, end) = table_range_bounds(&self.name);
        let mut count = 0_usize;
        self.repl.scan_range_batches(
            REL_ROWS_TABLE,
            &start,
            &end,
            ReadConsistency::Strong,
            COLUMNAR_SEGMENT_ROWS,
            &|| false,
            &mut |batch| {
                count = count.checked_add(batch.len()).ok_or_else(|| {
                    ReplError::Unsupported("table row count exceeds usize".to_owned())
                })?;
                Ok(true)
            },
        )?;
        Ok(count)
    }

    /// Scans a bounded page of rows in primary-key order.
    /// # Errors
    /// Fails when storage or decoding fails.
    pub fn scan_page(&self, offset: usize, limit: usize) -> Result<Vec<Row>, QueryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if self.layout == TableLayout::Columnar {
            return Ok(self
                .columnar_rows()?
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect());
        }

        let read_limit = offset.saturating_add(limit);
        self.scan_with_keys_limit(Some(read_limit), COLUMNAR_SEGMENT_ROWS)
            .map(|rows| rows.into_iter().skip(offset).map(|(_, row)| row).collect())
    }

    /// Queries equality on one column. Uses a relational index when available.
    /// # Errors
    /// Fails when index encoding, storage, or decoding fails.
    pub fn query_eq(&self, column: usize, value: &Value) -> Result<RelQueryResult, QueryError> {
        let stats = StatsCatalog::read_table(self.repl.as_ref(), &self.name)?;
        let cost_model = CostModel::new(CostProfile::Balanced);
        self.query_eq_with_cost_model(column, value, &cost_model, stats.as_ref())
            .map(|(result, _)| result)
    }

    /// Collects and persists optimizer statistics for this table.
    /// # Errors
    /// Fails when rows cannot be scanned or statistics cannot be written.
    pub fn analyze(&self, mode: AnalyzeMode) -> Result<TableStats, QueryError> {
        let rows = self.scan()?;
        let schema_columns = stats_columns_for_schema(self.schema.as_ref(), &rows);
        let stats_version = StatsCatalog::next_stats_version(self.repl.as_ref())?;
        let stats = optimizer::build_table_stats(
            &self.name,
            StatsObjectKind::Table,
            &schema_columns,
            &rows,
            mode,
            stats_version,
            stats_version,
        );
        StatsCatalog::write_table(self.repl.as_ref(), &stats)?;
        Ok(stats)
    }

    fn query_eq_with_cost_model(
        &self,
        column: usize,
        value: &Value,
        cost_model: &CostModel,
        stats: Option<&TableStats>,
    ) -> Result<(RelQueryResult, AccessPath), QueryError> {
        validate_column(self.schema.as_ref(), column)?;
        let column_name = column_name(self.schema.as_ref(), column);
        let path = cost_model.choose_eq_path_with_schema(
            EqPathRequest {
                layout: self.layout,
                indexes: &self.indexes,
                column,
                column_name: &column_name,
                value,
                stats,
                projected_columns: self
                    .schema
                    .as_ref()
                    .map_or(1, |schema| schema.columns.len()),
            },
            self.schema.as_ref(),
        );

        let result = self.query_eq_with_access_path(column, value, &path)?;

        Ok((result, path))
    }

    fn query_eq_with_access_path(
        &self,
        column: usize,
        value: &Value,
        path: &AccessPath,
    ) -> Result<RelQueryResult, QueryError> {
        validate_column(self.schema.as_ref(), column)?;
        let filter = RelPredicate::Eq {
            expression: RelIndexExpression::Column(column),
            value: value.clone(),
        };
        let projection = self
            .schema
            .as_ref()
            .map_or_else(|| vec![0], |schema| (0..schema.columns.len()).collect());
        self.query_filter_with_access_path(&filter, &projection, path)
    }

    fn query_filter_with_access_path(
        &self,
        filter: &RelPredicate,
        projection: &[usize],
        path: &AccessPath,
    ) -> Result<RelQueryResult, QueryError> {
        match path {
            AccessPath::BTreeIndex { index_id, .. } if self.layout == TableLayout::Row => {
                self.query_filter_index(*index_id, filter)
            }
            AccessPath::IndexOnly { index_id, .. } if self.layout == TableLayout::Row => {
                self.query_filter_index_only(*index_id, filter, projection)
            }
            AccessPath::BitmapIndex { op, index_ids, .. } if self.layout == TableLayout::Row => {
                self.query_filter_bitmap(*op, index_ids, filter)
            }
            _ => self.query_filter_full_scan(filter),
        }
    }

    fn query_filter_index(
        &self,
        index_id: u32,
        filter: &RelPredicate,
    ) -> Result<RelQueryResult, QueryError> {
        let index = self.index_by_id(index_id)?;
        let value = filter_eq_for_expression(filter, index.expression)
            .ok_or_else(|| QueryError::Unsupported("index does not match filter".to_owned()))?;
        let prefix = make_index_prefix(&self.name, index_id, value)?;
        let end = keyenc::range_end(&prefix);
        let entries = self
            .repl
            .range(REL_INDEX_TABLE, &prefix, &end, ReadConsistency::Strong)?;

        let row_keys: Vec<Bytes> = entries
            .into_iter()
            .filter_map(|(key, _)| row_key_from_index_key(&key).map(<[u8]>::to_vec))
            .collect();
        let examined_rows = row_keys.len();
        let mut rows = Vec::new();
        for keys in row_keys.chunks(256) {
            let values = self
                .repl
                .read_many(REL_ROWS_TABLE, keys, ReadConsistency::Strong)?;
            if values.len() != keys.len() {
                return Err(QueryError::Storage(StorageError::Corruption(format!(
                    "index row fetch returned {} values for {} keys",
                    values.len(),
                    keys.len(),
                ))));
            }
            for bytes in values.into_iter().flatten() {
                if let Some(row) = decode_row(&bytes)?
                    && predicate_matches(&row, filter)?
                {
                    rows.push(row);
                }
            }
        }

        Ok(RelQueryResult {
            plan: RelPlanKind::IndexScan(index_id),
            examined_rows,
            rows,
        })
    }

    fn query_filter_index_only(
        &self,
        index_id: u32,
        filter: &RelPredicate,
        projection: &[usize],
    ) -> Result<RelQueryResult, QueryError> {
        let index = self.index_by_id(index_id)?;
        let value = filter_eq_for_expression(filter, index.expression)
            .ok_or_else(|| QueryError::Unsupported("index does not match filter".to_owned()))?;
        let prefix = make_index_prefix(&self.name, index_id, value)?;
        let end = keyenc::range_end(&prefix);
        let entries = self
            .repl
            .range(REL_INDEX_TABLE, &prefix, &end, ReadConsistency::Strong)?;
        let width = self
            .schema
            .as_ref()
            .map_or(1, |schema| schema.columns.len());
        let mut rows = Vec::new();
        let mut examined_rows = 0;
        for (key, payload) in entries {
            let Some(row_key) = row_key_from_index_key(&key) else {
                continue;
            };
            examined_rows += 1;
            let payload = decode_index_payload(&payload)?;
            let row = reconstruct_covering_row(width, index, value, &payload)?;
            if index.covers_projection(projection) && predicate_matches(&row, filter)? {
                rows.push(row);
            } else if let Some(row) = self.get_by_row_key(row_key)?
                && predicate_matches(&row, filter)?
            {
                rows.push(row);
            }
        }

        Ok(RelQueryResult {
            plan: RelPlanKind::IndexOnlyScan(index_id),
            examined_rows,
            rows,
        })
    }

    fn query_filter_bitmap(
        &self,
        op: BitmapOp,
        index_ids: &[u32],
        filter: &RelPredicate,
    ) -> Result<RelQueryResult, QueryError> {
        let mut sets = Vec::new();
        for index_id in index_ids {
            let index = self.index_by_id(*index_id)?;
            let Some(value) = filter_eq_for_expression_for_bitmap(filter, index.expression, op)
            else {
                continue;
            };
            let prefix = make_index_prefix(&self.name, *index_id, value)?;
            let end = keyenc::range_end(&prefix);
            let keys = self
                .repl
                .range(REL_INDEX_TABLE, &prefix, &end, ReadConsistency::Strong)?
                .into_iter()
                .filter_map(|(key, _)| row_key_from_index_key(&key).map(Bytes::from))
                .collect::<BTreeSet<_>>();
            sets.push(keys);
        }

        let row_keys = combine_bitmap_sets(op, sets);
        let mut rows = Vec::new();
        for row_key in &row_keys {
            if let Some(row) = self.get_by_row_key(row_key)?
                && predicate_matches(&row, filter)?
            {
                rows.push(row);
            }
        }

        Ok(RelQueryResult {
            plan: RelPlanKind::BitmapIndex {
                op,
                indexes: index_ids.to_vec(),
            },
            examined_rows: row_keys.len(),
            rows,
        })
    }

    fn query_filter_full_scan(&self, filter: &RelPredicate) -> Result<RelQueryResult, QueryError> {
        if self.layout == TableLayout::Columnar {
            return self.query_filter_columnar(filter);
        }
        let mut rows = Vec::new();
        let mut examined_rows = 0;
        for row in self.scan()? {
            examined_rows += 1;
            if predicate_matches(&row, filter)? {
                rows.push(row);
            }
        }

        Ok(RelQueryResult {
            plan: RelPlanKind::FullScan,
            examined_rows,
            rows,
        })
    }

    fn query_filter_columnar(&self, filter: &RelPredicate) -> Result<RelQueryResult, QueryError> {
        let segments = self.columnar_segments_for_filter(filter)?;
        let mut rows = Vec::new();
        let mut examined_rows = 0;
        for (_, bytes) in segments {
            for row in self.decode_columnar_rows(Some(&bytes))? {
                examined_rows += 1;
                if predicate_matches(&row, filter)? {
                    rows.push(row);
                }
            }
        }
        let rows = self.sorted_columnar_rows(rows)?;
        Ok(RelQueryResult {
            plan: RelPlanKind::FullScan,
            examined_rows,
            rows,
        })
    }

    fn index_by_id(&self, index_id: u32) -> Result<&RelIndexSpec, QueryError> {
        self.indexes
            .iter()
            .find(|index| index.id == index_id)
            .ok_or_else(|| QueryError::InvalidSchema(format!("missing index {index_id}")))
    }

    fn row_key(&self, row: &Row) -> Result<Bytes, QueryError> {
        validate_row(self.schema.as_ref(), row)?;
        let pk = primary_key_value(self.schema.as_ref(), row)?;
        make_row_key(&self.name, pk)
    }

    fn put_ops(&self, row: Row, row_key: &[u8]) -> Result<Vec<Op>, QueryError> {
        let mut ops = Vec::new();
        self.append_put_ops(&mut ops, row, row_key)?;
        Ok(ops)
    }

    fn append_put_ops(
        &self,
        ops: &mut Vec<Op>,
        row: Row,
        row_key: &[u8],
    ) -> Result<(), QueryError> {
        self.append_index_put_ops(ops, &row, row_key)?;
        ops.push(Op::Put {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key.to_vec(),
            value: encode_value(&Value::Array(row))?,
        });
        Ok(())
    }

    fn get_by_row_key(&self, row_key: &[u8]) -> Result<Option<Row>, QueryError> {
        let Some(bytes) = self
            .repl
            .read(REL_ROWS_TABLE, row_key, ReadConsistency::Strong)?
        else {
            return Ok(None);
        };

        decode_row(&bytes)
    }

    fn scan_with_keys(&self) -> Result<Vec<(Bytes, Row)>, QueryError> {
        self.scan_with_keys_limit(None, COLUMNAR_SEGMENT_ROWS)
    }

    fn scan_with_keys_limit(
        &self,
        limit: Option<usize>,
        batch_rows: usize,
    ) -> Result<Vec<(Bytes, Row)>, QueryError> {
        let (start, end) = table_range_bounds(&self.name);
        let mut rows = Vec::new();
        let mut decode_error = None;
        self.repl.scan_range_batches(
            REL_ROWS_TABLE,
            &start,
            &end,
            ReadConsistency::Strong,
            batch_rows,
            &|| false,
            &mut |batch| {
                for (key, value) in batch {
                    let row = match decode_row(value) {
                        Ok(Some(row)) => row,
                        Ok(None) => {
                            decode_error = Some(QueryError::Storage(StorageError::Corruption(
                                "relational row is missing".to_owned(),
                            )));
                            return Ok(false);
                        }
                        Err(error) => {
                            decode_error = Some(error);
                            return Ok(false);
                        }
                    };
                    rows.push((key.clone(), row));
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        )?;
        if let Some(error) = decode_error {
            return Err(error);
        }
        Ok(rows)
    }

    fn append_index_put_ops(
        &self,
        ops: &mut Vec<Op>,
        row: &Row,
        row_key: &[u8],
    ) -> Result<(), QueryError> {
        for index in &self.indexes {
            if !index_predicate_matches(index, row)? {
                continue;
            }
            let Some(value) = index_expression_value(row, index.expression)? else {
                continue;
            };

            ops.push(Op::Put {
                table: REL_INDEX_TABLE.to_owned(),
                key: make_index_key(&self.name, index.id, &value, row_key)?,
                value: encode_index_payload(row, index)?,
            });
        }

        Ok(())
    }

    fn append_index_delete_ops(
        &self,
        ops: &mut Vec<Op>,
        row: &Row,
        row_key: &[u8],
    ) -> Result<(), QueryError> {
        for index in &self.indexes {
            if !index_predicate_matches(index, row)? {
                continue;
            }
            if let Some(value) = index_expression_value(row, index.expression)? {
                ops.push(Op::Delete {
                    table: REL_INDEX_TABLE.to_owned(),
                    key: make_index_key(&self.name, index.id, &value, row_key)?,
                });
            }
        }

        Ok(())
    }

    pub(crate) fn primary_key_value_for_row(&self, row: &Row) -> Result<Value, QueryError> {
        validate_row(self.schema.as_ref(), row)?;
        Ok(primary_key_value(self.schema.as_ref(), row)?.clone())
    }

    pub(crate) fn columnar_segment_bounds(&self) -> (Bytes, Bytes) {
        columnar_segment_range_bounds(&self.name)
    }

    pub(crate) fn decode_columnar_rows(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<Vec<Row>, QueryError> {
        let Some(bytes) = bytes else {
            return Ok(Vec::new());
        };

        let schema = self.required_schema()?;
        let bytes = cloud::resolve_tiered_segment_payload(bytes)?;
        parquet_bytes_to_rows(&bytes, schema)
    }

    /// Builds ops that replace every stored row with `rows`, for seeding columnar
    /// tables in tests. Writers read the segments first and use
    /// `columnar_rewrite_ops` or `rewrite_columnar_rows`.
    #[cfg(test)]
    pub(crate) fn columnar_replace_ops(&self, rows: Vec<Row>) -> Result<Vec<Op>, QueryError> {
        self.columnar_replace_ops_from_rows(rows)
    }

    fn columnar_insert_ops(&self, row: Row) -> Result<Vec<Op>, QueryError> {
        let segments = self.columnar_segments()?;
        self.columnar_append_ops(&segments, &[row])
    }

    fn columnar_update_ops(&self, row: Row) -> Result<Vec<Op>, QueryError> {
        let primary_key = self.primary_key_value_for_row(&row)?;
        let mut rows = self.columnar_rows()?;

        if let Some(index) = row_with_primary_key(self.schema.as_ref(), &rows, &primary_key) {
            rows[index] = row;
        } else {
            rows.push(row);
        }

        self.columnar_replace_ops_from_rows(rows)
    }

    fn columnar_delete_ops(&self, primary_key: &Value) -> Result<Vec<Op>, QueryError> {
        let mut rows = self.columnar_rows()?;
        let before = rows.len();
        rows.retain(|row| !row_primary_key_eq(self.schema.as_ref(), row, primary_key));

        if rows.len() == before {
            return Ok(Vec::new());
        }

        self.columnar_replace_ops_from_rows(rows)
    }

    fn columnar_get(&self, primary_key: &Value) -> Result<Option<Row>, QueryError> {
        Ok(self
            .columnar_rows()?
            .into_iter()
            .find(|row| row_primary_key_eq(self.schema.as_ref(), row, primary_key)))
    }

    fn columnar_rows(&self) -> Result<Vec<Row>, QueryError> {
        self.decode_columnar_segments(&self.columnar_segments()?)
    }

    fn columnar_segments(&self) -> Result<Vec<(Bytes, Bytes)>, QueryError> {
        let (start, end) = columnar_segment_range_bounds(&self.name);
        Ok(self.repl.range(
            REL_COLUMNAR_SEGMENTS_TABLE,
            &start,
            &end,
            ReadConsistency::Strong,
        )?)
    }

    fn decode_columnar_segments(
        &self,
        segments: &[(Bytes, Bytes)],
    ) -> Result<Vec<Row>, QueryError> {
        let mut rows = Vec::new();
        for (_, bytes) in segments {
            rows.extend(self.decode_columnar_rows(Some(bytes))?);
        }
        self.sorted_columnar_rows(rows)
    }

    fn sorted_columnar_rows(&self, rows: Vec<Row>) -> Result<Vec<Row>, QueryError> {
        let mut keyed_rows = rows
            .into_iter()
            .map(|row| self.row_key(&row).map(|key| (key, row)))
            .collect::<Result<Vec<_>, _>>()?;
        keyed_rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(keyed_rows.into_iter().map(|(_, row)| row).collect())
    }

    /// Appends without re-encoding closed segments. Existing rows are still read to
    /// enforce primary-key uniqueness; only a partial tail and new chunks are written.
    pub(crate) fn columnar_append_ops(
        &self,
        segments: &[(Bytes, Bytes)],
        rows: &[Row],
    ) -> Result<Vec<Op>, QueryError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut keys = BTreeSet::new();
        for existing in self.decode_columnar_segments(segments)? {
            keys.insert(self.row_key(&existing)?);
        }
        for row in rows {
            if !keys.insert(self.row_key(row)?) {
                return Err(QueryError::DuplicatePrimaryKey);
            }
        }

        let mut pending = rows.to_vec();
        let mut next_id = 0;
        let mut ops = Vec::new();
        if let Some((key, bytes)) = segments.last() {
            let id = columnar_segment_id(&self.name, key).ok_or_else(|| {
                QueryError::Storage(StorageError::Corruption(
                    "invalid columnar segment key".to_owned(),
                ))
            })?;
            let mut tail = self.decode_columnar_rows(Some(bytes))?;
            if tail.len() < COLUMNAR_SEGMENT_ROWS {
                tail.extend(pending);
                pending = tail;
                next_id = id;
            } else {
                next_id = id.checked_add(1).ok_or_else(|| {
                    QueryError::ResourceLimit("columnar segment ids exhausted".to_owned())
                })?;
                // Keep a shared write-version conflict marker for snapshot transactions.
                // This also preserves an immutable tiered pointer without modifying its object.
                ops.push(Op::Put {
                    table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                    key: key.clone(),
                    value: bytes.clone(),
                });
            }
        }
        let pending = self.sorted_columnar_rows(pending)?;
        for chunk in pending.chunks(COLUMNAR_SEGMENT_ROWS) {
            let key = columnar_segment_key(&self.name, next_id);
            let value = rows_to_parquet_bytes(self.required_schema()?, chunk)?;
            let meta = columnar_segment_meta(
                &self.name,
                next_id,
                self.required_schema()?,
                chunk,
                value.len(),
            );
            ops.push(Op::Put {
                table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                key: key.clone(),
                value,
            });
            ops.push(Op::Put {
                table: REL_COLUMNAR_SEGMENT_META_TABLE.to_owned(),
                key,
                value: serde_json::to_vec(&meta)
                    .map_err(|error| QueryError::Serde(error.to_string()))?,
            });
            next_id = next_id.checked_add(1).ok_or_else(|| {
                QueryError::ResourceLimit("columnar segment ids exhausted".to_owned())
            })?;
        }
        Ok(ops)
    }

    fn columnar_append_batch(
        &self,
        segments: &[(Bytes, Bytes)],
        rows: &[Row],
    ) -> Result<ConditionalBatch, QueryError> {
        let ops = self.columnar_append_ops(segments, rows)?;
        let mut conditions = segments
            .iter()
            .map(|(key, bytes)| {
                (
                    key.clone(),
                    WriteCondition::ValueEquals {
                        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                        key: key.clone(),
                        expected: Some(bytes.clone()),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for op in &ops {
            if let Op::Put { table, key, .. } = op
                && table == REL_COLUMNAR_SEGMENTS_TABLE
            {
                conditions
                    .entry(key.clone())
                    .or_insert(WriteCondition::KeyMissing {
                        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                        key: key.clone(),
                    });
            }
        }
        let next_id = segments
            .last()
            .and_then(|(key, _)| columnar_segment_id(&self.name, key))
            .map_or(Some(0), |id| id.checked_add(1))
            .ok_or_else(|| {
                QueryError::ResourceLimit("columnar segment ids exhausted".to_owned())
            })?;
        let key = columnar_segment_key(&self.name, next_id);
        conditions
            .entry(key.clone())
            .or_insert(WriteCondition::KeyMissing {
                table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                key,
            });
        Ok(ConditionalBatch::new(
            conditions.into_values().collect(),
            ops,
        ))
    }

    fn append_columnar_rows(&self, rows: &[Row]) -> Result<(), QueryError> {
        if rows.is_empty() {
            return Ok(());
        }
        for _ in 0..=WRITE_CONFLICT_RETRIES {
            let segments = self.columnar_segments()?;
            let batch = self.columnar_append_batch(&segments, rows)?;
            match self.repl.propose_conditional_batch(batch) {
                Ok(()) => return Ok(()),
                Err(ReplError::Conflict | ReplError::Storage(StorageError::Conflict)) => {
                    thread::yield_now();
                }
                Err(ReplError::Unsupported(_)) => {
                    let segments = self.columnar_segments()?;
                    self.repl
                        .propose_batch(self.columnar_append_ops(&segments, rows)?)?;
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(QueryError::Repl(ReplError::Conflict))
    }

    /// Rewrites the columnar rows with `change`, applied to the rows currently stored.
    ///
    /// The rewrite commits only if no other writer changed the table's segments
    /// after they were read; otherwise the rows are read again and `change` runs
    /// again. `change` returns `false` to leave the table untouched.
    fn rewrite_columnar_rows(
        &self,
        mut change: impl FnMut(&mut Vec<Row>) -> Result<bool, QueryError>,
    ) -> Result<(), QueryError> {
        for _ in 0..=WRITE_CONFLICT_RETRIES {
            let segments = self.columnar_segments()?;
            let mut rows = self.decode_columnar_segments(&segments)?;
            if !change(&mut rows)? {
                return Ok(());
            }

            let batch = self.columnar_rewrite_batch(&segments, rows)?;
            match self.repl.propose_conditional_batch(batch) {
                Ok(()) => return Ok(()),
                Err(ReplError::Conflict | ReplError::Storage(StorageError::Conflict)) => {
                    thread::yield_now();
                }
                // Without conditional writes, write unconditionally as the row
                // layout does. The rewrite is rebuilt here rather than copied on
                // every attempt, since backends that support conditions never
                // get here.
                Err(ReplError::Unsupported(_)) => {
                    let segments = self.columnar_segments()?;
                    let mut rows = self.decode_columnar_segments(&segments)?;
                    if change(&mut rows)? {
                        self.repl
                            .propose_batch(self.columnar_rewrite_ops(&segments, rows)?)?;
                    }
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            }
        }

        Err(QueryError::Repl(ReplError::Conflict))
    }

    /// Builds a rewrite that commits only while `segments` are still the table's segments.
    fn columnar_rewrite_batch(
        &self,
        segments: &[(Bytes, Bytes)],
        rows: Vec<Row>,
    ) -> Result<ConditionalBatch, QueryError> {
        let ops = self.columnar_rewrite_ops(segments, rows)?;
        let mut conditions = segments
            .iter()
            .map(|(key, bytes)| {
                let condition = WriteCondition::ValueEquals {
                    table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                    key: key.clone(),
                    expected: Some(bytes.clone()),
                };
                (key.clone(), condition)
            })
            .collect::<BTreeMap<_, _>>();
        // Segment ids are dense, so a writer that grows the table creates the
        // segment after the last one read.
        let next_segment = segments
            .iter()
            .filter_map(|(key, _)| columnar_segment_id(&self.name, key))
            .max()
            .map_or(0, |id| id.saturating_add(1));
        let written = ops.iter().filter_map(|op| match op {
            Op::Put { table, key, .. } if table == REL_COLUMNAR_SEGMENTS_TABLE => Some(key.clone()),
            _ => None,
        });
        for key in written.chain([columnar_segment_key(&self.name, next_segment)]) {
            conditions
                .entry(key.clone())
                .or_insert(WriteCondition::KeyMissing {
                    table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                    key,
                });
        }

        Ok(ConditionalBatch::new(
            conditions.into_values().collect(),
            ops,
        ))
    }

    fn columnar_record_batches(
        &self,
        projection: Option<&[usize]>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let (start, end) = columnar_segment_range_bounds(&self.name);
        let segments = self.repl.range(
            REL_COLUMNAR_SEGMENTS_TABLE,
            &start,
            &end,
            ReadConsistency::Strong,
        )?;
        if segments.is_empty() {
            return Ok(Vec::new());
        }

        let decoded = segments
            .par_iter()
            .map(|(_, bytes)| {
                let bytes = cloud::resolve_tiered_segment_payload(bytes)?;
                parquet_bytes_to_batches(&bytes, projection)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(decoded.into_iter().flatten().collect())
    }

    /// Reports how many columnar segments a simple filter can skip.
    /// # Errors
    /// Fails when segment metadata cannot be read or decoded.
    pub fn segment_skip_report(
        &self,
        filter: &RelPredicate,
    ) -> Result<SegmentSkipReport, QueryError> {
        let mut report = SegmentSkipReport::default();
        for (_, bytes, meta) in self.columnar_segments_with_meta()? {
            let bytes_len = usize_to_u64(bytes.len());
            match meta {
                Some(meta) if !segment_may_match_filter(&meta, filter)? => {
                    report.skipped_segments = report.skipped_segments.saturating_add(1);
                    report.skipped_bytes = report.skipped_bytes.saturating_add(bytes_len);
                }
                _ => {
                    report.scanned_segments = report.scanned_segments.saturating_add(1);
                    report.scanned_bytes = report.scanned_bytes.saturating_add(bytes_len);
                }
            }
        }
        Ok(report)
    }

    fn columnar_segments_for_filter(
        &self,
        filter: &RelPredicate,
    ) -> Result<Vec<(Bytes, Bytes)>, QueryError> {
        self.columnar_segments_with_meta()?
            .into_iter()
            .filter_map(|(key, bytes, meta)| match meta {
                Some(meta) => match segment_may_match_filter(&meta, filter) {
                    Ok(true) => Some(Ok((key, bytes))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                },
                None => Some(Ok((key, bytes))),
            })
            .collect()
    }

    /// Reads the table's segments together with the zone map of each one.
    ///
    /// Segments and zone maps come from separate storage reads, and every write
    /// re-chunks the whole table, so a commit between the reads would pair a
    /// segment with a zone map that describes other rows. The commit LSN is
    /// global, so any commit voids an attempt, even one to another table or the
    /// audit record written after each statement. The reads are repeated up to
    /// `SEGMENT_PRUNING_READ_ATTEMPTS` times; if the LSN moves under every one,
    /// the segments the last attempt read are returned without zone maps, so
    /// nothing is pruned. Under steady commit traffic a filtered scan therefore
    /// decodes every segment, in exchange for reading the segments at most that
    /// many times and never pairing them with zone maps from another commit.
    /// That guarantee relies on every commit to the segment tables moving the
    /// commit LSN, which holds on single-node and CP backends (the ones
    /// `multidb serve` uses). On AP and sharded backends the check cannot see
    /// those commits and always passes.
    fn columnar_segments_with_meta(
        &self,
    ) -> Result<Vec<(Bytes, Bytes, Option<ColumnarSegmentMeta>)>, QueryError> {
        let (start, end) = columnar_segment_range_bounds(&self.name);
        let mut attempts = 0;
        loop {
            attempts += 1;
            let before = cdc::current_lsn(self.repl.as_ref())?;
            let segments = self.columnar_segments()?;
            let metas = self.repl.range(
                REL_COLUMNAR_SEGMENT_META_TABLE,
                &start,
                &end,
                ReadConsistency::Strong,
            )?;
            if cdc::current_lsn(self.repl.as_ref())? != before {
                if attempts < SEGMENT_PRUNING_READ_ATTEMPTS {
                    continue;
                }
                // One range read of the segments is itself consistent; only its
                // pairing with the zone maps is in doubt.
                return Ok(segments
                    .into_iter()
                    .map(|(key, bytes)| (key, bytes, None))
                    .collect());
            }

            let mut metas = metas.into_iter().collect::<BTreeMap<_, _>>();
            return segments
                .into_iter()
                .map(|(key, bytes)| {
                    let meta = metas
                        .remove(&key)
                        .map(|meta| {
                            serde_json::from_slice(&meta)
                                .map_err(|error| QueryError::Serde(error.to_string()))
                        })
                        .transpose()?;
                    Ok((key, bytes, meta))
                })
                .collect();
        }
    }

    fn columnar_replace_ops_from_rows(&self, rows: Vec<Row>) -> Result<Vec<Op>, QueryError> {
        let segments = self.columnar_segments()?;
        self.columnar_rewrite_ops(&segments, rows)
    }

    pub(crate) fn columnar_rewrite_ops(
        &self,
        segments: &[(Bytes, Bytes)],
        rows: Vec<Row>,
    ) -> Result<Vec<Op>, QueryError> {
        for row in &rows {
            validate_row(self.schema.as_ref(), row)?;
        }

        let mut keyed_rows = rows
            .into_iter()
            .map(|row| self.row_key(&row).map(|key| (key, row)))
            .collect::<Result<Vec<_>, _>>()?;
        keyed_rows.sort_by(|left, right| left.0.cmp(&right.0));
        let rows = keyed_rows
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();

        let mut ops = segments
            .iter()
            .flat_map(|(key, _)| {
                [
                    Op::Delete {
                        table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                        key: key.clone(),
                    },
                    Op::Delete {
                        table: REL_COLUMNAR_SEGMENT_META_TABLE.to_owned(),
                        key: key.clone(),
                    },
                ]
            })
            .collect::<Vec<_>>();

        if rows.is_empty() {
            return Ok(ops);
        }

        for (segment_id, chunk) in rows.chunks(COLUMNAR_SEGMENT_ROWS).enumerate() {
            let segment_id = u64::try_from(segment_id).unwrap_or(u64::MAX);
            let key = columnar_segment_key(&self.name, segment_id);
            let value = rows_to_parquet_bytes(self.required_schema()?, chunk)?;
            let meta = columnar_segment_meta(
                &self.name,
                segment_id,
                self.required_schema()?,
                chunk,
                value.len(),
            );
            ops.push(Op::Put {
                table: REL_COLUMNAR_SEGMENTS_TABLE.to_owned(),
                key: key.clone(),
                value,
            });
            ops.push(Op::Put {
                table: REL_COLUMNAR_SEGMENT_META_TABLE.to_owned(),
                key,
                value: serde_json::to_vec(&meta)
                    .map_err(|error| QueryError::Serde(error.to_string()))?,
            });
        }

        Ok(ops)
    }

    fn required_schema(&self) -> Result<&TableSchema, QueryError> {
        self.schema
            .as_ref()
            .ok_or_else(|| QueryError::InvalidSchema("columnar tables require a schema".to_owned()))
    }
}

impl SqlEngine {
    #[must_use]
    pub fn new(repl: Arc<dyn Replication>) -> Self {
        Self::with_layout(repl, TableLayout::Row)
    }

    #[must_use]
    pub fn with_layout(repl: Arc<dyn Replication>, layout: TableLayout) -> Self {
        Self::with_layout_profile_and_cache(
            repl,
            layout,
            CostProfile::Balanced,
            Arc::new(Mutex::new(PlanCache::default())),
        )
    }

    #[must_use]
    pub fn with_layout_profile_and_cache(
        repl: Arc<dyn Replication>,
        layout: TableLayout,
        profile: CostProfile,
        plan_cache: Arc<Mutex<PlanCache>>,
    ) -> Self {
        Self::with_layout_profile_cache_and_performance(
            repl,
            layout,
            profile,
            plan_cache,
            PerformanceConfig::default(),
        )
    }

    #[must_use]
    pub fn with_layout_profile_cache_and_performance(
        repl: Arc<dyn Replication>,
        layout: TableLayout,
        profile: CostProfile,
        plan_cache: Arc<Mutex<PlanCache>>,
        performance: PerformanceConfig,
    ) -> Self {
        Self {
            repl,
            tables: BTreeMap::new(),
            collections: BTreeMap::new(),
            foreign_tables: BTreeMap::new(),
            materialized_views: BTreeMap::new(),
            temporal_tables: BTreeMap::new(),
            layout,
            cost_model: CostModel::new(profile),
            plan_cache,
            batch_cache: Arc::new(batch_cache::BatchCache::new(&performance)),
            performance,
            snapshot_lsn: None,
        }
    }

    #[must_use]
    pub const fn with_snapshot_lsn(mut self, snapshot_lsn: u64) -> Self {
        self.snapshot_lsn = Some(snapshot_lsn);
        self
    }

    /// Creates and registers a relational table.
    /// # Errors
    /// Fails when schema metadata cannot be written.
    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        schema: Option<TableSchema>,
        indexes: Vec<RelIndexSpec>,
    ) -> Result<(), QueryError> {
        let table =
            RelTable::create_with_layout(self.repl.clone(), name, schema, indexes, self.layout)?;
        self.tables.insert(table.name.clone(), table);
        self.invalidate_plan_cache()?;
        Ok(())
    }

    /// Opens and registers an existing table.
    /// # Errors
    /// Fails when schema metadata cannot be read.
    pub fn open_table(
        &mut self,
        name: impl Into<String>,
        indexes: Vec<RelIndexSpec>,
    ) -> Result<(), QueryError> {
        self.open_table_with_layout(name, indexes, self.layout)
    }

    /// Opens and registers an existing table with an explicit layout.
    /// # Errors
    /// Fails when schema metadata cannot be read or layout validation fails.
    pub fn open_table_with_layout(
        &mut self,
        name: impl Into<String>,
        indexes: Vec<RelIndexSpec>,
        layout: TableLayout,
    ) -> Result<(), QueryError> {
        let table = RelTable::open_with_layout(self.repl.clone(), name, indexes, layout)?;
        self.tables.insert(table.name.clone(), table);
        self.invalidate_plan_cache()?;
        Ok(())
    }

    /// Registers a document collection as a queryable table.
    /// # Errors
    /// Fails when the collection name or projected fields are invalid.
    pub fn register_collection(
        &mut self,
        name: impl Into<String>,
        collection_id: CollectionId,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    ) -> Result<(), QueryError> {
        let name = name.into();
        validate_table_name(&name)?;
        validate_doc_fields(&fields)?;
        let provider =
            DocCollectionProvider::new(self.repl.clone(), collection_id, fields, indexes)?;
        self.collections.insert(name, provider);
        self.invalidate_plan_cache()?;
        Ok(())
    }

    /// Registers a foreign table provider for `DataFusion` execution.
    /// # Errors
    /// Fails when the table name or schema are invalid.
    pub fn register_foreign_table(
        &mut self,
        name: impl Into<String>,
        spec: ForeignTableSpec,
    ) -> Result<(), QueryError> {
        let name = name.into();
        validate_table_name(&name)?;
        validate_schema(&spec.schema)?;
        self.foreign_tables
            .insert(name, ForeignTableProvider::new(spec));
        self.invalidate_plan_cache()?;
        Ok(())
    }

    /// Registers a persisted materialized view as a queryable table.
    /// # Errors
    /// Fails when the view output schema cannot be inferred.
    pub fn register_materialized_view(
        &mut self,
        name: impl Into<String>,
        spec: cdc::MaterializedViewSpec,
        source_schema: &TableSchema,
    ) -> Result<(), QueryError> {
        let name = name.into();
        validate_table_name(&name)?;
        let provider = MaterializedViewProvider::new(self.repl.clone(), spec, source_schema)?;
        self.materialized_views.insert(name, provider);
        self.invalidate_plan_cache()?;
        Ok(())
    }

    /// Registers a system-versioned table as a history table provider.
    /// # Errors
    /// Fails when the history object name is invalid.
    pub fn register_temporal_table(
        &mut self,
        name: impl Into<String>,
        base_table: String,
        base_schema: TableSchema,
        retention: TemporalRetention,
    ) -> Result<(), QueryError> {
        let name = name.into();
        validate_table_name(&name)?;
        validate_schema(&base_schema)?;
        self.temporal_tables.insert(
            name,
            TemporalTableProvider::new(self.repl.clone(), base_table, base_schema, retention),
        );
        self.invalidate_plan_cache()?;
        Ok(())
    }

    #[must_use]
    pub fn table(&self, name: &str) -> Option<&RelTable> {
        self.tables.get(name)
    }

    #[must_use]
    pub fn plan_cache_metrics(&self) -> PlanCacheMetrics {
        self.plan_cache
            .lock()
            .map_or_else(|_| PlanCacheMetrics::default(), |cache| cache.metrics())
    }

    /// Collects optimizer statistics for one object or all registered objects.
    /// # Errors
    /// Fails when an object cannot be scanned or stats cannot be persisted.
    pub fn analyze(
        &self,
        target: AnalyzeTarget,
        mode: AnalyzeMode,
    ) -> Result<AnalyzeReport, QueryError> {
        let names = match target {
            AnalyzeTarget::All => self
                .tables
                .keys()
                .chain(self.collections.keys())
                .cloned()
                .collect::<Vec<_>>(),
            AnalyzeTarget::Named(name) => vec![name],
        };

        let mut analyzed = Vec::new();
        for name in names {
            if let Some(table) = self.tables.get(&name) {
                analyzed.push(table.analyze(mode)?);
            } else if let Some(collection) = self.collections.get(&name) {
                analyzed.push(collection.analyze(&name, mode)?);
            } else {
                return Err(QueryError::MissingTable(name));
            }
        }

        self.invalidate_plan_cache()?;
        Ok(AnalyzeReport { mode, analyzed })
    }

    /// Statistics expose raw row counts and value distributions, so they cannot
    /// be computed under a caller's row or masking policies.
    fn analyze_read_targets(
        &self,
        target: &AnalyzeTarget,
        policies: &BTreeMap<String, SqlRelationPolicy>,
    ) -> Result<BTreeSet<SqlReadTarget>, QueryError> {
        let names: BTreeSet<String> = match target {
            AnalyzeTarget::All => self
                .tables
                .keys()
                .chain(self.collections.keys())
                .cloned()
                .collect(),
            AnalyzeTarget::Named(name) => BTreeSet::from([name.clone()]),
        };
        if let Some(name) = names.iter().find(|name| policies.contains_key(*name)) {
            return Err(QueryError::Unsupported(format!(
                "ANALYZE over {name} is unsupported while it has row or masking policies"
            )));
        }
        Ok(names.into_iter().map(SqlReadTarget::Catalog).collect())
    }

    /// Builds an optimizer explanation without necessarily executing the query.
    /// # Errors
    /// Fails when SQL is unsupported or execution fails under `ANALYZE`.
    pub fn explain(&self, sql: &str, options: ExplainOptions) -> Result<ExplainReport, QueryError> {
        if self.snapshot_lsn.is_none()
            && let Some(plan) = row_scan::plan(self, sql)?
        {
            return self.explain_range(sql, &plan, options);
        }
        if self.snapshot_lsn.is_none()
            && let Some(simple) = self.simple_select_plan(sql)?
        {
            return self.explain_simple(sql, &simple, options);
        }
        self.explain_fallback_sync(sql, options)
    }

    fn explain_simple(
        &self,
        sql: &str,
        simple: &SimpleSelectPlan,
        options: ExplainOptions,
    ) -> Result<ExplainReport, QueryError> {
        let table = self
            .tables
            .get(&simple.table)
            .ok_or_else(|| QueryError::MissingTable(simple.table.clone()))?;
        let stats = StatsCatalog::read_table(self.repl.as_ref(), &simple.table)?;
        let fingerprint = QueryFingerprint::new(sql);
        let plan = self.choose_eq_plan(
            &fingerprint,
            table,
            &simple.filter,
            &simple.projection,
            stats.as_ref(),
        )?;

        let (actual_rows, actual_ms) = if options.analyze {
            let started = std::time::Instant::now();
            let SqlOutput::Rows(result) = self.run_simple_select(sql, simple)? else {
                return Err(QueryError::Unsupported(
                    "EXPLAIN requires a read query".to_owned(),
                ));
            };
            (
                Some(usize_to_u64(result.rows.len())),
                Some(optimizer::elapsed_ms(started)),
            )
        } else {
            (None, None)
        };

        let column = column_name(table.schema.as_ref(), simple.filter_column);
        let report = ExplainReport {
            fingerprint,
            analyze: options.analyze,
            nodes: vec![optimizer::explain_node_for_eq(
                &simple.table,
                &column,
                &plan.access_path,
                actual_rows,
                actual_ms,
                plan.used_cache,
            )],
        };

        if options.analyze
            && let Some(feedback) = optimizer::maybe_feedback(&report)
        {
            let _ = StatsCatalog::record_feedback(self.repl.as_ref(), &feedback);
        }

        Ok(report)
    }

    /// Executes one or more SQL statements and returns the last result.
    /// # Errors
    /// Fails when SQL is unsupported, storage fails, or `DataFusion` fails.
    pub async fn execute(&self, sql: &str) -> Result<SqlOutput, QueryError> {
        self.execute_with_read_guard(sql, None).await
    }

    /// Executes SQL after `guard` authorizes the relations each statement reads.
    /// Every statement is planned and authorized before any statement runs, and
    /// the plan that was authorized is the plan that executes. The guard's row and
    /// masking policies are applied where each relation is scanned.
    /// # Errors
    /// Fails when the guard denies a read, SQL is unsupported, storage fails, or
    /// `DataFusion` fails.
    pub async fn execute_with_read_guard(
        &self,
        sql: &str,
        mut guard: Option<&mut dyn SqlReadGuard>,
    ) -> Result<SqlOutput, QueryError> {
        if sql.len() > self.performance.query.max_sql_bytes {
            return Err(QueryError::InputLimit(format!(
                "SQL text is {} bytes, limit is {} bytes",
                sql.len(),
                self.performance.query.max_sql_bytes
            )));
        }

        if let Some((target, mode)) = parse_analyze_command(sql)? {
            let policies = guard
                .as_deref()
                .map(SqlReadGuard::relation_policies)
                .unwrap_or_default();
            let reads = self.analyze_read_targets(&target, &policies)?;
            if let Some(guard) = guard.as_deref_mut() {
                guard
                    .authorize_reads(&reads)
                    .map_err(QueryError::AuthzDenied)?;
            }
            return Ok(self.analyze(target, mode)?.to_sql_output());
        }

        let statements = parse_with_limits(sql, &self.performance.query)?;
        let policies = guard
            .as_deref()
            .map(SqlReadGuard::relation_policies)
            .unwrap_or_default();
        let mut prepared = Vec::with_capacity(statements.len());
        for statement in statements {
            let (statement, reads) = self.prepare_statement(statement, &policies).await?;
            if let Some(reads) = reads
                && let Some(guard) = guard.as_deref_mut()
            {
                guard
                    .authorize_reads(&reads)
                    .map_err(QueryError::AuthzDenied)?;
            }
            prepared.push(statement);
        }

        let mut last = None;
        for statement in prepared {
            last = Some(self.run_statement(statement).await?);
        }

        last.ok_or_else(|| QueryError::Unsupported("empty SQL".to_owned()))
    }

    /// Plans one statement and returns its output schema without scanning data or writing.
    /// The caller must check write/admin requirements separately, as for execution.
    /// # Errors
    /// Fails when SQL is unsupported, cannot be planned, or the guard denies a read.
    pub(crate) async fn describe_with_read_guard(
        &self,
        sql: &str,
        guard: &mut dyn SqlReadGuard,
    ) -> Result<Option<SchemaRef>, QueryError> {
        let statements = parse_with_limits(sql, &self.performance.query)?;
        let [statement] = statements.as_slice() else {
            return Err(QueryError::Unsupported(
                "Describe requires exactly one SQL statement".to_owned(),
            ));
        };
        let policies = guard.relation_policies();
        let select_sql = match statement {
            Statement::Query(_) => statement.to_string(),
            Statement::Insert(insert) => {
                let Some(returning) = &insert.returning else {
                    return Ok(None);
                };
                let name = match &insert.table {
                    SqlTableObject::TableName(name) => name,
                    other => return Err(QueryError::Unsupported(other.to_string())),
                };
                let table = object_name_to_string(name)?;
                if policies.contains_key(&table) {
                    return Err(QueryError::Unsupported(format!(
                        "INSERT ... RETURNING into {table} is unsupported while it has row or masking policies"
                    )));
                }
                // RETURNING supports stored columns and aliases, not arbitrary expressions.
                let relation = self
                    .tables
                    .get(&table)
                    .ok_or_else(|| QueryError::MissingTable(table.clone()))?;
                returning_rows(relation.schema.as_ref(), returning, &[])?;
                let projection = returning
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("SELECT {projection} FROM {name}")
            }
            Statement::Explain { statement, .. } => {
                let (_, reads) = self
                    .prepare_explain(&statement.to_string(), ExplainOptions::default(), &policies)
                    .await?;
                guard
                    .authorize_reads(&reads)
                    .map_err(QueryError::AuthzDenied)?;
                return Ok(Some(explain_fallback::schema()));
            }
            // These commands have no normal row description on the wire. Their
            // write/admin checks are the Database caller's responsibility.
            Statement::Analyze(_) => return Ok(None),
            other => return Err(QueryError::Unsupported(other.to_string())),
        };
        let (ctx, providers, _) = self.datafusion_catalog_context(&policies)?;
        let plan = self.plan_datafusion_select(&ctx, &select_sql).await?;
        guard
            .authorize_reads(&planned_read_targets(&plan, &providers)?)
            .map_err(QueryError::AuthzDenied)?;
        Ok(Some(Arc::clone(plan.schema().inner())))
    }

    /// Plans one statement without running it. Returns the relations it reads,
    /// or `None` for statements that are authorized by their write/admin
    /// requirements instead.
    async fn prepare_statement(
        &self,
        statement: Statement,
        policies: &BTreeMap<String, SqlRelationPolicy>,
    ) -> Result<(PreparedStatement, Option<BTreeSet<SqlReadTarget>>), QueryError> {
        match statement {
            Statement::Query(_) => {
                let (select, reads) = self
                    .prepare_select(&statement.to_string(), policies)
                    .await?;
                Ok((PreparedStatement::Select(select), Some(reads)))
            }
            Statement::Analyze(analyze) => {
                let target = analyze
                    .table_name
                    .as_ref()
                    .map_or(Ok(AnalyzeTarget::All), |name| {
                        object_name_to_string(name).map(AnalyzeTarget::Named)
                    })?;
                let reads = self.analyze_read_targets(&target, policies)?;
                Ok((PreparedStatement::Analyze(target), Some(reads)))
            }
            Statement::Explain {
                analyze, statement, ..
            } => {
                let (prepared, reads) = self
                    .prepare_explain(&statement.to_string(), ExplainOptions { analyze }, policies)
                    .await?;
                Ok((PreparedStatement::Explain(prepared), Some(reads)))
            }
            Statement::Insert(insert) => {
                // RETURNING hands back stored rows, the existing row of an
                // ON CONFLICT DO UPDATE included, so it reads the table.
                let reads = if insert.returning.is_some() {
                    let table = match &insert.table {
                        SqlTableObject::TableName(name) => object_name_to_string(name)?,
                        other => return Err(QueryError::Unsupported(other.to_string())),
                    };
                    if policies.contains_key(&table) {
                        return Err(QueryError::Unsupported(format!(
                            "INSERT ... RETURNING into {table} is unsupported while it has row or masking policies"
                        )));
                    }
                    Some(BTreeSet::from([SqlReadTarget::Catalog(table)]))
                } else {
                    None
                };
                Ok((PreparedStatement::Insert(Box::new(insert)), reads))
            }
            other => Err(QueryError::Unsupported(other.to_string())),
        }
    }

    async fn run_statement(&self, statement: PreparedStatement) -> Result<SqlOutput, QueryError> {
        match statement {
            PreparedStatement::Select(PreparedSelect::RowRange(plan)) => {
                row_scan::execute(self, &plan)
            }
            PreparedStatement::Select(PreparedSelect::Simple { sql, plan }) => {
                self.run_simple_select(&sql, &plan)
            }
            PreparedStatement::Select(PreparedSelect::DataFusion {
                ctx,
                plan,
                planning,
                budget,
            }) => {
                self.run_datafusion_select(&ctx, *plan, planning, &budget)
                    .await
            }
            PreparedStatement::Analyze(target) => Ok(self
                .analyze(target, AnalyzeMode::default())?
                .to_sql_output()),
            PreparedStatement::Explain(prepared) => {
                Ok(self.run_explain(prepared).await?.to_sql_output())
            }
            PreparedStatement::Insert(insert) => self.execute_insert(&insert),
        }
    }

    async fn prepare_select(
        &self,
        sql: &str,
        policies: &BTreeMap<String, SqlRelationPolicy>,
    ) -> Result<(PreparedSelect, BTreeSet<SqlReadTarget>), QueryError> {
        // Policy-protected relations always retain their provider barrier. This fast
        // plan has a complete literal-PK grammar and reports its catalog read to the guard.
        if self.snapshot_lsn.is_none()
            && let Some(plan) = row_scan::plan(self, sql)?
            && !policies.contains_key(&plan.table)
        {
            let reads = BTreeSet::from([SqlReadTarget::Catalog(plan.table.clone())]);
            return Ok((PreparedSelect::RowRange(Box::new(plan)), reads));
        }
        // The simple path reads stored rows directly, so a relation with policies
        // is always planned through `DataFusion`, where they are applied.
        if self.snapshot_lsn.is_none()
            && let Some(simple) = self.simple_select_plan(sql)?
            && !policies.contains_key(&simple.table)
        {
            let reads = BTreeSet::from([SqlReadTarget::Catalog(simple.table.clone())]);
            return Ok((
                PreparedSelect::Simple {
                    sql: sql.to_owned(),
                    plan: Box::new(simple),
                },
                reads,
            ));
        }

        let (ctx, providers, budget) = self.datafusion_catalog_context(policies)?;
        let started = std::time::Instant::now();
        let plan = self.plan_datafusion_select(&ctx, sql).await?;
        let planning = started.elapsed();
        let reads = planned_read_targets(&plan, &providers)?;
        Ok((
            PreparedSelect::DataFusion {
                ctx,
                plan: Box::new(plan),
                planning,
                budget,
            },
            reads,
        ))
    }

    /// Registers every catalog object in a fresh `DataFusion` context and
    /// returns the registered providers, so plan scans can be mapped back to
    /// catalog names by provider identity. An object with policies is
    /// registered behind them.
    fn datafusion_catalog_context(
        &self,
        policies: &BTreeMap<String, SqlRelationPolicy>,
    ) -> Result<(SessionContext, RegisteredProviders, Arc<budget::Budget>), QueryError> {
        let budget = budget::Budget::new(&self.performance.query);
        let ctx = self.datafusion_context(&budget)?;
        let mut providers: RegisteredProviders = Vec::new();

        for (name, table) in &self.tables {
            providers.push((
                name.clone(),
                Arc::new(StorageTableProvider::new(
                    {
                        let mut table = table.clone();
                        if self.snapshot_lsn.is_some() || table.layout == TableLayout::Columnar {
                            table.repl = budget::replication(Arc::clone(&table.repl), &budget);
                        }
                        table
                    },
                    self.performance.clone(),
                    self.snapshot_lsn,
                    Arc::clone(&self.batch_cache),
                    Arc::clone(&budget),
                )),
            ));
        }

        for (name, provider) in &self.collections {
            let mut provider = provider.clone();
            provider.repl = budget::replication(Arc::clone(&provider.repl), &budget);
            providers.push((name.clone(), Arc::new(provider)));
        }

        for (name, provider) in &self.foreign_tables {
            providers.push((name.clone(), Arc::new(provider.clone())));
        }

        for (name, provider) in &self.materialized_views {
            let mut provider = provider.clone();
            provider.repl = budget::replication(Arc::clone(&provider.repl), &budget);
            providers.push((name.clone(), Arc::new(provider)));
        }

        for (name, provider) in &self.temporal_tables {
            let mut provider = provider.clone();
            provider.repl = budget::replication(Arc::clone(&provider.repl), &budget);
            providers.push((name.clone(), Arc::new(provider)));
        }

        for (name, provider) in &mut providers {
            if let Some(policy) = policies.get(name) {
                *provider = policy_table_provider(
                    name,
                    Arc::clone(provider),
                    self.collections.get(name),
                    policy,
                    Arc::clone(&budget),
                )?;
            }
        }

        for (name, provider) in &providers {
            ctx.register_table(name, Arc::clone(provider))
                .map_err(|error| QueryError::DataFusion(error.to_string()))?;
        }

        Ok((ctx, providers, budget))
    }

    async fn plan_datafusion_select(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<LogicalPlan, QueryError> {
        let timeout = std::time::Duration::from_millis(self.performance.query.timeout_ms);
        let state = ctx.state();
        let plan = tokio::time::timeout(timeout, state.create_logical_plan(sql))
            .await
            .map_err(|_| self.timeout_error())?
            .map_err(|error| datafusion_to_query_error(&error))?;

        // DataFusion re-parses the statement text with its own dialect, so a
        // read statement must not plan into anything that changes state.
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
            .verify_plan(&plan)
            .map_err(|error| QueryError::Unsupported(error.strip_backtrace()))?;
        Ok(plan)
    }

    async fn run_datafusion_select(
        &self,
        ctx: &SessionContext,
        plan: LogicalPlan,
        planning: std::time::Duration,
        budget: &Arc<budget::Budget>,
    ) -> Result<SqlOutput, QueryError> {
        let _cancel_on_drop = budget.start(planning)?;
        // Planning and execution share one `timeout_ms` budget per statement.
        let timeout = std::time::Duration::from_millis(self.performance.query.timeout_ms)
            .saturating_sub(planning);
        let execution = async {
            let dataframe = ctx
                .execute_logical_plan(plan)
                .await
                .map_err(|error| datafusion_to_query_error(&error))?;
            let columns = dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            let mut stream = dataframe
                .execute_stream()
                .await
                .map_err(|error| datafusion_to_query_error(&error))?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|error| datafusion_to_query_error(&error))?;
                budget.charge_batch(&batch)?;
                batches.push(batch);
            }
            let mut rows = Vec::new();
            for batch in batches {
                budget.check()?;
                rows.extend(record_batches_to_rows(&[batch], Vec::new())?.rows);
            }
            budget.check()?;
            Ok::<_, QueryError>(SqlOutput::Rows(SqlRows { columns, rows }))
        };

        tokio::time::timeout(timeout, execution)
            .await
            .map_err(|_| self.timeout_error())?
    }

    fn timeout_error(&self) -> QueryError {
        QueryError::Timeout(format!(
            "query exceeded {} ms",
            self.performance.query.timeout_ms
        ))
    }

    fn datafusion_context(&self, budget: &budget::Budget) -> Result<SessionContext, QueryError> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&budget.pool))
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_max_temp_directory_size(self.performance.query.spill_limit_bytes),
            )
            .build_arc()
            .map_err(|error| QueryError::DataFusion(error.to_string()))?;

        let config = SessionConfig::new()
            .with_target_partitions(self.performance.parallelism.target_partitions.max(1))
            .with_information_schema(true);

        Ok(SessionContext::new_with_config_rt(config, runtime))
    }

    fn execute_insert(&self, insert: &sqlparser::ast::Insert) -> Result<SqlOutput, QueryError> {
        let table_name = match &insert.table {
            SqlTableObject::TableName(name) => object_name_to_string(name)?,
            other => return Err(QueryError::Unsupported(other.to_string())),
        };

        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| QueryError::MissingTable(table_name.clone()))?;

        let rows = values_from_insert(insert, table)?;
        let affected_rows = apply_insert_rows_atomic(table, &rows, insert)?;

        if let Some(returning) = &insert.returning {
            let (columns, rows) = returning_rows(table.schema.as_ref(), returning, &affected_rows)?;
            return Ok(SqlOutput::Rows(SqlRows { columns, rows }));
        }

        Ok(SqlOutput::AffectedRows(affected_rows.len()))
    }

    fn run_simple_select(
        &self,
        sql: &str,
        simple: &SimpleSelectPlan,
    ) -> Result<SqlOutput, QueryError> {
        let table = self
            .tables
            .get(&simple.table)
            .ok_or_else(|| QueryError::MissingTable(simple.table.clone()))?;
        let budget = budget::Budget::new(&self.performance.query);
        let _cancel_on_drop = budget.start(std::time::Duration::ZERO)?;
        let mut table = table.clone();
        table.repl = budget::replication(Arc::clone(&table.repl), &budget);
        let stats = StatsCatalog::read_table(self.repl.as_ref(), &simple.table)?;
        let fingerprint = QueryFingerprint::new(sql);
        let plan = self.choose_eq_plan(
            &fingerprint,
            &table,
            &simple.filter,
            &simple.projection,
            stats.as_ref(),
        )?;

        let result = table.query_filter_with_access_path(
            &simple.filter,
            &simple.projection,
            &plan.access_path,
        );
        budget.check()?;
        let result = result?;
        for row in &result.rows {
            budget.charge_row(row, Some(&simple.projection))?;
        }
        let mut rows = result
            .rows
            .into_iter()
            .map(|row| project_row(&row, &simple.projection))
            .collect::<Vec<_>>();
        if let Some(limit) = simple.limit {
            rows.truncate(limit);
        }

        Ok(SqlOutput::Rows(SqlRows {
            columns: simple.projection_names.clone(),
            rows,
        }))
    }

    fn simple_select_plan(&self, sql: &str) -> Result<Option<SimpleSelectPlan>, QueryError> {
        let statements = parse_with_limits(sql, &self.performance.query)?;
        let [Statement::Query(query)] = statements.as_slice() else {
            return Ok(None);
        };
        if !simple_grammar::supported(query) {
            return Ok(None);
        }

        if query.with.is_some()
            || query.order_by.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Ok(None);
        }

        let limit = match parse_limit(query) {
            Ok(limit) => limit,
            Err(QueryError::Unsupported(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };

        if select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || select.from.len() != 1
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !is_empty_group_by(&select.group_by)
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
        {
            return Ok(None);
        }

        let relation = &select.from[0];
        if !relation.joins.is_empty() {
            return Ok(None);
        }
        let table_name = match &relation.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => match object_name_to_string(name) {
                Ok(name) => name,
                Err(QueryError::Unsupported(_)) => return Ok(None),
                Err(error) => return Err(error),
            },
            _ => return Ok(None),
        };
        let Some(table) = self.tables.get(&table_name) else {
            return Ok(None);
        };
        let Some(schema) = table.schema.as_ref() else {
            return Ok(None);
        };
        let Some(selection) = &select.selection else {
            return Ok(None);
        };
        let Some(filter) = (match simple_filter(selection, schema) {
            Ok(filter) => filter,
            Err(QueryError::Unsupported(_)) => return Ok(None),
            Err(error) => return Err(error),
        }) else {
            return Ok(None);
        };
        let (filter_column, filter_value) = first_filter_eq(&filter)
            .map(|(expression, value)| (expression.column(), value.clone()))
            .ok_or_else(|| QueryError::Unsupported("missing equality filter".to_owned()))?;
        let (projection, projection_names) = match projection_columns(&select.projection, schema) {
            Ok(projection) => projection,
            Err(QueryError::Unsupported(_)) => return Ok(None),
            Err(error) => return Err(error),
        };

        Ok(Some(SimpleSelectPlan {
            table: table_name,
            filter_column,
            filter_value,
            filter,
            projection,
            projection_names,
            limit,
        }))
    }

    fn choose_eq_plan(
        &self,
        fingerprint: &QueryFingerprint,
        table: &RelTable,
        filter: &RelPredicate,
        projection: &[usize],
        stats: Option<&TableStats>,
    ) -> Result<EqPlan, QueryError> {
        let stats_version = stats.map_or(0, |stats| stats.stats_version);
        if let Some(cached) = self
            .plan_cache
            .lock()
            .map_err(|_| {
                QueryError::Storage(StorageError::Backend("plan cache lock poisoned".to_owned()))
            })?
            .get(fingerprint, stats_version)
            && optimizer::cached_path_applies(
                &cached.access_path,
                &table.indexes,
                filter,
                projection,
            )
        {
            return Ok(EqPlan {
                access_path: cached.access_path,
                stats_version,
                used_cache: true,
            });
        }

        let access_path = self.cost_model.choose_filter_path_with_schema(
            FilterPathRequest {
                layout: table.layout,
                indexes: table.indexes(),
                filter,
                stats,
                projection,
            },
            table.schema.as_ref(),
        );
        self.plan_cache
            .lock()
            .map_err(|_| {
                QueryError::Storage(StorageError::Backend("plan cache lock poisoned".to_owned()))
            })?
            .insert(CachedPlan {
                fingerprint: fingerprint.clone(),
                dependencies: vec![PlanDependency::Table {
                    name: table.name.clone(),
                    stats_version,
                }],
                stats_version,
                access_path: access_path.clone(),
                hits: 0,
            });

        Ok(EqPlan {
            access_path,
            stats_version,
            used_cache: false,
        })
    }

    fn invalidate_plan_cache(&self) -> Result<(), QueryError> {
        self.plan_cache
            .lock()
            .map_err(|_| {
                QueryError::Storage(StorageError::Backend("plan cache lock poisoned".to_owned()))
            })?
            .invalidate_all();
        Ok(())
    }
}

impl StorageTableProvider {
    fn new(
        table: RelTable,
        performance: PerformanceConfig,
        snapshot_lsn: Option<u64>,
        batch_cache: Arc<batch_cache::BatchCache>,
        budget: Arc<budget::Budget>,
    ) -> Self {
        Self {
            arrow_schema: schema_to_arrow(table.schema.as_ref()),
            table,
            performance,
            snapshot_lsn,
            batch_cache,
            budget,
        }
    }
}

impl DocCollectionProvider {
    /// Creates a `DataFusion` provider for one document collection.
    /// # Errors
    /// Fails when projected fields are invalid.
    pub fn new(
        repl: Arc<dyn Replication>,
        collection_id: CollectionId,
        fields: Vec<DocField>,
        indexes: Vec<IndexSpec>,
    ) -> Result<Self, QueryError> {
        validate_doc_fields(&fields)?;
        Ok(Self {
            repl,
            collection_id,
            arrow_schema: doc_fields_to_arrow(&fields),
            fields,
            indexes,
            visible_when: Arc::from(Vec::new()),
        })
    }

    /// The same collection, scanning only the documents `visible_when` lets through.
    fn with_visible_when(&self, visible_when: &[Vec<(FieldPath, Value)>]) -> Self {
        Self {
            visible_when: Arc::from(visible_when),
            ..self.clone()
        }
    }

    fn scan_record_batches(
        &self,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let collection = DocumentCollection::with_indexes(
            self.repl.as_ref(),
            self.collection_id,
            self.indexes.clone(),
        );
        let mut documents = collection.scan()?;
        // The same test as `PolicyConfig::row_visible`: a document without the path, or
        // holding a value of another type, never matches, although its column reads NULL.
        documents.retain(|(_, doc)| {
            self.visible_when.iter().all(|any_of| {
                any_of
                    .iter()
                    .any(|(path, value)| extract_path(doc, path) == Some(value))
            })
        });
        if let Some(limit) = limit {
            documents.truncate(limit);
        }
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let rows = documents
            .into_iter()
            .map(|(id, doc)| self.document_to_row(id, &doc))
            .collect::<Vec<_>>();
        Ok(vec![rows_to_record_batch(schema, &rows)?])
    }

    fn analyze(&self, name: &str, mode: AnalyzeMode) -> Result<TableStats, QueryError> {
        let rows = batches_to_rows(&self.scan_record_batches(self.arrow_schema.clone(), None)?)?;
        let columns = self
            .fields
            .iter()
            .map(|field| (field.name.clone(), field.ty.clone()))
            .collect::<Vec<_>>();
        let stats_version = StatsCatalog::next_stats_version(self.repl.as_ref())?;
        let stats = optimizer::build_table_stats(
            name,
            StatsObjectKind::Collection,
            &columns,
            &rows,
            mode,
            stats_version,
            stats_version,
        );
        StatsCatalog::write_table(self.repl.as_ref(), &stats)?;
        Ok(stats)
    }

    fn document_to_row(&self, id: DocumentId, doc: &Value) -> Row {
        self.fields
            .iter()
            .map(|field| {
                let value = match &field.source {
                    DocFieldSource::DocumentId => Value::Bytes(id.as_bytes().to_vec()),
                    DocFieldSource::Path(path) => {
                        extract_path(doc, path).cloned().unwrap_or(Value::Null)
                    }
                };
                coerce_doc_value(value, &field.ty)
            })
            .collect()
    }
}

impl ForeignTableProvider {
    fn new(spec: ForeignTableSpec) -> Self {
        Self {
            arrow_schema: schema_to_arrow(Some(&spec.schema)),
            spec,
        }
    }

    async fn scan_rows(
        &self,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Vec<Row>, QueryError> {
        let result = federation::scan_foreign_table(
            &self.spec,
            ForeignScanRequest {
                projection,
                filters: Vec::new(),
                limit,
            },
        )
        .await
        .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?;
        Ok(result.rows)
    }
}

impl fmt::Debug for ForeignTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignTableProvider")
            .field("source", &self.spec.source)
            .finish_non_exhaustive()
    }
}

impl MaterializedViewProvider {
    fn new(
        repl: Arc<dyn Replication>,
        spec: cdc::MaterializedViewSpec,
        source_schema: &TableSchema,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            repl,
            arrow_schema: materialized_view_arrow_schema(&spec, source_schema)?,
            spec,
        })
    }

    fn scan_rows(&self, limit: Option<usize>) -> Result<Vec<Row>, QueryError> {
        let mut rows = cdc::read_materialized_view(self.repl.as_ref(), &self.spec.name)
            .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?
            .rows;
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

impl fmt::Debug for MaterializedViewProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedViewProvider")
            .field("name", &self.spec.name)
            .field("source_table", &self.spec.source_table)
            .finish_non_exhaustive()
    }
}

impl TemporalTableProvider {
    fn new(
        repl: Arc<dyn Replication>,
        base_table: String,
        base_schema: TableSchema,
        retention: TemporalRetention,
    ) -> Self {
        let history_schema = temporal::system_versioned_schema(&base_schema);
        Self {
            repl,
            base_table,
            base_schema,
            retention,
            arrow_schema: schema_to_arrow(Some(&history_schema)),
        }
    }

    fn scan_rows(&self, limit: Option<usize>) -> Result<Vec<Row>, QueryError> {
        let mut rows = temporal::history_rows_for_table(
            self.repl.as_ref(),
            &self.base_table,
            &self.base_schema,
            self.retention,
        )
        .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?;
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

impl fmt::Debug for TemporalTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalTableProvider")
            .field("base_table", &self.base_table)
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for StorageTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageTableProvider")
            .field("table", &self.table.name)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for DocCollectionProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocCollectionProvider")
            .field("collection_id", &self.collection_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for StorageTableProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        let stats =
            StatsCatalog::read_table(self.table.repl.as_ref(), self.table.name()).ok()??;
        let mut statistics = Statistics::new_unknown(self.arrow_schema.as_ref());
        statistics.num_rows = Precision::Inexact(usize::try_from(stats.row_count).ok()?);
        statistics.calculate_total_byte_size(self.arrow_schema.as_ref());
        Some(statistics)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        if self.table.layout == TableLayout::Columnar {
            return Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()]);
        }
        Ok(filters
            .iter()
            .map(|filter| {
                if self.snapshot_lsn.is_none() && row_scan::supports_filter(&self.table, filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table = self.table.clone();
        let schema = self.arrow_schema.clone();
        let projection = projection.cloned();
        let projection_for_scan = projection.clone();
        let batch_rows = self.performance.query.batch_rows;
        let snapshot_lsn = self.snapshot_lsn;
        let row_filters = filters.to_vec();
        let batch_cache = Arc::clone(&self.batch_cache);
        let budget = Arc::clone(&self.budget);

        let batches = tokio::task::spawn_blocking(move || {
            budget.check()?;
            if let Some(snapshot_lsn) = snapshot_lsn {
                table.scan_record_batches_as_of_lsn(&schema, limit, batch_rows, snapshot_lsn)
            } else if table.layout == TableLayout::Row {
                let filled = std::cell::Cell::new(false);
                let scan = || {
                    filled.set(true);
                    row_scan::batches(
                        &table,
                        &schema,
                        projection_for_scan.as_deref(),
                        &row_filters,
                        limit,
                        batch_rows,
                        Some(&budget),
                    )
                };
                if row_filters.is_empty() && limit.is_none() {
                    let batches = batch_cache.batches(
                        &table,
                        &schema,
                        projection_for_scan.as_deref(),
                        batch_rows,
                        scan,
                    )?;
                    if !filled.get() {
                        for batch in &batches {
                            budget.charge_cached_source(batch)?;
                        }
                    }
                    budget.check()?;
                    Ok(batches)
                } else {
                    scan()
                }
            } else {
                table.scan_record_batches(
                    &schema,
                    projection_for_scan.as_deref(),
                    limit,
                    batch_rows,
                )
            }
        })
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .map_err(query_to_datafusion)?;

        if self.table.layout == TableLayout::Columnar || snapshot_lsn.is_none() {
            let schema = projection.as_deref().map_or_else(
                || self.arrow_schema.clone(),
                |projection| project_schema(&self.arrow_schema, projection),
            );
            let partitions = if batches.is_empty() {
                vec![Vec::new()]
            } else if self.table.layout == TableLayout::Row {
                vec![batches]
            } else {
                split_into_partitions(
                    batches,
                    self.performance.parallelism.target_partitions.max(1),
                )
            };
            observability::record_parallel_scan(self.table.name(), partitions.len());
            let mem = MemTable::try_new(schema, partitions)?;
            let safe_limit = if self.table.layout == TableLayout::Row && !filters.is_empty() {
                None
            } else {
                limit
            };
            return mem.scan(state, None, &[], safe_limit).await;
        }

        let mem = MemTable::try_new(self.arrow_schema.clone(), vec![batches])?;
        mem.scan(state, projection.as_ref(), filters, limit).await
    }
}

#[async_trait]
impl TableProvider for ForeignTableProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        let mut statistics = Statistics::new_unknown(self.arrow_schema.as_ref());
        if let Some(row_count) = self.spec.stats.row_count {
            statistics.num_rows = Precision::Inexact(usize::try_from(row_count).ok()?);
        }
        statistics.calculate_total_byte_size(self.arrow_schema.as_ref());
        Some(statistics)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let rows = self
            .scan_rows(None, limit)
            .await
            .map_err(query_to_datafusion)?;
        let batches = if rows.is_empty() {
            Vec::new()
        } else {
            vec![
                rows_to_record_batch(self.arrow_schema.clone(), &rows)
                    .map_err(query_to_datafusion)?,
            ]
        };
        let mem = MemTable::try_new(self.arrow_schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
impl TableProvider for MaterializedViewProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let provider = self.clone();
        let schema = self.arrow_schema.clone();
        let batches = tokio::task::spawn_blocking(move || {
            let rows = provider.scan_rows(limit)?;
            if rows.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![rows_to_record_batch(schema, &rows)?])
            }
        })
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .map_err(query_to_datafusion)?;
        let mem = MemTable::try_new(self.arrow_schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
impl TableProvider for TemporalTableProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let provider = self.clone();
        let schema = self.arrow_schema.clone();
        let batches = tokio::task::spawn_blocking(move || {
            let rows = provider.scan_rows(limit)?;
            if rows.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![rows_to_record_batch(schema, &rows)?])
            }
        })
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .map_err(query_to_datafusion)?;
        let mem = MemTable::try_new(self.arrow_schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
impl TableProvider for DocCollectionProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let provider = self.clone();
        let schema = self.arrow_schema.clone();

        let batches =
            tokio::task::spawn_blocking(move || provider.scan_record_batches(schema, limit))
                .await
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
                .map_err(query_to_datafusion)?;

        let mem = MemTable::try_new(self.arrow_schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

impl RelTable {
    fn scan_record_batches(
        &self,
        schema: &SchemaRef,
        projection: Option<&[usize]>,
        limit: Option<usize>,
        batch_rows: usize,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return Ok(limit_record_batches(
                self.columnar_record_batches(projection)?,
                limit,
            ));
        }

        let rows = self
            .scan_with_keys_limit(limit, batch_rows)?
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        rows.chunks(batch_rows.max(1))
            .map(|chunk| rows_to_record_batch(schema.clone(), chunk))
            .collect()
    }

    fn scan_record_batches_as_of_lsn(
        &self,
        schema: &SchemaRef,
        limit: Option<usize>,
        batch_rows: usize,
        snapshot_lsn: u64,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        if self.layout == TableLayout::Columnar {
            return Err(QueryError::Unsupported(
                "temporal AS OF scans are supported for row-layout tables".to_owned(),
            ));
        }
        let table_schema = self
            .schema
            .as_ref()
            .ok_or_else(|| QueryError::InvalidSchema("AS OF requires a typed table".to_owned()))?;
        let mut rows =
            temporal::rows_as_of_lsn(self.repl.as_ref(), &self.name, table_schema, snapshot_lsn)
                .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?;
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        rows.chunks(batch_rows.max(1))
            .map(|chunk| rows_to_record_batch(schema.clone(), chunk))
            .collect()
    }
}

fn is_empty_group_by(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            expressions.is_empty() && modifiers.is_empty()
        }
        GroupByExpr::All(_) => false,
    }
}

fn simple_filter(expr: &SqlExpr, schema: &TableSchema) -> Result<Option<RelPredicate>, QueryError> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let Some(left) = simple_filter(left, schema)? else {
                return Ok(None);
            };
            let Some(right) = simple_filter(right, schema)? else {
                return Ok(None);
            };
            Ok(Some(RelPredicate::And(flatten_predicates(
                BitmapOp::And,
                vec![left, right],
            ))))
        }
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
            let Some(left) = simple_filter(left, schema)? else {
                return Ok(None);
            };
            let Some(right) = simple_filter(right, schema)? else {
                return Ok(None);
            };
            Ok(Some(RelPredicate::Or(flatten_predicates(
                BitmapOp::Or,
                vec![left, right],
            ))))
        }
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            simple_eq_filter(left, right, schema)
        }
        _ => Ok(None),
    }
}

fn simple_eq_filter(
    left: &SqlExpr,
    right: &SqlExpr,
    schema: &TableSchema,
) -> Result<Option<RelPredicate>, QueryError> {
    if let Some(expression) = simple_index_expression(left, schema)? {
        return literal_filter(expression, right, schema);
    }
    if let Some(expression) = simple_index_expression(right, schema)? {
        return literal_filter(expression, left, schema);
    }
    Ok(None)
}

fn literal_filter(
    expression: RelIndexExpression,
    literal: &SqlExpr,
    schema: &TableSchema,
) -> Result<Option<RelPredicate>, QueryError> {
    let value = sql_expr_to_value(literal)?;
    let Some(value) = coerce_simple_filter_value(schema, expression.column(), value)? else {
        return Ok(None);
    };
    let value = match expression {
        RelIndexExpression::Column(_) => value,
        RelIndexExpression::LowerAscii(_) => match value {
            Value::Str(value) => Value::Str(value.to_ascii_lowercase()),
            _ => return Ok(None),
        },
    };
    Ok(Some(RelPredicate::Eq { expression, value }))
}

fn simple_index_expression(
    expr: &SqlExpr,
    schema: &TableSchema,
) -> Result<Option<RelIndexExpression>, QueryError> {
    if let Some(column) = column_expr(expr) {
        return schema
            .column_index(&column)
            .map(|column| Some(RelIndexExpression::Column(column)))
            .ok_or(QueryError::MissingColumn(column));
    }

    let SqlExpr::Function(function) = expr else {
        return Ok(None);
    };
    if !function.name.to_string().eq_ignore_ascii_case("lower") {
        return Ok(None);
    }
    let FunctionArguments::List(args) = &function.args else {
        return Ok(None);
    };
    if !args.clauses.is_empty() {
        return Ok(None);
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))] = args.args.as_slice() else {
        return Ok(None);
    };
    let Some(column) = column_expr(arg) else {
        return Ok(None);
    };
    schema
        .column_index(&column)
        .map(|column| Some(RelIndexExpression::LowerAscii(column)))
        .ok_or(QueryError::MissingColumn(column))
}

fn flatten_predicates(op: BitmapOp, predicates: Vec<RelPredicate>) -> Vec<RelPredicate> {
    let mut flattened = Vec::new();
    for predicate in predicates {
        match (op, predicate) {
            (BitmapOp::And, RelPredicate::And(parts)) | (BitmapOp::Or, RelPredicate::Or(parts)) => {
                flattened.extend(parts);
            }
            (_, predicate) => flattened.push(predicate),
        }
    }
    flattened
}

fn first_filter_eq(filter: &RelPredicate) -> Option<(RelIndexExpression, &Value)> {
    match filter {
        RelPredicate::Eq { expression, value } => Some((*expression, value)),
        RelPredicate::And(predicates) | RelPredicate::Or(predicates) => {
            predicates.iter().find_map(first_filter_eq)
        }
    }
}

fn column_expr(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(identifier) => Some(identifier.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            let identifier = parts.last()?;
            Some(identifier.value.clone())
        }
        _ => None,
    }
}

fn coerce_simple_filter_value(
    schema: &TableSchema,
    column: usize,
    value: Value,
) -> Result<Option<Value>, QueryError> {
    if value == Value::Null {
        return Ok(None);
    }

    let Some(column) = schema.columns.get(column) else {
        return Err(QueryError::MissingColumn(column.to_string()));
    };

    match (&column.ty, value) {
        (ColumnType::Float, Value::Int(value)) => Ok(Some(Value::Float(i64_to_f64(value)?))),
        (ColumnType::Float, Value::Float(value)) if value.is_finite() => {
            Ok(Some(Value::Float(value)))
        }
        (ColumnType::Float, Value::Float(_)) => Err(QueryError::InvalidValue(
            "non-finite float literal".to_owned(),
        )),
        (ColumnType::Int, Value::Int(value)) => Ok(Some(Value::Int(value))),
        (ColumnType::Str, Value::Str(value)) => Ok(Some(Value::Str(value))),
        (ColumnType::Bool, Value::Bool(value)) => Ok(Some(Value::Bool(value))),
        (ColumnType::Bytes, Value::Bytes(value)) => Ok(Some(Value::Bytes(value))),
        _ => Ok(None),
    }
}

fn i64_to_f64(value: i64) -> Result<f64, QueryError> {
    value
        .to_string()
        .parse::<f64>()
        .map_err(|error| QueryError::InvalidValue(error.to_string()))
}

fn projection_columns(
    projection: &[SelectItem],
    schema: &TableSchema,
) -> Result<(Vec<usize>, Vec<String>), QueryError> {
    let mut columns = Vec::new();
    let mut names = Vec::new();

    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                columns.extend(0..schema.columns.len());
                names.extend(schema.columns.iter().map(|column| column.name.clone()));
            }
            SelectItem::UnnamedExpr(expr) => {
                let Some(name) = column_expr(expr) else {
                    return Err(QueryError::Unsupported(expr.to_string()));
                };
                let index = schema
                    .column_index(&name)
                    .ok_or_else(|| QueryError::MissingColumn(name.clone()))?;
                columns.push(index);
                names.push(name);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let Some(name) = column_expr(expr) else {
                    return Err(QueryError::Unsupported(expr.to_string()));
                };
                let index = schema
                    .column_index(&name)
                    .ok_or_else(|| QueryError::MissingColumn(name.clone()))?;
                columns.push(index);
                names.push(alias.value.clone());
            }
            other => return Err(QueryError::Unsupported(other.to_string())),
        }
    }

    Ok((columns, names))
}

fn project_row(row: &Row, projection: &[usize]) -> Row {
    projection
        .iter()
        .filter_map(|index| row.get(*index).cloned())
        .collect()
}

fn stats_columns_for_schema(
    schema: Option<&TableSchema>,
    rows: &[Row],
) -> Vec<(String, ColumnType)> {
    if let Some(schema) = schema {
        return schema
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.ty.clone()))
            .collect();
    }

    let width = rows.first().map_or(1, Vec::len);
    (0..width)
        .map(|index| (column_name(None, index), ColumnType::Str))
        .collect()
}

fn column_name(schema: Option<&TableSchema>, column: usize) -> String {
    schema
        .and_then(|schema| schema.columns.get(column))
        .map_or_else(|| format!("column_{column}"), |column| column.name.clone())
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn validate_table_name(name: &str) -> Result<(), QueryError> {
    if name.is_empty() {
        return Err(QueryError::InvalidSchema(
            "table name cannot be empty".to_owned(),
        ));
    }

    Ok(())
}

fn validate_layout(
    schema: Option<&TableSchema>,
    indexes: &[RelIndexSpec],
    layout: TableLayout,
) -> Result<(), QueryError> {
    if let Some(schema) = schema {
        validate_rel_indexes(schema, indexes)?;
    }

    if layout == TableLayout::Row {
        return Ok(());
    }

    if schema.is_none() {
        return Err(QueryError::InvalidSchema(
            "columnar tables require a schema".to_owned(),
        ));
    }

    if !indexes.is_empty() {
        return Err(QueryError::InvalidSchema(
            "columnar tables do not support B-tree indexes".to_owned(),
        ));
    }

    Ok(())
}

fn validate_rel_indexes(schema: &TableSchema, indexes: &[RelIndexSpec]) -> Result<(), QueryError> {
    let mut ids = BTreeSet::new();
    for index in indexes {
        if !ids.insert(index.id) {
            return Err(QueryError::InvalidSchema(format!(
                "duplicate index id {}",
                index.id
            )));
        }
        validate_index_expression(schema, index.expression)?;
        for column in &index.include {
            validate_column(Some(schema), *column)?;
        }
        if let Some(predicate) = &index.predicate {
            validate_rel_predicate(schema, predicate)?;
        }
    }
    Ok(())
}

fn validate_index_expression(
    schema: &TableSchema,
    expression: RelIndexExpression,
) -> Result<(), QueryError> {
    validate_column(Some(schema), expression.column())?;
    if let RelIndexExpression::LowerAscii(column) = expression
        && schema.columns.get(column).map(|column| &column.ty) != Some(&ColumnType::Str)
    {
        return Err(QueryError::InvalidSchema(
            "lower_ascii indexes require a string column".to_owned(),
        ));
    }
    Ok(())
}

fn validate_rel_predicate(
    schema: &TableSchema,
    predicate: &RelPredicate,
) -> Result<(), QueryError> {
    match predicate {
        RelPredicate::Eq { expression, .. } => validate_index_expression(schema, *expression),
        RelPredicate::And(predicates) | RelPredicate::Or(predicates) => {
            for predicate in predicates {
                validate_rel_predicate(schema, predicate)?;
            }
            Ok(())
        }
    }
}

fn validate_doc_fields(fields: &[DocField]) -> Result<(), QueryError> {
    if fields.is_empty() {
        return Err(QueryError::InvalidSchema(
            "document provider needs at least one field".to_owned(),
        ));
    }

    let mut names = BTreeSet::new();
    for field in fields {
        if field.name.is_empty() {
            return Err(QueryError::InvalidSchema(
                "document field name cannot be empty".to_owned(),
            ));
        }

        if !names.insert(field.name.clone()) {
            return Err(QueryError::InvalidSchema(format!(
                "duplicate document field {}",
                field.name
            )));
        }
    }

    Ok(())
}

fn validate_schema(schema: &TableSchema) -> Result<(), QueryError> {
    if schema.columns.is_empty() {
        return Err(QueryError::InvalidSchema(
            "schema needs at least one column".to_owned(),
        ));
    }

    if schema.primary_key >= schema.columns.len() {
        return Err(QueryError::InvalidSchema(
            "primary key index is out of bounds".to_owned(),
        ));
    }

    let mut names = BTreeSet::new();
    for column in &schema.columns {
        if column.name.is_empty() {
            return Err(QueryError::InvalidSchema(
                "column name cannot be empty".to_owned(),
            ));
        }

        if !names.insert(column.name.clone()) {
            return Err(QueryError::InvalidSchema(format!(
                "duplicate column {}",
                column.name
            )));
        }
    }

    Ok(())
}

fn validate_row(schema: Option<&TableSchema>, row: &Row) -> Result<(), QueryError> {
    let Some(schema) = schema else {
        if row.len() == 1 {
            return Ok(());
        }

        return Err(QueryError::InvalidRow(
            "schemaless table expects one value column".to_owned(),
        ));
    };

    if row.len() != schema.columns.len() {
        return Err(QueryError::InvalidRow(format!(
            "expected {} columns, got {}",
            schema.columns.len(),
            row.len()
        )));
    }

    for (column, value) in schema.columns.iter().zip(row) {
        if !value_matches_column(value, column) {
            return Err(QueryError::InvalidRow(format!(
                "column {} has invalid type",
                column.name
            )));
        }
    }

    Ok(())
}

fn value_matches_column(value: &Value, column: &ColumnDef) -> bool {
    match value {
        Value::Null => column.nullable || column.ty == ColumnType::Null,
        Value::Int(_) => column.ty == ColumnType::Int,
        Value::Float(value) => column.ty == ColumnType::Float && !value.is_nan(),
        Value::Str(_) => column.ty == ColumnType::Str,
        Value::Bool(_) => column.ty == ColumnType::Bool,
        Value::Bytes(_) => column.ty == ColumnType::Bytes,
        Value::Array(_) | Value::Object(_) | Value::Vector(_) | Value::GeoPoint { .. } => false,
    }
}

fn validate_column(schema: Option<&TableSchema>, column: usize) -> Result<(), QueryError> {
    let Some(schema) = schema else {
        if column == 0 {
            return Ok(());
        }

        return Err(QueryError::MissingColumn(column.to_string()));
    };

    if column < schema.columns.len() {
        Ok(())
    } else {
        Err(QueryError::MissingColumn(column.to_string()))
    }
}

fn primary_key_value<'row>(
    schema: Option<&TableSchema>,
    row: &'row Row,
) -> Result<&'row Value, QueryError> {
    let index = schema.map_or(0, |schema| schema.primary_key);
    let value = row
        .get(index)
        .ok_or_else(|| QueryError::InvalidRow("missing primary key".to_owned()))?;

    if matches!(value, Value::Null) {
        return Err(QueryError::InvalidRow(
            "primary key cannot be null".to_owned(),
        ));
    }

    Ok(value)
}

fn row_with_primary_key(
    schema: Option<&TableSchema>,
    rows: &[Row],
    primary_key: &Value,
) -> Option<usize> {
    rows.iter()
        .position(|row| row_primary_key_eq(schema, row, primary_key))
}

fn row_primary_key_eq(schema: Option<&TableSchema>, row: &Row, primary_key: &Value) -> bool {
    primary_key_value(schema, row).is_ok_and(|value| value == primary_key)
}

fn decode_row(bytes: &[u8]) -> Result<Option<Row>, QueryError> {
    match decode_value(bytes)? {
        Value::Array(row) => Ok(Some(row)),
        _ => Err(QueryError::Storage(StorageError::Corruption(
            "relational row is not an array".to_owned(),
        ))),
    }
}

pub(crate) fn decode_row_bytes(bytes: &[u8]) -> Result<Option<Row>, QueryError> {
    decode_row(bytes)
}

pub(crate) fn schema_put_op(table: &str, schema: &TableSchema) -> Result<Op, QueryError> {
    Ok(Op::Put {
        table: REL_SCHEMA_TABLE.to_owned(),
        key: table.as_bytes().to_vec(),
        value: serde_json::to_vec(schema).map_err(|error| QueryError::Serde(error.to_string()))?,
    })
}

fn read_schema(repl: &dyn Replication, table: &str) -> Result<Option<TableSchema>, QueryError> {
    let Some(bytes) = repl.read(REL_SCHEMA_TABLE, table.as_bytes(), ReadConsistency::Strong)?
    else {
        return Ok(None);
    };

    serde_json::from_slice(&bytes).map_err(|error| QueryError::Serde(error.to_string()))
}

fn table_prefix(table: &str) -> Bytes {
    let table = table.as_bytes();
    let mut key = Vec::with_capacity(8 + table.len());
    keyenc::push_len_bytes(&mut key, table);
    key
}

fn table_range_bounds(table: &str) -> (Bytes, Bytes) {
    let start = table_prefix(table);
    let end = keyenc::range_end(&start);
    (start, end)
}

fn columnar_segment_range_bounds(table: &str) -> (Bytes, Bytes) {
    table_range_bounds(table)
}

fn columnar_segment_key(table: &str, segment_id: u64) -> Bytes {
    let mut key = table_prefix(table);
    key.extend_from_slice(&segment_id.to_be_bytes());
    key
}

fn columnar_segment_id(table: &str, key: &[u8]) -> Option<u64> {
    let id = key.strip_prefix(table_prefix(table).as_slice())?;
    Some(u64::from_be_bytes(id.try_into().ok()?))
}

fn make_row_key(table: &str, primary_key: &Value) -> Result<Bytes, QueryError> {
    let mut key = table_prefix(table);
    key.extend_from_slice(&encode_rel_key(primary_key)?);
    Ok(key)
}

fn make_index_prefix(table: &str, index_id: u32, value: &Value) -> Result<Bytes, QueryError> {
    let mut key = table_prefix(table);
    key.extend_from_slice(&index_id.to_be_bytes());
    key.extend_from_slice(&encode_rel_key(value)?);
    Ok(key)
}

fn make_index_key(
    table: &str,
    index_id: u32,
    value: &Value,
    row_key: &[u8],
) -> Result<Bytes, QueryError> {
    let mut key = make_index_prefix(table, index_id, value)?;
    key.extend_from_slice(row_key);
    key.extend_from_slice(&(row_key.len() as u64).to_be_bytes());
    Ok(key)
}

fn index_expression_value(
    row: &Row,
    expression: RelIndexExpression,
) -> Result<Option<Value>, QueryError> {
    let column = expression.column();
    let value = row
        .get(column)
        .ok_or_else(|| QueryError::InvalidRow(format!("missing indexed column {column}")))?;
    Ok(match (expression, value) {
        (RelIndexExpression::Column(_), value) => Some(value.clone()),
        (RelIndexExpression::LowerAscii(_), Value::Str(value)) => {
            Some(Value::Str(value.to_ascii_lowercase()))
        }
        (RelIndexExpression::LowerAscii(_), _) => None,
    })
}

fn index_predicate_matches(index: &RelIndexSpec, row: &Row) -> Result<bool, QueryError> {
    index
        .predicate
        .as_ref()
        .map_or(Ok(true), |predicate| predicate_matches(row, predicate))
}

fn predicate_matches(row: &Row, predicate: &RelPredicate) -> Result<bool, QueryError> {
    match predicate {
        RelPredicate::Eq { expression, value } => {
            Ok(index_expression_value(row, *expression)?.as_ref() == Some(value))
        }
        RelPredicate::And(predicates) => {
            for predicate in predicates {
                if !predicate_matches(row, predicate)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        RelPredicate::Or(predicates) => {
            for predicate in predicates {
                if predicate_matches(row, predicate)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn encode_index_payload(row: &Row, index: &RelIndexSpec) -> Result<Bytes, QueryError> {
    if index.include.is_empty() {
        return Ok(EMPTY_INDEX_VALUE.to_vec());
    }
    let values = index
        .include
        .iter()
        .map(|column| {
            row.get(*column)
                .cloned()
                .ok_or_else(|| QueryError::InvalidRow(format!("missing included column {column}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    encode_value(&Value::Array(values)).map_err(Into::into)
}

fn decode_index_payload(bytes: &[u8]) -> Result<Vec<Value>, QueryError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    match decode_value(bytes)? {
        Value::Array(values) => Ok(values),
        _ => Err(QueryError::Storage(StorageError::Corruption(
            "index payload is not an array".to_owned(),
        ))),
    }
}

fn reconstruct_covering_row(
    width: usize,
    index: &RelIndexSpec,
    indexed_value: &Value,
    payload: &[Value],
) -> Result<Row, QueryError> {
    let mut row = vec![Value::Null; width];
    if let RelIndexExpression::Column(column) = index.expression
        && let Some(slot) = row.get_mut(column)
    {
        *slot = indexed_value.clone();
    }
    for (column, value) in index.include.iter().zip(payload) {
        let Some(slot) = row.get_mut(*column) else {
            return Err(QueryError::InvalidSchema(format!(
                "included column {column} is out of bounds"
            )));
        };
        *slot = value.clone();
    }
    Ok(row)
}

fn combine_bitmap_sets(op: BitmapOp, mut sets: Vec<BTreeSet<Bytes>>) -> BTreeSet<Bytes> {
    let Some(first) = sets.pop() else {
        return BTreeSet::new();
    };

    match op {
        BitmapOp::And => sets
            .into_iter()
            .fold(first, |acc, set| acc.intersection(&set).cloned().collect()),
        BitmapOp::Or => sets.into_iter().fold(first, |mut acc, set| {
            acc.extend(set);
            acc
        }),
    }
}

fn filter_eq_for_expression(
    filter: &RelPredicate,
    expression: RelIndexExpression,
) -> Option<&Value> {
    match filter {
        RelPredicate::Eq {
            expression: candidate,
            value,
        } if *candidate == expression => Some(value),
        RelPredicate::And(predicates) => predicates
            .iter()
            .find_map(|predicate| filter_eq_for_expression(predicate, expression)),
        RelPredicate::Or(_) | RelPredicate::Eq { .. } => None,
    }
}

fn filter_eq_for_expression_for_bitmap(
    filter: &RelPredicate,
    expression: RelIndexExpression,
    op: BitmapOp,
) -> Option<&Value> {
    match op {
        BitmapOp::And => filter_eq_for_expression(filter, expression),
        BitmapOp::Or => match filter {
            RelPredicate::Eq {
                expression: candidate,
                value,
            } if *candidate == expression => Some(value),
            RelPredicate::Or(predicates) => predicates.iter().find_map(|predicate| {
                filter_eq_for_expression_for_bitmap(predicate, expression, op)
            }),
            RelPredicate::And(predicates) => predicates
                .iter()
                .find_map(|predicate| filter_eq_for_expression(predicate, expression)),
            RelPredicate::Eq { .. } => None,
        },
    }
}

fn row_key_from_index_key(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 8 {
        return None;
    }

    let len_start = key.len().checked_sub(8)?;
    let row_len = u64::from_be_bytes(key[len_start..].try_into().ok()?);
    let row_len = usize::try_from(row_len).ok()?;
    let row_start = len_start.checked_sub(row_len)?;
    Some(&key[row_start..len_start])
}

fn encode_rel_key(value: &Value) -> Result<Bytes, QueryError> {
    match value {
        Value::Bytes(bytes) => {
            let mut encoded = vec![0x05];
            encode_bytes(bytes, &mut encoded);
            Ok(encoded)
        }
        Value::Array(_) | Value::Object(_) | Value::Vector(_) | Value::GeoPoint { .. } => Err(
            QueryError::InvalidValue("complex values cannot be used as relational keys".to_owned()),
        ),
        other => encode_index_value(other).map_err(Into::into),
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Bytes) {
    keyenc::encode_terminated_bytes(bytes, out);
}

fn schema_to_arrow(schema: Option<&TableSchema>) -> SchemaRef {
    let fields = if let Some(schema) = schema {
        schema
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    column_type_to_arrow(&column.ty),
                    column.nullable || column.ty == ColumnType::Null,
                )
            })
            .collect()
    } else {
        vec![Field::new("value", DataType::Utf8, true)]
    };

    Arc::new(Schema::new(fields))
}

fn materialized_view_arrow_schema(
    spec: &cdc::MaterializedViewSpec,
    source_schema: &TableSchema,
) -> Result<SchemaRef, QueryError> {
    let group = source_schema
        .columns
        .get(spec.group_by)
        .ok_or_else(|| QueryError::MissingColumn(format!("group column {}", spec.group_by)))?;
    let mut fields = vec![Field::new(
        group.name.clone(),
        column_type_to_arrow(&group.ty),
        group.nullable,
    )];
    for aggregate in &spec.aggregates {
        let ty = match aggregate.kind {
            cdc::AggregateKind::Count => DataType::Int64,
            cdc::AggregateKind::Sum | cdc::AggregateKind::Avg => DataType::Float64,
        };
        fields.push(Field::new(aggregate.output_name.clone(), ty, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn project_schema(schema: &SchemaRef, projection: &[usize]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| schema.field(*column).clone())
            .collect::<Vec<_>>(),
    ))
}

fn doc_fields_to_arrow(fields: &[DocField]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|field| Field::new(field.name.clone(), column_type_to_arrow(&field.ty), true))
            .collect::<Vec<_>>(),
    ))
}

fn column_type_to_arrow(ty: &ColumnType) -> DataType {
    match ty {
        ColumnType::Int => DataType::Int64,
        ColumnType::Float => DataType::Float64,
        ColumnType::Str => DataType::Utf8,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::Bytes => DataType::Binary,
        ColumnType::Null => DataType::Null,
    }
}

fn coerce_doc_value(value: Value, ty: &ColumnType) -> Value {
    if matches!(value, Value::Null) || matches!(ty, ColumnType::Null) {
        return Value::Null;
    }

    match (value, ty) {
        (Value::Int(value), ColumnType::Int) => Value::Int(value),
        (Value::Float(value), ColumnType::Float) if !value.is_nan() => Value::Float(value),
        (Value::Str(value), ColumnType::Str) => Value::Str(value),
        (Value::Bool(value), ColumnType::Bool) => Value::Bool(value),
        (Value::Bytes(value), ColumnType::Bytes) => Value::Bytes(value),
        _ => Value::Null,
    }
}

pub(crate) fn rows_to_record_batch(
    schema: SchemaRef,
    rows: &[Row],
) -> Result<RecordBatch, QueryError> {
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column_index, field)| values_to_array(rows, column_index, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, arrays).map_err(|error| QueryError::DataFusion(error.to_string()))
}

fn columnar_segment_meta(
    table: &str,
    segment_id: u64,
    schema: &TableSchema,
    rows: &[Row],
    bytes: usize,
) -> ColumnarSegmentMeta {
    let columns = schema
        .columns
        .iter()
        .enumerate()
        .map(|(column, _)| segment_column_stats(rows, column))
        .collect::<Vec<_>>();
    ColumnarSegmentMeta {
        table: table.to_owned(),
        segment_id,
        row_count: usize_to_u64(rows.len()),
        bytes: usize_to_u64(bytes),
        columns,
    }
}

fn segment_column_stats(rows: &[Row], column: usize) -> SegmentColumnStats {
    let mut min: Option<Value> = None;
    let mut max: Option<Value> = None;
    let mut null_count = 0_u64;
    for row in rows {
        let value = row.get(column).cloned().unwrap_or(Value::Null);
        if matches!(value, Value::Null) {
            null_count = null_count.saturating_add(1);
            continue;
        }
        if min.as_ref().is_none_or(|current| {
            matches!(rel_value_cmp(&value, current), Ok(std::cmp::Ordering::Less))
        }) {
            min = Some(value.clone());
        }
        if max.as_ref().is_none_or(|current| {
            matches!(
                rel_value_cmp(&value, current),
                Ok(std::cmp::Ordering::Greater)
            )
        }) {
            max = Some(value);
        }
    }
    SegmentColumnStats {
        min,
        max,
        null_count,
    }
}

fn segment_may_match_filter(
    meta: &ColumnarSegmentMeta,
    filter: &RelPredicate,
) -> Result<bool, QueryError> {
    match filter {
        RelPredicate::Eq { expression, value } => {
            let RelIndexExpression::Column(column) = expression else {
                return Ok(true);
            };
            let Some(stats) = meta.columns.get(*column) else {
                return Ok(true);
            };
            segment_may_contain_value(stats, value)
        }
        RelPredicate::And(predicates) => {
            for predicate in predicates {
                if !segment_may_match_filter(meta, predicate)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        RelPredicate::Or(predicates) => {
            for predicate in predicates {
                if segment_may_match_filter(meta, predicate)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn segment_may_contain_value(
    stats: &SegmentColumnStats,
    value: &Value,
) -> Result<bool, QueryError> {
    if matches!(value, Value::Null) {
        return Ok(stats.null_count > 0);
    }
    let Some(min) = &stats.min else {
        return Ok(false);
    };
    let Some(max) = &stats.max else {
        return Ok(false);
    };
    Ok(rel_value_cmp(value, min)?.is_ge() && rel_value_cmp(value, max)?.is_le())
}

fn rel_value_cmp(left: &Value, right: &Value) -> Result<std::cmp::Ordering, QueryError> {
    Ok(encode_rel_key(left)?.cmp(&encode_rel_key(right)?))
}

fn rows_to_parquet_bytes(schema: &TableSchema, rows: &[Row]) -> Result<Bytes, QueryError> {
    let batch = rows_to_record_batch(schema_to_arrow(Some(schema)), rows)?;
    record_batches_to_parquet_bytes(&[batch])
}

fn record_batches_to_parquet_bytes(batches: &[RecordBatch]) -> Result<Bytes, QueryError> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };

    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, first.schema(), None)
            .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?;
        }
        writer
            .close()
            .map_err(|error| QueryError::Storage(StorageError::Backend(error.to_string())))?;
    }

    Ok(bytes)
}

pub(crate) fn parquet_bytes_to_rows(
    bytes: &[u8],
    schema: &TableSchema,
) -> Result<Vec<Row>, QueryError> {
    let batches = parquet_bytes_to_batches(bytes, None)?;
    let rows = batches_to_rows(&batches)?;
    for row in &rows {
        validate_row(Some(schema), row)?;
    }
    Ok(rows)
}

fn parquet_bytes_to_batches(
    bytes: &[u8],
    projection: Option<&[usize]>,
) -> Result<Vec<RecordBatch>, QueryError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(ByteBuf::copy_from_slice(bytes))
        .map_err(|error| QueryError::Storage(StorageError::Corruption(error.to_string())))?;

    if let Some(projection) = projection {
        let mask = ProjectionMask::leaves(builder.parquet_schema(), projection.iter().copied());
        builder = builder.with_projection(mask);
    }

    let reader = builder
        .build()
        .map_err(|error| QueryError::Storage(StorageError::Corruption(error.to_string())))?;

    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| QueryError::Storage(StorageError::Corruption(error.to_string())))
}

fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<Row>, QueryError> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for column_index in 0..batch.num_columns() {
                row.push(arrow_value_to_value(
                    batch.column(column_index).as_ref(),
                    batch.schema().field(column_index).data_type(),
                    row_index,
                )?);
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn values_to_array(
    rows: &[Row],
    column_index: usize,
    data_type: &DataType,
) -> Result<ArrayRef, QueryError> {
    match data_type {
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| match row.get(column_index) {
                    Some(Value::Int(value)) => Ok(Some(*value)),
                    Some(Value::Null) => Ok(None),
                    _ => Err(invalid_arrow_value(column_index)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| match row.get(column_index) {
                    Some(Value::Float(value)) => Ok(Some(*value)),
                    Some(Value::Null) => Ok(None),
                    _ => Err(invalid_arrow_value(column_index)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| match row.get(column_index) {
                    Some(Value::Str(value)) => Ok(Some(value.clone())),
                    Some(Value::Null) => Ok(None),
                    Some(value) if column_index == 0 && row.len() == 1 => {
                        serde_json::to_string(value)
                            .map(Some)
                            .map_err(|error| QueryError::Serde(error.to_string()))
                    }
                    _ => Err(invalid_arrow_value(column_index)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| match row.get(column_index) {
                    Some(Value::Bool(value)) => Ok(Some(*value)),
                    Some(Value::Null) => Ok(None),
                    _ => Err(invalid_arrow_value(column_index)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match row.get(column_index) {
                    Some(Value::Bytes(value)) => builder.append_value(value),
                    Some(Value::Null) => builder.append_null(),
                    _ => return Err(invalid_arrow_value(column_index)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
        other => Err(QueryError::DataFusion(format!(
            "unsupported arrow type {other:?}"
        ))),
    }
}

fn invalid_arrow_value(column_index: usize) -> QueryError {
    QueryError::InvalidRow(format!("cannot convert column {column_index} to Arrow"))
}

fn limit_record_batches(batches: Vec<RecordBatch>, limit: Option<usize>) -> Vec<RecordBatch> {
    let Some(limit) = limit else {
        return batches;
    };

    let mut remaining = limit;
    let mut limited = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            limited.push(batch);
        } else {
            limited.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    limited
}

fn record_batches_to_rows(
    batches: &[RecordBatch],
    fallback_columns: Vec<String>,
) -> Result<SqlRows, QueryError> {
    let columns = if let Some(first) = batches.first() {
        first
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    } else {
        fallback_columns
    };

    if batches.is_empty() {
        return Ok(SqlRows {
            columns,
            rows: Vec::new(),
        });
    }

    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for column_index in 0..batch.num_columns() {
                row.push(arrow_value_to_value(
                    batch.column(column_index).as_ref(),
                    batch.schema().field(column_index).data_type(),
                    row_index,
                )?);
            }
            rows.push(row);
        }
    }

    Ok(SqlRows { columns, rows })
}

fn arrow_value_to_value(
    array: &dyn Array,
    data_type: &DataType,
    row_index: usize,
) -> Result<Value, QueryError> {
    if array.is_null(row_index) {
        return Ok(Value::Null);
    }

    match data_type {
        DataType::Int64 => Ok(Value::Int(
            downcast_array::<Int64Array>(array)?.value(row_index),
        )),
        DataType::UInt64 => {
            let value = downcast_array::<UInt64Array>(array)?.value(row_index);
            let value =
                i64::try_from(value).map_err(|error| QueryError::DataFusion(error.to_string()))?;
            Ok(Value::Int(value))
        }
        DataType::Float64 => Ok(Value::Float(
            downcast_array::<Float64Array>(array)?.value(row_index),
        )),
        DataType::Utf8 => Ok(Value::Str(
            downcast_array::<StringArray>(array)?
                .value(row_index)
                .to_owned(),
        )),
        DataType::Boolean => Ok(Value::Bool(
            downcast_array::<BooleanArray>(array)?.value(row_index),
        )),
        DataType::Binary => Ok(Value::Bytes(
            downcast_array::<BinaryArray>(array)?
                .value(row_index)
                .to_vec(),
        )),
        DataType::Null => Ok(Value::Null),
        other => Err(QueryError::DataFusion(format!(
            "unsupported result type {other:?}"
        ))),
    }
}

fn downcast_array<T: 'static>(array: &dyn Array) -> Result<&T, QueryError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| QueryError::DataFusion("arrow array type did not match schema".to_owned()))
}

fn values_from_insert(
    insert: &sqlparser::ast::Insert,
    table: &RelTable,
) -> Result<Vec<Row>, QueryError> {
    let source = insert
        .source
        .as_deref()
        .ok_or_else(|| QueryError::Unsupported("INSERT without source".to_owned()))?;
    let values = values_query(source)?;

    values
        .rows
        .iter()
        .map(|row| insert_row_from_sql(row, &insert.columns, table))
        .collect()
}

/// Outcome of one optimistic attempt to apply an INSERT to a row-layout table.
enum InsertAttempt {
    Committed(Vec<Row>),
    /// A row this attempt read or inserted changed before it committed.
    Conflict,
}

fn apply_insert_rows_atomic(
    table: &RelTable,
    rows: &[Row],
    insert: &sqlparser::ast::Insert,
) -> Result<Vec<Row>, QueryError> {
    if let Some(OnInsert::OnConflict(conflict)) = &insert.on {
        validate_conflict_target(table.schema.as_ref(), conflict)?;
    }
    if table.layout == TableLayout::Columnar {
        return apply_columnar_insert_rows_atomic(table, rows, insert);
    }

    // A plain INSERT only loses a race to a row with the same primary key.
    // ON CONFLICT reads the rows again and reapplies its action instead.
    let retry_conflicts = matches!(insert.on, Some(OnInsert::OnConflict(_)));
    for _ in 0..=WRITE_CONFLICT_RETRIES {
        match try_apply_row_insert(table, rows, insert)? {
            InsertAttempt::Committed(affected) => return Ok(affected),
            InsertAttempt::Conflict if retry_conflicts => thread::yield_now(),
            InsertAttempt::Conflict => return Err(QueryError::DuplicatePrimaryKey),
        }
    }

    Err(QueryError::Repl(ReplError::Conflict))
}

fn try_apply_row_insert(
    table: &RelTable,
    rows: &[Row],
    insert: &sqlparser::ast::Insert,
) -> Result<InsertAttempt, QueryError> {
    if insert.on.is_none() {
        return write_batch::try_plain_insert(table, rows);
    }
    let mut initial = BTreeMap::<Bytes, Option<(Option<Bytes>, Row)>>::new();
    let mut staged = BTreeMap::<Bytes, Row>::new();
    let mut conditions = BTreeMap::<Bytes, WriteCondition>::new();
    let mut ops = Vec::new();
    let mut affected = Vec::new();

    for row in rows {
        let primary_key = table.primary_key_value_for_row(row)?;
        let row_key = table.row_key_for_primary_key(&primary_key)?;
        let existing_row = if let Some(staged_row) = staged.get(&row_key) {
            Some(staged_row.clone())
        } else {
            if !initial.contains_key(&row_key) {
                let old_bytes =
                    table
                        .repl
                        .read(REL_ROWS_TABLE, &row_key, ReadConsistency::Strong)?;
                let old_row = old_bytes.as_deref().map(decode_row).transpose()?.flatten();
                initial.insert(
                    row_key.clone(),
                    old_row.map(|decoded| (old_bytes.clone(), decoded)),
                );
            }
            initial
                .get(&row_key)
                .and_then(|entry| entry.as_ref().map(|(_, decoded)| decoded.clone()))
        };

        match existing_row {
            None => {
                ensure_insert_condition(&mut conditions, &initial, &row_key);
                table.append_put_ops(&mut ops, row.clone(), &row_key)?;
                staged.insert(row_key, row.clone());
                affected.push(row.clone());
            }
            Some(existing_row) => {
                let Some(OnInsert::OnConflict(conflict)) = &insert.on else {
                    return Err(QueryError::DuplicatePrimaryKey);
                };
                match &conflict.action {
                    OnConflictAction::DoNothing => {}
                    OnConflictAction::DoUpdate(update) => {
                        if update.selection.is_some() {
                            return Err(QueryError::Unsupported(
                                "ON CONFLICT DO UPDATE WHERE is unsupported".to_owned(),
                            ));
                        }
                        let updated = apply_update_assignments(
                            table.schema.as_ref(),
                            existing_row.clone(),
                            row,
                            &update.assignments,
                        )?;
                        ensure_insert_condition(&mut conditions, &initial, &row_key);
                        ops.extend(table.update_ops_for_key(
                            &existing_row,
                            updated.clone(),
                            &row_key,
                        )?);
                        staged.insert(row_key, updated.clone());
                        affected.push(updated);
                    }
                }
            }
        }
    }

    write_batch::commit_insert(table, conditions.into_values().collect(), ops, affected)
}

fn ensure_insert_condition(
    conditions: &mut BTreeMap<Bytes, WriteCondition>,
    initial: &BTreeMap<Bytes, Option<(Option<Bytes>, Row)>>,
    row_key: &Bytes,
) {
    if conditions.contains_key(row_key) {
        return;
    }

    let old_bytes = initial
        .get(row_key)
        .and_then(|entry| entry.as_ref().and_then(|(bytes, _)| bytes.clone()));
    let condition = match old_bytes {
        Some(bytes) => WriteCondition::ValueEquals {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key.clone(),
            expected: Some(bytes),
        },
        None => WriteCondition::KeyMissing {
            table: REL_ROWS_TABLE.to_owned(),
            key: row_key.clone(),
        },
    };
    conditions.insert(row_key.clone(), condition);
}

fn apply_columnar_insert_rows_atomic(
    table: &RelTable,
    rows: &[Row],
    insert: &sqlparser::ast::Insert,
) -> Result<Vec<Row>, QueryError> {
    if insert.on.is_none() {
        table.append_columnar_rows(rows)?;
        return Ok(rows.to_vec());
    }
    let mut affected = Vec::new();
    table.rewrite_columnar_rows(|current| {
        affected.clear();
        for row in rows {
            let primary_key = table.primary_key_value_for_row(row)?;
            match row_with_primary_key(table.schema.as_ref(), current, &primary_key) {
                None => {
                    current.push(row.clone());
                    affected.push(row.clone());
                }
                Some(index) => {
                    let Some(OnInsert::OnConflict(conflict)) = &insert.on else {
                        return Err(QueryError::DuplicatePrimaryKey);
                    };
                    match &conflict.action {
                        OnConflictAction::DoNothing => {}
                        OnConflictAction::DoUpdate(update) => {
                            if update.selection.is_some() {
                                return Err(QueryError::Unsupported(
                                    "ON CONFLICT DO UPDATE WHERE is unsupported".to_owned(),
                                ));
                            }
                            let updated = apply_update_assignments(
                                table.schema.as_ref(),
                                current[index].clone(),
                                row,
                                &update.assignments,
                            )?;
                            current[index].clone_from(&updated);
                            affected.push(updated);
                        }
                    }
                }
            }
        }
        Ok(!affected.is_empty())
    })?;

    Ok(affected)
}

fn validate_conflict_target(
    schema: Option<&TableSchema>,
    conflict: &sqlparser::ast::OnConflict,
) -> Result<(), QueryError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    let Some(target) = &conflict.conflict_target else {
        return Ok(());
    };
    match target {
        ConflictTarget::Columns(columns) if columns.len() == 1 => {
            let column = &columns[0].value;
            let primary = schema
                .primary_key_column()
                .ok_or_else(|| QueryError::InvalidSchema("missing primary key".to_owned()))?;
            if &primary.name == column {
                return Ok(());
            }
            Err(QueryError::Unsupported(format!(
                "ON CONFLICT only supports primary key target {}, got {column}",
                primary.name
            )))
        }
        ConflictTarget::Columns(_) | ConflictTarget::OnConstraint(_) => {
            Err(QueryError::Unsupported(
                "ON CONFLICT only supports a single primary key column".to_owned(),
            ))
        }
    }
}

fn apply_update_assignments(
    schema: Option<&TableSchema>,
    mut existing: Row,
    excluded: &Row,
    assignments: &[sqlparser::ast::Assignment],
) -> Result<Row, QueryError> {
    let schema = schema.ok_or_else(|| {
        QueryError::InvalidSchema("ON CONFLICT DO UPDATE requires a schema".to_owned())
    })?;
    for assignment in assignments {
        let AssignmentTarget::ColumnName(column) = &assignment.target else {
            return Err(QueryError::Unsupported(
                "tuple assignment in ON CONFLICT is unsupported".to_owned(),
            ));
        };
        let column_name = object_name_to_string(column)?;
        let index = schema
            .column_index(&column_name)
            .ok_or_else(|| QueryError::MissingColumn(column_name.clone()))?;
        existing[index] = conflict_assignment_value(schema, excluded, &assignment.value)?;
    }
    Ok(existing)
}

fn conflict_assignment_value(
    schema: &TableSchema,
    excluded: &Row,
    expr: &SqlExpr,
) -> Result<Value, QueryError> {
    match expr {
        SqlExpr::CompoundIdentifier(parts)
            if parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("excluded") =>
        {
            let column = &parts[1].value;
            let index = schema
                .column_index(column)
                .ok_or_else(|| QueryError::MissingColumn(column.clone()))?;
            excluded
                .get(index)
                .cloned()
                .ok_or_else(|| QueryError::InvalidRow(format!("missing excluded column {column}")))
        }
        SqlExpr::Identifier(identifier) => {
            let index = schema
                .column_index(&identifier.value)
                .ok_or_else(|| QueryError::MissingColumn(identifier.value.clone()))?;
            excluded.get(index).cloned().ok_or_else(|| {
                QueryError::InvalidRow(format!("missing excluded column {}", identifier.value))
            })
        }
        _ => sql_expr_to_value(expr),
    }
}

fn returning_rows(
    schema: Option<&TableSchema>,
    returning: &[SelectItem],
    rows: &[Row],
) -> Result<(Vec<String>, Vec<Row>), QueryError> {
    let schema = schema
        .ok_or_else(|| QueryError::InvalidSchema("RETURNING requires a schema".to_owned()))?;
    if returning.len() == 1 && matches!(returning[0], SelectItem::Wildcard(_)) {
        return Ok((
            schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            rows.to_vec(),
        ));
    }

    let mut indexes = Vec::new();
    let mut columns = Vec::new();
    for item in returning {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(identifier)) => {
                let index = schema
                    .column_index(&identifier.value)
                    .ok_or_else(|| QueryError::MissingColumn(identifier.value.clone()))?;
                indexes.push(index);
                columns.push(identifier.value.clone());
            }
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Identifier(identifier),
                alias,
            } => {
                let index = schema
                    .column_index(&identifier.value)
                    .ok_or_else(|| QueryError::MissingColumn(identifier.value.clone()))?;
                indexes.push(index);
                columns.push(alias.value.clone());
            }
            other => return Err(QueryError::Unsupported(format!("RETURNING {other}"))),
        }
    }

    Ok((
        columns,
        rows.iter()
            .map(|row| indexes.iter().map(|index| row[*index].clone()).collect())
            .collect(),
    ))
}

fn values_query(query: &Query) -> Result<&sqlparser::ast::Values, QueryError> {
    match query.body.as_ref() {
        SetExpr::Values(values) => Ok(values),
        other => Err(QueryError::Unsupported(other.to_string())),
    }
}

fn insert_row_from_sql(
    values: &[SqlExpr],
    columns: &[ObjectName],
    table: &RelTable,
) -> Result<Row, QueryError> {
    let Some(schema) = table.schema.as_ref() else {
        if values.len() != 1 {
            return Err(QueryError::InvalidRow(
                "schemaless INSERT needs one value".to_owned(),
            ));
        }
        return Ok(vec![sql_expr_to_value(&values[0])?]);
    };

    if columns.is_empty() {
        if values.len() != schema.columns.len() {
            return Err(QueryError::InvalidRow(format!(
                "expected {} values, got {}",
                schema.columns.len(),
                values.len()
            )));
        }

        return values.iter().map(sql_expr_to_value).collect();
    }

    if columns.len() != values.len() {
        return Err(QueryError::InvalidRow(
            "INSERT columns and values length mismatch".to_owned(),
        ));
    }

    let mut row = vec![Value::Null; schema.columns.len()];
    for (column, value) in columns.iter().zip(values) {
        let column = object_name_to_string(column)?;
        let index = schema
            .column_index(&column)
            .ok_or_else(|| QueryError::MissingColumn(column.clone()))?;
        row[index] = sql_expr_to_value(value)?;
    }

    Ok(row)
}

fn sql_expr_to_value(expr: &SqlExpr) -> Result<Value, QueryError> {
    match expr {
        SqlExpr::Value(value) => sql_value_to_value(&value.value),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match sql_expr_to_value(expr)? {
            Value::Int(value) => Ok(Value::Int(-value)),
            Value::Float(value) => Ok(Value::Float(-value)),
            _ => Err(QueryError::Unsupported(expr.to_string())),
        },
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => sql_expr_to_value(expr),
        other => Err(QueryError::Unsupported(other.to_string())),
    }
}

fn sql_value_to_value(value: &SqlValue) -> Result<Value, QueryError> {
    match value {
        SqlValue::Number(raw, _) => {
            if raw.contains('.') || raw.contains('e') || raw.contains('E') {
                raw.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|error| QueryError::InvalidValue(error.to_string()))
            } else {
                raw.parse::<i64>()
                    .map(Value::Int)
                    .map_err(|error| QueryError::InvalidValue(error.to_string()))
            }
        }
        SqlValue::SingleQuotedString(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::TripleDoubleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value) => Ok(Value::Str(value.clone())),
        SqlValue::Boolean(value) => Ok(Value::Bool(*value)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(QueryError::Unsupported(other.to_string())),
    }
}

fn object_name_to_string(name: &ObjectName) -> Result<String, QueryError> {
    let [part] = name.0.as_slice() else {
        return Err(QueryError::Unsupported(name.to_string()));
    };

    match part {
        ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
        ObjectNamePart::Function(_) => Err(QueryError::Unsupported(name.to_string())),
    }
}

/// Collects every relation a `DataFusion` plan scans, including scans inside
/// subqueries. Scans are mapped back to catalog names by provider identity, so
/// a name is reported exactly as it was registered, whatever spelling the SQL
/// used. Scans of unregistered providers are reported as builtin relations.
fn planned_read_targets(
    plan: &LogicalPlan,
    providers: &[(String, Arc<dyn TableProvider>)],
) -> Result<BTreeSet<SqlReadTarget>, QueryError> {
    let mut reads = BTreeSet::new();
    let mut extension = None;
    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::Extension(node) => {
                extension = Some(node.node.name().to_owned());
                return Ok(TreeNodeRecursion::Stop);
            }
            LogicalPlan::TableScan(scan) => {
                let target = match source_as_provider(&scan.source) {
                    // Recursive CTE work tables only re-read the CTE's own
                    // terms, which this walk visits anyway.
                    Ok(provider) if provider.is::<CteWorkTable>() => None,
                    Ok(provider) => Some(
                        providers
                            .iter()
                            .find(|(_, registered)| Arc::ptr_eq(registered, &provider))
                            .map_or_else(
                                || SqlReadTarget::Builtin(scan.table_name.to_string()),
                                |(name, _)| SqlReadTarget::Catalog(name.clone()),
                            ),
                    ),
                    Err(_) => Some(SqlReadTarget::Builtin(scan.table_name.to_string())),
                };
                reads.extend(target);
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|error| datafusion_to_query_error(&error))?;

    if let Some(name) = extension {
        return Err(QueryError::Unsupported(format!(
            "extension plan node {name} in a read query"
        )));
    }
    Ok(reads)
}

/// A catalog relation as one caller's row and masking policies let it be seen.
///
/// The scan runs the policies to completion and serves only the rows they let
/// through, already masked. No predicate of the query, wherever an optimizer
/// rule pushes it, is therefore evaluated on a hidden row or an unmasked value:
/// a predicate that fails on some value cannot reveal that a hidden row holds it.
#[derive(Debug)]
struct PolicyTableProvider {
    /// The policies over the stored relation, or why SQL cannot apply them.
    plan: Result<LogicalPlan, String>,
    schema: SchemaRef,
    table_type: TableType,
    budget: Arc<budget::Budget>,
}

#[async_trait]
impl TableProvider for PolicyTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        self.table_type
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let plan = match &self.plan {
            Ok(plan) => state.create_physical_plan(plan).await?,
            Err(reason) => {
                return Err(query_to_datafusion(QueryError::Unsupported(reason.clone())));
            }
        };
        let mut stream = datafusion::physical_plan::execute_stream(plan, state.task_ctx())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.budget
                .charge_batch(&batch)
                .map_err(query_to_datafusion)?;
            batches.push(batch);
        }
        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        mem.scan(state, projection, &[], limit).await
    }
}

/// Puts `provider` behind `policy`. When the relation is the document
/// `collection`, its columns read the paths its fields name and its row
/// policies are decided on the stored documents; every other relation's column
/// `c` reads the path `c`. A mask SQL cannot represent refuses the queries that
/// read the relation, not every query.
fn policy_table_provider(
    name: &str,
    provider: Arc<dyn TableProvider>,
    collection: Option<&DocCollectionProvider>,
    policy: &SqlRelationPolicy,
    budget: Arc<budget::Budget>,
) -> Result<Arc<dyn TableProvider>, QueryError> {
    // A collection column reads NULL where a document lacks its path or holds
    // a value of another type, so SQL predicates cannot decide its row policies.
    let (provider, fields, row_filters): (Arc<dyn TableProvider>, _, _) = match collection {
        Some(collection) => (
            Arc::new(collection.with_visible_when(&policy.visible_when)),
            Some(collection.fields.as_slice()),
            &[][..],
        ),
        None => (provider, None, policy.visible_when.as_slice()),
    };
    let table_type = provider.table_type();
    let schema = provider.schema();
    let qualifier = TableReference::bare(name);
    let column_paths = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| match fields {
            Some(fields) => match fields.get(index).map(|field| &field.source) {
                Some(DocFieldSource::Path(path)) => Some(path.segments().to_vec()),
                Some(DocFieldSource::DocumentId) | None => None,
            },
            None => Some(vec![field.name().clone()]),
        })
        .collect::<Vec<_>>();
    let column =
        |field: &Field| DfExpr::Column(DfColumn::new(Some(qualifier.clone()), field.name()));

    let mut plan = LogicalPlanBuilder::scan(qualifier.clone(), provider_as_source(provider), None)
        .map_err(|error| datafusion_to_query_error(&error))?;
    for any_of in row_filters {
        let mut predicate = None::<DfExpr>;
        for (path, value) in any_of {
            // Columns hold scalars, so no row equals any other value.
            let Some(scalar) = policy_scalar(value) else {
                continue;
            };
            for (field, column_path) in schema.fields().iter().zip(&column_paths) {
                if column_path.as_deref() != Some(path.segments()) {
                    continue;
                }
                // A value of another type never equals what the column holds.
                let condition = match &scalar {
                    ScalarValue::Null => column(field).is_null(),
                    scalar if &scalar.data_type() == field.data_type() => {
                        column(field).eq(lit(scalar.clone()))
                    }
                    _ => continue,
                };
                predicate = Some(match predicate {
                    Some(predicate) => predicate.or(condition),
                    None => condition,
                });
            }
        }
        plan = plan
            .filter(predicate.unwrap_or_else(|| lit(false)))
            .map_err(|error| datafusion_to_query_error(&error))?;
    }
    if !policy.masks.is_empty() {
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (field, column_path) in schema.fields().iter().zip(&column_paths) {
            let mask = policy.masks.iter().rev().find(|(path, _)| {
                column_path
                    .as_deref()
                    .is_some_and(|column_path| column_path.starts_with(path.segments()))
            });
            columns.push(match mask {
                Some((_, replacement)) => {
                    let scalar = match policy_scalar(replacement) {
                        Some(ScalarValue::Null) => ScalarValue::try_from(field.data_type())
                            .map_err(|error| datafusion_to_query_error(&error))?,
                        Some(scalar) => scalar,
                        None => {
                            return Ok(Arc::new(PolicyTableProvider {
                                plan: Err(format!(
                                    "{name} has a masking policy whose replacement is not a scalar, which SQL cannot return"
                                )),
                                schema: Arc::clone(&schema),
                                table_type,
                                budget,
                            }));
                        }
                    };
                    lit(scalar).alias(field.name())
                }
                None => column(field),
            });
        }
        plan = plan
            .project(columns)
            .map_err(|error| datafusion_to_query_error(&error))?;
    }
    let plan = plan
        .build()
        .map_err(|error| datafusion_to_query_error(&error))?;
    let schema = Arc::clone(plan.schema().inner());
    Ok(Arc::new(PolicyTableProvider {
        plan: Ok(plan),
        schema,
        table_type,
        budget,
    }))
}

/// The SQL literal of a policy value, typed the way a column holding it is, or
/// `None` for a value no SQL column holds.
fn policy_scalar(value: &Value) -> Option<ScalarValue> {
    Some(match value {
        Value::Null => ScalarValue::Null,
        Value::Bool(value) => ScalarValue::Boolean(Some(*value)),
        Value::Int(value) => ScalarValue::Int64(Some(*value)),
        Value::Float(value) => ScalarValue::Float64(Some(*value)),
        Value::Str(value) => ScalarValue::Utf8(Some(value.clone())),
        Value::Bytes(value) => ScalarValue::Binary(Some(value.clone())),
        Value::Array(_) | Value::Object(_) | Value::Vector(_) | Value::GeoPoint { .. } => {
            return None;
        }
    })
}

fn datafusion_to_query_error(error: &DataFusionError) -> QueryError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    // DataFusion parses the SQL again: its sqlparser errors, its own recursion limit of 50
    // included, are client errors like the engine's parse errors.
    if matches!(error.find_root(), DataFusionError::SQL(..)) {
        QueryError::Parse(message)
    } else if lower.contains("query timeout") || lower.contains("query exceeded") {
        QueryError::Timeout(message)
    } else if lower.contains("cancel") {
        QueryError::Cancelled(message)
    } else if lower.contains("memory")
        || lower.contains("resource")
        || lower.contains("spill")
        || lower.contains("out of")
    {
        QueryError::ResourceLimit(message)
    } else {
        QueryError::DataFusion(message)
    }
}

fn query_to_datafusion(error: QueryError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod analyze_guard_tests;
#[cfg(test)]
mod audit_tests;
#[cfg(test)]
mod index_read_tests;
#[cfg(test)]
mod tests;
