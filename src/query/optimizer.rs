use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};

use sqlparser::{
    ast::{ObjectName, Query, SetExpr, Statement, TableFactor, TableObject},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::{
    model::Value,
    repl::{Op, ReadConsistency, Replication, propose_system, propose_system_batch},
    storage::Bytes,
};

use super::{
    BitmapOp, ColumnType, QueryError, RelIndexExpression, RelIndexSpec, RelPredicate, Row,
    SqlOutput, SqlRows, TableLayout, encode_rel_key,
};

pub const STATS_TABLE: &str = "__stats";
pub const PLANNER_FEEDBACK_TABLE: &str = "__planner_feedback";
pub const PLANNER_META_TABLE: &str = "__planner_meta";

const STATS_VERSION_KEY: &[u8] = b"stats_version";
const DEFAULT_SAMPLE_ROWS: usize = 30_000;
const DEFAULT_CACHE_CAPACITY: usize = 1024;
const HISTOGRAM_BUCKETS: usize = 10;
const MCV_LIMIT: usize = 32;
const FEEDBACK_RATIO_THRESHOLD: f64 = 10.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum AnalyzeMode {
    Sample { max_rows: usize },
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyzeTarget {
    All,
    Named(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum StatsObjectKind {
    Table,
    Collection,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Bucket {
    pub lower: Option<Value>,
    pub upper: Option<Value>,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MostCommonValue {
    pub value: Value,
    pub count: u64,
    pub frequency: f64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ColumnStats {
    pub row_count: u64,
    pub sample_count: u64,
    pub ndv: u64,
    pub null_frac: f64,
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub histogram: Vec<Bucket>,
    pub mcv: Vec<MostCommonValue>,
    pub stats_version: u64,
    pub analyzed_at_txn: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableStats {
    pub object_name: String,
    pub object_kind: StatsObjectKind,
    pub row_count: u64,
    pub sample_count: u64,
    pub columns: BTreeMap<String, ColumnStats>,
    pub stats_version: u64,
    pub analyzed_at_txn: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnalyzeReport {
    pub mode: AnalyzeMode,
    pub analyzed: Vec<TableStats>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Cost(pub f64);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostCoefficients {
    pub cpu_tuple: f64,
    pub io_seq: f64,
    pub io_random: f64,
    pub btree_descent: f64,
    pub columnar_tuple: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostProfile {
    InMemory,
    Transactional,
    Analytical,
    Balanced,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostModel {
    pub coefficients: CostCoefficients,
}

#[derive(Clone, Copy)]
pub struct EqPathRequest<'stats, 'table> {
    pub layout: TableLayout,
    pub indexes: &'table [RelIndexSpec],
    pub column: usize,
    pub column_name: &'table str,
    pub value: &'table Value,
    pub stats: Option<&'stats TableStats>,
    pub projected_columns: usize,
}

#[derive(Clone, Copy)]
pub struct FilterPathRequest<'stats, 'table> {
    pub layout: TableLayout,
    pub indexes: &'table [RelIndexSpec],
    pub filter: &'table RelPredicate,
    pub stats: Option<&'stats TableStats>,
    pub projection: &'table [usize],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CardinalityEstimator;

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum AccessPath {
    SeqScan {
        estimated_rows: u64,
        cost: Cost,
    },
    PrimaryKeyRange {
        estimated_rows: u64,
        cost: Cost,
    },
    BTreeIndex {
        index_id: u32,
        estimated_rows: u64,
        cost: Cost,
        covering: bool,
    },
    IndexOnly {
        index_id: u32,
        estimated_rows: u64,
        cost: Cost,
    },
    BitmapIndex {
        op: BitmapOp,
        index_ids: Vec<u32>,
        estimated_rows: u64,
        cost: Cost,
    },
    ColumnarScan {
        estimated_rows: u64,
        cost: Cost,
        projected_columns: usize,
    },
    KnnFirst {
        index_id: u32,
        estimated_rows: u64,
        cost: Cost,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct QueryFingerprint(pub String);

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum PlanDependency {
    Table { name: String, stats_version: u64 },
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CachedPlan {
    pub fingerprint: QueryFingerprint,
    pub dependencies: Vec<PlanDependency>,
    pub stats_version: u64,
    pub access_path: AccessPath,
    pub hits: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanCacheMetrics {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

#[derive(Clone, Debug)]
pub struct PlanCache {
    capacity: usize,
    entries: BTreeMap<QueryFingerprint, CachedPlan>,
    order: VecDeque<QueryFingerprint>,
    hits: u64,
    misses: u64,
    evictions: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ExplainNode {
    pub operator: String,
    pub access_path: AccessPath,
    pub estimated_rows: u64,
    pub actual_rows: Option<u64>,
    pub cost: Cost,
    pub actual_ms: Option<f64>,
    pub details: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ExplainReport {
    pub fingerprint: QueryFingerprint,
    pub analyze: bool,
    pub nodes: Vec<ExplainNode>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExplainOptions {
    pub analyze: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PlannerFeedback {
    pub fingerprint: QueryFingerprint,
    pub operator: String,
    pub estimated_rows: u64,
    pub actual_rows: u64,
    pub ratio: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EqPlan {
    pub access_path: AccessPath,
    pub stats_version: u64,
    pub used_cache: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SimpleSelectPlan {
    pub table: String,
    pub filter_column: usize,
    pub filter_value: Value,
    pub filter: RelPredicate,
    pub projection: Vec<usize>,
    pub projection_names: Vec<String>,
    pub limit: Option<usize>,
}

impl Default for AnalyzeMode {
    fn default() -> Self {
        Self::Sample {
            max_rows: DEFAULT_SAMPLE_ROWS,
        }
    }
}

impl AnalyzeReport {
    #[must_use]
    pub fn to_sql_output(&self) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "object".to_owned(),
                "kind".to_owned(),
                "rows".to_owned(),
                "sample_rows".to_owned(),
                "stats_version".to_owned(),
            ],
            rows: self
                .analyzed
                .iter()
                .map(|stats| {
                    vec![
                        Value::Str(stats.object_name.clone()),
                        Value::Str(match stats.object_kind {
                            StatsObjectKind::Table => "table".to_owned(),
                            StatsObjectKind::Collection => "collection".to_owned(),
                        }),
                        Value::Int(saturating_i64(stats.row_count)),
                        Value::Int(saturating_i64(stats.sample_count)),
                        Value::Int(saturating_i64(stats.stats_version)),
                    ]
                })
                .collect(),
        })
    }
}

impl ExplainReport {
    #[must_use]
    pub fn to_sql_output(&self) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "operator".to_owned(),
                "access_path".to_owned(),
                "est_rows".to_owned(),
                "actual_rows".to_owned(),
                "cost".to_owned(),
                "actual_ms".to_owned(),
                "details".to_owned(),
            ],
            rows: self
                .nodes
                .iter()
                .map(|node| {
                    vec![
                        Value::Str(node.operator.clone()),
                        Value::Str(node.access_path.label()),
                        Value::Int(saturating_i64(node.estimated_rows)),
                        node.actual_rows
                            .map_or(Value::Null, |rows| Value::Int(saturating_i64(rows))),
                        Value::Float(node.cost.0),
                        node.actual_ms.map_or(Value::Null, Value::Float),
                        Value::Str(node.details.clone()),
                    ]
                })
                .collect(),
        })
    }
}

impl CostProfile {
    #[must_use]
    pub const fn coefficients(self) -> CostCoefficients {
        match self {
            Self::InMemory => CostCoefficients {
                cpu_tuple: 1.0,
                io_seq: 0.05,
                io_random: 0.20,
                btree_descent: 2.0,
                columnar_tuple: 0.80,
            },
            Self::Transactional => CostCoefficients {
                cpu_tuple: 1.0,
                io_seq: 0.30,
                io_random: 4.0,
                btree_descent: 8.0,
                columnar_tuple: 0.90,
            },
            Self::Analytical => CostCoefficients {
                cpu_tuple: 1.0,
                io_seq: 0.20,
                io_random: 5.0,
                btree_descent: 8.0,
                columnar_tuple: 0.25,
            },
            Self::Balanced => CostCoefficients {
                cpu_tuple: 1.0,
                io_seq: 0.25,
                io_random: 2.0,
                btree_descent: 6.0,
                columnar_tuple: 0.60,
            },
        }
    }
}

impl CostModel {
    #[must_use]
    pub const fn new(profile: CostProfile) -> Self {
        Self {
            coefficients: profile.coefficients(),
        }
    }

    #[must_use]
    pub fn choose_eq_path(&self, request: EqPathRequest<'_, '_>) -> AccessPath {
        let row_count = request.stats.map_or(1_000, |stats| stats.row_count);
        let selectivity = request
            .stats
            .and_then(|stats| stats.columns.get(request.column_name))
            .map_or(0.005, |stats| {
                CardinalityEstimator::eq_selectivity(stats, request.value)
            });
        let estimated_rows = estimated_rows(row_count, selectivity);

        if request.layout == TableLayout::Columnar {
            return AccessPath::ColumnarScan {
                estimated_rows,
                cost: self.columnar_scan_cost(row_count, request.projected_columns),
                projected_columns: request.projected_columns,
            };
        }

        let seq = AccessPath::SeqScan {
            estimated_rows,
            cost: self.seq_scan_cost(row_count),
        };

        let Some(index) = request
            .indexes
            .iter()
            .find(|index| index.expression == RelIndexExpression::Column(request.column))
        else {
            return seq;
        };

        let index_path = AccessPath::BTreeIndex {
            index_id: index.id,
            estimated_rows,
            cost: self.index_scan_cost(estimated_rows),
            covering: false,
        };

        if index_path.cost() < seq.cost() {
            index_path
        } else {
            seq
        }
    }

    #[must_use]
    pub fn choose_filter_path(&self, request: FilterPathRequest<'_, '_>) -> AccessPath {
        let row_count = request.stats.map_or(1_000, |stats| stats.row_count);
        let selectivity = estimate_filter_selectivity(request.stats, request.filter);
        let estimated_rows = estimated_rows(row_count, selectivity);

        if request.layout == TableLayout::Columnar {
            return AccessPath::ColumnarScan {
                estimated_rows,
                cost: self.columnar_scan_cost(row_count, request.projection.len()),
                projected_columns: request.projection.len(),
            };
        }

        let seq = AccessPath::SeqScan {
            estimated_rows,
            cost: self.seq_scan_cost(row_count),
        };

        let eligible = eligible_indexes(request.indexes, request.filter);
        let bitmap = bitmap_path_for_filter(request.filter, &eligible, estimated_rows, self);
        let best_single = (!matches!(request.filter, RelPredicate::Or(_)))
            .then(|| {
                eligible
                    .iter()
                    .map(|index| {
                        if index.covers_projection(request.projection) {
                            AccessPath::IndexOnly {
                                index_id: index.id,
                                estimated_rows,
                                cost: self.index_only_cost(estimated_rows),
                            }
                        } else {
                            AccessPath::BTreeIndex {
                                index_id: index.id,
                                estimated_rows,
                                cost: self.index_scan_cost(estimated_rows),
                                covering: false,
                            }
                        }
                    })
                    .min_by(|left, right| left.cost().0.total_cmp(&right.cost().0))
            })
            .flatten();

        [Some(seq), best_single, bitmap]
            .into_iter()
            .flatten()
            .min_by(|left, right| left.cost().0.total_cmp(&right.cost().0))
            .unwrap_or(AccessPath::SeqScan {
                estimated_rows,
                cost: self.seq_scan_cost(row_count),
            })
    }

    #[allow(clippy::cast_precision_loss)]
    fn index_only_cost(&self, rows: u64) -> Cost {
        Cost(self.coefficients.btree_descent + rows as f64 * self.coefficients.cpu_tuple)
    }

    #[allow(clippy::cast_precision_loss)]
    fn bitmap_cost(&self, indexes: usize, rows: u64) -> Cost {
        Cost(
            usize_to_f64(indexes) * self.coefficients.btree_descent * 0.5
                + rows as f64 * (self.coefficients.cpu_tuple + self.coefficients.io_seq),
        )
    }

    fn bitmap_path(&self, op: BitmapOp, index_ids: Vec<u32>, rows: u64) -> Option<AccessPath> {
        if index_ids.len() < 2 {
            return None;
        }
        Some(AccessPath::BitmapIndex {
            op,
            cost: self.bitmap_cost(index_ids.len(), rows),
            index_ids,
            estimated_rows: rows,
        })
    }

    #[allow(clippy::cast_precision_loss)]
    fn seq_scan_cost(&self, rows: u64) -> Cost {
        Cost(rows as f64 * (self.coefficients.cpu_tuple + self.coefficients.io_seq))
    }

    #[allow(clippy::cast_precision_loss)]
    fn index_scan_cost(&self, rows: u64) -> Cost {
        Cost(
            self.coefficients.btree_descent
                + rows as f64 * (self.coefficients.cpu_tuple + self.coefficients.io_random),
        )
    }

    #[allow(clippy::cast_precision_loss)]
    fn columnar_scan_cost(&self, rows: u64, projected_columns: usize) -> Cost {
        Cost(
            rows as f64 * self.coefficients.columnar_tuple * usize_to_f64(projected_columns.max(1)),
        )
    }
}

impl CardinalityEstimator {
    #[must_use]
    pub fn eq_selectivity(stats: &ColumnStats, value: &Value) -> f64 {
        if stats.row_count == 0 {
            return 0.0;
        }

        if matches!(value, Value::Null) {
            return stats.null_frac.clamp(0.0, 1.0);
        }

        if let Some(mcv) = stats.mcv.iter().find(|mcv| mcv.value == *value) {
            return mcv.frequency.clamp(0.0, 1.0);
        }

        if stats.ndv == 0 {
            return 0.005;
        }

        ((1.0 - stats.null_frac) / u64_to_f64(stats.ndv)).clamp(0.000_001, 1.0)
    }

    #[must_use]
    pub fn range_selectivity(stats: &ColumnStats, start: &Value, end: &Value) -> f64 {
        if stats.row_count == 0 || stats.histogram.is_empty() {
            return 0.30;
        }

        let Ok(start_key) = encode_rel_key(start) else {
            return 0.30;
        };
        let Ok(end_key) = encode_rel_key(end) else {
            return 0.30;
        };

        let matched = stats
            .histogram
            .iter()
            .filter(|bucket| {
                let Some(lower) = &bucket.lower else {
                    return false;
                };
                let Some(upper) = &bucket.upper else {
                    return false;
                };
                let Ok(lower_key) = encode_rel_key(lower) else {
                    return false;
                };
                let Ok(upper_key) = encode_rel_key(upper) else {
                    return false;
                };
                upper_key >= start_key && lower_key < end_key
            })
            .map(|bucket| bucket.count)
            .sum::<u64>();

        (u64_to_f64(matched) / u64_to_f64(stats.row_count)).clamp(0.000_001, 1.0)
    }
}

fn estimate_filter_selectivity(stats: Option<&TableStats>, filter: &RelPredicate) -> f64 {
    match filter {
        RelPredicate::Eq { expression, value } => stats
            .and_then(|stats| {
                stats
                    .columns
                    .get(&fallback_column_name(expression.column()))
            })
            .map_or(0.005, |stats| {
                CardinalityEstimator::eq_selectivity(stats, value)
            }),
        RelPredicate::And(predicates) => predicates
            .iter()
            .map(|predicate| estimate_filter_selectivity(stats, predicate))
            .product::<f64>()
            .clamp(0.000_001, 1.0),
        RelPredicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| estimate_filter_selectivity(stats, predicate))
            .sum::<f64>()
            .clamp(0.000_001, 1.0),
    }
}

fn fallback_column_name(column: usize) -> String {
    format!("col{column}")
}

fn eligible_indexes<'a>(
    indexes: &'a [RelIndexSpec],
    filter: &RelPredicate,
) -> Vec<&'a RelIndexSpec> {
    indexes
        .iter()
        .filter(|index| {
            filter_has_eq_for_expression(filter, index.expression)
                && index
                    .predicate
                    .as_ref()
                    .is_none_or(|predicate| predicate_implies(filter, predicate))
        })
        .collect()
}

fn bitmap_path_for_filter(
    filter: &RelPredicate,
    eligible: &[&RelIndexSpec],
    estimated_rows: u64,
    cost_model: &CostModel,
) -> Option<AccessPath> {
    match filter {
        RelPredicate::And(predicates) => {
            let index_ids = predicates
                .iter()
                .filter_map(|predicate| {
                    eligible
                        .iter()
                        .find(|index| {
                            filter_eq_for_expression(predicate, index.expression).is_some()
                        })
                        .map(|index| index.id)
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            cost_model.bitmap_path(BitmapOp::And, index_ids, estimated_rows)
        }
        RelPredicate::Or(predicates) => {
            let index_ids = predicates
                .iter()
                .filter_map(|predicate| {
                    eligible
                        .iter()
                        .find(|index| {
                            filter_eq_for_expression(predicate, index.expression).is_some()
                        })
                        .map(|index| index.id)
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            cost_model.bitmap_path(BitmapOp::Or, index_ids, estimated_rows)
        }
        RelPredicate::Eq { .. } => None,
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

fn filter_has_eq_for_expression(filter: &RelPredicate, expression: RelIndexExpression) -> bool {
    match filter {
        RelPredicate::Eq {
            expression: candidate,
            ..
        } => *candidate == expression,
        RelPredicate::And(predicates) | RelPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| filter_has_eq_for_expression(predicate, expression)),
    }
}

fn predicate_implies(query: &RelPredicate, required: &RelPredicate) -> bool {
    if query == required {
        return true;
    }

    match (query, required) {
        (_, RelPredicate::And(required_parts)) => required_parts
            .iter()
            .all(|part| predicate_implies(query, part)),
        (_, RelPredicate::Or(required_parts)) => required_parts
            .iter()
            .any(|part| predicate_implies(query, part)),
        (RelPredicate::And(query_parts), _) => query_parts
            .iter()
            .any(|part| predicate_implies(part, required)),
        (RelPredicate::Or(query_parts), _) => query_parts
            .iter()
            .all(|part| predicate_implies(part, required)),
        (RelPredicate::Eq { .. }, RelPredicate::Eq { .. }) => false,
    }
}

impl AccessPath {
    #[must_use]
    pub const fn cost(&self) -> Cost {
        match self {
            Self::SeqScan { cost, .. }
            | Self::PrimaryKeyRange { cost, .. }
            | Self::BTreeIndex { cost, .. }
            | Self::IndexOnly { cost, .. }
            | Self::BitmapIndex { cost, .. }
            | Self::ColumnarScan { cost, .. }
            | Self::KnnFirst { cost, .. } => *cost,
        }
    }

    #[must_use]
    pub const fn estimated_rows(&self) -> u64 {
        match self {
            Self::SeqScan { estimated_rows, .. }
            | Self::PrimaryKeyRange { estimated_rows, .. }
            | Self::BTreeIndex { estimated_rows, .. }
            | Self::IndexOnly { estimated_rows, .. }
            | Self::BitmapIndex { estimated_rows, .. }
            | Self::ColumnarScan { estimated_rows, .. }
            | Self::KnnFirst { estimated_rows, .. } => *estimated_rows,
        }
    }

    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::SeqScan { .. } => "seq_scan".to_owned(),
            Self::PrimaryKeyRange { .. } => "primary_key_range".to_owned(),
            Self::BTreeIndex { index_id, .. } => format!("btree_index:{index_id}"),
            Self::IndexOnly { index_id, .. } => format!("index_only:{index_id}"),
            Self::BitmapIndex { op, index_ids, .. } => {
                format!(
                    "bitmap_{op:?}:{}",
                    index_ids
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Self::ColumnarScan { .. } => "columnar_scan".to_owned(),
            Self::KnnFirst { index_id, .. } => format!("knn_first:{index_id}"),
        }
    }
}

impl QueryFingerprint {
    #[must_use]
    pub fn new(sql: &str) -> Self {
        let normalized = normalize_sql(sql);
        let context = fingerprint_context(sql);
        Self(context.map_or(normalized.clone(), |context| {
            format!("{context}|sql={normalized}")
        }))
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_CAPACITY)
    }
}

impl PlanCache {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    pub fn get(
        &mut self,
        fingerprint: &QueryFingerprint,
        stats_version: u64,
    ) -> Option<CachedPlan> {
        let Some(entry) = self.entries.get_mut(fingerprint) else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };

        if entry.stats_version != stats_version {
            self.entries.remove(fingerprint);
            self.order.retain(|item| item != fingerprint);
            self.misses = self.misses.saturating_add(1);
            return None;
        }

        entry.hits = entry.hits.saturating_add(1);
        self.hits = self.hits.saturating_add(1);
        let cached = entry.clone();
        self.touch(fingerprint);
        Some(cached)
    }

    pub fn insert(&mut self, plan: CachedPlan) {
        if self.entries.contains_key(&plan.fingerprint) {
            self.entries.insert(plan.fingerprint.clone(), plan.clone());
            self.touch(&plan.fingerprint);
            return;
        }

        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                if self.entries.remove(&oldest).is_some() {
                    self.evictions = self.evictions.saturating_add(1);
                }
            } else {
                break;
            }
        }

        self.order.push_back(plan.fingerprint.clone());
        self.entries.insert(plan.fingerprint.clone(), plan);
    }

    pub fn invalidate_all(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    #[must_use]
    pub fn metrics(&self) -> PlanCacheMetrics {
        PlanCacheMetrics {
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }

    fn touch(&mut self, fingerprint: &QueryFingerprint) {
        self.order.retain(|item| item != fingerprint);
        self.order.push_back(fingerprint.clone());
    }
}

pub struct StatsCatalog;

impl StatsCatalog {
    /// Reads persisted optimizer statistics for one catalog object.
    /// # Errors
    /// Fails when replication or JSON decoding fails.
    pub fn read_table(
        repl: &dyn Replication,
        object_name: &str,
    ) -> Result<Option<TableStats>, QueryError> {
        let Some(bytes) = repl.read(
            STATS_TABLE,
            &stats_key(object_name),
            ReadConsistency::Strong,
        )?
        else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map_err(|error| QueryError::Serde(error.to_string()))
    }

    /// Persists optimizer statistics and advances the global stats version.
    /// # Errors
    /// Fails when serialization or replication fails.
    pub fn write_table(repl: &dyn Replication, stats: &TableStats) -> Result<(), QueryError> {
        let value =
            serde_json::to_vec(stats).map_err(|error| QueryError::Serde(error.to_string()))?;
        propose_system_batch(
            repl,
            vec![
                Op::Put {
                    table: STATS_TABLE.to_owned(),
                    key: stats_key(&stats.object_name),
                    value,
                },
                Op::Put {
                    table: PLANNER_META_TABLE.to_owned(),
                    key: STATS_VERSION_KEY.to_vec(),
                    value: stats.stats_version.to_be_bytes().to_vec(),
                },
            ],
        )?;
        Ok(())
    }

    /// Returns the next monotonic stats version.
    /// # Errors
    /// Fails when metadata cannot be read or the version overflows.
    pub fn next_stats_version(repl: &dyn Replication) -> Result<u64, QueryError> {
        let current = repl
            .read(
                PLANNER_META_TABLE,
                STATS_VERSION_KEY,
                ReadConsistency::Strong,
            )?
            .and_then(|bytes| bytes.as_slice().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0);
        current
            .checked_add(1)
            .ok_or_else(|| QueryError::InvalidValue("stats version overflow".to_owned()))
    }

    /// Reads recorded planner feedback rows.
    /// # Errors
    /// Fails when replication or JSON decoding fails.
    pub fn read_feedback(repl: &dyn Replication) -> Result<Vec<PlannerFeedback>, QueryError> {
        repl.range(
            PLANNER_FEEDBACK_TABLE,
            &[],
            &[0xFF],
            ReadConsistency::Strong,
        )?
        .into_iter()
        .map(|(_, value)| {
            serde_json::from_slice(&value).map_err(|error| QueryError::Serde(error.to_string()))
        })
        .collect()
    }

    /// Persists one estimated-vs-actual feedback row.
    /// # Errors
    /// Fails when serialization or replication fails.
    pub fn record_feedback(
        repl: &dyn Replication,
        feedback: &PlannerFeedback,
    ) -> Result<(), QueryError> {
        let value =
            serde_json::to_vec(feedback).map_err(|error| QueryError::Serde(error.to_string()))?;
        propose_system(
            repl,
            Op::Put {
                table: PLANNER_FEEDBACK_TABLE.to_owned(),
                key: feedback_key(feedback),
                value,
            },
        )?;
        Ok(())
    }
}

#[must_use]
pub fn build_table_stats(
    object_name: &str,
    object_kind: StatsObjectKind,
    columns: &[(String, ColumnType)],
    rows: &[Row],
    mode: AnalyzeMode,
    stats_version: u64,
    analyzed_at_txn: u64,
) -> TableStats {
    let sample = sample_rows(rows, mode);
    let row_count = usize_to_u64(rows.len());
    let sample_count = usize_to_u64(sample.len());
    let columns = columns
        .iter()
        .enumerate()
        .map(|(index, (name, _))| {
            (
                name.clone(),
                build_column_stats(rows, &sample, index, stats_version, analyzed_at_txn),
            )
        })
        .collect();

    TableStats {
        object_name: object_name.to_owned(),
        object_kind,
        row_count,
        sample_count,
        columns,
        stats_version,
        analyzed_at_txn,
    }
}

pub fn explain_node_for_eq(
    table: &str,
    column: &str,
    path: &AccessPath,
    actual_rows: Option<u64>,
    actual_ms: Option<f64>,
    used_cache: bool,
) -> ExplainNode {
    let cache = if used_cache { "cache_hit" } else { "planned" };
    ExplainNode {
        operator: "filter_eq".to_owned(),
        access_path: path.clone(),
        estimated_rows: path.estimated_rows(),
        actual_rows,
        cost: path.cost(),
        actual_ms,
        details: format!("{table}.{column}; {cache}"),
    }
}

pub fn maybe_feedback(report: &ExplainReport) -> Option<PlannerFeedback> {
    report.nodes.iter().find_map(|node| {
        let actual = node.actual_rows?;
        let estimated = node.estimated_rows.max(1);
        let ratio = feedback_ratio(estimated, actual);
        if ratio >= FEEDBACK_RATIO_THRESHOLD {
            Some(PlannerFeedback {
                fingerprint: report.fingerprint.clone(),
                operator: node.operator.clone(),
                estimated_rows: node.estimated_rows,
                actual_rows: actual,
                ratio,
            })
        } else {
            None
        }
    })
}

#[must_use]
pub fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn build_column_stats(
    rows: &[Row],
    sample: &[&Row],
    column: usize,
    stats_version: u64,
    analyzed_at_txn: u64,
) -> ColumnStats {
    let row_count = usize_to_u64(rows.len());
    let sample_count = usize_to_u64(sample.len());
    let mut nulls = 0_u64;
    let mut frequencies: BTreeMap<Bytes, (Value, u64)> = BTreeMap::new();

    for row in sample {
        let value = row.get(column).cloned().unwrap_or(Value::Null);
        if matches!(value, Value::Null) {
            nulls = nulls.saturating_add(1);
            continue;
        }
        if let Ok(key) = encode_rel_key(&value) {
            let entry = frequencies.entry(key).or_insert((value, 0));
            entry.1 = entry.1.saturating_add(1);
        }
    }

    let ndv = estimate_ndv(&frequencies, sample.len(), rows.len());
    let mut ordered = frequencies.into_iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.0.cmp(&right.0));

    let min = ordered.first().map(|(_, (value, _))| value.clone());
    let max = ordered.last().map(|(_, (value, _))| value.clone());
    let histogram = build_histogram(&ordered);
    let mcv = build_mcv(ordered, sample_count);

    ColumnStats {
        row_count,
        sample_count,
        ndv,
        null_frac: if sample_count == 0 {
            0.0
        } else {
            u64_to_f64(nulls) / u64_to_f64(sample_count)
        },
        min,
        max,
        histogram,
        mcv,
        stats_version,
        analyzed_at_txn,
    }
}

fn sample_rows(rows: &[Row], mode: AnalyzeMode) -> Vec<&Row> {
    match mode {
        AnalyzeMode::Full => rows.iter().collect(),
        AnalyzeMode::Sample { max_rows } if rows.len() <= max_rows => rows.iter().collect(),
        AnalyzeMode::Sample { max_rows } => {
            let limit = max_rows.max(1);
            let mut sample = Vec::with_capacity(limit);
            for (index, row) in rows.iter().enumerate() {
                if sample.len() < limit {
                    sample.push(row);
                } else {
                    let slot = deterministic_slot(index, index + 1);
                    if slot < limit {
                        sample[slot] = row;
                    }
                }
            }
            sample
        }
    }
}

fn deterministic_slot(index: usize, limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&usize_to_u64(index).to_be_bytes());
    let hash = blake3::hash(&bytes);
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&hash.as_bytes()[..8]);
    let value = u64::from_be_bytes(raw);
    usize::try_from(value % usize_to_u64(limit)).unwrap_or(0)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn estimate_ndv(
    frequencies: &BTreeMap<Bytes, (Value, u64)>,
    sample_len: usize,
    total_len: usize,
) -> u64 {
    if sample_len == 0 {
        return 0;
    }
    let unique_sample = frequencies.len();
    if sample_len == total_len {
        return usize_to_u64(unique_sample);
    }

    let singletons = frequencies
        .values()
        .filter(|(_, count)| *count == 1)
        .count() as f64;
    let doubletons = frequencies
        .values()
        .filter(|(_, count)| *count == 2)
        .count() as f64;
    let observed = unique_sample as f64;
    let unseen = if doubletons > 0.0 {
        (singletons * singletons) / (2.0 * doubletons)
    } else {
        (singletons * (singletons - 1.0)) / 2.0
    };
    (observed + unseen).ceil().min(total_len as f64) as u64
}

fn build_histogram(ordered: &[(Bytes, (Value, u64))]) -> Vec<Bucket> {
    let total = ordered.iter().map(|(_, (_, count))| *count).sum::<u64>();
    if total == 0 {
        return Vec::new();
    }

    let target = total.div_ceil(usize_to_u64(HISTOGRAM_BUCKETS)).max(1);
    let mut buckets = Vec::new();
    let mut lower = None;
    let mut upper = None;
    let mut count = 0_u64;

    for (_, (value, value_count)) in ordered {
        if lower.is_none() {
            lower = Some(value.clone());
        }
        upper = Some(value.clone());
        count = count.saturating_add(*value_count);
        if count >= target && buckets.len() + 1 < HISTOGRAM_BUCKETS {
            buckets.push(Bucket {
                lower: lower.take(),
                upper: upper.take(),
                count,
            });
            count = 0;
        }
    }

    if count > 0 || buckets.is_empty() {
        buckets.push(Bucket {
            lower,
            upper,
            count,
        });
    }

    buckets
}

fn build_mcv(ordered: Vec<(Bytes, (Value, u64))>, sample_count: u64) -> Vec<MostCommonValue> {
    let mut counts = ordered
        .into_iter()
        .map(|(_, (value, count))| (value, count))
        .collect::<Vec<_>>();
    counts.sort_by_key(|item| Reverse(item.1));
    counts
        .into_iter()
        .take(MCV_LIMIT)
        .map(|(value, count)| MostCommonValue {
            value,
            count,
            frequency: if sample_count == 0 {
                0.0
            } else {
                u64_to_f64(count) / u64_to_f64(sample_count)
            },
        })
        .collect()
}

fn normalize_sql(sql: &str) -> String {
    let mut output = String::new();
    let mut chars = sql.chars().peekable();
    let mut last_was_space = false;

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            output.push('?');
            while let Some(next) = chars.next() {
                if next == '\'' {
                    if chars.peek() == Some(&'\'') {
                        let _ = chars.next();
                    } else {
                        break;
                    }
                }
            }
            last_was_space = false;
            continue;
        }

        if ch.is_ascii_digit()
            && output
                .chars()
                .last()
                .is_none_or(|previous| !is_identifier_char(previous))
        {
            output.push('?');
            while chars
                .peek()
                .is_some_and(|next| next.is_ascii_digit() || matches!(next, '.' | '_'))
            {
                let _ = chars.next();
            }
            last_was_space = false;
            continue;
        }

        if ch.is_whitespace() {
            if !last_was_space && !output.is_empty() {
                output.push(' ');
            }
            last_was_space = true;
            continue;
        }

        output.push(ch.to_ascii_lowercase());
        last_was_space = false;
    }

    output.trim().trim_end_matches(';').to_owned()
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$')
}

fn fingerprint_context(sql: &str) -> Option<String> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let mut parts = Vec::new();
    for statement in statements {
        let mut tables = BTreeSet::new();
        let kind = match &statement {
            Statement::Query(query) => {
                collect_fingerprint_query_tables(query, &mut tables);
                "query"
            }
            Statement::Insert(insert) => {
                if let TableObject::TableName(name) = &insert.table {
                    tables.insert(fingerprint_object_name(name));
                }
                if let Some(source) = &insert.source {
                    collect_fingerprint_query_tables(source, &mut tables);
                }
                "insert"
            }
            Statement::Analyze(analyze) => {
                if let Some(name) = &analyze.table_name {
                    tables.insert(fingerprint_object_name(name));
                }
                "analyze"
            }
            Statement::Explain { statement, .. } => {
                if let Statement::Query(query) = statement.as_ref() {
                    collect_fingerprint_query_tables(query, &mut tables);
                }
                "explain"
            }
            _ => "other",
        };
        parts.push(format!(
            "kind={kind};tables={}",
            tables.into_iter().collect::<Vec<_>>().join(",")
        ));
    }
    Some(parts.join("|"))
}

fn collect_fingerprint_query_tables(query: &Query, tables: &mut BTreeSet<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_fingerprint_query_tables(&cte.query, tables);
        }
    }
    collect_fingerprint_set_expr_tables(query.body.as_ref(), tables);
}

fn collect_fingerprint_set_expr_tables(expr: &SetExpr, tables: &mut BTreeSet<String>) {
    match expr {
        SetExpr::Select(select) => {
            for relation in &select.from {
                collect_fingerprint_table_factor(&relation.relation, tables);
                for join in &relation.joins {
                    collect_fingerprint_table_factor(&join.relation, tables);
                }
            }
        }
        SetExpr::Query(query) => collect_fingerprint_query_tables(query, tables),
        SetExpr::SetOperation { left, right, .. } => {
            collect_fingerprint_set_expr_tables(left, tables);
            collect_fingerprint_set_expr_tables(right, tables);
        }
        _ => {}
    }
}

fn collect_fingerprint_table_factor(factor: &TableFactor, tables: &mut BTreeSet<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            tables.insert(fingerprint_object_name(name));
        }
        TableFactor::Derived { subquery, .. } => {
            collect_fingerprint_query_tables(subquery, tables);
        }
        _ => {}
    }
}

fn fingerprint_object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.to_string().trim_matches('"').to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn stats_key(object_name: &str) -> Bytes {
    object_name.as_bytes().to_vec()
}

fn feedback_key(feedback: &PlannerFeedback) -> Bytes {
    let mut key = feedback.fingerprint.0.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&feedback.actual_rows.to_be_bytes());
    key
}

#[allow(clippy::cast_precision_loss)]
fn feedback_ratio(estimated: u64, actual: u64) -> f64 {
    let estimated = estimated.max(1) as f64;
    let actual = actual.max(1) as f64;
    if actual >= estimated {
        actual / estimated
    } else {
        estimated / actual
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn estimated_rows(row_count: u64, selectivity: f64) -> u64 {
    if row_count == 0 {
        return 0;
    }

    let selected = (u64_to_f64(row_count) * selectivity).ceil();
    let selected = selected.clamp(1.0, u64_to_f64(row_count));
    selected as u64
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::{AnalyzeMode, sample_rows};
    use crate::model::Value;

    #[test]
    fn reservoir_sample_is_not_tail_biased() {
        let rows = (0..1_000)
            .map(|id| vec![Value::Int(id)])
            .collect::<Vec<_>>();
        let sample = sample_rows(&rows, AnalyzeMode::Sample { max_rows: 16 });
        let ids = sample
            .iter()
            .filter_map(|row| match row.first() {
                Some(Value::Int(id)) => Some(*id),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(ids.len(), 16);
        assert!(ids.iter().any(|id| *id < 500));
    }
}
