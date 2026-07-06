#![allow(
    clippy::cast_precision_loss,
    clippy::missing_errors_doc,
    clippy::too_many_lines
)]

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sqlparser::{
    ast::{BinaryOperator, Expr as SqlExpr, ObjectName, Query, SetExpr, Statement, TableFactor},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::{
    model::Value,
    observability,
    performance::PerformanceConfig,
    query::{QueryError, QueryFingerprint, RelIndexSpec, SqlOutput, SqlRows, TableLayout},
    repl::{Op, ReadConsistency, ReplError, Replication, propose_system, propose_system_batch},
    storage::Bytes,
};

pub const WORKLOAD_TABLE: &str = "__workload";
pub const TUNING_POLICY_TABLE: &str = "__tuning_policy";
pub const TUNING_ADVICE_TABLE: &str = "__tuning_advice";
pub const TUNING_LOG_TABLE: &str = "__tuning_log";
pub const REPROFILE_JOBS_TABLE: &str = "__reprofile_jobs";

const POLICY_KEY: &[u8] = b"default";
const DEFAULT_HEAVY_SCAN_RATIO: f64 = 10.0;
const DEFAULT_HEAVY_SCAN_ROWS: u64 = 100;

#[derive(thiserror::Error, Debug)]
pub enum TuningError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("query: {0}")]
    Query(#[from] QueryError),

    #[error("serialization: {0}")]
    Serde(String),

    #[error("invalid tuning policy: {0}")]
    InvalidPolicy(String),

    #[error("tuning decision outside envelope: {0}")]
    OutsideEnvelope(String),

    #[error("missing tuning decision: {0}")]
    MissingDecision(String),

    #[error("unsupported reprofile plan: {0}")]
    UnsupportedReprofile(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum WorkloadWindow {
    LastMinute,
    LastHour,
    LastDay,
    #[default]
    All,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum AccessPattern {
    PointLookup { table: String, column: String },
    RangeScan { table: String, column: String },
    FullScan { table: String },
    Unknown,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WorkloadSample {
    pub sql: String,
    pub latency_ms: f64,
    pub returned_rows: u64,
    pub examined_rows: u64,
    pub resource: Option<String>,
    pub filter_column: Option<String>,
    pub used_index: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WorkloadEntry {
    pub fingerprint: QueryFingerprint,
    pub canonical_sql: String,
    pub executions: u64,
    pub total_latency_ms: f64,
    pub max_latency_ms: f64,
    pub returned_rows: u64,
    pub examined_rows: u64,
    pub access_pattern: AccessPattern,
    pub used_indexes: BTreeSet<u32>,
    pub first_seen_millis: u64,
    pub last_seen_millis: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WorkloadReport {
    pub window: WorkloadWindow,
    pub generated_at_millis: u64,
    pub entries: Vec<WorkloadEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct IndexCandidate {
    pub table: String,
    pub column: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WhatIfIndex {
    pub candidate: IndexCandidate,
    pub estimated_scan_cost: f64,
    pub estimated_index_cost: f64,
    pub estimated_benefit: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RecommendationStatus {
    Proposed,
    Accepted,
    Rejected,
    Applied,
    Superseded,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum IndexRecommendation {
    Create {
        id: String,
        candidate: IndexCandidate,
        what_if: WhatIfIndex,
        reason: String,
        status: RecommendationStatus,
    },
    DropBallast {
        id: String,
        table: String,
        index: RelIndexSpec,
        reason: String,
        status: RecommendationStatus,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IndexAdviceReport {
    pub generated_at_millis: u64,
    pub recommendations: Vec<IndexRecommendation>,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum TunableParameter {
    CacheMaxCapacity,
    GroupCommitWindowMs,
    TargetPartitions,
    ScanBypassThresholdRows,
    VectorEfSearch,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ParameterEnvelope {
    pub min: u64,
    pub max: u64,
    pub step: u64,
    pub cooldown_secs: u64,
    pub max_changes_per_hour: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TuningPolicy {
    pub enabled: bool,
    pub recommend_only: bool,
    pub envelopes: BTreeMap<TunableParameter, ParameterEnvelope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TuningDecisionStatus {
    Recommended,
    Applied,
    Rejected,
    RolledBack,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TuningDecision {
    pub id: String,
    pub parameter: TunableParameter,
    pub old_value: u64,
    pub new_value: u64,
    pub reason: String,
    pub status: TuningDecisionStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TuningLogEntry {
    pub id: String,
    pub at_millis: u64,
    pub decision: TuningDecision,
    pub outcome: String,
    pub rollback_of: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReprofilePlan {
    pub object: String,
    pub from_layout: TableLayout,
    pub to_layout: TableLayout,
    pub reversible: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ReprofileStatus {
    Recommended,
    PlanningOnly,
    Copying,
    ReadyToSwitch,
    Switched,
    RolledBack,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReprofileJob {
    pub id: String,
    pub plan: ReprofilePlan,
    pub status: ReprofileStatus,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub detail: String,
}

pub struct WorkloadProfiler;
pub struct IndexAdvisor;

impl Default for WorkloadSample {
    fn default() -> Self {
        Self {
            sql: String::new(),
            latency_ms: 0.0,
            returned_rows: 0,
            examined_rows: 0,
            resource: None,
            filter_column: None,
            used_index: None,
        }
    }
}

impl WorkloadSample {
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_observed_rows(mut self, returned_rows: u64, examined_rows: u64) -> Self {
        self.returned_rows = returned_rows;
        self.examined_rows = examined_rows;
        self
    }

    #[must_use]
    pub fn with_latency_ms(mut self, latency_ms: f64) -> Self {
        self.latency_ms = latency_ms;
        self
    }

    #[must_use]
    pub fn with_access(mut self, resource: impl Into<String>, column: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self.filter_column = Some(column.into());
        self
    }

    #[must_use]
    pub const fn with_used_index(mut self, index: u32) -> Self {
        self.used_index = Some(index);
        self
    }
}

impl WorkloadWindow {
    #[must_use]
    pub const fn duration(self) -> Option<Duration> {
        match self {
            Self::LastMinute => Some(Duration::from_secs(60)),
            Self::LastHour => Some(Duration::from_secs(60 * 60)),
            Self::LastDay => Some(Duration::from_secs(60 * 60 * 24)),
            Self::All => None,
        }
    }
}

impl WorkloadEntry {
    #[must_use]
    pub fn average_latency_ms(&self) -> f64 {
        if self.executions == 0 {
            return 0.0;
        }
        self.total_latency_ms / self.executions as f64
    }

    #[must_use]
    pub fn selectivity(&self) -> f64 {
        if self.examined_rows == 0 {
            return 1.0;
        }
        self.returned_rows as f64 / self.examined_rows as f64
    }

    #[must_use]
    pub fn has_index_usage(&self) -> bool {
        !self.used_indexes.is_empty()
    }
}

impl WorkloadReport {
    #[must_use]
    pub fn to_sql_output(&self) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "fingerprint".to_owned(),
                "executions".to_owned(),
                "avg_latency_ms".to_owned(),
                "returned_rows".to_owned(),
                "examined_rows".to_owned(),
                "access_pattern".to_owned(),
                "last_seen_millis".to_owned(),
            ],
            rows: self
                .entries
                .iter()
                .map(|entry| {
                    vec![
                        Value::Str(entry.fingerprint.0.clone()),
                        Value::Int(u64_to_i64(entry.executions)),
                        Value::Float(entry.average_latency_ms()),
                        Value::Int(u64_to_i64(entry.returned_rows)),
                        Value::Int(u64_to_i64(entry.examined_rows)),
                        Value::Str(access_pattern_name(&entry.access_pattern).to_owned()),
                        Value::Int(u64_to_i64(entry.last_seen_millis)),
                    ]
                })
                .collect(),
        })
    }
}

impl IndexRecommendation {
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Create { id, .. } | Self::DropBallast { id, .. } => id,
        }
    }

    #[must_use]
    pub fn to_row(&self) -> Vec<Value> {
        match self {
            Self::Create {
                id,
                candidate,
                what_if,
                reason,
                status,
            } => vec![
                Value::Str(id.clone()),
                Value::Str("create_index".to_owned()),
                Value::Str(candidate.table.clone()),
                Value::Str(candidate.column.clone()),
                Value::Float(what_if.estimated_benefit),
                Value::Str(format!("{status:?}")),
                Value::Str(reason.clone()),
            ],
            Self::DropBallast {
                id,
                table,
                index,
                reason,
                status,
            } => vec![
                Value::Str(id.clone()),
                Value::Str("drop_ballast".to_owned()),
                Value::Str(table.clone()),
                Value::Str(index.column.to_string()),
                Value::Float(0.0),
                Value::Str(format!("{status:?}")),
                Value::Str(reason.clone()),
            ],
        }
    }
}

impl IndexAdviceReport {
    #[must_use]
    pub fn to_sql_output(&self) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "id".to_owned(),
                "action".to_owned(),
                "table_name".to_owned(),
                "column_name".to_owned(),
                "estimated_benefit".to_owned(),
                "status".to_owned(),
                "reason".to_owned(),
            ],
            rows: self
                .recommendations
                .iter()
                .map(IndexRecommendation::to_row)
                .collect(),
        })
    }
}

impl Default for TuningPolicy {
    fn default() -> Self {
        let mut envelopes = BTreeMap::new();
        envelopes.insert(
            TunableParameter::CacheMaxCapacity,
            ParameterEnvelope::new(64, 65_536, 64),
        );
        envelopes.insert(
            TunableParameter::GroupCommitWindowMs,
            ParameterEnvelope::new(0, 250, 1),
        );
        envelopes.insert(
            TunableParameter::TargetPartitions,
            ParameterEnvelope::new(1, 256, 1),
        );
        envelopes.insert(
            TunableParameter::ScanBypassThresholdRows,
            ParameterEnvelope::new(1, 1_000_000, 1),
        );
        envelopes.insert(
            TunableParameter::VectorEfSearch,
            ParameterEnvelope::new(8, 1_024, 1),
        );
        Self {
            enabled: false,
            recommend_only: true,
            envelopes,
        }
    }
}

impl ParameterEnvelope {
    #[must_use]
    pub const fn new(min: u64, max: u64, step: u64) -> Self {
        Self {
            min,
            max,
            step,
            cooldown_secs: 60,
            max_changes_per_hour: 1,
        }
    }

    #[must_use]
    pub fn contains(&self, value: u64) -> bool {
        if value < self.min || value > self.max {
            return false;
        }
        self.step != 0 && (value - self.min).is_multiple_of(self.step)
    }
}

impl TuningPolicy {
    pub fn validate(&self) -> Result<(), TuningError> {
        for (parameter, envelope) in &self.envelopes {
            if envelope.min > envelope.max {
                return Err(TuningError::InvalidPolicy(format!(
                    "{parameter:?} has min greater than max"
                )));
            }
            if envelope.step == 0 {
                return Err(TuningError::InvalidPolicy(format!(
                    "{parameter:?} has zero step"
                )));
            }
        }
        Ok(())
    }

    pub fn validate_decision(&self, decision: &TuningDecision) -> Result<(), TuningError> {
        let envelope = self.envelopes.get(&decision.parameter).ok_or_else(|| {
            TuningError::OutsideEnvelope(format!("no envelope for {:?}", decision.parameter))
        })?;
        if !envelope.contains(decision.new_value) {
            return Err(TuningError::OutsideEnvelope(format!(
                "{:?} value {} outside {}..{} step {}",
                decision.parameter, decision.new_value, envelope.min, envelope.max, envelope.step
            )));
        }
        Ok(())
    }

    pub fn validate_decision_timing(
        &self,
        decision: &TuningDecision,
        history: &[TuningLogEntry],
        at_millis: u64,
    ) -> Result<(), TuningError> {
        let envelope = self.envelopes.get(&decision.parameter).ok_or_else(|| {
            TuningError::OutsideEnvelope(format!("no envelope for {:?}", decision.parameter))
        })?;
        let applied = history
            .iter()
            .filter(|entry| {
                entry.decision.parameter == decision.parameter
                    && entry.decision.status == TuningDecisionStatus::Applied
                    && entry.rollback_of.is_none()
            })
            .collect::<Vec<_>>();
        if let Some(last) = applied.iter().map(|entry| entry.at_millis).max() {
            let cooldown_millis = envelope.cooldown_secs.saturating_mul(1_000);
            if at_millis < last.saturating_add(cooldown_millis) {
                return Err(TuningError::OutsideEnvelope(format!(
                    "{:?} is inside cooldown window",
                    decision.parameter
                )));
            }
        }
        let window_start = at_millis.saturating_sub(60 * 60 * 1_000);
        let recent_changes = applied
            .iter()
            .filter(|entry| entry.at_millis >= window_start)
            .count();
        if recent_changes >= usize::try_from(envelope.max_changes_per_hour).unwrap_or(usize::MAX) {
            return Err(TuningError::OutsideEnvelope(format!(
                "{:?} exceeded max_changes_per_hour {}",
                decision.parameter, envelope.max_changes_per_hour
            )));
        }
        Ok(())
    }
}

impl TuningDecision {
    #[must_use]
    pub fn new(
        parameter: TunableParameter,
        old_value: u64,
        new_value: u64,
        reason: impl Into<String>,
    ) -> Self {
        let id = format!(
            "{}-{:?}-{}-{}",
            now_millis(),
            parameter,
            old_value,
            new_value
        )
        .to_ascii_lowercase();
        Self {
            id,
            parameter,
            old_value,
            new_value,
            reason: reason.into(),
            status: TuningDecisionStatus::Recommended,
        }
    }
}

impl TuningLogEntry {
    #[must_use]
    pub fn to_sql_output(entries: &[Self]) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "id".to_owned(),
                "at_millis".to_owned(),
                "parameter".to_owned(),
                "old_value".to_owned(),
                "new_value".to_owned(),
                "status".to_owned(),
                "outcome".to_owned(),
                "rollback_of".to_owned(),
            ],
            rows: entries
                .iter()
                .map(|entry| {
                    vec![
                        Value::Str(entry.id.clone()),
                        Value::Int(u64_to_i64(entry.at_millis)),
                        Value::Str(format!("{:?}", entry.decision.parameter)),
                        Value::Int(u64_to_i64(entry.decision.old_value)),
                        Value::Int(u64_to_i64(entry.decision.new_value)),
                        Value::Str(format!("{:?}", entry.decision.status)),
                        Value::Str(entry.outcome.clone()),
                        entry.rollback_of.clone().map_or(Value::Null, Value::Str),
                    ]
                })
                .collect(),
        })
    }
}

impl ReprofileJob {
    #[must_use]
    pub fn new(plan: ReprofilePlan) -> Self {
        let at = now_millis();
        let id = format!("reprofile-{at}-{}", sanitize_identifier(&plan.object));
        Self {
            id,
            plan,
            status: ReprofileStatus::Recommended,
            created_at_millis: at,
            updated_at_millis: at,
            detail: "recommended".to_owned(),
        }
    }

    #[must_use]
    pub fn to_sql_output(jobs: &[Self]) -> SqlOutput {
        SqlOutput::Rows(SqlRows {
            columns: vec![
                "id".to_owned(),
                "object".to_owned(),
                "from_layout".to_owned(),
                "to_layout".to_owned(),
                "status".to_owned(),
                "detail".to_owned(),
            ],
            rows: jobs
                .iter()
                .map(|job| {
                    vec![
                        Value::Str(job.id.clone()),
                        Value::Str(job.plan.object.clone()),
                        Value::Str(format!("{:?}", job.plan.from_layout)),
                        Value::Str(format!("{:?}", job.plan.to_layout)),
                        Value::Str(format!("{:?}", job.status)),
                        Value::Str(job.detail.clone()),
                    ]
                })
                .collect(),
        })
    }
}

impl WorkloadProfiler {
    pub fn record(repl: &dyn Replication, sample: &WorkloadSample) -> Result<(), TuningError> {
        let now = now_millis();
        let fingerprint = QueryFingerprint::new(&sample.sql);
        let key = workload_key(&fingerprint);
        let previous = repl
            .read(WORKLOAD_TABLE, &key, ReadConsistency::Strong)?
            .map(|bytes| decode::<WorkloadEntry>(&bytes))
            .transpose()?;
        let mut entry = previous.unwrap_or_else(|| WorkloadEntry {
            fingerprint: fingerprint.clone(),
            canonical_sql: canonical_sql(&sample.sql),
            executions: 0,
            total_latency_ms: 0.0,
            max_latency_ms: 0.0,
            returned_rows: 0,
            examined_rows: 0,
            access_pattern: access_pattern_from_sample(sample),
            used_indexes: BTreeSet::new(),
            first_seen_millis: now,
            last_seen_millis: now,
        });

        entry.executions = entry.executions.saturating_add(1);
        entry.total_latency_ms += sample.latency_ms.max(0.0);
        entry.max_latency_ms = entry.max_latency_ms.max(sample.latency_ms.max(0.0));
        entry.returned_rows = entry.returned_rows.saturating_add(sample.returned_rows);
        entry.examined_rows = entry.examined_rows.saturating_add(sample.examined_rows);
        if let Some(index) = sample.used_index {
            entry.used_indexes.insert(index);
        }
        entry.last_seen_millis = now;
        if matches!(entry.access_pattern, AccessPattern::Unknown) {
            entry.access_pattern = access_pattern_from_sample(sample);
        }

        let value = encode(&entry)?;
        propose_system(
            repl,
            Op::Put {
                table: WORKLOAD_TABLE.to_owned(),
                key,
                value,
            },
        )?;
        observability::record_workload_fingerprint(&entry.fingerprint.0);
        Ok(())
    }

    pub fn report(
        repl: &dyn Replication,
        window: WorkloadWindow,
    ) -> Result<WorkloadReport, TuningError> {
        let now = now_millis();
        let min_seen = window.duration().and_then(|duration| {
            let millis = u128_to_u64(duration.as_millis());
            now.checked_sub(millis)
        });
        let mut entries = Vec::new();
        for (_, value) in repl.range(WORKLOAD_TABLE, &[], &[0xFF], ReadConsistency::Strong)? {
            let entry = decode::<WorkloadEntry>(&value)?;
            if min_seen.is_none_or(|min| entry.last_seen_millis >= min) {
                entries.push(entry);
            }
        }
        entries.sort_by_key(|entry| Reverse(entry.examined_rows));
        Ok(WorkloadReport {
            window,
            generated_at_millis: now,
            entries,
        })
    }
}

impl IndexAdvisor {
    pub fn advise(
        repl: &dyn Replication,
        existing: &BTreeMap<String, Vec<RelIndexSpec>>,
    ) -> Result<IndexAdviceReport, TuningError> {
        let report = WorkloadProfiler::report(repl, WorkloadWindow::All)?;
        let mut recommendations = BTreeMap::<String, IndexRecommendation>::new();
        let mut used_indexes = BTreeMap::<String, BTreeSet<u32>>::new();

        for entry in &report.entries {
            if let Some((table, column)) = pattern_table_column(&entry.access_pattern) {
                for index in &entry.used_indexes {
                    used_indexes
                        .entry(table.clone())
                        .or_default()
                        .insert(*index);
                }

                let is_heavy_scan = entry.examined_rows >= DEFAULT_HEAVY_SCAN_ROWS
                    && entry.selectivity() <= 1.0 / DEFAULT_HEAVY_SCAN_RATIO
                    && !entry.has_index_usage();
                let has_any_index = existing
                    .get(table)
                    .is_some_and(|indexes| !indexes.is_empty());
                if is_heavy_scan && !has_any_index {
                    let candidate = IndexCandidate {
                        table: table.clone(),
                        column: column.clone(),
                    };
                    let scan_cost = entry.examined_rows as f64;
                    let index_cost = (entry.returned_rows.max(1) as f64) * 4.0 + 16.0;
                    let what_if = WhatIfIndex {
                        candidate: candidate.clone(),
                        estimated_scan_cost: scan_cost,
                        estimated_index_cost: index_cost,
                        estimated_benefit: (scan_cost - index_cost).max(0.0),
                    };
                    let id = advice_id("create", &candidate.table, &candidate.column);
                    recommendations.entry(id.clone()).or_insert_with(|| {
                        IndexRecommendation::Create {
                            id,
                            candidate,
                            what_if,
                            reason: format!(
                                "fingerprint {} examined {} rows for {} returned rows",
                                entry.fingerprint.0, entry.examined_rows, entry.returned_rows
                            ),
                            status: RecommendationStatus::Proposed,
                        }
                    });
                }
            }
        }

        for (table, indexes) in existing {
            let used = used_indexes.get(table);
            for index in indexes {
                if used.is_some_and(|ids| ids.contains(&index.id)) {
                    continue;
                }
                let id = advice_id("drop", table, &index.id.to_string());
                recommendations.entry(id.clone()).or_insert_with(|| {
                    IndexRecommendation::DropBallast {
                        id,
                        table: table.clone(),
                        index: index.clone(),
                        reason: "index had no recorded workload usage in the selected window"
                            .to_owned(),
                        status: RecommendationStatus::Proposed,
                    }
                });
            }
        }

        let mut recommendations = recommendations.into_values().collect::<Vec<_>>();
        recommendations.sort_by(|left, right| left.id().cmp(right.id()));
        let advice = IndexAdviceReport {
            generated_at_millis: now_millis(),
            recommendations,
        };
        persist_advice(repl, &advice)?;
        Ok(advice)
    }
}

pub fn read_advice(repl: &dyn Replication) -> Result<IndexAdviceReport, TuningError> {
    let mut recommendations = Vec::new();
    let mut generated_at_millis = 0;
    for (_, value) in repl.range(TUNING_ADVICE_TABLE, &[], &[0xFF], ReadConsistency::Strong)? {
        let recommendation = decode::<IndexRecommendation>(&value)?;
        generated_at_millis = generated_at_millis.max(now_millis());
        recommendations.push(recommendation);
    }
    recommendations.sort_by(|left, right| left.id().cmp(right.id()));
    Ok(IndexAdviceReport {
        generated_at_millis,
        recommendations,
    })
}

pub fn write_policy(repl: &dyn Replication, policy: &TuningPolicy) -> Result<(), TuningError> {
    policy.validate()?;
    propose_system(
        repl,
        Op::Put {
            table: TUNING_POLICY_TABLE.to_owned(),
            key: POLICY_KEY.to_vec(),
            value: encode(policy)?,
        },
    )?;
    Ok(())
}

pub fn read_policy(repl: &dyn Replication) -> Result<TuningPolicy, TuningError> {
    let Some(value) = repl.read(TUNING_POLICY_TABLE, POLICY_KEY, ReadConsistency::Strong)? else {
        return Ok(TuningPolicy::default());
    };
    decode(&value)
}

pub fn apply_decision_to_config(
    config: &mut PerformanceConfig,
    policy: &TuningPolicy,
    decision: TuningDecision,
) -> Result<TuningLogEntry, TuningError> {
    apply_decision_to_config_with_history(config, policy, decision, &[])
}

pub fn apply_decision_to_config_with_history(
    config: &mut PerformanceConfig,
    policy: &TuningPolicy,
    mut decision: TuningDecision,
    history: &[TuningLogEntry],
) -> Result<TuningLogEntry, TuningError> {
    let at_millis = now_millis();
    if !policy.enabled || policy.recommend_only {
        decision.status = TuningDecisionStatus::Recommended;
        return Ok(TuningLogEntry {
            id: decision.id.clone(),
            at_millis,
            decision,
            outcome: "recommend_only".to_owned(),
            rollback_of: None,
        });
    }
    policy.validate_decision(&decision)?;
    policy.validate_decision_timing(&decision, history, at_millis)?;
    set_config_parameter(config, decision.parameter, decision.new_value)?;
    decision.status = TuningDecisionStatus::Applied;
    Ok(TuningLogEntry {
        id: decision.id.clone(),
        at_millis,
        decision,
        outcome: "applied".to_owned(),
        rollback_of: None,
    })
}

pub fn rollback_decision_in_config(
    config: &mut PerformanceConfig,
    entry: &TuningLogEntry,
    reason: &str,
) -> Result<TuningLogEntry, TuningError> {
    set_config_parameter(config, entry.decision.parameter, entry.decision.old_value)?;
    let mut decision = entry.decision.clone();
    decision.status = TuningDecisionStatus::RolledBack;
    let rollback = TuningLogEntry {
        id: format!("rollback-{}-{}", now_millis(), entry.id),
        at_millis: now_millis(),
        decision,
        outcome: sanitize_detail(reason),
        rollback_of: Some(entry.id.clone()),
    };
    Ok(rollback)
}

pub fn write_tuning_log(repl: &dyn Replication, entry: &TuningLogEntry) -> Result<(), TuningError> {
    propose_system(
        repl,
        Op::Put {
            table: TUNING_LOG_TABLE.to_owned(),
            key: tuning_log_key(entry),
            value: encode(entry)?,
        },
    )?;
    observability::record_tuning_decision(
        &format!("{:?}", entry.decision.parameter),
        &entry.outcome,
    );
    Ok(())
}

pub fn read_tuning_log(repl: &dyn Replication) -> Result<Vec<TuningLogEntry>, TuningError> {
    repl.range(TUNING_LOG_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
        .into_iter()
        .map(|(_, value)| decode(&value))
        .collect()
}

pub fn find_tuning_log_entry(
    repl: &dyn Replication,
    id: &str,
) -> Result<TuningLogEntry, TuningError> {
    read_tuning_log(repl)?
        .into_iter()
        .find(|entry| entry.id == id)
        .ok_or_else(|| TuningError::MissingDecision(id.to_owned()))
}

pub fn write_reprofile_job(repl: &dyn Replication, job: &ReprofileJob) -> Result<(), TuningError> {
    propose_system(
        repl,
        Op::Put {
            table: REPROFILE_JOBS_TABLE.to_owned(),
            key: job.id.as_bytes().to_vec(),
            value: encode(job)?,
        },
    )?;
    observability::record_reprofile_lag(0.0);
    Ok(())
}

pub fn read_reprofile_job(
    repl: &dyn Replication,
    id: &str,
) -> Result<Option<ReprofileJob>, TuningError> {
    repl.read(REPROFILE_JOBS_TABLE, id.as_bytes(), ReadConsistency::Strong)?
        .map(|value| decode(&value))
        .transpose()
}

pub fn read_reprofile_jobs(repl: &dyn Replication) -> Result<Vec<ReprofileJob>, TuningError> {
    repl.range(REPROFILE_JOBS_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
        .into_iter()
        .map(|(_, value)| decode(&value))
        .collect()
}

pub fn start_reprofile_job(
    repl: &dyn Replication,
    plan: ReprofilePlan,
) -> Result<ReprofileJob, TuningError> {
    if plan.from_layout == plan.to_layout {
        return Err(TuningError::UnsupportedReprofile(
            "source and target layout are the same".to_owned(),
        ));
    }
    if !plan.reversible {
        return Err(TuningError::UnsupportedReprofile(
            "reprofiling jobs must be reversible".to_owned(),
        ));
    }
    let mut job = ReprofileJob::new(plan);
    job.status = ReprofileStatus::PlanningOnly;
    "planning-only: shadow copy and atomic switch are not wired".clone_into(&mut job.detail);
    write_reprofile_job(repl, &job)?;
    Ok(job)
}

pub fn advance_reprofile_job(
    repl: &dyn Replication,
    id: &str,
    status: ReprofileStatus,
    detail: &str,
) -> Result<ReprofileJob, TuningError> {
    let mut job = read_reprofile_job(repl, id)?
        .ok_or_else(|| TuningError::UnsupportedReprofile(format!("missing job {id}")))?;
    if job.status == ReprofileStatus::PlanningOnly
        && matches!(
            status,
            ReprofileStatus::Copying | ReprofileStatus::ReadyToSwitch | ReprofileStatus::Switched
        )
    {
        return Err(TuningError::UnsupportedReprofile(
            "reprofile jobs are planning-only until shadow copy and atomic switch are wired"
                .to_owned(),
        ));
    }
    job.status = status;
    job.detail = sanitize_detail(detail);
    job.updated_at_millis = now_millis();
    write_reprofile_job(repl, &job)?;
    Ok(job)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TuningSystemView {
    Workload,
    TuningLog,
    IndexAdvice,
    ReprofileJobs,
}

pub fn parse_system_view(sql: &str) -> Result<Option<TuningSystemView>, TuningError> {
    let trimmed = sql.trim_start();
    if !trimmed
        .get(..trimmed.len().min(6))
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
    {
        return Ok(None);
    }
    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
        return Ok(None);
    };
    if statements.len() != 1 {
        return Ok(None);
    }
    let Statement::Query(query) = &statements[0] else {
        return Ok(None);
    };
    let Some(name) = single_table_name(query)? else {
        return Ok(None);
    };
    Ok(match name.to_ascii_lowercase().as_str() {
        "system.workload" => Some(TuningSystemView::Workload),
        "system.tuning_log" => Some(TuningSystemView::TuningLog),
        "system.index_advice" => Some(TuningSystemView::IndexAdvice),
        "system.reprofile_jobs" => Some(TuningSystemView::ReprofileJobs),
        _ => None,
    })
}

#[must_use]
pub fn output_to_workload_sample(
    sql: &str,
    elapsed: Duration,
    output: &SqlOutput,
) -> WorkloadSample {
    let returned_rows = match output {
        SqlOutput::Rows(rows) => usize_to_u64(rows.rows.len()),
        SqlOutput::AffectedRows(rows) => usize_to_u64(*rows),
    };
    let access = parse_access_pattern(sql).unwrap_or(AccessPattern::Unknown);
    let examined_rows = match access {
        AccessPattern::FullScan { .. } => returned_rows.saturating_mul(4).max(returned_rows),
        AccessPattern::PointLookup { .. }
        | AccessPattern::RangeScan { .. }
        | AccessPattern::Unknown => returned_rows,
    };
    let (resource, filter_column) = pattern_table_column(&access)
        .map_or((None, None), |(table, column)| {
            (Some(table.clone()), Some(column.clone()))
        });
    WorkloadSample {
        sql: sql.to_owned(),
        latency_ms: duration_ms(elapsed),
        returned_rows,
        examined_rows,
        resource,
        filter_column,
        used_index: None,
    }
}

#[must_use]
pub fn current_parameter_value(config: &PerformanceConfig, parameter: TunableParameter) -> u64 {
    match parameter {
        TunableParameter::CacheMaxCapacity => config.cache.max_capacity,
        TunableParameter::GroupCommitWindowMs => config.io.group_commit_window_ms,
        TunableParameter::TargetPartitions => usize_to_u64(config.parallelism.target_partitions),
        TunableParameter::ScanBypassThresholdRows => {
            usize_to_u64(config.cache.scan_bypass_threshold_rows)
        }
        TunableParameter::VectorEfSearch => 64,
    }
}

fn set_config_parameter(
    config: &mut PerformanceConfig,
    parameter: TunableParameter,
    value: u64,
) -> Result<(), TuningError> {
    match parameter {
        TunableParameter::CacheMaxCapacity => config.cache.max_capacity = value,
        TunableParameter::GroupCommitWindowMs => config.io.group_commit_window_ms = value,
        TunableParameter::TargetPartitions => {
            config.parallelism.target_partitions = u64_to_usize(value)?;
        }
        TunableParameter::ScanBypassThresholdRows => {
            config.cache.scan_bypass_threshold_rows = u64_to_usize(value)?;
        }
        TunableParameter::VectorEfSearch => {}
    }
    Ok(())
}

fn persist_advice(repl: &dyn Replication, advice: &IndexAdviceReport) -> Result<(), TuningError> {
    let ops = advice
        .recommendations
        .iter()
        .map(|recommendation| {
            Ok(Op::Put {
                table: TUNING_ADVICE_TABLE.to_owned(),
                key: recommendation.id().as_bytes().to_vec(),
                value: encode(recommendation)?,
            })
        })
        .collect::<Result<Vec<_>, TuningError>>()?;
    if !ops.is_empty() {
        propose_system_batch(repl, ops)?;
    }
    Ok(())
}

fn access_pattern_from_sample(sample: &WorkloadSample) -> AccessPattern {
    if let (Some(table), Some(column)) = (&sample.resource, &sample.filter_column) {
        return AccessPattern::PointLookup {
            table: table.clone(),
            column: column.clone(),
        };
    }
    parse_access_pattern(&sample.sql).unwrap_or(AccessPattern::Unknown)
}

fn parse_access_pattern(sql: &str) -> Option<AccessPattern> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let Statement::Query(query) = statements.first()? else {
        return None;
    };
    let table = single_table_name(query).ok().flatten()?;
    let filter = simple_filter_column(query);
    Some(match filter {
        Some((column, BinaryOperator::Eq)) => AccessPattern::PointLookup { table, column },
        Some((column, _)) => AccessPattern::RangeScan { table, column },
        None => AccessPattern::FullScan { table },
    })
}

fn single_table_name(query: &Query) -> Result<Option<String>, TuningError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.from.len() != 1 {
        return Ok(None);
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    Ok(Some(object_name_to_string(name)?))
}

fn simple_filter_column(query: &Query) -> Option<(String, BinaryOperator)> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let SqlExpr::BinaryOp { left, op, right } = select.selection.as_ref()? else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (SqlExpr::Identifier(ident), _) | (_, SqlExpr::Identifier(ident)) => {
            Some((ident.value.clone(), op.clone()))
        }
        (SqlExpr::CompoundIdentifier(parts), _) => {
            parts.last().map(|ident| (ident.value.clone(), op.clone()))
        }
        (_, SqlExpr::CompoundIdentifier(parts)) => {
            parts.last().map(|ident| (ident.value.clone(), op.clone()))
        }
        _ => None,
    }
}

fn pattern_table_column(pattern: &AccessPattern) -> Option<(&String, &String)> {
    match pattern {
        AccessPattern::PointLookup { table, column }
        | AccessPattern::RangeScan { table, column } => Some((table, column)),
        AccessPattern::FullScan { .. } | AccessPattern::Unknown => None,
    }
}

fn access_pattern_name(pattern: &AccessPattern) -> &'static str {
    match pattern {
        AccessPattern::PointLookup { .. } => "point_lookup",
        AccessPattern::RangeScan { .. } => "range_scan",
        AccessPattern::FullScan { .. } => "full_scan",
        AccessPattern::Unknown => "unknown",
    }
}

fn object_name_to_string(name: &ObjectName) -> Result<String, TuningError> {
    let rendered = name
        .0
        .iter()
        .map(|part| part.to_string().trim_matches('"').to_owned())
        .collect::<Vec<_>>()
        .join(".");
    if rendered.is_empty() {
        return Err(QueryError::Unsupported("empty object name".to_owned()).into());
    }
    Ok(rendered)
}

fn canonical_sql(sql: &str) -> String {
    QueryFingerprint::new(sql).0
}

fn advice_id(kind: &str, table: &str, column: &str) -> String {
    format!(
        "{kind}-{}-{}",
        sanitize_identifier(table),
        sanitize_identifier(column)
    )
}

fn workload_key(fingerprint: &QueryFingerprint) -> Bytes {
    fingerprint.0.as_bytes().to_vec()
}

fn tuning_log_key(entry: &TuningLogEntry) -> Bytes {
    let mut key = Vec::with_capacity(16 + entry.id.len());
    key.extend_from_slice(&entry.at_millis.to_be_bytes());
    key.extend_from_slice(entry.id.as_bytes());
    key
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Bytes, TuningError> {
    serde_json::to_vec(value).map_err(|error| TuningError::Serde(error.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, TuningError> {
    serde_json::from_slice(bytes).map_err(|error| TuningError::Serde(error.to_string()))
}

fn sanitize_identifier(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_detail(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_control())
        .take(256)
        .collect()
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u128_to_u64(duration.as_millis()))
}

fn u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_to_usize(value: u64) -> Result<usize, TuningError> {
    usize::try_from(value)
        .map_err(|_| TuningError::OutsideEnvelope(format!("value {value} exceeds usize")))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::{
        IndexAdvisor, ParameterEnvelope, ReprofilePlan, ReprofileStatus, TunableParameter,
        TuningDecision, TuningDecisionStatus, TuningPolicy, WorkloadProfiler, WorkloadSample,
        WorkloadWindow, advance_reprofile_job, apply_decision_to_config,
        apply_decision_to_config_with_history, read_policy, read_reprofile_job, read_tuning_log,
        rollback_decision_in_config, start_reprofile_job, write_policy, write_tuning_log,
    };
    use crate::{
        db::Profile,
        performance::PerformanceConfig,
        query::{RelIndexSpec, TableLayout},
        repl::SingleNode,
        storage::MemEngine,
    };

    #[test]
    fn workload_profiler_merges_fingerprints() -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let sample = WorkloadSample::new("SELECT * FROM users WHERE age = 37")
            .with_latency_ms(2.0)
            .with_observed_rows(1, 500);
        WorkloadProfiler::record(&repl, &sample)?;
        WorkloadProfiler::record(&repl, &sample)?;

        let report = WorkloadProfiler::report(&repl, WorkloadWindow::All)?;
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].executions, 2);
        assert_eq!(report.entries[0].examined_rows, 1_000);
        Ok(())
    }

    #[test]
    fn index_advisor_recommends_missing_and_ballast_indexes()
    -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let sample = WorkloadSample::new("SELECT * FROM users WHERE age = 37")
            .with_observed_rows(1, 2_000)
            .with_access("users", "age");
        WorkloadProfiler::record(&repl, &sample)?;

        let mut existing = BTreeMap::new();
        existing.insert("orders".to_owned(), vec![RelIndexSpec::new(9, 1)]);
        let report = IndexAdvisor::advise(&repl, &existing)?;

        assert!(report.recommendations.iter().any(|recommendation| {
            matches!(
                recommendation,
                super::IndexRecommendation::Create { candidate, .. }
                    if candidate.table == "users" && candidate.column == "age"
            )
        }));
        assert!(report.recommendations.iter().any(|recommendation| {
            matches!(
                recommendation,
                super::IndexRecommendation::DropBallast { table, .. } if table == "orders"
            )
        }));
        Ok(())
    }

    #[test]
    fn tuning_policy_applies_and_rolls_back_config() -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let mut policy = TuningPolicy {
            enabled: true,
            recommend_only: false,
            ..TuningPolicy::default()
        };
        policy.envelopes.insert(
            TunableParameter::CacheMaxCapacity,
            ParameterEnvelope::new(64, 1_024, 64),
        );
        write_policy(&repl, &policy)?;
        assert_eq!(read_policy(&repl)?, policy);

        let mut config = PerformanceConfig::for_profile(Profile::Balanced);
        let decision = TuningDecision::new(
            TunableParameter::CacheMaxCapacity,
            config.cache.max_capacity,
            512,
            "cache miss pressure",
        );
        let entry = apply_decision_to_config(&mut config, &policy, decision)?;
        assert_eq!(entry.decision.status, TuningDecisionStatus::Applied);
        assert_eq!(config.cache.max_capacity, 512);
        write_tuning_log(&repl, &entry)?;

        let rollback = rollback_decision_in_config(&mut config, &entry, "p95 regression")?;
        write_tuning_log(&repl, &rollback)?;
        assert_eq!(config.cache.max_capacity, entry.decision.old_value);
        assert_eq!(read_tuning_log(&repl)?.len(), 2);
        Ok(())
    }

    #[test]
    fn tuning_policy_rejects_values_outside_envelope() {
        let mut policy = TuningPolicy {
            enabled: true,
            recommend_only: false,
            ..TuningPolicy::default()
        };
        policy.envelopes.insert(
            TunableParameter::GroupCommitWindowMs,
            ParameterEnvelope::new(0, 10, 1),
        );
        let mut config = PerformanceConfig::for_profile(Profile::Balanced);
        let decision = TuningDecision::new(
            TunableParameter::GroupCommitWindowMs,
            config.io.group_commit_window_ms,
            20,
            "too high",
        );
        assert!(apply_decision_to_config(&mut config, &policy, decision).is_err());
    }

    #[test]
    fn tuning_policy_enforces_cooldown() -> Result<(), Box<dyn std::error::Error>> {
        let mut policy = TuningPolicy {
            enabled: true,
            recommend_only: false,
            ..TuningPolicy::default()
        };
        policy.envelopes.insert(
            TunableParameter::CacheMaxCapacity,
            ParameterEnvelope {
                cooldown_secs: 60,
                max_changes_per_hour: 10,
                ..ParameterEnvelope::new(64, 1_024, 64)
            },
        );
        let mut config = PerformanceConfig::for_profile(Profile::Balanced);
        let old_value = config.cache.max_capacity;
        let first = apply_decision_to_config_with_history(
            &mut config,
            &policy,
            TuningDecision::new(
                TunableParameter::CacheMaxCapacity,
                old_value,
                512,
                "first change",
            ),
            &[],
        )?;

        let second = TuningDecision::new(
            TunableParameter::CacheMaxCapacity,
            config.cache.max_capacity,
            768,
            "too soon",
        );
        let Err(err) =
            apply_decision_to_config_with_history(&mut config, &policy, second, &[first])
        else {
            panic!("second change inside cooldown must be rejected");
        };
        assert!(err.to_string().contains("cooldown"));
        Ok(())
    }

    #[test]
    fn tuning_policy_enforces_max_changes_per_hour() -> Result<(), Box<dyn std::error::Error>> {
        let mut policy = TuningPolicy {
            enabled: true,
            recommend_only: false,
            ..TuningPolicy::default()
        };
        policy.envelopes.insert(
            TunableParameter::CacheMaxCapacity,
            ParameterEnvelope {
                cooldown_secs: 0,
                max_changes_per_hour: 1,
                ..ParameterEnvelope::new(64, 1_024, 64)
            },
        );
        let mut config = PerformanceConfig::for_profile(Profile::Balanced);
        let old_value = config.cache.max_capacity;
        let first = apply_decision_to_config_with_history(
            &mut config,
            &policy,
            TuningDecision::new(
                TunableParameter::CacheMaxCapacity,
                old_value,
                512,
                "first change",
            ),
            &[],
        )?;

        let second = TuningDecision::new(
            TunableParameter::CacheMaxCapacity,
            config.cache.max_capacity,
            768,
            "too many changes",
        );
        let Err(err) =
            apply_decision_to_config_with_history(&mut config, &policy, second, &[first])
        else {
            panic!("second change in same hour must be rejected");
        };
        assert!(err.to_string().contains("max_changes_per_hour"));
        Ok(())
    }

    #[test]
    fn reprofile_job_lifecycle_is_persisted() -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let job = start_reprofile_job(
            &repl,
            ReprofilePlan {
                object: "sales".to_owned(),
                from_layout: TableLayout::Row,
                to_layout: TableLayout::Columnar,
                reversible: true,
            },
        )?;
        assert_eq!(job.status, ReprofileStatus::PlanningOnly);
        assert!(job.detail.contains("planning-only"));

        assert!(
            advance_reprofile_job(
                &repl,
                &job.id,
                ReprofileStatus::ReadyToSwitch,
                "shadow copy verified",
            )
            .is_err()
        );
        assert_eq!(
            read_reprofile_job(&repl, &job.id)?.map(|job| job.status),
            Some(ReprofileStatus::PlanningOnly)
        );
        Ok(())
    }

    #[test]
    fn output_samples_detect_full_scan() {
        let sample = super::output_to_workload_sample(
            "SELECT * FROM users",
            Duration::from_millis(5),
            &crate::query::SqlOutput::AffectedRows(3),
        );
        assert_eq!(sample.examined_rows, 12);
    }
}
