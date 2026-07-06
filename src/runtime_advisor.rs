#![allow(
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::too_many_lines
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config_spec::{
        CollectionIndexKind, CollectionRole, ConflictResolution, DatabaseSpec, GuaranteeValidator,
        ImpactLevel, MigrationPlan, MigrationPlanner, PlanImpact, RiskLevel, WriteAck,
        built_in_extension_manifest,
    },
    query::{PlannerFeedback, QueryError, RelIndexSpec, StatsCatalog},
    repl::{Op, ReadConsistency, ReplError, Replication, propose_system},
    storage::Bytes,
    tuning::{IndexAdviceReport, IndexAdvisor, IndexCandidate, IndexRecommendation, TuningError},
};

pub const RUNTIME_ADVICE_SCHEMA_VERSION: u32 = 1;
pub const RUNTIME_ADVICE_DECISIONS_TABLE: &str = "__runtime_advice_decisions";
pub const DEFAULT_REJECTION_SUPPRESSION_MILLIS: u64 = 24 * 60 * 60 * 1_000;

#[derive(thiserror::Error, Debug)]
pub enum RuntimeAdvisorError {
    #[error("tuning: {0}")]
    Tuning(#[from] TuningError),

    #[error("query: {0}")]
    Query(#[from] QueryError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("serialization: {0}")]
    Serde(String),

    #[error("missing runtime advice: {0}")]
    MissingAdvice(String),

    #[error("invalid runtime advice decision: {0}")]
    InvalidDecision(String),
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdviceReport {
    pub schema_version: u32,
    pub generated_at_millis: u64,
    pub auto_apply_enabled: bool,
    pub sources: Vec<AdviceSource>,
    pub suppressed_recommendations: usize,
    pub recommendations: Vec<RuntimeAdvice>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdviceSource {
    pub name: String,
    pub status: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdvice {
    pub id: String,
    pub code: String,
    pub message: String,
    pub rationale: String,
    pub cost: AdviceCost,
    pub risk: RiskLevel,
    pub expected_gain: String,
    pub rollback_conditions: Vec<String>,
    pub dry_run: MigrationPlanRef,
    pub status: RuntimeAdviceStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdviceCost {
    pub summary: String,
    pub write_amplification: ImpactLevel,
    pub disk: ImpactLevel,
    pub cpu: ImpactLevel,
    pub operator_effort: ImpactLevel,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationPlanRef {
    pub plan_id: String,
    pub operation_hint: String,
    pub cli_command: String,
    pub control_plane_endpoint: String,
    pub plan: MigrationPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAdviceStatus {
    Proposed,
    Accepted,
    Rejected,
    Applied,
    Superseded,
    Suppressed,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdvicePlanRequest {
    pub advice_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdviceDecisionRequest {
    pub advice_id: String,
    pub status: RuntimeAdviceStatus,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdviceDecision {
    pub advice_id: String,
    pub status: RuntimeAdviceStatus,
    pub reason: String,
    pub decided_by: String,
    pub decided_at_millis: u64,
    pub suppress_until_millis: Option<u64>,
}

pub struct RuntimeAdvisor;

impl RuntimeAdvisor {
    pub fn advise(
        repl: &dyn Replication,
        current: &DatabaseSpec,
        existing_indexes: &BTreeMap<String, Vec<RelIndexSpec>>,
    ) -> Result<RuntimeAdviceReport, RuntimeAdvisorError> {
        Self::advise_at(repl, current, existing_indexes, now_millis())
    }

    pub fn plan_by_id(
        repl: &dyn Replication,
        current: &DatabaseSpec,
        existing_indexes: &BTreeMap<String, Vec<RelIndexSpec>>,
        advice_id: &str,
    ) -> Result<MigrationPlan, RuntimeAdvisorError> {
        let report = Self::advise(repl, current, existing_indexes)?;
        report
            .recommendations
            .into_iter()
            .find(|advice| advice.id == advice_id)
            .map(|advice| advice.dry_run.plan)
            .ok_or_else(|| RuntimeAdvisorError::MissingAdvice(advice_id.to_owned()))
    }

    pub fn record_decision(
        repl: &dyn Replication,
        request: RuntimeAdviceDecisionRequest,
        decided_by: &str,
    ) -> Result<RuntimeAdviceDecision, RuntimeAdvisorError> {
        record_decision_at(repl, request, decided_by, now_millis())
    }

    fn advise_at(
        repl: &dyn Replication,
        current: &DatabaseSpec,
        existing_indexes: &BTreeMap<String, Vec<RelIndexSpec>>,
        generated_at_millis: u64,
    ) -> Result<RuntimeAdviceReport, RuntimeAdvisorError> {
        let index_advice = IndexAdvisor::advise(repl, existing_indexes)?;
        let planner_feedback = StatsCatalog::read_feedback(repl)?;
        let decisions = latest_decisions(repl)?;

        let mut recommendations = Vec::new();
        push_index_advice(current, &index_advice, &mut recommendations);
        push_planner_feedback_advice(current, &planner_feedback, &mut recommendations);
        push_validator_advice(current, &mut recommendations);
        push_collection_role_advice(current, &mut recommendations);
        push_unused_extension_advice(current, &mut recommendations);

        recommendations.sort_by(|left, right| left.id.cmp(&right.id));
        let (recommendations, suppressed_recommendations) =
            apply_decision_memory(recommendations, &decisions, generated_at_millis);

        Ok(RuntimeAdviceReport {
            schema_version: RUNTIME_ADVICE_SCHEMA_VERSION,
            generated_at_millis,
            auto_apply_enabled: false,
            sources: advice_sources(),
            suppressed_recommendations,
            recommendations,
        })
    }
}

fn push_index_advice(
    current: &DatabaseSpec,
    report: &IndexAdviceReport,
    recommendations: &mut Vec<RuntimeAdvice>,
) {
    for recommendation in &report.recommendations {
        match recommendation {
            IndexRecommendation::Create {
                id,
                candidate,
                what_if,
                reason,
                ..
            } => {
                let advice_id = format!("index-{id}");
                let hint_key = index_create_hint(candidate);
                let mut desired = current.clone();
                desired.operation_hints.insert(
                    hint_key.clone(),
                    format!(
                        "create_index table={} column={}",
                        candidate.table, candidate.column
                    ),
                );
                push_with_valid_plan(
                    current,
                    &desired,
                    recommendations,
                    RuntimeAdviceDraft {
                        id: advice_id,
                        code: "CREATE_INDEX".to_owned(),
                        message: format!(
                            "Create an index for {}.{}",
                            candidate.table, candidate.column
                        ),
                        rationale: reason.clone(),
                        cost: AdviceCost {
                            summary: "Physical index build after operator approval; dry-run is metadata-only."
                                .to_owned(),
                            write_amplification: ImpactLevel::Medium,
                            disk: ImpactLevel::Medium,
                            cpu: ImpactLevel::Medium,
                            operator_effort: ImpactLevel::Low,
                        },
                        risk: RiskLevel::Medium,
                        expected_gain: format!(
                            "Estimated scan cost drops from {:.1} to {:.1}; benefit {:.1}.",
                            what_if.estimated_scan_cost,
                            what_if.estimated_index_cost,
                            what_if.estimated_benefit
                        ),
                        rollback_conditions: vec![
                            "Drop the new index if write latency or disk growth exceeds the operator budget."
                                .to_owned(),
                            "Keep the previous query path until the index build is verified.".to_owned(),
                        ],
                        operation_hint: hint_key,
                    },
                );
            }
            IndexRecommendation::DropBallast {
                id,
                table,
                index,
                reason,
                ..
            } => {
                let advice_id = format!("index-{id}");
                let hint_key = format!("advisor.index.drop.{}.{}", table, index.id);
                let mut desired = current.clone();
                desired.operation_hints.insert(
                    hint_key.clone(),
                    format!("drop_index table={table} index_id={}", index.id),
                );
                push_with_valid_plan(
                    current,
                    &desired,
                    recommendations,
                    RuntimeAdviceDraft {
                        id: advice_id,
                        code: "DROP_UNUSED_INDEX".to_owned(),
                        message: format!("Review unused index {} on {table}", index.id),
                        rationale: reason.clone(),
                        cost: AdviceCost {
                            summary: "Operator review plus physical index drop after a dry-run."
                                .to_owned(),
                            write_amplification: ImpactLevel::Low,
                            disk: ImpactLevel::Low,
                            cpu: ImpactLevel::Low,
                            operator_effort: ImpactLevel::Low,
                        },
                        risk: RiskLevel::Medium,
                        expected_gain: "Lower write amplification and disk usage for an index absent from the workload window."
                            .to_owned(),
                        rollback_conditions: vec![
                            "Recreate the index if the workload starts using the path again.".to_owned(),
                            "Do not drop while query coverage for the table is incomplete.".to_owned(),
                        ],
                        operation_hint: hint_key,
                    },
                );
            }
        }
    }
}

fn push_planner_feedback_advice(
    current: &DatabaseSpec,
    feedback: &[PlannerFeedback],
    recommendations: &mut Vec<RuntimeAdvice>,
) {
    let mut by_fingerprint = BTreeMap::<String, &PlannerFeedback>::new();
    for entry in feedback {
        by_fingerprint
            .entry(entry.fingerprint.0.clone())
            .and_modify(|current| {
                if entry.ratio > current.ratio {
                    *current = entry;
                }
            })
            .or_insert(entry);
    }

    for entry in by_fingerprint.into_values() {
        let fingerprint = sanitize_identifier(&entry.fingerprint.0);
        let advice_id = format!("planner-refresh-statistics-{fingerprint}");
        let hint_key = format!("advisor.stats.refresh.{fingerprint}");
        let mut desired = current.clone();
        desired.operation_hints.insert(
            hint_key.clone(),
            format!(
                "refresh_statistics fingerprint={} ratio={:.2}",
                entry.fingerprint.0, entry.ratio
            ),
        );
        push_with_valid_plan(
            current,
            &desired,
            recommendations,
            RuntimeAdviceDraft {
                id: advice_id,
                code: "REFRESH_STATISTICS".to_owned(),
                message: "Refresh optimizer statistics for a misestimated plan".to_owned(),
                rationale: format!(
                    "{} estimated {} rows but observed {} rows (ratio {:.2}).",
                    entry.operator, entry.estimated_rows, entry.actual_rows, entry.ratio
                ),
                cost: AdviceCost {
                    summary: "ANALYZE-style statistics refresh; no data layout change.".to_owned(),
                    write_amplification: ImpactLevel::None,
                    disk: ImpactLevel::Low,
                    cpu: ImpactLevel::Low,
                    operator_effort: ImpactLevel::Low,
                },
                risk: RiskLevel::Low,
                expected_gain: "Better plan choices after correcting stale or incomplete cardinality estimates."
                    .to_owned(),
                rollback_conditions: vec![
                    "Restore the previous stats version if plan quality regresses.".to_owned(),
                ],
                operation_hint: hint_key,
            },
        );
    }
}

fn push_validator_advice(current: &DatabaseSpec, recommendations: &mut Vec<RuntimeAdvice>) {
    let validation = GuaranteeValidator::validate(current);
    for issue in validation.issues {
        let Some((advice_id, code, message, desired, hint_key, expected_gain)) =
            validator_fix(current, &issue.code)
        else {
            continue;
        };
        push_with_valid_plan(
            current,
            &desired,
            recommendations,
            RuntimeAdviceDraft {
                id: advice_id,
                code,
                message,
                rationale: issue.message,
                cost: AdviceCost {
                    summary: "Configuration guarantee change; operator must review the dry-run."
                        .to_owned(),
                    write_amplification: ImpactLevel::Low,
                    disk: ImpactLevel::Low,
                    cpu: ImpactLevel::Low,
                    operator_effort: ImpactLevel::Medium,
                },
                risk: RiskLevel::Medium,
                expected_gain,
                rollback_conditions: vec![
                    "Restore the previous guarantee setting if validation or rollout checks fail."
                        .to_owned(),
                ],
                operation_hint: hint_key,
            },
        );
    }
}

fn push_collection_role_advice(current: &DatabaseSpec, recommendations: &mut Vec<RuntimeAdvice>) {
    for collection in &current.collections {
        let target = if collection.name.contains("cache") || collection.name.contains("session") {
            Some(CollectionRole::Cache)
        } else if collection.name.contains("event") || collection.name.contains("log") {
            Some(CollectionRole::EventLog)
        } else if (collection.name.contains("vector") || collection.name.contains("embedding"))
            && collection.indexes.contains(&CollectionIndexKind::Vector)
        {
            Some(CollectionRole::VectorMemory)
        } else {
            None
        };
        let Some(target) = target else {
            continue;
        };
        if collection.role == target {
            continue;
        }

        let mut desired = current.clone();
        if let Some(candidate) = desired
            .collections
            .iter_mut()
            .find(|candidate| candidate.name == collection.name)
        {
            candidate.role = target;
        }
        let role = format!("{target:?}").to_ascii_lowercase();
        let advice_id = format!("role-{}-{role}", sanitize_identifier(&collection.name));
        let hint_key = format!(
            "advisor.role.{}.{}",
            sanitize_identifier(&collection.name),
            role
        );
        desired.operation_hints.insert(
            hint_key.clone(),
            format!(
                "change_collection_role collection={} role={role}",
                collection.name
            ),
        );
        push_with_valid_plan(
            current,
            &desired,
            recommendations,
            RuntimeAdviceDraft {
                id: advice_id,
                code: "CHANGE_COLLECTION_ROLE".to_owned(),
                message: format!("Review collection {} role as {role}", collection.name),
                rationale: "Collection name and declared indexes match a built-in role pattern."
                    .to_owned(),
                cost: AdviceCost {
                    summary: "Role change dry-run; physical behavior remains operator-controlled."
                        .to_owned(),
                    write_amplification: ImpactLevel::Low,
                    disk: ImpactLevel::Low,
                    cpu: ImpactLevel::Low,
                    operator_effort: ImpactLevel::Medium,
                },
                risk: RiskLevel::Medium,
                expected_gain: "A more explicit collection role improves validation, extension requirements and operator intent."
                    .to_owned(),
                rollback_conditions: vec!["Restore the previous collection role.".to_owned()],
                operation_hint: hint_key,
            },
        );
    }
}

fn push_unused_extension_advice(current: &DatabaseSpec, recommendations: &mut Vec<RuntimeAdvice>) {
    let required = required_extensions_from_collections(current);
    for extension in &current.extensions {
        if required.contains(&extension.name)
            || built_in_extension_manifest(&extension.name).is_none()
        {
            continue;
        }
        let mut desired = current.clone();
        desired
            .extensions
            .retain(|candidate| candidate.name != extension.name);
        let advice_id = format!("extension-unused-{}", sanitize_identifier(&extension.name));
        let hint_key = format!(
            "advisor.extension.remove.{}",
            sanitize_identifier(&extension.name)
        );
        desired.operation_hints.insert(
            hint_key.clone(),
            format!("remove_unused_extension name={}", extension.name),
        );
        push_with_valid_plan(
            current,
            &desired,
            recommendations,
            RuntimeAdviceDraft {
                id: advice_id,
                code: "REMOVE_UNUSED_EXTENSION".to_owned(),
                message: format!("Review unused extension {}", extension.name),
                rationale:
                    "The extension is explicitly configured but no collection role or index requires it."
                        .to_owned(),
                cost: AdviceCost {
                    summary: "Operator-managed extension removal after compatibility review.".to_owned(),
                    write_amplification: ImpactLevel::None,
                    disk: ImpactLevel::Low,
                    cpu: ImpactLevel::Low,
                    operator_effort: ImpactLevel::Medium,
                },
                risk: RiskLevel::Medium,
                expected_gain: "Smaller runtime capability surface and simpler support status."
                    .to_owned(),
                rollback_conditions: vec![
                    "Re-add the extension if any workload or object depends on it.".to_owned(),
                ],
                operation_hint: hint_key,
            },
        );
    }
}

struct RuntimeAdviceDraft {
    id: String,
    code: String,
    message: String,
    rationale: String,
    cost: AdviceCost,
    risk: RiskLevel,
    expected_gain: String,
    rollback_conditions: Vec<String>,
    operation_hint: String,
}

fn push_with_valid_plan(
    current: &DatabaseSpec,
    desired: &DatabaseSpec,
    recommendations: &mut Vec<RuntimeAdvice>,
    draft: RuntimeAdviceDraft,
) {
    let desired_validation = GuaranteeValidator::validate(desired);
    if !desired_validation.valid {
        return;
    }
    let plan = MigrationPlanner::plan(current, desired);
    if !plan.valid {
        return;
    }
    recommendations.push(RuntimeAdvice {
        id: draft.id.clone(),
        code: draft.code,
        message: draft.message,
        rationale: draft.rationale,
        cost: draft.cost,
        risk: draft.risk,
        expected_gain: draft.expected_gain,
        rollback_conditions: draft.rollback_conditions,
        dry_run: MigrationPlanRef {
            plan_id: plan.plan_id.clone(),
            operation_hint: draft.operation_hint,
            cli_command: format!(
                "multidb advice plan --advice-id {} --db <path> --profile <profile> --out {}.plan.json",
                draft.id, draft.id
            ),
            control_plane_endpoint: "/advice/plan".to_owned(),
            plan,
        },
        status: RuntimeAdviceStatus::Proposed,
    });
}

fn validator_fix(
    current: &DatabaseSpec,
    code: &str,
) -> Option<(String, String, String, DatabaseSpec, String, String)> {
    let mut desired = current.clone();
    match code {
        "CP_LOCAL_ACK" => {
            desired.guarantees.write_ack = WriteAck::Quorum;
            let hint = "advisor.validator.write_ack".to_owned();
            desired
                .operation_hints
                .insert(hint.clone(), "set write_ack=quorum".to_owned());
            Some((
                "validator-cp-local-ack".to_owned(),
                "FIX_CP_LOCAL_ACK".to_owned(),
                "Use quorum acknowledgement for strong CP".to_owned(),
                desired,
                hint,
                "Restores certifiable durability semantics for CP writes.".to_owned(),
            ))
        }
        "AP_MISSING_CONFLICT_POLICY" => {
            desired.guarantees.conflict_resolution = ConflictResolution::VectorClock;
            let hint = "advisor.validator.conflict_resolution".to_owned();
            desired.operation_hints.insert(
                hint.clone(),
                "set conflict_resolution=vector_clock".to_owned(),
            );
            Some((
                "validator-ap-conflict-policy".to_owned(),
                "FIX_AP_CONFLICT_POLICY".to_owned(),
                "Declare vector-clock conflict resolution for AP domains".to_owned(),
                desired,
                hint,
                "Makes eventual consistency convergence explicit and validator-approved."
                    .to_owned(),
            ))
        }
        "PRODUCTION_BACKUP_DISABLED" => {
            desired.guarantees.backup.enabled = true;
            desired.guarantees.backup.pitr = true;
            let hint = "advisor.validator.backup".to_owned();
            desired
                .operation_hints
                .insert(hint.clone(), "enable backup and pitr".to_owned());
            Some((
                "validator-production-backup".to_owned(),
                "ENABLE_BACKUP".to_owned(),
                "Enable backup coverage for production CP".to_owned(),
                desired,
                hint,
                "Restores recovery coverage required by the production profile.".to_owned(),
            ))
        }
        "SENSITIVE_WITHOUT_ENCRYPTION" => {
            desired.guarantees.encryption.at_rest = true;
            let hint = "advisor.validator.encryption".to_owned();
            desired
                .operation_hints
                .insert(hint.clone(), "enable encryption.at_rest".to_owned());
            Some((
                "validator-sensitive-encryption".to_owned(),
                "ENABLE_ENCRYPTION".to_owned(),
                "Enable at-rest encryption for sensitive data".to_owned(),
                desired,
                hint,
                "Restores the sensitive-data guarantee required by the validator.".to_owned(),
            ))
        }
        _ => None,
    }
}

fn apply_decision_memory(
    recommendations: Vec<RuntimeAdvice>,
    decisions: &BTreeMap<String, RuntimeAdviceDecision>,
    now: u64,
) -> (Vec<RuntimeAdvice>, usize) {
    let mut visible = Vec::with_capacity(recommendations.len());
    let mut suppressed = 0;
    for mut recommendation in recommendations {
        if let Some(decision) = decisions.get(&recommendation.id) {
            if decision.status == RuntimeAdviceStatus::Rejected
                && decision
                    .suppress_until_millis
                    .is_some_and(|until| until > now)
            {
                suppressed += 1;
                continue;
            }
            if decision.status != RuntimeAdviceStatus::Rejected {
                recommendation.status = decision.status;
            }
        }
        visible.push(recommendation);
    }
    (visible, suppressed)
}

fn record_decision_at(
    repl: &dyn Replication,
    request: RuntimeAdviceDecisionRequest,
    decided_by: &str,
    now: u64,
) -> Result<RuntimeAdviceDecision, RuntimeAdvisorError> {
    if !matches!(
        request.status,
        RuntimeAdviceStatus::Accepted | RuntimeAdviceStatus::Rejected
    ) {
        return Err(RuntimeAdvisorError::InvalidDecision(format!(
            "status {:?} is not accepted by the decision endpoint",
            request.status
        )));
    }
    if request.advice_id.trim().is_empty() {
        return Err(RuntimeAdvisorError::InvalidDecision(
            "advice_id must not be empty".to_owned(),
        ));
    }
    let decision = RuntimeAdviceDecision {
        advice_id: request.advice_id,
        status: request.status,
        reason: sanitize_detail(&request.reason),
        decided_by: sanitize_detail(decided_by),
        decided_at_millis: now,
        suppress_until_millis: (request.status == RuntimeAdviceStatus::Rejected)
            .then_some(now.saturating_add(DEFAULT_REJECTION_SUPPRESSION_MILLIS)),
    };
    propose_system(
        repl,
        Op::Put {
            table: RUNTIME_ADVICE_DECISIONS_TABLE.to_owned(),
            key: decision_key(&decision),
            value: encode(&decision)?,
        },
    )?;
    Ok(decision)
}

fn latest_decisions(
    repl: &dyn Replication,
) -> Result<BTreeMap<String, RuntimeAdviceDecision>, RuntimeAdvisorError> {
    let mut decisions = BTreeMap::new();
    for (_, value) in repl.range(
        RUNTIME_ADVICE_DECISIONS_TABLE,
        &[],
        &[0xFF],
        ReadConsistency::Strong,
    )? {
        let decision = decode::<RuntimeAdviceDecision>(&value)?;
        decisions
            .entry(decision.advice_id.clone())
            .and_modify(|current: &mut RuntimeAdviceDecision| {
                if decision.decided_at_millis > current.decided_at_millis {
                    *current = decision.clone();
                }
            })
            .or_insert(decision);
    }
    Ok(decisions)
}

fn advice_sources() -> Vec<AdviceSource> {
    vec![
        source(
            "workload_profiler",
            "active",
            "system.workload observations",
        ),
        source("index_advisor", "active", "index advice"),
        source(
            "planner_feedback",
            "active",
            "EXPLAIN ANALYZE estimate feedback",
        ),
        source(
            "guarantee_validator",
            "active",
            "configuration validation issues",
        ),
        source("migration_planner", "active", "migration dry-run plans"),
        source(
            "performance_baselines",
            "metadata",
            "baselines/perf profiles are referenced by release governance",
        ),
    ]
}

fn source(name: &str, status: &str, detail: &str) -> AdviceSource {
    AdviceSource {
        name: name.to_owned(),
        status: status.to_owned(),
        detail: detail.to_owned(),
    }
}

fn index_create_hint(candidate: &IndexCandidate) -> String {
    format!(
        "advisor.index.create.{}.{}",
        sanitize_identifier(&candidate.table),
        sanitize_identifier(&candidate.column)
    )
}

fn required_extensions_from_collections(spec: &DatabaseSpec) -> BTreeSet<String> {
    let mut required = BTreeSet::new();
    for collection in &spec.collections {
        match collection.role {
            CollectionRole::EventLog => {
                required.insert("cdc".to_owned());
            }
            CollectionRole::VectorMemory => {
                required.insert("vector_hnsw".to_owned());
            }
            CollectionRole::Audit => {
                required.insert("audit".to_owned());
            }
            CollectionRole::Graph => {
                required.insert("graph_index".to_owned());
            }
            CollectionRole::Analytics => {
                required.insert("columnar_layout".to_owned());
            }
            CollectionRole::TimeSeries => {
                required.insert("time_series".to_owned());
            }
            CollectionRole::DocumentEntity | CollectionRole::KeyValue | CollectionRole::Cache => {}
        }
        for index in &collection.indexes {
            match index {
                CollectionIndexKind::Document => {
                    required.insert("document_index".to_owned());
                }
                CollectionIndexKind::Vector => {
                    required.insert("vector_hnsw".to_owned());
                }
                CollectionIndexKind::Graph => {
                    required.insert("graph_index".to_owned());
                }
                CollectionIndexKind::FullText => {
                    required.insert("full_text".to_owned());
                }
                CollectionIndexKind::Columnar => {
                    required.insert("columnar_layout".to_owned());
                }
                CollectionIndexKind::TimeSeries => {
                    required.insert("time_series".to_owned());
                }
                CollectionIndexKind::Primary => {}
            }
        }
    }
    required
}

fn decision_key(decision: &RuntimeAdviceDecision) -> Bytes {
    format!("{}:{}", decision.advice_id, decision.decided_at_millis).into_bytes()
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Bytes, RuntimeAdvisorError> {
    serde_json::to_vec(value).map_err(|error| RuntimeAdvisorError::Serde(error.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, RuntimeAdvisorError> {
    serde_json::from_slice(bytes).map_err(|error| RuntimeAdvisorError::Serde(error.to_string()))
}

fn sanitize_identifier(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_detail(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[allow(dead_code)]
fn _assert_plan_impact_is_used(_: PlanImpact) {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        RuntimeAdviceDecisionRequest, RuntimeAdviceStatus, RuntimeAdvisor, record_decision_at,
    };
    use crate::{
        config_spec::{DatabaseSpec, WriteAck},
        db::{DbConfig, Profile},
        query::{PlannerFeedback, QueryFingerprint, StatsCatalog},
        repl::SingleNode,
        storage::MemEngine,
        tuning::{WorkloadProfiler, WorkloadSample},
    };

    #[test]
    fn missing_index_gets_costed_dry_run_advice() -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let sample = WorkloadSample::new("SELECT * FROM users WHERE age = 37")
            .with_observed_rows(1, 2_000)
            .with_access("users", "age");
        WorkloadProfiler::record(&repl, &sample)?;
        let current = DatabaseSpec::from_db_config("current", &DbConfig::new(Profile::InMemory));

        let report = RuntimeAdvisor::advise(&repl, &current, &BTreeMap::new())?;
        let advice = report
            .recommendations
            .iter()
            .find(|advice| advice.code == "CREATE_INDEX")
            .ok_or("missing create-index advice")?;

        assert!(!report.auto_apply_enabled);
        assert!(advice.expected_gain.contains("Estimated scan cost"));
        assert!(advice.dry_run.plan.valid);
        assert!(
            advice
                .dry_run
                .operation_hint
                .contains("advisor.index.create.users.age")
        );
        Ok(())
    }

    #[test]
    fn rejected_advice_is_suppressed_for_twenty_four_hours()
    -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let sample = WorkloadSample::new("SELECT * FROM users WHERE age = 37")
            .with_observed_rows(1, 2_000)
            .with_access("users", "age");
        WorkloadProfiler::record(&repl, &sample)?;
        let current = DatabaseSpec::from_db_config("current", &DbConfig::new(Profile::InMemory));
        let first = RuntimeAdvisor::advise_at(&repl, &current, &BTreeMap::new(), 10)?;
        let advice_id = first.recommendations[0].id.clone();

        record_decision_at(
            &repl,
            RuntimeAdviceDecisionRequest {
                advice_id,
                status: RuntimeAdviceStatus::Rejected,
                reason: "not worth it".to_owned(),
            },
            "tester",
            20,
        )?;
        let second = RuntimeAdvisor::advise_at(&repl, &current, &BTreeMap::new(), 30)?;

        assert_eq!(second.suppressed_recommendations, 1);
        assert!(second.recommendations.is_empty());
        Ok(())
    }

    #[test]
    fn validator_advice_is_skipped_when_desired_spec_remains_invalid()
    -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        let mut current =
            DatabaseSpec::from_db_config("current", &DbConfig::on_disk(Profile::Vector, "x.redb"));
        current.guarantees.write_ack = WriteAck::Local;
        current.guarantees.sensitive_data = true;

        let report = RuntimeAdvisor::advise(&repl, &current, &BTreeMap::new())?;

        assert!(
            !report
                .recommendations
                .iter()
                .any(|advice| advice.code == "FIX_CP_LOCAL_ACK")
        );
        Ok(())
    }

    #[test]
    fn planner_feedback_generates_refresh_statistics_advice()
    -> Result<(), Box<dyn std::error::Error>> {
        let repl = SingleNode::new(MemEngine::new());
        StatsCatalog::record_feedback(
            &repl,
            &PlannerFeedback {
                fingerprint: QueryFingerprint::new("SELECT * FROM users WHERE age = 37"),
                operator: "SeqScan".to_owned(),
                estimated_rows: 1,
                actual_rows: 100,
                ratio: 100.0,
            },
        )?;
        let current = DatabaseSpec::from_db_config("current", &DbConfig::new(Profile::InMemory));

        let report = RuntimeAdvisor::advise(&repl, &current, &BTreeMap::new())?;

        assert!(
            report
                .recommendations
                .iter()
                .any(|advice| advice.code == "REFRESH_STATISTICS")
        );
        Ok(())
    }
}
