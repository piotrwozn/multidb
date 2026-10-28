//! EXPLAIN uses the same selected read path as execution. `DataFusion` diagnostics
//! stop at the optimized logical plan: constructing a physical plan can call a
//! provider's scan method, which would make plain EXPLAIN read business data.

use super::{
    AccessPath, BTreeMap, BTreeSet, Cost, DataType, ExplainNode, ExplainOptions, ExplainReport,
    Field, PreparedSelect, QueryError, QueryFingerprint, Schema, SchemaRef, SqlEngine, SqlOutput,
    SqlReadTarget, SqlRelationPolicy, optimizer, row_scan,
};

pub(super) struct PreparedExplain {
    sql: String,
    select: PreparedSelect,
    options: ExplainOptions,
}

pub(super) fn schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("operator", DataType::Utf8, false),
        Field::new("access_path", DataType::Utf8, false),
        Field::new("est_rows", DataType::Int64, true),
        Field::new("actual_rows", DataType::Int64, true),
        Field::new("cost", DataType::Float64, true),
        Field::new("actual_ms", DataType::Float64, true),
        Field::new("details", DataType::Utf8, false),
    ]))
}

impl SqlEngine {
    pub(super) async fn prepare_explain(
        &self,
        sql: &str,
        options: ExplainOptions,
        policies: &BTreeMap<String, SqlRelationPolicy>,
    ) -> Result<(PreparedExplain, BTreeSet<SqlReadTarget>), QueryError> {
        let (select, reads) = self.prepare_select(sql, policies).await?;
        // Plan first to discover the actual relations, including aliases/CTEs.
        // Policy-bearing EXPLAIN remains fail closed, as on the simple path.
        if let Some(SqlReadTarget::Catalog(name)) = reads.iter().find(
            |target| matches!(target, SqlReadTarget::Catalog(name) if policies.contains_key(name)),
        ) {
            return Err(QueryError::Unsupported(format!(
                "EXPLAIN over {name} is unsupported while it has row or masking policies"
            )));
        }
        Ok((
            PreparedExplain {
                sql: sql.to_owned(),
                select,
                options,
            },
            reads,
        ))
    }

    /// Explains a read without blocking the caller's Tokio runtime. ANALYZE runs
    /// the selected query only when explicitly requested.
    /// # Errors
    /// Fails when SQL cannot be planned or explicit ANALYZE execution fails.
    pub async fn explain_async(
        &self,
        sql: &str,
        options: ExplainOptions,
    ) -> Result<ExplainReport, QueryError> {
        let (prepared, _) = self.prepare_explain(sql, options, &BTreeMap::new()).await?;
        self.run_explain(prepared).await
    }

    pub(super) async fn run_explain(
        &self,
        prepared: PreparedExplain,
    ) -> Result<ExplainReport, QueryError> {
        let PreparedExplain {
            sql,
            select,
            options,
        } = prepared;
        match select {
            PreparedSelect::RowRange(plan) => self.explain_range(&sql, &plan, options),
            PreparedSelect::Simple { plan, .. } => self.explain_simple(&sql, &plan, options),
            PreparedSelect::DataFusion {
                ctx,
                plan,
                planning,
            } => {
                let started = std::time::Instant::now();
                let optimized = ctx
                    .state()
                    .optimize(&plan)
                    .map_err(|error| super::datafusion_to_query_error(&error))?;
                let details = format!(
                    "optimized logical plan; cardinality/cost unavailable; snapshot_lsn={:?}\n{}",
                    self.snapshot_lsn,
                    optimized.display_indent_schema()
                );
                let planning = planning.saturating_add(started.elapsed());
                let (actual_rows, actual_ms) = if options.analyze {
                    let started = std::time::Instant::now();
                    let output = self.run_datafusion_select(&ctx, *plan, planning).await?;
                    let SqlOutput::Rows(rows) = output else {
                        return Err(QueryError::Unsupported(
                            "EXPLAIN requires a read query".to_owned(),
                        ));
                    };
                    (
                        Some(super::usize_to_u64(rows.rows.len())),
                        Some(optimizer::elapsed_ms(started)),
                    )
                } else {
                    (None, None)
                };
                Ok(ExplainReport {
                    fingerprint: QueryFingerprint::new(&sql),
                    analyze: options.analyze,
                    nodes: vec![ExplainNode {
                        operator: "datafusion_logical_plan".to_owned(),
                        access_path: AccessPath::DataFusionPlan,
                        estimated_rows: 0,
                        actual_rows,
                        cost: Cost(0.0),
                        actual_ms,
                        details,
                    }],
                })
            }
        }
    }

    pub(super) fn explain_range(
        &self,
        sql: &str,
        plan: &row_scan::RowRangePlan,
        options: ExplainOptions,
    ) -> Result<ExplainReport, QueryError> {
        let (estimated_rows, details) = row_scan::explain_details(plan);
        let (actual_rows, actual_ms) = if options.analyze {
            let started = std::time::Instant::now();
            let SqlOutput::Rows(rows) = row_scan::execute(self, plan)? else {
                return Err(QueryError::Unsupported(
                    "EXPLAIN requires a read query".to_owned(),
                ));
            };
            (
                Some(super::usize_to_u64(rows.rows.len())),
                Some(optimizer::elapsed_ms(started)),
            )
        } else {
            (None, None)
        };
        let cost = Cost(0.0);
        Ok(ExplainReport {
            fingerprint: QueryFingerprint::new(sql),
            analyze: options.analyze,
            nodes: vec![ExplainNode {
                operator: "primary_key_range".to_owned(),
                access_path: AccessPath::PrimaryKeyRange {
                    estimated_rows,
                    cost,
                },
                estimated_rows,
                actual_rows,
                cost,
                actual_ms,
                details: format!("{}; {details}; cost unavailable", plan.table),
            }],
        })
    }

    pub(super) fn explain_fallback_sync(
        &self,
        sql: &str,
        options: ExplainOptions,
    ) -> Result<ExplainReport, QueryError> {
        // Keep the legacy synchronous API usable inside either Tokio runtime
        // flavor without nested block_on or blocking an async SQL dispatch.
        std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("multidb-explain".to_owned())
                .spawn_scoped(scope, || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| QueryError::DataFusion(error.to_string()))?;
                    runtime.block_on(self.explain_async(sql, options))
                })
                .map_err(|error| QueryError::DataFusion(error.to_string()))?;
            worker
                .join()
                .map_err(|_| QueryError::DataFusion("EXPLAIN worker panicked".to_owned()))?
        })
    }
}

#[cfg(test)]
mod tests;
