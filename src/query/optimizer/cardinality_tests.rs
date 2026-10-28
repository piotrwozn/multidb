use std::{collections::BTreeMap, sync::Arc};

use super::*;
use crate::{
    db::{DbConfig, Profile, create_database},
    query::{ColumnDef, SqlEngine},
};

fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Int, false),
        ],
        0,
    )
}

fn column_stats(row_count: u64, ndv: u64) -> ColumnStats {
    ColumnStats {
        row_count,
        sample_count: row_count,
        ndv,
        null_frac: 0.0,
        min: None,
        max: None,
        histogram: Vec::new(),
        mcv: Vec::new(),
        stats_version: 1,
        analyzed_at_txn: 1,
    }
}

fn table_stats() -> TableStats {
    TableStats {
        object_name: "items".to_owned(),
        object_kind: StatsObjectKind::Table,
        row_count: 10_000,
        sample_count: 10_000,
        columns: BTreeMap::from([
            ("id".to_owned(), column_stats(10_000, 10_000)),
            ("category".to_owned(), column_stats(10_000, 100)),
        ]),
        stats_version: 1,
        analyzed_at_txn: 1,
    }
}

fn eq(column: usize, value: i64) -> RelPredicate {
    RelPredicate::Eq {
        expression: RelIndexExpression::Column(column),
        value: Value::Int(value),
    }
}

fn filter_estimate(
    filter: &RelPredicate,
    stats: Option<&TableStats>,
    schema: Option<&TableSchema>,
) -> u64 {
    CostModel::new(CostProfile::Transactional)
        .choose_filter_path_with_schema(
            FilterPathRequest {
                layout: TableLayout::Row,
                indexes: &[],
                filter,
                stats,
                projection: &[0, 1],
            },
            schema,
        )
        .estimated_rows()
}

#[test]
fn filter_statistics_use_schema_names_and_not_map_order() {
    let stats = table_stats();
    let schema = schema();
    // The BTreeMap orders category before id, opposite to their schema positions.
    assert_eq!(filter_estimate(&eq(1, 7), Some(&stats), Some(&schema)), 100);
    assert_eq!(filter_estimate(&eq(0, 7), Some(&stats), Some(&schema)), 1);

    // A misleading legacy name must not override authoritative schema names.
    let mut stats = stats;
    stats
        .columns
        .insert("col1".to_owned(), column_stats(10_000, 2));
    assert_eq!(filter_estimate(&eq(1, 7), Some(&stats), Some(&schema)), 100);
}

#[test]
fn schemaless_statistics_keep_both_legacy_column_names() {
    for name in ["col1", "column_1"] {
        let mut stats = table_stats();
        stats.columns = BTreeMap::from([(name.to_owned(), column_stats(10_000, 100))]);
        let request = FilterPathRequest {
            layout: TableLayout::Row,
            indexes: &[],
            filter: &eq(1, 7),
            stats: Some(&stats),
            projection: &[0],
        };
        assert_eq!(
            CostModel::new(CostProfile::Transactional)
                .choose_filter_path(request)
                .estimated_rows(),
            100
        );
    }
}

#[test]
fn primary_key_bounds_do_not_depend_on_sampled_cardinality() {
    let schema = schema();
    let mut stats = table_stats();
    stats.sample_count = 10;
    stats
        .columns
        .insert("id".to_owned(), column_stats(10_000, 10));
    let pk = eq(0, 7);
    let category = eq(1, 7);
    assert_eq!(filter_estimate(&pk, None, Some(&schema)), 1);
    assert_eq!(filter_estimate(&pk, Some(&stats), Some(&schema)), 1);
    assert_eq!(
        filter_estimate(
            &RelPredicate::And(vec![pk.clone(), category.clone()]),
            Some(&stats),
            Some(&schema)
        ),
        1
    );
    assert_eq!(
        filter_estimate(
            &RelPredicate::Or(vec![pk.clone(), eq(0, 8)]),
            Some(&stats),
            Some(&schema)
        ),
        2
    );
    assert!(
        filter_estimate(
            &RelPredicate::Or(vec![pk, category]),
            Some(&stats),
            Some(&schema)
        ) > 2
    );
}

#[test]
fn eq_api_uses_primary_key_uniqueness_with_or_without_statistics() {
    let schema = schema();
    let mut stats = table_stats();
    stats
        .columns
        .insert("id".to_owned(), column_stats(10_000, 10));
    for stats in [None, Some(&stats)] {
        let plan = CostModel::new(CostProfile::Transactional).choose_eq_path_with_schema(
            EqPathRequest {
                layout: TableLayout::Row,
                indexes: &[],
                column: 0,
                column_name: "id",
                value: &Value::Int(7),
                stats,
                projected_columns: 2,
            },
            Some(&schema),
        );
        assert_eq!(plan.estimated_rows(), 1);
    }
}

#[test]
fn lower_ascii_does_not_inherit_raw_column_distribution_or_uniqueness() {
    let schema = TableSchema::new(vec![ColumnDef::new("id", ColumnType::Str, false)], 0);
    let mut stats = table_stats();
    stats.columns = BTreeMap::from([("id".to_owned(), column_stats(10_000, 10_000))]);
    let filter = RelPredicate::Eq {
        expression: RelIndexExpression::LowerAscii(0),
        value: Value::Str("ada".to_owned()),
    };
    assert_eq!(filter_estimate(&filter, Some(&stats), Some(&schema)), 50);
}

#[test]
#[allow(clippy::float_cmp)]
fn histogram_counts_are_normalized_by_sample_size() {
    let mut stats = column_stats(1_000, 1_000);
    stats.sample_count = 100;
    stats.histogram = vec![
        Bucket {
            lower: Some(Value::Int(0)),
            upper: Some(Value::Int(49)),
            count: 50,
        },
        Bucket {
            lower: Some(Value::Int(50)),
            upper: Some(Value::Int(99)),
            count: 50,
        },
    ];
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(0), &Value::Int(100)),
        1.0
    );
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(0), &Value::Int(50)),
        0.5
    );
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(100), &Value::Int(200)),
        0.0
    );
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(50), &Value::Int(50)),
        0.0
    );
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(100), &Value::Int(0)),
        0.0
    );

    stats.histogram[0].count = u64::MAX;
    stats.histogram[1].count = u64::MAX;
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(0), &Value::Int(100)),
        1.0
    );
    stats.sample_count = 0;
    assert_eq!(
        CardinalityEstimator::range_selectivity(&stats, &Value::Int(0), &Value::Int(100)),
        0.30
    );
}

#[test]
fn rare_values_use_probability_mass_remaining_after_common_values()
-> Result<(), Box<dyn std::error::Error>> {
    let rows = (0..1_000)
        .map(|id| vec![Value::Int(if id < 900 { 0 } else { id - 899 })])
        .collect::<Vec<_>>();
    let stats = build_table_stats(
        "skewed",
        StatsObjectKind::Table,
        &[("value".to_owned(), ColumnType::Int)],
        &rows,
        AnalyzeMode::Full,
        1,
        1,
    );
    let column = &stats.columns["value"];
    let rare = (1..=100)
        .map(Value::Int)
        .find(|value| !column.mcv.iter().any(|common| common.value == *value))
        .ok_or("MCV limit leaves some rare values unlisted")?;
    assert!((CardinalityEstimator::eq_selectivity(column, &Value::Int(0)) - 0.9).abs() < 1e-12);
    assert!((CardinalityEstimator::eq_selectivity(column, &rare) - 0.001).abs() < 1e-12);
    Ok(())
}

#[test]
fn malformed_frequencies_cannot_produce_nonfinite_cost_estimates() {
    for frequency in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 2.0] {
        let mut stats = column_stats(100, 10);
        stats.null_frac = frequency;
        stats.mcv = vec![MostCommonValue {
            value: Value::Int(1),
            count: 10,
            frequency,
        }];
        for value in [Value::Null, Value::Int(1), Value::Int(2)] {
            let selectivity = CardinalityEstimator::eq_selectivity(&stats, &value);
            assert!(selectivity.is_finite());
            assert!((0.0..=1.0).contains(&selectivity));
        }
    }
}

#[tokio::test]
async fn analyzed_sql_explain_reports_named_column_cardinality()
-> Result<(), Box<dyn std::error::Error>> {
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let mut engine = SqlEngine::new(repl);
    engine.create_table("items", Some(schema()), vec![RelIndexSpec::new(1, 1)])?;
    let values = (0..100)
        .map(|id| format!("({id}, {})", id % 10))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("insert into items (id, category) values {values}"))
        .await?;
    engine.analyze(AnalyzeTarget::Named("items".to_owned()), AnalyzeMode::Full)?;
    for (sql, count) in [
        ("select id from items where id = 73", 1),
        ("select id from items where category = 3", 10),
    ] {
        let report = engine.explain(sql, ExplainOptions { analyze: true })?;
        assert_eq!(report.nodes[0].estimated_rows, count);
        assert_eq!(report.nodes[0].actual_rows, Some(count));
    }
    Ok(())
}

#[tokio::test]
async fn stale_zero_statistics_never_suppress_current_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let mut engine = SqlEngine::new(repl.clone());
    engine.create_table("items", Some(schema()), vec![RelIndexSpec::new(1, 1)])?;
    engine
        .execute("insert into items (id, category) values (7, 3)")
        .await?;
    let stats = build_table_stats(
        "items",
        StatsObjectKind::Table,
        &[
            ("id".to_owned(), ColumnType::Int),
            ("category".to_owned(), ColumnType::Int),
        ],
        &[],
        AnalyzeMode::Full,
        1,
        1,
    );
    StatsCatalog::write_table(repl.as_ref(), &stats)?;
    assert_eq!(filter_estimate(&eq(0, 7), Some(&stats), Some(&schema())), 0);
    for (sql, expected_estimate) in [
        ("select id from items where id = 7", 1),
        ("select id from items where category = 3", 0),
    ] {
        assert_eq!(
            engine.execute(sql).await?,
            SqlOutput::Rows(SqlRows {
                columns: vec!["id".to_owned()],
                rows: vec![vec![Value::Int(7)]]
            })
        );
        let report = engine.explain(sql, ExplainOptions { analyze: true })?;
        assert_eq!(report.nodes[0].estimated_rows, expected_estimate);
        assert_eq!(report.nodes[0].actual_rows, Some(1));
    }
    // DataFusion's COUNT optimization must not treat analyzed counts as exact.
    assert_eq!(
        engine
            .execute("select count(*) as total from items")
            .await?,
        SqlOutput::Rows(SqlRows {
            columns: vec!["total".to_owned()],
            rows: vec![vec![Value::Int(1)]]
        })
    );
    Ok(())
}
