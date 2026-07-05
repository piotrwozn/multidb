use std::{env, hint::black_box, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};
use multidb::{
    db::{Database, DbConfig, Profile, create_database},
    model::Value,
    query::{ColumnDef, ColumnType, TableSchema},
};

const AGG_SQL: &str =
    "select category, avg(price), count(*) from sales group by category order by category";

fn bench_aggregation(c: &mut Criterion) {
    let rows = env::var("MULTIDB_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let (_row_dir, row_db) = setup_database(Profile::Transactional, rows);
    let (_columnar_dir, columnar_db) = setup_database(Profile::Analytical, rows);

    let mut group = c.benchmark_group("sales_group_by_category");
    group.bench_function("row_store_redb", |b| {
        b.iter(|| black_box(runtime.block_on(row_db.query(AGG_SQL)).expect("row query")))
    });
    group.bench_function("columnar_arrow_parquet", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(columnar_db.query(AGG_SQL))
                    .expect("columnar query"),
            )
        })
    });
    group.finish();

    let mut point = c.benchmark_group("point_insert_tradeoff");
    point.bench_function("row_store_redb_fresh_table", |b| {
        b.iter(|| {
            let (_dir, database) = setup_database(Profile::Transactional, 0);
            let table = database.table("sales").expect("row point table");
            table
                .insert(black_box(sales_row(1)))
                .expect("row point insert");
        })
    });
    point.bench_function("columnar_arrow_parquet_fresh_table", |b| {
        b.iter(|| {
            let (_dir, database) = setup_database(Profile::Analytical, 0);
            let table = database.table("sales").expect("columnar point table");
            table
                .insert(black_box(sales_row(1)))
                .expect("columnar point insert");
        })
    });
    point.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500))
}

fn setup_database(profile: Profile, rows: usize) -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join(format!("{profile:?}.redb"));
    let mut database = create_database(DbConfig::on_disk(profile, path)).expect("database");
    let table = database
        .create_table("sales", Some(sales_schema()), Vec::new())
        .expect("sales table");

    for id in 0..rows {
        table.insert(sales_row(id as i64)).expect("sales insert");
    }

    (dir, database)
}

fn sales_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("category", ColumnType::Str, false),
            ColumnDef::new("price", ColumnType::Float, false),
        ],
        0,
    )
}

fn sales_row(id: i64) -> Vec<Value> {
    vec![
        Value::Int(id),
        Value::Str(format!("cat{}", id.rem_euclid(8))),
        Value::Float((id.rem_euclid(97) as f64) + 0.5),
    ]
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_aggregation
}
criterion_main!(benches);
