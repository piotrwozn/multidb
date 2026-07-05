use std::{env, hint::black_box, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};
use multidb::{
    db::{Database, DbConfig, Profile, create_database},
    model::{Value, encode_value},
    query::{ColumnDef, ColumnType, RelIndexExpression, RelIndexSpec, RelPredicate, TableSchema},
    timeseries::{TimeChunk, TimePoint, encode_chunk},
    vector::{QuantizationConfig, product_quantize, quantized_len, scalar_quantize},
};

fn bench_storage_and_model(c: &mut Criterion) {
    let rows = env::var("MULTIDB_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000);

    let mut database = create_database(DbConfig::new(Profile::InMemory)).expect("database");
    let users = database
        .create_table("users", Some(user_schema()), vec![RelIndexSpec::new(1, 2)])
        .expect("users table");
    for id in 0..rows {
        users.insert(user_row(id as i64)).expect("seed user");
    }

    let mut group = c.benchmark_group("storage_micro");
    group.bench_function("primary_key_get", |b| {
        b.iter(|| black_box(users.get(&Value::Int((rows / 2) as i64)).expect("get")))
    });
    group.bench_function("index_lookup", |b| {
        b.iter(|| black_box(users.query_eq(2, &Value::Int(37)).expect("index lookup")))
    });
    group.bench_function("value_encode", |b| {
        let value = Value::Array(user_row(42));
        b.iter(|| black_box(encode_value(&value).expect("encode value")))
    });
    group.bench_function("scalar_quantize_128d", |b| {
        let vector = bench_vector();
        b.iter(|| black_box(scalar_quantize(black_box(&vector))))
    });
    group.bench_function("product_quantize_128d", |b| {
        let vector = bench_vector();
        b.iter(|| black_box(product_quantize(black_box(&vector), 16)))
    });
    group.bench_function("gorilla_encode_regular_chunk", |b| {
        let chunk = regular_chunk(rows);
        b.iter(|| black_box(encode_chunk(black_box(&chunk)).expect("gorilla encode")))
    });
    group.finish();

    let mut advanced = create_database(DbConfig::new(Profile::InMemory)).expect("advanced db");
    let advanced_users = advanced
        .create_table(
            "users",
            Some(user_schema()),
            vec![
                RelIndexSpec::new(1, 2).with_include(vec![0, 1]),
                RelIndexSpec::new(2, 3).with_predicate(RelPredicate::Eq {
                    expression: RelIndexExpression::Column(3),
                    value: Value::Bool(true),
                }),
            ],
        )
        .expect("advanced users");
    for id in 0..rows {
        advanced_users
            .insert(user_row(id as i64))
            .expect("advanced seed user");
    }
    let mut phase31 = c.benchmark_group("phase31_features");
    phase31.bench_function("covering_index_lookup", |b| {
        b.iter(|| {
            black_box(
                advanced_users
                    .query_eq(2, &Value::Int(37))
                    .expect("covering lookup"),
            )
        })
    });
    phase31.bench_function("quantized_len_scalar_128d", |b| {
        b.iter(|| {
            black_box(quantized_len(
                black_box(&QuantizationConfig::Scalar { bits: 8 }),
                128,
            ))
        })
    });
    phase31.finish();

    let mut writes = c.benchmark_group("write_micro");
    writes.bench_function("fresh_insert", |b| {
        b.iter_batched(
            fresh_database,
            |database| {
                let table = database.table("users").expect("users");
                table.insert(black_box(user_row(10_000))).expect("insert");
            },
            criterion::BatchSize::SmallInput,
        )
    });
    writes.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500))
}

fn fresh_database() -> Database {
    let mut database = create_database(DbConfig::new(Profile::InMemory)).expect("database");
    database
        .create_table("users", Some(user_schema()), vec![RelIndexSpec::new(1, 2)])
        .expect("users table");
    database
}

fn user_schema() -> TableSchema {
    TableSchema::new(
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Str, false),
            ColumnDef::new("age", ColumnType::Int, false),
            ColumnDef::new("active", ColumnType::Bool, false),
        ],
        0,
    )
}

fn user_row(id: i64) -> Vec<Value> {
    vec![
        Value::Int(id),
        Value::Str(format!("user-{id}")),
        Value::Int(id.rem_euclid(100)),
        Value::Bool(id.rem_euclid(2) == 0),
    ]
}

fn regular_chunk(rows: usize) -> TimeChunk {
    TimeChunk {
        series: "bench".to_owned(),
        bucket_start: 0,
        points: (0..rows.max(1))
            .map(|idx| TimePoint {
                timestamp_millis: i64::try_from(idx).unwrap_or(i64::MAX) * 1_000,
                value: 42.0,
            })
            .collect(),
    }
}

fn bench_vector() -> Vec<f32> {
    (0..128)
        .map(|idx| f32::from(u16::try_from(idx).unwrap_or(0)) / 128.0)
        .collect()
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_storage_and_model
}
criterion_main!(benches);
