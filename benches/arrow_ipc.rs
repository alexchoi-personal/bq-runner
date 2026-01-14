use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::json;
use std::sync::Arc;

fn generate_test_rows(count: usize) -> Vec<Vec<serde_json::Value>> {
    (0..count)
        .map(|i| {
            vec![
                json!(i as i64),
                json!(format!("name_{}", i)),
                json!(i as f64 * 1.5),
                json!(i % 2 == 0),
            ]
        })
        .collect()
}

fn generate_query_result(row_count: usize) -> bq_runner::executor::QueryResult {
    bq_runner::executor::QueryResult {
        columns: vec![
            bq_runner::executor::ColumnInfo {
                name: "id".to_string(),
                data_type: "INT64".to_string(),
            },
            bq_runner::executor::ColumnInfo {
                name: "name".to_string(),
                data_type: "STRING".to_string(),
            },
            bq_runner::executor::ColumnInfo {
                name: "score".to_string(),
                data_type: "FLOAT64".to_string(),
            },
            bq_runner::executor::ColumnInfo {
                name: "active".to_string(),
                data_type: "BOOL".to_string(),
            },
        ],
        rows: generate_test_rows(row_count),
    }
}

fn bench_json_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_serialization");

    for row_count in [100, 1000, 10000] {
        let result = generate_query_result(row_count);

        group.bench_with_input(
            BenchmarkId::new("to_bq_response", row_count),
            &result,
            |b, result| {
                b.iter(|| black_box(result.to_bq_response()))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("serde_json_to_string", row_count),
            &result,
            |b, result| {
                let response = result.to_bq_response();
                b.iter(|| black_box(serde_json::to_string(&response).unwrap()))
            },
        );
    }

    group.finish();
}

fn bench_row_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_generation");

    for row_count in [1000, 10000, 100000] {
        group.bench_with_input(
            BenchmarkId::new("generate_test_rows", row_count),
            &row_count,
            |b, &count| {
                b.iter(|| black_box(generate_test_rows(count)))
            },
        );
    }

    group.finish();
}

fn bench_query_result_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_result_creation");

    for row_count in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("create_query_result", row_count),
            &row_count,
            |b, &count| {
                b.iter(|| black_box(generate_query_result(count)))
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_json_serialization,
    bench_row_generation,
    bench_query_result_creation
);

criterion_main!(benches);
