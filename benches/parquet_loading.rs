use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use bq_runner::{ColumnDef, ExecutorBackend, YachtSqlExecutor};

fn create_test_parquet(dir: &TempDir, name: &str, row_count: usize) -> (String, Vec<ColumnDef>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let ids: Vec<i64> = (0..row_count as i64).collect();
    let names: Vec<String> = (0..row_count).map(|i| format!("name_{}", i)).collect();
    let scores: Vec<f64> = (0..row_count).map(|i| i as f64 * 0.5).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .unwrap();

    let path = dir.path().join(format!("{}.parquet", name));
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let col_schema = vec![
        ColumnDef::int64("id"),
        ColumnDef::string("name"),
        ColumnDef::float64("score"),
    ];

    (path.to_str().unwrap().to_string(), col_schema)
}

fn bench_parquet_loading(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parquet_loading");
    let dir = TempDir::new().unwrap();

    for row_count in [100, 1_000, 10_000] {
        let (path, schema) = create_test_parquet(&dir, &format!("bench_{}", row_count), row_count);

        group.bench_with_input(
            BenchmarkId::new("load_rows", row_count),
            &(path, schema),
            |b, (path, schema)| {
                b.to_async(&rt)
                    .iter_with_setup(YachtSqlExecutor::new, |executor| async move {
                        let table_name = format!("bench_table_{}", rand::random::<u32>());
                        let result = executor
                            .load_parquet(
                                black_box(&table_name),
                                black_box(path),
                                black_box(schema),
                            )
                            .await;
                        black_box(result)
                    });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_parquet_loading);
criterion_main!(benches);
