use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

use bq_runner::domain::ColumnType;
use bq_runner::{ColumnDef, DagTableDef, Pipeline, YachtSqlExecutor};
use serde_json::json;

fn create_source_table(name: &str, columns: Vec<(&str, ColumnType)>) -> DagTableDef {
    DagTableDef {
        name: name.to_string(),
        sql: None,
        schema: Some(
            columns
                .into_iter()
                .map(|(n, t)| ColumnDef {
                    name: n.to_string(),
                    column_type: t,
                })
                .collect(),
        ),
        rows: vec![json!([1, "test"])],
    }
}

fn create_computed_table(name: &str, sql: &str) -> DagTableDef {
    DagTableDef {
        name: name.to_string(),
        sql: Some(sql.to_string()),
        schema: None,
        rows: vec![],
    }
}

fn bench_pipeline_register(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_register");

    for size in [10, 50, 100] {
        group.bench_with_input(BenchmarkId::new("tables", size), &size, |b, &size| {
            b.iter(|| {
                let mut pipeline = Pipeline::new();
                let tables: Vec<DagTableDef> = (0..size)
                    .map(|i| {
                        create_source_table(
                            &format!("table_{}", i),
                            vec![("id", ColumnType::Int64)],
                        )
                    })
                    .collect();
                pipeline.register(tables).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_pipeline_build_execution_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_execution_plan");

    for depth in [5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::new("chain_depth", depth),
            &depth,
            |b, &depth| {
                b.iter_with_setup(
                    || {
                        let mut pipeline = Pipeline::new();
                        let source = create_source_table("source", vec![("id", ColumnType::Int64)]);
                        pipeline.register(vec![source]).unwrap();

                        for i in 0..depth {
                            let prev = if i == 0 {
                                "source".to_string()
                            } else {
                                format!("step_{}", i - 1)
                            };
                            let table = create_computed_table(
                                &format!("step_{}", i),
                                &format!("SELECT * FROM {}", prev),
                            );
                            pipeline.register(vec![table]).unwrap();
                        }
                        pipeline
                    },
                    |pipeline| {
                        pipeline.build_execution_plan(None, false).unwrap();
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_pipeline_execution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pipeline_execution");
    group.sample_size(20);

    for size in [5, 10, 20] {
        group.bench_with_input(BenchmarkId::new("tables", size), &size, |b, &size| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let mut pipeline = Pipeline::new();
                    let source = create_source_table(
                        "source",
                        vec![("id", ColumnType::Int64), ("name", ColumnType::String)],
                    );
                    pipeline.register(vec![source]).unwrap();

                    for i in 0..size {
                        let table = create_computed_table(
                            &format!("derived_{}", i),
                            "SELECT id, name FROM source",
                        );
                        pipeline.register(vec![table]).unwrap();
                    }

                    let executor = Arc::new(YachtSqlExecutor::new());
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        pipeline.run(executor, None).await.unwrap();
                    });
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_pipeline_register,
    bench_pipeline_build_execution_plan,
    bench_pipeline_execution,
);

criterion_main!(benches);
