use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use bq_runner::domain::DagTableDef;
use bq_runner::session::Pipeline;

fn bench_dependency_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("dependency_extraction");

    let cases = [
        (
            "simple_select",
            "SELECT * FROM users",
            vec!["users"],
        ),
        (
            "join_two_tables",
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            vec!["users", "orders"],
        ),
        (
            "with_cte",
            "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active JOIN orders ON active.id = orders.user_id",
            vec!["users", "orders"],
        ),
        (
            "complex_join",
            "SELECT u.id, o.total, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id WHERE u.active = true",
            vec!["users", "orders", "products"],
        ),
        (
            "subquery",
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)",
            vec!["users", "orders"],
        ),
        (
            "union",
            "SELECT id, name FROM users UNION ALL SELECT id, name FROM archived_users",
            vec!["users", "archived_users"],
        ),
    ];

    for (name, sql, source_tables) in cases {
        group.bench_with_input(
            BenchmarkId::new("extract", name),
            &(sql, source_tables),
            |b, (sql, source_tables)| {
                b.iter(|| {
                    let mut pipeline = Pipeline::new();
                    let mut defs: Vec<DagTableDef> = source_tables
                        .iter()
                        .map(|name| DagTableDef {
                            name: name.to_string(),
                            sql: None,
                            schema: None,
                            rows: vec![],
                        })
                        .collect();
                    defs.push(DagTableDef {
                        name: "test_table".to_string(),
                        sql: Some(sql.to_string()),
                        schema: None,
                        rows: vec![],
                    });
                    let _ = pipeline.register(black_box(defs));
                })
            },
        );
    }

    group.finish();
}

fn bench_pipeline_register(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_register");

    group.bench_function("register_10_source_tables", |b| {
        b.iter(|| {
            let mut pipeline = Pipeline::new();
            let defs: Vec<DagTableDef> = (0..10)
                .map(|i| DagTableDef {
                    name: format!("table_{}", i),
                    sql: None,
                    schema: None,
                    rows: vec![],
                })
                .collect();
            let _ = pipeline.register(defs);
        })
    });

    group.bench_function("register_100_source_tables", |b| {
        b.iter(|| {
            let mut pipeline = Pipeline::new();
            let defs: Vec<DagTableDef> = (0..100)
                .map(|i| DagTableDef {
                    name: format!("table_{}", i),
                    sql: None,
                    schema: None,
                    rows: vec![],
                })
                .collect();
            let _ = pipeline.register(defs);
        })
    });

    group.bench_function("register_chain_10_tables", |b| {
        b.iter(|| {
            let mut pipeline = Pipeline::new();
            let mut defs = vec![DagTableDef {
                name: "table_0".to_string(),
                sql: None,
                schema: None,
                rows: vec![],
            }];
            for i in 1..10 {
                defs.push(DagTableDef {
                    name: format!("table_{}", i),
                    sql: Some(format!("SELECT * FROM table_{}", i - 1)),
                    schema: None,
                    rows: vec![],
                });
            }
            let _ = pipeline.register(defs);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_dependency_extraction,
    bench_pipeline_register
);
criterion_main!(benches);
