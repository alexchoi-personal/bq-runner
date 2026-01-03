use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

use bq_runner::validation::{
    quote_identifier, validate_sql_for_define_table, validate_sql_for_query, validate_table_name,
};

fn bench_validate_table_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_table_name");

    let cases = [
        ("simple", "users"),
        ("qualified", "project.dataset.table"),
        ("long", &"a".repeat(128)),
    ];

    for (name, input) in cases {
        group.bench_with_input(BenchmarkId::new("validate", name), input, |b, input| {
            b.iter(|| validate_table_name(black_box(input)))
        });
    }

    group.finish();
}

fn bench_quote_identifier(c: &mut Criterion) {
    let mut group = c.benchmark_group("quote_identifier");

    let cases = [
        ("no_backticks", "simple_name"),
        ("with_backticks", "table`with`backticks"),
        ("many_backticks", "a`b`c`d`e`f`g"),
    ];

    for (name, input) in cases {
        group.bench_with_input(BenchmarkId::new("quote", name), input, |b, input| {
            b.iter(|| quote_identifier(black_box(input)))
        });
    }

    group.finish();
}

fn bench_validate_sql_for_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_sql_for_query");

    let cases = [
        ("simple_select", "SELECT * FROM users"),
        ("complex_select", "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id, u.name, o.total HAVING COUNT(*) > 1 ORDER BY o.total DESC LIMIT 100"),
        ("with_cte", "WITH active_users AS (SELECT * FROM users WHERE active = true), recent_orders AS (SELECT * FROM orders WHERE created_at > '2024-01-01') SELECT * FROM active_users JOIN recent_orders ON active_users.id = recent_orders.user_id"),
        ("subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"),
        ("union", "SELECT id, name FROM users UNION ALL SELECT id, name FROM archived_users"),
    ];

    for (name, sql) in cases {
        group.bench_with_input(BenchmarkId::new("validate", name), sql, |b, sql| {
            b.iter(|| validate_sql_for_query(black_box(sql)))
        });
    }

    group.finish();
}

fn bench_validate_sql_for_define_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_sql_for_define_table");

    let cases = [
        ("simple", "SELECT id, name FROM users"),
        ("aggregation", "SELECT user_id, SUM(total) as total_spent FROM orders GROUP BY user_id"),
        ("nested_subquery", "SELECT * FROM (SELECT * FROM (SELECT * FROM users) t1) t2"),
    ];

    for (name, sql) in cases {
        group.bench_with_input(BenchmarkId::new("validate", name), sql, |b, sql| {
            b.iter(|| validate_sql_for_define_table(black_box(sql)))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_validate_table_name,
    bench_quote_identifier,
    bench_validate_sql_for_query,
    bench_validate_sql_for_define_table,
);

criterion_main!(benches);
