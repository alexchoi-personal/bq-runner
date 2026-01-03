use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use serde_json::json;

use bq_runner::json_to_sql_value;

fn bench_json_to_sql_simple(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_to_sql_simple");

    group.bench_function("null", |b| {
        let val = json!(null);
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("bool_true", |b| {
        let val = json!(true);
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("integer", |b| {
        let val = json!(42);
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("float", |b| {
        let val = json!(3.14159);
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("string_short", |b| {
        let val = json!("hello");
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("string_with_quotes", |b| {
        let val = json!("it's a test");
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.finish();
}

fn bench_json_to_sql_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_to_sql_complex");

    group.bench_function("array_small", |b| {
        let val = json!([1, 2, 3, 4, 5]);
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("array_large", |b| {
        let val = json!((0..100).collect::<Vec<_>>());
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("object_small", |b| {
        let val = json!({"name": "John", "age": 30});
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.bench_function("object_nested", |b| {
        let val = json!({
            "user": {
                "name": "John",
                "address": {
                    "city": "NYC",
                    "zip": "10001"
                }
            },
            "orders": [1, 2, 3]
        });
        b.iter(|| json_to_sql_value(black_box(&val)))
    });

    group.finish();
}

fn bench_json_to_sql_string_escaping(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_to_sql_escaping");

    for size in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("string_len", size), &size, |b, &size| {
            let val = json!("a".repeat(size));
            b.iter(|| json_to_sql_value(black_box(&val)))
        });
    }

    for count in [1, 10, 50] {
        group.bench_with_input(BenchmarkId::new("quote_count", count), &count, |b, &count| {
            let s: String = (0..count).map(|i| format!("it's test {}", i)).collect::<Vec<_>>().join(" ");
            let val = json!(s);
            b.iter(|| json_to_sql_value(black_box(&val)))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_json_to_sql_simple,
    bench_json_to_sql_complex,
    bench_json_to_sql_string_escaping,
);

criterion_main!(benches);
