use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

use bq_runner::executor::ExecutorMode;
use bq_runner::{SessionConfig, SessionManager, SessionManagerBuilder};

fn bench_session_creation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("session_creation");
    group.sample_size(50);

    group.bench_function("create_and_destroy", |b| {
        b.iter_custom(|iters| {
            let manager: Arc<SessionManager> = Arc::new(
                SessionManagerBuilder::default()
                    .mode(ExecutorMode::Mock)
                    .build(),
            );

            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let m = Arc::clone(&manager);
                let start = std::time::Instant::now();
                rt.block_on(async {
                    let session_id = m.create_session().await.unwrap();
                    m.destroy_session(session_id).unwrap();
                });
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

fn bench_concurrent_sessions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_sessions");
    group.sample_size(20);

    for num_sessions in [5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::new("sessions", num_sessions),
            &num_sessions,
            |b, &num_sessions| {
                b.iter_custom(|iters| {
                    let config = SessionConfig {
                        max_sessions: 100,
                        ..Default::default()
                    };
                    let manager: Arc<SessionManager> = Arc::new(
                        SessionManagerBuilder::default()
                            .mode(ExecutorMode::Mock)
                            .session_config(config)
                            .build(),
                    );

                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let m = Arc::clone(&manager);
                        let start = std::time::Instant::now();
                        rt.block_on(async {
                            let mut session_ids = Vec::with_capacity(num_sessions);
                            for _ in 0..num_sessions {
                                let session_id = m.create_session().await.unwrap();
                                session_ids.push(session_id);
                            }
                            for session_id in session_ids {
                                m.destroy_session(session_id).unwrap();
                            }
                        });
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_session_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("session_operations");
    group.sample_size(50);

    group.bench_function("query_execution", |b| {
        b.iter_custom(|iters| {
            let manager: Arc<SessionManager> = Arc::new(
                SessionManagerBuilder::default()
                    .mode(ExecutorMode::Mock)
                    .build(),
            );

            let session_id = rt.block_on(manager.create_session()).unwrap();

            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let m = Arc::clone(&manager);
                let start = std::time::Instant::now();
                rt.block_on(async {
                    m.execute_query(session_id, "SELECT 1 AS id, 'test' AS name")
                        .await
                        .unwrap();
                });
                total += start.elapsed();
            }

            manager.destroy_session(session_id).unwrap();
            total
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_session_creation,
    bench_concurrent_sessions,
    bench_session_operations,
);

criterion_main!(benches);
