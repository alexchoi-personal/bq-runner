use metrics::{counter, gauge, histogram};
use std::time::Instant;

pub fn record_rpc_request(method: &str) {
    counter!("rpc_requests_total", "method" => method.to_string()).increment(1);
}

pub fn record_rpc_success(method: &str) {
    counter!("rpc_requests_success_total", "method" => method.to_string()).increment(1);
}

pub fn record_rpc_error(method: &str, code: i32) {
    counter!("rpc_requests_error_total", "method" => method.to_string(), "code" => code.to_string()).increment(1);
}

pub fn record_rpc_duration(method: &str, start: Instant) {
    let duration = start.elapsed().as_secs_f64();
    histogram!("rpc_request_duration_seconds", "method" => method.to_string()).record(duration);
}

pub fn set_active_sessions(count: usize) {
    gauge!("active_sessions").set(count as f64);
}

pub fn record_session_created() {
    counter!("sessions_created_total").increment(1);
}

pub fn record_session_destroyed() {
    counter!("sessions_destroyed_total").increment(1);
}

pub fn record_query_executed() {
    counter!("queries_executed_total").increment(1);
}

pub fn record_tables_defined(count: usize) {
    counter!("tables_defined_total").increment(count as u64);
}

pub fn record_dag_execution(tables_succeeded: usize, tables_failed: usize) {
    counter!("dag_tables_succeeded_total").increment(tables_succeeded as u64);
    counter!("dag_tables_failed_total").increment(tables_failed as u64);
}
