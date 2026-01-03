use std::borrow::Cow;
use std::time::Instant;

use metrics::{counter, gauge, histogram};

fn intern_method(method: &str) -> Cow<'static, str> {
    match method {
        "bq.ping" => Cow::Borrowed("bq.ping"),
        "bq.health" => Cow::Borrowed("bq.health"),
        "bq.readiness" => Cow::Borrowed("bq.readiness"),
        "bq.liveness" => Cow::Borrowed("bq.liveness"),
        "bq.createSession" => Cow::Borrowed("bq.createSession"),
        "bq.destroySession" => Cow::Borrowed("bq.destroySession"),
        "bq.query" => Cow::Borrowed("bq.query"),
        "bq.createTable" => Cow::Borrowed("bq.createTable"),
        "bq.insert" => Cow::Borrowed("bq.insert"),
        "bq.defineTable" => Cow::Borrowed("bq.defineTable"),
        "bq.defineTables" => Cow::Borrowed("bq.defineTables"),
        "bq.dropTable" => Cow::Borrowed("bq.dropTable"),
        "bq.dropAllTables" => Cow::Borrowed("bq.dropAllTables"),
        "bq.execute" => Cow::Borrowed("bq.execute"),
        "bq.loadDirectory" => Cow::Borrowed("bq.loadDirectory"),
        "bq.registerDag" => Cow::Borrowed("bq.registerDag"),
        "bq.runDag" => Cow::Borrowed("bq.runDag"),
        "bq.retryDag" => Cow::Borrowed("bq.retryDag"),
        "bq.getDag" => Cow::Borrowed("bq.getDag"),
        "bq.clearDag" => Cow::Borrowed("bq.clearDag"),
        "bq.loadParquet" => Cow::Borrowed("bq.loadParquet"),
        "bq.listTables" => Cow::Borrowed("bq.listTables"),
        "bq.describeTable" => Cow::Borrowed("bq.describeTable"),
        "bq.setDefaultProject" => Cow::Borrowed("bq.setDefaultProject"),
        "bq.getDefaultProject" => Cow::Borrowed("bq.getDefaultProject"),
        "bq.getProjects" => Cow::Borrowed("bq.getProjects"),
        "bq.getDatasets" => Cow::Borrowed("bq.getDatasets"),
        "bq.getTablesInDataset" => Cow::Borrowed("bq.getTablesInDataset"),
        "bq.loadSqlDirectory" => Cow::Borrowed("bq.loadSqlDirectory"),
        "bq.loadParquetDirectory" => Cow::Borrowed("bq.loadParquetDirectory"),
        "bq.loadDagFromDirectory" => Cow::Borrowed("bq.loadDagFromDirectory"),
        _ => Cow::Owned(method.to_string()),
    }
}

pub fn record_rpc_request(method: &str) {
    counter!("rpc_requests_total", "method" => intern_method(method)).increment(1);
}

pub fn record_rpc_success(method: &str) {
    counter!("rpc_requests_success_total", "method" => intern_method(method)).increment(1);
}

pub fn record_rpc_error(method: &str, code: i32) {
    counter!("rpc_requests_error_total", "method" => intern_method(method), "code" => code.to_string()).increment(1);
}

pub fn record_rpc_duration(method: &str, start: Instant) {
    let duration = start.elapsed().as_secs_f64();
    histogram!("rpc_request_duration_seconds", "method" => intern_method(method)).record(duration);
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
