use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::domain::{ColumnDef, TableError};

#[derive(Debug, Clone)]
pub(crate) struct PipelineTable {
    pub(crate) name: String,
    pub(crate) sql: Option<String>,
    pub(crate) schema: Option<Vec<ColumnDef>>,
    pub(crate) rows: Vec<Value>,
    pub(crate) dependencies: Vec<String>,
    pub(crate) is_source: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipelineResult {
    pub succeeded: Vec<String>,
    pub failed: Vec<TableError>,
    pub skipped: Vec<String>,
}

impl PipelineResult {
    pub fn all_succeeded(&self) -> bool {
        self.failed.is_empty() && self.skipped.is_empty()
    }
}

pub(super) const DEFAULT_MAX_CONCURRENCY: usize = 8;
pub(super) const DEFAULT_TABLE_TIMEOUT_SECS: u64 = 300;

pub(super) struct StreamState {
    pub pending_deps: HashMap<String, HashSet<String>>,
    pub completed: HashSet<String>,
    pub blocked: HashSet<String>,
    pub in_flight: HashSet<String>,
    pub max_concurrency: usize,
    reverse_deps: HashMap<String, Vec<String>>,
}

impl StreamState {
    pub fn new(pending_deps: HashMap<String, HashSet<String>>, max_concurrency: usize) -> Self {
        let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();
        for (table, deps) in &pending_deps {
            for dep in deps {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(table.clone());
            }
        }
        Self {
            pending_deps,
            completed: HashSet::new(),
            blocked: HashSet::new(),
            in_flight: HashSet::new(),
            max_concurrency,
            reverse_deps,
        }
    }

    pub fn is_pending(&self, name: &str) -> bool {
        !self.completed.contains(name)
            && !self.blocked.contains(name)
            && !self.in_flight.contains(name)
    }

    pub fn is_ready(&self, name: &str) -> bool {
        self.pending_deps
            .get(name)
            .map(|deps| deps.is_empty())
            .unwrap_or(false)
            && self.is_pending(name)
    }

    pub fn mark_completed(&mut self, name: String) {
        if let Some(dependents) = self.reverse_deps.get(&name) {
            for dependent in dependents {
                if let Some(deps) = self.pending_deps.get_mut(dependent) {
                    deps.remove(&name);
                }
            }
        }
        self.completed.insert(name);
    }

    pub fn mark_blocked(&mut self, name: String) {
        self.blocked.insert(name);
    }

    pub fn mark_in_flight(&mut self, name: String) {
        self.in_flight.insert(name);
    }

    pub fn finish_in_flight(&mut self, name: &str) {
        self.in_flight.remove(name);
    }

    pub fn ready_tables(&self) -> Vec<&String> {
        let available_slots = self.max_concurrency.saturating_sub(self.in_flight.len());
        if available_slots == 0 {
            return vec![];
        }

        self.pending_deps
            .keys()
            .filter(|name| self.is_ready(name))
            .take(available_slots)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_pipeline_table_clone() {
        let table = PipelineTable {
            name: "test".to_string(),
            sql: Some("SELECT 1".to_string()),
            schema: None,
            rows: vec![json!(1)],
            dependencies: vec!["dep".to_string()],
            is_source: false,
        };
        let cloned = table.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.sql, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_pipeline_table_debug() {
        let table = PipelineTable {
            name: "t".to_string(),
            sql: None,
            schema: Some(vec![]),
            rows: vec![],
            dependencies: vec![],
            is_source: true,
        };
        let debug = format!("{:?}", table);
        assert!(debug.contains("PipelineTable"));
    }

    #[test]
    fn test_pipeline_result_default() {
        let result: PipelineResult = Default::default();
        assert!(result.succeeded.is_empty());
        assert!(result.failed.is_empty());
        assert!(result.skipped.is_empty());
    }

    #[test]
    fn test_pipeline_result_all_succeeded_true() {
        let result = PipelineResult {
            succeeded: vec!["a".to_string(), "b".to_string()],
            failed: vec![],
            skipped: vec![],
        };
        assert!(result.all_succeeded());
    }

    #[test]
    fn test_pipeline_result_all_succeeded_false_with_failed() {
        let result = PipelineResult {
            succeeded: vec!["a".to_string()],
            failed: vec![TableError {
                table: "b".to_string(),
                error: "err".to_string(),
            }],
            skipped: vec![],
        };
        assert!(!result.all_succeeded());
    }

    #[test]
    fn test_pipeline_result_all_succeeded_false_with_skipped() {
        let result = PipelineResult {
            succeeded: vec!["a".to_string()],
            failed: vec![],
            skipped: vec!["c".to_string()],
        };
        assert!(!result.all_succeeded());
    }

    #[test]
    fn test_pipeline_result_clone() {
        let result = PipelineResult {
            succeeded: vec!["x".to_string()],
            failed: vec![TableError {
                table: "y".to_string(),
                error: "e".to_string(),
            }],
            skipped: vec!["z".to_string()],
        };
        let cloned = result.clone();
        assert_eq!(cloned.succeeded, vec!["x".to_string()]);
        assert_eq!(cloned.failed.len(), 1);
        assert_eq!(cloned.skipped, vec!["z".to_string()]);
    }

    #[test]
    fn test_pipeline_result_eq() {
        let r1 = PipelineResult {
            succeeded: vec!["a".to_string()],
            failed: vec![],
            skipped: vec![],
        };
        let r2 = PipelineResult {
            succeeded: vec!["a".to_string()],
            failed: vec![],
            skipped: vec![],
        };
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_pipeline_result_debug() {
        let result = PipelineResult::default();
        let debug = format!("{:?}", result);
        assert!(debug.contains("PipelineResult"));
    }

    #[test]
    fn test_table_error_clone() {
        let err = TableError {
            table: "t".to_string(),
            error: "failed".to_string(),
        };
        let cloned = err.clone();
        assert_eq!(cloned.table, "t");
        assert_eq!(cloned.error, "failed");
    }

    #[test]
    fn test_table_error_eq() {
        let e1 = TableError {
            table: "t".to_string(),
            error: "e".to_string(),
        };
        let e2 = TableError {
            table: "t".to_string(),
            error: "e".to_string(),
        };
        assert_eq!(e1, e2);
    }

    #[test]
    fn test_table_error_debug() {
        let err = TableError {
            table: "t".to_string(),
            error: "e".to_string(),
        };
        let debug = format!("{:?}", err);
        assert!(debug.contains("TableError"));
    }

    #[test]
    fn test_stream_state_is_pending() {
        let state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );
        assert!(state.is_pending("a"));
        assert!(state.is_pending("nonexistent"));
    }

    #[test]
    fn test_stream_state_is_pending_false_when_completed() {
        let mut state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );
        state.mark_completed("a".to_string());
        assert!(!state.is_pending("a"));
    }

    #[test]
    fn test_stream_state_is_pending_false_when_blocked() {
        let mut state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );
        state.mark_blocked("a".to_string());
        assert!(!state.is_pending("a"));
    }

    #[test]
    fn test_stream_state_is_pending_false_when_in_flight() {
        let mut state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );
        state.mark_in_flight("a".to_string());
        assert!(!state.is_pending("a"));
    }

    #[test]
    fn test_stream_state_is_ready_true() {
        let state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );
        assert!(state.is_ready("a"));
    }

    #[test]
    fn test_stream_state_is_ready_false_has_deps() {
        let state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::from(["b".to_string()]))]),
            DEFAULT_MAX_CONCURRENCY,
        );
        assert!(!state.is_ready("a"));
    }

    #[test]
    fn test_stream_state_is_ready_false_not_in_pending() {
        let state = StreamState::new(HashMap::new(), DEFAULT_MAX_CONCURRENCY);
        assert!(!state.is_ready("a"));
    }

    #[test]
    fn test_stream_state_mark_completed() {
        let mut state = StreamState::new(
            HashMap::from([
                ("a".to_string(), HashSet::new()),
                ("b".to_string(), HashSet::from(["a".to_string()])),
            ]),
            DEFAULT_MAX_CONCURRENCY,
        );

        state.mark_completed("a".to_string());

        assert!(state.completed.contains("a"));
        assert!(state.pending_deps.get("b").unwrap().is_empty());
    }

    #[test]
    fn test_stream_state_mark_blocked() {
        let mut state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );

        state.mark_blocked("a".to_string());

        assert!(state.blocked.contains("a"));
        assert!(!state.is_pending("a"));
    }

    #[test]
    fn test_stream_state_mark_in_flight() {
        let mut state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );

        state.mark_in_flight("a".to_string());

        assert!(state.in_flight.contains("a"));
        assert!(!state.is_pending("a"));
    }

    #[test]
    fn test_stream_state_finish_in_flight() {
        let mut state = StreamState::new(
            HashMap::from([("a".to_string(), HashSet::new())]),
            DEFAULT_MAX_CONCURRENCY,
        );
        state.mark_in_flight("a".to_string());

        state.finish_in_flight("a");

        assert!(!state.in_flight.contains("a"));
    }

    #[test]
    fn test_stream_state_ready_tables_empty_when_at_capacity() {
        let mut state = StreamState::new(HashMap::from([("a".to_string(), HashSet::new())]), 2);
        state.mark_in_flight("b".to_string());
        state.mark_in_flight("c".to_string());

        let ready = state.ready_tables();
        assert!(ready.is_empty());
    }

    #[test]
    fn test_stream_state_ready_tables_returns_ready() {
        let state = StreamState::new(
            HashMap::from([
                ("a".to_string(), HashSet::new()),
                ("b".to_string(), HashSet::new()),
                ("c".to_string(), HashSet::from(["a".to_string()])),
            ]),
            DEFAULT_MAX_CONCURRENCY,
        );

        let ready = state.ready_tables();
        assert!(ready.iter().any(|s| s.as_str() == "a") || ready.iter().any(|s| s.as_str() == "b"));
        assert!(!ready.iter().any(|s| s.as_str() == "c"));
    }

    #[test]
    fn test_stream_state_ready_tables_respects_concurrency() {
        let state = StreamState::new(
            HashMap::from([
                ("a".to_string(), HashSet::new()),
                ("b".to_string(), HashSet::new()),
                ("c".to_string(), HashSet::new()),
            ]),
            2,
        );

        let ready = state.ready_tables();
        assert!(ready.len() <= 2);
    }

    #[test]
    fn test_default_max_concurrency() {
        assert_eq!(DEFAULT_MAX_CONCURRENCY, 8);
    }
}
