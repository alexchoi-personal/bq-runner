use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::rpc::types::ColumnDef;

#[derive(Debug, Clone)]
pub struct PipelineTable {
    pub name: String,
    pub sql: Option<String>,
    pub schema: Option<Vec<ColumnDef>>,
    pub rows: Vec<Value>,
    pub dependencies: Vec<String>,
    pub is_source: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipelineResult {
    pub succeeded: Vec<String>,
    pub failed: Vec<TableError>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableError {
    pub table: String,
    pub error: String,
}

impl PipelineResult {
    pub fn all_succeeded(&self) -> bool {
        self.failed.is_empty() && self.skipped.is_empty()
    }
}

pub(super) const DEFAULT_MAX_CONCURRENCY: usize = 8;

pub(super) struct StreamState {
    pub pending_deps: HashMap<String, HashSet<String>>,
    pub completed: HashSet<String>,
    pub blocked: HashSet<String>,
    pub in_flight: HashSet<String>,
    pub max_concurrency: usize,
}

impl StreamState {
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

    pub fn mark_completed(&mut self, name: &str) {
        self.completed.insert(name.to_string());
        for deps in self.pending_deps.values_mut() {
            deps.remove(name);
        }
    }

    pub fn mark_blocked(&mut self, name: &str) {
        self.blocked.insert(name.to_string());
    }

    pub fn mark_in_flight(&mut self, name: &str) {
        self.in_flight.insert(name.to_string());
    }

    pub fn finish_in_flight(&mut self, name: &str) {
        self.in_flight.remove(name);
    }

    pub fn ready_tables(&self) -> Vec<String> {
        let available_slots = self.max_concurrency.saturating_sub(self.in_flight.len());
        if available_slots == 0 {
            return vec![];
        }

        self.pending_deps
            .keys()
            .filter(|name| self.is_ready(name))
            .take(available_slots)
            .cloned()
            .collect()
    }
}
