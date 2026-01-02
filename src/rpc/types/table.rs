use serde::{Deserialize, Serialize};

fn default_limit() -> usize {
    100
}

#[derive(Debug, Deserialize, Default)]
pub struct PaginationParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

#[derive(Debug, Serialize)]
pub struct HealthResult {
    pub status: String,
    pub session_count: usize,
    pub uptime_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct DefineTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Serialize)]
pub struct DefineTableResult {
    pub success: bool,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct DefineTablesParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub tables: Vec<TableDefinition>,
}

#[derive(Debug, Serialize)]
pub struct DefineTablesResult {
    pub success: bool,
    pub tables: Vec<DefineTableResult>,
}

#[derive(Debug, Deserialize)]
pub struct DropTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct DropAllTablesParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ExecuteParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub tables: Option<Vec<String>>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Serialize)]
pub struct TableError {
    pub table: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct ExecuteResult {
    pub success: bool,
    pub succeeded: Vec<String>,
    pub failed: Vec<TableError>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoadDirectoryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct LoadedTableInfo {
    pub name: String,
    pub kind: String,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct LoadDirectoryResult {
    pub success: bool,
    pub tables: Vec<LoadedTableInfo>,
}
