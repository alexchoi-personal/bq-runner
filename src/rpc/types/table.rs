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

#[derive(Debug, Serialize)]
pub struct ReadinessResult {
    pub ready: bool,
    pub status: String,
    pub checks: Vec<HealthCheck>,
}

#[derive(Debug, Serialize)]
pub struct LivenessResult {
    pub alive: bool,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: String,
    pub message: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_pagination_params_default() {
        let params: PaginationParams = serde_json::from_value(json!({})).unwrap();
        assert_eq!(params.limit, 100);
        assert_eq!(params.offset, 0);
    }

    #[test]
    fn test_pagination_params_custom() {
        let params: PaginationParams = serde_json::from_value(json!({
            "limit": 50,
            "offset": 10
        }))
        .unwrap();
        assert_eq!(params.limit, 50);
        assert_eq!(params.offset, 10);
    }

    #[test]
    fn test_health_result_serialize() {
        let result = HealthResult {
            status: "healthy".to_string(),
            session_count: 5,
            uptime_seconds: 3600,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["session_count"], 5);
        assert_eq!(json["uptime_seconds"], 3600);
    }

    #[test]
    fn test_define_table_params_deserialize() {
        let params: DefineTableParams = serde_json::from_value(json!({
            "sessionId": "abc-123",
            "name": "my_table",
            "sql": "SELECT 1"
        }))
        .unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.name, "my_table");
        assert_eq!(params.sql, "SELECT 1");
    }

    #[test]
    fn test_define_table_result_serialize() {
        let result = DefineTableResult {
            success: true,
            dependencies: vec!["table1".to_string(), "table2".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["dependencies"], json!(["table1", "table2"]));
    }

    #[test]
    fn test_table_definition_deserialize() {
        let def: TableDefinition = serde_json::from_value(json!({
            "name": "orders",
            "sql": "SELECT * FROM users"
        }))
        .unwrap();
        assert_eq!(def.name, "orders");
        assert_eq!(def.sql, "SELECT * FROM users");
    }

    #[test]
    fn test_define_tables_params_deserialize() {
        let params: DefineTablesParams = serde_json::from_value(json!({
            "sessionId": "session-1",
            "tables": [
                {"name": "t1", "sql": "SELECT 1"},
                {"name": "t2", "sql": "SELECT 2"}
            ]
        }))
        .unwrap();
        assert_eq!(params.session_id, "session-1");
        assert_eq!(params.tables.len(), 2);
    }

    #[test]
    fn test_execute_params_deserialize() {
        let params: ExecuteParams = serde_json::from_value(json!({
            "sessionId": "sess-1",
            "tables": ["a", "b"],
            "force": true
        }))
        .unwrap();
        assert_eq!(params.session_id, "sess-1");
        assert_eq!(params.tables, Some(vec!["a".to_string(), "b".to_string()]));
        assert!(params.force);
    }

    #[test]
    fn test_execute_params_optional_fields() {
        let params: ExecuteParams = serde_json::from_value(json!({
            "sessionId": "sess-1"
        }))
        .unwrap();
        assert!(params.tables.is_none());
        assert!(!params.force);
    }

    #[test]
    fn test_table_error_serialize() {
        let err = TableError {
            table: "bad_table".to_string(),
            error: "syntax error".to_string(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["table"], "bad_table");
        assert_eq!(json["error"], "syntax error");
    }

    #[test]
    fn test_execute_result_serialize() {
        let result = ExecuteResult {
            success: false,
            succeeded: vec!["a".to_string()],
            failed: vec![TableError {
                table: "b".to_string(),
                error: "failed".to_string(),
            }],
            skipped: vec!["c".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], false);
        assert_eq!(json["succeeded"], json!(["a"]));
        assert_eq!(json["skipped"], json!(["c"]));
    }

    #[test]
    fn test_list_tables_params_deserialize() {
        let params: ListTablesParams = serde_json::from_value(json!({
            "sessionId": "s1",
            "limit": 50,
            "offset": 10
        }))
        .unwrap();
        assert_eq!(params.session_id, "s1");
        assert_eq!(params.limit, Some(50));
        assert_eq!(params.offset, Some(10));
    }

    #[test]
    fn test_drop_table_params_deserialize() {
        let params: DropTableParams = serde_json::from_value(json!({
            "sessionId": "s1",
            "name": "old_table"
        }))
        .unwrap();
        assert_eq!(params.session_id, "s1");
        assert_eq!(params.name, "old_table");
    }

    #[test]
    fn test_drop_all_tables_params_deserialize() {
        let params: DropAllTablesParams = serde_json::from_value(json!({
            "sessionId": "s1"
        }))
        .unwrap();
        assert_eq!(params.session_id, "s1");
    }

    #[test]
    fn test_load_directory_params_deserialize() {
        let params: LoadDirectoryParams = serde_json::from_value(json!({
            "sessionId": "s1",
            "path": "/data/tables"
        }))
        .unwrap();
        assert_eq!(params.session_id, "s1");
        assert_eq!(params.path, "/data/tables");
    }

    #[test]
    fn test_loaded_table_info_serialize() {
        let info = LoadedTableInfo {
            name: "users".to_string(),
            kind: "parquet".to_string(),
            dependencies: vec![],
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["name"], "users");
        assert_eq!(json["kind"], "parquet");
    }

    #[test]
    fn test_load_directory_result_serialize() {
        let result = LoadDirectoryResult {
            success: true,
            tables: vec![LoadedTableInfo {
                name: "t1".to_string(),
                kind: "sql".to_string(),
                dependencies: vec!["t2".to_string()],
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["tables"][0]["name"], "t1");
    }

    #[test]
    fn test_readiness_result_serialize() {
        let result = ReadinessResult {
            ready: true,
            status: "ready".to_string(),
            checks: vec![
                HealthCheck {
                    name: "executor".to_string(),
                    status: "pass".to_string(),
                    message: None,
                },
                HealthCheck {
                    name: "sessions".to_string(),
                    status: "pass".to_string(),
                    message: Some("5 active".to_string()),
                },
            ],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["ready"], true);
        assert_eq!(json["status"], "ready");
        assert_eq!(json["checks"][0]["name"], "executor");
        assert_eq!(json["checks"][0]["status"], "pass");
        assert_eq!(json["checks"][1]["message"], "5 active");
    }

    #[test]
    fn test_readiness_result_not_ready() {
        let result = ReadinessResult {
            ready: false,
            status: "not_ready".to_string(),
            checks: vec![HealthCheck {
                name: "executor".to_string(),
                status: "fail".to_string(),
                message: Some("Connection failed".to_string()),
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["ready"], false);
        assert_eq!(json["status"], "not_ready");
        assert_eq!(json["checks"][0]["status"], "fail");
    }

    #[test]
    fn test_liveness_result_serialize() {
        let result = LivenessResult {
            alive: true,
            uptime_seconds: 3600,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["alive"], true);
        assert_eq!(json["uptime_seconds"], 3600);
    }

    #[test]
    fn test_health_check_clone() {
        let check = HealthCheck {
            name: "test".to_string(),
            status: "pass".to_string(),
            message: Some("ok".to_string()),
        };
        let cloned = check.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.status, "pass");
        assert_eq!(cloned.message, Some("ok".to_string()));
    }
}
