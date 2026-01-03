use serde::{Deserialize, Serialize};

pub use crate::domain::{
    DagTableDef, DagTableDetail, DagTableInfo, ParquetTableInfo, SqlTableInfo, TableError,
};

#[derive(Debug, Deserialize)]
pub struct RegisterDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub tables: Vec<DagTableDef>,
}

#[derive(Debug, Serialize)]
pub struct RegisterDagResult {
    pub success: bool,
    pub tables: Vec<DagTableInfo>,
}

#[derive(Debug, Deserialize)]
pub struct RunDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableNames")]
    pub table_names: Option<Vec<String>>,
    #[serde(rename = "retryCount", default)]
    pub retry_count: u32,
}

#[derive(Debug, Serialize)]
pub struct RunDagResult {
    pub success: bool,
    #[serde(rename = "succeededTables")]
    pub succeeded_tables: Vec<String>,
    #[serde(rename = "failedTables")]
    pub failed_tables: Vec<TableError>,
    #[serde(rename = "skippedTables")]
    pub skipped_tables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RetryDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "failedTables")]
    pub failed_tables: Vec<String>,
    #[serde(rename = "skippedTables")]
    pub skipped_tables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct GetDagResult {
    pub tables: Vec<DagTableDetail>,
}

#[derive(Debug, Deserialize)]
pub struct ClearDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct ClearDagResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct LoadSqlDirectoryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "rootPath")]
    pub root_path: String,
}

#[derive(Debug, Serialize)]
pub struct LoadSqlDirectoryResult {
    pub success: bool,
    #[serde(rename = "tablesLoaded")]
    pub tables_loaded: Vec<SqlTableInfo>,
}

#[derive(Debug, Deserialize)]
pub struct LoadParquetDirectoryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "rootPath")]
    pub root_path: String,
}

#[derive(Debug, Serialize)]
pub struct LoadParquetDirectoryResult {
    pub success: bool,
    #[serde(rename = "tablesLoaded")]
    pub tables_loaded: Vec<ParquetTableInfo>,
}

#[derive(Debug, Deserialize)]
pub struct LoadDagFromDirectoryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "rootPath")]
    pub root_path: String,
}

#[derive(Debug, Serialize)]
pub struct LoadDagFromDirectoryResult {
    pub success: bool,
    #[serde(rename = "sourceTables")]
    pub source_tables: Vec<ParquetTableInfo>,
    #[serde(rename = "computedTables")]
    pub computed_tables: Vec<SqlTableInfo>,
    #[serde(rename = "dagInfo")]
    pub dag_info: Vec<DagTableInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_register_dag_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","tables":[{"name":"t1","sql":"SELECT 1"}]}"#;
        let params: RegisterDagParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.tables.len(), 1);
        assert_eq!(params.tables[0].name, "t1");
        assert_eq!(params.tables[0].sql, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_dag_table_def_deserialization_with_schema_and_rows() {
        let json_str =
            r#"{"name":"source","schema":[{"name":"id","type":"INT64"}],"rows":[[1],[2]]}"#;
        let def: DagTableDef = serde_json::from_str(json_str).unwrap();
        assert_eq!(def.name, "source");
        assert!(def.sql.is_none());
        assert!(def.schema.is_some());
        assert_eq!(def.rows.len(), 2);
    }

    #[test]
    fn test_dag_table_def_serialization_skip_empty() {
        let def = DagTableDef {
            name: "t1".to_string(),
            sql: Some("SELECT 1".to_string()),
            schema: None,
            rows: vec![],
        };
        let json = serde_json::to_value(&def).unwrap();
        assert!(!json.as_object().unwrap().contains_key("schema"));
        assert!(!json.as_object().unwrap().contains_key("rows"));
    }

    #[test]
    fn test_register_dag_result_serialization() {
        let result = RegisterDagResult {
            success: true,
            tables: vec![DagTableInfo {
                name: "t1".to_string(),
                dependencies: vec!["t0".to_string()],
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["tables"][0]["name"], "t1");
        assert_eq!(json["tables"][0]["dependencies"][0], "t0");
    }

    #[test]
    fn test_run_dag_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","tableNames":["t1","t2"],"retryCount":3}"#;
        let params: RunDagParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(
            params.table_names,
            Some(vec!["t1".to_string(), "t2".to_string()])
        );
        assert_eq!(params.retry_count, 3);
    }

    #[test]
    fn test_run_dag_params_default_retry_count() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: RunDagParams = serde_json::from_str(json_str).unwrap();
        assert!(params.table_names.is_none());
        assert_eq!(params.retry_count, 0);
    }

    #[test]
    fn test_run_dag_result_serialization() {
        let result = RunDagResult {
            success: false,
            succeeded_tables: vec!["t1".to_string()],
            failed_tables: vec![TableError {
                table: "t2".to_string(),
                error: "error msg".to_string(),
            }],
            skipped_tables: vec!["t3".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], false);
        assert_eq!(json["succeededTables"][0], "t1");
        assert_eq!(json["failedTables"][0]["table"], "t2");
        assert_eq!(json["failedTables"][0]["error"], "error msg");
        assert_eq!(json["skippedTables"][0], "t3");
    }

    #[test]
    fn test_retry_dag_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","failedTables":["f1"],"skippedTables":["s1"]}"#;
        let params: RetryDagParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.failed_tables, vec!["f1"]);
        assert_eq!(params.skipped_tables, vec!["s1"]);
    }

    #[test]
    fn test_get_dag_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: GetDagParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
    }

    #[test]
    fn test_get_dag_result_serialization() {
        let result = GetDagResult {
            tables: vec![DagTableDetail {
                name: "t1".to_string(),
                sql: Some("SELECT 1".to_string()),
                is_source: false,
                dependencies: vec!["t0".to_string()],
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["tables"][0]["name"], "t1");
        assert_eq!(json["tables"][0]["sql"], "SELECT 1");
        assert_eq!(json["tables"][0]["isSource"], false);
    }

    #[test]
    fn test_dag_table_detail_no_sql_serialization() {
        let detail = DagTableDetail {
            name: "source".to_string(),
            sql: None,
            is_source: true,
            dependencies: vec![],
        };
        let json = serde_json::to_value(&detail).unwrap();
        assert!(!json.as_object().unwrap().contains_key("sql"));
        assert_eq!(json["isSource"], true);
    }

    #[test]
    fn test_clear_dag_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: ClearDagParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
    }

    #[test]
    fn test_clear_dag_result_serialization() {
        let result = ClearDagResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_load_sql_directory_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","rootPath":"/tmp/sql"}"#;
        let params: LoadSqlDirectoryParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.root_path, "/tmp/sql");
    }

    #[test]
    fn test_load_sql_directory_result_serialization() {
        let result = LoadSqlDirectoryResult {
            success: true,
            tables_loaded: vec![SqlTableInfo {
                project: "p".to_string(),
                dataset: "d".to_string(),
                table: "t".to_string(),
                path: "/tmp/p/d/t.sql".to_string(),
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["tablesLoaded"][0]["project"], "p");
        assert_eq!(json["tablesLoaded"][0]["dataset"], "d");
        assert_eq!(json["tablesLoaded"][0]["table"], "t");
        assert_eq!(json["tablesLoaded"][0]["path"], "/tmp/p/d/t.sql");
    }

    #[test]
    fn test_load_parquet_directory_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","rootPath":"/tmp/pq"}"#;
        let params: LoadParquetDirectoryParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.root_path, "/tmp/pq");
    }

    #[test]
    fn test_load_parquet_directory_result_serialization() {
        let result = LoadParquetDirectoryResult {
            success: true,
            tables_loaded: vec![ParquetTableInfo {
                project: "p".to_string(),
                dataset: "d".to_string(),
                table: "t".to_string(),
                path: "/tmp/p/d/t.parquet".to_string(),
                row_count: 200,
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["tablesLoaded"][0]["project"], "p");
        assert_eq!(json["tablesLoaded"][0]["rowCount"], 200);
    }

    #[test]
    fn test_load_dag_from_directory_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","rootPath":"/tmp/dag"}"#;
        let params: LoadDagFromDirectoryParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.root_path, "/tmp/dag");
    }

    #[test]
    fn test_load_dag_from_directory_result_serialization() {
        let result = LoadDagFromDirectoryResult {
            success: true,
            source_tables: vec![],
            computed_tables: vec![],
            dag_info: vec![DagTableInfo {
                name: "t".to_string(),
                dependencies: vec![],
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["sourceTables"], json!([]));
        assert_eq!(json["computedTables"], json!([]));
        assert_eq!(json["dagInfo"][0]["name"], "t");
    }

    #[test]
    fn test_table_error_serialization() {
        let info = TableError {
            table: "failed_table".to_string(),
            error: "SQL error".to_string(),
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["table"], "failed_table");
        assert_eq!(json["error"], "SQL error");
    }

    #[test]
    fn test_dag_table_info_serialization() {
        let info = DagTableInfo {
            name: "my_table".to_string(),
            dependencies: vec!["dep1".to_string(), "dep2".to_string()],
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["name"], "my_table");
        assert_eq!(json["dependencies"], json!(["dep1", "dep2"]));
    }
}
