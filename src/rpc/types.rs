use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct RpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcResponse {
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }

    pub fn parse_error() -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32700,
                message: "Parse error".to_string(),
                data: None,
            }),
            id: Value::Null,
        }
    }

    pub fn invalid_request() -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32600,
                message: "Invalid Request".to_string(),
                data: None,
            }),
            id: Value::Null,
        }
    }

    pub fn method_not_found(id: Value, method: &str) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32601,
                message: format!("Method not found: {}", method),
                data: None,
            }),
            id,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PingResult {
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct CreateSessionResult {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Deserialize)]
pub struct DestroySessionParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct DestroySessionResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
        }
    }

    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, "INT64")
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, "STRING")
    }

    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, "FLOAT64")
    }

    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, "BOOLEAN")
    }

    pub fn date(name: impl Into<String>) -> Self {
        Self::new(name, "DATE")
    }

    pub fn timestamp(name: impl Into<String>) -> Self {
        Self::new(name, "TIMESTAMP")
    }

    pub fn numeric(name: impl Into<String>) -> Self {
        Self::new(name, "NUMERIC")
    }

    pub fn bytes(name: impl Into<String>) -> Self {
        Self::new(name, "BYTES")
    }
}

impl From<(String, String)> for ColumnDef {
    fn from((name, column_type): (String, String)) -> Self {
        Self { name, column_type }
    }
}

impl From<(&str, &str)> for ColumnDef {
    fn from((name, column_type): (&str, &str)) -> Self {
        Self {
            name: name.to_string(),
            column_type: column_type.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CreateTableResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct InsertParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct InsertResult {
    #[serde(rename = "insertedRows")]
    pub inserted_rows: u64,
}

#[derive(Debug, Deserialize)]
pub struct RegisterDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub tables: Vec<DagTableDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DagTableDef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<ColumnDef>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct RegisterDagResult {
    pub success: bool,
    pub tables: Vec<DagTableInfo>,
}

#[derive(Debug, Serialize)]
pub struct DagTableInfo {
    pub name: String,
    pub dependencies: Vec<String>,
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
    pub failed_tables: Vec<TableErrorInfo>,
    #[serde(rename = "skippedTables")]
    pub skipped_tables: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TableErrorInfo {
    pub table: String,
    pub error: String,
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

#[derive(Debug, Serialize)]
pub struct DagTableDetail {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(rename = "isSource")]
    pub is_source: bool,
    pub dependencies: Vec<String>,
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
pub struct LoadParquetParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub path: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Serialize)]
pub struct LoadParquetResult {
    pub success: bool,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResult {
    pub tables: Vec<TableInfo>,
}

#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub name: String,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct DescribeTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
}

#[derive(Debug, Serialize)]
pub struct DescribeTableResult {
    pub name: String,
    pub schema: Vec<ColumnDef>,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct SetDefaultProjectParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub project: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SetDefaultProjectResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct GetDefaultProjectParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct GetDefaultProjectResult {
    pub project: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetProjectsParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct GetProjectsResult {
    pub projects: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetDatasetsParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub project: String,
}

#[derive(Debug, Serialize)]
pub struct GetDatasetsResult {
    pub datasets: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetTablesInDatasetParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub project: String,
    pub dataset: String,
}

#[derive(Debug, Serialize)]
pub struct GetTablesInDatasetResult {
    pub tables: Vec<String>,
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

#[derive(Debug, Clone, Serialize)]
pub struct SqlTableInfo {
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub path: String,
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

#[derive(Debug, Clone, Serialize)]
pub struct ParquetTableInfo {
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub path: String,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
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
    fn test_rpc_request_deserialization() {
        let json_str = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{},"id":1}"#;
        let req: RpcRequest = serde_json::from_str(json_str).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "bq.ping");
        assert_eq!(req.id, Some(json!(1)));
    }

    #[test]
    fn test_rpc_request_without_id() {
        let json_str = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{}}"#;
        let req: RpcRequest = serde_json::from_str(json_str).unwrap();
        assert!(req.id.is_none());
    }

    #[test]
    fn test_rpc_request_without_params() {
        let json_str = r#"{"jsonrpc":"2.0","method":"bq.ping"}"#;
        let req: RpcRequest = serde_json::from_str(json_str).unwrap();
        assert_eq!(req.params, Value::Null);
    }

    #[test]
    fn test_rpc_response_success() {
        let resp = RpcResponse::success(json!(1), json!({"message": "pong"}));
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
        assert_eq!(resp.id, json!(1));
    }

    #[test]
    fn test_rpc_response_success_serialization() {
        let resp = RpcResponse::success(json!(1), json!({"message": "pong"}));
        let serialized = serde_json::to_string(&resp).unwrap();
        assert!(serialized.contains("\"jsonrpc\":\"2.0\""));
        assert!(serialized.contains("\"result\""));
        assert!(!serialized.contains("\"error\""));
    }

    #[test]
    fn test_rpc_response_error() {
        let resp = RpcResponse::error(json!(1), -32600, "Invalid Request".to_string());
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_none());
        assert!(resp.error.is_some());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "Invalid Request");
        assert!(err.data.is_none());
    }

    #[test]
    fn test_rpc_response_error_serialization() {
        let resp = RpcResponse::error(json!(1), -32600, "Invalid Request".to_string());
        let serialized = serde_json::to_string(&resp).unwrap();
        assert!(serialized.contains("\"error\""));
        assert!(!serialized.contains("\"result\""));
        assert!(!serialized.contains("\"data\""));
    }

    #[test]
    fn test_rpc_response_parse_error() {
        let resp = RpcResponse::parse_error();
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32700);
        assert_eq!(err.message, "Parse error");
        assert_eq!(resp.id, Value::Null);
    }

    #[test]
    fn test_rpc_response_invalid_request() {
        let resp = RpcResponse::invalid_request();
        assert_eq!(resp.jsonrpc, "2.0");
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "Invalid Request");
        assert_eq!(resp.id, Value::Null);
    }

    #[test]
    fn test_rpc_response_method_not_found() {
        let resp = RpcResponse::method_not_found(json!(42), "unknown.method");
        assert_eq!(resp.jsonrpc, "2.0");
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.message, "Method not found: unknown.method");
        assert_eq!(resp.id, json!(42));
    }

    #[test]
    fn test_ping_result_serialization() {
        let result = PingResult {
            message: "pong".to_string(),
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["message"], "pong");
    }

    #[test]
    fn test_create_session_result_serialization() {
        let result = CreateSessionResult {
            session_id: "abc-123".to_string(),
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["sessionId"], "abc-123");
    }

    #[test]
    fn test_destroy_session_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: DestroySessionParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
    }

    #[test]
    fn test_destroy_session_result_serialization() {
        let result = DestroySessionResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_query_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","sql":"SELECT 1"}"#;
        let params: QueryParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.sql, "SELECT 1");
    }

    #[test]
    fn test_create_table_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","tableName":"test_table","schema":[{"name":"id","type":"INT64"}]}"#;
        let params: CreateTableParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.table_name, "test_table");
        assert_eq!(params.schema.len(), 1);
        assert_eq!(params.schema[0].name, "id");
        assert_eq!(params.schema[0].column_type, "INT64");
    }

    #[test]
    fn test_column_def_new() {
        let col = ColumnDef::new("id", "INT64");
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, "INT64");
    }

    #[test]
    fn test_column_def_int64() {
        let col = ColumnDef::int64("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, "INT64");
    }

    #[test]
    fn test_column_def_string() {
        let col = ColumnDef::string("name");
        assert_eq!(col.name, "name");
        assert_eq!(col.column_type, "STRING");
    }

    #[test]
    fn test_column_def_float64() {
        let col = ColumnDef::float64("price");
        assert_eq!(col.name, "price");
        assert_eq!(col.column_type, "FLOAT64");
    }

    #[test]
    fn test_column_def_bool() {
        let col = ColumnDef::bool("active");
        assert_eq!(col.name, "active");
        assert_eq!(col.column_type, "BOOLEAN");
    }

    #[test]
    fn test_column_def_date() {
        let col = ColumnDef::date("created_at");
        assert_eq!(col.name, "created_at");
        assert_eq!(col.column_type, "DATE");
    }

    #[test]
    fn test_column_def_timestamp() {
        let col = ColumnDef::timestamp("updated_at");
        assert_eq!(col.name, "updated_at");
        assert_eq!(col.column_type, "TIMESTAMP");
    }

    #[test]
    fn test_column_def_numeric() {
        let col = ColumnDef::numeric("amount");
        assert_eq!(col.name, "amount");
        assert_eq!(col.column_type, "NUMERIC");
    }

    #[test]
    fn test_column_def_bytes() {
        let col = ColumnDef::bytes("data");
        assert_eq!(col.name, "data");
        assert_eq!(col.column_type, "BYTES");
    }

    #[test]
    fn test_column_def_from_string_tuple() {
        let col: ColumnDef = ("name".to_string(), "STRING".to_string()).into();
        assert_eq!(col.name, "name");
        assert_eq!(col.column_type, "STRING");
    }

    #[test]
    fn test_column_def_from_str_tuple() {
        let col: ColumnDef = ("id", "INT64").into();
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, "INT64");
    }

    #[test]
    fn test_column_def_serialization() {
        let col = ColumnDef::int64("id");
        let json = serde_json::to_value(&col).unwrap();
        assert_eq!(json["name"], "id");
        assert_eq!(json["type"], "INT64");
    }

    #[test]
    fn test_create_table_result_serialization() {
        let result = CreateTableResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_insert_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","tableName":"test_table","rows":[[1,"a"],[2,"b"]]}"#;
        let params: InsertParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.table_name, "test_table");
        assert_eq!(params.rows.len(), 2);
    }

    #[test]
    fn test_insert_result_serialization() {
        let result = InsertResult { inserted_rows: 10 };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["insertedRows"], 10);
    }

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
        let json_str = r#"{"name":"source","schema":[{"name":"id","type":"INT64"}],"rows":[[1],[2]]}"#;
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
        assert_eq!(params.table_names, Some(vec!["t1".to_string(), "t2".to_string()]));
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
            failed_tables: vec![TableErrorInfo {
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
    fn test_load_parquet_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","tableName":"t1","path":"/tmp/t.parquet","schema":[{"name":"id","type":"INT64"}]}"#;
        let params: LoadParquetParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.table_name, "t1");
        assert_eq!(params.path, "/tmp/t.parquet");
        assert_eq!(params.schema.len(), 1);
    }

    #[test]
    fn test_load_parquet_result_serialization() {
        let result = LoadParquetResult {
            success: true,
            row_count: 100,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["rowCount"], 100);
    }

    #[test]
    fn test_list_tables_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: ListTablesParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
    }

    #[test]
    fn test_list_tables_result_serialization() {
        let result = ListTablesResult {
            tables: vec![TableInfo {
                name: "t1".to_string(),
                row_count: 50,
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["tables"][0]["name"], "t1");
        assert_eq!(json["tables"][0]["rowCount"], 50);
    }

    #[test]
    fn test_describe_table_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","tableName":"my_table"}"#;
        let params: DescribeTableParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.table_name, "my_table");
    }

    #[test]
    fn test_describe_table_result_serialization() {
        let result = DescribeTableResult {
            name: "my_table".to_string(),
            schema: vec![ColumnDef::int64("id")],
            row_count: 100,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["name"], "my_table");
        assert_eq!(json["schema"][0]["name"], "id");
        assert_eq!(json["rowCount"], 100);
    }

    #[test]
    fn test_set_default_project_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","project":"my-project"}"#;
        let params: SetDefaultProjectParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.project, Some("my-project".to_string()));
    }

    #[test]
    fn test_set_default_project_params_null_project() {
        let json_str = r#"{"sessionId":"abc","project":null}"#;
        let params: SetDefaultProjectParams = serde_json::from_str(json_str).unwrap();
        assert!(params.project.is_none());
    }

    #[test]
    fn test_set_default_project_result_serialization() {
        let result = SetDefaultProjectResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_get_default_project_params_deserialization() {
        let json_str = r#"{"sessionId":"abc"}"#;
        let params: GetDefaultProjectParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
    }

    #[test]
    fn test_get_default_project_result_serialization() {
        let result = GetDefaultProjectResult {
            project: Some("my-proj".to_string()),
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["project"], "my-proj");
    }

    #[test]
    fn test_get_projects_params_deserialization() {
        let json_str = r#"{"sessionId":"abc"}"#;
        let params: GetProjectsParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
    }

    #[test]
    fn test_get_projects_result_serialization() {
        let result = GetProjectsResult {
            projects: vec!["p1".to_string(), "p2".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["projects"], json!(["p1", "p2"]));
    }

    #[test]
    fn test_get_datasets_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","project":"my-proj"}"#;
        let params: GetDatasetsParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.project, "my-proj");
    }

    #[test]
    fn test_get_datasets_result_serialization() {
        let result = GetDatasetsResult {
            datasets: vec!["d1".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["datasets"], json!(["d1"]));
    }

    #[test]
    fn test_get_tables_in_dataset_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","project":"proj","dataset":"ds"}"#;
        let params: GetTablesInDatasetParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.project, "proj");
        assert_eq!(params.dataset, "ds");
    }

    #[test]
    fn test_get_tables_in_dataset_result_serialization() {
        let result = GetTablesInDatasetResult {
            tables: vec!["t1".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["tables"], json!(["t1"]));
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
    fn test_table_error_info_serialization() {
        let info = TableErrorInfo {
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
