use std::sync::Arc;

use bq_runner_macros::rpc;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::session::SessionManager;
use crate::utils::json_to_sql_value;

use super::types::{
    ClearDagParams, ClearDagResult, ColumnDef, CreateSessionResult, CreateTableParams,
    CreateTableResult, DescribeTableParams, DescribeTableResult, DestroySessionParams,
    DestroySessionResult, GetDagParams, GetDagResult, GetDatasetsParams, GetDatasetsResult,
    GetDefaultProjectParams, GetDefaultProjectResult, GetProjectsParams, GetProjectsResult,
    GetTablesInDatasetParams, GetTablesInDatasetResult, InsertParams, InsertResult,
    ListTablesParams, ListTablesResult, LoadDagFromDirectoryParams, LoadDagFromDirectoryResult,
    LoadParquetDirectoryParams, LoadParquetDirectoryResult, LoadParquetParams, LoadParquetResult,
    LoadSqlDirectoryParams, LoadSqlDirectoryResult, PingResult, QueryParams, RegisterDagParams,
    RegisterDagResult, RetryDagParams, RunDagParams, RunDagResult, SetDefaultProjectParams,
    SetDefaultProjectResult, TableErrorInfo, TableInfo,
};

pub struct RpcMethods {
    session_manager: Arc<SessionManager>,
}

impl RpcMethods {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }

    pub async fn dispatch(&self, method: &str, params: Value) -> Result<Value> {
        match method {
            "bq.ping" => self.ping(params).await,
            "bq.createSession" => self.create_session(params).await,
            "bq.destroySession" => self.destroy_session(params).await,
            "bq.query" => self.query(params).await,
            "bq.createTable" => self.create_table(params).await,
            "bq.insert" => self.insert(params).await,
            "bq.registerDag" => self.register_dag(params).await,
            "bq.runDag" => self.run_dag(params).await,
            "bq.retryDag" => self.retry_dag(params).await,
            "bq.getDag" => self.get_dag(params).await,
            "bq.clearDag" => self.clear_dag(params).await,
            "bq.loadParquet" => self.load_parquet(params).await,
            "bq.listTables" => self.list_tables(params).await,
            "bq.describeTable" => self.describe_table(params).await,
            "bq.setDefaultProject" => self.set_default_project(params).await,
            "bq.getDefaultProject" => self.get_default_project(params).await,
            "bq.getProjects" => self.get_projects(params).await,
            "bq.getDatasets" => self.get_datasets(params).await,
            "bq.getTablesInDataset" => self.get_tables_in_dataset(params).await,
            "bq.loadSqlDirectory" => self.load_sql_directory(params).await,
            "bq.loadParquetDirectory" => self.load_parquet_directory(params).await,
            "bq.loadDagFromDirectory" => self.load_dag_from_directory(params).await,
            _ => Err(Error::InvalidRequest(format!("Unknown method: {}", method))),
        }
    }

    #[rpc]
    fn ping() -> PingResult {
        PingResult { message: "pong".to_string() }
    }

    async fn create_session(&self, _params: Value) -> Result<Value> {
        let session_id = self.session_manager.create_session().await?;

        Ok(json!(CreateSessionResult {
            session_id: session_id.to_string(),
        }))
    }

    #[rpc(session)]
    fn destroy_session(p: DestroySessionParams) -> DestroySessionResult {
        sm.destroy_session(session_id)?;
        DestroySessionResult { success: true }
    }

    async fn query(&self, params: Value) -> Result<Value> {
        let p: QueryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let result = self
            .session_manager
            .execute_query(session_id, &p.sql)
            .await?;

        Ok(result.to_bq_response())
    }

    async fn create_table(&self, params: Value) -> Result<Value> {
        let p: CreateTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let columns: Vec<String> = p
            .schema
            .iter()
            .map(|col| format!("{} {}", col.name, col.column_type))
            .collect();

        let sql = format!("CREATE TABLE {} ({})", p.table_name, columns.join(", "));

        self.session_manager
            .execute_statement(session_id, &sql)
            .await?;

        Ok(json!(CreateTableResult { success: true }))
    }

    async fn insert(&self, params: Value) -> Result<Value> {
        let p: InsertParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        if p.rows.is_empty() {
            return Ok(json!(InsertResult { inserted_rows: 0 }));
        }

        let values: Vec<String> = p
            .rows
            .iter()
            .filter_map(|row| {
                if let Value::Array(arr) = row {
                    let vals: Vec<String> = arr.iter().map(json_to_sql_value).collect();
                    Some(format!("({})", vals.join(", ")))
                } else if let Value::Object(obj) = row {
                    let vals: Vec<String> = obj.values().map(json_to_sql_value).collect();
                    Some(format!("({})", vals.join(", ")))
                } else {
                    None
                }
            })
            .collect();

        let row_count = values.len() as u64;
        let sql = format!("INSERT INTO {} VALUES {}", p.table_name, values.join(", "));

        self.session_manager
            .execute_statement(session_id, &sql)
            .await?;

        Ok(json!(InsertResult {
            inserted_rows: row_count,
        }))
    }

    async fn register_dag(&self, params: Value) -> Result<Value> {
        let p: RegisterDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let table_infos = self.session_manager.register_dag(session_id, p.tables)?;

        Ok(json!(RegisterDagResult {
            success: true,
            tables: table_infos,
        }))
    }

    async fn run_dag(&self, params: Value) -> Result<Value> {
        let p: RunDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let targets = p.table_names;
        let retry_count = p.retry_count;

        let result = self
            .session_manager
            .run_dag(session_id, targets, retry_count)
            .await?;

        Ok(json!(RunDagResult {
            success: result.all_succeeded(),
            succeeded_tables: result.succeeded,
            failed_tables: result
                .failed
                .into_iter()
                .map(|e| TableErrorInfo {
                    table: e.table,
                    error: e.error,
                })
                .collect(),
            skipped_tables: result.skipped,
        }))
    }

    async fn retry_dag(&self, params: Value) -> Result<Value> {
        let p: RetryDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let failed_tables = p.failed_tables;
        let skipped_tables = p.skipped_tables;

        let result = self
            .session_manager
            .retry_dag(session_id, failed_tables, skipped_tables)
            .await?;

        Ok(json!(RunDagResult {
            success: result.all_succeeded(),
            succeeded_tables: result.succeeded,
            failed_tables: result
                .failed
                .into_iter()
                .map(|e| TableErrorInfo {
                    table: e.table,
                    error: e.error,
                })
                .collect(),
            skipped_tables: result.skipped,
        }))
    }

    #[rpc(session)]
    fn get_dag(p: GetDagParams) -> GetDagResult {
        let tables = sm.get_dag(session_id)?;
        GetDagResult { tables }
    }

    async fn clear_dag(&self, params: Value) -> Result<Value> {
        let p: ClearDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        self.session_manager.clear_dag(session_id).await?;

        Ok(json!(ClearDagResult { success: true }))
    }

    async fn load_parquet(&self, params: Value) -> Result<Value> {
        let p: LoadParquetParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let row_count = self
            .session_manager
            .load_parquet(session_id, &p.table_name, &p.path, &p.schema)
            .await?;

        Ok(json!(LoadParquetResult {
            success: true,
            row_count,
        }))
    }

    async fn list_tables(&self, params: Value) -> Result<Value> {
        let p: ListTablesParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let table_infos = self.session_manager.list_tables(session_id).await?;

        Ok(json!(ListTablesResult {
            tables: table_infos
                .into_iter()
                .map(|(name, row_count)| TableInfo { name, row_count })
                .collect(),
        }))
    }

    async fn describe_table(&self, params: Value) -> Result<Value> {
        let p: DescribeTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let (schema, row_count) = self
            .session_manager
            .describe_table(session_id, &p.table_name)
            .await?;

        Ok(json!(DescribeTableResult {
            name: p.table_name,
            schema: schema
                .into_iter()
                .map(|(name, col_type)| ColumnDef::from((name, col_type)))
                .collect(),
            row_count,
        }))
    }

    async fn set_default_project(&self, params: Value) -> Result<Value> {
        let p: SetDefaultProjectParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        self.session_manager
            .set_default_project(session_id, p.project)?;

        Ok(json!(SetDefaultProjectResult { success: true }))
    }

    #[rpc(session)]
    fn get_default_project(p: GetDefaultProjectParams) -> GetDefaultProjectResult {
        let project = sm.get_default_project(session_id)?;
        GetDefaultProjectResult { project }
    }

    #[rpc(session)]
    fn get_projects(p: GetProjectsParams) -> GetProjectsResult {
        let projects = sm.get_projects(session_id)?;
        GetProjectsResult { projects }
    }

    #[rpc(session)]
    fn get_datasets(p: GetDatasetsParams) -> GetDatasetsResult {
        let datasets = sm.get_datasets(session_id, &p.project)?;
        GetDatasetsResult { datasets }
    }

    #[rpc(session)]
    fn get_tables_in_dataset(p: GetTablesInDatasetParams) -> GetTablesInDatasetResult {
        let tables = sm.get_tables_in_dataset(session_id, &p.project, &p.dataset)?;
        GetTablesInDatasetResult { tables }
    }

    async fn load_sql_directory(&self, params: Value) -> Result<Value> {
        let p: LoadSqlDirectoryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let (_, table_infos) = self
            .session_manager
            .load_sql_directory(session_id, &p.root_path)?;

        Ok(json!(LoadSqlDirectoryResult {
            success: true,
            tables_loaded: table_infos,
        }))
    }

    async fn load_parquet_directory(&self, params: Value) -> Result<Value> {
        let p: LoadParquetDirectoryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let table_infos = self
            .session_manager
            .load_parquet_directory(session_id, &p.root_path)
            .await?;

        Ok(json!(LoadParquetDirectoryResult {
            success: true,
            tables_loaded: table_infos,
        }))
    }

    async fn load_dag_from_directory(&self, params: Value) -> Result<Value> {
        let p: LoadDagFromDirectoryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let (source_tables, computed_tables, dag_info) = self
            .session_manager
            .load_dag_from_directory(session_id, &p.root_path)
            .await?;

        Ok(json!(LoadDagFromDirectoryResult {
            success: true,
            source_tables,
            computed_tables,
            dag_info,
        }))
    }
}

fn parse_uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| Error::InvalidRequest(format!("Invalid session ID: {}", s)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::SessionManager;
    use serde_json::json;

    fn create_rpc_methods() -> RpcMethods {
        let session_manager = Arc::new(SessionManager::new());
        RpcMethods::new(session_manager)
    }

    async fn create_session_for_test(methods: &RpcMethods) -> String {
        let result = methods
            .dispatch("bq.createSession", json!({}))
            .await
            .unwrap();
        result["sessionId"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_new() {
        let session_manager = Arc::new(SessionManager::new());
        let _ = RpcMethods::new(session_manager);
    }

    #[tokio::test]
    async fn test_dispatch_ping() {
        let methods = create_rpc_methods();
        let result = methods.dispatch("bq.ping", json!({})).await.unwrap();
        assert_eq!(result["message"], "pong");
    }

    #[tokio::test]
    async fn test_dispatch_unknown_method() {
        let methods = create_rpc_methods();
        let err = methods
            .dispatch("unknown.method", json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn test_create_session() {
        let methods = create_rpc_methods();
        let result = methods
            .dispatch("bq.createSession", json!({}))
            .await
            .unwrap();
        assert!(result["sessionId"].is_string());
    }

    #[tokio::test]
    async fn test_destroy_session() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch("bq.destroySession", json!({"sessionId": session_id}))
            .await
            .unwrap();
        assert_eq!(result["success"], true);
    }

    #[tokio::test]
    async fn test_destroy_session_invalid_uuid() {
        let methods = create_rpc_methods();
        let err = methods
            .dispatch("bq.destroySession", json!({"sessionId": "not-a-uuid"}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn test_query() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.query",
                json!({"sessionId": session_id, "sql": "SELECT 1 AS value"}),
            )
            .await
            .unwrap();
        assert!(result["schema"].is_object() || result["rows"].is_array());
    }

    #[tokio::test]
    async fn test_create_table() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.createTable",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table",
                    "schema": [{"name": "id", "type": "INT64"}, {"name": "name", "type": "STRING"}]
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["success"], true);
    }

    #[tokio::test]
    async fn test_insert_empty_rows() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.insert",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table",
                    "rows": []
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["insertedRows"], 0);
    }

    #[tokio::test]
    async fn test_insert_with_array_rows() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods
            .dispatch(
                "bq.createTable",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table",
                    "schema": [{"name": "id", "type": "INT64"}, {"name": "name", "type": "STRING"}]
                }),
            )
            .await
            .unwrap();
        let result = methods
            .dispatch(
                "bq.insert",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table",
                    "rows": [[1, "Alice"], [2, "Bob"]]
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["insertedRows"], 2);
    }

    #[tokio::test]
    async fn test_insert_with_object_rows() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods
            .dispatch(
                "bq.createTable",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table2",
                    "schema": [{"name": "id", "type": "INT64"}, {"name": "name", "type": "STRING"}]
                }),
            )
            .await
            .unwrap();
        let result = methods
            .dispatch(
                "bq.insert",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table2",
                    "rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["insertedRows"], 2);
    }

    #[tokio::test]
    async fn test_insert_with_mixed_rows_filters_invalid() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods
            .dispatch(
                "bq.createTable",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table3",
                    "schema": [{"name": "id", "type": "INT64"}]
                }),
            )
            .await
            .unwrap();
        let result = methods
            .dispatch(
                "bq.insert",
                json!({
                    "sessionId": session_id,
                    "tableName": "test_table3",
                    "rows": [[1], "invalid", [2], null, [3]]
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["insertedRows"], 3);
    }

    #[tokio::test]
    async fn test_register_dag() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods.dispatch("bq.registerDag", json!({
            "sessionId": session_id,
            "tables": [
                {"name": "source", "schema": [{"name": "id", "type": "INT64"}], "rows": [[1], [2]]},
                {"name": "derived", "sql": "SELECT * FROM source"}
            ]
        })).await.unwrap();
        assert_eq!(result["success"], true);
        assert!(result["tables"].is_array());
    }

    #[tokio::test]
    async fn test_run_dag() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods.dispatch("bq.registerDag", json!({
            "sessionId": session_id,
            "tables": [
                {"name": "source", "schema": [{"name": "id", "type": "INT64"}], "rows": [[1], [2]]},
                {"name": "derived", "sql": "SELECT * FROM source"}
            ]
        })).await.unwrap();
        let result = methods
            .dispatch(
                "bq.runDag",
                json!({
                    "sessionId": session_id
                }),
            )
            .await
            .unwrap();
        assert!(result["success"].is_boolean());
        assert!(result["succeededTables"].is_array());
        assert!(result["failedTables"].is_array());
        assert!(result["skippedTables"].is_array());
    }

    #[tokio::test]
    async fn test_run_dag_with_targets() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods.dispatch("bq.registerDag", json!({
            "sessionId": session_id,
            "tables": [
                {"name": "source", "schema": [{"name": "id", "type": "INT64"}], "rows": [[1]]},
                {"name": "derived", "sql": "SELECT * FROM source"}
            ]
        })).await.unwrap();
        let result = methods
            .dispatch(
                "bq.runDag",
                json!({
                    "sessionId": session_id,
                    "tableNames": ["derived"],
                    "retryCount": 1
                }),
            )
            .await
            .unwrap();
        assert!(result["success"].is_boolean());
    }

    #[tokio::test]
    async fn test_retry_dag() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods.dispatch("bq.registerDag", json!({
            "sessionId": session_id,
            "tables": [
                {"name": "source", "schema": [{"name": "id", "type": "INT64"}], "rows": [[1]]}
            ]
        })).await.unwrap();
        let result = methods
            .dispatch(
                "bq.retryDag",
                json!({
                    "sessionId": session_id,
                    "failedTables": [],
                    "skippedTables": []
                }),
            )
            .await
            .unwrap();
        assert!(result["success"].is_boolean());
    }

    #[tokio::test]
    async fn test_get_dag() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods.dispatch("bq.registerDag", json!({
            "sessionId": session_id,
            "tables": [
                {"name": "source", "schema": [{"name": "id", "type": "INT64"}], "rows": [[1]]}
            ]
        })).await.unwrap();
        let result = methods
            .dispatch(
                "bq.getDag",
                json!({
                    "sessionId": session_id
                }),
            )
            .await
            .unwrap();
        assert!(result["tables"].is_array());
    }

    #[test]
    fn test_clear_dag_params_parsing() {
        let params_json = r#"{"sessionId":"abc"}"#;
        let params: crate::rpc::types::ClearDagParams = serde_json::from_str(params_json).unwrap();
        assert_eq!(params.session_id, "abc");
    }

    #[tokio::test]
    async fn test_list_tables_params_valid() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.listTables",
                json!({
                    "sessionId": session_id
                }),
            )
            .await;
        assert!(result.is_err() || result.unwrap()["tables"].is_array());
    }

    #[tokio::test]
    async fn test_describe_table_params_valid() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.describeTable",
                json!({
                    "sessionId": session_id,
                    "tableName": "describe_me"
                }),
            )
            .await;
        assert!(result.is_err() || result.unwrap()["name"].is_string());
    }

    #[tokio::test]
    async fn test_set_and_get_default_project() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.setDefaultProject",
                json!({
                    "sessionId": session_id,
                    "project": "MY_PROJECT"
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["success"], true);
        let result = methods
            .dispatch(
                "bq.getDefaultProject",
                json!({
                    "sessionId": session_id
                }),
            )
            .await
            .unwrap();
        assert!(result["project"].is_string());
    }

    #[tokio::test]
    async fn test_set_default_project_to_none() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods
            .dispatch(
                "bq.setDefaultProject",
                json!({
                    "sessionId": session_id,
                    "project": "SOME_PROJECT"
                }),
            )
            .await
            .unwrap();
        let result = methods
            .dispatch(
                "bq.setDefaultProject",
                json!({
                    "sessionId": session_id,
                    "project": null
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["success"], true);
        let result = methods
            .dispatch(
                "bq.getDefaultProject",
                json!({
                    "sessionId": session_id
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["project"], Value::Null);
    }

    #[tokio::test]
    async fn test_get_projects() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.getProjects",
                json!({
                    "sessionId": session_id
                }),
            )
            .await
            .unwrap();
        assert!(result["projects"].is_array());
    }

    #[tokio::test]
    async fn test_get_datasets() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.getDatasets",
                json!({
                    "sessionId": session_id,
                    "project": "test-project"
                }),
            )
            .await
            .unwrap();
        assert!(result["datasets"].is_array());
    }

    #[tokio::test]
    async fn test_get_tables_in_dataset() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let result = methods
            .dispatch(
                "bq.getTablesInDataset",
                json!({
                    "sessionId": session_id,
                    "project": "test-project",
                    "dataset": "test-dataset"
                }),
            )
            .await
            .unwrap();
        assert!(result["tables"].is_array());
    }

    #[tokio::test]
    async fn test_parse_uuid_invalid() {
        let err = parse_uuid("not-a-uuid").unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn test_parse_uuid_valid() {
        let uuid = parse_uuid("00000000-0000-0000-0000-000000000000").unwrap();
        assert_eq!(uuid.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[tokio::test]
    async fn test_dispatch_all_methods() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        methods
            .dispatch(
                "bq.createTable",
                json!({
                    "sessionId": session_id,
                    "tableName": "t",
                    "schema": [{"name": "id", "type": "INT64"}]
                }),
            )
            .await
            .unwrap();
        let all_methods = vec![("bq.ping", json!({})), ("bq.createSession", json!({}))];
        for (method, params) in all_methods {
            let result = methods.dispatch(method, params).await;
            assert!(
                result.is_ok(),
                "Method {} failed: {:?}",
                method,
                result.err()
            );
        }
    }

    #[tokio::test]
    async fn test_load_sql_directory_not_found() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let err = methods
            .dispatch(
                "bq.loadSqlDirectory",
                json!({
                    "sessionId": session_id,
                    "rootPath": "/nonexistent/path"
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[tokio::test]
    async fn test_load_parquet_directory_not_found() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let err = methods
            .dispatch(
                "bq.loadParquetDirectory",
                json!({
                    "sessionId": session_id,
                    "rootPath": "/nonexistent/path"
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[tokio::test]
    async fn test_load_dag_from_directory_not_found() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let err = methods
            .dispatch(
                "bq.loadDagFromDirectory",
                json!({
                    "sessionId": session_id,
                    "rootPath": "/nonexistent/path"
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[tokio::test]
    async fn test_load_parquet_not_found() {
        let methods = create_rpc_methods();
        let session_id = create_session_for_test(&methods).await;
        let err = methods
            .dispatch(
                "bq.loadParquet",
                json!({
                    "sessionId": session_id,
                    "tableName": "t",
                    "path": "/nonexistent/file.parquet",
                    "schema": [{"name": "id", "type": "INT64"}]
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }
}
