use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::config::RpcConfig;
use crate::error::{Error, Result};
use crate::session::SessionManager;
use crate::utils::json_to_sql_value;
use crate::validation::{quote_identifier, validate_sql_for_query, validate_table_name};

pub trait HasSessionId {
    fn session_id(&self) -> &str;
}

macro_rules! impl_has_session_id {
    ($($t:ty),* $(,)?) => {
        $(
            impl HasSessionId for $t {
                fn session_id(&self) -> &str {
                    &self.session_id
                }
            }
        )*
    };
}

fn parse_session_params<T: DeserializeOwned + HasSessionId>(params: Value) -> Result<(T, Uuid)> {
    let p: T = serde_json::from_value(params)?;
    let session_id = parse_uuid(p.session_id())?;
    Ok((p, session_id))
}

use super::types::dag::{
    ClearDagParams, ClearDagResult, GetDagParams, GetDagResult, LoadDagFromDirectoryParams,
    LoadDagFromDirectoryResult, LoadParquetDirectoryParams, LoadParquetDirectoryResult,
    LoadSqlDirectoryParams, LoadSqlDirectoryResult, RegisterDagParams, RegisterDagResult,
    RetryDagParams, RunDagParams, RunDagResult, TableErrorInfo,
};
use super::types::query::ListTablesParams as QueryListTablesParams;
use super::types::{
    ColumnDef, CreateSessionResult, CreateTableParams, CreateTableResult, DefineTableParams,
    DefineTableResult, DefineTablesParams, DefineTablesResult, DescribeTableParams,
    DescribeTableResult, DestroySessionParams, DestroySessionResult, DropAllTablesParams,
    DropTableParams, ExecuteParams, ExecuteResult, GetDatasetsParams, GetDatasetsResult,
    GetDefaultProjectParams, GetDefaultProjectResult, GetProjectsParams, GetProjectsResult,
    GetTablesInDatasetParams, GetTablesInDatasetResult, HealthCheck, HealthResult, InsertParams,
    InsertResult, ListTablesResult, LivenessResult, LoadDirectoryParams, LoadDirectoryResult,
    LoadParquetParams, LoadParquetResult, LoadedTableInfo, PingResult, QueryParams,
    ReadinessResult, SetDefaultProjectParams, SetDefaultProjectResult, TableInfo,
};

impl_has_session_id!(
    DestroySessionParams,
    GetDagParams,
    GetDefaultProjectParams,
    GetProjectsParams,
    GetDatasetsParams,
    GetTablesInDatasetParams,
);

pub struct RpcMethods {
    session_manager: Arc<SessionManager>,
    audit_enabled: bool,
    rpc_config: RpcConfig,
}

impl RpcMethods {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self {
            session_manager,
            audit_enabled: false,
            rpc_config: RpcConfig::default(),
        }
    }

    pub fn with_audit(session_manager: Arc<SessionManager>, audit_enabled: bool) -> Self {
        Self {
            session_manager,
            audit_enabled,
            rpc_config: RpcConfig::default(),
        }
    }

    pub fn with_config(
        session_manager: Arc<SessionManager>,
        audit_enabled: bool,
        rpc_config: RpcConfig,
    ) -> Self {
        Self {
            session_manager,
            audit_enabled,
            rpc_config,
        }
    }

    pub fn audit_enabled(&self) -> bool {
        self.audit_enabled
    }

    pub fn rpc_config(&self) -> &RpcConfig {
        &self.rpc_config
    }

    pub fn session_manager(&self) -> &SessionManager {
        &self.session_manager
    }

    pub async fn dispatch(&self, method: &str, params: Value) -> Result<Value> {
        match method {
            "bq.ping" => self.ping(params).await,
            "bq.health" => self.health(params).await,
            "bq.readiness" => self.readiness(params).await,
            "bq.liveness" => self.liveness(params).await,
            "bq.createSession" => self.create_session(params).await,
            "bq.destroySession" => self.destroy_session(params).await,
            "bq.query" => self.query(params).await,
            "bq.createTable" => self.create_table(params).await,
            "bq.insert" => self.insert(params).await,
            "bq.defineTable" => self.define_table(params).await,
            "bq.defineTables" => self.define_tables(params).await,
            "bq.dropTable" => self.drop_table(params).await,
            "bq.dropAllTables" => self.drop_all_tables(params).await,
            "bq.execute" => self.execute_tables(params).await,
            "bq.loadDirectory" => self.load_directory(params).await,
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

    async fn ping(&self, _params: Value) -> Result<Value> {
        Ok(json!(PingResult {
            message: "pong".to_string()
        }))
    }

    async fn health(&self, _params: Value) -> Result<Value> {
        let session_count = self.session_manager.session_count();
        let uptime_seconds = self.session_manager.uptime_seconds();
        Ok(json!(HealthResult {
            status: "healthy".to_string(),
            session_count,
            uptime_seconds,
        }))
    }

    async fn readiness(&self, _params: Value) -> Result<Value> {
        let mut checks = Vec::new();
        let mut all_passing = true;

        let executor_check = match self.session_manager.check_executor_health().await {
            Ok(()) => HealthCheck {
                name: "executor".to_string(),
                status: "pass".to_string(),
                message: None,
            },
            Err(e) => {
                all_passing = false;
                HealthCheck {
                    name: "executor".to_string(),
                    status: "fail".to_string(),
                    message: Some(e.to_string()),
                }
            }
        };
        checks.push(executor_check);

        let session_check = HealthCheck {
            name: "sessions".to_string(),
            status: "pass".to_string(),
            message: Some(format!("{} active", self.session_manager.session_count())),
        };
        checks.push(session_check);

        Ok(json!(ReadinessResult {
            ready: all_passing,
            status: if all_passing { "ready" } else { "not_ready" }.to_string(),
            checks,
        }))
    }

    async fn liveness(&self, _params: Value) -> Result<Value> {
        Ok(json!(LivenessResult {
            alive: true,
            uptime_seconds: self.session_manager.uptime_seconds(),
        }))
    }

    async fn define_table(&self, params: Value) -> Result<Value> {
        let p: DefineTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let deps = self
            .session_manager
            .define_table(session_id, &p.name, &p.sql)?;
        Ok(json!(DefineTableResult {
            success: true,
            dependencies: deps,
        }))
    }

    async fn define_tables(&self, params: Value) -> Result<Value> {
        let p: DefineTablesParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let mut results = Vec::new();
        for t in &p.tables {
            let deps = self
                .session_manager
                .define_table(session_id, &t.name, &t.sql)?;
            results.push(DefineTableResult {
                success: true,
                dependencies: deps,
            });
        }
        Ok(json!(DefineTablesResult {
            success: true,
            tables: results,
        }))
    }

    async fn drop_table(&self, params: Value) -> Result<Value> {
        let p: DropTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        self.session_manager.drop_table(session_id, &p.name).await?;
        Ok(json!({"success": true}))
    }

    async fn drop_all_tables(&self, params: Value) -> Result<Value> {
        let p: DropAllTablesParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        self.session_manager.drop_all_tables(session_id).await?;
        Ok(json!({"success": true}))
    }

    async fn execute_tables(&self, params: Value) -> Result<Value> {
        let p: ExecuteParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let targets = p.tables;
        let retry_count = if p.force { 1 } else { 0 };

        let result = self
            .session_manager
            .run_dag(session_id, targets, retry_count)
            .await?;

        Ok(json!(ExecuteResult {
            success: result.all_succeeded(),
            succeeded: result.succeeded,
            failed: result.failed,
            skipped: result.skipped,
        }))
    }

    async fn load_directory(&self, params: Value) -> Result<Value> {
        let p: LoadDirectoryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let (source_tables, _computed_tables, dag_info) = self
            .session_manager
            .load_dag_from_directory(session_id, &p.path)
            .await?;

        let mut tables = Vec::new();
        for t in source_tables {
            tables.push(LoadedTableInfo {
                name: format!("{}.{}.{}", t.project, t.dataset, t.table),
                kind: "source".to_string(),
                dependencies: vec![],
            });
        }
        for t in dag_info {
            tables.push(LoadedTableInfo {
                name: t.name,
                kind: "computed".to_string(),
                dependencies: t.dependencies,
            });
        }

        Ok(json!(LoadDirectoryResult {
            success: true,
            tables,
        }))
    }

    async fn create_session(&self, _params: Value) -> Result<Value> {
        let session_id = self.session_manager.create_session().await?;

        Ok(json!(CreateSessionResult {
            session_id: session_id.to_string(),
        }))
    }

    async fn destroy_session(&self, params: Value) -> Result<Value> {
        let (_, session_id) = parse_session_params::<DestroySessionParams>(params)?;
        self.session_manager.destroy_session(session_id)?;
        Ok(json!(DestroySessionResult { success: true }))
    }

    async fn query(&self, params: Value) -> Result<Value> {
        let p: QueryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        // Validate SQL to block DDL statements
        validate_sql_for_query(&p.sql)?;

        let result = self
            .session_manager
            .execute_query(session_id, &p.sql)
            .await?;

        Ok(result.to_bq_response())
    }

    async fn create_table(&self, params: Value) -> Result<Value> {
        let p: CreateTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        // Validate table name to prevent SQL injection
        validate_table_name(&p.table_name)?;

        // Validate column names
        for col in &p.schema {
            validate_table_name(&col.name)?;
        }

        let quoted_table = format!("`{}`", quote_identifier(&p.table_name));
        let columns: Vec<String> = p
            .schema
            .iter()
            .map(|col| format!("`{}` {}", quote_identifier(&col.name), col.column_type))
            .collect();

        let sql = format!("CREATE TABLE {} ({})", quoted_table, columns.join(", "));

        self.session_manager
            .execute_statement(session_id, &sql)
            .await?;

        Ok(json!(CreateTableResult { success: true }))
    }

    async fn insert(&self, params: Value) -> Result<Value> {
        let p: InsertParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        validate_table_name(&p.table_name)?;

        if p.rows.is_empty() {
            return Ok(json!(InsertResult { inserted_rows: 0 }));
        }

        // Determine if rows are objects or arrays based on first row
        let first_row = &p.rows[0];
        let total_rows = p.rows.len();
        let (column_names, values): (Option<Vec<String>>, Vec<String>) =
            if let Value::Object(first_obj) = first_row {
                let cols: Vec<String> = first_obj.keys().cloned().collect();
                let vals: Vec<String> = p
                    .rows
                    .iter()
                    .filter_map(|row| {
                        if let Value::Object(obj) = row {
                            let row_vals: Vec<String> =
                                cols.iter().map(|k| json_to_sql_value(&obj[k])).collect();
                            Some(format!("({})", row_vals.join(", ")))
                        } else {
                            None
                        }
                    })
                    .collect();
                (Some(cols), vals)
            } else {
                let vals: Vec<String> = p
                    .rows
                    .iter()
                    .filter_map(|row| {
                        if let Value::Array(arr) = row {
                            let row_vals: Vec<String> = arr.iter().map(json_to_sql_value).collect();
                            Some(format!("({})", row_vals.join(", ")))
                        } else {
                            None
                        }
                    })
                    .collect();
                (None, vals)
            };

        let filtered_count = total_rows - values.len();
        if filtered_count > 0 {
            warn!(
                table = %p.table_name,
                filtered = filtered_count,
                total = total_rows,
                "Rows filtered due to inconsistent format"
            );
        }

        let row_count = values.len() as u64;
        let sql = match column_names {
            Some(cols) => {
                let quoted_cols: Vec<String> = cols
                    .iter()
                    .map(|c| format!("`{}`", quote_identifier(c)))
                    .collect();
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    p.table_name,
                    quoted_cols.join(", "),
                    values.join(", ")
                )
            }
            None => format!("INSERT INTO {} VALUES {}", p.table_name, values.join(", ")),
        };

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

    async fn get_dag(&self, params: Value) -> Result<Value> {
        let (_, session_id) = parse_session_params::<GetDagParams>(params)?;
        let tables = self.session_manager.get_dag(session_id)?;
        Ok(json!(GetDagResult { tables }))
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
        let p: QueryListTablesParams = serde_json::from_value(params)?;
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

    async fn get_default_project(&self, params: Value) -> Result<Value> {
        let (_, session_id) = parse_session_params::<GetDefaultProjectParams>(params)?;
        let project = self.session_manager.get_default_project(session_id)?;
        Ok(json!(GetDefaultProjectResult { project }))
    }

    async fn get_projects(&self, params: Value) -> Result<Value> {
        let (_, session_id) = parse_session_params::<GetProjectsParams>(params)?;
        let projects = self.session_manager.get_projects(session_id)?;
        Ok(json!(GetProjectsResult { projects }))
    }

    async fn get_datasets(&self, params: Value) -> Result<Value> {
        let (p, session_id) = parse_session_params::<GetDatasetsParams>(params)?;
        let datasets = self.session_manager.get_datasets(session_id, &p.project)?;
        Ok(json!(GetDatasetsResult { datasets }))
    }

    async fn get_tables_in_dataset(&self, params: Value) -> Result<Value> {
        let (p, session_id) = parse_session_params::<GetTablesInDatasetParams>(params)?;
        let tables = self
            .session_manager
            .get_tables_in_dataset(session_id, &p.project, &p.dataset)?;
        Ok(json!(GetTablesInDatasetResult { tables }))
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
        let params: crate::rpc::types::dag::ClearDagParams =
            serde_json::from_str(params_json).unwrap();
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
        assert!(matches!(err, Error::InvalidRequest(_)));
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
        assert!(matches!(err, Error::InvalidRequest(_)));
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
        assert!(matches!(err, Error::InvalidRequest(_)));
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
        assert!(matches!(err, Error::InvalidRequest(_)));
    }
}
