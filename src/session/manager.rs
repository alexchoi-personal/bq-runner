use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::OnceCell;
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::{SecurityConfig, SessionConfig};
use crate::domain::{DagTableDef, DagTableDetail, DagTableInfo, ParquetTableInfo, SqlTableInfo};
use crate::error::{Error, Result};
use crate::executor::{
    BigQueryExecutor, ExecutorBackend, ExecutorMode, MockExecutorExt, QueryResult, YachtSqlExecutor,
};
use crate::loader;
use crate::metrics::{
    record_dag_execution, record_query_executed, record_session_created, record_session_destroyed,
    record_tables_defined, set_active_sessions,
};
use crate::validation::{
    quote_identifier, validate_path, validate_sql_for_define_table, validate_table_name,
};

use super::pipeline::{Pipeline, PipelineResult, TableError};

pub struct SessionManager {
    sessions: RwLock<HashMap<Uuid, Session>>,
    mode: ExecutorMode,
    start_time: Instant,
    security_config: SecurityConfig,
    session_config: SessionConfig,
    health_executor: OnceCell<Arc<dyn ExecutorBackend>>,
}

struct Session {
    executor: Arc<dyn ExecutorBackend>,
    mock_executor: Option<Arc<YachtSqlExecutor>>,
    pipeline: Pipeline,
    last_accessed_nanos: AtomicU64,
    created_at: Instant,
}

impl Session {
    fn new(
        executor: Arc<dyn ExecutorBackend>,
        mock_executor: Option<Arc<YachtSqlExecutor>>,
    ) -> Self {
        Self {
            executor,
            mock_executor,
            pipeline: Pipeline::new(),
            last_accessed_nanos: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    fn touch(&self) {
        let nanos = self.created_at.elapsed().as_nanos() as u64;
        self.last_accessed_nanos.store(nanos, Ordering::Relaxed);
    }

    fn last_accessed(&self) -> Instant {
        let nanos = self.last_accessed_nanos.load(Ordering::Relaxed);
        self.created_at + Duration::from_nanos(nanos)
    }
}

impl SessionManager {
    pub fn new() -> Self {
        Self::with_security_config(SecurityConfig::default())
    }

    pub fn with_security_config(security_config: SecurityConfig) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode: ExecutorMode::Mock,
            start_time: Instant::now(),
            security_config,
            session_config: SessionConfig::default(),
            health_executor: OnceCell::new(),
        }
    }

    pub fn session_count(&self) -> usize {
        self.sessions.read().len()
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub async fn check_executor_health(&self) -> Result<()> {
        let executor = self
            .health_executor
            .get_or_try_init(|| async {
                let executor: Arc<dyn ExecutorBackend> = match self.mode {
                    ExecutorMode::Mock => Arc::new(YachtSqlExecutor::new()),
                    ExecutorMode::BigQuery => Arc::new(BigQueryExecutor::new().await?),
                };
                Ok::<_, Error>(executor)
            })
            .await?;
        executor.execute_query("SELECT 1").await.map(|_| ())
    }

    pub fn with_mode(mode: ExecutorMode) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode,
            start_time: Instant::now(),
            security_config: SecurityConfig::default(),
            session_config: SessionConfig::default(),
            health_executor: OnceCell::new(),
        }
    }

    pub fn with_mode_and_security(mode: ExecutorMode, security_config: SecurityConfig) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode,
            start_time: Instant::now(),
            security_config,
            session_config: SessionConfig::default(),
            health_executor: OnceCell::new(),
        }
    }

    pub fn with_full_config(
        mode: ExecutorMode,
        security_config: SecurityConfig,
        session_config: SessionConfig,
    ) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode,
            start_time: Instant::now(),
            security_config,
            session_config,
            health_executor: OnceCell::new(),
        }
    }

    pub fn security_config(&self) -> &SecurityConfig {
        &self.security_config
    }

    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    pub fn cleanup_expired_sessions(&self) -> usize {
        let timeout = Duration::from_secs(self.session_config.session_timeout_secs);
        let now = Instant::now();
        let mut sessions = self.sessions.write();
        let before = sessions.len();
        sessions.retain(|id, session| {
            let expired = now.duration_since(session.last_accessed()) > timeout;
            if expired {
                debug!(session_id = %id, "Session expired and removed");
            }
            !expired
        });
        let removed = before - sessions.len();
        if removed > 0 {
            info!(
                removed = removed,
                remaining = sessions.len(),
                "Cleaned up expired sessions"
            );
        }
        removed
    }

    fn get_executor(&self, session_id: Uuid) -> Result<Arc<dyn ExecutorBackend>> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.touch();
        Ok(Arc::clone(&session.executor))
    }

    fn get_mock_executor(&self, session_id: Uuid) -> Result<Arc<YachtSqlExecutor>> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.touch();
        session
            .mock_executor
            .clone()
            .ok_or_else(|| Error::Executor("This operation is only available in mock mode".into()))
    }

    fn with_session<F, T>(&self, session_id: Uuid, f: F) -> Result<T>
    where
        F: FnOnce(&Session) -> Result<T>,
    {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.touch();
        f(session)
    }

    fn with_session_mut<F, T>(&self, session_id: Uuid, f: F) -> Result<T>
    where
        F: FnOnce(&mut Session) -> Result<T>,
    {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.touch();
        f(session)
    }

    pub async fn create_session(&self) -> Result<Uuid> {
        {
            let sessions = self.sessions.read();
            if sessions.len() >= self.session_config.max_sessions {
                return Err(Error::InvalidRequest(format!(
                    "Maximum session limit ({}) reached",
                    self.session_config.max_sessions
                )));
            }
        }

        let session_id = Uuid::new_v4();
        let (executor, mock_executor): (Arc<dyn ExecutorBackend>, Option<Arc<YachtSqlExecutor>>) =
            match self.mode {
                ExecutorMode::Mock => {
                    let mock = Arc::new(YachtSqlExecutor::new());
                    (Arc::clone(&mock) as Arc<dyn ExecutorBackend>, Some(mock))
                }
                ExecutorMode::BigQuery => (Arc::new(BigQueryExecutor::new().await?), None),
            };

        let session = Session::new(executor, mock_executor);

        let session_count = {
            let mut sessions = self.sessions.write();
            if sessions.len() >= self.session_config.max_sessions {
                return Err(Error::InvalidRequest(format!(
                    "Maximum session limit ({}) reached",
                    self.session_config.max_sessions
                )));
            }
            sessions.insert(session_id, session);
            sessions.len()
        };

        record_session_created();
        set_active_sessions(session_count);

        Ok(session_id)
    }

    pub fn destroy_session(&self, session_id: Uuid) -> Result<()> {
        let session_count = {
            let mut sessions = self.sessions.write();
            sessions
                .remove(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            sessions.len()
        };

        record_session_destroyed();
        set_active_sessions(session_count);

        Ok(())
    }

    pub async fn execute_query(&self, session_id: Uuid, sql: &str) -> Result<QueryResult> {
        let executor = self.get_executor(session_id)?;
        let result = executor.execute_query(sql).await;
        if result.is_ok() {
            record_query_executed();
        }
        result
    }

    pub async fn execute_statement(&self, session_id: Uuid, sql: &str) -> Result<u64> {
        let executor = self.get_executor(session_id)?;
        executor.execute_statement(sql).await
    }

    pub fn register_dag(
        &self,
        session_id: Uuid,
        tables: Vec<DagTableDef>,
    ) -> Result<Vec<DagTableInfo>> {
        for table in &tables {
            validate_table_name(&table.name)?;
            if let Some(sql) = &table.sql {
                validate_sql_for_define_table(sql)?;
            }
        }
        self.with_session_mut(session_id, |session| session.pipeline.register(tables))
    }

    pub async fn run_dag(
        &self,
        session_id: Uuid,
        targets: Option<Vec<String>>,
        retry_count: u32,
    ) -> Result<PipelineResult> {
        let (pipeline, executor) = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            (session.pipeline.clone(), Arc::clone(&session.executor))
        };

        let mut result = pipeline.run(Arc::clone(&executor), targets).await?;

        for _ in 0..retry_count {
            if result.all_succeeded() {
                break;
            }

            let retry_result = pipeline
                .retry_failed(Arc::clone(&executor), &result)
                .await?;

            result.succeeded.extend(retry_result.succeeded);
            result.failed = retry_result.failed;
            result.skipped = retry_result.skipped;
        }

        record_dag_execution(result.succeeded.len(), result.failed.len());
        Ok(result)
    }

    pub async fn retry_dag(
        &self,
        session_id: Uuid,
        failed_tables: Vec<String>,
        skipped_tables: Vec<String>,
    ) -> Result<PipelineResult> {
        let (pipeline, executor) = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            (session.pipeline.clone(), Arc::clone(&session.executor))
        };

        let previous_result = PipelineResult {
            succeeded: vec![],
            failed: failed_tables
                .into_iter()
                .map(|t| TableError {
                    table: t,
                    error: String::new(),
                })
                .collect(),
            skipped: skipped_tables,
        };

        pipeline.retry_failed(executor, &previous_result).await
    }

    pub fn get_dag(&self, session_id: Uuid) -> Result<Vec<DagTableDetail>> {
        self.with_session(session_id, |session| Ok(session.pipeline.get_tables()))
    }

    pub async fn clear_dag(&self, session_id: Uuid) -> Result<()> {
        let (executor, mut pipeline) = {
            let mut sessions = self.sessions.write();
            let session = sessions
                .get_mut(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            (
                Arc::clone(&session.executor),
                std::mem::take(&mut session.pipeline),
            )
        };
        pipeline.clear(executor.as_ref()).await;
        Ok(())
    }

    pub fn define_table(&self, session_id: Uuid, name: &str, sql: &str) -> Result<Vec<String>> {
        validate_table_name(name)?;
        validate_sql_for_define_table(sql)?;
        let result = self.with_session_mut(session_id, |session| {
            session.pipeline.register_table(name, sql)
        });
        if result.is_ok() {
            record_tables_defined(1);
        }
        result
    }

    pub async fn drop_table(&self, session_id: Uuid, name: &str) -> Result<()> {
        validate_table_name(name)?;
        let executor = self.get_executor(session_id)?;

        let drop_sql = format!("DROP TABLE IF EXISTS `{}`", quote_identifier(name));
        executor.execute_statement(&drop_sql).await?;

        self.with_session_mut(session_id, |session| {
            session.pipeline.remove_table(name);
            Ok(())
        })
    }

    pub async fn drop_all_tables(&self, session_id: Uuid) -> Result<()> {
        let (executor, table_names) = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            let table_names = session.pipeline.table_names();
            (Arc::clone(&session.executor), table_names)
        };

        for name in &table_names {
            let drop_sql = format!("DROP TABLE IF EXISTS `{}`", quote_identifier(name));
            let _ = executor.execute_statement(&drop_sql).await;
        }

        self.with_session_mut(session_id, |session| {
            session.pipeline.clear_state();
            Ok(())
        })
    }

    pub async fn load_parquet(
        &self,
        session_id: Uuid,
        table_name: &str,
        path: &str,
        schema: &[crate::domain::ColumnDef],
    ) -> Result<u64> {
        let validated_path = validate_path(path, &self.security_config)?;
        let executor = self.get_executor(session_id)?;
        executor
            .load_parquet(
                table_name,
                validated_path.to_string_lossy().as_ref(),
                schema,
            )
            .await
    }

    pub async fn list_tables(&self, session_id: Uuid) -> Result<Vec<(String, u64)>> {
        let executor = self.get_mock_executor(session_id)?;
        executor.list_tables().await
    }

    pub async fn describe_table(
        &self,
        session_id: Uuid,
        table_name: &str,
    ) -> Result<(Vec<(String, String)>, u64)> {
        let executor = self.get_mock_executor(session_id)?;
        executor.describe_table(table_name).await
    }

    pub fn set_default_project(&self, session_id: Uuid, project: Option<String>) -> Result<()> {
        let executor = self.get_mock_executor(session_id)?;
        executor.set_default_project(project);
        Ok(())
    }

    pub fn get_default_project(&self, session_id: Uuid) -> Result<Option<String>> {
        let executor = self.get_mock_executor(session_id)?;
        Ok(executor.get_default_project())
    }

    pub fn get_projects(&self, session_id: Uuid) -> Result<Vec<String>> {
        let executor = self.get_mock_executor(session_id)?;
        Ok(executor.get_projects())
    }

    pub fn get_datasets(&self, session_id: Uuid, project: &str) -> Result<Vec<String>> {
        let executor = self.get_mock_executor(session_id)?;
        Ok(executor.get_datasets(project))
    }

    pub fn get_tables_in_dataset(
        &self,
        session_id: Uuid,
        project: &str,
        dataset: &str,
    ) -> Result<Vec<String>> {
        let executor = self.get_mock_executor(session_id)?;
        Ok(executor.get_tables_in_dataset(project, dataset))
    }

    pub fn load_sql_directory(
        &self,
        session_id: Uuid,
        root_path: &str,
    ) -> Result<(Vec<DagTableInfo>, Vec<SqlTableInfo>)> {
        let sql_files = loader::discover_sql_files_secure(root_path, &self.security_config)?;

        let tables: Vec<DagTableDef> = sql_files
            .iter()
            .map(|sf| DagTableDef {
                name: sf.full_table_name(),
                sql: Some(sf.sql.clone()),
                schema: None,
                rows: vec![],
            })
            .collect();

        let table_infos: Vec<SqlTableInfo> = sql_files
            .into_iter()
            .map(|sf| SqlTableInfo {
                project: sf.project,
                dataset: sf.dataset,
                table: sf.table,
                path: sf.path,
            })
            .collect();

        let dag_infos = self.register_dag(session_id, tables)?;

        Ok((dag_infos, table_infos))
    }

    pub async fn load_parquet_directory(
        &self,
        session_id: Uuid,
        root_path: &str,
    ) -> Result<Vec<ParquetTableInfo>> {
        let parquet_files =
            loader::discover_parquet_files_secure(root_path, &self.security_config)?;
        let executor = self.get_executor(session_id)?;
        self.load_parquet_files_parallel(executor, parquet_files)
            .await
    }

    async fn load_parquet_files_parallel(
        &self,
        executor: Arc<dyn ExecutorBackend>,
        parquet_files: Vec<loader::ParquetFile>,
    ) -> Result<Vec<ParquetTableInfo>> {
        let mut handles = Vec::new();

        for pf in parquet_files {
            let executor = Arc::clone(&executor);
            let handle = tokio::spawn(async move {
                let row_count = executor
                    .load_parquet(&pf.full_table_name(), &pf.path, &pf.schema)
                    .await?;
                Ok::<_, Error>(ParquetTableInfo {
                    project: pf.project,
                    dataset: pf.dataset,
                    table: pf.table,
                    path: pf.path,
                    row_count,
                })
            });
            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| Error::Executor(format!("Task join error: {}", e)))??;
            results.push(result);
        }

        Ok(results)
    }

    pub async fn load_dag_from_directory(
        &self,
        session_id: Uuid,
        root_path: &str,
    ) -> Result<(Vec<ParquetTableInfo>, Vec<SqlTableInfo>, Vec<DagTableInfo>)> {
        let discovered = loader::discover_files_secure(root_path, &self.security_config)?;
        let executor = self.get_executor(session_id)?;
        let parquet_results = self
            .load_parquet_files_parallel(executor, discovered.parquet_files)
            .await?;

        let dag_tables: Vec<DagTableDef> = discovered
            .sql_files
            .iter()
            .map(|sf| DagTableDef {
                name: sf.full_table_name(),
                sql: Some(sf.sql.clone()),
                schema: None,
                rows: vec![],
            })
            .collect();

        let sql_table_infos: Vec<SqlTableInfo> = discovered
            .sql_files
            .into_iter()
            .map(|sf| SqlTableInfo {
                project: sf.project,
                dataset: sf.dataset,
                table: sf.table,
                path: sf.path,
            })
            .collect();

        let dag_infos = self.register_dag(session_id, dag_tables)?;

        Ok((parquet_results, sql_table_infos, dag_infos))
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        let result = manager.execute_query(session_id, "SELECT 1 AS x").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_multiple_sessions() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();
        let s3 = manager.create_session().await.unwrap();

        assert_ne!(s1, s2);
        assert_ne!(s2, s3);
    }

    #[tokio::test]
    async fn test_destroy_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        assert!(manager.execute_query(session_id, "SELECT 1").await.is_ok());
        manager.destroy_session(session_id).unwrap();
        assert!(manager.execute_query(session_id, "SELECT 1").await.is_err());
    }

    #[tokio::test]
    async fn test_destroy_nonexistent_session() {
        let manager = SessionManager::new();
        let fake_id = Uuid::new_v4();

        let result = manager.destroy_session(fake_id);
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore = "requires COUNT aggregate which is not yet implemented in concurrent executor"]
    async fn test_session_isolation() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();

        manager
            .execute_statement(s1, "CREATE TABLE users (id INT64, name STRING)")
            .await
            .unwrap();

        manager
            .execute_statement(s1, "INSERT INTO users VALUES (1, 'Alice')")
            .await
            .unwrap();

        manager
            .execute_statement(s2, "CREATE TABLE users (id INT64, name STRING)")
            .await
            .unwrap();

        manager
            .execute_statement(s2, "INSERT INTO users VALUES (2, 'Bob'), (3, 'Charlie')")
            .await
            .unwrap();

        let result1 = manager
            .execute_query(s1, "SELECT COUNT(*) FROM users")
            .await
            .unwrap();

        let result2 = manager
            .execute_query(s2, "SELECT COUNT(*) FROM users")
            .await
            .unwrap();

        assert_eq!(result1.rows.len(), 1);
        assert_eq!(result2.rows.len(), 1);

        let count1: i64 = result1.rows[0][0].as_i64().unwrap();
        let count2: i64 = result2.rows[0][0].as_i64().unwrap();

        assert_eq!(count1, 1);
        assert_eq!(count2, 2);
    }

    #[tokio::test]
    async fn test_load_parquet() {
        use arrow::array::{
            ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
            TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        use tempfile::NamedTempFile;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("created_date", DataType::Date32, true),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));
        let score_array: ArrayRef =
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), Some(92.1)]));
        let active_array: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ]));
        let date_array: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(19000),
            Some(19001),
            Some(19002),
        ]));
        let timestamp_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1640000000000000i64),
            Some(1640000001000000i64),
            Some(1640000002000000i64),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                id_array,
                name_array,
                score_array,
                active_array,
                date_array,
                timestamp_array,
            ],
        )
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let security_config = SecurityConfig {
            allowed_paths: vec![temp_file.path().parent().unwrap().to_path_buf()],
            block_symlinks: false,
        };
        let manager = SessionManager::with_security_config(security_config);
        let session_id = manager.create_session().await.unwrap();

        let bq_schema = vec![
            crate::domain::ColumnDef {
                name: "id".to_string(),
                column_type: crate::domain::ColumnType::Int64,
            },
            crate::domain::ColumnDef {
                name: "name".to_string(),
                column_type: crate::domain::ColumnType::String,
            },
            crate::domain::ColumnDef {
                name: "score".to_string(),
                column_type: crate::domain::ColumnType::Float64,
            },
            crate::domain::ColumnDef {
                name: "active".to_string(),
                column_type: crate::domain::ColumnType::Bool,
            },
            crate::domain::ColumnDef {
                name: "created_date".to_string(),
                column_type: crate::domain::ColumnType::Date,
            },
            crate::domain::ColumnDef {
                name: "updated_at".to_string(),
                column_type: crate::domain::ColumnType::Timestamp,
            },
        ];

        let result = manager
            .load_parquet(session_id, "test_data", &path, &bq_schema)
            .await;
        println!("Load result: {:?}", result);
        assert!(result.is_ok(), "Failed to load parquet: {:?}", result.err());
        let row_count = result.unwrap();
        println!("Loaded {} rows", row_count);

        let query_result = manager
            .execute_query(session_id, "SELECT * FROM test_data ORDER BY id")
            .await;

        println!("Query result: {:?}", query_result);
        let query_result = query_result.unwrap();

        assert_eq!(query_result.rows.len(), 3);
        assert_eq!(query_result.columns.len(), 6);

        assert_eq!(query_result.rows[0][0].as_i64().unwrap(), 1);
        assert_eq!(query_result.rows[0][1].as_str().unwrap(), "Alice");
        assert_eq!(query_result.rows[0][2].as_f64().unwrap(), 95.5);
        assert_eq!(query_result.rows[0][3].as_bool().unwrap(), true);

        assert_eq!(query_result.rows[1][0].as_i64().unwrap(), 2);
        assert_eq!(query_result.rows[1][1].as_str().unwrap(), "Bob");

        assert_eq!(query_result.rows[2][0].as_i64().unwrap(), 3);
        assert!(query_result.rows[2][1].is_null());

        println!("Columns: {:?}", query_result.columns);
        for (i, row) in query_result.rows.iter().enumerate() {
            println!("Row {}: {:?}", i, row);
        }
    }

    #[tokio::test]
    #[ignore = "requires SUM/COUNT aggregates which are not yet implemented in concurrent executor"]
    async fn test_multiple_sessions_run_dags_in_parallel() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        let manager = Arc::new(SessionManager::new());
        let num_sessions = 4;

        let mut session_ids = Vec::new();
        for _ in 0..num_sessions {
            let session_id = manager.create_session().await.unwrap();

            manager
                .execute_statement(session_id, "CREATE TABLE base (v INT64)")
                .await
                .unwrap();
            for i in 0..100 {
                manager
                    .execute_statement(session_id, &format!("INSERT INTO base VALUES ({})", i))
                    .await
                    .unwrap();
            }

            let tables = vec![
                DagTableDef {
                    name: "sum_table".to_string(),
                    sql: Some("SELECT SUM(v) AS total FROM base".to_string()),
                    schema: None,
                    rows: vec![],
                },
                DagTableDef {
                    name: "count_table".to_string(),
                    sql: Some("SELECT COUNT(*) AS cnt FROM base".to_string()),
                    schema: None,
                    rows: vec![],
                },
            ];
            manager.register_dag(session_id, tables).unwrap();

            session_ids.push(session_id);
        }

        static CONCURRENT_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);
        static MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

        CONCURRENT_EXECUTIONS.store(0, Ordering::SeqCst);
        MAX_CONCURRENT.store(0, Ordering::SeqCst);

        let start = Instant::now();

        let handles: Vec<_> = session_ids
            .into_iter()
            .map(|session_id| {
                let manager = Arc::clone(&manager);
                tokio::spawn(async move {
                    let prev = CONCURRENT_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
                    let current = prev + 1;

                    loop {
                        let max = MAX_CONCURRENT.load(Ordering::SeqCst);
                        if current <= max {
                            break;
                        }
                        if MAX_CONCURRENT
                            .compare_exchange(max, current, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            break;
                        }
                    }

                    let result = manager.run_dag(session_id, None, 0).await;

                    CONCURRENT_EXECUTIONS.fetch_sub(1, Ordering::SeqCst);

                    (session_id, result)
                })
            })
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        let elapsed = start.elapsed();

        for (session_id, result) in &results {
            let dag_result = result.as_ref().expect("DAG should execute successfully");
            assert_eq!(
                dag_result.succeeded.len(),
                2,
                "Each session should execute 2 tables"
            );

            let sum_result = manager
                .execute_query(*session_id, "SELECT * FROM sum_table")
                .await
                .unwrap();
            assert_eq!(sum_result.rows[0][0].as_i64().unwrap(), 4950);

            let count_result = manager
                .execute_query(*session_id, "SELECT * FROM count_table")
                .await
                .unwrap();
            assert_eq!(count_result.rows[0][0].as_i64().unwrap(), 100);
        }

        let max_concurrent = MAX_CONCURRENT.load(Ordering::SeqCst);
        assert!(
            max_concurrent >= 2,
            "Expected at least 2 sessions to run concurrently, but max was {}",
            max_concurrent
        );

        println!(
            "Parallel session test: {} sessions, max {} concurrent, took {:?}",
            num_sessions, max_concurrent, elapsed
        );
    }

    #[tokio::test]
    #[ignore = "requires runtime context in spawned threads - needs refactoring for async executor"]
    async fn test_sessions_are_isolated_during_parallel_dag_execution() {
        let manager = Arc::new(SessionManager::new());

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();

        manager
            .execute_statement(s1, "CREATE TABLE data (v INT64)")
            .await
            .unwrap();
        manager
            .execute_statement(s1, "INSERT INTO data VALUES (100)")
            .await
            .unwrap();

        manager
            .execute_statement(s2, "CREATE TABLE data (v INT64)")
            .await
            .unwrap();
        manager
            .execute_statement(s2, "INSERT INTO data VALUES (200)")
            .await
            .unwrap();

        manager
            .register_dag(
                s1,
                vec![DagTableDef {
                    name: "result".to_string(),
                    sql: Some("SELECT v * 2 AS doubled FROM data".to_string()),
                    schema: None,
                    rows: vec![],
                }],
            )
            .unwrap();

        manager
            .register_dag(
                s2,
                vec![DagTableDef {
                    name: "result".to_string(),
                    sql: Some("SELECT v * 3 AS tripled FROM data".to_string()),
                    schema: None,
                    rows: vec![],
                }],
            )
            .unwrap();

        let manager1 = Arc::clone(&manager);
        let manager2 = Arc::clone(&manager);

        let handle1 = tokio::spawn(async move { manager1.run_dag(s1, None, 0).await });
        let handle2 = tokio::spawn(async move { manager2.run_dag(s2, None, 0).await });

        let result1 = handle1.await.unwrap().unwrap();
        let result2 = handle2.await.unwrap().unwrap();

        assert_eq!(result1.succeeded, vec!["result"]);
        assert_eq!(result2.succeeded, vec!["result"]);

        let query1 = manager
            .execute_query(s1, "SELECT * FROM result")
            .await
            .unwrap();
        let query2 = manager
            .execute_query(s2, "SELECT * FROM result")
            .await
            .unwrap();

        assert_eq!(query1.rows[0][0].as_i64().unwrap(), 200);
        assert_eq!(query2.rows[0][0].as_i64().unwrap(), 600);
    }

    #[tokio::test]
    #[ignore = "requires runtime context in spawned threads - needs refactoring for async executor"]
    async fn test_many_sessions_parallel_with_complex_dags() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let manager = Arc::new(SessionManager::new());
        let num_sessions = 8;

        static COMPLETED_SESSIONS: AtomicUsize = AtomicUsize::new(0);
        COMPLETED_SESSIONS.store(0, Ordering::SeqCst);

        let mut session_ids = Vec::new();
        for i in 0..num_sessions {
            let session_id = manager.create_session().await.unwrap();

            manager
                .execute_statement(session_id, "CREATE TABLE source (n INT64)")
                .await
                .unwrap();
            manager
                .execute_statement(
                    session_id,
                    &format!("INSERT INTO source VALUES ({})", i + 1),
                )
                .await
                .unwrap();

            let tables = vec![
                DagTableDef {
                    name: "step1".to_string(),
                    sql: Some("SELECT n * 2 AS n FROM source".to_string()),
                    schema: None,
                    rows: vec![],
                },
                DagTableDef {
                    name: "step2".to_string(),
                    sql: Some("SELECT n + 10 AS n FROM step1".to_string()),
                    schema: None,
                    rows: vec![],
                },
                DagTableDef {
                    name: "step3".to_string(),
                    sql: Some("SELECT n * n AS n FROM step2".to_string()),
                    schema: None,
                    rows: vec![],
                },
            ];
            manager.register_dag(session_id, tables).unwrap();

            session_ids.push((session_id, i + 1));
        }

        let handles: Vec<_> = session_ids
            .into_iter()
            .map(|(session_id, base_value)| {
                let manager = Arc::clone(&manager);
                tokio::spawn(async move {
                    let result = manager.run_dag(session_id, None, 0).await;
                    COMPLETED_SESSIONS.fetch_add(1, Ordering::SeqCst);
                    (session_id, base_value, result)
                })
            })
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        assert_eq!(
            COMPLETED_SESSIONS.load(Ordering::SeqCst),
            num_sessions,
            "All sessions should complete"
        );

        for (session_id, base_value, result) in results {
            let dag_result = result.unwrap();
            assert_eq!(dag_result.succeeded.len(), 3);

            assert_eq!(dag_result.succeeded[0], "step1");
            assert_eq!(dag_result.succeeded[1], "step2");
            assert_eq!(dag_result.succeeded[2], "step3");

            let final_result = manager
                .execute_query(session_id, "SELECT * FROM step3")
                .await
                .unwrap();

            let expected = {
                let step1 = base_value * 2;
                let step2 = step1 + 10;
                let step3 = step2 * step2;
                step3
            };

            assert_eq!(
                final_result.rows[0][0].as_i64().unwrap(),
                expected as i64,
                "Session with base {} should have final result {}",
                base_value,
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_dag_execution_order_within_session_is_serial() {
        let manager = Arc::new(SessionManager::new());

        let session_id = manager.create_session().await.unwrap();

        manager
            .execute_statement(session_id, "CREATE TABLE root (v INT64)")
            .await
            .unwrap();
        manager
            .execute_statement(session_id, "INSERT INTO root VALUES (1)")
            .await
            .unwrap();

        let tables: Vec<DagTableDef> = (0..5)
            .map(|i| DagTableDef {
                name: format!("branch_{}", i),
                sql: Some(format!("SELECT v + {} AS v FROM root", i)),
                schema: None,
                rows: vec![],
            })
            .collect();

        let infos = manager.register_dag(session_id, tables).unwrap();

        for info in &infos {
            assert!(
                info.dependencies.is_empty() || info.dependencies == vec!["root"],
                "All branches should only depend on root (external table)"
            );
        }

        let dag_result = manager.run_dag(session_id, None, 0).await.unwrap();

        assert_eq!(dag_result.succeeded.len(), 5);

        let mut sorted_executed = dag_result.succeeded.clone();
        sorted_executed.sort();
        assert_eq!(
            dag_result.succeeded, sorted_executed,
            "In mock mode, tables at same level should execute in alphabetical order"
        );

        for i in 0..5 {
            let result = manager
                .execute_query(session_id, &format!("SELECT * FROM branch_{}", i))
                .await
                .unwrap();
            assert_eq!(result.rows[0][0].as_i64().unwrap(), 1 + i as i64);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_table_registration_in_session() {
        let manager = Arc::new(SessionManager::new());
        let session_id = manager.create_session().await.unwrap();

        let num_tables = 20;
        let mut handles = Vec::new();

        for i in 0..num_tables {
            let manager = Arc::clone(&manager);
            let handle = tokio::spawn(async move {
                let table_name = format!("parallel_table_{}", i);
                let create_sql = format!(
                    "CREATE TABLE {} (id INT64, value STRING, num FLOAT64)",
                    table_name
                );
                manager
                    .execute_statement(session_id, &create_sql)
                    .await
                    .unwrap();

                let insert_sql = format!(
                    "INSERT INTO {} VALUES ({}, 'value_{}', {}.5)",
                    table_name, i, i, i
                );
                manager
                    .execute_statement(session_id, &insert_sql)
                    .await
                    .unwrap();

                table_name
            });
            handles.push(handle);
        }

        let mut created_tables = Vec::new();
        for handle in handles {
            let table_name = handle.await.unwrap();
            created_tables.push(table_name);
        }

        assert_eq!(created_tables.len(), num_tables);

        for i in 0..num_tables {
            let table_name = format!("parallel_table_{}", i);
            let select_sql = format!("SELECT * FROM {}", table_name);
            let result = manager
                .execute_query(session_id, &select_sql)
                .await
                .unwrap();
            assert_eq!(result.rows.len(), 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_table_registration_with_qualified_names() {
        let manager = Arc::new(SessionManager::new());
        let session_id = manager.create_session().await.unwrap();

        let projects = vec!["proj1", "proj2", "proj3"];
        let datasets = vec!["ds1", "ds2"];
        let tables_per_dataset = 5;

        let mut handles = Vec::new();

        for project in &projects {
            for dataset in &datasets {
                for i in 0..tables_per_dataset {
                    let manager = Arc::clone(&manager);
                    let project = project.to_string();
                    let dataset = dataset.to_string();
                    let handle = tokio::spawn(async move {
                        let table_name = format!("{}.{}.table_{}", project, dataset, i);
                        let create_sql =
                            format!("CREATE TABLE {} (id INT64, data STRING)", table_name);
                        manager
                            .execute_statement(session_id, &create_sql)
                            .await
                            .unwrap();

                        let insert_sql = format!(
                            "INSERT INTO {} VALUES ({}, '{}_data')",
                            table_name, i, table_name
                        );
                        manager
                            .execute_statement(session_id, &insert_sql)
                            .await
                            .unwrap();

                        (project, dataset, format!("table_{}", i))
                    });
                    handles.push(handle);
                }
            }
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let catalog_projects = manager.get_projects(session_id).unwrap();
        assert_eq!(catalog_projects.len(), 3);
        assert!(catalog_projects.contains(&"PROJ1".to_string()));
        assert!(catalog_projects.contains(&"PROJ2".to_string()));
        assert!(catalog_projects.contains(&"PROJ3".to_string()));

        for project in &projects {
            let catalog_datasets = manager.get_datasets(session_id, project).unwrap();
            assert_eq!(catalog_datasets.len(), 2);

            for dataset in &datasets {
                let tables = manager
                    .get_tables_in_dataset(session_id, project, dataset)
                    .unwrap();
                assert_eq!(tables.len(), tables_per_dataset);
            }
        }

        let result = manager
            .execute_query(session_id, "SELECT * FROM proj1.ds1.table_0")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_source_table_loading() {
        let manager = Arc::new(SessionManager::new());
        let session_id = manager.create_session().await.unwrap();

        let source_tables = vec![
            ("proj.raw.users", "id INT64, name STRING, age INT64"),
            ("proj.raw.orders", "id INT64, user_id INT64, amount FLOAT64"),
            ("proj.raw.products", "id INT64, name STRING, price FLOAT64"),
            ("proj.raw.categories", "id INT64, name STRING"),
        ];

        let mut handles = Vec::new();
        for (table_name, schema) in &source_tables {
            let manager = Arc::clone(&manager);
            let table_name = table_name.to_string();
            let schema = schema.to_string();
            let handle = tokio::spawn(async move {
                let create_sql = format!("CREATE TABLE {} ({})", table_name, schema);
                manager
                    .execute_statement(session_id, &create_sql)
                    .await
                    .unwrap();

                let insert_sql = match table_name.as_str() {
                    "proj.raw.users" => "INSERT INTO proj.raw.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)".to_string(),
                    "proj.raw.orders" => "INSERT INTO proj.raw.orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0), (4, 3, 300.0)".to_string(),
                    "proj.raw.products" => "INSERT INTO proj.raw.products VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99), (3, 'Keyboard', 79.99)".to_string(),
                    "proj.raw.categories" => "INSERT INTO proj.raw.categories VALUES (1, 'Electronics'), (2, 'Accessories')".to_string(),
                    _ => panic!("Unknown table"),
                };
                manager
                    .execute_statement(session_id, &insert_sql)
                    .await
                    .unwrap();

                table_name
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let tables = manager
            .get_tables_in_dataset(session_id, "proj", "raw")
            .unwrap();
        assert_eq!(tables.len(), 4);

        let result = manager
            .execute_query(
                session_id,
                "SELECT u.name, SUM(o.amount) as total
                 FROM proj.raw.users u
                 JOIN proj.raw.orders o ON u.id = o.user_id
                 GROUP BY u.name
                 ORDER BY total DESC",
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_define_table_validates_sql() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        let result = manager.define_table(session_id, "valid_table", "SELECT 1 AS id");
        assert!(result.is_ok());

        let result = manager.define_table(session_id, "bad_table", "DROP TABLE users");
        assert!(result.is_err());
    }

    #[test]
    fn test_manager_with_security_config() {
        let config = SecurityConfig {
            allowed_paths: vec![std::path::PathBuf::from("/tmp")],
            block_symlinks: false,
        };
        let manager = SessionManager::with_security_config(config);
        assert!(!manager.security_config().block_symlinks);
    }

    #[tokio::test]
    async fn test_max_sessions_limit() {
        let session_config = SessionConfig {
            max_sessions: 2,
            session_timeout_secs: 3600,
            cleanup_interval_secs: 60,
        };
        let manager = SessionManager::with_full_config(
            ExecutorMode::Mock,
            SecurityConfig::default(),
            session_config,
        );

        let s1 = manager.create_session().await;
        assert!(s1.is_ok());
        let s2 = manager.create_session().await;
        assert!(s2.is_ok());
        let s3 = manager.create_session().await;
        assert!(s3.is_err());
        assert!(s3
            .unwrap_err()
            .to_string()
            .contains("Maximum session limit"));

        manager.destroy_session(s1.unwrap()).unwrap();
        let s4 = manager.create_session().await;
        assert!(s4.is_ok());
    }

    #[tokio::test]
    async fn test_session_cleanup_expired() {
        let session_config = SessionConfig {
            max_sessions: 100,
            session_timeout_secs: 0,
            cleanup_interval_secs: 60,
        };
        let manager = SessionManager::with_full_config(
            ExecutorMode::Mock,
            SecurityConfig::default(),
            session_config,
        );

        let _s1 = manager.create_session().await.unwrap();
        let _s2 = manager.create_session().await.unwrap();
        assert_eq!(manager.session_count(), 2);

        std::thread::sleep(std::time::Duration::from_millis(10));
        let removed = manager.cleanup_expired_sessions();
        assert_eq!(removed, 2);
        assert_eq!(manager.session_count(), 0);
    }

    #[test]
    fn test_uptime_seconds() {
        let manager = SessionManager::new();
        let uptime1 = manager.uptime_seconds();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let uptime2 = manager.uptime_seconds();
        assert!(uptime2 >= uptime1);
    }

    #[test]
    fn test_session_config_accessor() {
        let session_config = SessionConfig {
            max_sessions: 50,
            session_timeout_secs: 1800,
            cleanup_interval_secs: 30,
        };
        let manager = SessionManager::with_full_config(
            ExecutorMode::Mock,
            SecurityConfig::default(),
            session_config,
        );
        assert_eq!(manager.session_config().max_sessions, 50);
        assert_eq!(manager.session_config().session_timeout_secs, 1800);
    }
}
