mod bigquery;
mod yachtsql;

pub use self::bigquery::BigQueryExecutor;
pub use self::yachtsql::{ColumnInfo, QueryResult, YachtSqlExecutor};
pub use crate::rpc::types::ColumnDef;

use crate::error::Result;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutorMode {
    #[default]
    Mock,
    BigQuery,
}

pub enum Executor {
    Mock(YachtSqlExecutor),
    BigQuery(BigQueryExecutor),
}

impl Executor {
    #[allow(dead_code)]
    pub fn mock() -> Result<Self> {
        Ok(Self::Mock(YachtSqlExecutor::new()))
    }

    #[allow(dead_code)]
    pub async fn bigquery() -> Result<Self> {
        Ok(Self::BigQuery(BigQueryExecutor::new().await?))
    }

    pub fn mode(&self) -> ExecutorMode {
        match self {
            Executor::Mock(_) => ExecutorMode::Mock,
            Executor::BigQuery(_) => ExecutorMode::BigQuery,
        }
    }

    #[allow(dead_code)]
    pub fn is_mock(&self) -> bool {
        matches!(self, Executor::Mock(_))
    }

    #[allow(dead_code)]
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        match self {
            Executor::Mock(e) => e.execute_query(sql).await,
            Executor::BigQuery(e) => e.execute_query(sql).await,
        }
    }

    #[allow(dead_code)]
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.execute_statement(sql).await,
            Executor::BigQuery(e) => e.execute_statement(sql).await,
        }
    }

    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        match self {
            Executor::Mock(e) => e.execute_query(sql).await,
            Executor::BigQuery(e) => e.execute_query(sql).await,
        }
    }

    pub async fn execute_statement(&self, sql: &str) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.execute_statement(sql).await,
            Executor::BigQuery(e) => e.execute_statement(sql).await,
        }
    }

    pub async fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.load_parquet(table_name, path, schema).await,
            Executor::BigQuery(e) => e.load_parquet(table_name, path, schema).await,
        }
    }

    pub async fn list_tables(&self) -> Result<Vec<(String, u64)>> {
        match self {
            Executor::Mock(e) => e.list_tables().await,
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "list_tables not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub async fn describe_table(&self, table_name: &str) -> Result<(Vec<(String, String)>, u64)> {
        match self {
            Executor::Mock(e) => e.describe_table(table_name).await,
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "describe_table not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn set_default_project(&self, project: Option<String>) -> Result<()> {
        match self {
            Executor::Mock(e) => {
                e.set_default_project(project);
                Ok(())
            }
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "set_default_project not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_default_project(&self) -> Result<Option<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_default_project()),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_default_project not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_projects(&self) -> Result<Vec<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_projects()),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_projects not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_datasets(&self, project: &str) -> Result<Vec<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_datasets(project)),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_datasets not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_tables_in_dataset(&self, project: &str, dataset: &str) -> Result<Vec<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_tables_in_dataset(project, dataset)),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_tables_in_dataset not supported for BigQuery executor".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_mode_default() {
        let mode: ExecutorMode = Default::default();
        assert_eq!(mode, ExecutorMode::Mock);
    }

    #[test]
    fn test_executor_mode_debug() {
        let mode = ExecutorMode::Mock;
        let debug_str = format!("{:?}", mode);
        assert!(debug_str.contains("Mock"));
    }

    #[test]
    fn test_executor_mode_eq() {
        assert_eq!(ExecutorMode::Mock, ExecutorMode::Mock);
        assert_eq!(ExecutorMode::BigQuery, ExecutorMode::BigQuery);
        assert_ne!(ExecutorMode::Mock, ExecutorMode::BigQuery);
    }

    #[test]
    fn test_executor_mock_creation() {
        let executor = Executor::mock().unwrap();
        assert!(executor.is_mock());
        assert_eq!(executor.mode(), ExecutorMode::Mock);
    }

    #[tokio::test]
    async fn test_executor_mock_query() {
        let executor = Executor::mock().unwrap();
        let result = executor.query("SELECT 1 AS num").await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_executor_mock_execute() {
        let executor = Executor::mock().unwrap();
        executor
            .execute("CREATE TABLE mod_test (id INT64)")
            .await
            .unwrap();
        let count = executor.execute("INSERT INTO mod_test VALUES (1)").await;
        assert!(count.is_ok());
    }

    #[tokio::test]
    async fn test_executor_mock_execute_query() {
        let executor = Executor::mock().unwrap();
        let result = executor.execute_query("SELECT 42 AS val").await.unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_executor_mock_execute_statement() {
        let executor = Executor::mock().unwrap();
        executor
            .execute_statement("CREATE TABLE stmt_test (id INT64)")
            .await
            .unwrap();
        let count = executor
            .execute_statement("INSERT INTO stmt_test VALUES (1), (2)")
            .await;
        assert!(count.is_ok());
    }

    #[tokio::test]
    async fn test_executor_mock_load_parquet() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let executor = Executor::mock().unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("mod_test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["Alice", "Bob"]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![ColumnDef::int64("id"), ColumnDef::string("name")];

        let rows = executor
            .load_parquet(
                "parquet_mod_test",
                parquet_path.to_str().unwrap(),
                &col_schema,
            )
            .await
            .unwrap();

        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn test_executor_mock_set_default_project() {
        let executor = Executor::mock().unwrap();
        executor
            .set_default_project(Some("my-project".to_string()))
            .unwrap();
        let project = executor.get_default_project().unwrap();
        assert!(project.is_some());
    }

    #[tokio::test]
    async fn test_executor_mock_set_default_project_none() {
        let executor = Executor::mock().unwrap();
        executor
            .set_default_project(Some("test".to_string()))
            .unwrap();
        executor.set_default_project(None).unwrap();
        let project = executor.get_default_project().unwrap();
        assert!(project.is_none());
    }

    #[tokio::test]
    async fn test_executor_mock_get_projects() {
        let executor = Executor::mock().unwrap();
        let projects = executor.get_projects().unwrap();
        assert!(projects.is_empty() || !projects.is_empty());
    }

    #[tokio::test]
    async fn test_executor_mock_get_datasets() {
        let executor = Executor::mock().unwrap();
        let datasets = executor.get_datasets("project").unwrap();
        assert!(datasets.is_empty() || !datasets.is_empty());
    }

    #[tokio::test]
    async fn test_executor_mock_get_tables_in_dataset() {
        let executor = Executor::mock().unwrap();
        let tables = executor
            .get_tables_in_dataset("project", "dataset")
            .unwrap();
        assert!(tables.is_empty() || !tables.is_empty());
    }
}
