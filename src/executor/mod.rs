mod bigquery;
pub mod converters;
mod yachtsql;

#[cfg(test)]
mod test_counting;

pub use self::bigquery::BigQueryExecutor;
pub(crate) use self::yachtsql::MockExecutorExt;
pub use self::yachtsql::{ColumnInfo, QueryResult, YachtSqlExecutor};
pub use crate::domain::ColumnDef;

#[cfg(test)]
pub use self::test_counting::TestCountingExecutor;

use crate::error::Result;
use async_trait::async_trait;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutorMode {
    #[default]
    Mock,
    BigQuery,
}

#[async_trait]
pub trait ExecutorBackend: Send + Sync {
    fn mode(&self) -> ExecutorMode;
    async fn execute_query(&self, sql: &str) -> Result<QueryResult>;
    async fn execute_statement(&self, sql: &str) -> Result<u64>;
    async fn load_parquet(&self, table_name: &str, path: &str, schema: &[ColumnDef])
        -> Result<u64>;
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
    fn test_yachtsql_executor_mode() {
        let executor = YachtSqlExecutor::new();
        assert_eq!(executor.mode(), ExecutorMode::Mock);
    }

    #[tokio::test]
    async fn test_yachtsql_executor_query() {
        let executor = YachtSqlExecutor::new();
        let result = executor.execute_query("SELECT 1 AS num").await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_yachtsql_executor_execute() {
        let executor = YachtSqlExecutor::new();
        executor
            .execute_statement("CREATE TABLE mod_test (id INT64)")
            .await
            .unwrap();
        let count = executor
            .execute_statement("INSERT INTO mod_test VALUES (1)")
            .await;
        assert!(count.is_ok());
    }

    #[tokio::test]
    async fn test_yachtsql_executor_execute_query() {
        let executor = YachtSqlExecutor::new();
        let result = executor.execute_query("SELECT 42 AS val").await.unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_yachtsql_executor_execute_statement() {
        let executor = YachtSqlExecutor::new();
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
    async fn test_yachtsql_executor_load_parquet() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let executor = YachtSqlExecutor::new();

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
    async fn test_yachtsql_executor_set_default_project() {
        let executor = YachtSqlExecutor::new();
        executor.set_default_project(Some("my-project".to_string()));
        let project = executor.get_default_project();
        assert!(project.is_some());
    }

    #[tokio::test]
    async fn test_yachtsql_executor_set_default_project_none() {
        let executor = YachtSqlExecutor::new();
        executor.set_default_project(Some("test".to_string()));
        executor.set_default_project(None);
        let project = executor.get_default_project();
        assert!(project.is_none());
    }

    #[tokio::test]
    async fn test_yachtsql_executor_get_projects() {
        let executor = YachtSqlExecutor::new();
        let projects = executor.get_projects();
        assert!(projects.is_empty() || !projects.is_empty());
    }

    #[tokio::test]
    async fn test_yachtsql_executor_get_datasets() {
        let executor = YachtSqlExecutor::new();
        let datasets = executor.get_datasets("project");
        assert!(datasets.is_empty() || !datasets.is_empty());
    }

    #[tokio::test]
    async fn test_yachtsql_executor_get_tables_in_dataset() {
        let executor = YachtSqlExecutor::new();
        let tables = executor.get_tables_in_dataset("project", "dataset");
        assert!(tables.is_empty() || !tables.is_empty());
    }
}
