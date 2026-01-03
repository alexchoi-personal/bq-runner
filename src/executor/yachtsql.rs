use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value as JsonValue};
use yachtsql::{AsyncQueryExecutor, Table};

use super::converters::{arrow_value_to_sql_into, datatype_to_bq_type, yacht_value_into_json};
use super::{ExecutorBackend, ExecutorMode};
use crate::domain::ColumnDef;
use crate::error::{Error, Result};
use crate::validation::{open_file_secure, quote_identifier};

pub(crate) trait MockExecutorExt {
    fn list_tables(&self) -> impl std::future::Future<Output = Result<Vec<(String, u64)>>> + Send;
    fn describe_table(
        &self,
        table_name: &str,
    ) -> impl std::future::Future<Output = Result<(Vec<(String, String)>, u64)>> + Send;
    fn set_default_project(&self, project: Option<String>);
    fn get_default_project(&self) -> Option<String>;
    fn get_projects(&self) -> Vec<String>;
    fn get_datasets(&self, project: &str) -> Vec<String>;
    fn get_tables_in_dataset(&self, project: &str, dataset: &str) -> Vec<String>;
}

#[derive(Clone)]
pub struct YachtSqlExecutor {
    executor: AsyncQueryExecutor,
}

impl YachtSqlExecutor {
    pub fn new() -> Self {
        Self {
            executor: AsyncQueryExecutor::new(),
        }
    }

    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let result = self
            .executor
            .execute_sql(sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        table_to_query_result(&result)
    }

    pub async fn execute_statement(&self, sql: &str) -> Result<u64> {
        let result = self
            .executor
            .execute_sql(sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        Ok(result.row_count() as u64)
    }

    async fn load_parquet_impl(
        &self,
        table_name: &str,
        path: &str,
        schema: &[ColumnDef],
    ) -> Result<u64> {
        let path_buf = PathBuf::from(path);
        let batches = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
            read_parquet_batches(&path_buf)
        })
        .await
        .map_err(|e| Error::Executor(format!("Parquet read task failed: {}", e)))??;

        let quoted_table = format!("`{}`", quote_identifier(table_name));
        let columns: Vec<String> = schema
            .iter()
            .map(|col| format!("`{}` {}", quote_identifier(&col.name), col.column_type))
            .collect();

        let create_sql = format!(
            "CREATE OR REPLACE TABLE {} ({})",
            quoted_table,
            columns.join(", ")
        );

        self.executor
            .execute_sql(&create_sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, create_sql)))?;

        let mut total_rows = 0u64;
        const INSERT_BATCH_SIZE: usize = 1000;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let num_cols = schema.len().min(batch.num_columns());
            let columns: Vec<_> = (0..num_cols).map(|i| batch.column(i)).collect();
            let bq_types: Vec<_> = schema
                .iter()
                .take(num_cols)
                .map(|c| c.column_type.as_str())
                .collect();

            let mut batch_buffer = String::with_capacity(INSERT_BATCH_SIZE * num_cols * 16);
            let mut rows_in_batch = 0;

            for row_idx in 0..batch.num_rows() {
                if rows_in_batch > 0 {
                    batch_buffer.push_str(", ");
                }
                batch_buffer.push('(');
                for (col_idx, col) in columns.iter().enumerate() {
                    if col_idx > 0 {
                        batch_buffer.push_str(", ");
                    }
                    arrow_value_to_sql_into(
                        col.as_ref(),
                        row_idx,
                        bq_types[col_idx],
                        &mut batch_buffer,
                    );
                }
                batch_buffer.push(')');
                rows_in_batch += 1;

                if rows_in_batch >= INSERT_BATCH_SIZE {
                    let insert_sql =
                        format!("INSERT INTO {} VALUES {}", quoted_table, batch_buffer);
                    self.executor
                        .execute_sql(&insert_sql)
                        .await
                        .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, insert_sql)))?;
                    batch_buffer.clear();
                    rows_in_batch = 0;
                }
            }

            if rows_in_batch > 0 {
                let insert_sql = format!("INSERT INTO {} VALUES {}", quoted_table, batch_buffer);
                self.executor
                    .execute_sql(&insert_sql)
                    .await
                    .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, insert_sql)))?;
            }

            total_rows += batch.num_rows() as u64;
        }

        Ok(total_rows)
    }
}

fn read_parquet_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = open_file_secure(path)
        .map_err(|e| Error::Executor(format!("Failed to open parquet file: {}", e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Executor(format!("Failed to read parquet: {}", e)))?;

    let reader = builder
        .build()
        .map_err(|e| Error::Executor(format!("Failed to build parquet reader: {}", e)))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| Error::Executor(format!("Failed to read batch: {}", e)))?;
        batches.push(batch);
    }
    Ok(batches)
}

impl Default for YachtSqlExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutorBackend for YachtSqlExecutor {
    fn mode(&self) -> ExecutorMode {
        ExecutorMode::Mock
    }

    async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let result = self
            .executor
            .execute_sql(sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        table_to_query_result(&result)
    }

    async fn execute_statement(&self, sql: &str) -> Result<u64> {
        let result = self
            .executor
            .execute_sql(sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        Ok(result.row_count() as u64)
    }

    async fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[ColumnDef],
    ) -> Result<u64> {
        self.load_parquet_impl(table_name, path, schema).await
    }
}

impl MockExecutorExt for YachtSqlExecutor {
    async fn list_tables(&self) -> Result<Vec<(String, u64)>> {
        let result = self.execute_query(
            "SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema = 'public'"
        ).await?;

        let tables: Vec<(String, u64)> = result
            .rows
            .into_iter()
            .filter_map(|row| {
                let name = row.first().and_then(|v| v.as_str())?;
                let row_count = row.get(1).and_then(|v| v.as_u64()).unwrap_or(0);
                Some((name.to_string(), row_count))
            })
            .collect();

        Ok(tables)
    }

    async fn describe_table(&self, table_name: &str) -> Result<(Vec<(String, String)>, u64)> {
        let escaped_name = table_name.replace('\'', "''");
        let schema_sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
            escaped_name
        );
        let schema_result = self.execute_query(&schema_sql).await?;

        let schema: Vec<(String, String)> = schema_result
            .rows
            .into_iter()
            .filter_map(|row| {
                let name = row.first().and_then(|v| v.as_str())?;
                let col_type = row.get(1).and_then(|v| v.as_str()).unwrap_or("STRING");
                Some((name.to_string(), col_type.to_string()))
            })
            .collect();

        let quoted_name = format!("`{}`", quote_identifier(table_name));
        let count_sql = format!("SELECT COUNT(*) FROM {}", quoted_name);
        let count_result = self.execute_query(&count_sql).await?;
        let row_count = count_result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok((schema, row_count))
    }

    fn set_default_project(&self, project: Option<String>) {
        self.executor.catalog().set_default_project(project);
    }

    fn get_default_project(&self) -> Option<String> {
        self.executor.catalog().get_default_project()
    }

    fn get_projects(&self) -> Vec<String> {
        self.executor.catalog().get_projects()
    }

    fn get_datasets(&self, project: &str) -> Vec<String> {
        self.executor.catalog().get_datasets(project)
    }

    fn get_tables_in_dataset(&self, project: &str, dataset: &str) -> Vec<String> {
        self.executor
            .catalog()
            .get_tables_in_dataset(project, dataset)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<JsonValue>>,
}

impl QueryResult {
    pub fn to_bq_response(&self) -> JsonValue {
        let schema_fields: Vec<JsonValue> = self
            .columns
            .iter()
            .map(|col| json!({ "name": col.name, "type": col.data_type }))
            .collect();

        let rows: Vec<JsonValue> = self
            .rows
            .iter()
            .map(|row| {
                let fields: Vec<JsonValue> = row.iter().map(|v| json!({ "v": v })).collect();
                json!({ "f": fields })
            })
            .collect();

        json!({
            "kind": "bigquery#queryResponse",
            "schema": { "fields": schema_fields },
            "rows": rows,
            "totalRows": self.rows.len().to_string(),
            "jobComplete": true
        })
    }
}

fn table_to_query_result(table: &Table) -> Result<QueryResult> {
    let schema = table.schema();
    let columns: Vec<ColumnInfo> = schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name.clone(),
            data_type: datatype_to_bq_type(&f.data_type),
        })
        .collect();

    let records = table
        .to_records()
        .map_err(|e| Error::Executor(e.to_string()))?;
    let rows: Vec<Vec<JsonValue>> = records
        .into_iter()
        .map(|record| {
            record
                .into_values()
                .into_iter()
                .map(yacht_value_into_json)
                .collect()
        })
        .collect();

    Ok(QueryResult { columns, rows })
}

#[cfg(test)]
mod tests {
    use super::super::converters::{arrow_value_to_sql, base64_encode, datatype_to_bq_type};
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use std::sync::Arc;
    use yachtsql::DataType;

    #[tokio::test]
    async fn test_yachtsql_executor_new() {
        let executor = YachtSqlExecutor::new();
        assert!(executor.get_default_project().is_none());
    }

    #[tokio::test]
    async fn test_yachtsql_executor_default() {
        let executor = YachtSqlExecutor::default();
        assert!(executor.get_default_project().is_none());
    }

    #[tokio::test]
    async fn test_execute_query_basic() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT 1 AS num, 'hello' AS str")
            .await
            .unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "num");
        assert_eq!(result.columns[1].name, "str");
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_execute_query_error() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT * FROM nonexistent_table")
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, crate::error::Error::Executor(_)));
    }

    #[tokio::test]
    async fn test_execute_statement_basic() {
        let executor = YachtSqlExecutor::new();
        executor
            .execute_statement("CREATE TABLE test_stmt (id INT64)")
            .await
            .unwrap();
        let count = executor
            .execute_statement("INSERT INTO test_stmt VALUES (1), (2)")
            .await;
        assert!(count.is_ok());
    }

    #[tokio::test]
    async fn test_execute_statement_error() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_statement("INSERT INTO nonexistent VALUES (1)")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_tables() {
        let executor = YachtSqlExecutor::new();
        executor
            .execute_statement("CREATE TABLE list_test (id INT64)")
            .await
            .unwrap();
        let result = executor.list_tables().await;
        assert!(
            result.is_err(),
            "Expected error (information_schema not supported) but got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_describe_table() {
        let executor = YachtSqlExecutor::new();
        executor
            .execute_statement("CREATE TABLE desc_test (id INT64, name STRING)")
            .await
            .unwrap();
        let result = executor.describe_table("desc_test").await;
        assert!(
            result.is_err(),
            "Expected error (information_schema not supported) but got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_set_get_default_project() {
        let executor = YachtSqlExecutor::new();
        assert!(executor.get_default_project().is_none());
        executor.set_default_project(Some("my-project".to_string()));
        let project = executor.get_default_project();
        assert!(project.is_some());
        executor.set_default_project(None);
        assert!(executor.get_default_project().is_none());
    }

    #[tokio::test]
    async fn test_get_projects() {
        let executor = YachtSqlExecutor::new();
        let projects = executor.get_projects();
        assert!(projects.is_empty(), "New executor should have no projects");
    }

    #[tokio::test]
    async fn test_get_datasets() {
        let executor = YachtSqlExecutor::new();
        let datasets = executor.get_datasets("some_project");
        assert!(datasets.is_empty(), "New executor should have no datasets");
    }

    #[tokio::test]
    async fn test_get_tables_in_dataset() {
        let executor = YachtSqlExecutor::new();
        let tables = executor.get_tables_in_dataset("proj", "dataset");
        assert!(tables.is_empty(), "New executor should have no tables");
    }

    #[tokio::test]
    async fn test_load_parquet_file_not_found() {
        let executor = YachtSqlExecutor::new();
        let schema = vec![crate::domain::ColumnDef::int64("id")];
        let result = executor
            .load_parquet("test_table", "/nonexistent/file.parquet", &schema)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_parquet_success() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![
            crate::domain::ColumnDef::int64("id"),
            crate::domain::ColumnDef::string("name"),
        ];

        let rows = executor
            .load_parquet("parquet_test", parquet_path.to_str().unwrap(), &col_schema)
            .await
            .unwrap();

        assert_eq!(rows, 3);

        let result = executor
            .execute_query("SELECT * FROM parquet_test")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_load_parquet_with_nulls() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("nulls.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            true,
        )]));

        let id_array = Int64Array::from(vec![Some(1), None, Some(3)]);

        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)])
                .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![crate::domain::ColumnDef::int64("id")];

        let rows = executor
            .load_parquet("nulls_test", parquet_path.to_str().unwrap(), &col_schema)
            .await
            .unwrap();

        assert_eq!(rows, 3);
    }

    #[tokio::test]
    async fn test_load_parquet_various_types() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("types.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("bool_col", ArrowDataType::Boolean, false),
            Field::new("i8_col", ArrowDataType::Int8, false),
            Field::new("i16_col", ArrowDataType::Int16, false),
            Field::new("i32_col", ArrowDataType::Int32, false),
            Field::new("i64_col", ArrowDataType::Int64, false),
            Field::new("u8_col", ArrowDataType::UInt8, false),
            Field::new("u16_col", ArrowDataType::UInt16, false),
            Field::new("u32_col", ArrowDataType::UInt32, false),
            Field::new("u64_col", ArrowDataType::UInt64, false),
            Field::new("f32_col", ArrowDataType::Float32, false),
            Field::new("f64_col", ArrowDataType::Float64, false),
            Field::new("str_col", ArrowDataType::Utf8, false),
            Field::new("large_str_col", ArrowDataType::LargeUtf8, false),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int8Array::from(vec![1i8])),
                Arc::new(Int16Array::from(vec![2i16])),
                Arc::new(Int32Array::from(vec![3i32])),
                Arc::new(Int64Array::from(vec![4i64])),
                Arc::new(UInt8Array::from(vec![5u8])),
                Arc::new(UInt16Array::from(vec![6u16])),
                Arc::new(UInt32Array::from(vec![7u32])),
                Arc::new(UInt64Array::from(vec![8u64])),
                Arc::new(Float32Array::from(vec![9.5f32])),
                Arc::new(Float64Array::from(vec![10.5f64])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(LargeStringArray::from(vec!["world"])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![
            crate::domain::ColumnDef::bool("bool_col"),
            crate::domain::ColumnDef::int64("i8_col"),
            crate::domain::ColumnDef::int64("i16_col"),
            crate::domain::ColumnDef::int64("i32_col"),
            crate::domain::ColumnDef::int64("i64_col"),
            crate::domain::ColumnDef::int64("u8_col"),
            crate::domain::ColumnDef::int64("u16_col"),
            crate::domain::ColumnDef::int64("u32_col"),
            crate::domain::ColumnDef::int64("u64_col"),
            crate::domain::ColumnDef::float64("f32_col"),
            crate::domain::ColumnDef::float64("f64_col"),
            crate::domain::ColumnDef::string("str_col"),
            crate::domain::ColumnDef::string("large_str_col"),
        ];

        let rows = executor
            .load_parquet("types_test", parquet_path.to_str().unwrap(), &col_schema)
            .await
            .unwrap();

        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn test_load_parquet_date_types() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("dates.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("date32_col", ArrowDataType::Date32, false),
            Field::new("date64_col", ArrowDataType::Date64, false),
            Field::new("i64_date", ArrowDataType::Int64, false),
            Field::new("i64_ts", ArrowDataType::Int64, false),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Date32Array::from(vec![19000])),
                Arc::new(Date64Array::from(vec![1640995200000i64])),
                Arc::new(Int64Array::from(vec![19000i64])),
                Arc::new(Int64Array::from(vec![1640995200000000i64])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![
            crate::domain::ColumnDef::date("date32_col"),
            crate::domain::ColumnDef::date("date64_col"),
            crate::domain::ColumnDef::date("i64_date"),
            crate::domain::ColumnDef::timestamp("i64_ts"),
        ];

        let rows = executor
            .load_parquet("dates_test", parquet_path.to_str().unwrap(), &col_schema)
            .await
            .unwrap();

        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn test_load_parquet_timestamp_units() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("timestamps.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts_s",
                ArrowDataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new(
                "ts_ms",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts_us",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_ns",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampSecondArray::from(vec![1640995200i64])),
                Arc::new(TimestampMillisecondArray::from(vec![1640995200000i64])),
                Arc::new(TimestampMicrosecondArray::from(vec![1640995200000000i64])),
                Arc::new(TimestampNanosecondArray::from(vec![1640995200000000000i64])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![
            crate::domain::ColumnDef::timestamp("ts_s"),
            crate::domain::ColumnDef::timestamp("ts_ms"),
            crate::domain::ColumnDef::timestamp("ts_us"),
            crate::domain::ColumnDef::timestamp("ts_ns"),
        ];

        let rows = executor
            .load_parquet(
                "timestamps_test",
                parquet_path.to_str().unwrap(),
                &col_schema,
            )
            .await
            .unwrap();

        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn test_load_parquet_string_with_quotes() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("quotes.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "text",
            ArrowDataType::Utf8,
            false,
        )]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["it's a test"]))],
        )
        .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![crate::domain::ColumnDef::string("text")];

        let rows = executor
            .load_parquet("quotes_test", parquet_path.to_str().unwrap(), &col_schema)
            .await
            .unwrap();

        assert_eq!(rows, 1);
    }

    #[test]
    fn test_query_result_to_bq_response() {
        let result = QueryResult {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INT64".to_string(),
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "STRING".to_string(),
                },
            ],
            rows: vec![vec![json!(1), json!("Alice")], vec![json!(2), json!("Bob")]],
        };

        let response = result.to_bq_response();

        assert_eq!(response["kind"], "bigquery#queryResponse");
        assert_eq!(response["jobComplete"], true);
        assert_eq!(response["totalRows"], "2");
        assert!(response["schema"]["fields"].is_array());
        assert!(response["rows"].is_array());
    }

    #[test]
    fn test_query_result_to_bq_response_empty() {
        let result = QueryResult {
            columns: vec![],
            rows: vec![],
        };

        let response = result.to_bq_response();
        assert_eq!(response["totalRows"], "0");
    }

    #[test]
    fn test_datatype_to_bq_type_all_variants() {
        assert_eq!(datatype_to_bq_type(&DataType::Bool), "BOOLEAN");
        assert_eq!(datatype_to_bq_type(&DataType::Int64), "INT64");
        assert_eq!(datatype_to_bq_type(&DataType::Float64), "FLOAT64");
        assert_eq!(datatype_to_bq_type(&DataType::Numeric(None)), "NUMERIC");
        assert_eq!(datatype_to_bq_type(&DataType::BigNumeric), "NUMERIC");
        assert_eq!(datatype_to_bq_type(&DataType::String), "STRING");
        assert_eq!(datatype_to_bq_type(&DataType::Bytes), "BYTES");
        assert_eq!(datatype_to_bq_type(&DataType::Date), "DATE");
        assert_eq!(datatype_to_bq_type(&DataType::DateTime), "DATETIME");
        assert_eq!(datatype_to_bq_type(&DataType::Time), "TIME");
        assert_eq!(datatype_to_bq_type(&DataType::Timestamp), "TIMESTAMP");
        assert_eq!(datatype_to_bq_type(&DataType::Geography), "GEOGRAPHY");
        assert_eq!(datatype_to_bq_type(&DataType::Json), "JSON");
        assert_eq!(datatype_to_bq_type(&DataType::Struct(vec![])), "STRUCT");
        assert_eq!(
            datatype_to_bq_type(&DataType::Array(Box::new(DataType::Int64))),
            "ARRAY<INT64>"
        );
        assert_eq!(datatype_to_bq_type(&DataType::Interval), "INTERVAL");
        assert_eq!(
            datatype_to_bq_type(&DataType::Range(Box::new(DataType::Date))),
            "STRING"
        );
        assert_eq!(datatype_to_bq_type(&DataType::Unknown), "STRING");
    }

    #[tokio::test]
    async fn test_yacht_value_to_json_all_variants() {
        let executor = YachtSqlExecutor::new();

        let result = executor.execute_query("SELECT NULL AS n").await.unwrap();
        assert_eq!(result.rows[0][0], JsonValue::Null);

        let result = executor.execute_query("SELECT TRUE AS b").await.unwrap();
        assert_eq!(result.rows[0][0], JsonValue::Bool(true));

        let result = executor.execute_query("SELECT 42 AS i").await.unwrap();
        assert_eq!(result.rows[0][0], json!(42));

        let result = executor.execute_query("SELECT 3.14 AS f").await.unwrap();
        assert!(result.rows[0][0].as_f64().is_some());

        let result = executor.execute_query("SELECT 'hello' AS s").await.unwrap();
        assert_eq!(result.rows[0][0], json!("hello"));

        let result = executor
            .execute_query("SELECT DATE '2022-01-01' AS d")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());

        let result = executor
            .execute_query("SELECT TIME '12:30:00' AS t")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());

        let result = executor
            .execute_query("SELECT DATETIME '2022-01-01 12:30:00' AS dt")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());

        let result = executor
            .execute_query("SELECT TIMESTAMP '2022-01-01 12:30:00 UTC' AS ts")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());

        let result = executor
            .execute_query("SELECT [1, 2, 3] AS arr")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_array());

        let result = executor
            .execute_query("SELECT STRUCT(1 AS a, 'b' AS b) AS s")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_object());
    }

    #[tokio::test]
    async fn test_yacht_value_bytes() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT b'hello' AS b")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());
    }

    #[tokio::test]
    async fn test_yacht_value_json() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT JSON '{\"a\": 1}' AS j")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_object() || result.rows[0][0].is_string());
    }

    #[tokio::test]
    async fn test_yacht_value_numeric() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT NUMERIC '123.456' AS n")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string() || result.rows[0][0].is_number());
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_arrow_value_to_sql_null() {
        let arr = Int64Array::from(vec![Some(1), None]);
        let result = arrow_value_to_sql(&arr, 1, "INT64");
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_arrow_value_to_sql_binary_type() {
        let arr = arrow::array::BinaryArray::from(vec![Some(b"hello".as_slice())]);
        let result = arrow_value_to_sql(&arr, 0, "BYTES");
        assert_eq!(result, "FROM_BASE64('aGVsbG8=')");
    }

    #[test]
    fn test_arrow_value_to_sql_unsupported_type() {
        let arr = arrow::array::DurationSecondArray::from(vec![Some(100i64)]);
        let result = arrow_value_to_sql(&arr, 0, "INTERVAL");
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_column_info_struct() {
        let col = ColumnInfo {
            name: "test".to_string(),
            data_type: "INT64".to_string(),
        };
        assert_eq!(col.name, "test");
        assert_eq!(col.data_type, "INT64");
    }

    #[test]
    fn test_query_result_clone() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "INT64".to_string(),
            }],
            rows: vec![vec![json!(1)]],
        };
        let cloned = result.clone();
        assert_eq!(cloned.columns.len(), 1);
        assert_eq!(cloned.rows.len(), 1);
    }

    #[test]
    fn test_column_info_clone() {
        let col = ColumnInfo {
            name: "test".to_string(),
            data_type: "STRING".to_string(),
        };
        let cloned = col.clone();
        assert_eq!(cloned.name, "test");
    }

    #[tokio::test]
    async fn test_load_parquet_empty_batch() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let parquet_path = temp_dir.path().join("empty.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));

        let id_array: Int64Array = Int64Array::from(vec![] as Vec<i64>);

        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)])
                .unwrap();

        let file = std::fs::File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let col_schema = vec![crate::domain::ColumnDef::int64("id")];

        let rows = executor
            .load_parquet("empty_test", parquet_path.to_str().unwrap(), &col_schema)
            .await
            .unwrap();

        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn test_load_parquet_invalid_parquet() {
        let executor = YachtSqlExecutor::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let invalid_path = temp_dir.path().join("invalid.parquet");
        std::fs::write(&invalid_path, b"not a parquet file").unwrap();

        let col_schema = vec![crate::domain::ColumnDef::int64("id")];
        let result = executor
            .load_parquet("invalid_test", invalid_path.to_str().unwrap(), &col_schema)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_table_to_query_result_error() {
        let executor = YachtSqlExecutor::new();
        executor
            .execute_statement("CREATE TABLE err_test (id INT64)")
            .await
            .unwrap();
        let result = executor.execute_query("SELECT * FROM err_test").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_yacht_value_interval() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT INTERVAL 1 DAY AS i")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());
    }

    #[tokio::test]
    async fn test_yacht_value_bignumeric() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT BIGNUMERIC '12345678901234567890.123456789' AS bn")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string() || result.rows[0][0].is_number());
    }

    #[tokio::test]
    async fn test_yacht_value_geography() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT ST_GEOGPOINT(-122.4194, 37.7749) AS g")
            .await
            .unwrap();
        assert!(result.rows[0][0].is_string());
    }

    #[test]
    fn test_large_string_with_quotes() {
        let arr = LargeStringArray::from(vec!["it's a test"]);
        let result = arrow_value_to_sql(&arr, 0, "STRING");
        assert_eq!(result, "'it''s a test'");
    }

    #[test]
    fn test_int64_with_date_type() {
        let arr = Int64Array::from(vec![19000i64]);
        let result = arrow_value_to_sql(&arr, 0, "DATE");
        assert_eq!(result, "DATE_FROM_UNIX_DATE(19000)");
    }

    #[test]
    fn test_int64_with_timestamp_type() {
        let arr = Int64Array::from(vec![1640995200000000i64]);
        let result = arrow_value_to_sql(&arr, 0, "TIMESTAMP");
        assert_eq!(result, "TIMESTAMP_MICROS(1640995200000000)");
    }

    #[test]
    fn test_int64_with_regular_type() {
        let arr = Int64Array::from(vec![42i64]);
        let result = arrow_value_to_sql(&arr, 0, "INT64");
        assert_eq!(result, "42");
    }

    #[tokio::test]
    async fn test_yacht_value_range() {
        let executor = YachtSqlExecutor::new();
        let result = executor
            .execute_query("SELECT RANGE(DATE '2020-01-01', DATE '2020-12-31') AS r")
            .await;
        assert!(
            result.is_ok(),
            "Expected RANGE query to succeed but got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_yacht_value_default() {
        let executor = YachtSqlExecutor::new();
        executor
            .execute_statement("CREATE TABLE def_test (id INT64 DEFAULT 0)")
            .await
            .unwrap();
        let result = executor.execute_query("SELECT * FROM def_test").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_result_debug() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "INT64".to_string(),
            }],
            rows: vec![vec![json!(1)]],
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("QueryResult"));
    }

    #[test]
    fn test_column_info_debug() {
        let col = ColumnInfo {
            name: "test".to_string(),
            data_type: "STRING".to_string(),
        };
        let debug_str = format!("{:?}", col);
        assert!(debug_str.contains("ColumnInfo"));
    }
}
