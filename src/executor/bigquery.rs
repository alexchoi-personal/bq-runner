use async_trait::async_trait;
use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::cancel::CancelJobRequest;
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::job::{
    Job, JobConfiguration, JobConfigurationLoad, JobReference, JobState, JobType, WriteDisposition,
};
use google_cloud_bigquery::http::table::{
    SourceFormat, TableFieldSchema, TableFieldType, TableReference, TableSchema,
};
use google_cloud_bigquery::http::tabledata::list::Value as BqValue;
use serde_json::Value as JsonValue;

use super::yachtsql::ColumnInfo;
use super::{ExecutorBackend, ExecutorMode, QueryResult};
use crate::domain::ColumnDef;
use crate::error::{Error, Result};

pub struct BigQueryExecutor {
    client: Client,
    project_id: String,
    dataset_id: Option<String>,
    query_timeout_ms: Option<i64>,
}

impl BigQueryExecutor {
    pub async fn new() -> Result<Self> {
        Self::with_config(None, None).await
    }

    pub async fn with_config(
        config: Option<ClientConfig>,
        project_id: Option<String>,
    ) -> Result<Self> {
        let (config, project_id) = match config {
            Some(c) => (c, project_id),
            None => {
                let (c, p) = ClientConfig::new_with_auth()
                    .await
                    .map_err(|e| Error::Executor(format!("Failed to authenticate: {}", e)))?;
                (c, p)
            }
        };

        let project_id =
            project_id.ok_or_else(|| Error::Executor("No project_id in credentials".into()))?;

        let client = Client::new(config)
            .await
            .map_err(|e| Error::Executor(format!("Failed to create BigQuery client: {}", e)))?;

        let dataset_id = std::env::var("BQ_DATASET").ok();
        let query_timeout_ms = std::env::var("BQ_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok());

        Ok(Self {
            client,
            project_id,
            dataset_id,
            query_timeout_ms,
        })
    }

    async fn cancel_job(&self, job_id: &str) -> Result<()> {
        tracing::info!(job_id = %job_id, "Attempting to cancel BigQuery job");

        let request = CancelJobRequest { location: None };

        match self
            .client
            .job()
            .cancel(&self.project_id, job_id, &request)
            .await
        {
            Ok(_) => {
                tracing::info!(job_id = %job_id, "BigQuery job cancelled successfully");
                Ok(())
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("404") || error_str.contains("notFound") {
                    tracing::debug!(job_id = %job_id, "Job already completed, nothing to cancel");
                    Ok(())
                } else {
                    tracing::warn!(job_id = %job_id, error = %e, "Failed to cancel BigQuery job");
                    Err(Error::BigQuery(format!(
                        "Failed to cancel job {}: {}",
                        job_id, e
                    )))
                }
            }
        }
    }

    async fn load_parquet_impl(
        &self,
        table_name: &str,
        path: &str,
        schema: &[ColumnDef],
    ) -> Result<u64> {
        if !path.starts_with("gs://") {
            return Err(Error::Executor(
                "BigQuery load_parquet requires a GCS path (gs://bucket/path)".to_string(),
            ));
        }

        let dataset_id = self.dataset_id.as_ref().ok_or_else(|| {
            Error::Executor("BQ_DATASET environment variable must be set for load_parquet".into())
        })?;

        let table_schema = TableSchema {
            fields: schema
                .iter()
                .map(|col| TableFieldSchema {
                    name: col.name.clone(),
                    data_type: string_to_bq_type(col.column_type.as_str()),
                    ..Default::default()
                })
                .collect(),
        };

        let load_config = JobConfigurationLoad {
            source_uris: vec![path.to_string()],
            destination_table: TableReference {
                project_id: self.project_id.clone(),
                dataset_id: dataset_id.to_string(),
                table_id: table_name.to_string(),
            },
            schema: Some(table_schema),
            source_format: Some(SourceFormat::Parquet),
            write_disposition: Some(WriteDisposition::WriteTruncate),
            ..Default::default()
        };

        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: format!("load_parquet_{}", uuid::Uuid::new_v4()),
                location: None,
            },
            configuration: JobConfiguration {
                job_type: "LOAD".to_string(),
                job: JobType::Load(load_config),
                ..Default::default()
            },
            ..Default::default()
        };

        let created_job = self
            .client
            .job()
            .create(&job)
            .await
            .map_err(|e| Error::Executor(format!("Failed to create load job: {}", e)))?;

        let job_id = &created_job.job_reference.job_id;
        if job_id.is_empty() {
            return Err(Error::Executor(
                "Load job created but no job ID returned".into(),
            ));
        }

        let start = std::time::Instant::now();
        let mut interval = std::time::Duration::from_secs(1);
        let max_interval = std::time::Duration::from_secs(30);
        let timeout = std::time::Duration::from_secs(1800);
        let deadline = start + timeout;
        let mut poll_count = 0u32;

        loop {
            poll_count += 1;
            let elapsed = start.elapsed();

            if poll_count.is_multiple_of(10) {
                tracing::info!(
                    job_id = %job_id,
                    poll_count = poll_count,
                    elapsed_secs = elapsed.as_secs(),
                    "BigQuery job polling in progress"
                );
            }

            if std::time::Instant::now() > deadline {
                tracing::warn!(
                    job_id = %job_id,
                    poll_count = poll_count,
                    timeout_secs = timeout.as_secs(),
                    "BigQuery job timed out, attempting cancellation"
                );
                if let Err(e) = self.cancel_job(job_id).await {
                    tracing::warn!(job_id = %job_id, error = %e, "Cancellation failed");
                }
                return Err(Error::Timeout {
                    job_id: job_id.to_string(),
                    elapsed_secs: timeout.as_secs(),
                });
            }

            let get_request = GetJobRequest { location: None };
            let status = self
                .client
                .job()
                .get(&self.project_id, job_id, &get_request)
                .await
                .map_err(|e| Error::Executor(format!("Failed to get job status: {}", e)))?;

            if status.status.state == JobState::Done {
                if let Some(err) = &status.status.error_result {
                    return Err(Error::Executor(format!(
                        "Load job failed: {:?}",
                        err.message
                    )));
                }

                tracing::debug!(
                    job_id = %job_id,
                    poll_count = poll_count,
                    elapsed_secs = elapsed.as_secs(),
                    "BigQuery job completed"
                );

                let rows = status
                    .statistics
                    .and_then(|s| s.load)
                    .and_then(|l| l.output_rows)
                    .unwrap_or(0) as u64;

                return Ok(rows);
            }

            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(max_interval);
        }
    }

    async fn execute_query_impl(&self, sql: &str) -> Result<QueryResult> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            timeout_ms: self.query_timeout_ms,
            ..Default::default()
        };

        let response = self
            .client
            .job()
            .query(&self.project_id, &request)
            .await
            .map_err(|e| Error::Executor(format!("BigQuery query failed: {}", e)))?;

        let columns: Vec<ColumnInfo> = response
            .schema
            .as_ref()
            .map(|s| {
                s.fields
                    .iter()
                    .map(|field| ColumnInfo {
                        name: field.name.clone(),
                        data_type: bq_type_to_string(&field.data_type),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let rows: Vec<Vec<JsonValue>> = response
            .rows
            .unwrap_or_default()
            .into_iter()
            .map(|tuple| {
                tuple
                    .f
                    .into_iter()
                    .map(|cell| bq_value_to_json(cell.v))
                    .collect()
            })
            .collect();

        Ok(QueryResult { columns, rows })
    }

    async fn execute_statement_impl(&self, sql: &str) -> Result<u64> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            timeout_ms: self.query_timeout_ms,
            ..Default::default()
        };

        let response = self
            .client
            .job()
            .query(&self.project_id, &request)
            .await
            .map_err(|e| Error::Executor(format!("BigQuery statement failed: {}", e)))?;

        Ok(response.num_dml_affected_rows.unwrap_or(0) as u64)
    }
}

#[async_trait]
impl ExecutorBackend for BigQueryExecutor {
    fn mode(&self) -> ExecutorMode {
        ExecutorMode::BigQuery
    }

    async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        self.execute_query_impl(sql).await
    }

    async fn execute_statement(&self, sql: &str) -> Result<u64> {
        self.execute_statement_impl(sql).await
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

fn bq_type_to_string(field_type: &TableFieldType) -> String {
    match field_type {
        TableFieldType::String => "STRING".to_string(),
        TableFieldType::Bytes => "BYTES".to_string(),
        TableFieldType::Integer | TableFieldType::Int64 => "INT64".to_string(),
        TableFieldType::Float | TableFieldType::Float64 => "FLOAT64".to_string(),
        TableFieldType::Boolean | TableFieldType::Bool => "BOOLEAN".to_string(),
        TableFieldType::Timestamp => "TIMESTAMP".to_string(),
        TableFieldType::Record | TableFieldType::Struct => "STRUCT".to_string(),
        TableFieldType::Date => "DATE".to_string(),
        TableFieldType::Time => "TIME".to_string(),
        TableFieldType::Datetime => "DATETIME".to_string(),
        TableFieldType::Numeric | TableFieldType::Decimal => "NUMERIC".to_string(),
        TableFieldType::Bignumeric | TableFieldType::Bigdecimal => "BIGNUMERIC".to_string(),
        TableFieldType::Interval => "INTERVAL".to_string(),
        TableFieldType::Json => "JSON".to_string(),
    }
}

fn string_to_bq_type(type_str: &str) -> TableFieldType {
    match type_str.to_uppercase().as_str() {
        "STRING" => TableFieldType::String,
        "BYTES" => TableFieldType::Bytes,
        "INT64" | "INTEGER" => TableFieldType::Int64,
        "FLOAT64" | "FLOAT" => TableFieldType::Float64,
        "BOOLEAN" | "BOOL" => TableFieldType::Boolean,
        "TIMESTAMP" => TableFieldType::Timestamp,
        "DATE" => TableFieldType::Date,
        "TIME" => TableFieldType::Time,
        "DATETIME" => TableFieldType::Datetime,
        "NUMERIC" | "DECIMAL" => TableFieldType::Numeric,
        "BIGNUMERIC" | "BIGDECIMAL" => TableFieldType::Bignumeric,
        "INTERVAL" => TableFieldType::Interval,
        "JSON" => TableFieldType::Json,
        "STRUCT" | "RECORD" => TableFieldType::Struct,
        _ => TableFieldType::String,
    }
}

fn bq_value_to_json(value: BqValue) -> JsonValue {
    match value {
        BqValue::Null => JsonValue::Null,
        BqValue::String(s) => JsonValue::String(s),
        BqValue::Array(cells) => {
            JsonValue::Array(cells.into_iter().map(|c| bq_value_to_json(c.v)).collect())
        }
        BqValue::Struct(tuple) => {
            JsonValue::Array(tuple.f.into_iter().map(|c| bq_value_to_json(c.v)).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_bigquery::http::table::TableFieldType;
    use serde_json::json;

    #[test]
    fn test_bq_type_to_string_all_types() {
        assert_eq!(bq_type_to_string(&TableFieldType::String), "STRING");
        assert_eq!(bq_type_to_string(&TableFieldType::Bytes), "BYTES");
        assert_eq!(bq_type_to_string(&TableFieldType::Integer), "INT64");
        assert_eq!(bq_type_to_string(&TableFieldType::Int64), "INT64");
        assert_eq!(bq_type_to_string(&TableFieldType::Float), "FLOAT64");
        assert_eq!(bq_type_to_string(&TableFieldType::Float64), "FLOAT64");
        assert_eq!(bq_type_to_string(&TableFieldType::Boolean), "BOOLEAN");
        assert_eq!(bq_type_to_string(&TableFieldType::Bool), "BOOLEAN");
        assert_eq!(bq_type_to_string(&TableFieldType::Timestamp), "TIMESTAMP");
        assert_eq!(bq_type_to_string(&TableFieldType::Record), "STRUCT");
        assert_eq!(bq_type_to_string(&TableFieldType::Struct), "STRUCT");
        assert_eq!(bq_type_to_string(&TableFieldType::Date), "DATE");
        assert_eq!(bq_type_to_string(&TableFieldType::Time), "TIME");
        assert_eq!(bq_type_to_string(&TableFieldType::Datetime), "DATETIME");
        assert_eq!(bq_type_to_string(&TableFieldType::Numeric), "NUMERIC");
        assert_eq!(bq_type_to_string(&TableFieldType::Decimal), "NUMERIC");
        assert_eq!(bq_type_to_string(&TableFieldType::Bignumeric), "BIGNUMERIC");
        assert_eq!(bq_type_to_string(&TableFieldType::Bigdecimal), "BIGNUMERIC");
        assert_eq!(bq_type_to_string(&TableFieldType::Interval), "INTERVAL");
        assert_eq!(bq_type_to_string(&TableFieldType::Json), "JSON");
    }

    #[test]
    fn test_string_to_bq_type_all_types() {
        assert!(matches!(
            string_to_bq_type("STRING"),
            TableFieldType::String
        ));
        assert!(matches!(string_to_bq_type("BYTES"), TableFieldType::Bytes));
        assert!(matches!(string_to_bq_type("INT64"), TableFieldType::Int64));
        assert!(matches!(
            string_to_bq_type("INTEGER"),
            TableFieldType::Int64
        ));
        assert!(matches!(
            string_to_bq_type("FLOAT64"),
            TableFieldType::Float64
        ));
        assert!(matches!(
            string_to_bq_type("FLOAT"),
            TableFieldType::Float64
        ));
        assert!(matches!(
            string_to_bq_type("BOOLEAN"),
            TableFieldType::Boolean
        ));
        assert!(matches!(string_to_bq_type("BOOL"), TableFieldType::Boolean));
        assert!(matches!(
            string_to_bq_type("TIMESTAMP"),
            TableFieldType::Timestamp
        ));
        assert!(matches!(string_to_bq_type("DATE"), TableFieldType::Date));
        assert!(matches!(string_to_bq_type("TIME"), TableFieldType::Time));
        assert!(matches!(
            string_to_bq_type("DATETIME"),
            TableFieldType::Datetime
        ));
        assert!(matches!(
            string_to_bq_type("NUMERIC"),
            TableFieldType::Numeric
        ));
        assert!(matches!(
            string_to_bq_type("DECIMAL"),
            TableFieldType::Numeric
        ));
        assert!(matches!(
            string_to_bq_type("BIGNUMERIC"),
            TableFieldType::Bignumeric
        ));
        assert!(matches!(
            string_to_bq_type("BIGDECIMAL"),
            TableFieldType::Bignumeric
        ));
        assert!(matches!(
            string_to_bq_type("INTERVAL"),
            TableFieldType::Interval
        ));
        assert!(matches!(string_to_bq_type("JSON"), TableFieldType::Json));
        assert!(matches!(
            string_to_bq_type("STRUCT"),
            TableFieldType::Struct
        ));
        assert!(matches!(
            string_to_bq_type("RECORD"),
            TableFieldType::Struct
        ));
    }

    #[test]
    fn test_string_to_bq_type_case_insensitive() {
        assert!(matches!(
            string_to_bq_type("string"),
            TableFieldType::String
        ));
        assert!(matches!(
            string_to_bq_type("String"),
            TableFieldType::String
        ));
        assert!(matches!(string_to_bq_type("int64"), TableFieldType::Int64));
        assert!(matches!(
            string_to_bq_type("Boolean"),
            TableFieldType::Boolean
        ));
    }

    #[test]
    fn test_string_to_bq_type_unknown_defaults_to_string() {
        assert!(matches!(
            string_to_bq_type("UNKNOWN"),
            TableFieldType::String
        ));
        assert!(matches!(
            string_to_bq_type("FOOBAR"),
            TableFieldType::String
        ));
        assert!(matches!(string_to_bq_type(""), TableFieldType::String));
    }

    #[test]
    fn test_bq_value_to_json_null() {
        let result = bq_value_to_json(BqValue::Null);
        assert_eq!(result, JsonValue::Null);
    }

    #[test]
    fn test_bq_value_to_json_string() {
        let result = bq_value_to_json(BqValue::String("hello".to_string()));
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn test_bq_value_to_json_array() {
        use google_cloud_bigquery::http::tabledata::list::Cell;
        let cells = vec![
            Cell {
                v: BqValue::String("a".to_string()),
            },
            Cell {
                v: BqValue::String("b".to_string()),
            },
        ];
        let result = bq_value_to_json(BqValue::Array(cells));
        assert_eq!(result, json!(["a", "b"]));
    }

    #[test]
    fn test_bq_value_to_json_struct() {
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple};
        let tuple = Tuple {
            f: vec![
                Cell {
                    v: BqValue::String("field1".to_string()),
                },
                Cell {
                    v: BqValue::String("field2".to_string()),
                },
            ],
        };
        let result = bq_value_to_json(BqValue::Struct(tuple));
        assert_eq!(result, json!(["field1", "field2"]));
    }

    #[test]
    fn test_bq_value_to_json_nested_array() {
        use google_cloud_bigquery::http::tabledata::list::Cell;
        let inner_cells = vec![Cell {
            v: BqValue::String("inner".to_string()),
        }];
        let outer_cells = vec![
            Cell {
                v: BqValue::Array(inner_cells),
            },
            Cell { v: BqValue::Null },
        ];
        let result = bq_value_to_json(BqValue::Array(outer_cells));
        assert_eq!(result, json!([["inner"], null]));
    }

    #[test]
    fn test_executor_mode() {
        assert_eq!(format!("{:?}", ExecutorMode::BigQuery), "BigQuery");
    }

    #[test]
    fn test_type_conversions_roundtrip() {
        let types = [
            "STRING",
            "BYTES",
            "INT64",
            "FLOAT64",
            "BOOLEAN",
            "TIMESTAMP",
            "DATE",
            "TIME",
            "DATETIME",
            "NUMERIC",
            "BIGNUMERIC",
            "INTERVAL",
            "JSON",
            "STRUCT",
        ];

        for type_str in types {
            let bq_type = string_to_bq_type(type_str);
            let back_to_str = bq_type_to_string(&bq_type);
            assert_eq!(type_str, back_to_str, "Roundtrip failed for {}", type_str);
        }
    }

    #[test]
    fn test_type_aliases() {
        assert_eq!(bq_type_to_string(&string_to_bq_type("INTEGER")), "INT64");
        assert_eq!(bq_type_to_string(&string_to_bq_type("FLOAT")), "FLOAT64");
        assert_eq!(bq_type_to_string(&string_to_bq_type("BOOL")), "BOOLEAN");
        assert_eq!(bq_type_to_string(&string_to_bq_type("DECIMAL")), "NUMERIC");
        assert_eq!(
            bq_type_to_string(&string_to_bq_type("BIGDECIMAL")),
            "BIGNUMERIC"
        );
        assert_eq!(bq_type_to_string(&string_to_bq_type("RECORD")), "STRUCT");
    }

    #[ignore = "Requires BigQuery emulator running on localhost:9050/9060"]
    #[tokio::test]
    async fn test_executor_with_emulator() {
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        let mock_uri = mock_server.uri();

        Mock::given(method("POST"))
            .and(path_regex("/bigquery/v2/projects/.*/queries"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "kind": "bigquery#queryResponse",
                "schema": {
                    "fields": [
                        {"name": "count", "type": "INTEGER", "mode": "NULLABLE"}
                    ]
                },
                "rows": [
                    {"f": [{"v": "42"}]}
                ],
                "jobComplete": true,
                "totalRows": "1"
            })))
            .mount(&mock_server)
            .await;

        let config = ClientConfig::new_with_emulator("localhost:9060", mock_uri);
        let executor = BigQueryExecutor::with_config(Some(config), Some("test-project".into()))
            .await
            .expect("Failed to create executor");

        let result = executor
            .execute_query("SELECT COUNT(*) as count FROM table")
            .await;
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.columns.len(), 1);
        assert_eq!(query_result.columns[0].name, "count");
    }

    #[ignore = "Requires real BigQuery credentials"]
    #[tokio::test]
    async fn test_real_bigquery_connection() {
        let executor = BigQueryExecutor::new().await;
        if executor.is_err() {
            println!("Skipping: No BigQuery credentials available");
            return;
        }
        let executor = executor.unwrap();
        let result = executor.execute_query("SELECT 1 as test").await;
        assert!(result.is_ok());
    }

    #[ignore = "Requires BigQuery emulator on localhost:9050/9060"]
    #[test]
    fn test_with_config_validates_project_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = ClientConfig::new_with_emulator("localhost:9060", "http://localhost:9050");
            let result = BigQueryExecutor::with_config(Some(config), None).await;

            assert!(result.is_err());
            let err = result.err().unwrap();
            match err {
                Error::Executor(msg) => {
                    assert!(msg.contains("project_id") || msg.contains("credentials"));
                }
                _ => panic!("Expected Executor error about missing project_id"),
            }
        });
    }

    #[ignore = "Requires BigQuery emulator on localhost:9050/9060"]
    #[test]
    fn test_load_parquet_validates_gcs_path() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = ClientConfig::new_with_emulator("localhost:9060", "http://localhost:9050");
            let executor = BigQueryExecutor::with_config(Some(config), Some("test-project".into()))
                .await
                .expect("Failed to create executor");

            let result = executor
                .load_parquet("test_table", "/local/path/file.parquet", &[])
                .await;
            assert!(result.is_err());

            let error = result.unwrap_err();
            match error {
                Error::Executor(msg) => {
                    assert!(msg.contains("GCS path"));
                }
                _ => panic!("Expected Executor error about GCS path"),
            }
        });
    }

    #[ignore = "Requires BigQuery emulator on localhost:9050/9060"]
    #[test]
    fn test_load_parquet_requires_dataset_env() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            std::env::remove_var("BQ_DATASET");

            let config = ClientConfig::new_with_emulator("localhost:9060", "http://localhost:9050");
            let executor = BigQueryExecutor::with_config(Some(config), Some("test-project".into()))
                .await
                .expect("Failed to create executor");

            let result = executor
                .load_parquet("test_table", "gs://bucket/file.parquet", &[])
                .await;
            assert!(result.is_err());

            let error = result.unwrap_err();
            match error {
                Error::Executor(msg) => {
                    assert!(msg.contains("BQ_DATASET"));
                }
                _ => panic!("Expected Executor error about BQ_DATASET"),
            }
        });
    }
}
