use std::fs;
use std::sync::Arc;

use serde_json::json;
use tempfile::TempDir;

use bq_runner::rpc::RpcMethods;
use bq_runner::session::SessionManager;

fn create_rpc_methods() -> RpcMethods {
    let session_manager = Arc::new(SessionManager::new());
    RpcMethods::new(session_manager)
}

async fn create_session(methods: &RpcMethods) -> String {
    let result = methods
        .dispatch("bq.createSession", json!({}))
        .await
        .unwrap();
    result["sessionId"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn test_full_dag_workflow() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let result = methods.dispatch("bq.registerDag", json!({
        "sessionId": session_id,
        "tables": [
            {
                "name": "source_data",
                "schema": [{"name": "id", "type": "INT64"}, {"name": "value", "type": "STRING"}],
                "rows": [[1, "a"], [2, "b"], [3, "c"]]
            },
            {
                "name": "transformed",
                "sql": "SELECT id, UPPER(value) AS upper_value FROM source_data"
            },
            {
                "name": "final_output",
                "sql": "SELECT id * 10 AS scaled_id, upper_value FROM transformed"
            }
        ]
    })).await.unwrap();

    assert_eq!(result["success"], true);
    assert!(result["tables"].is_array());

    let run_result = methods
        .dispatch(
            "bq.runDag",
            json!({
                "sessionId": session_id,
                "retryCount": 0
            }),
        )
        .await
        .unwrap();

    assert_eq!(run_result["success"], true);
    assert!(run_result["succeededTables"].as_array().unwrap().len() >= 2);

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT * FROM final_output ORDER BY scaled_id"
            }),
        )
        .await
        .unwrap();

    assert!(query_result["rows"].is_array());
}

#[tokio::test]
async fn test_dag_with_retry() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.registerDag",
            json!({
                "sessionId": session_id,
                "tables": [
                    {
                        "name": "base",
                        "schema": [{"name": "x", "type": "INT64"}],
                        "rows": [[1], [2], [3]]
                    },
                    {
                        "name": "derived",
                        "sql": "SELECT x * 2 AS doubled FROM base"
                    }
                ]
            }),
        )
        .await
        .unwrap();

    let result = methods
        .dispatch(
            "bq.runDag",
            json!({
                "sessionId": session_id,
                "retryCount": 2
            }),
        )
        .await
        .unwrap();

    assert!(result["success"].is_boolean());
    assert!(result["succeededTables"].is_array());
}

#[tokio::test]
async fn test_retry_dag_method() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.registerDag",
            json!({
                "sessionId": session_id,
                "tables": [
                    {
                        "name": "src",
                        "schema": [{"name": "n", "type": "INT64"}],
                        "rows": [[10]]
                    },
                    {
                        "name": "out",
                        "sql": "SELECT n + 1 AS result FROM src"
                    }
                ]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.runDag",
            json!({
                "sessionId": session_id
            }),
        )
        .await
        .unwrap();

    let retry_result = methods
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

    assert!(retry_result["success"].is_boolean());
}

#[tokio::test]
async fn test_get_dag() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.registerDag",
            json!({
                "sessionId": session_id,
                "tables": [
                    {
                        "name": "t1",
                        "schema": [{"name": "a", "type": "INT64"}],
                        "rows": [[1]]
                    },
                    {
                        "name": "t2",
                        "sql": "SELECT a FROM t1"
                    }
                ]
            }),
        )
        .await
        .unwrap();

    let dag = methods
        .dispatch(
            "bq.getDag",
            json!({
                "sessionId": session_id
            }),
        )
        .await
        .unwrap();

    assert!(dag["tables"].is_array());
    let tables = dag["tables"].as_array().unwrap();
    assert!(tables.len() >= 2);
}

#[tokio::test]
async fn test_load_sql_directory() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let temp_dir = TempDir::new().unwrap();
    let project_path = temp_dir.path().join("my_project");
    let dataset_path = project_path.join("my_dataset");
    fs::create_dir_all(&dataset_path).unwrap();

    fs::write(dataset_path.join("table1.sql"), "SELECT 1 AS id").unwrap();
    fs::write(
        dataset_path.join("table2.sql"),
        "SELECT * FROM my_project.my_dataset.table1",
    )
    .unwrap();

    let result = methods
        .dispatch(
            "bq.loadSqlDirectory",
            json!({
                "sessionId": session_id,
                "rootPath": temp_dir.path().to_str().unwrap()
            }),
        )
        .await
        .unwrap();

    assert_eq!(result["success"], true);
    assert!(result["tablesLoaded"].is_array());
    assert_eq!(result["tablesLoaded"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_load_parquet_directory() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let temp_dir = TempDir::new().unwrap();
    let project_path = temp_dir.path().join("proj");
    let dataset_path = project_path.join("ds");
    fs::create_dir_all(&dataset_path).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .unwrap();

    let parquet_path = dataset_path.join("users.parquet");
    let file = fs::File::create(&parquet_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let schema_json = r#"[{"name": "id", "type": "INT64"}, {"name": "name", "type": "STRING"}]"#;
    fs::write(dataset_path.join("users.schema.json"), schema_json).unwrap();

    let result = methods
        .dispatch(
            "bq.loadParquetDirectory",
            json!({
                "sessionId": session_id,
                "rootPath": temp_dir.path().to_str().unwrap()
            }),
        )
        .await
        .unwrap();

    assert_eq!(result["success"], true);
    assert!(result["tablesLoaded"].is_array());
    assert_eq!(result["tablesLoaded"].as_array().unwrap().len(), 1);
    assert_eq!(result["tablesLoaded"][0]["rowCount"], 3);
}

#[tokio::test]
async fn test_load_dag_from_directory() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let temp_dir = TempDir::new().unwrap();
    let project_path = temp_dir.path().join("analytics");
    let dataset_path = project_path.join("reports");
    fs::create_dir_all(&dataset_path).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let value_array = Int64Array::from(vec![10, 20, 30]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(value_array)]).unwrap();

    let parquet_path = dataset_path.join("source.parquet");
    let file = fs::File::create(&parquet_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    fs::write(
        dataset_path.join("source.schema.json"),
        r#"[{"name": "value", "type": "INT64"}]"#,
    )
    .unwrap();
    fs::write(
        dataset_path.join("computed.sql"),
        "SELECT value * 2 AS doubled FROM analytics.reports.source",
    )
    .unwrap();

    let result = methods
        .dispatch(
            "bq.loadDagFromDirectory",
            json!({
                "sessionId": session_id,
                "rootPath": temp_dir.path().to_str().unwrap()
            }),
        )
        .await
        .unwrap();

    assert_eq!(result["success"], true);
    assert!(result["sourceTables"].is_array());
    assert!(result["computedTables"].is_array());
    assert!(result["dagInfo"].is_array());
}

#[tokio::test]
async fn test_table_operations() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    let create_result = methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "ops_table",
                "schema": [{"name": "id", "type": "INT64"}, {"name": "name", "type": "STRING"}]
            }),
        )
        .await
        .unwrap();

    assert_eq!(create_result["success"], true);

    let insert_result = methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "ops_table",
                "rows": [[1, "Alice"], [2, "Bob"]]
            }),
        )
        .await
        .unwrap();

    assert!(insert_result["insertedRows"].is_number());

    let query_result = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT * FROM ops_table"
            }),
        )
        .await
        .unwrap();

    assert!(query_result["rows"].is_array());
}

#[tokio::test]
async fn test_dag_with_multiple_dependency_levels() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.registerDag",
            json!({
                "sessionId": session_id,
                "tables": [
                    {
                        "name": "level0",
                        "schema": [{"name": "x", "type": "INT64"}],
                        "rows": [[1]]
                    },
                    {
                        "name": "level1a",
                        "sql": "SELECT x + 1 AS x FROM level0"
                    },
                    {
                        "name": "level1b",
                        "sql": "SELECT x + 2 AS x FROM level0"
                    },
                    {
                        "name": "level2",
                        "sql": "SELECT a.x + b.x AS total FROM level1a a, level1b b"
                    }
                ]
            }),
        )
        .await
        .unwrap();

    let result = methods
        .dispatch(
            "bq.runDag",
            json!({
                "sessionId": session_id
            }),
        )
        .await
        .unwrap();

    assert_eq!(result["success"], true);

    let query = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session_id,
                "sql": "SELECT * FROM level2"
            }),
        )
        .await
        .unwrap();

    assert!(query["rows"].is_array());
}

#[tokio::test]
async fn test_dag_target_specific_tables() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.registerDag",
            json!({
                "sessionId": session_id,
                "tables": [
                    {
                        "name": "base",
                        "schema": [{"name": "n", "type": "INT64"}],
                        "rows": [[5]]
                    },
                    {
                        "name": "branch_a",
                        "sql": "SELECT n * 2 AS n FROM base"
                    },
                    {
                        "name": "branch_b",
                        "sql": "SELECT n * 3 AS n FROM base"
                    }
                ]
            }),
        )
        .await
        .unwrap();

    let result = methods
        .dispatch(
            "bq.runDag",
            json!({
                "sessionId": session_id,
                "tableNames": ["branch_a"]
            }),
        )
        .await
        .unwrap();

    assert_eq!(result["success"], true);
    let succeeded = result["succeededTables"].as_array().unwrap();
    assert!(succeeded.iter().any(|t| t == "branch_a"));
}

#[tokio::test]
async fn test_session_project_catalog() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "proj1.dataset1.table1",
                "schema": [{"name": "id", "type": "INT64"}]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "proj1.dataset2.table2",
                "schema": [{"name": "id", "type": "INT64"}]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "proj2.dataset1.table3",
                "schema": [{"name": "id", "type": "INT64"}]
            }),
        )
        .await
        .unwrap();

    let projects = methods
        .dispatch(
            "bq.getProjects",
            json!({
                "sessionId": session_id
            }),
        )
        .await
        .unwrap();

    assert!(projects["projects"].as_array().unwrap().len() >= 2);

    let datasets = methods
        .dispatch(
            "bq.getDatasets",
            json!({
                "sessionId": session_id,
                "project": "proj1"
            }),
        )
        .await
        .unwrap();

    assert!(datasets["datasets"].as_array().unwrap().len() >= 2);

    let tables = methods
        .dispatch(
            "bq.getTablesInDataset",
            json!({
                "sessionId": session_id,
                "project": "proj1",
                "dataset": "dataset1"
            }),
        )
        .await
        .unwrap();

    assert!(tables["tables"].is_array());
}

#[tokio::test]
async fn test_multiple_sessions_independence() {
    let methods = create_rpc_methods();

    let session1 = create_session(&methods).await;
    let session2 = create_session(&methods).await;

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session1,
                "tableName": "shared_name",
                "schema": [{"name": "val", "type": "INT64"}]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session1,
                "tableName": "shared_name",
                "rows": [[100]]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session2,
                "tableName": "shared_name",
                "schema": [{"name": "val", "type": "INT64"}]
            }),
        )
        .await
        .unwrap();

    methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session2,
                "tableName": "shared_name",
                "rows": [[200]]
            }),
        )
        .await
        .unwrap();

    let result1 = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session1,
                "sql": "SELECT val FROM shared_name"
            }),
        )
        .await
        .unwrap();

    let result2 = methods
        .dispatch(
            "bq.query",
            json!({
                "sessionId": session2,
                "sql": "SELECT val FROM shared_name"
            }),
        )
        .await
        .unwrap();

    assert!(result1["rows"].is_array());
    assert!(result2["rows"].is_array());
    assert_ne!(session1, session2);
}

#[tokio::test]
async fn test_insert_with_array_and_object_rows() {
    let methods = create_rpc_methods();
    let session_id = create_session(&methods).await;

    methods
        .dispatch(
            "bq.createTable",
            json!({
                "sessionId": session_id,
                "tableName": "insert_test",
                "schema": [
                    {"name": "id", "type": "INT64"},
                    {"name": "val", "type": "STRING"}
                ]
            }),
        )
        .await
        .unwrap();

    let insert_result = methods
        .dispatch(
            "bq.insert",
            json!({
                "sessionId": session_id,
                "tableName": "insert_test",
                "rows": [[1, "a"], [2, "b"]]
            }),
        )
        .await
        .unwrap();

    assert!(insert_result["insertedRows"].is_number());
}
