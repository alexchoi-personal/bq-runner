use std::sync::Arc;

use bq_runner::session::SessionManager;
use serde_json::json;

#[tokio::test]
async fn test_query_arrow_basic() {
    let manager = Arc::new(SessionManager::new());
    let session_id = manager.create_session().await.unwrap();

    let result = manager
        .execute_query(session_id, "SELECT 1 AS num, 'hello' AS str")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 2);
}

#[tokio::test]
async fn test_query_result_to_record_batch_conversion() {
    use bq_runner::executor::QueryResult;

    #[derive(Debug, Clone)]
    struct ColumnInfo {
        name: String,
        data_type: String,
    }

    let result = QueryResult {
        columns: vec![
            bq_runner::executor::ColumnInfo {
                name: "id".to_string(),
                data_type: "INT64".to_string(),
            },
            bq_runner::executor::ColumnInfo {
                name: "name".to_string(),
                data_type: "STRING".to_string(),
            },
            bq_runner::executor::ColumnInfo {
                name: "score".to_string(),
                data_type: "FLOAT64".to_string(),
            },
            bq_runner::executor::ColumnInfo {
                name: "active".to_string(),
                data_type: "BOOL".to_string(),
            },
        ],
        rows: vec![
            vec![json!(1), json!("Alice"), json!(95.5), json!(true)],
            vec![json!(2), json!("Bob"), json!(87.3), json!(false)],
            vec![json!(null), json!(null), json!(null), json!(null)],
        ],
    };

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.columns.len(), 4);
}

#[tokio::test]
async fn test_session_with_multiple_queries() {
    let manager = Arc::new(SessionManager::new());
    let session_id = manager.create_session().await.unwrap();

    for i in 0..5 {
        let sql = format!("SELECT {} AS num", i);
        let result = manager.execute_query(session_id, &sql).await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    manager.destroy_session(session_id).unwrap();
}

#[tokio::test]
async fn test_session_cleanup_on_destroy() {
    let manager = Arc::new(SessionManager::new());
    let session_id = manager.create_session().await.unwrap();

    let result = manager
        .execute_query(session_id, "SELECT 42 AS x")
        .await
        .unwrap();
    assert_eq!(result.rows[0][0].as_i64().unwrap(), 42);

    manager.destroy_session(session_id).unwrap();

    let query_result = manager.execute_query(session_id, "SELECT 1").await;
    assert!(query_result.is_err());
}

#[tokio::test]
async fn test_concurrent_sessions() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let manager = Arc::new(SessionManager::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let num_sessions: usize = 10;

    let mut handles = Vec::new();
    for i in 0..num_sessions {
        let manager = Arc::clone(&manager);
        let completed = Arc::clone(&completed);

        let handle = tokio::spawn(async move {
            let session_id = manager.create_session().await.unwrap();
            let sql = format!("SELECT {} AS num", i);
            let result = manager.execute_query(session_id, &sql).await.unwrap();
            assert_eq!(result.rows[0][0].as_i64().unwrap(), i as i64);
            manager.destroy_session(session_id).unwrap();
            completed.fetch_add(1, Ordering::SeqCst);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(completed.load(Ordering::SeqCst), num_sessions);
}

#[tokio::test]
async fn test_query_with_various_types() {
    let manager = Arc::new(SessionManager::new());
    let session_id = manager.create_session().await.unwrap();

    let result = manager
        .execute_query(
            session_id,
            "SELECT 
                42 AS int_val,
                3.14 AS float_val,
                'hello' AS str_val,
                TRUE AS bool_val,
                DATE '2022-01-15' AS date_val",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.rows[0][0].as_i64().unwrap(), 42);
    assert!((result.rows[0][1].as_f64().unwrap() - 3.14).abs() < 0.001);
    assert_eq!(result.rows[0][2].as_str().unwrap(), "hello");
    assert!(result.rows[0][3].as_bool().unwrap());
}

#[tokio::test]
async fn test_query_with_null_values() {
    let manager = Arc::new(SessionManager::new());
    let session_id = manager.create_session().await.unwrap();

    let result = manager
        .execute_query(
            session_id,
            "SELECT NULL AS null_val, 1 AS int_val",
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert!(result.rows[0][0].is_null());
    assert_eq!(result.rows[0][1].as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_empty_result_set() {
    let manager = Arc::new(SessionManager::new());
    let session_id = manager.create_session().await.unwrap();

    manager
        .execute_statement(session_id, "CREATE TABLE empty_test (id INT64)")
        .await
        .unwrap();

    let result = manager
        .execute_query(session_id, "SELECT * FROM empty_test")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 0);
    assert_eq!(result.columns.len(), 1);
}
