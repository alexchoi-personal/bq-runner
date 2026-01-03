use std::time::Duration;

use serde_json::Value;
use tokio::time::timeout;

use crate::error::{Error, Result};
use crate::executor::converters::json_to_sql_value;
use crate::executor::ExecutorBackend;
use crate::validation::quote_identifier;

use super::types::{PipelineTable, DEFAULT_TABLE_TIMEOUT_SECS};

pub async fn execute_table(executor: &dyn ExecutorBackend, table: &PipelineTable) -> Result<()> {
    let timeout_duration = Duration::from_secs(DEFAULT_TABLE_TIMEOUT_SECS);
    timeout(timeout_duration, execute_table_inner(executor, table))
        .await
        .map_err(|_| Error::RequestTimeout(DEFAULT_TABLE_TIMEOUT_SECS))?
}

async fn execute_table_inner(executor: &dyn ExecutorBackend, table: &PipelineTable) -> Result<()> {
    if table.is_source {
        create_source_table_standalone(executor, table).await?;
    } else if let Some(sql) = &table.sql {
        let quoted_name = format!("`{}`", quote_identifier(&table.name));
        let drop_sql = format!("DROP TABLE IF EXISTS {}", quoted_name);
        if let Err(e) = executor.execute_statement(&drop_sql).await {
            tracing::warn!(table = %table.name, error = %e, "Failed to drop table before recreation");
        }

        let query_result = executor.execute_query(sql).await.map_err(|e| {
            Error::Executor(format!(
                "Failed to execute query for table {}: {}",
                table.name, e
            ))
        })?;

        if !query_result.columns.is_empty() {
            let column_types: Vec<String> = query_result
                .columns
                .iter()
                .map(|col| format!("`{}` {}", quote_identifier(&col.name), col.data_type))
                .collect();

            let create_sql = format!("CREATE TABLE {} ({})", quoted_name, column_types.join(", "));
            executor.execute_statement(&create_sql).await?;

            if !query_result.rows.is_empty() {
                const INSERT_BATCH_SIZE: usize = 1000;
                let values: Vec<String> = query_result
                    .rows
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(json_to_sql_value).collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();

                for chunk in values.chunks(INSERT_BATCH_SIZE) {
                    let insert_sql =
                        format!("INSERT INTO {} VALUES {}", quoted_name, chunk.join(", "));
                    executor.execute_statement(&insert_sql).await?;
                }
            }
        }
    }

    Ok(())
}

async fn create_source_table_standalone(
    executor: &dyn ExecutorBackend,
    table: &PipelineTable,
) -> Result<()> {
    if let Some(schema) = &table.schema {
        let quoted_name = format!("`{}`", quote_identifier(&table.name));
        let columns: Vec<String> = schema
            .iter()
            .map(|col| format!("`{}` {}", quote_identifier(&col.name), col.column_type))
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            quoted_name,
            columns.join(", ")
        );
        executor.execute_statement(&create_sql).await?;

        if !table.rows.is_empty() {
            const INSERT_BATCH_SIZE: usize = 1000;
            let mut values: Vec<String> = Vec::with_capacity(table.rows.len());
            for (idx, row) in table.rows.iter().enumerate() {
                if let Value::Array(arr) = row {
                    let vals: Vec<String> = arr.iter().map(json_to_sql_value).collect();
                    values.push(format!("({})", vals.join(", ")));
                } else {
                    return Err(Error::Executor(format!(
                        "Invalid row format at index {} in table '{}': expected array, got {}",
                        idx,
                        table.name,
                        match row {
                            Value::Object(_) => "object",
                            Value::String(_) => "string",
                            Value::Number(_) => "number",
                            Value::Bool(_) => "boolean",
                            Value::Null => "null",
                            _ => "unknown",
                        }
                    )));
                }
            }

            for chunk in values.chunks(INSERT_BATCH_SIZE) {
                let insert_sql = format!("INSERT INTO {} VALUES {}", quoted_name, chunk.join(", "));
                executor.execute_statement(&insert_sql).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ColumnDef;
    use crate::executor::YachtSqlExecutor;
    use serde_json::json;
    use std::sync::Arc;

    fn make_source_table(name: &str, schema: Vec<ColumnDef>, rows: Vec<Value>) -> PipelineTable {
        PipelineTable {
            name: name.to_string(),
            sql: None,
            schema: Some(schema),
            rows,
            dependencies: vec![],
            is_source: true,
        }
    }

    fn make_query_table(name: &str, sql: &str, dependencies: Vec<String>) -> PipelineTable {
        PipelineTable {
            name: name.to_string(),
            sql: Some(sql.to_string()),
            schema: None,
            rows: vec![],
            dependencies,
            is_source: false,
        }
    }

    fn create_executor() -> Arc<YachtSqlExecutor> {
        Arc::new(YachtSqlExecutor::new())
    }

    #[tokio::test]
    async fn test_execute_source_table_with_schema_and_rows() {
        let executor = create_executor();
        let table = make_source_table(
            "src_table",
            vec![ColumnDef::int64("id"), ColumnDef::string("name")],
            vec![json!([1, "Alice"]), json!([2, "Bob"])],
        );

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_source_table_empty_rows() {
        let executor = create_executor();
        let table = make_source_table("empty_src", vec![ColumnDef::int64("id")], vec![]);

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_source_table_no_schema() {
        let executor = create_executor();
        let table = PipelineTable {
            name: "no_schema_src".to_string(),
            sql: None,
            schema: None,
            rows: vec![json!([1])],
            dependencies: vec![],
            is_source: true,
        };

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_basic() {
        let executor = create_executor();
        let table = make_query_table("query_table", "SELECT 1 AS id, 'test' AS name", vec![]);

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_with_dependency() {
        let executor = create_executor();

        let source = make_source_table(
            "dep_source",
            vec![ColumnDef::int64("id"), ColumnDef::string("value")],
            vec![json!([1, "a"]), json!([2, "b"])],
        );
        execute_table(executor.as_ref(), &source).await.unwrap();

        let derived = make_query_table(
            "derived_table",
            "SELECT id, value FROM dep_source WHERE id > 0",
            vec!["dep_source".to_string()],
        );
        let result = execute_table(executor.as_ref(), &derived).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_drops_existing() {
        let executor = create_executor();

        executor
            .execute_statement("CREATE TABLE drop_test (old_col INT64)")
            .await
            .unwrap();
        executor
            .execute_statement("INSERT INTO drop_test VALUES (999)")
            .await
            .unwrap();

        let table = make_query_table("drop_test", "SELECT 1 AS new_col", vec![]);
        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());

        let query_result = executor
            .execute_query("SELECT * FROM drop_test")
            .await
            .unwrap();
        assert_eq!(query_result.columns[0].name, "new_col");
        assert_eq!(query_result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_execute_query_table_empty_result() {
        let executor = create_executor();
        let table = make_query_table("empty_query", "SELECT 1 AS id WHERE FALSE", vec![]);

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_invalid_sql() {
        let executor = create_executor();
        let table = make_query_table("bad_query", "SELEC INVALID", vec![]);

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[tokio::test]
    async fn test_execute_table_no_sql_not_source() {
        let executor = create_executor();
        let table = PipelineTable {
            name: "no_sql_not_source".to_string(),
            sql: None,
            schema: None,
            rows: vec![],
            dependencies: vec![],
            is_source: false,
        };

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_source_table_non_array_rows_returns_error() {
        let executor = create_executor();
        let table = make_source_table(
            "mixed_rows",
            vec![ColumnDef::int64("id")],
            vec![json!([1]), json!({"not": "array"}), json!([2])],
        );

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
        let msg = err.to_string();
        assert!(msg.contains("Invalid row format"));
        assert!(msg.contains("index 1"));
    }

    #[tokio::test]
    async fn test_execute_source_table_various_types() {
        let executor = create_executor();
        let table = make_source_table(
            "types_table",
            vec![
                ColumnDef::int64("int_col"),
                ColumnDef::float64("float_col"),
                ColumnDef::string("str_col"),
                ColumnDef::bool("bool_col"),
            ],
            vec![
                json!([42, 1.234, "hello", true]),
                json!([null, null, null, false]),
            ],
        );

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_table_diamond_dependency() {
        let executor = create_executor();

        let a = make_source_table("diamond_a", vec![ColumnDef::int64("id")], vec![json!([1])]);
        execute_table(executor.as_ref(), &a).await.unwrap();

        let b = make_query_table(
            "diamond_b",
            "SELECT id * 2 AS id FROM diamond_a",
            vec!["diamond_a".to_string()],
        );
        execute_table(executor.as_ref(), &b).await.unwrap();

        let c = make_query_table(
            "diamond_c",
            "SELECT id * 3 AS id FROM diamond_a",
            vec!["diamond_a".to_string()],
        );
        execute_table(executor.as_ref(), &c).await.unwrap();

        let d = make_query_table(
            "diamond_d",
            "SELECT b.id AS b_id, c.id AS c_id FROM diamond_b b, diamond_c c",
            vec!["diamond_b".to_string(), "diamond_c".to_string()],
        );
        let result = execute_table(executor.as_ref(), &d).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_multiple_columns() {
        let executor = create_executor();
        let table = make_query_table(
            "multi_col",
            "SELECT 1 AS a, 2 AS b, 3 AS c, 'test' AS d",
            vec![],
        );

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_multiple_rows() {
        let executor = create_executor();
        let table = make_query_table(
            "multi_row",
            "SELECT * FROM UNNEST([1, 2, 3, 4, 5]) AS num",
            vec![],
        );

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_table_chain_dependencies() {
        let executor = create_executor();

        let t1 = make_source_table("chain1", vec![ColumnDef::int64("v")], vec![json!([1])]);
        execute_table(executor.as_ref(), &t1).await.unwrap();

        let t2 = make_query_table(
            "chain2",
            "SELECT v + 1 AS v FROM chain1",
            vec!["chain1".to_string()],
        );
        execute_table(executor.as_ref(), &t2).await.unwrap();

        let t3 = make_query_table(
            "chain3",
            "SELECT v + 1 AS v FROM chain2",
            vec!["chain2".to_string()],
        );
        execute_table(executor.as_ref(), &t3).await.unwrap();

        let result = executor
            .execute_query("SELECT v FROM chain3")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], json!(3));
    }

    #[tokio::test]
    async fn test_execute_source_table_string_escaping() {
        let executor = create_executor();
        let table = make_source_table(
            "escape_test",
            vec![ColumnDef::string("text")],
            vec![json!(["it's a test"]), json!(["quote: \"here\""])],
        );

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_query_table_no_columns_result() {
        let executor = create_executor();

        executor
            .execute_statement("CREATE TABLE stmt_only (id INT64)")
            .await
            .unwrap();

        let table = PipelineTable {
            name: "stmt_result".to_string(),
            sql: Some("INSERT INTO stmt_only VALUES (1)".to_string()),
            schema: None,
            rows: vec![],
            dependencies: vec![],
            is_source: false,
        };

        let result = execute_table(executor.as_ref(), &table).await;
        assert!(result.is_ok());
    }
}
