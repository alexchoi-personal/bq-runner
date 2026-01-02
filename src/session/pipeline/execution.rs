use serde_json::Value;

use crate::error::{Error, Result};
use crate::executor::ExecutorBackend;
use crate::utils::json_to_sql_value;

use super::types::PipelineTable;

pub fn execute_table(executor: &dyn ExecutorBackend, table: &PipelineTable) -> Result<()> {
    let handle = tokio::runtime::Handle::current();

    if table.is_source {
        create_source_table_standalone(executor, table)?;
    } else if let Some(sql) = &table.sql {
        let drop_sql = format!("DROP TABLE IF EXISTS {}", table.name);
        let _ = handle.block_on(executor.execute_statement(&drop_sql));

        let query_result = handle.block_on(executor.execute_query(sql)).map_err(|e| {
            Error::Executor(format!(
                "Failed to execute query for table {}: {}",
                table.name, e
            ))
        })?;

        if !query_result.columns.is_empty() {
            let column_types: Vec<String> = query_result
                .columns
                .iter()
                .map(|col| format!("{} {}", col.name, col.data_type))
                .collect();

            let create_sql = format!("CREATE TABLE {} ({})", table.name, column_types.join(", "));
            handle.block_on(executor.execute_statement(&create_sql))?;

            if !query_result.rows.is_empty() {
                let values: Vec<String> = query_result
                    .rows
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(json_to_sql_value).collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();

                let insert_sql = format!("INSERT INTO {} VALUES {}", table.name, values.join(", "));
                handle.block_on(executor.execute_statement(&insert_sql))?;
            }
        }
    }

    Ok(())
}

fn create_source_table_standalone(
    executor: &dyn ExecutorBackend,
    table: &PipelineTable,
) -> Result<()> {
    let handle = tokio::runtime::Handle::current();

    if let Some(schema) = &table.schema {
        let columns: Vec<String> = schema
            .iter()
            .map(|col| format!("{} {}", col.name, col.column_type))
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table.name,
            columns.join(", ")
        );
        handle.block_on(executor.execute_statement(&create_sql))?;

        if !table.rows.is_empty() {
            let values: Vec<String> = table
                .rows
                .iter()
                .filter_map(|row| {
                    if let Value::Array(arr) = row {
                        let vals: Vec<String> = arr.iter().map(json_to_sql_value).collect();
                        Some(format!("({})", vals.join(", ")))
                    } else {
                        None
                    }
                })
                .collect();

            if !values.is_empty() {
                let insert_sql = format!("INSERT INTO {} VALUES {}", table.name, values.join(", "));
                handle.block_on(executor.execute_statement(&insert_sql))?;
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

    fn with_runtime<F, R>(f: F) -> R
    where
        F: FnOnce(&tokio::runtime::Runtime, Arc<YachtSqlExecutor>) -> R,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let executor = Arc::new(YachtSqlExecutor::new());
        f(&rt, executor)
    }

    #[test]
    fn test_execute_source_table_with_schema_and_rows() {
        with_runtime(|rt, executor| {
            let table = make_source_table(
                "src_table",
                vec![ColumnDef::int64("id"), ColumnDef::string("name")],
                vec![json!([1, "Alice"]), json!([2, "Bob"])],
            );

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_source_table_empty_rows() {
        with_runtime(|rt, executor| {
            let table = make_source_table("empty_src", vec![ColumnDef::int64("id")], vec![]);

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_source_table_no_schema() {
        with_runtime(|rt, executor| {
            let table = PipelineTable {
                name: "no_schema_src".to_string(),
                sql: None,
                schema: None,
                rows: vec![json!([1])],
                dependencies: vec![],
                is_source: true,
            };

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_basic() {
        with_runtime(|rt, executor| {
            let table = make_query_table("query_table", "SELECT 1 AS id, 'test' AS name", vec![]);

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_with_dependency() {
        with_runtime(|rt, executor| {
            let source = make_source_table(
                "dep_source",
                vec![ColumnDef::int64("id"), ColumnDef::string("value")],
                vec![json!([1, "a"]), json!([2, "b"])],
            );

            let exec1 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec1.as_ref(), &source))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let derived = make_query_table(
                "derived_table",
                "SELECT id, value FROM dep_source WHERE id > 0",
                vec!["dep_source".to_string()],
            );

            let exec2 = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec2.as_ref(), &derived))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_drops_existing() {
        with_runtime(|rt, executor| {
            rt.block_on(executor.execute_statement("CREATE TABLE drop_test (old_col INT64)"))
                .unwrap();
            rt.block_on(executor.execute_statement("INSERT INTO drop_test VALUES (999)"))
                .unwrap();

            let table = make_query_table("drop_test", "SELECT 1 AS new_col", vec![]);
            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());

            let query_result = rt
                .block_on(executor.execute_query("SELECT * FROM drop_test"))
                .unwrap();
            assert_eq!(query_result.columns[0].name, "new_col");
            assert_eq!(query_result.rows.len(), 1);
        });
    }

    #[test]
    fn test_execute_query_table_empty_result() {
        with_runtime(|rt, executor| {
            let table = make_query_table("empty_query", "SELECT 1 AS id WHERE FALSE", vec![]);

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_invalid_sql() {
        with_runtime(|rt, executor| {
            let table = make_query_table("bad_query", "SELEC INVALID", vec![]);

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err, Error::Executor(_)));
        });
    }

    #[test]
    fn test_execute_table_no_sql_not_source() {
        with_runtime(|rt, executor| {
            let table = PipelineTable {
                name: "no_sql_not_source".to_string(),
                sql: None,
                schema: None,
                rows: vec![],
                dependencies: vec![],
                is_source: false,
            };

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_source_table_non_array_rows_filtered() {
        with_runtime(|rt, executor| {
            let table = make_source_table(
                "mixed_rows",
                vec![ColumnDef::int64("id")],
                vec![
                    json!([1]),
                    json!({"not": "array"}),
                    json!([2]),
                    json!("string"),
                    json!([3]),
                ],
            );

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_source_table_various_types() {
        with_runtime(|rt, executor| {
            let table = make_source_table(
                "types_table",
                vec![
                    ColumnDef::int64("int_col"),
                    ColumnDef::float64("float_col"),
                    ColumnDef::string("str_col"),
                    ColumnDef::bool("bool_col"),
                ],
                vec![
                    json!([42, 3.14, "hello", true]),
                    json!([null, null, null, false]),
                ],
            );

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_table_diamond_dependency() {
        with_runtime(|rt, executor| {
            let a = make_source_table("diamond_a", vec![ColumnDef::int64("id")], vec![json!([1])]);
            let exec1 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec1.as_ref(), &a))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let b = make_query_table(
                "diamond_b",
                "SELECT id * 2 AS id FROM diamond_a",
                vec!["diamond_a".to_string()],
            );
            let exec2 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec2.as_ref(), &b))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let c = make_query_table(
                "diamond_c",
                "SELECT id * 3 AS id FROM diamond_a",
                vec!["diamond_a".to_string()],
            );
            let exec3 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec3.as_ref(), &c))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let d = make_query_table(
                "diamond_d",
                "SELECT b.id AS b_id, c.id AS c_id FROM diamond_b b, diamond_c c",
                vec!["diamond_b".to_string(), "diamond_c".to_string()],
            );
            let exec4 = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec4.as_ref(), &d))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_multiple_columns() {
        with_runtime(|rt, executor| {
            let table = make_query_table(
                "multi_col",
                "SELECT 1 AS a, 2 AS b, 3 AS c, 'test' AS d",
                vec![],
            );

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_multiple_rows() {
        with_runtime(|rt, executor| {
            let table = make_query_table(
                "multi_row",
                "SELECT * FROM UNNEST([1, 2, 3, 4, 5]) AS num",
                vec![],
            );

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_table_chain_dependencies() {
        with_runtime(|rt, executor| {
            let t1 = make_source_table("chain1", vec![ColumnDef::int64("v")], vec![json!([1])]);
            let exec1 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec1.as_ref(), &t1))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let t2 = make_query_table(
                "chain2",
                "SELECT v + 1 AS v FROM chain1",
                vec!["chain1".to_string()],
            );
            let exec2 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec2.as_ref(), &t2))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let t3 = make_query_table(
                "chain3",
                "SELECT v + 1 AS v FROM chain2",
                vec!["chain2".to_string()],
            );
            let exec3 = executor.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec3.as_ref(), &t3))
                    .await
                    .unwrap()
                    .unwrap()
            });

            let result = rt
                .block_on(executor.execute_query("SELECT v FROM chain3"))
                .unwrap();
            assert_eq!(result.rows[0][0], json!(3));
        });
    }

    #[test]
    fn test_execute_source_table_string_escaping() {
        with_runtime(|rt, executor| {
            let table = make_source_table(
                "escape_test",
                vec![ColumnDef::string("text")],
                vec![json!(["it's a test"]), json!(["quote: \"here\""])],
            );

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_execute_query_table_no_columns_result() {
        with_runtime(|rt, executor| {
            rt.block_on(executor.execute_statement("CREATE TABLE stmt_only (id INT64)"))
                .unwrap();

            let table = PipelineTable {
                name: "stmt_result".to_string(),
                sql: Some("INSERT INTO stmt_only VALUES (1)".to_string()),
                schema: None,
                rows: vec![],
                dependencies: vec![],
                is_source: false,
            };

            let exec = executor.clone();
            let result = rt.block_on(async move {
                tokio::task::spawn_blocking(move || execute_table(exec.as_ref(), &table))
                    .await
                    .unwrap()
            });
            assert!(result.is_ok());
        });
    }
}
