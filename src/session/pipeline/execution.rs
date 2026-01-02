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

fn create_source_table_standalone(executor: &dyn ExecutorBackend, table: &PipelineTable) -> Result<()> {
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
