use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;

use super::{ColumnDef, ColumnInfo, ExecutorBackend, ExecutorMode, QueryResult};
use crate::error::Result;

pub struct TestCountingExecutor<F>
where
    F: Fn(&str) + Send + Sync,
{
    callback: F,
    inner: Arc<Mutex<super::YachtSqlExecutor>>,
}

impl<F> TestCountingExecutor<F>
where
    F: Fn(&str) + Send + Sync,
{
    pub fn new(callback: F) -> Self {
        Self {
            callback,
            inner: Arc::new(Mutex::new(super::YachtSqlExecutor::new())),
        }
    }
}

#[async_trait]
impl<F> ExecutorBackend for TestCountingExecutor<F>
where
    F: Fn(&str) + Send + Sync,
{
    fn mode(&self) -> ExecutorMode {
        ExecutorMode::Mock
    }

    async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let inner = self.inner.lock();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(inner.execute_query(sql))
    }

    async fn execute_statement(&self, sql: &str) -> Result<u64> {
        if sql.starts_with("CREATE TABLE") {
            if let Some(table_name) = extract_table_name_from_create(sql) {
                (self.callback)(&table_name);
            }
        } else if sql.starts_with("DROP TABLE") {
            if let Some(table_name) = extract_table_name_from_drop(sql) {
                (self.callback)(&table_name);
            }
        }

        let inner = self.inner.lock();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(inner.execute_statement(sql))
    }

    async fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[ColumnDef],
    ) -> Result<u64> {
        (self.callback)(table_name);
        let inner = self.inner.lock();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(inner.load_parquet(table_name, path, schema))
    }
}

fn extract_table_name_from_create(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let start = upper.find("CREATE TABLE")? + "CREATE TABLE".len();
    let rest = &sql[start..].trim_start();

    let rest = if rest.to_uppercase().starts_with("IF NOT EXISTS") {
        rest["IF NOT EXISTS".len()..].trim_start()
    } else {
        rest
    };

    let end = rest.find(|c: char| c == ' ' || c == '(' || c == '\n')?;
    Some(rest[..end].to_string())
}

fn extract_table_name_from_drop(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let start = upper.find("DROP TABLE")? + "DROP TABLE".len();
    let rest = &sql[start..].trim_start();

    let rest = if rest.to_uppercase().starts_with("IF EXISTS") {
        rest["IF EXISTS".len()..].trim_start()
    } else {
        rest
    };

    let end = rest
        .find(|c: char| c == ' ' || c == ';' || c == '\n')
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name_from_create() {
        assert_eq!(
            extract_table_name_from_create("CREATE TABLE foo (id INT64)"),
            Some("foo".to_string())
        );
        assert_eq!(
            extract_table_name_from_create("CREATE TABLE IF NOT EXISTS bar (id INT64)"),
            Some("bar".to_string())
        );
    }

    #[test]
    fn test_extract_table_name_from_drop() {
        assert_eq!(
            extract_table_name_from_drop("DROP TABLE foo"),
            Some("foo".to_string())
        );
        assert_eq!(
            extract_table_name_from_drop("DROP TABLE IF EXISTS bar"),
            Some("bar".to_string())
        );
    }
}
