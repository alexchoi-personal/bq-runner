use crate::error::{Error, Result};
use regex::Regex;
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use std::sync::LazyLock;

static TABLE_NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$").unwrap()
});

pub fn validate_table_name(name: &str) -> Result<()> {
    if name.len() > 128 || !TABLE_NAME_REGEX.is_match(name) {
        return Err(Error::InvalidRequest(format!("Invalid table name: {}", name)));
    }
    Ok(())
}

pub fn quote_identifier(name: &str) -> String {
    name.replace('`', "``")
}

pub fn validate_sql_for_define_table(sql: &str) -> Result<()> {
    if sql.trim().is_empty() {
        return Err(Error::InvalidRequest("SQL cannot be empty".into()));
    }

    let dialect = BigQueryDialect {};

    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| Error::InvalidRequest(format!("Invalid SQL syntax: {}", e)))?;

    if statements.len() != 1 {
        return Err(Error::InvalidRequest(
            "Only single SELECT statements allowed".into(),
        ));
    }

    match &statements[0] {
        Statement::Query(query) => {
            validate_query_recursive(query)?;
            Ok(())
        }
        Statement::Merge { .. } => Err(Error::InvalidRequest("MERGE statements not allowed".into())),
        Statement::Call(_) => Err(Error::InvalidRequest("CALL statements not allowed".into())),
        _ => Err(Error::InvalidRequest(
            "Only SELECT statements allowed in defineTable".into(),
        )),
    }
}

fn validate_query_recursive(query: &Query) -> Result<()> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            validate_query_recursive(&cte.query)?;
        }
    }

    validate_set_expr(&query.body)?;

    Ok(())
}

fn validate_set_expr(body: &SetExpr) -> Result<()> {
    match body {
        SetExpr::Select(select) => {
            for table in &select.from {
                validate_table_factor(&table.relation)?;
                for join in &table.joins {
                    validate_table_factor(&join.relation)?;
                }
            }
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        validate_expr(expr)?;
                    }
                    _ => {}
                }
            }
            if let Some(where_clause) = &select.selection {
                validate_expr(where_clause)?;
            }
            if let Some(having) = &select.having {
                validate_expr(having)?;
            }
            Ok(())
        }
        SetExpr::Query(subquery) => validate_query_recursive(subquery),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left)?;
            validate_set_expr(right)?;
            Ok(())
        }
        SetExpr::Values(_) => Ok(()),
        SetExpr::Insert(_) => Err(Error::InvalidRequest("INSERT not allowed".into())),
        SetExpr::Update(_) => Err(Error::InvalidRequest("UPDATE not allowed".into())),
        SetExpr::Table(_) => Ok(()),
    }
}

fn validate_table_factor(tf: &TableFactor) -> Result<()> {
    match tf {
        TableFactor::Derived { subquery, .. } => validate_query_recursive(subquery),
        TableFactor::NestedJoin { table_with_joins, .. } => {
            validate_table_factor(&table_with_joins.relation)?;
            for join in &table_with_joins.joins {
                validate_table_factor(&join.relation)?;
            }
            Ok(())
        }
        TableFactor::TableFunction { .. } => Ok(()),
        TableFactor::Function { .. } => Ok(()),
        TableFactor::UNNEST { .. } => Ok(()),
        TableFactor::Table { .. } => Ok(()),
        TableFactor::Pivot { .. } => Ok(()),
        TableFactor::Unpivot { .. } => Ok(()),
        TableFactor::MatchRecognize { .. } => Ok(()),
        _ => Ok(()),
    }
}

fn validate_expr(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Subquery(query) => validate_query_recursive(query),
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left)?;
            validate_expr(right)
        }
        Expr::UnaryOp { expr, .. } => validate_expr(expr),
        Expr::InSubquery { subquery, .. } => validate_query_recursive(subquery),
        Expr::Exists { subquery, .. } => validate_query_recursive(subquery),
        Expr::InList { list, .. } => {
            for e in list {
                validate_expr(e)?;
            }
            Ok(())
        }
        Expr::Between { low, high, .. } => {
            validate_expr(low)?;
            validate_expr(high)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                validate_expr(op)?;
            }
            for cond in conditions {
                validate_expr(cond)?;
            }
            for res in results {
                validate_expr(res)?;
            }
            if let Some(el) = else_result {
                validate_expr(el)?;
            }
            Ok(())
        }
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        validate_expr(e)?;
                    }
                }
            }
            Ok(())
        }
        Expr::Nested(inner) => validate_expr(inner),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_table_name_simple() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("my_table").is_ok());
        assert!(validate_table_name("_private").is_ok());
    }

    #[test]
    fn test_validate_table_name_qualified() {
        assert!(validate_table_name("project.dataset.table").is_ok());
        assert!(validate_table_name("my_project.my_dataset.my_table").is_ok());
    }

    #[test]
    fn test_validate_table_name_invalid_start() {
        assert!(validate_table_name("123table").is_err());
        assert!(validate_table_name("-table").is_err());
    }

    #[test]
    fn test_validate_table_name_invalid_chars() {
        assert!(validate_table_name("table-name").is_err());
        assert!(validate_table_name("table name").is_err());
        assert!(validate_table_name("table@name").is_err());
    }

    #[test]
    fn test_validate_table_name_too_long() {
        let long_name = "a".repeat(129);
        assert!(validate_table_name(&long_name).is_err());
    }

    #[test]
    fn test_validate_table_name_max_length() {
        let max_name = "a".repeat(128);
        assert!(validate_table_name(&max_name).is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn test_quote_identifier_no_backticks() {
        assert_eq!(quote_identifier("my_table"), "my_table");
    }

    #[test]
    fn test_quote_identifier_with_backticks() {
        assert_eq!(quote_identifier("my`table"), "my``table");
        assert_eq!(quote_identifier("`table`"), "``table``");
    }

    #[test]
    fn test_quote_identifier_multiple_backticks() {
        assert_eq!(quote_identifier("a`b`c"), "a``b``c");
    }

    #[test]
    fn test_sql_simple_select_allowed() {
        assert!(validate_sql_for_define_table("SELECT 1").is_ok());
        assert!(validate_sql_for_define_table("SELECT id, name FROM users").is_ok());
    }

    #[test]
    fn test_sql_select_with_cte_allowed() {
        assert!(
            validate_sql_for_define_table("WITH cte AS (SELECT 1 AS id) SELECT * FROM cte").is_ok()
        );
    }

    #[test]
    fn test_sql_select_with_subquery_allowed() {
        assert!(
            validate_sql_for_define_table("SELECT * FROM (SELECT 1 AS id) AS sub").is_ok()
        );
    }

    #[test]
    fn test_sql_select_with_joins_allowed() {
        assert!(validate_sql_for_define_table(
            "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id"
        )
        .is_ok());
    }

    #[test]
    fn test_sql_drop_blocked() {
        let result = validate_sql_for_define_table("DROP TABLE users");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Only SELECT"));
    }

    #[test]
    fn test_sql_truncate_blocked() {
        let result = validate_sql_for_define_table("TRUNCATE TABLE users");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_delete_blocked() {
        let result = validate_sql_for_define_table("DELETE FROM users");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_insert_blocked() {
        let result = validate_sql_for_define_table("INSERT INTO users VALUES (1)");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_update_blocked() {
        let result = validate_sql_for_define_table("UPDATE users SET name = 'x'");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_create_blocked() {
        let result = validate_sql_for_define_table("CREATE TABLE users (id INT)");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_merge_blocked() {
        let result = validate_sql_for_define_table(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("MERGE"));
    }

    #[test]
    fn test_sql_multistatement_blocked() {
        let result = validate_sql_for_define_table("SELECT 1; DROP TABLE users");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("single SELECT"));
    }

    #[test]
    fn test_sql_empty_blocked() {
        let result = validate_sql_for_define_table("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_sql_whitespace_only_blocked() {
        let result = validate_sql_for_define_table("   \n\t  ");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_quoted_keyword_allowed() {
        assert!(validate_sql_for_define_table("SELECT `DROP` AS col FROM t").is_ok());
    }

    #[test]
    fn test_sql_string_with_keyword_allowed() {
        assert!(validate_sql_for_define_table("SELECT 'DROP TABLE users' AS msg").is_ok());
    }

    #[test]
    fn test_sql_union_allowed() {
        assert!(validate_sql_for_define_table("SELECT 1 AS id UNION ALL SELECT 2 AS id").is_ok());
    }

    #[test]
    fn test_sql_subquery_in_where_allowed() {
        assert!(validate_sql_for_define_table(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        )
        .is_ok());
    }

    #[test]
    fn test_sql_exists_subquery_allowed() {
        assert!(validate_sql_for_define_table(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
        )
        .is_ok());
    }

    #[test]
    fn test_sql_case_expression_allowed() {
        assert!(
            validate_sql_for_define_table(
                "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_sql_function_call_allowed() {
        assert!(
            validate_sql_for_define_table("SELECT COUNT(*), SUM(amount), AVG(price) FROM orders")
                .is_ok()
        );
    }

    #[test]
    fn test_sql_window_function_allowed() {
        assert!(validate_sql_for_define_table(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY date) FROM t"
        )
        .is_ok());
    }

    #[test]
    fn test_sql_nested_cte_allowed() {
        assert!(validate_sql_for_define_table(
            "WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT * FROM b"
        )
        .is_ok());
    }
}
