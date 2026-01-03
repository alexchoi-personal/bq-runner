use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use sqlparser::ast::{
    Expr, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, With,
};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;

const SQL_CACHE_SIZE_PER_SHARD: usize = 64;
const CACHE_SHARDS: usize = 8;

struct ParseCacheShard {
    entries: HashMap<u64, Arc<Vec<Statement>>>,
    order: VecDeque<u64>,
}

impl ParseCacheShard {
    fn new() -> Self {
        Self {
            entries: HashMap::with_capacity(SQL_CACHE_SIZE_PER_SHARD),
            order: VecDeque::with_capacity(SQL_CACHE_SIZE_PER_SHARD),
        }
    }

    fn get(&self, hash: u64) -> Option<Arc<Vec<Statement>>> {
        self.entries.get(&hash).cloned()
    }

    fn insert(&mut self, hash: u64, statements: Arc<Vec<Statement>>) {
        if self.entries.contains_key(&hash) {
            return;
        }
        if self.entries.len() >= SQL_CACHE_SIZE_PER_SHARD {
            if let Some(old_hash) = self.order.pop_front() {
                self.entries.remove(&old_hash);
            }
        }
        self.entries.insert(hash, statements);
        self.order.push_back(hash);
    }
}

struct ShardedParseCache {
    shards: [Mutex<ParseCacheShard>; CACHE_SHARDS],
}

impl ShardedParseCache {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(ParseCacheShard::new())),
        }
    }

    fn get_shard(&self, hash: u64) -> &Mutex<ParseCacheShard> {
        &self.shards[(hash as usize) % CACHE_SHARDS]
    }
}

static PARSE_CACHE: LazyLock<ShardedParseCache> = LazyLock::new(ShardedParseCache::new);

fn hash_sql(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn parse_sql_cached(sql: &str) -> Option<Arc<Vec<Statement>>> {
    let hash = hash_sql(sql);
    let shard = PARSE_CACHE.get_shard(hash);

    {
        let cache = shard.lock();
        if let Some(statements) = cache.get(hash) {
            return Some(statements);
        }
    }

    let dialect = BigQueryDialect {};
    let statements = Arc::new(Parser::parse_sql(&dialect, sql).ok()?);

    {
        let mut cache = shard.lock();
        cache.insert(hash, Arc::clone(&statements));
    }

    Some(statements)
}

pub fn extract_dependencies(sql: &str, table_name_lookup: &HashMap<String, String>) -> Vec<String> {
    let Some(statements) = parse_sql_cached(sql) else {
        return vec![];
    };

    let mut cte_names = HashSet::new();
    let mut referenced_tables = HashSet::new();
    for statement in statements.iter() {
        if let Statement::Query(query) = statement {
            collect_cte_names_from_query(query, &mut cte_names);
        }
        collect_tables_from_statement(statement, &mut referenced_tables);
    }

    let mut deps: Vec<String> = referenced_tables
        .into_iter()
        .filter_map(|t| {
            let upper = t.to_uppercase();
            if !cte_names.contains(&upper) {
                table_name_lookup.get(&upper).cloned()
            } else {
                None
            }
        })
        .collect();

    deps.sort();
    deps.dedup();
    deps
}

#[cfg(test)]
pub(super) fn extract_cte_names(sql: &str) -> HashSet<String> {
    let dialect = BigQueryDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return HashSet::new();
    };

    let mut cte_names = HashSet::new();
    for statement in statements {
        if let Statement::Query(query) = statement {
            collect_cte_names_from_query(&query, &mut cte_names);
        }
    }
    cte_names
}

fn collect_cte_names_from_query(query: &Query, cte_names: &mut HashSet<String>) {
    if let Some(with) = &query.with {
        collect_cte_names_from_with(with, cte_names);
    }
}

fn collect_cte_names_from_with(with: &With, cte_names: &mut HashSet<String>) {
    for cte in &with.cte_tables {
        cte_names.insert(cte.alias.name.value.to_uppercase());
    }
}

fn collect_tables_from_statement(statement: &Statement, tables: &mut HashSet<String>) {
    if let Statement::Query(query) = statement {
        collect_tables_from_query(query, tables);
    }
}

fn collect_tables_from_query(query: &Query, tables: &mut HashSet<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_tables_from_query(&cte.query, tables);
        }
    }
    collect_tables_from_set_expr(&query.body, tables);
}

fn collect_tables_from_set_expr(body: &SetExpr, tables: &mut HashSet<String>) {
    match body {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_tables_from_table_with_joins(table_with_joins, tables);
            }
            for item in &select.projection {
                if let SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) = item
                {
                    collect_tables_from_expr(expr, tables);
                }
            }
            if let Some(selection) = &select.selection {
                collect_tables_from_expr(selection, tables);
            }
            if let Some(having) = &select.having {
                collect_tables_from_expr(having, tables);
            }
        }
        SetExpr::Query(subquery) => collect_tables_from_query(subquery, tables),
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_from_set_expr(left, tables);
            collect_tables_from_set_expr(right, tables);
        }
        SetExpr::Values(_) => {}
        _ => {}
    }
}

fn collect_tables_from_table_with_joins(twj: &TableWithJoins, tables: &mut HashSet<String>) {
    collect_tables_from_table_factor(&twj.relation, tables);
    for join in &twj.joins {
        collect_tables_from_table_factor(&join.relation, tables);
    }
}

fn collect_tables_from_table_factor(factor: &TableFactor, tables: &mut HashSet<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            let table_name = name.0.last().map(|id| id.value.clone()).unwrap_or_default();
            if !table_name.is_empty() {
                tables.insert(table_name);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_tables_from_query(subquery, tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_tables_from_table_with_joins(table_with_joins, tables);
        }
        TableFactor::TableFunction { expr, .. } => {
            collect_tables_from_expr(expr, tables);
        }
        TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                collect_tables_from_expr(expr, tables);
            }
        }
        _ => {}
    }
}

fn collect_tables_from_expr(expr: &Expr, tables: &mut HashSet<String>) {
    match expr {
        Expr::Subquery(query) => collect_tables_from_query(query, tables),
        Expr::InSubquery { subquery, expr, .. } => {
            collect_tables_from_query(subquery, tables);
            collect_tables_from_expr(expr, tables);
        }
        Expr::Exists { subquery, .. } => collect_tables_from_query(subquery, tables),
        Expr::BinaryOp { left, right, .. } => {
            collect_tables_from_expr(left, tables);
            collect_tables_from_expr(right, tables);
        }
        Expr::UnaryOp { expr, .. } => collect_tables_from_expr(expr, tables),
        Expr::Nested(inner) => collect_tables_from_expr(inner, tables),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_tables_from_expr(op, tables);
            }
            for cond in conditions {
                collect_tables_from_expr(cond, tables);
            }
            for res in results {
                collect_tables_from_expr(res, tables);
            }
            if let Some(else_expr) = else_result {
                collect_tables_from_expr(else_expr, tables);
            }
        }
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        collect_tables_from_expr(e, tables);
                    }
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_lookup(names: &[&str]) -> HashMap<String, String> {
        names
            .iter()
            .map(|n| (n.to_uppercase(), n.to_string()))
            .collect()
    }

    #[test]
    fn test_extract_dependencies_empty_sql() {
        let lookup = HashMap::new();
        let deps = extract_dependencies("SELECT 1", &lookup);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_extract_dependencies_no_known_tables() {
        let lookup = HashMap::new();
        let deps = extract_dependencies("SELECT * FROM users", &lookup);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_extract_dependencies_single_from() {
        let lookup = make_lookup(&["users"]);
        let deps = extract_dependencies("SELECT * FROM users", &lookup);
        assert_eq!(deps, vec!["users"]);
    }

    #[test]
    fn test_extract_dependencies_case_insensitive() {
        let lookup = make_lookup(&["Users"]);
        let deps = extract_dependencies("SELECT * FROM USERS", &lookup);
        assert_eq!(deps, vec!["Users"]);
    }

    #[test]
    fn test_extract_dependencies_multiple_tables() {
        let lookup = make_lookup(&["users", "orders"]);
        let deps = extract_dependencies(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            &lookup,
        );
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_dependencies_comma_syntax() {
        let lookup = make_lookup(&["a", "b"]);
        let deps = extract_dependencies("SELECT * FROM a, b WHERE a.id = b.id", &lookup);
        assert!(deps.contains(&"a".to_string()));
        assert!(deps.contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_dependencies_excludes_cte_names() {
        let lookup = make_lookup(&["temp", "users"]);
        let sql = "WITH temp AS (SELECT * FROM users) SELECT * FROM temp";
        let deps = extract_dependencies(sql, &lookup);
        assert_eq!(deps, vec!["users"]);
        assert!(!deps.contains(&"temp".to_string()));
    }

    #[test]
    fn test_extract_dependencies_no_duplicates() {
        let lookup = make_lookup(&["users"]);
        let sql = "SELECT * FROM users UNION SELECT * FROM users";
        let deps = extract_dependencies(sql, &lookup);
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], "users");
    }

    #[test]
    fn test_extract_dependencies_sorted() {
        let lookup = make_lookup(&["zebra", "apple", "middle"]);
        let sql = "SELECT * FROM zebra, middle JOIN apple ON true";
        let deps = extract_dependencies(sql, &lookup);
        assert_eq!(deps, vec!["apple", "middle", "zebra"]);
    }

    #[test]
    fn test_extract_dependencies_partial_match_excluded() {
        let lookup = make_lookup(&["user"]);
        let sql = "SELECT * FROM users";
        let deps = extract_dependencies(sql, &lookup);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_extract_cte_names_no_with() {
        let ctes = extract_cte_names("SELECT * FROM users");
        assert!(ctes.is_empty());
    }

    #[test]
    fn test_extract_cte_names_single() {
        let ctes = extract_cte_names("WITH temp AS (SELECT 1) SELECT * FROM temp");
        assert!(ctes.contains("TEMP"));
    }

    #[test]
    fn test_extract_cte_names_multiple() {
        let sql = "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("A"));
        assert!(ctes.contains("B"));
    }

    #[test]
    fn test_extract_cte_names_recursive() {
        let sql = "WITH RECURSIVE tree AS (SELECT 1) SELECT * FROM tree";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("TREE"));
    }

    #[test]
    fn test_extract_cte_names_nested_parens() {
        let sql = "WITH cte AS (SELECT * FROM (SELECT 1) sub) SELECT * FROM cte";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE"));
    }

    #[test]
    fn test_extract_cte_names_stops_at_select() {
        let sql = "WITH cte AS (SELECT 1) SELECT * FROM cte, other_table";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE"));
        assert!(!ctes.contains("OTHER_TABLE"));
    }

    #[test]
    fn test_extract_cte_names_stops_at_insert() {
        let sql = "WITH cte AS (SELECT 1) INSERT INTO target SELECT * FROM cte";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE"));
    }

    #[test]
    fn test_extract_cte_names_stops_at_update() {
        let sql = "WITH cte AS (SELECT 1) UPDATE target SET x = 1";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE"));
    }

    #[test]
    fn test_extract_cte_names_invalid_sql_returns_empty() {
        let sql = "WITH cte AS (SELECT 1) DELETE FROM target";
        let ctes = extract_cte_names(sql);
        assert!(ctes.is_empty());
    }

    #[test]
    fn test_extract_cte_names_underscore_in_name() {
        let sql = "WITH my_cte AS (SELECT 1) SELECT * FROM my_cte";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("MY_CTE"));
    }

    #[test]
    fn test_extract_cte_names_numeric_suffix() {
        let sql = "WITH cte1 AS (SELECT 1) SELECT * FROM cte1";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE1"));
    }

    #[test]
    fn test_extract_dependencies_subquery() {
        let lookup = make_lookup(&["users", "orders"]);
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
        let deps = extract_dependencies(sql, &lookup);
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_dependencies_derived_table() {
        let lookup = make_lookup(&["users"]);
        let sql = "SELECT * FROM (SELECT * FROM users) AS sub";
        let deps = extract_dependencies(sql, &lookup);
        assert_eq!(deps, vec!["users"]);
    }

    #[test]
    fn test_extract_dependencies_exists_clause() {
        let lookup = make_lookup(&["users", "orders"]);
        let sql = "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)";
        let deps = extract_dependencies(sql, &lookup);
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_dependencies_union_query() {
        let lookup = make_lookup(&["a", "b"]);
        let sql = "SELECT * FROM a UNION ALL SELECT * FROM b";
        let deps = extract_dependencies(sql, &lookup);
        assert!(deps.contains(&"a".to_string()));
        assert!(deps.contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_dependencies_nested_cte() {
        let lookup = make_lookup(&["base", "intermediate"]);
        let sql = "WITH intermediate AS (SELECT * FROM base) SELECT * FROM intermediate";
        let deps = extract_dependencies(sql, &lookup);
        assert_eq!(deps, vec!["base"]);
    }

    #[test]
    fn test_diamond_dependency_pattern() {
        let lookup = make_lookup(&["A", "B", "C", "D"]);

        let sql_b = "SELECT * FROM A";
        let sql_c = "SELECT * FROM A";
        let sql_d = "SELECT * FROM B, C";

        let deps_b = extract_dependencies(sql_b, &lookup);
        let deps_c = extract_dependencies(sql_c, &lookup);
        let deps_d = extract_dependencies(sql_d, &lookup);

        assert_eq!(deps_b, vec!["A"]);
        assert_eq!(deps_c, vec!["A"]);
        assert!(deps_d.contains(&"B".to_string()));
        assert!(deps_d.contains(&"C".to_string()));
    }

    #[test]
    fn test_extract_dependencies_table_at_end() {
        let lookup = make_lookup(&["t"]);
        let deps = extract_dependencies("SELECT * FROM t", &lookup);
        assert_eq!(deps, vec!["t"]);
    }

    #[test]
    fn test_extract_cte_names_whitespace_handling() {
        let sql = "WITH   cte   AS (SELECT 1) SELECT * FROM cte";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE"));
    }

    #[test]
    fn test_extract_cte_names_empty_parens() {
        let ctes = extract_cte_names("WITH ");
        assert!(ctes.is_empty());
    }

    #[test]
    fn test_extract_dependencies_complex_join() {
        let lookup = make_lookup(&["users", "orders", "products"]);
        let sql = "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id";
        let deps = extract_dependencies(sql, &lookup);
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"orders".to_string()));
        assert!(deps.contains(&"products".to_string()));
    }
}
