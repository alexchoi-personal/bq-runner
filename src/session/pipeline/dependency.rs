use std::collections::{HashMap, HashSet};

use super::types::PipelineTable;

pub fn extract_dependencies(
    sql: &str,
    known_tables: &HashMap<String, PipelineTable>,
) -> Vec<String> {
    let cte_names = extract_cte_names(sql);
    let mut deps = Vec::new();
    let sql_upper = sql.to_uppercase();

    for table_name in known_tables.keys() {
        let name_upper = table_name.to_uppercase();

        if cte_names.contains(&name_upper) {
            continue;
        }

        if is_table_referenced(&sql_upper, &name_upper) && !deps.contains(table_name) {
            deps.push(table_name.clone());
        }
    }

    deps.sort();
    deps
}

pub fn extract_cte_names(sql: &str) -> HashSet<String> {
    let mut cte_names = HashSet::new();
    let sql_upper = sql.to_uppercase();

    let Some(with_pos) = sql_upper.find("WITH ") else {
        return cte_names;
    };

    let mut sql_after_with = &sql_upper[with_pos + 5..];
    let trimmed = sql_after_with.trim_start();
    if let Some(stripped) = trimmed.strip_prefix("RECURSIVE ") {
        sql_after_with = stripped;
    }

    let mut in_parens = 0;
    let mut current_pos = 0;
    let mut looking_for_name = true;
    let chars: Vec<char> = sql_after_with.chars().collect();

    while current_pos < chars.len() {
        let ch = chars[current_pos];

        if looking_for_name {
            while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                current_pos += 1;
            }
            if current_pos >= chars.len() {
                break;
            }

            let start = current_pos;
            while current_pos < chars.len()
                && (chars[current_pos].is_alphanumeric() || chars[current_pos] == '_')
            {
                current_pos += 1;
            }

            if current_pos > start {
                let name: String = chars[start..current_pos].iter().collect();
                cte_names.insert(name);
            }

            looking_for_name = false;
            continue;
        }

        match ch {
            '(' => in_parens += 1,
            ')' => in_parens -= 1,
            ',' if in_parens == 0 => {
                looking_for_name = true;
            }
            _ => {}
        }

        if in_parens == 0 {
            let remaining: String = chars[current_pos..].iter().collect();
            let trimmed = remaining.trim_start();

            if trimmed.starts_with("SELECT")
                || trimmed.starts_with("INSERT")
                || trimmed.starts_with("UPDATE")
                || trimmed.starts_with("DELETE")
            {
                break;
            }
        }

        current_pos += 1;
    }

    cte_names
}

fn is_table_referenced(sql_upper: &str, table_name_upper: &str) -> bool {
    let patterns = [
        format!("FROM {}", table_name_upper),
        format!("JOIN {}", table_name_upper),
        format!(", {}", table_name_upper),
    ];

    for pattern in &patterns {
        if let Some(pos) = sql_upper.find(pattern.as_str()) {
            let after_pos = pos + pattern.len();
            if after_pos >= sql_upper.len() {
                return true;
            }
            let next_char = sql_upper.chars().nth(after_pos).unwrap_or(' ');
            if !next_char.is_alphanumeric() && next_char != '_' {
                return true;
            }
        }
    }

    let qualified_patterns = [
        format!("FROM {}.{}", table_name_upper, ""),
        format!("JOIN {}.{}", table_name_upper, ""),
    ];

    for pattern in &qualified_patterns {
        let base = pattern.trim_end_matches('.');
        if sql_upper.contains(&format!("{}.", base)) {
            return false;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pipeline_table(name: &str) -> PipelineTable {
        PipelineTable {
            name: name.to_string(),
            sql: None,
            schema: None,
            rows: vec![],
            dependencies: vec![],
            is_source: false,
        }
    }

    #[test]
    fn test_extract_dependencies_empty_sql() {
        let known_tables = HashMap::new();
        let deps = extract_dependencies("SELECT 1", &known_tables);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_extract_dependencies_no_known_tables() {
        let known_tables = HashMap::new();
        let deps = extract_dependencies("SELECT * FROM users", &known_tables);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_extract_dependencies_single_from() {
        let mut known_tables = HashMap::new();
        known_tables.insert("users".to_string(), make_pipeline_table("users"));
        let deps = extract_dependencies("SELECT * FROM users", &known_tables);
        assert_eq!(deps, vec!["users"]);
    }

    #[test]
    fn test_extract_dependencies_case_insensitive() {
        let mut known_tables = HashMap::new();
        known_tables.insert("Users".to_string(), make_pipeline_table("Users"));
        let deps = extract_dependencies("SELECT * FROM USERS", &known_tables);
        assert_eq!(deps, vec!["Users"]);
    }

    #[test]
    fn test_extract_dependencies_multiple_tables() {
        let mut known_tables = HashMap::new();
        known_tables.insert("users".to_string(), make_pipeline_table("users"));
        known_tables.insert("orders".to_string(), make_pipeline_table("orders"));
        let deps = extract_dependencies(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            &known_tables,
        );
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"orders".to_string()));
    }

    #[test]
    fn test_extract_dependencies_comma_syntax() {
        let mut known_tables = HashMap::new();
        known_tables.insert("a".to_string(), make_pipeline_table("a"));
        known_tables.insert("b".to_string(), make_pipeline_table("b"));
        let deps = extract_dependencies("SELECT * FROM a, b WHERE a.id = b.id", &known_tables);
        assert!(deps.contains(&"a".to_string()));
        assert!(deps.contains(&"b".to_string()));
    }

    #[test]
    fn test_extract_dependencies_excludes_cte_names() {
        let mut known_tables = HashMap::new();
        known_tables.insert("temp".to_string(), make_pipeline_table("temp"));
        known_tables.insert("users".to_string(), make_pipeline_table("users"));
        let sql = "WITH temp AS (SELECT * FROM users) SELECT * FROM temp";
        let deps = extract_dependencies(sql, &known_tables);
        assert_eq!(deps, vec!["users"]);
        assert!(!deps.contains(&"temp".to_string()));
    }

    #[test]
    fn test_extract_dependencies_no_duplicates() {
        let mut known_tables = HashMap::new();
        known_tables.insert("users".to_string(), make_pipeline_table("users"));
        let sql = "SELECT * FROM users UNION SELECT * FROM users";
        let deps = extract_dependencies(sql, &known_tables);
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], "users");
    }

    #[test]
    fn test_extract_dependencies_sorted() {
        let mut known_tables = HashMap::new();
        known_tables.insert("zebra".to_string(), make_pipeline_table("zebra"));
        known_tables.insert("apple".to_string(), make_pipeline_table("apple"));
        known_tables.insert("middle".to_string(), make_pipeline_table("middle"));
        let sql = "SELECT * FROM zebra, middle JOIN apple ON true";
        let deps = extract_dependencies(sql, &known_tables);
        assert_eq!(deps, vec!["apple", "middle", "zebra"]);
    }

    #[test]
    fn test_extract_dependencies_partial_match_excluded() {
        let mut known_tables = HashMap::new();
        known_tables.insert("user".to_string(), make_pipeline_table("user"));
        let sql = "SELECT * FROM users";
        let deps = extract_dependencies(sql, &known_tables);
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
    fn test_extract_cte_names_stops_at_delete() {
        let sql = "WITH cte AS (SELECT 1) DELETE FROM target";
        let ctes = extract_cte_names(sql);
        assert!(ctes.contains("CTE"));
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
    fn test_is_table_referenced_from() {
        assert!(is_table_referenced("SELECT * FROM USERS", "USERS"));
    }

    #[test]
    fn test_is_table_referenced_join() {
        assert!(is_table_referenced(
            "SELECT * FROM A JOIN USERS ON TRUE",
            "USERS"
        ));
    }

    #[test]
    fn test_is_table_referenced_comma() {
        assert!(is_table_referenced("SELECT * FROM A, USERS", "USERS"));
    }

    #[test]
    fn test_is_table_referenced_end_of_string() {
        assert!(is_table_referenced("FROM USERS", "USERS"));
    }

    #[test]
    fn test_is_table_referenced_followed_by_keyword() {
        assert!(is_table_referenced("SELECT * FROM USERS WHERE", "USERS"));
    }

    #[test]
    fn test_is_table_referenced_partial_name_rejected() {
        assert!(!is_table_referenced("SELECT * FROM USERS_TABLE", "USERS"));
    }

    #[test]
    fn test_is_table_referenced_not_found() {
        assert!(!is_table_referenced("SELECT * FROM ORDERS", "USERS"));
    }

    #[test]
    fn test_diamond_dependency_pattern() {
        let mut known_tables = HashMap::new();
        known_tables.insert("A".to_string(), make_pipeline_table("A"));
        known_tables.insert("B".to_string(), make_pipeline_table("B"));
        known_tables.insert("C".to_string(), make_pipeline_table("C"));
        known_tables.insert("D".to_string(), make_pipeline_table("D"));

        let sql_b = "SELECT * FROM A";
        let sql_c = "SELECT * FROM A";
        let sql_d = "SELECT * FROM B, C";

        let deps_b = extract_dependencies(sql_b, &known_tables);
        let deps_c = extract_dependencies(sql_c, &known_tables);
        let deps_d = extract_dependencies(sql_d, &known_tables);

        assert_eq!(deps_b, vec!["A"]);
        assert_eq!(deps_c, vec!["A"]);
        assert!(deps_d.contains(&"B".to_string()));
        assert!(deps_d.contains(&"C".to_string()));
    }

    #[test]
    fn test_extract_dependencies_table_at_end() {
        let mut known_tables = HashMap::new();
        known_tables.insert("t".to_string(), make_pipeline_table("t"));
        let deps = extract_dependencies("SELECT * FROM t", &known_tables);
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
        let mut known_tables = HashMap::new();
        known_tables.insert("users".to_string(), make_pipeline_table("users"));
        known_tables.insert("orders".to_string(), make_pipeline_table("orders"));
        known_tables.insert("products".to_string(), make_pipeline_table("products"));
        let sql = "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id";
        let deps = extract_dependencies(sql, &known_tables);
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"orders".to_string()));
        assert!(deps.contains(&"products".to_string()));
    }
}
