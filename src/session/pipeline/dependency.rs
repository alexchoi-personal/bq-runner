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
