use serde_json::Value;

use crate::error::{Error, Result};
use crate::validation::quote_identifier;

use super::converters::json_to_sql_value_into;

#[derive(Debug)]
pub(crate) struct ParsedRows {
    pub column_names: Option<Vec<String>>,
    pub values: Vec<String>,
}

pub(crate) fn parse_insert_rows(rows: &[Value]) -> Result<ParsedRows> {
    if rows.is_empty() {
        return Ok(ParsedRows {
            column_names: None,
            values: vec![],
        });
    }

    let first_row = &rows[0];
    let expects_object = matches!(first_row, Value::Object(_));

    for (idx, row) in rows.iter().enumerate() {
        let is_object = matches!(row, Value::Object(_));
        let is_array = matches!(row, Value::Array(_));

        if !is_object && !is_array {
            return Err(Error::InvalidRequest(format!(
                "Mixed row formats detected: row {} has invalid type '{}'. All rows must be either objects or arrays.",
                idx,
                match row {
                    Value::String(_) => "string",
                    Value::Number(_) => "number",
                    Value::Bool(_) => "boolean",
                    Value::Null => "null",
                    _ => "unknown",
                }
            )));
        }

        if is_object != expects_object {
            return Err(Error::InvalidRequest(format!(
                "Mixed row formats detected: row 0 is {}, but row {} is {}. All rows must be either objects or arrays.",
                if expects_object { "object" } else { "array" },
                idx,
                if is_object { "object" } else { "array" }
            )));
        }
    }

    let (column_names, values) = if expects_object {
        let first_obj = first_row
            .as_object()
            .ok_or_else(|| Error::InvalidRequest("Expected object row format".into()))?;
        let cols: Vec<String> = first_obj.keys().cloned().collect();
        let avg_row_len = cols.len() * 16 + 2;
        let mut all_values = String::with_capacity(rows.len() * (avg_row_len + 1));
        let mut offsets: Vec<usize> = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for (row_idx, row) in rows.iter().enumerate() {
            let obj = row
                .as_object()
                .ok_or_else(|| Error::InvalidRequest("Expected object row format".into()))?;
            all_values.push('(');
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    all_values.push_str(", ");
                }
                let val = obj.get(col).ok_or_else(|| {
                    Error::InvalidRequest(format!("Row {} is missing column '{}'", row_idx, col))
                })?;
                json_to_sql_value_into(val, &mut all_values);
            }
            all_values.push(')');
            offsets.push(all_values.len());
        }
        let vals: Vec<String> = offsets
            .windows(2)
            .map(|w| all_values[w[0]..w[1]].to_string())
            .collect();
        (Some(cols), vals)
    } else {
        let avg_cols = rows
            .first()
            .and_then(|r| r.as_array())
            .map(|a| a.len())
            .unwrap_or(4);
        let avg_row_len = avg_cols * 16 + 2;
        let mut all_values = String::with_capacity(rows.len() * (avg_row_len + 1));
        let mut offsets: Vec<usize> = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for row in rows {
            let arr = row
                .as_array()
                .ok_or_else(|| Error::InvalidRequest("Expected array row format".into()))?;
            all_values.push('(');
            for (i, val) in arr.iter().enumerate() {
                if i > 0 {
                    all_values.push_str(", ");
                }
                json_to_sql_value_into(val, &mut all_values);
            }
            all_values.push(')');
            offsets.push(all_values.len());
        }
        let vals: Vec<String> = offsets
            .windows(2)
            .map(|w| all_values[w[0]..w[1]].to_string())
            .collect();
        (None, vals)
    };

    Ok(ParsedRows {
        column_names,
        values,
    })
}

pub(crate) fn build_insert_sql(
    table_name: &str,
    column_names: Option<&[String]>,
    values: &[String],
) -> String {
    let quoted_table = format!("`{}`", quote_identifier(table_name));
    let values_total_len: usize = values.iter().map(|v| v.len()).sum();
    let values_joined_len = values_total_len + values.len().saturating_sub(1) * 2;

    match column_names {
        Some(cols) => {
            let cols_estimated_len: usize = cols.iter().map(|c| c.len() + 4).sum();
            let estimated_len = 20 + quoted_table.len() + cols_estimated_len + values_joined_len;
            let mut sql = String::with_capacity(estimated_len);

            sql.push_str("INSERT INTO ");
            sql.push_str(&quoted_table);
            sql.push_str(" (");
            for (i, c) in cols.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push('`');
                sql.push_str(&quote_identifier(c));
                sql.push('`');
            }
            sql.push_str(") VALUES ");
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(v);
            }
            sql
        }
        None => {
            let estimated_len = 20 + quoted_table.len() + values_joined_len;
            let mut sql = String::with_capacity(estimated_len);

            sql.push_str("INSERT INTO ");
            sql.push_str(&quoted_table);
            sql.push_str(" VALUES ");
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(v);
            }
            sql
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_insert_rows_empty() {
        let result = parse_insert_rows(&[]).unwrap();
        assert!(result.column_names.is_none());
        assert!(result.values.is_empty());
    }

    #[test]
    fn test_parse_insert_rows_array_format() {
        let rows = vec![json!([1, "Alice"]), json!([2, "Bob"])];
        let result = parse_insert_rows(&rows).unwrap();
        assert!(result.column_names.is_none());
        assert_eq!(result.values.len(), 2);
        assert_eq!(result.values[0], "(1, 'Alice')");
        assert_eq!(result.values[1], "(2, 'Bob')");
    }

    #[test]
    fn test_parse_insert_rows_object_format() {
        let rows = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let result = parse_insert_rows(&rows).unwrap();
        assert!(result.column_names.is_some());
        assert_eq!(result.values.len(), 2);
    }

    #[test]
    fn test_parse_insert_rows_mixed_format_error() {
        let rows = vec![json!([1]), json!({"id": 2})];
        let result = parse_insert_rows(&rows);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Mixed row formats"));
    }

    #[test]
    fn test_parse_insert_rows_invalid_type_error() {
        let rows = vec![json!([1]), json!("invalid")];
        let result = parse_insert_rows(&rows);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid type"));
    }

    #[test]
    fn test_parse_insert_rows_missing_column_error() {
        let rows = vec![json!({"id": 1, "name": "Alice"}), json!({"id": 2})];
        let result = parse_insert_rows(&rows);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing column"));
    }

    #[test]
    fn test_build_insert_sql_no_columns() {
        let values = vec!["(1, 'Alice')".to_string(), "(2, 'Bob')".to_string()];
        let sql = build_insert_sql("users", None, &values);
        assert_eq!(sql, "INSERT INTO `users` VALUES (1, 'Alice'), (2, 'Bob')");
    }

    #[test]
    fn test_build_insert_sql_with_columns() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let values = vec!["(1, 'Alice')".to_string()];
        let sql = build_insert_sql("users", Some(&cols), &values);
        assert_eq!(
            sql,
            "INSERT INTO `users` (`id`, `name`) VALUES (1, 'Alice')"
        );
    }

    #[test]
    fn test_build_insert_sql_empty_values() {
        let sql = build_insert_sql("users", None, &[]);
        assert_eq!(sql, "INSERT INTO `users` VALUES ");
    }

    #[test]
    fn test_build_insert_sql_special_table_name() {
        let values = vec!["(1)".to_string()];
        let sql = build_insert_sql("my`table", None, &values);
        assert!(sql.contains("`my``table`"));
    }
}
