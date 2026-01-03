mod arrow;
mod json;
mod yacht;

pub(crate) use arrow::arrow_value_to_sql;
pub use json::json_to_sql_value;
pub(crate) use yacht::{base64_encode, datatype_to_bq_type, yacht_value_to_json};

fn escape_sql_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\"),
            '\0' => result.push_str("\\0"),
            _ => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_sql_string_no_special_chars() {
        assert_eq!(escape_sql_string("hello"), "hello");
    }

    #[test]
    fn test_escape_sql_string_single_quote() {
        assert_eq!(escape_sql_string("it's"), "it''s");
    }

    #[test]
    fn test_escape_sql_string_backslash() {
        assert_eq!(escape_sql_string("path\\file"), "path\\\\file");
    }

    #[test]
    fn test_escape_sql_string_null_byte() {
        assert_eq!(escape_sql_string("null\0byte"), "null\\0byte");
    }

    #[test]
    fn test_escape_sql_string_multiple_special() {
        assert_eq!(escape_sql_string("it's a\\path"), "it''s a\\\\path");
    }

    #[test]
    fn test_escape_sql_string_empty() {
        assert_eq!(escape_sql_string(""), "");
    }
}
