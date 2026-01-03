mod arrow;
mod json;
mod yacht;

#[cfg(test)]
use std::borrow::Cow;

#[cfg(test)]
pub(crate) use arrow::arrow_value_to_sql;
pub(crate) use arrow::arrow_value_to_sql_into;
pub use json::json_to_sql_value;
pub(crate) use json::json_to_sql_value_into;
pub(crate) use yacht::{base64_encode, datatype_to_bq_type, yacht_value_to_json};

#[cfg(test)]
fn escape_sql_string(s: &str) -> Cow<'_, str> {
    let needs_escaping = s.chars().any(|c| c == '\'' || c == '\\' || c == '\0');
    if !needs_escaping {
        return Cow::Borrowed(s);
    }

    let escaped_count = s
        .chars()
        .filter(|&c| c == '\'' || c == '\\' || c == '\0')
        .count();
    let mut result = String::with_capacity(s.len() + escaped_count);
    for c in s.chars() {
        match c {
            '\'' => result.push_str("''"),
            '\\' => result.push_str("\\\\"),
            '\0' => result.push_str("\\0"),
            _ => result.push(c),
        }
    }
    Cow::Owned(result)
}

fn escape_sql_string_into(s: &str, buf: &mut String) {
    if !s.bytes().any(|b| b == b'\'' || b == b'\\' || b == 0) {
        buf.push_str(s);
        return;
    }
    for c in s.chars() {
        match c {
            '\'' => buf.push_str("''"),
            '\\' => buf.push_str("\\\\"),
            '\0' => buf.push_str("\\0"),
            _ => buf.push(c),
        }
    }
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
