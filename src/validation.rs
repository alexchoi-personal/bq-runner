use crate::error::{Error, Result};
use regex::Regex;
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
}
