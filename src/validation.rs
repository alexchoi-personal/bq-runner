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
