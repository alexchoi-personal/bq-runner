use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
        }
    }

    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, "INT64")
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, "STRING")
    }

    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, "FLOAT64")
    }

    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, "BOOLEAN")
    }

    pub fn date(name: impl Into<String>) -> Self {
        Self::new(name, "DATE")
    }

    pub fn timestamp(name: impl Into<String>) -> Self {
        Self::new(name, "TIMESTAMP")
    }

    pub fn numeric(name: impl Into<String>) -> Self {
        Self::new(name, "NUMERIC")
    }

    pub fn bytes(name: impl Into<String>) -> Self {
        Self::new(name, "BYTES")
    }
}

impl From<(String, String)> for ColumnDef {
    fn from((name, column_type): (String, String)) -> Self {
        Self { name, column_type }
    }
}

impl From<(&str, &str)> for ColumnDef {
    fn from((name, column_type): (&str, &str)) -> Self {
        Self {
            name: name.to_string(),
            column_type: column_type.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub sql: Option<String>,
    pub schema: Option<Vec<ColumnDef>>,
    pub rows: Vec<Value>,
    pub dependencies: Vec<String>,
    pub is_source: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    pub name: String,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_def_new() {
        let col = ColumnDef::new("id", "INT64");
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, "INT64");
    }

    #[test]
    fn test_column_def_int64() {
        let col = ColumnDef::int64("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, "INT64");
    }

    #[test]
    fn test_column_def_string() {
        let col = ColumnDef::string("name");
        assert_eq!(col.name, "name");
        assert_eq!(col.column_type, "STRING");
    }

    #[test]
    fn test_column_def_float64() {
        let col = ColumnDef::float64("price");
        assert_eq!(col.name, "price");
        assert_eq!(col.column_type, "FLOAT64");
    }

    #[test]
    fn test_column_def_bool() {
        let col = ColumnDef::bool("active");
        assert_eq!(col.name, "active");
        assert_eq!(col.column_type, "BOOLEAN");
    }

    #[test]
    fn test_column_def_date() {
        let col = ColumnDef::date("created_at");
        assert_eq!(col.name, "created_at");
        assert_eq!(col.column_type, "DATE");
    }

    #[test]
    fn test_column_def_timestamp() {
        let col = ColumnDef::timestamp("updated_at");
        assert_eq!(col.name, "updated_at");
        assert_eq!(col.column_type, "TIMESTAMP");
    }

    #[test]
    fn test_column_def_numeric() {
        let col = ColumnDef::numeric("amount");
        assert_eq!(col.name, "amount");
        assert_eq!(col.column_type, "NUMERIC");
    }

    #[test]
    fn test_column_def_bytes() {
        let col = ColumnDef::bytes("data");
        assert_eq!(col.name, "data");
        assert_eq!(col.column_type, "BYTES");
    }

    #[test]
    fn test_column_def_from_string_tuple() {
        let col: ColumnDef = ("name".to_string(), "STRING".to_string()).into();
        assert_eq!(col.name, "name");
        assert_eq!(col.column_type, "STRING");
    }

    #[test]
    fn test_column_def_from_str_tuple() {
        let col: ColumnDef = ("id", "INT64").into();
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, "INT64");
    }

    #[test]
    fn test_column_def_serialization() {
        let col = ColumnDef::int64("id");
        let json = serde_json::to_value(&col).unwrap();
        assert_eq!(json["name"], "id");
        assert_eq!(json["type"], "INT64");
    }

    #[test]
    fn test_table_def_clone() {
        let table = TableDef {
            name: "test".to_string(),
            sql: Some("SELECT 1".to_string()),
            schema: None,
            rows: vec![serde_json::json!(1)],
            dependencies: vec!["dep".to_string()],
            is_source: false,
        };
        let cloned = table.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.sql, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_table_def_debug() {
        let table = TableDef {
            name: "t".to_string(),
            sql: None,
            schema: Some(vec![]),
            rows: vec![],
            dependencies: vec![],
            is_source: true,
        };
        let debug = format!("{:?}", table);
        assert!(debug.contains("TableDef"));
    }

    #[test]
    fn test_table_info_serialization() {
        let info = TableInfo {
            name: "my_table".to_string(),
            row_count: 100,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["name"], "my_table");
        assert_eq!(json["rowCount"], 100);
    }
}
