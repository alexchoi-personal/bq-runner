use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ColumnType {
    Int64,
    Float64,
    Bool,
    String,
    Bytes,
    Date,
    Datetime,
    Time,
    Timestamp,
    Numeric,
    Geography,
    Json,
    Struct,
    Record,
    #[serde(other)]
    Unknown,
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Int64 => "INT64",
            Self::Float64 => "FLOAT64",
            Self::Bool => "BOOL",
            Self::String => "STRING",
            Self::Bytes => "BYTES",
            Self::Date => "DATE",
            Self::Datetime => "DATETIME",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::Numeric => "NUMERIC",
            Self::Geography => "GEOGRAPHY",
            Self::Json => "JSON",
            Self::Struct => "STRUCT",
            Self::Record => "RECORD",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for ColumnType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_uppercase().as_str() {
            "INT64" | "INTEGER" | "INT" => Self::Int64,
            "FLOAT64" | "FLOAT" => Self::Float64,
            "BOOL" | "BOOLEAN" => Self::Bool,
            "STRING" => Self::String,
            "BYTES" => Self::Bytes,
            "DATE" => Self::Date,
            "DATETIME" => Self::Datetime,
            "TIME" => Self::Time,
            "TIMESTAMP" => Self::Timestamp,
            "NUMERIC" | "DECIMAL" | "BIGNUMERIC" => Self::Numeric,
            "GEOGRAPHY" => Self::Geography,
            "JSON" => Self::Json,
            "STRUCT" => Self::Struct,
            "RECORD" => Self::Record,
            _ => Self::Unknown,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: ColumnType,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
        }
    }

    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Int64)
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::String)
    }

    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Float64)
    }

    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Bool)
    }

    pub fn date(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Date)
    }

    pub fn timestamp(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Timestamp)
    }

    pub fn numeric(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Numeric)
    }

    pub fn bytes(name: impl Into<String>) -> Self {
        Self::new(name, ColumnType::Bytes)
    }
}

impl From<(String, String)> for ColumnDef {
    fn from((name, column_type): (String, String)) -> Self {
        Self {
            name,
            column_type: column_type.parse().unwrap_or(ColumnType::Unknown),
        }
    }
}

impl From<(&str, &str)> for ColumnDef {
    fn from((name, column_type): (&str, &str)) -> Self {
        Self {
            name: name.to_string(),
            column_type: column_type.parse().unwrap_or(ColumnType::Unknown),
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
    fn test_column_type_as_str() {
        assert_eq!(ColumnType::Int64.as_str(), "INT64");
        assert_eq!(ColumnType::Float64.as_str(), "FLOAT64");
        assert_eq!(ColumnType::Bool.as_str(), "BOOL");
        assert_eq!(ColumnType::String.as_str(), "STRING");
        assert_eq!(ColumnType::Bytes.as_str(), "BYTES");
        assert_eq!(ColumnType::Date.as_str(), "DATE");
        assert_eq!(ColumnType::Datetime.as_str(), "DATETIME");
        assert_eq!(ColumnType::Time.as_str(), "TIME");
        assert_eq!(ColumnType::Timestamp.as_str(), "TIMESTAMP");
        assert_eq!(ColumnType::Numeric.as_str(), "NUMERIC");
        assert_eq!(ColumnType::Geography.as_str(), "GEOGRAPHY");
        assert_eq!(ColumnType::Json.as_str(), "JSON");
        assert_eq!(ColumnType::Struct.as_str(), "STRUCT");
        assert_eq!(ColumnType::Record.as_str(), "RECORD");
        assert_eq!(ColumnType::Unknown.as_str(), "UNKNOWN");
    }

    #[test]
    fn test_column_type_display() {
        assert_eq!(format!("{}", ColumnType::Int64), "INT64");
        assert_eq!(format!("{}", ColumnType::String), "STRING");
        assert_eq!(format!("{}", ColumnType::Unknown), "UNKNOWN");
    }

    #[test]
    fn test_column_type_from_str() {
        assert_eq!("INT64".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("INTEGER".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("INT".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!(
            "FLOAT64".parse::<ColumnType>().unwrap(),
            ColumnType::Float64
        );
        assert_eq!("FLOAT".parse::<ColumnType>().unwrap(), ColumnType::Float64);
        assert_eq!("BOOL".parse::<ColumnType>().unwrap(), ColumnType::Bool);
        assert_eq!("BOOLEAN".parse::<ColumnType>().unwrap(), ColumnType::Bool);
        assert_eq!("STRING".parse::<ColumnType>().unwrap(), ColumnType::String);
        assert_eq!("BYTES".parse::<ColumnType>().unwrap(), ColumnType::Bytes);
        assert_eq!("DATE".parse::<ColumnType>().unwrap(), ColumnType::Date);
        assert_eq!(
            "DATETIME".parse::<ColumnType>().unwrap(),
            ColumnType::Datetime
        );
        assert_eq!("TIME".parse::<ColumnType>().unwrap(), ColumnType::Time);
        assert_eq!(
            "TIMESTAMP".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamp
        );
        assert_eq!(
            "NUMERIC".parse::<ColumnType>().unwrap(),
            ColumnType::Numeric
        );
        assert_eq!(
            "DECIMAL".parse::<ColumnType>().unwrap(),
            ColumnType::Numeric
        );
        assert_eq!(
            "BIGNUMERIC".parse::<ColumnType>().unwrap(),
            ColumnType::Numeric
        );
        assert_eq!(
            "GEOGRAPHY".parse::<ColumnType>().unwrap(),
            ColumnType::Geography
        );
        assert_eq!("JSON".parse::<ColumnType>().unwrap(), ColumnType::Json);
        assert_eq!("STRUCT".parse::<ColumnType>().unwrap(), ColumnType::Struct);
        assert_eq!("RECORD".parse::<ColumnType>().unwrap(), ColumnType::Record);
        assert_eq!(
            "INVALID".parse::<ColumnType>().unwrap(),
            ColumnType::Unknown
        );
    }

    #[test]
    fn test_column_type_from_str_case_insensitive() {
        assert_eq!("int64".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("Int64".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("string".parse::<ColumnType>().unwrap(), ColumnType::String);
    }

    #[test]
    fn test_column_type_serialization() {
        let json = serde_json::to_string(&ColumnType::Int64).unwrap();
        assert_eq!(json, "\"INT64\"");
        let json = serde_json::to_string(&ColumnType::String).unwrap();
        assert_eq!(json, "\"STRING\"");
    }

    #[test]
    fn test_column_type_deserialization() {
        let col_type: ColumnType = serde_json::from_str("\"INT64\"").unwrap();
        assert_eq!(col_type, ColumnType::Int64);
        let col_type: ColumnType = serde_json::from_str("\"STRING\"").unwrap();
        assert_eq!(col_type, ColumnType::String);
        let col_type: ColumnType = serde_json::from_str("\"INVALID_TYPE\"").unwrap();
        assert_eq!(col_type, ColumnType::Unknown);
    }

    #[test]
    fn test_column_type_eq_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ColumnType::Int64);
        set.insert(ColumnType::String);
        assert!(set.contains(&ColumnType::Int64));
        assert!(set.contains(&ColumnType::String));
        assert!(!set.contains(&ColumnType::Float64));
    }

    #[test]
    fn test_column_def_new() {
        let col = ColumnDef::new("id", ColumnType::Int64);
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, ColumnType::Int64);
    }

    #[test]
    fn test_column_def_int64() {
        let col = ColumnDef::int64("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, ColumnType::Int64);
    }

    #[test]
    fn test_column_def_string() {
        let col = ColumnDef::string("name");
        assert_eq!(col.name, "name");
        assert_eq!(col.column_type, ColumnType::String);
    }

    #[test]
    fn test_column_def_float64() {
        let col = ColumnDef::float64("price");
        assert_eq!(col.name, "price");
        assert_eq!(col.column_type, ColumnType::Float64);
    }

    #[test]
    fn test_column_def_bool() {
        let col = ColumnDef::bool("active");
        assert_eq!(col.name, "active");
        assert_eq!(col.column_type, ColumnType::Bool);
    }

    #[test]
    fn test_column_def_date() {
        let col = ColumnDef::date("created_at");
        assert_eq!(col.name, "created_at");
        assert_eq!(col.column_type, ColumnType::Date);
    }

    #[test]
    fn test_column_def_timestamp() {
        let col = ColumnDef::timestamp("updated_at");
        assert_eq!(col.name, "updated_at");
        assert_eq!(col.column_type, ColumnType::Timestamp);
    }

    #[test]
    fn test_column_def_numeric() {
        let col = ColumnDef::numeric("amount");
        assert_eq!(col.name, "amount");
        assert_eq!(col.column_type, ColumnType::Numeric);
    }

    #[test]
    fn test_column_def_bytes() {
        let col = ColumnDef::bytes("data");
        assert_eq!(col.name, "data");
        assert_eq!(col.column_type, ColumnType::Bytes);
    }

    #[test]
    fn test_column_def_from_string_tuple() {
        let col: ColumnDef = ("name".to_string(), "STRING".to_string()).into();
        assert_eq!(col.name, "name");
        assert_eq!(col.column_type, ColumnType::String);
    }

    #[test]
    fn test_column_def_from_str_tuple() {
        let col: ColumnDef = ("id", "INT64").into();
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, ColumnType::Int64);
    }

    #[test]
    fn test_column_def_from_tuple_unknown_type() {
        let col: ColumnDef = ("field", "INVALID_TYPE").into();
        assert_eq!(col.name, "field");
        assert_eq!(col.column_type, ColumnType::Unknown);
    }

    #[test]
    fn test_column_def_serialization() {
        let col = ColumnDef::int64("id");
        let json = serde_json::to_value(&col).unwrap();
        assert_eq!(json["name"], "id");
        assert_eq!(json["type"], "INT64");
    }

    #[test]
    fn test_column_def_deserialization() {
        let json_str = r#"{"name":"id","type":"INT64"}"#;
        let col: ColumnDef = serde_json::from_str(json_str).unwrap();
        assert_eq!(col.name, "id");
        assert_eq!(col.column_type, ColumnType::Int64);
    }

    #[test]
    fn test_column_def_deserialization_unknown_type() {
        let json_str = r#"{"name":"field","type":"CUSTOM_TYPE"}"#;
        let col: ColumnDef = serde_json::from_str(json_str).unwrap();
        assert_eq!(col.name, "field");
        assert_eq!(col.column_type, ColumnType::Unknown);
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
