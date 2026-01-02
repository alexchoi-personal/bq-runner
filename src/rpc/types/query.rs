use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use crate::domain::{ColumnDef, TableInfo};

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Serialize)]
pub struct CreateTableResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct InsertParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct InsertResult {
    #[serde(rename = "insertedRows")]
    pub inserted_rows: u64,
}

#[derive(Debug, Deserialize)]
pub struct LoadParquetParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub path: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Serialize)]
pub struct LoadParquetResult {
    pub success: bool,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResult {
    pub tables: Vec<TableInfo>,
}

#[derive(Debug, Deserialize)]
pub struct DescribeTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
}

#[derive(Debug, Serialize)]
pub struct DescribeTableResult {
    pub name: String,
    pub schema: Vec<ColumnDef>,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","sql":"SELECT 1"}"#;
        let params: QueryParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.sql, "SELECT 1");
    }

    #[test]
    fn test_create_table_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123","tableName":"test_table","schema":[{"name":"id","type":"INT64"}]}"#;
        let params: CreateTableParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.table_name, "test_table");
        assert_eq!(params.schema.len(), 1);
        assert_eq!(params.schema[0].name, "id");
        assert_eq!(params.schema[0].column_type, "INT64");
    }

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
    fn test_create_table_result_serialization() {
        let result = CreateTableResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_insert_params_deserialization() {
        let json_str =
            r#"{"sessionId":"abc-123","tableName":"test_table","rows":[[1,"a"],[2,"b"]]}"#;
        let params: InsertParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
        assert_eq!(params.table_name, "test_table");
        assert_eq!(params.rows.len(), 2);
    }

    #[test]
    fn test_insert_result_serialization() {
        let result = InsertResult { inserted_rows: 10 };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["insertedRows"], 10);
    }

    #[test]
    fn test_load_parquet_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","tableName":"t1","path":"/tmp/t.parquet","schema":[{"name":"id","type":"INT64"}]}"#;
        let params: LoadParquetParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.table_name, "t1");
        assert_eq!(params.path, "/tmp/t.parquet");
        assert_eq!(params.schema.len(), 1);
    }

    #[test]
    fn test_load_parquet_result_serialization() {
        let result = LoadParquetResult {
            success: true,
            row_count: 100,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["rowCount"], 100);
    }

    #[test]
    fn test_list_tables_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: ListTablesParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
    }

    #[test]
    fn test_list_tables_result_serialization() {
        let result = ListTablesResult {
            tables: vec![TableInfo {
                name: "t1".to_string(),
                row_count: 50,
            }],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["tables"][0]["name"], "t1");
        assert_eq!(json["tables"][0]["rowCount"], 50);
    }

    #[test]
    fn test_describe_table_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","tableName":"my_table"}"#;
        let params: DescribeTableParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.table_name, "my_table");
    }

    #[test]
    fn test_describe_table_result_serialization() {
        let result = DescribeTableResult {
            name: "my_table".to_string(),
            schema: vec![ColumnDef::int64("id")],
            row_count: 100,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["name"], "my_table");
        assert_eq!(json["schema"][0]["name"], "id");
        assert_eq!(json["rowCount"], 100);
    }
}
