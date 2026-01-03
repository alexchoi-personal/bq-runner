use serde_json::{json, Value as JsonValue};
use yachtsql::{DataType, Value as YachtValue};

pub fn datatype_to_bq_type(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOLEAN".to_string(),
        DataType::Int64 => "INT64".to_string(),
        DataType::Float64 => "FLOAT64".to_string(),
        DataType::Numeric(_) | DataType::BigNumeric => "NUMERIC".to_string(),
        DataType::String => "STRING".to_string(),
        DataType::Bytes => "BYTES".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::DateTime => "DATETIME".to_string(),
        DataType::Time => "TIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Geography => "GEOGRAPHY".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Array(inner) => format!("ARRAY<{}>", datatype_to_bq_type(inner)),
        DataType::Interval => "INTERVAL".to_string(),
        DataType::Range(_) => "STRING".to_string(),
        DataType::Unknown => "STRING".to_string(),
    }
}

pub fn yacht_value_into_json(value: YachtValue) -> JsonValue {
    match value {
        YachtValue::Null => JsonValue::Null,
        YachtValue::Bool(b) => JsonValue::Bool(b),
        YachtValue::Int64(i) => json!(i),
        YachtValue::Float64(f) => json!(f.into_inner()),
        YachtValue::Numeric(d) => JsonValue::String(d.to_string()),
        YachtValue::String(s) => JsonValue::String(s),
        YachtValue::Bytes(b) => JsonValue::String(base64_encode(&b)),
        YachtValue::Date(d) => JsonValue::String(d.to_string()),
        YachtValue::Time(t) => JsonValue::String(t.to_string()),
        YachtValue::DateTime(dt) => JsonValue::String(dt.to_string()),
        YachtValue::Timestamp(ts) => JsonValue::String(ts.to_string()),
        YachtValue::Json(j) => j,
        YachtValue::Array(arr) => {
            let items: Vec<JsonValue> = arr.into_iter().map(yacht_value_into_json).collect();
            JsonValue::Array(items)
        }
        YachtValue::Struct(fields) => {
            let obj: serde_json::Map<String, JsonValue> = fields
                .into_iter()
                .map(|(k, v)| (k, yacht_value_into_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
        YachtValue::Geography(g) => JsonValue::String(g),
        YachtValue::Interval(i) => JsonValue::String(format!("{:?}", i)),
        YachtValue::Range(r) => JsonValue::String(format!("{:?}", r)),
        YachtValue::BigNumeric(n) => JsonValue::String(n.to_string()),
        YachtValue::Default => JsonValue::Null,
    }
}

pub fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let encoded_len = data.len().div_ceil(3) * 4;
    let mut result = String::with_capacity(encoded_len);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_to_bq_type_primitives() {
        assert_eq!(datatype_to_bq_type(&DataType::Bool), "BOOLEAN");
        assert_eq!(datatype_to_bq_type(&DataType::Int64), "INT64");
        assert_eq!(datatype_to_bq_type(&DataType::Float64), "FLOAT64");
        assert_eq!(datatype_to_bq_type(&DataType::String), "STRING");
        assert_eq!(datatype_to_bq_type(&DataType::Bytes), "BYTES");
        assert_eq!(datatype_to_bq_type(&DataType::Date), "DATE");
        assert_eq!(datatype_to_bq_type(&DataType::DateTime), "DATETIME");
        assert_eq!(datatype_to_bq_type(&DataType::Time), "TIME");
        assert_eq!(datatype_to_bq_type(&DataType::Timestamp), "TIMESTAMP");
    }

    #[test]
    fn test_datatype_to_bq_type_array() {
        let arr_type = DataType::Array(Box::new(DataType::Int64));
        assert_eq!(datatype_to_bq_type(&arr_type), "ARRAY<INT64>");
    }

    #[test]
    fn test_yacht_value_into_json_null() {
        assert_eq!(yacht_value_into_json(YachtValue::Null), JsonValue::Null);
    }

    #[test]
    fn test_yacht_value_into_json_bool() {
        assert_eq!(
            yacht_value_into_json(YachtValue::Bool(true)),
            JsonValue::Bool(true)
        );
    }

    #[test]
    fn test_yacht_value_into_json_int64() {
        assert_eq!(yacht_value_into_json(YachtValue::Int64(42)), json!(42));
    }

    #[test]
    fn test_yacht_value_into_json_string() {
        assert_eq!(
            yacht_value_into_json(YachtValue::String("hello".to_string())),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_base64_encode_empty() {
        assert_eq!(base64_encode(&[]), "");
    }

    #[test]
    fn test_base64_encode_one_byte() {
        assert_eq!(base64_encode(&[0x4d]), "TQ==");
    }

    #[test]
    fn test_base64_encode_two_bytes() {
        assert_eq!(base64_encode(&[0x4d, 0x61]), "TWE=");
    }

    #[test]
    fn test_base64_encode_three_bytes() {
        assert_eq!(base64_encode(&[0x4d, 0x61, 0x6e]), "TWFu");
    }

    #[test]
    fn test_base64_encode_hello() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
    }
}
