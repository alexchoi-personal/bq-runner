use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;

use super::{base64_encode, escape_sql_string};

macro_rules! downcast_or_null {
    ($array:expr, $array_type:ty) => {
        match $array.as_any().downcast_ref::<$array_type>() {
            Some(arr) => arr,
            None => return "NULL".to_string(),
        }
    };
}

pub(crate) fn arrow_value_to_sql(array: &dyn Array, row: usize, bq_type: &str) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = downcast_or_null!(array, BooleanArray);
            arr.value(row).to_string()
        }
        ArrowDataType::Int8 => {
            let arr = downcast_or_null!(array, Int8Array);
            arr.value(row).to_string()
        }
        ArrowDataType::Int16 => {
            let arr = downcast_or_null!(array, Int16Array);
            arr.value(row).to_string()
        }
        ArrowDataType::Int32 => {
            let arr = downcast_or_null!(array, Int32Array);
            arr.value(row).to_string()
        }
        ArrowDataType::Int64 => {
            let arr = downcast_or_null!(array, Int64Array);
            let val = arr.value(row);
            match bq_type.to_uppercase().as_str() {
                "DATE" => format!("DATE_FROM_UNIX_DATE({})", val),
                "TIMESTAMP" => format!("TIMESTAMP_MICROS({})", val),
                _ => val.to_string(),
            }
        }
        ArrowDataType::UInt8 => {
            let arr = downcast_or_null!(array, UInt8Array);
            arr.value(row).to_string()
        }
        ArrowDataType::UInt16 => {
            let arr = downcast_or_null!(array, UInt16Array);
            arr.value(row).to_string()
        }
        ArrowDataType::UInt32 => {
            let arr = downcast_or_null!(array, UInt32Array);
            arr.value(row).to_string()
        }
        ArrowDataType::UInt64 => {
            let arr = downcast_or_null!(array, UInt64Array);
            arr.value(row).to_string()
        }
        ArrowDataType::Float32 => {
            let arr = downcast_or_null!(array, Float32Array);
            arr.value(row).to_string()
        }
        ArrowDataType::Float64 => {
            let arr = downcast_or_null!(array, Float64Array);
            arr.value(row).to_string()
        }
        ArrowDataType::Utf8 => {
            let arr = downcast_or_null!(array, StringArray);
            format!("'{}'", escape_sql_string(arr.value(row)))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = downcast_or_null!(array, LargeStringArray);
            format!("'{}'", escape_sql_string(arr.value(row)))
        }
        ArrowDataType::Date32 => {
            let arr = downcast_or_null!(array, Date32Array);
            let days = arr.value(row);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Date64 => {
            let arr = downcast_or_null!(array, Date64Array);
            let ms = arr.value(row);
            let days = ms / (24 * 60 * 60 * 1000);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Timestamp(unit, _) => {
            let micros = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = downcast_or_null!(array, TimestampSecondArray);
                    arr.value(row) * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = downcast_or_null!(array, TimestampMillisecondArray);
                    arr.value(row) * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = downcast_or_null!(array, TimestampMicrosecondArray);
                    arr.value(row)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = downcast_or_null!(array, TimestampNanosecondArray);
                    arr.value(row) / 1_000
                }
            };
            format!("TIMESTAMP_MICROS({})", micros)
        }
        ArrowDataType::Binary => {
            let arr = downcast_or_null!(array, BinaryArray);
            format!("FROM_BASE64('{}')", base64_encode(arr.value(row)))
        }
        ArrowDataType::LargeBinary => {
            let arr = downcast_or_null!(array, LargeBinaryArray);
            format!("FROM_BASE64('{}')", base64_encode(arr.value(row)))
        }
        ArrowDataType::FixedSizeBinary(_) => {
            let arr = downcast_or_null!(array, FixedSizeBinaryArray);
            format!("FROM_BASE64('{}')", base64_encode(arr.value(row)))
        }
        _ => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_value_to_sql_null() {
        let arr = Int64Array::from(vec![None]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "INT64"), "NULL");
    }

    #[test]
    fn test_arrow_value_to_sql_boolean() {
        let arr = BooleanArray::from(vec![Some(true), Some(false)]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "BOOLEAN"), "true");
        assert_eq!(arrow_value_to_sql(&arr, 1, "BOOLEAN"), "false");
    }

    #[test]
    fn test_arrow_value_to_sql_int64() {
        let arr = Int64Array::from(vec![Some(42)]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "INT64"), "42");
    }

    #[test]
    fn test_arrow_value_to_sql_int64_as_date() {
        let arr = Int64Array::from(vec![Some(19000)]);
        assert_eq!(
            arrow_value_to_sql(&arr, 0, "DATE"),
            "DATE_FROM_UNIX_DATE(19000)"
        );
    }

    #[test]
    fn test_arrow_value_to_sql_int64_as_timestamp() {
        let arr = Int64Array::from(vec![Some(1000000)]);
        assert_eq!(
            arrow_value_to_sql(&arr, 0, "TIMESTAMP"),
            "TIMESTAMP_MICROS(1000000)"
        );
    }

    #[test]
    fn test_arrow_value_to_sql_float64() {
        let arr = Float64Array::from(vec![Some(1.234)]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "FLOAT64"), "1.234");
    }

    #[test]
    fn test_arrow_value_to_sql_string() {
        let arr = StringArray::from(vec![Some("hello")]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "STRING"), "'hello'");
    }

    #[test]
    fn test_arrow_value_to_sql_string_with_quote() {
        let arr = StringArray::from(vec![Some("it's")]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "STRING"), "'it''s'");
    }

    #[test]
    fn test_arrow_value_to_sql_date32() {
        let arr = Date32Array::from(vec![Some(19000)]);
        assert_eq!(
            arrow_value_to_sql(&arr, 0, "DATE"),
            "DATE_FROM_UNIX_DATE(19000)"
        );
    }
}
