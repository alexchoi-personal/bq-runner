use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;

pub fn arrow_value_to_sql(array: &dyn Array, row: usize, bq_type: &str) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = arr.value(row);
            match bq_type.to_uppercase().as_str() {
                "DATE" => format!("DATE_FROM_UNIX_DATE({})", val),
                "TIMESTAMP" => format!("TIMESTAMP_MICROS({})", val),
                _ => val.to_string(),
            }
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        ArrowDataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(row);
            let days = ms / (24 * 60 * 60 * 1000);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Timestamp(unit, _) => {
            let micros = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.value(row)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    arr.value(row) / 1_000
                }
            };
            format!("TIMESTAMP_MICROS({})", micros)
        }
        _ => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
        let arr = Float64Array::from(vec![Some(3.14)]);
        assert_eq!(arrow_value_to_sql(&arr, 0, "FLOAT64"), "3.14");
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
