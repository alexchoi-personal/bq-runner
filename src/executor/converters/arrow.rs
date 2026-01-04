use std::fmt::Write;

use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;

use super::{base64_encode_into, escape_sql_string_into};

macro_rules! downcast_or_null {
    ($array:expr, $array_type:ty, $buf:expr) => {
        match $array.as_any().downcast_ref::<$array_type>() {
            Some(arr) => arr,
            None => {
                $buf.push_str("NULL");
                return;
            }
        }
    };
}

#[cfg(test)]
pub(crate) fn arrow_value_to_sql(array: &dyn Array, row: usize, bq_type: &str) -> String {
    let mut buf = String::new();
    arrow_value_to_sql_into(array, row, bq_type, &mut buf);
    buf
}

pub(crate) fn arrow_value_to_sql_into(
    array: &dyn Array,
    row: usize,
    bq_type: &str,
    buf: &mut String,
) {
    if array.is_null(row) {
        buf.push_str("NULL");
        return;
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = downcast_or_null!(array, BooleanArray, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Int8 => {
            let arr = downcast_or_null!(array, Int8Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Int16 => {
            let arr = downcast_or_null!(array, Int16Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Int32 => {
            let arr = downcast_or_null!(array, Int32Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Int64 => {
            let arr = downcast_or_null!(array, Int64Array, buf);
            let val = arr.value(row);
            if bq_type.eq_ignore_ascii_case("DATE") {
                let _ = write!(buf, "DATE_FROM_UNIX_DATE({})", val);
            } else if bq_type.eq_ignore_ascii_case("TIMESTAMP") {
                let _ = write!(buf, "TIMESTAMP_MICROS({})", val);
            } else {
                let _ = write!(buf, "{}", val);
            }
        }
        ArrowDataType::UInt8 => {
            let arr = downcast_or_null!(array, UInt8Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::UInt16 => {
            let arr = downcast_or_null!(array, UInt16Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::UInt32 => {
            let arr = downcast_or_null!(array, UInt32Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::UInt64 => {
            let arr = downcast_or_null!(array, UInt64Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Float32 => {
            let arr = downcast_or_null!(array, Float32Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Float64 => {
            let arr = downcast_or_null!(array, Float64Array, buf);
            let _ = write!(buf, "{}", arr.value(row));
        }
        ArrowDataType::Utf8 => {
            let arr = downcast_or_null!(array, StringArray, buf);
            buf.push('\'');
            escape_sql_string_into(arr.value(row), buf);
            buf.push('\'');
        }
        ArrowDataType::LargeUtf8 => {
            let arr = downcast_or_null!(array, LargeStringArray, buf);
            buf.push('\'');
            escape_sql_string_into(arr.value(row), buf);
            buf.push('\'');
        }
        ArrowDataType::Date32 => {
            let arr = downcast_or_null!(array, Date32Array, buf);
            let _ = write!(buf, "DATE_FROM_UNIX_DATE({})", arr.value(row));
        }
        ArrowDataType::Date64 => {
            let arr = downcast_or_null!(array, Date64Array, buf);
            let ms = arr.value(row);
            let days = ms / (24 * 60 * 60 * 1000);
            let _ = write!(buf, "DATE_FROM_UNIX_DATE({})", days);
        }
        ArrowDataType::Timestamp(unit, _) => {
            let micros = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = downcast_or_null!(array, TimestampSecondArray, buf);
                    arr.value(row) * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = downcast_or_null!(array, TimestampMillisecondArray, buf);
                    arr.value(row) * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = downcast_or_null!(array, TimestampMicrosecondArray, buf);
                    arr.value(row)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = downcast_or_null!(array, TimestampNanosecondArray, buf);
                    arr.value(row) / 1_000
                }
            };
            let _ = write!(buf, "TIMESTAMP_MICROS({})", micros);
        }
        ArrowDataType::Binary => {
            let arr = downcast_or_null!(array, BinaryArray, buf);
            buf.push_str("FROM_BASE64('");
            base64_encode_into(arr.value(row), buf);
            buf.push_str("')");
        }
        ArrowDataType::LargeBinary => {
            let arr = downcast_or_null!(array, LargeBinaryArray, buf);
            buf.push_str("FROM_BASE64('");
            base64_encode_into(arr.value(row), buf);
            buf.push_str("')");
        }
        ArrowDataType::FixedSizeBinary(_) => {
            let arr = downcast_or_null!(array, FixedSizeBinaryArray, buf);
            buf.push_str("FROM_BASE64('");
            base64_encode_into(arr.value(row), buf);
            buf.push_str("')");
        }
        _ => buf.push_str("NULL"),
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
