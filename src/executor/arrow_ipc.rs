use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    ListBuilder, NullBuilder, StringBuilder, StructArray, TimestampMicrosecondBuilder,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use memmap2::MmapMut;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::executor::yachtsql::QueryResult;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(target_os = "linux")]
const SHM_DIR: &str = "/dev/shm";
#[cfg(target_os = "macos")]
const SHM_DIR: &str = "/tmp";
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const SHM_DIR: &str = "/tmp";

const SHM_PREFIX: &str = "bq_runner_";

fn parse_array_element_type(type_str: &str) -> Option<&str> {
    let upper = type_str.trim();
    if upper.to_uppercase().starts_with("ARRAY<") && upper.ends_with('>') {
        let inner_start = 6;
        let inner_end = upper.len() - 1;
        if inner_start < inner_end {
            return Some(&type_str[inner_start..inner_end]);
        }
    }
    None
}

fn infer_type_from_json(value: &JsonValue) -> ArrowDataType {
    match value {
        JsonValue::Null => ArrowDataType::Utf8,
        JsonValue::Bool(_) => ArrowDataType::Boolean,
        JsonValue::Number(n) => {
            if n.is_i64() {
                ArrowDataType::Int64
            } else {
                ArrowDataType::Float64
            }
        }
        JsonValue::String(_) => ArrowDataType::Utf8,
        JsonValue::Array(arr) => {
            let elem_type = arr
                .iter()
                .find(|v| !v.is_null())
                .map(infer_type_from_json)
                .unwrap_or(ArrowDataType::Utf8);
            ArrowDataType::List(Arc::new(Field::new("item", elem_type, true)))
        }
        JsonValue::Object(_) => ArrowDataType::Utf8,
    }
}

fn infer_struct_fields_from_rows(rows: &[Vec<JsonValue>], col_idx: usize) -> Vec<Field> {
    let mut field_types: BTreeMap<String, ArrowDataType> = BTreeMap::new();

    for row in rows {
        if let Some(value) = row.get(col_idx) {
            if let JsonValue::Object(obj) = value {
                for (key, val) in obj {
                    if !val.is_null() && !field_types.contains_key(key) {
                        field_types.insert(key.clone(), infer_type_from_json(val));
                    }
                }
            }
        }
    }

    if field_types.is_empty() {
        return vec![Field::new("_value", ArrowDataType::Utf8, true)];
    }

    field_types
        .into_iter()
        .map(|(name, dt)| Field::new(name, dt, true))
        .collect()
}

fn infer_struct_fields_from_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Vec<Field> {
    let mut field_types: BTreeMap<String, ArrowDataType> = BTreeMap::new();

    for row in rows {
        if let Some(JsonValue::Array(arr)) = row.get(col_idx) {
            for elem in arr {
                if let JsonValue::Object(obj) = elem {
                    for (key, val) in obj {
                        if !val.is_null() && !field_types.contains_key(key) {
                            field_types.insert(key.clone(), infer_type_from_json(val));
                        }
                    }
                }
            }
        }
    }

    if field_types.is_empty() {
        return vec![Field::new("_value", ArrowDataType::Utf8, true)];
    }

    field_types
        .into_iter()
        .map(|(name, dt)| Field::new(name, dt, true))
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct SharedMemoryHandle {
    pub path: String,
    pub size: usize,
    pub row_count: usize,
    pub schema_json: String,
}

pub(crate) struct ArrowSharedMemoryWriter;

impl ArrowSharedMemoryWriter {
    pub fn write_query_result(
        result: &QueryResult,
        session_id: &Uuid,
    ) -> Result<SharedMemoryHandle> {
        let batch = query_result_to_record_batch(result)?;
        Self::write_to_shared_memory(&batch, session_id)
    }

    pub fn write_to_shared_memory(
        batch: &RecordBatch,
        session_id: &Uuid,
    ) -> Result<SharedMemoryHandle> {
        let request_id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let filename = format!("{}{}_{}", SHM_PREFIX, session_id, request_id);
        let path = PathBuf::from(SHM_DIR).join(&filename);

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())
                .map_err(|e| Error::Internal(format!("Failed to create Arrow stream writer: {}", e)))?;
            writer
                .write(batch)
                .map_err(|e| Error::Internal(format!("Failed to write Arrow batch: {}", e)))?;
            writer
                .finish()
                .map_err(|e| Error::Internal(format!("Failed to finish Arrow stream: {}", e)))?;
        }

        let total_size = 8 + buffer.len();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::Internal(format!("Failed to create shared memory file: {}", e)))?;

        file.set_len(total_size as u64)
            .map_err(|e| Error::Internal(format!("Failed to set file size: {}", e)))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = file
                .metadata()
                .map_err(|e| Error::Internal(format!("Failed to get file metadata: {}", e)))?
                .permissions();
            perms.set_mode(0o600);
            file.set_permissions(perms)
                .map_err(|e| Error::Internal(format!("Failed to set file permissions: {}", e)))?;
        }

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .map_err(|e| Error::Internal(format!("Failed to mmap file: {}", e)))?
        };

        mmap[..8].copy_from_slice(&(buffer.len() as u64).to_le_bytes());
        mmap[8..].copy_from_slice(&buffer);

        mmap.flush()
            .map_err(|e| Error::Internal(format!("Failed to flush mmap: {}", e)))?;

        let schema_json = schema_to_json(batch.schema().as_ref());

        Ok(SharedMemoryHandle {
            path: path.to_string_lossy().to_string(),
            size: buffer.len(),
            row_count: batch.num_rows(),
            schema_json,
        })
    }

    pub fn release(path: &str) -> Result<()> {
        Self::validate_shm_path(path)?;
        if std::path::Path::new(path).exists() {
            std::fs::remove_file(path)
                .map_err(|e| Error::Internal(format!("Failed to remove shared memory file: {}", e)))?;
        }
        Ok(())
    }

    pub fn validate_shm_path(path: &str) -> Result<()> {
        let path_obj = std::path::Path::new(path);
        let parent = path_obj
            .parent()
            .ok_or_else(|| Error::InvalidRequest("Invalid shared memory path".into()))?;

        let parent_str = parent.to_string_lossy();
        if parent_str != SHM_DIR {
            return Err(Error::InvalidRequest(format!(
                "Shared memory path must be in {}, got: {}",
                SHM_DIR, parent_str
            )));
        }

        let filename = path_obj
            .file_name()
            .ok_or_else(|| Error::InvalidRequest("Invalid shared memory filename".into()))?
            .to_string_lossy();

        if !filename.starts_with(SHM_PREFIX) {
            return Err(Error::InvalidRequest(format!(
                "Shared memory filename must start with {}, got: {}",
                SHM_PREFIX, filename
            )));
        }

        Ok(())
    }

    pub fn cleanup_session_files(session_id: &Uuid) -> Result<usize> {
        let pattern = format!("{}{}_*", 
            PathBuf::from(SHM_DIR).join(SHM_PREFIX).to_string_lossy(),
            session_id
        );

        let mut count = 0;
        if let Ok(entries) = glob::glob(&pattern) {
            for entry in entries.flatten() {
                if std::fs::remove_file(&entry).is_ok() {
                    count += 1;
                    tracing::debug!(path = %entry.display(), "Cleaned up shared memory file");
                }
            }
        }
        Ok(count)
    }
}

pub(crate) fn query_result_to_record_batch(result: &QueryResult) -> Result<RecordBatch> {
    if result.columns.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(RecordBatch::new_empty(schema));
    }

    let fields: Vec<Field> = result
        .columns
        .iter()
        .enumerate()
        .map(|(col_idx, col)| {
            let arrow_type =
                bq_type_to_arrow_with_inference(&col.data_type, &result.rows, col_idx);
            Field::new(&col.name, arrow_type, true)
        })
        .collect();

    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<ArrayRef> = result
        .columns
        .iter()
        .enumerate()
        .map(|(col_idx, col)| build_array(&col.data_type, &result.rows, col_idx))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Internal(format!("Failed to create RecordBatch: {}", e)))
}

fn bq_type_to_arrow_simple(bq_type: &str) -> ArrowDataType {
    match bq_type.to_uppercase().as_str() {
        "INT64" | "INTEGER" | "INT" => ArrowDataType::Int64,
        "FLOAT64" | "FLOAT" => ArrowDataType::Float64,
        "STRING" => ArrowDataType::Utf8,
        "BOOL" | "BOOLEAN" => ArrowDataType::Boolean,
        "DATE" => ArrowDataType::Date32,
        "TIMESTAMP" => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        "BYTES" => ArrowDataType::Binary,
        "NUMERIC" | "BIGNUMERIC" | "DECIMAL" => ArrowDataType::Utf8,
        "DATETIME" | "TIME" | "GEOGRAPHY" | "JSON" => ArrowDataType::Utf8,
        _ => ArrowDataType::Utf8,
    }
}

fn bq_type_to_arrow_with_inference(
    bq_type: &str,
    rows: &[Vec<JsonValue>],
    col_idx: usize,
) -> ArrowDataType {
    let upper = bq_type.to_uppercase();

    if let Some(elem_type) = parse_array_element_type(bq_type) {
        let elem_upper = elem_type.trim().to_uppercase();
        if elem_upper == "STRUCT" || elem_upper == "RECORD" {
            let fields = infer_struct_fields_from_array(rows, col_idx);
            let struct_type = ArrowDataType::Struct(Fields::from(fields));
            return ArrowDataType::List(Arc::new(Field::new("item", struct_type, true)));
        } else if elem_upper.starts_with("ARRAY<") {
            let inner_arrow = bq_type_to_arrow_with_inference(elem_type, rows, col_idx);
            return ArrowDataType::List(Arc::new(Field::new("item", inner_arrow, true)));
        } else {
            let elem_arrow = bq_type_to_arrow_simple(elem_type);
            return ArrowDataType::List(Arc::new(Field::new("item", elem_arrow, true)));
        }
    }

    if upper == "STRUCT" || upper == "RECORD" {
        let fields = infer_struct_fields_from_rows(rows, col_idx);
        return ArrowDataType::Struct(Fields::from(fields));
    }

    bq_type_to_arrow_simple(bq_type)
}

fn build_array(bq_type: &str, rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    if let Some(elem_type) = parse_array_element_type(bq_type) {
        return build_list_array(elem_type, rows, col_idx);
    }

    let upper_type = bq_type.to_uppercase();

    if upper_type == "STRUCT" || upper_type == "RECORD" {
        return build_struct_array(rows, col_idx);
    }

    match upper_type.as_str() {
        "INT64" | "INTEGER" | "INT" => build_int64_array(rows, col_idx),
        "FLOAT64" | "FLOAT" => build_float64_array(rows, col_idx),
        "STRING" | "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "DATETIME" | "TIME" | "GEOGRAPHY"
        | "JSON" => build_string_array(rows, col_idx),
        "BOOL" | "BOOLEAN" => build_bool_array(rows, col_idx),
        "DATE" => build_date_array(rows, col_idx),
        "TIMESTAMP" => build_timestamp_array(rows, col_idx),
        "BYTES" => build_binary_array(rows, col_idx),
        _ => build_string_array(rows, col_idx),
    }
}

fn build_int64_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(rows.len());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(i) = value.as_i64() {
            builder.append_value(i);
        } else if let Some(f) = value.as_f64() {
            builder.append_value(f as i64);
        } else if let Some(s) = value.as_str() {
            if let Ok(i) = s.parse::<i64>() {
                builder.append_value(i);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float64_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(rows.len());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(f) = value.as_f64() {
            builder.append_value(f);
        } else if let Some(i) = value.as_i64() {
            builder.append_value(i as f64);
        } else if let Some(s) = value.as_str() {
            if let Ok(f) = s.parse::<f64>() {
                builder.append_value(f);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_string_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            builder.append_value(s);
        } else {
            builder.append_value(value.to_string());
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_bool_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::with_capacity(rows.len());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(b) = value.as_bool() {
            builder.append_value(b);
        } else if let Some(s) = value.as_str() {
            match s.to_lowercase().as_str() {
                "true" | "1" => builder.append_value(true),
                "false" | "0" => builder.append_value(false),
                _ => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_date_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = Date32Builder::with_capacity(rows.len());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            if let Some(days) = parse_date_to_days(s) {
                builder.append_value(days);
            } else {
                builder.append_null();
            }
        } else if let Some(i) = value.as_i64() {
            builder.append_value(i as i32);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_timestamp_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            if let Some(micros) = parse_timestamp_to_micros(s) {
                builder.append_value(micros);
            } else {
                builder.append_null();
            }
        } else if let Some(i) = value.as_i64() {
            builder.append_value(i);
        } else if let Some(f) = value.as_f64() {
            builder.append_value(f as i64);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_binary_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(s) = value.as_str() {
            if let Ok(bytes) = base64_decode(s) {
                builder.append_value(&bytes);
            } else {
                builder.append_value(s.as_bytes());
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_array(
    element_type_str: &str,
    rows: &[Vec<JsonValue>],
    col_idx: usize,
) -> Result<ArrayRef> {
    let elem_upper = element_type_str.trim().to_uppercase();

    if elem_upper == "STRUCT" || elem_upper == "RECORD" {
        return build_list_of_struct_array(rows, col_idx);
    }

    if let Some(inner_elem) = parse_array_element_type(element_type_str) {
        return build_list_of_list_array(inner_elem, rows, col_idx);
    }

    match elem_upper.as_str() {
        "INT64" | "INTEGER" | "INT" => build_list_int64_array(rows, col_idx),
        "FLOAT64" | "FLOAT" => build_list_float64_array(rows, col_idx),
        "BOOL" | "BOOLEAN" => build_list_bool_array(rows, col_idx),
        "DATE" => build_list_date_array(rows, col_idx),
        "TIMESTAMP" => build_list_timestamp_array(rows, col_idx),
        "BYTES" => build_list_binary_array(rows, col_idx),
        _ => build_list_string_array(rows, col_idx),
    }
}

fn build_list_int64_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Int64Builder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(i) = elem.as_i64() {
                    values.append_value(i);
                } else if let Some(f) = elem.as_f64() {
                    values.append_value(f as i64);
                } else if let Some(s) = elem.as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        values.append_value(i);
                    } else {
                        values.append_null();
                    }
                } else {
                    values.append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_float64_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Float64Builder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(f) = elem.as_f64() {
                    values.append_value(f);
                } else if let Some(i) = elem.as_i64() {
                    values.append_value(i as f64);
                } else if let Some(s) = elem.as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        values.append_value(f);
                    } else {
                        values.append_null();
                    }
                } else {
                    values.append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_string_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(s) = elem.as_str() {
                    values.append_value(s);
                } else {
                    values.append_value(elem.to_string());
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_bool_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(BooleanBuilder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(b) = elem.as_bool() {
                    values.append_value(b);
                } else if let Some(s) = elem.as_str() {
                    match s.to_lowercase().as_str() {
                        "true" | "1" => values.append_value(true),
                        "false" | "0" => values.append_value(false),
                        _ => values.append_null(),
                    }
                } else {
                    values.append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_date_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(Date32Builder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(s) = elem.as_str() {
                    if let Some(days) = parse_date_to_days(s) {
                        values.append_value(days);
                    } else {
                        values.append_null();
                    }
                } else if let Some(i) = elem.as_i64() {
                    values.append_value(i as i32);
                } else {
                    values.append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_timestamp_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(TimestampMicrosecondBuilder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(s) = elem.as_str() {
                    if let Some(micros) = parse_timestamp_to_micros(s) {
                        values.append_value(micros);
                    } else {
                        values.append_null();
                    }
                } else if let Some(i) = elem.as_i64() {
                    values.append_value(i);
                } else if let Some(f) = elem.as_f64() {
                    values.append_value(f as i64);
                } else {
                    values.append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_binary_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let mut builder = ListBuilder::new(BinaryBuilder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(arr) = value.as_array() {
            let values = builder.values();
            for elem in arr {
                if elem.is_null() {
                    values.append_null();
                } else if let Some(s) = elem.as_str() {
                    if let Ok(bytes) = base64_decode(s) {
                        values.append_value(&bytes);
                    } else {
                        values.append_value(s.as_bytes());
                    }
                } else {
                    values.append_null();
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_list_of_struct_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let fields = infer_struct_fields_from_array(rows, col_idx);
    if fields.is_empty() {
        let mut builder = ListBuilder::new(NullBuilder::new());
        for row in rows {
            let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
            if value.is_null() {
                builder.append_null();
            } else if let Some(arr) = value.as_array() {
                for _ in arr {
                    builder.values().append_null();
                }
                builder.append(true);
            } else {
                builder.append_null();
            }
        }
        return Ok(Arc::new(builder.finish()));
    }

    let mut all_structs: Vec<Option<Vec<Option<JsonValue>>>> = Vec::with_capacity(rows.len());
    let mut offsets: Vec<i32> = vec![0];
    let mut current_offset: i32 = 0;
    let mut null_bitmap: Vec<bool> = Vec::with_capacity(rows.len());

    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            null_bitmap.push(false);
            offsets.push(current_offset);
        } else if let Some(arr) = value.as_array() {
            null_bitmap.push(true);
            for elem in arr {
                if let JsonValue::Object(obj) = elem {
                    let struct_values: Vec<Option<JsonValue>> = fields
                        .iter()
                        .map(|f| obj.get(f.name()).cloned())
                        .collect();
                    all_structs.push(Some(struct_values));
                } else {
                    all_structs.push(None);
                }
                current_offset += 1;
            }
            offsets.push(current_offset);
        } else {
            null_bitmap.push(false);
            offsets.push(current_offset);
        }
    }

    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .enumerate()
        .map(|(field_idx, field)| {
            build_struct_field_array(&all_structs, field_idx, field.data_type())
        })
        .collect::<Result<Vec<_>>>()?;

    let struct_array = StructArray::try_new(Fields::from(fields.clone()), child_arrays, None)
        .map_err(|e| Error::Internal(format!("Failed to create struct array: {}", e)))?;

    let list_data = arrow::array::ArrayDataBuilder::new(ArrowDataType::List(Arc::new(Field::new(
        "item",
        ArrowDataType::Struct(Fields::from(fields)),
        true,
    ))))
    .len(rows.len())
    .add_buffer(arrow::buffer::Buffer::from_slice_ref(&offsets))
    .add_child_data(struct_array.to_data())
    .null_bit_buffer(Some(create_null_bit_buffer(&null_bitmap)))
    .build()
    .map_err(|e| Error::Internal(format!("Failed to build list array data: {}", e)))?;

    Ok(Arc::new(arrow::array::ListArray::from(list_data)))
}

fn build_list_of_list_array(
    inner_elem_type: &str,
    rows: &[Vec<JsonValue>],
    col_idx: usize,
) -> Result<ArrayRef> {
    let inner_upper = inner_elem_type.trim().to_uppercase();
    let inner_arrow_type = if inner_upper == "STRUCT" || inner_upper == "RECORD" {
        let fields = infer_nested_struct_fields(rows, col_idx);
        ArrowDataType::List(Arc::new(Field::new(
            "item",
            ArrowDataType::Struct(Fields::from(fields)),
            true,
        )))
    } else {
        let elem_type = bq_type_to_arrow_simple(inner_elem_type);
        ArrowDataType::List(Arc::new(Field::new("item", elem_type, true)))
    };

    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(outer_arr) = value.as_array() {
            let values = builder.values();
            for inner_val in outer_arr {
                if inner_val.is_null() {
                    values.append_null();
                } else {
                    values.append_value(inner_val.to_string());
                }
            }
            builder.append(true);
        } else {
            builder.append_null();
        }
    }

    let string_list = builder.finish();
    let list_type =
        ArrowDataType::List(Arc::new(Field::new("item", inner_arrow_type.clone(), true)));

    let list_data = arrow::array::ArrayDataBuilder::new(list_type)
        .len(string_list.len())
        .add_buffer(string_list.offsets().inner().inner().clone())
        .add_child_data(string_list.values().to_data())
        .null_bit_buffer(string_list.nulls().map(|n| n.buffer().clone()))
        .build()
        .map_err(|e| Error::Internal(format!("Failed to build nested list array: {}", e)))?;

    Ok(Arc::new(arrow::array::ListArray::from(list_data)))
}

fn infer_nested_struct_fields(rows: &[Vec<JsonValue>], col_idx: usize) -> Vec<Field> {
    let mut field_types: BTreeMap<String, ArrowDataType> = BTreeMap::new();

    for row in rows {
        if let Some(JsonValue::Array(outer_arr)) = row.get(col_idx) {
            for inner_val in outer_arr {
                if let JsonValue::Array(inner_arr) = inner_val {
                    for elem in inner_arr {
                        if let JsonValue::Object(obj) = elem {
                            for (key, val) in obj {
                                if !val.is_null() && !field_types.contains_key(key) {
                                    field_types.insert(key.clone(), infer_type_from_json(val));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if field_types.is_empty() {
        return vec![Field::new("_value", ArrowDataType::Utf8, true)];
    }

    field_types
        .into_iter()
        .map(|(name, dt)| Field::new(name, dt, true))
        .collect()
}

fn build_struct_field_array(
    structs: &[Option<Vec<Option<JsonValue>>>],
    field_idx: usize,
    data_type: &ArrowDataType,
) -> Result<ArrayRef> {
    match data_type {
        ArrowDataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(structs.len());
            for s in structs {
                match s {
                    Some(fields) => match fields.get(field_idx) {
                        Some(Some(v)) if !v.is_null() => {
                            if let Some(i) = v.as_i64() {
                                builder.append_value(i);
                            } else if let Some(f) = v.as_f64() {
                                builder.append_value(f as i64);
                            } else if let Some(s) = v.as_str() {
                                if let Ok(i) = s.parse::<i64>() {
                                    builder.append_value(i);
                                } else {
                                    builder.append_null();
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(structs.len());
            for s in structs {
                match s {
                    Some(fields) => match fields.get(field_idx) {
                        Some(Some(v)) if !v.is_null() => {
                            if let Some(f) = v.as_f64() {
                                builder.append_value(f);
                            } else if let Some(i) = v.as_i64() {
                                builder.append_value(i as f64);
                            } else if let Some(s) = v.as_str() {
                                if let Ok(f) = s.parse::<f64>() {
                                    builder.append_value(f);
                                } else {
                                    builder.append_null();
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(structs.len());
            for s in structs {
                match s {
                    Some(fields) => match fields.get(field_idx) {
                        Some(Some(v)) if !v.is_null() => {
                            if let Some(b) = v.as_bool() {
                                builder.append_value(b);
                            } else if let Some(s) = v.as_str() {
                                match s.to_lowercase().as_str() {
                                    "true" | "1" => builder.append_value(true),
                                    "false" | "0" => builder.append_value(false),
                                    _ => builder.append_null(),
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            let mut builder = StringBuilder::with_capacity(structs.len(), structs.len() * 32);
            for s in structs {
                match s {
                    Some(fields) => match fields.get(field_idx) {
                        Some(Some(v)) if !v.is_null() => {
                            if let Some(s) = v.as_str() {
                                builder.append_value(s);
                            } else {
                                builder.append_value(v.to_string());
                            }
                        }
                        _ => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn create_null_buffer(null_bitmap: &[bool]) -> NullBuffer {
    NullBuffer::from(BooleanBuffer::from(null_bitmap.to_vec()))
}

fn create_null_bit_buffer(null_bitmap: &[bool]) -> arrow::buffer::Buffer {
    let num_bytes = (null_bitmap.len() + 7) / 8;
    let mut bytes = vec![0u8; num_bytes];
    for (i, &is_valid) in null_bitmap.iter().enumerate() {
        if is_valid {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    arrow::buffer::Buffer::from(bytes)
}

fn build_struct_array(rows: &[Vec<JsonValue>], col_idx: usize) -> Result<ArrayRef> {
    let fields = infer_struct_fields_from_rows(rows, col_idx);

    let mut all_structs: Vec<Option<Vec<Option<JsonValue>>>> = Vec::with_capacity(rows.len());
    let mut null_bitmap: Vec<bool> = Vec::with_capacity(rows.len());

    for row in rows {
        let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            null_bitmap.push(false);
            all_structs.push(None);
        } else if let JsonValue::Object(obj) = value {
            null_bitmap.push(true);
            let struct_values: Vec<Option<JsonValue>> = fields
                .iter()
                .map(|f| obj.get(f.name()).cloned())
                .collect();
            all_structs.push(Some(struct_values));
        } else {
            null_bitmap.push(false);
            all_structs.push(None);
        }
    }

    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .enumerate()
        .map(|(field_idx, field)| {
            build_struct_field_array(&all_structs, field_idx, field.data_type())
        })
        .collect::<Result<Vec<_>>>()?;

    let null_buffer = create_null_buffer(&null_bitmap);
    let struct_array =
        StructArray::try_new(Fields::from(fields), child_arrays, Some(null_buffer))
            .map_err(|e| Error::Internal(format!("Failed to create struct array: {}", e)))?;

    Ok(Arc::new(struct_array))
}

fn parse_date_to_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].split('T').next()?.parse().ok()?;
    
    let days_from_epoch = days_from_ymd(year, month, day);
    Some(days_from_epoch)
}

fn days_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    let y = year as i64;
    let m = month as i64;
    let d = day as i64;
    
    let a = (14 - m) / 12;
    let y_adj = y + 4800 - a;
    let m_adj = m + 12 * a - 3;
    
    let jdn = d + (153 * m_adj + 2) / 5 + 365 * y_adj + y_adj / 4 - y_adj / 100 + y_adj / 400 - 32045;
    (jdn - 2440588) as i32
}

fn parse_timestamp_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_time_part, _tz) = if s.ends_with('Z') {
        (&s[..s.len()-1], "UTC")
    } else if s.contains('+') || (s.len() > 10 && s[10..].contains('-')) {
        let idx = s.rfind(|c| c == '+' || c == '-').filter(|&i| i > 10)?;
        (&s[..idx], &s[idx..])
    } else {
        (s, "")
    };
    
    let parts: Vec<&str> = date_time_part.split(&['T', ' '][..]).collect();
    if parts.is_empty() {
        return None;
    }
    
    let date_parts: Vec<&str> = parts[0].split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    
    let year: i32 = date_parts[0].parse().ok()?;
    let month: u32 = date_parts[1].parse().ok()?;
    let day: u32 = date_parts[2].parse().ok()?;
    
    let days = days_from_ymd(year, month, day) as i64;
    let mut micros = days * 24 * 60 * 60 * 1_000_000;
    
    if parts.len() > 1 {
        let time_str = parts[1];
        let time_parts: Vec<&str> = time_str.split(':').collect();
        if time_parts.len() >= 2 {
            let hours: i64 = time_parts[0].parse().ok()?;
            let minutes: i64 = time_parts[1].parse().ok()?;
            micros += hours * 60 * 60 * 1_000_000;
            micros += minutes * 60 * 1_000_000;
            
            if time_parts.len() > 2 {
                let sec_part = time_parts[2];
                let (secs_str, frac_str) = if sec_part.contains('.') {
                    let idx = sec_part.find('.')?;
                    (&sec_part[..idx], Some(&sec_part[idx+1..]))
                } else {
                    (sec_part, None)
                };
                
                let seconds: i64 = secs_str.parse().ok()?;
                micros += seconds * 1_000_000;
                
                if let Some(frac) = frac_str {
                    let frac_clean: String = frac.chars().take_while(|c| c.is_ascii_digit()).collect();
                    if !frac_clean.is_empty() {
                        let frac_val: i64 = frac_clean.parse().ok()?;
                        let digits = frac_clean.len() as u32;
                        let scale = 10i64.pow(6u32.saturating_sub(digits));
                        let divisor = 10i64.pow(digits.saturating_sub(6));
                        if digits <= 6 {
                            micros += frac_val * scale;
                        } else {
                            micros += frac_val / divisor;
                        }
                    }
                }
            }
        }
    }
    
    Some(micros)
}

fn base64_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    const DECODE_TABLE: [i8; 256] = {
        let mut table = [-1i8; 256];
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < 64 {
            table[alphabet[i] as usize] = i as i8;
            i += 1;
        }
        table
    };
    
    let s = s.trim_end_matches('=');
    let mut result = Vec::with_capacity(s.len() * 3 / 4);
    let bytes = s.as_bytes();
    let chunks = bytes.chunks_exact(4);
    let remainder = chunks.remainder();
    
    for chunk in chunks {
        let a = DECODE_TABLE[chunk[0] as usize];
        let b = DECODE_TABLE[chunk[1] as usize];
        let c = DECODE_TABLE[chunk[2] as usize];
        let d = DECODE_TABLE[chunk[3] as usize];
        if a < 0 || b < 0 || c < 0 || d < 0 {
            return Err(());
        }
        result.push(((a as u8) << 2) | ((b as u8) >> 4));
        result.push(((b as u8) << 4) | ((c as u8) >> 2));
        result.push(((c as u8) << 6) | (d as u8));
    }
    
    match remainder.len() {
        2 => {
            let a = DECODE_TABLE[remainder[0] as usize];
            let b = DECODE_TABLE[remainder[1] as usize];
            if a < 0 || b < 0 {
                return Err(());
            }
            result.push(((a as u8) << 2) | ((b as u8) >> 4));
        }
        3 => {
            let a = DECODE_TABLE[remainder[0] as usize];
            let b = DECODE_TABLE[remainder[1] as usize];
            let c = DECODE_TABLE[remainder[2] as usize];
            if a < 0 || b < 0 || c < 0 {
                return Err(());
            }
            result.push(((a as u8) << 2) | ((b as u8) >> 4));
            result.push(((b as u8) << 4) | ((c as u8) >> 2));
        }
        _ => {}
    }
    
    Ok(result)
}

fn schema_to_json(schema: &Schema) -> String {
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": arrow_type_to_json(f.data_type()),
                "nullable": f.is_nullable()
            })
        })
        .collect();
    serde_json::json!({ "fields": fields }).to_string()
}

fn arrow_type_to_json(dt: &ArrowDataType) -> serde_json::Value {
    match dt {
        ArrowDataType::Int64 => serde_json::json!("INT64"),
        ArrowDataType::Float64 => serde_json::json!("FLOAT64"),
        ArrowDataType::Utf8 => serde_json::json!("STRING"),
        ArrowDataType::Boolean => serde_json::json!("BOOLEAN"),
        ArrowDataType::Date32 => serde_json::json!("DATE"),
        ArrowDataType::Timestamp(_, _) => serde_json::json!("TIMESTAMP"),
        ArrowDataType::Binary => serde_json::json!("BYTES"),
        ArrowDataType::List(field) => {
            serde_json::json!({
                "type": "ARRAY",
                "elementType": arrow_type_to_json(field.data_type())
            })
        }
        ArrowDataType::Struct(fields) => {
            let struct_fields: Vec<serde_json::Value> = fields
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name(),
                        "type": arrow_type_to_json(f.data_type()),
                        "nullable": f.is_nullable()
                    })
                })
                .collect();
            serde_json::json!({
                "type": "STRUCT",
                "fields": struct_fields
            })
        }
        _ => serde_json::json!("STRING"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::yachtsql::ColumnInfo;

    #[test]
    fn test_bq_type_to_arrow_simple() {
        assert!(matches!(
            bq_type_to_arrow_simple("INT64"),
            ArrowDataType::Int64
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("FLOAT64"),
            ArrowDataType::Float64
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("STRING"),
            ArrowDataType::Utf8
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("BOOL"),
            ArrowDataType::Boolean
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("DATE"),
            ArrowDataType::Date32
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("TIMESTAMP"),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("BYTES"),
            ArrowDataType::Binary
        ));
        assert!(matches!(
            bq_type_to_arrow_simple("NUMERIC"),
            ArrowDataType::Utf8
        ));
    }

    #[test]
    fn test_query_result_to_record_batch_empty() {
        let result = QueryResult {
            columns: vec![],
            rows: vec![],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_query_result_to_record_batch_int64() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "INT64".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_query_result_to_record_batch_mixed_types() {
        let result = QueryResult {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INT64".to_string(),
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "STRING".to_string(),
                },
                ColumnInfo {
                    name: "score".to_string(),
                    data_type: "FLOAT64".to_string(),
                },
                ColumnInfo {
                    name: "active".to_string(),
                    data_type: "BOOL".to_string(),
                },
            ],
            rows: vec![
                vec![
                    serde_json::json!(1),
                    serde_json::json!("Alice"),
                    serde_json::json!(95.5),
                    serde_json::json!(true),
                ],
                vec![
                    serde_json::json!(2),
                    serde_json::json!("Bob"),
                    serde_json::json!(87.3),
                    serde_json::json!(false),
                ],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_parse_date_to_days() {
        assert_eq!(parse_date_to_days("1970-01-01"), Some(0));
        assert_eq!(parse_date_to_days("2022-01-01"), Some(18993));
        assert!(parse_date_to_days("invalid").is_none());
    }

    #[test]
    fn test_parse_timestamp_to_micros() {
        assert_eq!(parse_timestamp_to_micros("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_timestamp_to_micros("1970-01-01 00:00:00"), Some(0));
        assert!(parse_timestamp_to_micros("invalid").is_none());
    }

    #[test]
    fn test_base64_decode() {
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("Zm9v").unwrap(), b"foo");
        assert!(base64_decode("!!!").is_err());
    }

    #[test]
    fn test_validate_shm_path() {
        let valid_path = format!("{}/{}{}", SHM_DIR, SHM_PREFIX, "test_123");
        assert!(ArrowSharedMemoryWriter::validate_shm_path(&valid_path).is_ok());

        assert!(ArrowSharedMemoryWriter::validate_shm_path("/etc/passwd").is_err());
        assert!(
            ArrowSharedMemoryWriter::validate_shm_path(&format!("{}/bad_file", SHM_DIR)).is_err()
        );
    }

    #[test]
    fn test_schema_to_json() {
        let schema = Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, true),
            Field::new("name", ArrowDataType::Utf8, true),
        ]);
        let json = schema_to_json(&schema);
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"INT64\""));
        assert!(json.contains("\"name\""));
        assert!(json.contains("\"STRING\""));
    }

    #[test]
    fn test_write_and_release() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "x".to_string(),
                data_type: "INT64".to_string(),
            }],
            rows: vec![vec![serde_json::json!(42)]],
        };

        let session_id = Uuid::new_v4();
        let handle = ArrowSharedMemoryWriter::write_query_result(&result, &session_id).unwrap();

        assert!(std::path::Path::new(&handle.path).exists());
        assert_eq!(handle.row_count, 1);
        assert!(handle.size > 0);

        ArrowSharedMemoryWriter::release(&handle.path).unwrap();
        assert!(!std::path::Path::new(&handle.path).exists());
    }

    #[test]
    fn test_arrow_type_to_json() {
        assert_eq!(arrow_type_to_json(&ArrowDataType::Int64), "INT64");
        assert_eq!(arrow_type_to_json(&ArrowDataType::Float64), "FLOAT64");
        assert_eq!(arrow_type_to_json(&ArrowDataType::Utf8), "STRING");
        assert_eq!(arrow_type_to_json(&ArrowDataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_type_to_json(&ArrowDataType::Date32), "DATE");
        assert_eq!(arrow_type_to_json(&ArrowDataType::Binary), "BYTES");
    }

    #[test]
    fn test_parse_array_element_type() {
        assert_eq!(parse_array_element_type("ARRAY<INT64>"), Some("INT64"));
        assert_eq!(parse_array_element_type("ARRAY<STRING>"), Some("STRING"));
        assert_eq!(parse_array_element_type("ARRAY<STRUCT>"), Some("STRUCT"));
        assert_eq!(
            parse_array_element_type("ARRAY<ARRAY<INT64>>"),
            Some("ARRAY<INT64>")
        );
        assert_eq!(parse_array_element_type("INT64"), None);
        assert_eq!(parse_array_element_type("STRUCT"), None);
    }

    #[test]
    fn test_infer_type_from_json() {
        assert!(matches!(
            infer_type_from_json(&serde_json::json!(null)),
            ArrowDataType::Utf8
        ));
        assert!(matches!(
            infer_type_from_json(&serde_json::json!(true)),
            ArrowDataType::Boolean
        ));
        assert!(matches!(
            infer_type_from_json(&serde_json::json!(42)),
            ArrowDataType::Int64
        ));
        assert!(matches!(
            infer_type_from_json(&serde_json::json!(3.14)),
            ArrowDataType::Float64
        ));
        assert!(matches!(
            infer_type_from_json(&serde_json::json!("hello")),
            ArrowDataType::Utf8
        ));
        assert!(matches!(
            infer_type_from_json(&serde_json::json!([1, 2, 3])),
            ArrowDataType::List(_)
        ));
    }

    #[test]
    fn test_query_result_to_record_batch_array_int64() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "numbers".to_string(),
                data_type: "ARRAY<INT64>".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!([1, 2, 3])],
                vec![serde_json::json!([4, 5])],
                vec![serde_json::json!(null)],
                vec![serde_json::json!([])],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 4);

        let schema = batch.schema();
        let field = &schema.fields()[0];
        assert!(matches!(field.data_type(), ArrowDataType::List(_)));
    }

    #[test]
    fn test_query_result_to_record_batch_array_string() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "names".to_string(),
                data_type: "ARRAY<STRING>".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!(["Alice", "Bob"])],
                vec![serde_json::json!(["Charlie"])],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_query_result_to_record_batch_struct() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "person".to_string(),
                data_type: "STRUCT".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!({"name": "Alice", "age": 30})],
                vec![serde_json::json!({"name": "Bob", "age": 25})],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);

        let schema = batch.schema();
        let field = &schema.fields()[0];
        assert!(matches!(field.data_type(), ArrowDataType::Struct(_)));
    }

    #[test]
    fn test_query_result_to_record_batch_array_of_struct() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "people".to_string(),
                data_type: "ARRAY<STRUCT>".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!([
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"}
                ])],
                vec![serde_json::json!([{"id": 3, "name": "Charlie"}])],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);

        let schema = batch.schema();
        let field = &schema.fields()[0];
        if let ArrowDataType::List(inner) = field.data_type() {
            assert!(matches!(inner.data_type(), ArrowDataType::Struct(_)));
        } else {
            panic!("Expected List type");
        }
    }

    #[test]
    fn test_schema_to_json_with_array() {
        let schema = Schema::new(vec![Field::new(
            "numbers",
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true))),
            true,
        )]);
        let json = schema_to_json(&schema);
        assert!(json.contains("\"ARRAY\""));
        assert!(json.contains("\"elementType\""));
        assert!(json.contains("\"INT64\""));
    }

    #[test]
    fn test_schema_to_json_with_struct() {
        let schema = Schema::new(vec![Field::new(
            "person",
            ArrowDataType::Struct(Fields::from(vec![
                Field::new("name", ArrowDataType::Utf8, true),
                Field::new("age", ArrowDataType::Int64, true),
            ])),
            true,
        )]);
        let json = schema_to_json(&schema);
        assert!(json.contains("\"STRUCT\""));
        assert!(json.contains("\"fields\""));
        assert!(json.contains("\"name\""));
        assert!(json.contains("\"age\""));
    }

    #[test]
    fn test_query_result_to_record_batch_array_float64() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "scores".to_string(),
                data_type: "ARRAY<FLOAT64>".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!([1.5, 2.5, 3.5])],
                vec![serde_json::json!([4.0])],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_query_result_to_record_batch_array_bool() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "flags".to_string(),
                data_type: "ARRAY<BOOL>".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!([true, false, true])],
                vec![serde_json::json!([false])],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_query_result_to_record_batch_array_date() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "dates".to_string(),
                data_type: "ARRAY<DATE>".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!(["2022-01-01", "2022-12-31"])],
                vec![serde_json::json!(["1970-01-01"])],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_query_result_to_record_batch_struct_with_nulls() {
        let result = QueryResult {
            columns: vec![ColumnInfo {
                name: "data".to_string(),
                data_type: "STRUCT".to_string(),
            }],
            rows: vec![
                vec![serde_json::json!({"a": 1, "b": "hello"})],
                vec![serde_json::json!({"a": null, "b": "world"})],
                vec![serde_json::json!({"a": 3})],
                vec![serde_json::json!(null)],
            ],
        };
        let batch = query_result_to_record_batch(&result).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 4);
    }

    #[test]
    fn test_bq_type_to_arrow_with_inference_array() {
        let rows = vec![vec![serde_json::json!([1, 2, 3])]];
        let dt = bq_type_to_arrow_with_inference("ARRAY<INT64>", &rows, 0);
        assert!(matches!(dt, ArrowDataType::List(_)));
        if let ArrowDataType::List(inner) = dt {
            assert!(matches!(inner.data_type(), ArrowDataType::Int64));
        }
    }

    #[test]
    fn test_bq_type_to_arrow_with_inference_struct() {
        let rows = vec![vec![serde_json::json!({"name": "Alice", "age": 30})]];
        let dt = bq_type_to_arrow_with_inference("STRUCT", &rows, 0);
        assert!(matches!(dt, ArrowDataType::Struct(_)));
    }

    #[test]
    fn test_infer_struct_fields_from_rows() {
        let rows = vec![
            vec![serde_json::json!({"name": "Alice", "age": 30})],
            vec![serde_json::json!({"name": "Bob", "age": 25, "active": true})],
        ];
        let fields = infer_struct_fields_from_rows(&rows, 0);
        assert_eq!(fields.len(), 3);
        let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"age"));
        assert!(field_names.contains(&"active"));
    }

    #[test]
    fn test_create_null_buffer() {
        let bitmap = vec![true, false, true, true, false];
        let null_buffer = create_null_buffer(&bitmap);
        assert_eq!(null_buffer.len(), 5);
        assert!(null_buffer.is_valid(0));
        assert!(!null_buffer.is_valid(1));
        assert!(null_buffer.is_valid(2));
        assert!(null_buffer.is_valid(3));
        assert!(!null_buffer.is_valid(4));
    }
}
