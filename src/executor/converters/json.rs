use serde_json::Value;

pub fn json_to_sql_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_sql_value).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("'{}': {}", k.replace('\'', "''"), json_to_sql_value(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_null() {
        let val = json!(null);
        assert_eq!(json_to_sql_value(&val), "NULL");
    }

    #[test]
    fn test_bool_true() {
        let val = json!(true);
        assert_eq!(json_to_sql_value(&val), "true");
    }

    #[test]
    fn test_bool_false() {
        let val = json!(false);
        assert_eq!(json_to_sql_value(&val), "false");
    }

    #[test]
    fn test_number_integer() {
        let val = json!(42);
        assert_eq!(json_to_sql_value(&val), "42");
    }

    #[test]
    fn test_number_float() {
        let val = json!(3.14);
        assert_eq!(json_to_sql_value(&val), "3.14");
    }

    #[test]
    fn test_number_negative() {
        let val = json!(-100);
        assert_eq!(json_to_sql_value(&val), "-100");
    }

    #[test]
    fn test_string_simple() {
        let val = json!("hello");
        assert_eq!(json_to_sql_value(&val), "'hello'");
    }

    #[test]
    fn test_string_with_single_quote() {
        let val = json!("it's a test");
        assert_eq!(json_to_sql_value(&val), "'it''s a test'");
    }

    #[test]
    fn test_string_with_multiple_quotes() {
        let val = json!("it's John's");
        assert_eq!(json_to_sql_value(&val), "'it''s John''s'");
    }

    #[test]
    fn test_array_empty() {
        let val = json!([]);
        assert_eq!(json_to_sql_value(&val), "[]");
    }

    #[test]
    fn test_array_single_element() {
        let val = json!([1]);
        assert_eq!(json_to_sql_value(&val), "[1]");
    }

    #[test]
    fn test_array_multiple_elements() {
        let val = json!([1, 2, 3]);
        assert_eq!(json_to_sql_value(&val), "[1, 2, 3]");
    }

    #[test]
    fn test_array_mixed_types() {
        let val = json!([1, "two", true, null]);
        assert_eq!(json_to_sql_value(&val), "[1, 'two', true, NULL]");
    }

    #[test]
    fn test_array_nested() {
        let val = json!([[1, 2], [3, 4]]);
        assert_eq!(json_to_sql_value(&val), "[[1, 2], [3, 4]]");
    }

    #[test]
    fn test_object_empty() {
        let val = json!({});
        assert_eq!(json_to_sql_value(&val), "{}");
    }

    #[test]
    fn test_object_single_field() {
        let val = json!({"key": "value"});
        assert_eq!(json_to_sql_value(&val), "{'key': 'value'}");
    }

    #[test]
    fn test_object_multiple_fields() {
        let val = json!({"a": 1, "b": 2});
        let result = json_to_sql_value(&val);
        assert!(result.contains("'a': 1"));
        assert!(result.contains("'b': 2"));
        assert!(result.starts_with('{'));
        assert!(result.ends_with('}'));
    }

    #[test]
    fn test_object_mixed_values() {
        let val = json!({"num": 42, "str": "hello", "bool": true, "null": null});
        let result = json_to_sql_value(&val);
        assert!(result.contains("'num': 42"));
        assert!(result.contains("'str': 'hello'"));
        assert!(result.contains("'bool': true"));
        assert!(result.contains("'null': NULL"));
    }

    #[test]
    fn test_object_nested() {
        let val = json!({"outer": {"inner": 1}});
        let result = json_to_sql_value(&val);
        assert!(result.contains("'outer': {'inner': 1}"));
    }

    #[test]
    fn test_object_with_array() {
        let val = json!({"items": [1, 2, 3]});
        let result = json_to_sql_value(&val);
        assert!(result.contains("'items': [1, 2, 3]"));
    }

    #[test]
    fn test_array_with_objects() {
        let val = json!([{"a": 1}, {"b": 2}]);
        let result = json_to_sql_value(&val);
        assert!(result.starts_with('['));
        assert!(result.ends_with(']'));
        assert!(result.contains("{'a': 1}"));
        assert!(result.contains("{'b': 2}"));
    }

    #[test]
    fn test_complex_nested_structure() {
        let val = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ],
            "count": 2
        });
        let result = json_to_sql_value(&val);
        assert!(result.contains("'users':"));
        assert!(result.contains("'count': 2"));
        assert!(result.contains("'name': 'Alice'"));
        assert!(result.contains("'age': 30"));
    }
}
