use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct RpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcResponse {
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }

    pub fn parse_error() -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32700,
                message: "Parse error".to_string(),
                data: None,
            }),
            id: Value::Null,
        }
    }

    pub fn invalid_request() -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32600,
                message: "Invalid Request".to_string(),
                data: None,
            }),
            id: Value::Null,
        }
    }

    pub fn method_not_found(id: Value, method: &str) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32601,
                message: format!("Method not found: {}", method),
                data: None,
            }),
            id,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PingResult {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_rpc_request_deserialization() {
        let json_str = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{},"id":1}"#;
        let req: RpcRequest = serde_json::from_str(json_str).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "bq.ping");
        assert_eq!(req.id, Some(json!(1)));
    }

    #[test]
    fn test_rpc_request_without_id() {
        let json_str = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{}}"#;
        let req: RpcRequest = serde_json::from_str(json_str).unwrap();
        assert!(req.id.is_none());
    }

    #[test]
    fn test_rpc_request_without_params() {
        let json_str = r#"{"jsonrpc":"2.0","method":"bq.ping"}"#;
        let req: RpcRequest = serde_json::from_str(json_str).unwrap();
        assert_eq!(req.params, Value::Null);
    }

    #[test]
    fn test_rpc_response_success() {
        let resp = RpcResponse::success(json!(1), json!({"message": "pong"}));
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
        assert_eq!(resp.id, json!(1));
    }

    #[test]
    fn test_rpc_response_success_serialization() {
        let resp = RpcResponse::success(json!(1), json!({"message": "pong"}));
        let serialized = serde_json::to_string(&resp).unwrap();
        assert!(serialized.contains("\"jsonrpc\":\"2.0\""));
        assert!(serialized.contains("\"result\""));
        assert!(!serialized.contains("\"error\""));
    }

    #[test]
    fn test_rpc_response_error() {
        let resp = RpcResponse::error(json!(1), -32600, "Invalid Request".to_string());
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_none());
        assert!(resp.error.is_some());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "Invalid Request");
        assert!(err.data.is_none());
    }

    #[test]
    fn test_rpc_response_error_serialization() {
        let resp = RpcResponse::error(json!(1), -32600, "Invalid Request".to_string());
        let serialized = serde_json::to_string(&resp).unwrap();
        assert!(serialized.contains("\"error\""));
        assert!(!serialized.contains("\"result\""));
        assert!(!serialized.contains("\"data\""));
    }

    #[test]
    fn test_rpc_response_parse_error() {
        let resp = RpcResponse::parse_error();
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32700);
        assert_eq!(err.message, "Parse error");
        assert_eq!(resp.id, Value::Null);
    }

    #[test]
    fn test_rpc_response_invalid_request() {
        let resp = RpcResponse::invalid_request();
        assert_eq!(resp.jsonrpc, "2.0");
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "Invalid Request");
        assert_eq!(resp.id, Value::Null);
    }

    #[test]
    fn test_rpc_response_method_not_found() {
        let resp = RpcResponse::method_not_found(json!(42), "unknown.method");
        assert_eq!(resp.jsonrpc, "2.0");
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.message, "Method not found: unknown.method");
        assert_eq!(resp.id, json!(42));
    }

    #[test]
    fn test_ping_result_serialization() {
        let result = PingResult {
            message: "pong".to_string(),
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["message"], "pong");
    }
}
