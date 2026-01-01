use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tracing::{error, info};

use super::methods::RpcMethods;
use super::types::{RpcRequest, RpcResponse};

pub async fn handle_websocket(socket: WebSocket, methods: Arc<RpcMethods>) {
    let (mut sender, mut receiver) = socket.split();

    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                info!("WebSocket closed by client");
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
        };

        let response = process_message(&msg, &methods).await;

        let response_text = match serde_json::to_string(&response) {
            Ok(text) => text,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                continue;
            }
        };

        if sender.send(Message::Text(response_text)).await.is_err() {
            error!("Failed to send response");
            break;
        }
    }
}

pub async fn process_message(msg: &str, methods: &RpcMethods) -> RpcResponse {
    let request: RpcRequest = match serde_json::from_str(msg) {
        Ok(req) => req,
        Err(_) => return RpcResponse::parse_error(),
    };

    if request.jsonrpc != "2.0" {
        return RpcResponse::invalid_request();
    }

    let id = request.id.clone().unwrap_or(Value::Null);
    let method_name = request.method.clone();

    let session_id = request
        .params
        .get("sessionId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    match methods.dispatch(&request.method, request.params).await {
        Ok(result) => RpcResponse::success(id, result),
        Err(e) => {
            if matches!(e, crate::error::Error::InvalidRequest(ref msg) if msg.starts_with("Unknown method"))
            {
                RpcResponse::method_not_found(id, &method_name)
            } else {
                let e = e.with_context(&method_name, session_id.as_deref());
                RpcResponse::error(id, e.code(), e.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::SessionManager;
    use serde_json::json;

    fn create_test_methods() -> Arc<RpcMethods> {
        let session_manager = Arc::new(SessionManager::new());
        Arc::new(RpcMethods::new(session_manager))
    }

    #[tokio::test]
    async fn test_process_message_parse_error() {
        let methods = create_test_methods();
        let response = process_message("not valid json", &methods).await;
        assert!(response.error.is_some());
        let err = response.error.unwrap();
        assert_eq!(err.code, -32700);
        assert_eq!(err.message, "Parse error");
    }

    #[tokio::test]
    async fn test_process_message_invalid_jsonrpc_version() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"1.0","method":"bq.ping","id":1}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.error.is_some());
        let err = response.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "Invalid Request");
    }

    #[tokio::test]
    async fn test_process_message_ping_success() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{},"id":1}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.result.is_some());
        assert!(response.error.is_none());
        assert_eq!(response.id, json!(1));
        let result = response.result.unwrap();
        assert_eq!(result["message"], "pong");
    }

    #[tokio::test]
    async fn test_process_message_unknown_method() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"unknown.method","params":{},"id":42}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.error.is_some());
        let err = response.error.unwrap();
        assert_eq!(err.code, -32601);
        assert!(err.message.contains("Method not found"));
        assert!(err.message.contains("unknown.method"));
        assert_eq!(response.id, json!(42));
    }

    #[tokio::test]
    async fn test_process_message_no_id() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{}}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.result.is_some());
        assert_eq!(response.id, Value::Null);
    }

    #[tokio::test]
    async fn test_process_message_create_session() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.createSession","params":{},"id":1}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.result.is_some());
        let result = response.result.unwrap();
        assert!(result["sessionId"].is_string());
    }

    #[tokio::test]
    async fn test_process_message_destroy_session_invalid_uuid() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.destroySession","params":{"sessionId":"invalid"},"id":1}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.error.is_some());
        let err = response.error.unwrap();
        assert_eq!(err.code, -32600);
    }

    #[tokio::test]
    async fn test_process_message_session_not_found() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.destroySession","params":{"sessionId":"00000000-0000-0000-0000-000000000000"},"id":1}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.error.is_some());
        let err = response.error.unwrap();
        assert_eq!(err.code, -32002);
    }

    #[tokio::test]
    async fn test_process_message_with_session_id_context() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.query","params":{"sessionId":"00000000-0000-0000-0000-000000000000","sql":"SELECT 1"},"id":1}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.error.is_some());
    }

    #[tokio::test]
    async fn test_process_message_string_id() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{},"id":"my-request-id"}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.result.is_some());
        assert_eq!(response.id, json!("my-request-id"));
    }

    #[tokio::test]
    async fn test_process_message_null_id() {
        let methods = create_test_methods();
        let msg = r#"{"jsonrpc":"2.0","method":"bq.ping","params":{},"id":null}"#;
        let response = process_message(msg, &methods).await;
        assert!(response.result.is_some());
        assert_eq!(response.id, Value::Null);
    }
}
