use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Executor error: {0}")]
    Executor(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Session not found: {0}")]
    SessionNotFound(uuid::Uuid),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Loader error: {0}")]
    Loader(String),

    #[error("BigQuery error: {0}")]
    BigQuery(String),

    #[error("BigQuery job {job_id} timed out after {elapsed_secs} seconds")]
    Timeout { job_id: String, elapsed_secs: u64 },

    #[error("Request timed out after {0} seconds")]
    RequestTimeout(u64),
}

impl Error {
    pub fn code(&self) -> i32 {
        match self {
            Error::Executor(_) => -32000,
            Error::Json(_) => -32700,
            Error::SessionNotFound(_) => -32002,
            Error::InvalidRequest(_) => -32600,
            Error::Internal(_) => -32603,
            Error::Loader(_) => -32001,
            Error::BigQuery(_) => -32003,
            Error::Timeout { .. } => -32004,
            Error::RequestTimeout(_) => -32005,
        }
    }

    pub fn with_context(self, method: &str, session_id: Option<&str>) -> Self {
        let context = match session_id {
            Some(sid) => format!("[method={}, session={}]", method, sid),
            None => format!("[method={}]", method),
        };

        match self {
            Error::Executor(msg) => Error::Executor(format!("{} {}", context, msg)),
            Error::Internal(msg) => Error::Internal(format!("{} {}", context, msg)),
            Error::Loader(msg) => Error::Loader(format!("{} {}", context, msg)),
            Error::BigQuery(msg) => Error::BigQuery(format!("{} {}", context, msg)),
            other => other,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_executor() {
        let err = Error::Executor("test error".to_string());
        assert_eq!(format!("{}", err), "Executor error: test error");
    }

    #[test]
    fn test_error_display_json() {
        let json_err: serde_json::Error = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = Error::Json(json_err);
        assert!(format!("{}", err).starts_with("JSON error:"));
    }

    #[test]
    fn test_error_display_session_not_found() {
        let id = uuid::Uuid::nil();
        let err = Error::SessionNotFound(id);
        assert_eq!(
            format!("{}", err),
            "Session not found: 00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn test_error_display_invalid_request() {
        let err = Error::InvalidRequest("bad request".to_string());
        assert_eq!(format!("{}", err), "Invalid request: bad request");
    }

    #[test]
    fn test_error_display_internal() {
        let err = Error::Internal("internal issue".to_string());
        assert_eq!(format!("{}", err), "Internal error: internal issue");
    }

    #[test]
    fn test_error_display_loader() {
        let err = Error::Loader("load failed".to_string());
        assert_eq!(format!("{}", err), "Loader error: load failed");
    }

    #[test]
    fn test_error_code_executor() {
        let err = Error::Executor("test".to_string());
        assert_eq!(err.code(), -32000);
    }

    #[test]
    fn test_error_code_json() {
        let json_err: serde_json::Error = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = Error::Json(json_err);
        assert_eq!(err.code(), -32700);
    }

    #[test]
    fn test_error_code_session_not_found() {
        let err = Error::SessionNotFound(uuid::Uuid::nil());
        assert_eq!(err.code(), -32002);
    }

    #[test]
    fn test_error_code_invalid_request() {
        let err = Error::InvalidRequest("test".to_string());
        assert_eq!(err.code(), -32600);
    }

    #[test]
    fn test_error_code_internal() {
        let err = Error::Internal("test".to_string());
        assert_eq!(err.code(), -32603);
    }

    #[test]
    fn test_error_code_loader() {
        let err = Error::Loader("test".to_string());
        assert_eq!(err.code(), -32001);
    }

    #[test]
    fn test_with_context_executor_with_session() {
        let err = Error::Executor("failed".to_string());
        let contextualized = err.with_context("run_query", Some("session-123"));
        match contextualized {
            Error::Executor(msg) => {
                assert!(msg.contains("[method=run_query, session=session-123]"));
                assert!(msg.contains("failed"));
            }
            _ => panic!("Expected Executor variant"),
        }
    }

    #[test]
    fn test_with_context_executor_without_session() {
        let err = Error::Executor("failed".to_string());
        let contextualized = err.with_context("run_query", None);
        match contextualized {
            Error::Executor(msg) => {
                assert!(msg.contains("[method=run_query]"));
                assert!(msg.contains("failed"));
            }
            _ => panic!("Expected Executor variant"),
        }
    }

    #[test]
    fn test_with_context_internal() {
        let err = Error::Internal("internal".to_string());
        let contextualized = err.with_context("execute", Some("sid"));
        match contextualized {
            Error::Internal(msg) => {
                assert!(msg.contains("[method=execute, session=sid]"));
                assert!(msg.contains("internal"));
            }
            _ => panic!("Expected Internal variant"),
        }
    }

    #[test]
    fn test_with_context_loader() {
        let err = Error::Loader("load".to_string());
        let contextualized = err.with_context("load_file", None);
        match contextualized {
            Error::Loader(msg) => {
                assert!(msg.contains("[method=load_file]"));
                assert!(msg.contains("load"));
            }
            _ => panic!("Expected Loader variant"),
        }
    }

    #[test]
    fn test_with_context_passthrough_json() {
        let json_err: serde_json::Error = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = Error::Json(json_err);
        let contextualized = err.with_context("parse", Some("s"));
        assert!(matches!(contextualized, Error::Json(_)));
    }

    #[test]
    fn test_with_context_passthrough_session_not_found() {
        let id = uuid::Uuid::nil();
        let err = Error::SessionNotFound(id);
        let contextualized = err.with_context("get_session", Some("s"));
        match contextualized {
            Error::SessionNotFound(returned_id) => assert_eq!(returned_id, id),
            _ => panic!("Expected SessionNotFound variant"),
        }
    }

    #[test]
    fn test_with_context_passthrough_invalid_request() {
        let err = Error::InvalidRequest("bad".to_string());
        let contextualized = err.with_context("validate", None);
        match contextualized {
            Error::InvalidRequest(msg) => assert_eq!(msg, "bad"),
            _ => panic!("Expected InvalidRequest variant"),
        }
    }

    #[test]
    fn test_error_from_serde_json() {
        let json_err: serde_json::Error = serde_json::from_str::<()>("invalid").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Json(_)));
    }

    #[test]
    fn test_error_debug() {
        let err = Error::Executor("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Executor"));
    }

    #[test]
    fn test_error_display_request_timeout() {
        let err = Error::RequestTimeout(300);
        assert_eq!(format!("{}", err), "Request timed out after 300 seconds");
    }

    #[test]
    fn test_error_code_request_timeout() {
        let err = Error::RequestTimeout(300);
        assert_eq!(err.code(), -32005);
    }
}
