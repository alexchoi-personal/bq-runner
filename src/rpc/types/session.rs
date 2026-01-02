use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct CreateSessionResult {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Deserialize)]
pub struct DestroySessionParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct DestroySessionResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct SetDefaultProjectParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub project: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SetDefaultProjectResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct GetDefaultProjectParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct GetDefaultProjectResult {
    pub project: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetProjectsParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct GetProjectsResult {
    pub projects: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetDatasetsParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub project: String,
}

#[derive(Debug, Serialize)]
pub struct GetDatasetsResult {
    pub datasets: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetTablesInDatasetParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub project: String,
    pub dataset: String,
}

#[derive(Debug, Serialize)]
pub struct GetTablesInDatasetResult {
    pub tables: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn test_create_session_result_serialization() {
        let result = CreateSessionResult {
            session_id: "abc-123".to_string(),
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["sessionId"], "abc-123");
    }

    #[test]
    fn test_destroy_session_params_deserialization() {
        let json_str = r#"{"sessionId":"abc-123"}"#;
        let params: DestroySessionParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc-123");
    }

    #[test]
    fn test_destroy_session_result_serialization() {
        let result = DestroySessionResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_set_default_project_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","project":"my-project"}"#;
        let params: SetDefaultProjectParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.project, Some("my-project".to_string()));
    }

    #[test]
    fn test_set_default_project_params_null_project() {
        let json_str = r#"{"sessionId":"abc","project":null}"#;
        let params: SetDefaultProjectParams = serde_json::from_str(json_str).unwrap();
        assert!(params.project.is_none());
    }

    #[test]
    fn test_set_default_project_result_serialization() {
        let result = SetDefaultProjectResult { success: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["success"], true);
    }

    #[test]
    fn test_get_default_project_params_deserialization() {
        let json_str = r#"{"sessionId":"abc"}"#;
        let params: GetDefaultProjectParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
    }

    #[test]
    fn test_get_default_project_result_serialization() {
        let result = GetDefaultProjectResult {
            project: Some("my-proj".to_string()),
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["project"], "my-proj");
    }

    #[test]
    fn test_get_projects_params_deserialization() {
        let json_str = r#"{"sessionId":"abc"}"#;
        let params: GetProjectsParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
    }

    #[test]
    fn test_get_projects_result_serialization() {
        let result = GetProjectsResult {
            projects: vec!["p1".to_string(), "p2".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["projects"], json!(["p1", "p2"]));
    }

    #[test]
    fn test_get_datasets_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","project":"my-proj"}"#;
        let params: GetDatasetsParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.project, "my-proj");
    }

    #[test]
    fn test_get_datasets_result_serialization() {
        let result = GetDatasetsResult {
            datasets: vec!["d1".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["datasets"], json!(["d1"]));
    }

    #[test]
    fn test_get_tables_in_dataset_params_deserialization() {
        let json_str = r#"{"sessionId":"abc","project":"proj","dataset":"ds"}"#;
        let params: GetTablesInDatasetParams = serde_json::from_str(json_str).unwrap();
        assert_eq!(params.session_id, "abc");
        assert_eq!(params.project, "proj");
        assert_eq!(params.dataset, "ds");
    }

    #[test]
    fn test_get_tables_in_dataset_result_serialization() {
        let result = GetTablesInDatasetResult {
            tables: vec!["t1".to_string()],
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["tables"], json!(["t1"]));
    }
}
