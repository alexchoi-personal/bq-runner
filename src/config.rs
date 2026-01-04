use crate::error::{Error, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::warn;

fn env_override<T: FromStr>(target: &mut T, env_var: &str) {
    if let Ok(val) = std::env::var(env_var) {
        match val.parse() {
            Ok(n) => *target = n,
            Err(_) => warn!("Invalid {} value '{}', using default", env_var, val),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    #[default]
    Text,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub allowed_paths: Vec<PathBuf>,
    #[serde(default = "default_block_symlinks")]
    pub block_symlinks: bool,
}

fn default_block_symlinks() -> bool {
    true
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            allowed_paths: Vec::new(),
            block_symlinks: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub format: LogFormat,
    #[serde(default)]
    pub audit_enabled: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::Text,
            audit_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionConfig {
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    #[serde(default = "default_session_timeout_secs")]
    pub session_timeout_secs: u64,
    #[serde(default = "default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_table_timeout_secs")]
    pub table_timeout_secs: u64,
}

fn default_max_sessions() -> usize {
    1000
}

fn default_session_timeout_secs() -> u64 {
    3600
}

fn default_cleanup_interval_secs() -> u64 {
    60
}

fn default_max_concurrency() -> usize {
    8
}

fn default_table_timeout_secs() -> u64 {
    300
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_sessions: default_max_sessions(),
            session_timeout_secs: default_session_timeout_secs(),
            cleanup_interval_secs: default_cleanup_interval_secs(),
            max_concurrency: default_max_concurrency(),
            table_timeout_secs: default_table_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_rate_limit_per_second")]
    pub rate_limit_per_second: u64,
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,
    #[serde(default = "default_max_sql_length")]
    pub max_sql_length: usize,
    #[serde(default = "default_max_rows_per_insert")]
    pub max_rows_per_insert: usize,
    #[serde(default = "default_max_ws_message_size")]
    pub max_ws_message_size: usize,
}

fn default_request_timeout_secs() -> u64 {
    300
}

fn default_rate_limit_per_second() -> u64 {
    100
}

fn default_rate_limit_burst() -> u32 {
    200
}

fn default_max_sql_length() -> usize {
    1_048_576
}

fn default_max_rows_per_insert() -> usize {
    10_000
}

fn default_max_ws_message_size() -> usize {
    4 * 1024 * 1024
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: default_request_timeout_secs(),
            rate_limit_per_second: default_rate_limit_per_second(),
            rate_limit_burst: default_rate_limit_burst(),
            max_sql_length: default_max_sql_length(),
            max_rows_per_insert: default_max_rows_per_insert(),
            max_ws_message_size: default_max_ws_message_size(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_key: Option<String>,
}

impl AuthConfig {
    pub fn validate_key(&self, provided_key: &str) -> bool {
        match &self.api_key {
            Some(expected) => {
                use subtle::ConstantTimeEq;
                expected.as_bytes().ct_eq(provided_key.as_bytes()).into()
            }
            None => false,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub session: SessionConfig,
    #[serde(default)]
    pub rpc: RpcConfig,
    #[serde(default)]
    pub auth: AuthConfig,
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut config = match path {
            Some(p) => {
                let contents = std::fs::read_to_string(p).map_err(|e| {
                    Error::InvalidRequest(format!("Cannot read config file: {}", e))
                })?;
                toml::from_str(&contents)
                    .map_err(|e| Error::InvalidRequest(format!("Invalid config file: {}", e)))?
            }
            None => Config::default(),
        };

        if let Ok(paths) = std::env::var("BQ_RUNNER_ALLOWED_PATHS") {
            config.security.allowed_paths = paths.split(':').map(PathBuf::from).collect();
        }
        if let Ok(val) = std::env::var("BQ_RUNNER_BLOCK_SYMLINKS") {
            config.security.block_symlinks = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("BQ_RUNNER_AUDIT_LOGGING") {
            config.logging.audit_enabled = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("BQ_RUNNER_LOG_FORMAT") {
            config.logging.format = match val.to_lowercase().as_str() {
                "json" => LogFormat::Json,
                _ => LogFormat::Text,
            };
        }
        env_override(&mut config.session.max_sessions, "BQ_RUNNER_MAX_SESSIONS");
        env_override(
            &mut config.session.session_timeout_secs,
            "BQ_RUNNER_SESSION_TIMEOUT_SECS",
        );
        env_override(&mut config.session.max_concurrency, "BQ_MAX_CONCURRENCY");
        env_override(
            &mut config.session.table_timeout_secs,
            "BQ_TABLE_TIMEOUT_SECS",
        );
        env_override(
            &mut config.rpc.request_timeout_secs,
            "BQ_RUNNER_REQUEST_TIMEOUT_SECS",
        );
        env_override(
            &mut config.rpc.rate_limit_per_second,
            "BQ_RUNNER_RATE_LIMIT_PER_SECOND",
        );
        env_override(
            &mut config.rpc.rate_limit_burst,
            "BQ_RUNNER_RATE_LIMIT_BURST",
        );
        if let Ok(val) = std::env::var("BQ_RUNNER_API_KEY") {
            config.auth.api_key = Some(val);
            config.auth.enabled = true;
        }
        if let Ok(val) = std::env::var("BQ_RUNNER_AUTH_ENABLED") {
            config.auth.enabled = val == "true" || val == "1";
        }

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        for path in &self.security.allowed_paths {
            if !path.exists() {
                warn!("Allowed path does not exist: {:?}", path);
            }
        }
        if self.security.allowed_paths.is_empty() {
            warn!("No allowed_paths configured - path validation will block all file loading");
        }

        if self.rpc.request_timeout_secs == 0 {
            return Err(Error::InvalidRequest(
                "request_timeout_secs must be greater than 0".into(),
            ));
        }
        if self.rpc.rate_limit_per_second == 0 {
            return Err(Error::InvalidRequest(
                "rate_limit_per_second must be greater than 0".into(),
            ));
        }
        if self.rpc.rate_limit_burst == 0 {
            return Err(Error::InvalidRequest(
                "rate_limit_burst must be greater than 0".into(),
            ));
        }
        if self.auth.enabled && self.auth.api_key.is_none() {
            return Err(Error::InvalidRequest(
                "auth.enabled is true but no api_key configured".into(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert!(config.security.allowed_paths.is_empty());
        assert!(config.security.block_symlinks);
        assert!(!config.logging.audit_enabled);
    }

    #[test]
    fn test_config_load_none_uses_defaults() {
        let config = Config::load(None).unwrap();
        assert!(config.security.allowed_paths.is_empty());
        assert!(config.security.block_symlinks);
    }

    #[test]
    fn test_config_load_valid_toml() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[security]
allowed_paths = ["/data", "/workspace"]
block_symlinks = false

[logging]
format = "json"
audit_enabled = true
"#
        )
        .unwrap();

        let config = Config::load(Some(file.path())).unwrap();
        assert_eq!(config.security.allowed_paths.len(), 2);
        assert!(!config.security.block_symlinks);
        assert!(config.logging.audit_enabled);
        assert!(matches!(config.logging.format, LogFormat::Json));
    }

    #[test]
    fn test_config_load_invalid_toml() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "not valid toml {{{{").unwrap();

        let result = Config::load(Some(file.path()));
        assert!(result.is_err());
    }

    #[test]
    fn test_config_load_missing_file() {
        let result = Config::load(Some(Path::new("/nonexistent/config.toml")));
        assert!(result.is_err());
    }

    #[test]
    fn test_env_var_override_allowed_paths() {
        env::set_var("BQ_RUNNER_ALLOWED_PATHS", "/tmp:/var");
        let config = Config::load(None).unwrap();
        env::remove_var("BQ_RUNNER_ALLOWED_PATHS");

        assert_eq!(config.security.allowed_paths.len(), 2);
        assert_eq!(config.security.allowed_paths[0], PathBuf::from("/tmp"));
        assert_eq!(config.security.allowed_paths[1], PathBuf::from("/var"));
    }

    #[test]
    fn test_env_var_override_audit_logging() {
        env::set_var("BQ_RUNNER_AUDIT_LOGGING", "true");
        let config = Config::load(None).unwrap();
        env::remove_var("BQ_RUNNER_AUDIT_LOGGING");

        assert!(config.logging.audit_enabled);
    }

    #[test]
    fn test_env_var_override_log_format() {
        env::set_var("BQ_RUNNER_LOG_FORMAT", "json");
        let config = Config::load(None).unwrap();
        env::remove_var("BQ_RUNNER_LOG_FORMAT");

        assert!(matches!(config.logging.format, LogFormat::Json));
    }

    #[test]
    fn test_env_var_override_block_symlinks() {
        env::set_var("BQ_RUNNER_BLOCK_SYMLINKS", "false");
        let config = Config::load(None).unwrap();
        env::remove_var("BQ_RUNNER_BLOCK_SYMLINKS");

        assert!(!config.security.block_symlinks);
    }

    #[test]
    fn test_security_config_default() {
        let sc = SecurityConfig::default();
        assert!(sc.allowed_paths.is_empty());
        assert!(sc.block_symlinks);
    }

    #[test]
    fn test_logging_config_default() {
        let lc = LoggingConfig::default();
        assert!(matches!(lc.format, LogFormat::Text));
        assert!(!lc.audit_enabled);
    }

    #[test]
    fn test_log_format_default() {
        let lf = LogFormat::default();
        assert!(matches!(lf, LogFormat::Text));
    }

    #[test]
    fn test_config_validate_empty_paths_warns() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rpc_config_default() {
        let rc = RpcConfig::default();
        assert_eq!(rc.request_timeout_secs, 300);
        assert_eq!(rc.rate_limit_per_second, 100);
        assert_eq!(rc.rate_limit_burst, 200);
        assert_eq!(rc.max_sql_length, 1_048_576);
        assert_eq!(rc.max_rows_per_insert, 10_000);
        assert_eq!(rc.max_ws_message_size, 4 * 1024 * 1024);
    }

    #[test]
    fn test_env_var_override_request_timeout() {
        env::set_var("BQ_RUNNER_REQUEST_TIMEOUT_SECS", "600");
        let config = Config::load(None).unwrap();
        env::remove_var("BQ_RUNNER_REQUEST_TIMEOUT_SECS");

        assert_eq!(config.rpc.request_timeout_secs, 600);
    }

    #[test]
    fn test_env_var_override_rate_limit() {
        env::set_var("BQ_RUNNER_RATE_LIMIT_PER_SECOND", "50");
        env::set_var("BQ_RUNNER_RATE_LIMIT_BURST", "100");
        let config = Config::load(None).unwrap();
        env::remove_var("BQ_RUNNER_RATE_LIMIT_PER_SECOND");
        env::remove_var("BQ_RUNNER_RATE_LIMIT_BURST");

        assert_eq!(config.rpc.rate_limit_per_second, 50);
        assert_eq!(config.rpc.rate_limit_burst, 100);
    }

    #[test]
    fn test_config_load_with_rpc_section() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rpc]
request_timeout_secs = 120
rate_limit_per_second = 50
rate_limit_burst = 100
"#
        )
        .unwrap();

        let config = Config::load(Some(file.path())).unwrap();
        assert_eq!(config.rpc.request_timeout_secs, 120);
        assert_eq!(config.rpc.rate_limit_per_second, 50);
        assert_eq!(config.rpc.rate_limit_burst, 100);
    }

    #[test]
    fn test_config_validate_zero_timeout_fails() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rpc]
request_timeout_secs = 0
"#
        )
        .unwrap();

        let result = Config::load(Some(file.path()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("request_timeout_secs must be greater than 0"));
    }

    #[test]
    fn test_config_validate_zero_rate_limit_fails() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rpc]
rate_limit_per_second = 0
"#
        )
        .unwrap();

        let result = Config::load(Some(file.path()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("rate_limit_per_second must be greater than 0"));
    }

    #[test]
    fn test_config_validate_zero_burst_fails() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[rpc]
rate_limit_burst = 0
"#
        )
        .unwrap();

        let result = Config::load(Some(file.path()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("rate_limit_burst must be greater than 0"));
    }
}
