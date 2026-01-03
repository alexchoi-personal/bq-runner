use serde::Deserialize;
use std::path::{Path, PathBuf};
use tracing::warn;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    Text,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Text
    }
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
pub struct Config {
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            security: SecurityConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut config = match path {
            Some(p) => {
                let contents = std::fs::read_to_string(p)
                    .map_err(|e| Error::InvalidRequest(format!("Cannot read config file: {}", e)))?;
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
        writeln!(file, r#"
[security]
allowed_paths = ["/data", "/workspace"]
block_symlinks = false

[logging]
format = "json"
audit_enabled = true
"#).unwrap();

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
}
