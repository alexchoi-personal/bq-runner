use crate::error::{Error, Result};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

static TABLE_NAME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$").unwrap()
});

pub fn validate_table_name(name: &str) -> Result<()> {
    if name.len() > 128 || !TABLE_NAME_REGEX.is_match(name) {
        return Err(Error::InvalidRequest(format!("Invalid table name: {}", name)));
    }
    Ok(())
}

pub fn quote_identifier(name: &str) -> String {
    name.replace('`', "``")
}

#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    pub allowed_paths: Vec<PathBuf>,
    pub block_symlinks: bool,
}

pub fn validate_path(path: &str, config: &SecurityConfig) -> Result<PathBuf> {
    let path = Path::new(path);

    if path.to_string_lossy().contains('\0') {
        return Err(Error::InvalidRequest("Path contains null byte".into()));
    }

    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(Error::InvalidRequest("Path traversal not allowed".into()));
        }
    }

    if config.block_symlinks {
        match std::fs::symlink_metadata(path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(Error::InvalidRequest("Symlinks not allowed".into()));
            }
            Err(e) => {
                return Err(Error::InvalidRequest(format!("Cannot stat path: {}", e)));
            }
            _ => {}
        }
    }

    let canonical = std::fs::canonicalize(path)
        .map_err(|e| Error::InvalidRequest(format!("Invalid path: {}", e)))?;

    if config.allowed_paths.is_empty() {
        return Err(Error::InvalidRequest("Path access denied".into()));
    }

    for allowed in &config.allowed_paths {
        if let Ok(allowed_canonical) = std::fs::canonicalize(allowed) {
            if canonical.starts_with(&allowed_canonical) {
                return Ok(canonical);
            }
        }
    }

    Err(Error::InvalidRequest("Path access denied".into()))
}

#[cfg(unix)]
pub fn open_file_secure(path: &Path) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| Error::InvalidRequest(format!("Cannot open file: {}", e)))
}

#[cfg(not(unix))]
pub fn open_file_secure(path: &Path) -> Result<std::fs::File> {
    std::fs::File::open(path)
        .map_err(|e| Error::InvalidRequest(format!("Cannot open file: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_table_name_simple() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("my_table").is_ok());
        assert!(validate_table_name("_private").is_ok());
    }

    #[test]
    fn test_validate_table_name_qualified() {
        assert!(validate_table_name("project.dataset.table").is_ok());
        assert!(validate_table_name("my_project.my_dataset.my_table").is_ok());
    }

    #[test]
    fn test_validate_table_name_invalid_start() {
        assert!(validate_table_name("123table").is_err());
        assert!(validate_table_name("-table").is_err());
    }

    #[test]
    fn test_validate_table_name_invalid_chars() {
        assert!(validate_table_name("table-name").is_err());
        assert!(validate_table_name("table name").is_err());
        assert!(validate_table_name("table@name").is_err());
    }

    #[test]
    fn test_validate_table_name_too_long() {
        let long_name = "a".repeat(129);
        assert!(validate_table_name(&long_name).is_err());
    }

    #[test]
    fn test_validate_table_name_max_length() {
        let max_name = "a".repeat(128);
        assert!(validate_table_name(&max_name).is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn test_quote_identifier_no_backticks() {
        assert_eq!(quote_identifier("my_table"), "my_table");
    }

    #[test]
    fn test_quote_identifier_with_backticks() {
        assert_eq!(quote_identifier("my`table"), "my``table");
        assert_eq!(quote_identifier("`table`"), "``table``");
    }

    #[test]
    fn test_quote_identifier_multiple_backticks() {
        assert_eq!(quote_identifier("a`b`c"), "a``b``c");
    }

    #[test]
    fn test_validate_path_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "test").unwrap();

        let config = SecurityConfig {
            allowed_paths: vec![dir.path().to_path_buf()],
            block_symlinks: true,
        };

        let result = validate_path(file_path.to_str().unwrap(), &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_disallowed() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "test").unwrap();

        let config = SecurityConfig {
            allowed_paths: vec![PathBuf::from("/some/other/path")],
            block_symlinks: true,
        };

        let result = validate_path(file_path.to_str().unwrap(), &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Path access denied"));
    }

    #[test]
    fn test_validate_path_empty_allowed_blocks_all() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "test").unwrap();

        let config = SecurityConfig {
            allowed_paths: vec![],
            block_symlinks: true,
        };

        let result = validate_path(file_path.to_str().unwrap(), &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_path_traversal_blocked() {
        let config = SecurityConfig {
            allowed_paths: vec![PathBuf::from("/tmp")],
            block_symlinks: true,
        };

        let result = validate_path("/tmp/../etc/passwd", &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Path traversal"));
    }

    #[test]
    fn test_validate_path_null_byte_blocked() {
        let config = SecurityConfig::default();

        let result = validate_path("/tmp/file\0.txt", &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null byte"));
    }

    #[test]
    #[cfg(unix)]
    fn test_validate_path_symlink_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.txt");
        let link = dir.path().join("link.txt");
        std::fs::write(&target, "test").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let config = SecurityConfig {
            allowed_paths: vec![dir.path().to_path_buf()],
            block_symlinks: true,
        };

        let result = validate_path(link.to_str().unwrap(), &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Symlinks not allowed"));
    }

    #[test]
    #[cfg(unix)]
    fn test_validate_path_symlink_allowed_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.txt");
        let link = dir.path().join("link.txt");
        std::fs::write(&target, "test").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let config = SecurityConfig {
            allowed_paths: vec![dir.path().to_path_buf()],
            block_symlinks: false,
        };

        let result = validate_path(link.to_str().unwrap(), &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_nonexistent() {
        let config = SecurityConfig {
            allowed_paths: vec![PathBuf::from("/tmp")],
            block_symlinks: true,
        };

        let result = validate_path("/tmp/nonexistent_file_12345.txt", &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_file_secure() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "test content").unwrap();

        let result = open_file_secure(&file_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_open_file_secure_nonexistent() {
        let result = open_file_secure(Path::new("/nonexistent/file.txt"));
        assert!(result.is_err());
    }

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();
        assert!(config.allowed_paths.is_empty());
        assert!(!config.block_symlinks);
    }
}
