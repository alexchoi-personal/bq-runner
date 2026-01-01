use std::fs;
use std::path::{Path, PathBuf};

use glob::glob;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct LoadedFile {
    pub name: String,
    pub content: String,
    pub path: PathBuf,
}

pub type SqlFile = LoadedFile;

pub struct FileLoader;

impl FileLoader {
    pub fn load_dir(path: impl AsRef<Path>, extension: &str) -> Result<Vec<LoadedFile>> {
        let pattern = path.as_ref().join(format!("**/*.{}", extension));
        let pattern_str = pattern.to_string_lossy();

        let files: Vec<PathBuf> = glob(&pattern_str)
            .map_err(|e| Error::Loader(format!("Invalid glob pattern: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        files
            .into_iter()
            .map(|file_path| Self::load_file(&file_path))
            .collect()
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<LoadedFile> {
        let path = path.as_ref();

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::Loader(format!("Invalid filename: {}", path.display())))?
            .to_string();

        let content = fs::read_to_string(path)
            .map_err(|e| Error::Loader(format!("Failed to read {}: {}", path.display(), e)))?;

        Ok(LoadedFile {
            name,
            content,
            path: path.to_path_buf(),
        })
    }
}

pub struct SqlLoader;

impl SqlLoader {
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Vec<SqlFile>> {
        FileLoader::load_dir(path, "sql")
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<SqlFile> {
        FileLoader::load_file(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_file_success() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.sql");
        fs::write(&file_path, "SELECT * FROM table").unwrap();

        let loaded = FileLoader::load_file(&file_path).unwrap();
        assert_eq!(loaded.name, "test");
        assert_eq!(loaded.content, "SELECT * FROM table");
        assert_eq!(loaded.path, file_path);
    }

    #[test]
    fn test_load_file_not_found() {
        let result = FileLoader::load_file("/nonexistent/path/file.sql");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::Loader(msg) => assert!(msg.contains("Failed to read")),
            _ => panic!("Expected Loader error"),
        }
    }

    #[test]
    fn test_load_file_invalid_filename() {
        let result = FileLoader::load_file("/");
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::Loader(msg) => assert!(msg.contains("Invalid filename")),
            _ => panic!("Expected Loader error"),
        }
    }

    #[test]
    fn test_load_dir_success() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join("query1.sql"), "SELECT 1").unwrap();
        fs::write(temp_dir.path().join("query2.sql"), "SELECT 2").unwrap();
        fs::write(temp_dir.path().join("other.txt"), "not sql").unwrap();

        let files = FileLoader::load_dir(temp_dir.path(), "sql").unwrap();
        assert_eq!(files.len(), 2);

        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"query1"));
        assert!(names.contains(&"query2"));
    }

    #[test]
    fn test_load_dir_empty() {
        let temp_dir = TempDir::new().unwrap();
        let files = FileLoader::load_dir(temp_dir.path(), "sql").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_load_dir_nested() {
        let temp_dir = TempDir::new().unwrap();
        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).unwrap();
        fs::write(temp_dir.path().join("root.sql"), "SELECT root").unwrap();
        fs::write(sub_dir.join("nested.sql"), "SELECT nested").unwrap();

        let files = FileLoader::load_dir(temp_dir.path(), "sql").unwrap();
        assert_eq!(files.len(), 2);

        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"root"));
        assert!(names.contains(&"nested"));
    }

    #[test]
    fn test_sql_loader_load_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("query.sql");
        fs::write(&file_path, "SELECT * FROM users").unwrap();

        let loaded = SqlLoader::load_file(&file_path).unwrap();
        assert_eq!(loaded.name, "query");
        assert_eq!(loaded.content, "SELECT * FROM users");
    }

    #[test]
    fn test_sql_loader_load_dir() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join("a.sql"), "SELECT a").unwrap();
        fs::write(temp_dir.path().join("b.sql"), "SELECT b").unwrap();

        let files = SqlLoader::load_dir(temp_dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_loaded_file_clone() {
        let file = LoadedFile {
            name: "test".to_string(),
            content: "content".to_string(),
            path: PathBuf::from("/test/path.sql"),
        };
        let cloned = file.clone();
        assert_eq!(cloned.name, file.name);
        assert_eq!(cloned.content, file.content);
        assert_eq!(cloned.path, file.path);
    }

    #[test]
    fn test_loaded_file_debug() {
        let file = LoadedFile {
            name: "test".to_string(),
            content: "content".to_string(),
            path: PathBuf::from("/test/path.sql"),
        };
        let debug_str = format!("{:?}", file);
        assert!(debug_str.contains("LoadedFile"));
        assert!(debug_str.contains("test"));
    }
}
