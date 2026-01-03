use std::io::Read;
use std::path::{Path, PathBuf};

#[cfg(test)]
use std::fs;

use glob::glob;

use crate::config::SecurityConfig;
use crate::domain::ColumnDef;
use crate::error::{Error, Result};
use crate::validation::{open_file_secure, validate_path, validate_path_async};

#[derive(Debug, Clone)]
pub struct LoadedFile {
    pub name: String,
    pub content: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SqlFile {
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub path: String,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct ParquetFile {
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub path: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct DiscoveredFiles {
    pub sql_files: Vec<SqlFile>,
    pub parquet_files: Vec<ParquetFile>,
}

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

        let mut file = open_file_secure(path)
            .map_err(|e| Error::Loader(format!("Failed to open {}: {}", path.display(), e)))?;
        let mut content = String::new();
        file.read_to_string(&mut content)
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
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Vec<LoadedFile>> {
        FileLoader::load_dir(path, "sql")
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<LoadedFile> {
        FileLoader::load_file(path)
    }
}

pub fn discover_files_secure(root_path: &str, config: &SecurityConfig) -> Result<DiscoveredFiles> {
    let validated_root = validate_path(root_path, config)?;
    discover_files_internal(&validated_root)
}

pub async fn discover_files_secure_async(
    root_path: &str,
    config: &SecurityConfig,
) -> Result<DiscoveredFiles> {
    let validated_root = validate_path_async(root_path, config).await?;
    tokio::task::spawn_blocking(move || discover_files_internal(&validated_root))
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
}

#[cfg(test)]
fn discover_files(root_path: &str) -> Result<DiscoveredFiles> {
    let root = Path::new(root_path);
    discover_files_internal(root)
}

fn discover_files_internal(root: &Path) -> Result<DiscoveredFiles> {
    if !root.is_dir() {
        return Err(Error::Executor(format!(
            "Root path is not a directory: {}",
            root.display()
        )));
    }

    let mut sql_files = Vec::new();
    let mut parquet_files = Vec::new();

    for project_entry in read_dir(root)? {
        let project_path = project_entry.path();
        if !project_path.is_dir() {
            continue;
        }
        let project_name = project_entry.file_name().to_string_lossy().to_string();

        for dataset_entry in read_dir(&project_path)? {
            let dataset_path = dataset_entry.path();
            if !dataset_path.is_dir() {
                continue;
            }
            let dataset_name = dataset_entry.file_name().to_string_lossy().to_string();

            for table_entry in read_dir(&dataset_path)? {
                let table_path = table_entry.path();
                if !table_path.is_file() {
                    continue;
                }
                let file_name = table_entry.file_name().to_string_lossy().to_string();

                if file_name.ends_with(".sql") {
                    let table_name = file_name.trim_end_matches(".sql").to_string();
                    let sql = read_file(&table_path)?;

                    sql_files.push(SqlFile {
                        project: project_name.clone(),
                        dataset: dataset_name.clone(),
                        table: table_name,
                        path: table_path.to_string_lossy().to_string(),
                        sql,
                    });
                } else if file_name.ends_with(".parquet") {
                    let table_name = file_name.trim_end_matches(".parquet").to_string();
                    let schema = load_schema(&dataset_path, &table_name)?;

                    parquet_files.push(ParquetFile {
                        project: project_name.clone(),
                        dataset: dataset_name.clone(),
                        table: table_name,
                        path: table_path.to_string_lossy().to_string(),
                        schema,
                    });
                }
            }
        }
    }

    Ok(DiscoveredFiles {
        sql_files,
        parquet_files,
    })
}

#[cfg(test)]
fn discover_sql_files(root_path: &str) -> Result<Vec<SqlFile>> {
    let discovered = discover_files(root_path)?;
    Ok(discovered.sql_files)
}

pub fn discover_sql_files_secure(root_path: &str, config: &SecurityConfig) -> Result<Vec<SqlFile>> {
    let discovered = discover_files_secure(root_path, config)?;
    Ok(discovered.sql_files)
}

pub async fn discover_sql_files_secure_async(
    root_path: &str,
    config: &SecurityConfig,
) -> Result<Vec<SqlFile>> {
    let discovered = discover_files_secure_async(root_path, config).await?;
    Ok(discovered.sql_files)
}

#[cfg(test)]
fn discover_parquet_files(root_path: &str) -> Result<Vec<ParquetFile>> {
    let discovered = discover_files(root_path)?;
    Ok(discovered.parquet_files)
}

pub fn discover_parquet_files_secure(
    root_path: &str,
    config: &SecurityConfig,
) -> Result<Vec<ParquetFile>> {
    let discovered = discover_files_secure(root_path, config)?;
    Ok(discovered.parquet_files)
}

pub async fn discover_parquet_files_secure_async(
    root_path: &str,
    config: &SecurityConfig,
) -> Result<Vec<ParquetFile>> {
    let discovered = discover_files_secure_async(root_path, config).await?;
    Ok(discovered.parquet_files)
}

fn read_dir(path: &Path) -> Result<Vec<std::fs::DirEntry>> {
    std::fs::read_dir(path)
        .map_err(|e| Error::Executor(format!("Failed to read directory: {}", e)))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Executor(format!("Failed to read entry: {}", e)))
}

fn read_file(path: &Path) -> Result<String> {
    let mut file = open_file_secure(path)
        .map_err(|e| Error::Executor(format!("Failed to open file {}: {}", path.display(), e)))?;
    let mut content = String::new();
    file.read_to_string(&mut content)
        .map_err(|e| Error::Executor(format!("Failed to read file {}: {}", path.display(), e)))?;
    Ok(content)
}

fn load_schema(dataset_path: &Path, table_name: &str) -> Result<Vec<ColumnDef>> {
    let schema_path = dataset_path.join(format!("{}.schema.json", table_name));
    if !schema_path.exists() {
        return Err(Error::Executor(format!(
            "Schema file not found: {}",
            schema_path.display()
        )));
    }

    let schema_content = read_file(&schema_path)?;
    serde_json::from_str(&schema_content)
        .map_err(|e| Error::Executor(format!("Failed to parse schema: {}", e)))
}

impl SqlFile {
    pub fn full_table_name(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
    }
}

impl ParquetFile {
    pub fn full_table_name(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
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
            Error::Loader(msg) => assert!(msg.contains("Failed to open")),
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

    fn create_test_structure(temp_dir: &TempDir) -> std::path::PathBuf {
        let root = temp_dir.path();
        let project_path = root.join("my_project");
        let dataset_path = project_path.join("my_dataset");
        fs::create_dir_all(&dataset_path).unwrap();
        dataset_path
    }

    #[test]
    fn test_discover_files_not_a_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("not_a_dir.txt");
        fs::write(&file_path, "content").unwrap();
        let err = discover_files(file_path.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[test]
    fn test_discover_files_nonexistent() {
        let err = discover_files("/nonexistent/path").unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[test]
    fn test_discover_files_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let result = discover_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(result.sql_files.is_empty());
        assert!(result.parquet_files.is_empty());
    }

    #[test]
    fn test_discover_sql_files() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        let sql_path = dataset_path.join("my_table.sql");
        fs::write(&sql_path, "SELECT 1").unwrap();
        let result = discover_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(result.sql_files.len(), 1);
        assert_eq!(result.sql_files[0].project, "my_project");
        assert_eq!(result.sql_files[0].dataset, "my_dataset");
        assert_eq!(result.sql_files[0].table, "my_table");
        assert_eq!(result.sql_files[0].sql, "SELECT 1");
    }

    #[test]
    fn test_discover_parquet_files_with_schema() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        let parquet_path = dataset_path.join("my_table.parquet");
        fs::write(&parquet_path, "dummy parquet data").unwrap();
        let schema_path = dataset_path.join("my_table.schema.json");
        fs::write(&schema_path, r#"[{"name": "id", "type": "INT64"}]"#).unwrap();
        let result = discover_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(result.parquet_files.len(), 1);
        assert_eq!(result.parquet_files[0].project, "my_project");
        assert_eq!(result.parquet_files[0].dataset, "my_dataset");
        assert_eq!(result.parquet_files[0].table, "my_table");
        assert_eq!(result.parquet_files[0].schema.len(), 1);
        assert_eq!(result.parquet_files[0].schema[0].name, "id");
    }

    #[test]
    fn test_discover_parquet_files_missing_schema() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        let parquet_path = dataset_path.join("no_schema.parquet");
        fs::write(&parquet_path, "dummy parquet data").unwrap();
        let err = discover_files(temp_dir.path().to_str().unwrap()).unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[test]
    fn test_discover_parquet_files_invalid_schema() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        let parquet_path = dataset_path.join("bad_schema.parquet");
        fs::write(&parquet_path, "dummy parquet data").unwrap();
        let schema_path = dataset_path.join("bad_schema.schema.json");
        fs::write(&schema_path, "not valid json").unwrap();
        let err = discover_files(temp_dir.path().to_str().unwrap()).unwrap_err();
        assert!(matches!(err, Error::Executor(_)));
    }

    #[test]
    fn test_discover_files_skips_non_directories_at_project_level() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        fs::write(root.join("file.txt"), "content").unwrap();
        let project_path = root.join("project");
        let dataset_path = project_path.join("dataset");
        fs::create_dir_all(&dataset_path).unwrap();
        fs::write(dataset_path.join("table.sql"), "SELECT 1").unwrap();
        let result = discover_files(root.to_str().unwrap()).unwrap();
        assert_eq!(result.sql_files.len(), 1);
    }

    #[test]
    fn test_discover_files_skips_non_directories_at_dataset_level() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        let project_path = root.join("project");
        fs::create_dir_all(&project_path).unwrap();
        fs::write(project_path.join("file.txt"), "content").unwrap();
        let dataset_path = project_path.join("dataset");
        fs::create_dir_all(&dataset_path).unwrap();
        fs::write(dataset_path.join("table.sql"), "SELECT 1").unwrap();
        let result = discover_files(root.to_str().unwrap()).unwrap();
        assert_eq!(result.sql_files.len(), 1);
    }

    #[test]
    fn test_discover_files_skips_subdirectories_at_table_level() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        fs::create_dir(dataset_path.join("subdir")).unwrap();
        fs::write(dataset_path.join("table.sql"), "SELECT 1").unwrap();
        let result = discover_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(result.sql_files.len(), 1);
    }

    #[test]
    fn test_discover_files_ignores_other_file_types() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        fs::write(dataset_path.join("readme.md"), "# Readme").unwrap();
        fs::write(dataset_path.join("data.csv"), "a,b,c").unwrap();
        fs::write(dataset_path.join("config.json"), "{}").unwrap();
        let result = discover_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(result.sql_files.is_empty());
        assert!(result.parquet_files.is_empty());
    }

    #[test]
    fn test_discover_sql_files_wrapper() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        fs::write(dataset_path.join("t.sql"), "SELECT 1").unwrap();
        let parquet_path = dataset_path.join("p.parquet");
        fs::write(&parquet_path, "dummy").unwrap();
        fs::write(
            dataset_path.join("p.schema.json"),
            r#"[{"name": "id", "type": "INT64"}]"#,
        )
        .unwrap();
        let sql_files = discover_sql_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(sql_files.len(), 1);
        assert_eq!(sql_files[0].table, "t");
    }

    #[test]
    fn test_discover_parquet_files_wrapper() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        fs::write(dataset_path.join("t.sql"), "SELECT 1").unwrap();
        let parquet_path = dataset_path.join("p.parquet");
        fs::write(&parquet_path, "dummy").unwrap();
        fs::write(
            dataset_path.join("p.schema.json"),
            r#"[{"name": "id", "type": "INT64"}]"#,
        )
        .unwrap();
        let parquet_files = discover_parquet_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(parquet_files.len(), 1);
        assert_eq!(parquet_files[0].table, "p");
    }

    #[test]
    fn test_sql_file_full_table_name() {
        let sql_file = SqlFile {
            project: "proj".to_string(),
            dataset: "ds".to_string(),
            table: "tbl".to_string(),
            path: "/path/to/file.sql".to_string(),
            sql: "SELECT 1".to_string(),
        };
        assert_eq!(sql_file.full_table_name(), "proj.ds.tbl");
    }

    #[test]
    fn test_parquet_file_full_table_name() {
        let parquet_file = ParquetFile {
            project: "proj".to_string(),
            dataset: "ds".to_string(),
            table: "tbl".to_string(),
            path: "/path/to/file.parquet".to_string(),
            schema: vec![],
        };
        assert_eq!(parquet_file.full_table_name(), "proj.ds.tbl");
    }

    #[test]
    fn test_sql_file_clone() {
        let sql_file = SqlFile {
            project: "p".to_string(),
            dataset: "d".to_string(),
            table: "t".to_string(),
            path: "/path".to_string(),
            sql: "SELECT 1".to_string(),
        };
        let cloned = sql_file.clone();
        assert_eq!(cloned.project, sql_file.project);
        assert_eq!(cloned.sql, sql_file.sql);
    }

    #[test]
    fn test_parquet_file_clone() {
        let parquet_file = ParquetFile {
            project: "p".to_string(),
            dataset: "d".to_string(),
            table: "t".to_string(),
            path: "/path".to_string(),
            schema: vec![ColumnDef::int64("id")],
        };
        let cloned = parquet_file.clone();
        assert_eq!(cloned.project, parquet_file.project);
        assert_eq!(cloned.schema.len(), 1);
    }

    #[test]
    fn test_discovered_files_clone() {
        let discovered = DiscoveredFiles {
            sql_files: vec![SqlFile {
                project: "p".to_string(),
                dataset: "d".to_string(),
                table: "t".to_string(),
                path: "/path".to_string(),
                sql: "SELECT 1".to_string(),
            }],
            parquet_files: vec![],
        };
        let cloned = discovered.clone();
        assert_eq!(cloned.sql_files.len(), 1);
        assert!(cloned.parquet_files.is_empty());
    }

    #[test]
    fn test_sql_file_debug() {
        let sql_file = SqlFile {
            project: "p".to_string(),
            dataset: "d".to_string(),
            table: "t".to_string(),
            path: "/path".to_string(),
            sql: "SELECT 1".to_string(),
        };
        let debug_str = format!("{:?}", sql_file);
        assert!(debug_str.contains("SqlFile"));
        assert!(debug_str.contains("project"));
    }

    #[test]
    fn test_parquet_file_debug() {
        let parquet_file = ParquetFile {
            project: "p".to_string(),
            dataset: "d".to_string(),
            table: "t".to_string(),
            path: "/path".to_string(),
            schema: vec![],
        };
        let debug_str = format!("{:?}", parquet_file);
        assert!(debug_str.contains("ParquetFile"));
    }

    #[test]
    fn test_discovered_files_debug() {
        let discovered = DiscoveredFiles {
            sql_files: vec![],
            parquet_files: vec![],
        };
        let debug_str = format!("{:?}", discovered);
        assert!(debug_str.contains("DiscoveredFiles"));
    }

    #[test]
    fn test_discover_multiple_projects_and_datasets() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        for project in ["proj1", "proj2"] {
            for dataset in ["ds1", "ds2"] {
                let path = root.join(project).join(dataset);
                fs::create_dir_all(&path).unwrap();
                fs::write(
                    path.join("table.sql"),
                    format!("SELECT * FROM {}.{}", project, dataset),
                )
                .unwrap();
            }
        }
        let result = discover_files(root.to_str().unwrap()).unwrap();
        assert_eq!(result.sql_files.len(), 4);
    }

    #[test]
    fn test_discover_mixed_sql_and_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_structure(&temp_dir);
        fs::write(dataset_path.join("computed.sql"), "SELECT 1").unwrap();
        fs::write(dataset_path.join("source.parquet"), "dummy").unwrap();
        fs::write(
            dataset_path.join("source.schema.json"),
            r#"[{"name": "x", "type": "STRING"}]"#,
        )
        .unwrap();
        let result = discover_files(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(result.sql_files.len(), 1);
        assert_eq!(result.parquet_files.len(), 1);
        assert_eq!(result.sql_files[0].table, "computed");
        assert_eq!(result.parquet_files[0].table, "source");
    }
}
