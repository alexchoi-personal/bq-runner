#![allow(dead_code, unused_imports)]

pub mod converters;
pub mod error;
pub mod executor;
pub mod loader;
pub mod rpc;
pub mod session;
pub mod utils;

pub use converters::json_to_sql_value;
pub use error::{Error, Result};
pub use executor::{ColumnDef, ColumnInfo, Executor, ExecutorMode, QueryResult};
pub use loader::{
    discover_files, discover_parquet_files, discover_sql_files, DiscoveredFiles, FileLoader,
    LoadedFile, ParquetFile, SqlFile, SqlLoader,
};
pub use session::SessionManager;
