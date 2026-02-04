//! Custom error types with user-friendly messages

use std::path::Path;
use thiserror::Error;

/// User-facing error with context
#[derive(Debug, Error)]
pub enum PqError {
    #[error("File not found: {path}")]
    FileNotFound { path: String },

    #[error("Not a valid Parquet file: {path}\n  {details}")]
    InvalidParquet { path: String, details: String },

    #[error("File appears corrupted: {path}\n  {details}")]
    CorruptedFile { path: String, details: String },

    #[error("Cannot read file: {path}\n  {details}")]
    ReadError { path: String, details: String },

    #[error("Cannot write file: {path}\n  {details}")]
    WriteError { path: String, details: String },

    #[error("No files matched pattern: {pattern}")]
    NoFilesMatched { pattern: String },

    #[error("Invalid SQL query: {details}")]
    InvalidSql { details: String },

    #[error("Query execution failed: {details}")]
    QueryFailed { details: String },

    #[error("Schema mismatch between files:\n  {file1}\n  {file2}\n  {details}")]
    SchemaMismatch {
        file1: String,
        file2: String,
        details: String,
    },

    #[error("Unsupported format: {format}\n  Supported formats: {supported}")]
    UnsupportedFormat { format: String, supported: String },

    #[error("Column not found: {column}\n  Available columns: {available}")]
    ColumnNotFound { column: String, available: String },

    #[error("Path is a directory, not a file: {path}")]
    IsDirectory { path: String },

    #[error("Empty file: {path}")]
    EmptyFile { path: String },

    #[error("{0}")]
    Other(String),
}

impl PqError {
    /// Create a file-not-found error with path context
    pub fn file_not_found(path: &Path) -> Self {
        Self::FileNotFound {
            path: path.display().to_string(),
        }
    }

    /// Create an invalid parquet error from a library error
    pub fn invalid_parquet(path: &Path, err: impl std::fmt::Display) -> Self {
        let details = err.to_string();
        // Simplify common error messages
        let details = simplify_parquet_error(&details);
        Self::InvalidParquet {
            path: path.display().to_string(),
            details,
        }
    }

    /// Create a corrupted file error
    pub fn corrupted(path: &Path, err: impl std::fmt::Display) -> Self {
        let details = err.to_string();
        let details = simplify_parquet_error(&details);
        Self::CorruptedFile {
            path: path.display().to_string(),
            details,
        }
    }

    /// Create a read error with path context
    pub fn read_error(path: &Path, err: impl std::fmt::Display) -> Self {
        Self::ReadError {
            path: path.display().to_string(),
            details: err.to_string(),
        }
    }

    /// Create a write error with path context
    pub fn write_error(path: &Path, err: impl std::fmt::Display) -> Self {
        Self::WriteError {
            path: path.display().to_string(),
            details: err.to_string(),
        }
    }

    /// Create an invalid SQL error
    pub fn invalid_sql(err: impl std::fmt::Display) -> Self {
        Self::InvalidSql {
            details: simplify_sql_error(&err.to_string()),
        }
    }

    /// Create a query execution error
    pub fn query_failed(err: impl std::fmt::Display) -> Self {
        Self::QueryFailed {
            details: simplify_sql_error(&err.to_string()),
        }
    }

    /// Create an "is directory" error
    pub fn is_directory(path: &Path) -> Self {
        Self::IsDirectory {
            path: path.display().to_string(),
        }
    }

    /// Create an empty file error
    pub fn empty_file(path: &Path) -> Self {
        Self::EmptyFile {
            path: path.display().to_string(),
        }
    }
}

/// Simplify parquet library error messages to be more user-friendly
fn simplify_parquet_error(msg: &str) -> String {
    // Handle common parquet error patterns
    if msg.contains("not a valid Parquet file") || msg.contains("Invalid Parquet file") {
        return "File does not have valid Parquet magic bytes".to_string();
    }

    if msg.contains("eof") || msg.contains("EOF") || msg.contains("unexpected end") {
        return "File is truncated or incomplete".to_string();
    }

    if msg.contains("Invalid thrift") || msg.contains("thrift") {
        return "File metadata is corrupted".to_string();
    }

    if msg.contains("out of spec") || msg.contains("out-of-spec") {
        return "File contains invalid or out-of-spec data".to_string();
    }

    // Return original if no simplification available
    msg.to_string()
}

/// Simplify SQL/DataFusion error messages
fn simplify_sql_error(msg: &str) -> String {
    // Strip Arrow/DataFusion internal prefixes
    let msg = msg
        .trim_start_matches("Arrow error: ")
        .trim_start_matches("External error: ")
        .trim_start_matches("Execution error: ");

    // Handle common patterns
    if msg.contains("table") && msg.contains("not found") {
        return msg.to_string();
    }

    if msg.contains("column") && msg.contains("not found") {
        return msg.to_string();
    }

    msg.to_string()
}

/// Extension trait for adding path context to Results
pub trait ResultExt<T> {
    /// Add path context to an error, converting it to a user-friendly message
    fn with_path_context(self, path: &Path) -> Result<T, PqError>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn with_path_context(self, path: &Path) -> Result<T, PqError> {
        self.map_err(|e| {
            let msg = e.to_string().to_lowercase();

            // Categorize the error based on message content
            if msg.contains("no such file")
                || msg.contains("not found")
                || msg.contains("does not exist")
            {
                PqError::file_not_found(path)
            } else if msg.contains("is a directory") {
                PqError::is_directory(path)
            } else if msg.contains("permission denied") {
                PqError::read_error(path, "Permission denied")
            } else if msg.contains("parquet") || msg.contains("magic") || msg.contains("thrift") {
                PqError::invalid_parquet(path, e)
            } else if msg.contains("eof") || msg.contains("truncat") || msg.contains("corrupt") {
                PqError::corrupted(path, e)
            } else {
                PqError::read_error(path, e)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PqError::file_not_found(Path::new("/tmp/missing.parquet"));
        assert!(err.to_string().contains("missing.parquet"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_simplify_parquet_error() {
        assert_eq!(
            simplify_parquet_error("not a valid Parquet file: missing magic"),
            "File does not have valid Parquet magic bytes"
        );
        assert_eq!(
            simplify_parquet_error("unexpected eof while reading"),
            "File is truncated or incomplete"
        );
    }
}
