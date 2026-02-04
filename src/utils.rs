//! Shared utilities for file reading and glob expansion

use crate::error::PqError;
use anyhow::{bail, Result};
use std::path::PathBuf;

/// Maximum number of files to process from a glob pattern
const MAX_GLOB_FILES: usize = 10_000;

/// Expand glob patterns in file paths
pub fn expand_globs(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut expanded = Vec::new();

    for path in paths {
        let path_str = path.to_string_lossy();

        // Check if path contains glob characters
        if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
            let matches: Vec<_> = glob::glob(&path_str)?
                .filter_map(Result::ok)
                .filter(|p| p.is_file())
                .take(MAX_GLOB_FILES + 1) // Take one extra to detect overflow
                .collect();

            if matches.is_empty() {
                return Err(PqError::NoFilesMatched {
                    pattern: path_str.to_string(),
                }
                .into());
            }

            if matches.len() > MAX_GLOB_FILES {
                bail!(
                    "Pattern '{path_str}' matched more than {MAX_GLOB_FILES} files. Use a more specific pattern."
                );
            }

            expanded.extend(matches);
        } else {
            // Validate the path before adding
            validate_file_path(path)?;
            expanded.push(path.clone());
        }
    }

    if expanded.is_empty() {
        bail!("No input files specified");
    }

    // Sort for consistent ordering
    expanded.sort();
    Ok(expanded)
}

/// Validate that a path exists and is a file (not a directory)
fn validate_file_path(path: &std::path::Path) -> Result<()> {
    if !path.exists() {
        return Err(PqError::file_not_found(path).into());
    }

    if path.is_dir() {
        return Err(PqError::is_directory(path).into());
    }

    Ok(())
}
