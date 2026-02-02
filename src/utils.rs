//! Shared utilities for file reading and glob expansion

use anyhow::{bail, Result};
use std::path::PathBuf;

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
                .collect();

            if matches.is_empty() {
                bail!("No files matched pattern: {path_str}");
            }

            expanded.extend(matches);
        } else {
            if !path.exists() {
                bail!("File not found: {path_str}");
            }
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
