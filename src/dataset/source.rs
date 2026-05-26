use crate::error::PqError;
use crate::Result;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const MAX_GLOB_FILES: usize = 10_000;

#[derive(Clone, Debug)]
pub struct Dataset {
    paths: Vec<PathBuf>,
}

impl Dataset {
    pub fn from_inputs(inputs: Vec<PathBuf>) -> Result<Self> {
        if inputs.is_empty() {
            return Err(PqError::NoInputFiles);
        }

        let mut paths = Vec::new();
        let mut seen_paths = BTreeSet::new();
        let mut seen_from_globs = BTreeSet::new();

        for input in inputs {
            if is_glob_pattern(&input) {
                expand_glob_input(&input, &mut paths, &mut seen_paths, &mut seen_from_globs)?;
            } else {
                validate_file_path(&input)?;
                if !seen_from_globs.contains(&input) {
                    seen_paths.insert(input.clone());
                    paths.push(input);
                }
            }
        }

        if paths.is_empty() {
            return Err(PqError::NoInputFiles);
        }

        Ok(Self { paths })
    }

    pub fn paths(&self) -> impl ExactSizeIterator<Item = &Path> {
        self.paths.iter().map(PathBuf::as_path)
    }

    pub fn is_multi_source(&self) -> bool {
        self.paths.len() > 1
    }
}

fn expand_glob_input(
    input: &Path,
    paths: &mut Vec<PathBuf>,
    seen_paths: &mut BTreeSet<PathBuf>,
    seen_from_globs: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let pattern = input.to_string_lossy().into_owned();
    let mut matches = Vec::new();

    for entry in
        glob::glob(&pattern).map_err(|error| PqError::invalid_glob_pattern(&pattern, error))?
    {
        let path = entry.map_err(|error| PqError::from_read(error.path(), error.error()))?;
        validate_file_path(&path)?;
        matches.push(path);

        if matches.len() > MAX_GLOB_FILES {
            return Err(PqError::TooManyFilesMatched {
                pattern,
                max_matches: MAX_GLOB_FILES,
            });
        }
    }

    if matches.is_empty() {
        return Err(PqError::NoFilesMatched { pattern });
    }

    matches.sort();
    for path in matches {
        if seen_paths.insert(path.clone()) {
            seen_from_globs.insert(path.clone());
            paths.push(path);
        }
    }
    Ok(())
}

fn is_glob_pattern(path: &Path) -> bool {
    let path = path.to_string_lossy();
    path.contains('*') || path.contains('?') || path.contains('[')
}

fn validate_file_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(PqError::file_not_found(path));
    }

    if path.is_dir() {
        return Err(PqError::is_directory(path));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(PqError::output_error)?
            .as_nanos();
        let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pq_dataset_{unique}_{counter}"));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[test]
    fn deduplicates_overlapping_explicit_and_glob_inputs() -> Result<()> {
        let dir = temp_dir()?;
        let file = dir.join("sample.parquet");
        fs::write(&file, b"PAR1")?;
        let glob = dir.join("*.parquet");

        let dataset = Dataset::from_inputs(vec![file.clone(), glob])?;
        let paths = dataset.paths().collect::<Vec<_>>();

        assert_eq!(paths, vec![file.as_path()]);

        fs::remove_file(file)?;
        fs::remove_dir(dir)?;
        Ok(())
    }

    #[test]
    fn deduplicates_overlapping_glob_and_explicit_inputs() -> Result<()> {
        let dir = temp_dir()?;
        let file = dir.join("sample.parquet");
        fs::write(&file, b"PAR1")?;
        let glob = dir.join("*.parquet");

        let dataset = Dataset::from_inputs(vec![glob, file.clone()])?;
        let paths = dataset.paths().collect::<Vec<_>>();

        assert_eq!(paths, vec![file.as_path()]);

        fs::remove_file(file)?;
        fs::remove_dir(dir)?;
        Ok(())
    }

    #[test]
    fn preserves_repeated_explicit_inputs() -> Result<()> {
        let dir = temp_dir()?;
        let file = dir.join("sample.parquet");
        fs::write(&file, b"PAR1")?;

        let dataset = Dataset::from_inputs(vec![file.clone(), file.clone()])?;
        let paths = dataset.paths().collect::<Vec<_>>();

        assert_eq!(paths, vec![file.as_path(), file.as_path()]);

        fs::remove_file(file)?;
        fs::remove_dir(dir)?;
        Ok(())
    }
}
