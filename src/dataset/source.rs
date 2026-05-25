use crate::error::PqError;
use crate::Result;
use std::path::{Path, PathBuf};

const MAX_GLOB_FILES: usize = 10_000;

#[derive(Clone, Debug)]
pub struct Dataset {
    sources: Vec<DatasetSource>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DatasetSource {
    path: PathBuf,
}

impl Dataset {
    pub fn from_inputs(inputs: Vec<PathBuf>) -> Result<Self> {
        if inputs.is_empty() {
            return Err(PqError::NoInputFiles.into());
        }

        let mut sources = Vec::new();

        for input in inputs {
            if is_glob_pattern(&input) {
                expand_glob_input(&input, &mut sources)?;
            } else {
                validate_file_path(&input)?;
                sources.push(DatasetSource { path: input });
            }
        }

        sources.sort();
        sources.dedup();

        if sources.is_empty() {
            return Err(PqError::NoInputFiles.into());
        }

        Ok(Self { sources })
    }

    pub fn from_source(source: DatasetSource) -> Self {
        Self {
            sources: vec![source],
        }
    }

    pub fn sources(&self) -> &[DatasetSource] {
        &self.sources
    }

    pub fn is_multi_source(&self) -> bool {
        self.sources.len() > 1
    }
}

impl DatasetSource {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn expand_glob_input(input: &Path, sources: &mut Vec<DatasetSource>) -> Result<()> {
    let pattern = input.to_string_lossy().into_owned();
    let mut matches = Vec::new();

    for entry in glob::glob(&pattern)? {
        let path = entry?;
        validate_file_path(&path)?;
        matches.push(DatasetSource { path });

        if matches.len() > MAX_GLOB_FILES {
            return Err(PqError::TooManyFilesMatched {
                pattern,
                max_matches: MAX_GLOB_FILES,
            }
            .into());
        }
    }

    if matches.is_empty() {
        return Err(PqError::NoFilesMatched { pattern }.into());
    }

    sources.extend(matches);
    Ok(())
}

fn is_glob_pattern(path: &Path) -> bool {
    let path = path.to_string_lossy();
    path.contains('*') || path.contains('?') || path.contains('[')
}

fn validate_file_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(PqError::file_not_found(path).into());
    }

    if path.is_dir() {
        return Err(PqError::is_directory(path).into());
    }

    Ok(())
}
