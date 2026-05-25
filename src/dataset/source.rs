use crate::error::PqError;
use crate::Result;
use anyhow::bail;
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
        let mut sources = Vec::new();

        for input in inputs {
            let path_str = input.to_string_lossy();
            if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
                let matches: Vec<_> = glob::glob(&path_str)?
                    .filter_map(std::result::Result::ok)
                    .filter(|path| path.is_file())
                    .take(MAX_GLOB_FILES + 1)
                    .map(|path| DatasetSource { path })
                    .collect();

                if matches.is_empty() {
                    return Err(PqError::NoFilesMatched {
                        pattern: path_str.into_owned(),
                    }
                    .into());
                }

                if matches.len() > MAX_GLOB_FILES {
                    bail!(
                        "Pattern '{path_str}' matched more than {MAX_GLOB_FILES} files. Use a more specific pattern."
                    );
                }

                sources.extend(matches);
            } else {
                validate_file_path(&input)?;
                sources.push(DatasetSource { path: input });
            }
        }

        if sources.is_empty() {
            bail!("No input files specified");
        }

        sources.sort();

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

fn validate_file_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(PqError::file_not_found(path).into());
    }

    if path.is_dir() {
        return Err(PqError::is_directory(path).into());
    }

    Ok(())
}
