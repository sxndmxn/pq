use crate::dataset::Dataset;
use crate::engine;
use crate::model::{ColumnInfo, ColumnStats, FileInfo};
use crate::Result;
use arrow::array::RecordBatch;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanKind {
    Head,
    Tail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanOptions {
    pub rows: usize,
}

#[derive(Clone, Debug)]
pub struct SchemaResult {
    pub path: PathBuf,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Clone, Debug)]
pub struct ScanResult {
    pub path: PathBuf,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountEntry {
    pub path: PathBuf,
    pub rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountResult {
    pub entries: Vec<CountEntry>,
    pub total_rows: i64,
}

#[derive(Clone, Debug)]
pub struct StatsResult {
    pub path: PathBuf,
    pub rows: Vec<ColumnStats>,
}

pub fn dataset_from_inputs(inputs: Vec<PathBuf>) -> Result<Dataset> {
    Dataset::from_inputs(inputs)
}

pub fn schema(dataset: &Dataset) -> Result<Vec<SchemaResult>> {
    dataset
        .paths()
        .map(|path| {
            let path = path.to_path_buf();
            let columns = engine::parquet::schema_columns(&path)?;
            Ok(SchemaResult { path, columns })
        })
        .collect()
}

pub fn scan(dataset: &Dataset, kind: ScanKind, options: ScanOptions) -> Result<Vec<ScanResult>> {
    dataset
        .paths()
        .map(|path| {
            let path = path.to_path_buf();
            let batches = match kind {
                ScanKind::Head => engine::parquet::read_head(&path, options.rows)?,
                ScanKind::Tail => engine::parquet::read_tail(&path, options.rows)?,
            };
            Ok(ScanResult { path, batches })
        })
        .collect()
}

pub fn count(dataset: &Dataset) -> Result<CountResult> {
    let mut entries = Vec::new();
    let mut total_rows = 0i64;

    for path in dataset.paths() {
        let rows = engine::parquet::row_count(path)?;
        total_rows += rows;
        entries.push(CountEntry {
            path: path.to_path_buf(),
            rows,
        });
    }

    Ok(CountResult {
        entries,
        total_rows,
    })
}

pub fn stats(dataset: &Dataset, column_name: Option<&str>) -> Result<Vec<StatsResult>> {
    dataset
        .paths()
        .map(|path| {
            let path = path.to_path_buf();
            let rows = engine::stats::column_stats(&path, column_name)?;
            Ok(StatsResult { path, rows })
        })
        .collect()
}

pub fn info(dataset: &Dataset) -> Result<Vec<FileInfo>> {
    dataset.paths().map(engine::parquet::file_info).collect()
}

pub fn convert(input: &Path, output: &Path) -> Result<()> {
    let batches = engine::parquet::read_batches(input)?;
    crate::output::write_batches_to_path(output, &batches)
}

pub fn merge(dataset: &Dataset, output: &Path) -> Result<()> {
    let paths: Vec<_> = dataset.paths().collect();
    engine::parquet::merge_files(&paths, output)
}
