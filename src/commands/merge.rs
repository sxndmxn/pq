//! File merging command

use crate::error::{PqError, ResultExt};
use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn run(paths: &[PathBuf], output: &Path) -> Result<()> {
    if paths.is_empty() {
        bail!("No input files specified");
    }

    // Read schema from first file
    let first_file = File::open(&paths[0]).with_path_context(&paths[0])?;
    let first_builder = ParquetRecordBatchReaderBuilder::try_new(first_file).map_err(|e| {
        let msg = e.to_string().to_lowercase();
        if msg.contains("magic") || msg.contains("not a valid parquet") {
            PqError::invalid_parquet(&paths[0], &e)
        } else if msg.contains("eof") || msg.contains("truncat") {
            PqError::corrupted(&paths[0], &e)
        } else {
            PqError::read_error(&paths[0], &e)
        }
    })?;
    let schema = Arc::clone(first_builder.schema());

    // Create output file with writer
    let output_file = File::create(output).map_err(|e| PqError::write_error(output, &e))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output_file, Arc::clone(&schema), Some(props))
        .map_err(|e| PqError::write_error(output, &e))?;

    // Process each input file
    for path in paths {
        let file = File::open(path).with_path_context(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("magic") || msg.contains("not a valid parquet") {
                PqError::invalid_parquet(path, &e)
            } else if msg.contains("eof") || msg.contains("truncat") {
                PqError::corrupted(path, &e)
            } else {
                PqError::read_error(path, &e)
            }
        })?;

        // Verify schema compatibility
        if builder.schema().as_ref() != schema.as_ref() {
            return Err(PqError::SchemaMismatch {
                file1: paths[0].display().to_string(),
                file2: path.display().to_string(),
                details: "Column names or types differ".to_string(),
            }
            .into());
        }

        let reader = builder.build().map_err(|e| PqError::read_error(path, &e))?;

        for batch_result in reader {
            let batch: RecordBatch = batch_result.map_err(|e| PqError::corrupted(path, &e))?;
            writer
                .write(&batch)
                .map_err(|e| PqError::write_error(output, &e))?;
        }
    }

    writer
        .close()
        .map_err(|e| PqError::write_error(output, &e))?;
    Ok(())
}
