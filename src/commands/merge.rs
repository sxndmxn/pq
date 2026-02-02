//! File merging command

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
    let first_file = File::open(&paths[0])?;
    let first_builder = ParquetRecordBatchReaderBuilder::try_new(first_file)?;
    let schema = Arc::clone(first_builder.schema());

    // Create output file with writer
    let output_file = File::create(output)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output_file, Arc::clone(&schema), Some(props))?;

    // Process each input file
    for path in paths {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Verify schema compatibility
        if builder.schema().as_ref() != schema.as_ref() {
            bail!(
                "Schema mismatch: {} has different schema than {}",
                path.display(),
                paths[0].display()
            );
        }

        let reader = builder.build()?;

        for batch_result in reader {
            let batch: RecordBatch = batch_result?;
            writer.write(&batch)?;
        }
    }

    writer.close()?;
    Ok(())
}
