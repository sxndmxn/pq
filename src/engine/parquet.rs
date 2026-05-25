use crate::error::{PqError, ResultExt};
use crate::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::{self, File};
use std::path::Path;

pub fn read_head(path: &Path, rows: usize) -> Result<Vec<RecordBatch>> {
    let builder = reader_builder(path)?;
    let reader = builder
        .with_batch_size(rows.min(1024))
        .build()
        .map_err(|error| PqError::read_error(path, &error))?;

    let mut batches = Vec::new();
    let mut total_rows = 0usize;

    for batch_result in reader {
        let batch = batch_result.map_err(|error| PqError::corrupted(path, &error))?;
        let rows_needed = rows.saturating_sub(total_rows);
        if rows_needed == 0 {
            break;
        }

        let batch = if batch.num_rows() > rows_needed {
            batch.slice(0, rows_needed)
        } else {
            batch
        };

        total_rows += batch.num_rows();
        batches.push(batch);
    }

    Ok(batches)
}

pub fn read_tail(path: &Path, rows: usize) -> Result<Vec<RecordBatch>> {
    let builder = reader_builder(path)?;
    let reader = builder
        .build()
        .map_err(|error| PqError::read_error(path, &error))?;

    let all_batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| PqError::corrupted(path, &error))?;

    let total_rows: usize = all_batches.iter().map(RecordBatch::num_rows).sum();
    let skip_rows = total_rows.saturating_sub(rows);

    let mut result_batches = Vec::new();
    let mut skipped = 0usize;

    for batch in all_batches {
        if skipped + batch.num_rows() <= skip_rows {
            skipped += batch.num_rows();
            continue;
        }

        let offset = skip_rows.saturating_sub(skipped);
        let sliced = batch.slice(offset, batch.num_rows() - offset);
        result_batches.push(sliced);
        skipped = skip_rows;
    }

    Ok(result_batches)
}

pub fn row_count(path: &Path) -> Result<i64> {
    let reader = serialized_reader(path)?;
    Ok(reader.metadata().file_metadata().num_rows())
}

pub fn schema_columns(path: &Path) -> Result<Vec<(String, String, bool)>> {
    let reader = serialized_reader(path)?;
    let schema = reader.metadata().file_metadata().schema_descr();

    Ok(schema
        .columns()
        .iter()
        .map(|column| {
            (
                column.name().to_string(),
                format!("{:?}", column.physical_type()),
                column.self_type().is_optional(),
            )
        })
        .collect())
}

pub fn file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).with_path_context(path)?.len())
}

fn reader_builder(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let file = File::open(path).with_path_context(path)?;
    ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| map_parquet_error(path, error))
}

fn serialized_reader(path: &Path) -> Result<SerializedFileReader<File>> {
    let file = File::open(path).with_path_context(path)?;
    SerializedFileReader::new(file).map_err(|error| map_parquet_error(path, error))
}

fn map_parquet_error(path: &Path, error: impl std::fmt::Display) -> PqError {
    let message = error.to_string().to_lowercase();
    if message.contains("magic") || message.contains("not a valid parquet") {
        PqError::invalid_parquet(path, error)
    } else if message.contains("eof") || message.contains("truncat") {
        PqError::corrupted(path, error)
    } else {
        PqError::read_error(path, error)
    }
}
