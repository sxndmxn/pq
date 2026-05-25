use crate::error::{PqError, ResultExt};
use crate::model::{ColumnInfo, FileInfo};
use crate::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

pub fn read_head(path: &Path, rows: usize) -> Result<Vec<RecordBatch>> {
    let builder = reader_builder(path)?;
    let reader = builder
        .with_batch_size(rows.min(1024))
        .build()
        .map_err(|error| PqError::from_read(path, error))?;

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
        .map_err(|error| PqError::from_read(path, error))?;

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

pub fn schema_columns(path: &Path) -> Result<Vec<ColumnInfo>> {
    let reader = serialized_reader(path)?;
    let schema = reader.metadata().file_metadata().schema_descr();

    Ok(schema
        .columns()
        .iter()
        .map(|column| ColumnInfo {
            name: column.name().to_string(),
            type_name: format!("{:?}", column.physical_type()),
            nullable: column.self_type().is_optional(),
        })
        .collect())
}

pub fn file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).with_path_context(path)?.len())
}

pub fn file_info(path: &Path) -> Result<FileInfo> {
    let reader = serialized_reader(path)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let num_row_groups = metadata.num_row_groups();

    let compression = if num_row_groups > 0 {
        let row_group = metadata.row_group(0);
        if row_group.num_columns() > 0 {
            format!("{:?}", row_group.column(0).compression())
        } else {
            "N/A".to_string()
        }
    } else {
        "N/A".to_string()
    };

    Ok(FileInfo {
        file: path.display().to_string(),
        file_size_bytes: file_size(path)?,
        num_rows: file_metadata.num_rows(),
        num_columns: file_metadata.schema_descr().num_columns(),
        num_row_groups,
        compression,
        created_by: file_metadata.created_by().unwrap_or("unknown").to_string(),
        version: file_metadata.version(),
    })
}

pub fn read_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let reader = reader_builder(path)?
        .build()
        .map_err(|error| PqError::from_read(path, error))?;

    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| PqError::corrupted(path, &error).into())
}

pub fn reader_builder(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let file = File::open(path).with_path_context(path)?;
    Ok(ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| PqError::from_read(path, error))?)
}

pub fn serialized_reader(path: &Path) -> Result<SerializedFileReader<File>> {
    let file = File::open(path).with_path_context(path)?;
    Ok(SerializedFileReader::new(file).map_err(|error| PqError::from_read(path, error))?)
}

pub fn merge_files(paths: &[&Path], output: &Path) -> Result<()> {
    if paths.is_empty() {
        return Err(anyhow::anyhow!("No input files specified"));
    }

    let first_builder = reader_builder(paths[0])?;
    let schema = Arc::clone(first_builder.schema());

    let output_file = File::create(output).map_err(|error| PqError::write_error(output, error))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output_file, Arc::clone(&schema), Some(props))
        .map_err(|error| PqError::write_error(output, error))?;

    for path in paths {
        let builder = reader_builder(path)?;

        if builder.schema().as_ref() != schema.as_ref() {
            return Err(PqError::SchemaMismatch {
                file1: paths[0].display().to_string(),
                file2: path.display().to_string(),
                details: "Column names or types differ".to_string(),
            }
            .into());
        }

        let reader = builder
            .build()
            .map_err(|error| PqError::from_read(path, error))?;

        for batch_result in reader {
            let batch = batch_result.map_err(|error| PqError::corrupted(path, error))?;
            writer
                .write(&batch)
                .map_err(|error| PqError::write_error(output, error))?;
        }
    }

    writer
        .close()
        .map_err(|error| PqError::write_error(output, error))?;
    Ok(())
}
