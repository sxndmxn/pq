use crate::cli::args::OutputFormat;
use crate::error::PqError;
use crate::model::{ColumnInfo, ColumnStats, FileInfo};
use crate::Result;
use arrow::array::RecordBatch;
use std::path::Path;

pub mod csv;
pub mod info;
pub mod json;
pub mod schema;
pub mod stats;
pub mod table;

#[derive(Debug)]
enum FileOutputFormat {
    Csv,
    Json,
    Jsonl,
}

pub fn write_batches(output: OutputFormat, quiet: bool, batches: &[RecordBatch]) -> Result<()> {
    match output {
        OutputFormat::Table => table::print_batches(batches, quiet),
        OutputFormat::Json => json::print_batches(batches),
        OutputFormat::Jsonl => json::print_batches_jsonl(batches),
        OutputFormat::Csv => csv::print_batches(batches, !quiet),
    }
}

pub fn write_schema(output: OutputFormat, quiet: bool, columns: &[ColumnInfo]) -> Result<()> {
    match output {
        OutputFormat::Table => {
            table::print_schema_table(columns, quiet);
            Ok(())
        }
        OutputFormat::Json => json::print_value(columns),
        OutputFormat::Jsonl => json::print_json_lines(columns),
        OutputFormat::Csv => {
            schema::print_csv(columns, !quiet);
            Ok(())
        }
    }
}

pub fn write_stats(output: OutputFormat, quiet: bool, rows: &[ColumnStats]) -> Result<()> {
    match output {
        OutputFormat::Table => {
            stats::print_table(rows, quiet);
            Ok(())
        }
        OutputFormat::Json => json::print_value(rows),
        OutputFormat::Jsonl => json::print_json_lines(rows),
        OutputFormat::Csv => {
            stats::print_csv(rows, !quiet);
            Ok(())
        }
    }
}

pub fn write_file_infos(output: OutputFormat, quiet: bool, rows: &[FileInfo]) -> Result<()> {
    match output {
        OutputFormat::Table => {
            info::print_table(rows, quiet);
            Ok(())
        }
        OutputFormat::Json => json::print_value(rows),
        OutputFormat::Jsonl => json::print_json_lines(rows),
        OutputFormat::Csv => {
            info::print_csv(rows, !quiet);
            Ok(())
        }
    }
}

pub fn write_batches_to_path(path: &Path, batches: &[RecordBatch]) -> Result<()> {
    match file_output_format(path)? {
        FileOutputFormat::Csv => csv::write_batches_to_file(batches, path)
            .map_err(|error| PqError::write_error(path, error).into()),
        FileOutputFormat::Json => json::write_batches_to_file(batches, path)
            .map_err(|error| PqError::write_error(path, error).into()),
        FileOutputFormat::Jsonl => json::write_batches_jsonl_to_file(batches, path)
            .map_err(|error| PqError::write_error(path, error).into()),
    }
}

fn file_output_format(path: &Path) -> Result<FileOutputFormat> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_lowercase)
        .as_deref()
    {
        Some("csv") => Ok(FileOutputFormat::Csv),
        Some("json") => Ok(FileOutputFormat::Json),
        Some("jsonl") => Ok(FileOutputFormat::Jsonl),
        Some(format) => Err(PqError::UnsupportedFormat {
            format: format.to_string(),
            supported: "csv, json, jsonl".to_string(),
        }
        .into()),
        None => Err(PqError::UnsupportedFormat {
            format: "(no extension)".to_string(),
            supported: "csv, json, jsonl".to_string(),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(extension: &str) -> Result<std::path::PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| anyhow::anyhow!("system clock error: {error}"))?
            .as_nanos();
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(std::env::temp_dir().join(format!("pq_output_{unique}_{counter}.{extension}")))
    }

    fn sample_batch() -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .map_err(Into::into)
    }

    #[test]
    fn infers_jsonl_output_from_path() -> Result<()> {
        let path = temp_path("jsonl")?;
        let batch = sample_batch()?;

        write_batches_to_path(&path, &[batch])?;

        let contents = fs::read_to_string(&path)?;
        assert!(contents.lines().all(|line| line.starts_with('{')));
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn rejects_unknown_output_extension() {
        let path_buf = std::env::temp_dir().join("output.unknown");
        let path = path_buf.as_path();
        assert!(matches!(
            file_output_format(path),
            Err(ref err) if err.to_string().contains("Unsupported format")
        ));
    }
}
