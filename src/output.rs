use crate::cli::args::OutputFormat;
use crate::error::PqError;
use crate::model::{ColumnInfo, ColumnStats, FileInfo, StatValue};
use crate::Result;
use arrow::array::RecordBatch;
use serde::Serialize;
use serde_json::Value;
use std::io;
use std::path::Path;

pub mod csv;
pub(crate) mod csv_support;
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

#[derive(Serialize)]
struct SchemaJsonRow {
    name: String,
    #[serde(rename = "type")]
    display_type: String,
    nullable: bool,
    physical_type: String,
    logical_type: Option<String>,
}

#[derive(Serialize)]
struct StatsJsonRow {
    column: String,
    #[serde(rename = "type")]
    display_type: String,
    null_count: u64,
    min: Option<Value>,
    max: Option<Value>,
    physical_type: String,
    logical_type: Option<String>,
}

#[derive(Serialize)]
struct FileInfoJsonRow {
    file: String,
    file_size_bytes: u64,
    num_rows: i64,
    num_columns: usize,
    num_row_groups: usize,
    compression: Option<String>,
    created_by: Option<String>,
    version: i32,
}

pub fn write_batches(output: OutputFormat, quiet: bool, batches: &[RecordBatch]) -> Result<()> {
    match output {
        OutputFormat::Table => table::write_batches(io::stdout().lock(), batches, quiet)?,
        OutputFormat::Json => json::write_json(io::stdout().lock(), batches)?,
        OutputFormat::Jsonl => json::write_jsonl(io::stdout().lock(), batches)?,
        OutputFormat::Csv => csv::write_batches(io::stdout().lock(), batches, !quiet)?,
    }
    Ok(())
}

pub fn write_schema(output: OutputFormat, quiet: bool, columns: &[ColumnInfo]) -> Result<()> {
    match output {
        OutputFormat::Table => table::write_schema_table(io::stdout().lock(), columns, quiet)?,
        OutputFormat::Json => json::write_value(io::stdout().lock(), &schema_rows(columns))?,
        OutputFormat::Jsonl => json::write_json_lines(io::stdout().lock(), &schema_rows(columns))?,
        OutputFormat::Csv => schema::write_csv(io::stdout().lock(), columns, !quiet)?,
    }
    Ok(())
}

pub fn write_stats(output: OutputFormat, quiet: bool, rows: &[ColumnStats]) -> Result<()> {
    match output {
        OutputFormat::Table => stats::write_table(io::stdout().lock(), rows, quiet)?,
        OutputFormat::Json => json::write_value(io::stdout().lock(), &stats_rows(rows))?,
        OutputFormat::Jsonl => json::write_json_lines(io::stdout().lock(), &stats_rows(rows))?,
        OutputFormat::Csv => stats::write_csv(io::stdout().lock(), rows, !quiet)?,
    }
    Ok(())
}

pub fn write_file_infos(output: OutputFormat, quiet: bool, rows: &[FileInfo]) -> Result<()> {
    match output {
        OutputFormat::Table => info::write_table(io::stdout().lock(), rows, quiet)?,
        OutputFormat::Json => json::write_value(io::stdout().lock(), &file_info_rows(rows))?,
        OutputFormat::Jsonl => json::write_json_lines(io::stdout().lock(), &file_info_rows(rows))?,
        OutputFormat::Csv => info::write_csv(io::stdout().lock(), rows, !quiet)?,
    }
    Ok(())
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

fn schema_rows(columns: &[ColumnInfo]) -> Vec<SchemaJsonRow> {
    columns
        .iter()
        .map(|column| SchemaJsonRow {
            name: column.name.clone(),
            display_type: column.display_type(),
            nullable: column.nullable,
            physical_type: column.column_type.physical.to_string(),
            logical_type: column
                .column_type
                .logical
                .as_ref()
                .map(|logical| logical.display_name()),
        })
        .collect()
}

fn stats_rows(rows: &[ColumnStats]) -> Vec<StatsJsonRow> {
    rows.iter()
        .map(|row| StatsJsonRow {
            column: row.column.clone(),
            display_type: row.display_type(),
            null_count: row.null_count,
            min: row.min.as_ref().map(stat_value_json),
            max: row.max.as_ref().map(stat_value_json),
            physical_type: row.column_type.physical.to_string(),
            logical_type: row
                .column_type
                .logical
                .as_ref()
                .map(|logical| logical.display_name()),
        })
        .collect()
}

fn file_info_rows(rows: &[FileInfo]) -> Vec<FileInfoJsonRow> {
    rows.iter()
        .map(|row| FileInfoJsonRow {
            file: row.path().display().to_string(),
            file_size_bytes: row.file_size_bytes,
            num_rows: row.num_rows,
            num_columns: row.num_columns,
            num_row_groups: row.num_row_groups,
            compression: row.compression.map(|compression| compression.to_string()),
            created_by: row.created_by.clone(),
            version: row.version,
        })
        .collect()
}

fn stat_value_json(value: &StatValue) -> Value {
    match value {
        StatValue::Int32(inner) => Value::from(*inner),
        StatValue::Int64(inner) => Value::from(*inner),
        StatValue::Float(inner) => Value::from(*inner),
        StatValue::Double(inner) => Value::from(*inner),
        StatValue::Binary(inner) | StatValue::FixedLenBinary(inner) => {
            Value::from(String::from_utf8_lossy(inner).to_string())
        }
        StatValue::Boolean(inner) => Value::from(*inner),
        StatValue::Int96(inner) => Value::from(inner.clone()),
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
