use crate::cli::args::OutputFormat;
use crate::model::{ColumnInfo, ColumnStats, FileInfo};
use crate::Result;
use arrow::array::RecordBatch;

pub mod csv;
pub mod info;
pub mod json;
pub mod schema;
pub mod stats;
pub mod table;

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
