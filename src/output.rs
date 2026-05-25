use crate::cli::args::OutputFormat;
use crate::Result;
use arrow::array::RecordBatch;

pub mod csv;
pub mod json;
pub mod jsonl;
pub mod table;

pub fn write_batches(output: OutputFormat, quiet: bool, batches: &[RecordBatch]) -> Result<()> {
    match output {
        OutputFormat::Table => table::print_batches(batches, quiet),
        OutputFormat::Json => json::print_batches(batches),
        OutputFormat::Jsonl => jsonl::print_batches(batches),
        OutputFormat::Csv => csv::print_batches(batches, !quiet),
    }
}
