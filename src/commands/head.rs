//! Head and tail commands

use crate::error::{PqError, ResultExt};
use crate::output::{csv, json, table};
use crate::OutputFormat;
use anyhow::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

pub fn run(paths: &[PathBuf], n: usize, output: OutputFormat, quiet: bool) -> Result<()> {
    for path in paths {
        if paths.len() > 1 && !quiet {
            println!("==> {} <==", path.display());
        }

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
        let reader = builder
            .with_batch_size(n.min(1024))
            .build()
            .map_err(|e| PqError::read_error(path, &e))?;

        let mut batches = Vec::new();
        let mut total_rows = 0;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| PqError::corrupted(path, &e))?;
            let rows_needed = n.saturating_sub(total_rows);
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

        output_batches(&batches, output, quiet)?;
    }
    Ok(())
}

pub fn run_tail(paths: &[PathBuf], n: usize, output: OutputFormat, quiet: bool) -> Result<()> {
    for path in paths {
        if paths.len() > 1 && !quiet {
            println!("==> {} <==", path.display());
        }

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
        let reader = builder.build().map_err(|e| PqError::read_error(path, &e))?;

        // Collect all batches first (for tail we need to read to the end)
        let all_batches: Vec<RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| PqError::corrupted(path, &e))?;

        // Calculate total rows and slice from the end
        let total_rows: usize = all_batches.iter().map(RecordBatch::num_rows).sum();
        let skip_rows = total_rows.saturating_sub(n);

        let mut result_batches = Vec::new();
        let mut skipped = 0;

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

        output_batches(&result_batches, output, quiet)?;
    }
    Ok(())
}

fn output_batches(batches: &[RecordBatch], output: OutputFormat, quiet: bool) -> Result<()> {
    match output {
        OutputFormat::Table => table::print_batches(batches, quiet),
        OutputFormat::Json => json::print_batches(batches),
        OutputFormat::Jsonl => json::print_batches_jsonl(batches),
        OutputFormat::Csv => csv::print_batches(batches, !quiet),
    }
}
