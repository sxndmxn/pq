//! Format conversion command

use crate::output::csv as csv_output;
use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn run(input: &Path, output: &Path) -> Result<()> {
    // Determine output format from extension
    let extension = output
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_lowercase);

    let format = match extension.as_deref() {
        Some("csv") => OutputType::Csv,
        Some("json") => OutputType::Json,
        Some("jsonl") => OutputType::Jsonl,
        Some(ext) => bail!("Unsupported output format: {ext}. Use .csv, .json, or .jsonl"),
        None => bail!("Output file must have an extension (.csv, .json, or .jsonl)"),
    };

    // Read parquet file
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    // Write output
    match format {
        OutputType::Csv => {
            csv_output::write_batches_to_file(&batches, output)?;
        }
        OutputType::Json => {
            write_json(&batches, output)?;
        }
        OutputType::Jsonl => {
            write_jsonl(&batches, output)?;
        }
    }

    Ok(())
}

enum OutputType {
    Csv,
    Json,
    Jsonl,
}

fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Map<String, Value>>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut row = Map::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let value_str = arrow::util::display::array_value_to_string(col, row_idx)?;

            let value = if value_str == "null" || value_str.is_empty() {
                Value::Null
            } else if let Ok(n) = value_str.parse::<i64>() {
                Value::Number(n.into())
            } else if let Ok(n) = value_str.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map_or_else(|| Value::String(value_str.clone()), Value::Number)
            } else if value_str == "true" {
                Value::Bool(true)
            } else if value_str == "false" {
                Value::Bool(false)
            } else {
                Value::String(value_str)
            };

            row.insert(field.name().clone(), value);
        }
        rows.push(row);
    }

    Ok(rows)
}

fn write_json(batches: &[RecordBatch], path: &Path) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    let mut all_rows = Vec::new();
    for batch in batches {
        all_rows.extend(batch_to_json_rows(batch)?);
    }

    serde_json::to_writer_pretty(&mut writer, &all_rows)?;
    writer.flush()?;
    Ok(())
}

fn write_jsonl(batches: &[RecordBatch], path: &Path) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    for batch in batches {
        for row in batch_to_json_rows(batch)? {
            serde_json::to_writer(&mut writer, &row)?;
            writeln!(writer)?;
        }
    }

    writer.flush()?;
    Ok(())
}
