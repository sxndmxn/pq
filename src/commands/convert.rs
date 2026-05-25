//! Format conversion command

use crate::cli::args::ConvertArgs;
use crate::dataset::Dataset;
use crate::error::{PqError, ResultExt};
use crate::output::csv as csv_output;
use crate::output::json as json_output;
use crate::Result;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

pub fn run(args: ConvertArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(vec![args.input])?;
    let input = dataset.sources()[0].path();
    let output = args.output_path.as_path();

    // Determine output format from extension
    let extension = output
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_lowercase);

    let format = match extension.as_deref() {
        Some("csv") => OutputType::Csv,
        Some("json") => OutputType::Json,
        Some("jsonl") => OutputType::Jsonl,
        Some(ext) => {
            return Err(PqError::UnsupportedFormat {
                format: ext.to_string(),
                supported: "csv, json, jsonl".to_string(),
            }
            .into())
        }
        None => {
            return Err(PqError::UnsupportedFormat {
                format: "(no extension)".to_string(),
                supported: "csv, json, jsonl".to_string(),
            }
            .into())
        }
    };

    // Read parquet file
    let file = File::open(input).with_path_context(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        let msg = e.to_string().to_lowercase();
        if msg.contains("magic") || msg.contains("not a valid parquet") {
            PqError::invalid_parquet(input, &e)
        } else if msg.contains("eof") || msg.contains("truncat") {
            PqError::corrupted(input, &e)
        } else {
            PqError::read_error(input, &e)
        }
    })?;
    let reader = builder
        .build()
        .map_err(|e| PqError::read_error(input, &e))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PqError::corrupted(input, &e))?;

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

fn write_json(batches: &[RecordBatch], path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| PqError::write_error(path, &e))?;
    let mut writer = BufWriter::new(file);
    json_output::write_json(&mut writer, batches).map_err(|e| PqError::write_error(path, &e))?;
    Ok(())
}

fn write_jsonl(batches: &[RecordBatch], path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| PqError::write_error(path, &e))?;
    let mut writer = BufWriter::new(file);
    json_output::write_jsonl(&mut writer, batches).map_err(|e| PqError::write_error(path, &e))?;
    Ok(())
}
