//! Schema display command

use crate::error::{PqError, ResultExt};
use crate::output::{csv, json, table};
use crate::OutputFormat;
use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::PathBuf;

pub fn run(paths: &[PathBuf], output: OutputFormat, quiet: bool) -> Result<()> {
    for path in paths {
        if paths.len() > 1 && !quiet {
            println!("==> {} <==", path.display());
        }

        let file = File::open(path).with_path_context(path)?;
        let reader = SerializedFileReader::new(file).map_err(|e| {
            // Check for common parquet validation errors
            let msg = e.to_string().to_lowercase();
            if msg.contains("magic") || msg.contains("not a valid parquet") {
                PqError::invalid_parquet(path, &e)
            } else if msg.contains("eof") || msg.contains("truncat") {
                PqError::corrupted(path, &e)
            } else {
                PqError::read_error(path, &e)
            }
        })?;
        let schema = reader.metadata().file_metadata().schema_descr();

        // Extract column info: (name, type_string, nullable)
        let columns: Vec<(String, String, bool)> = schema
            .columns()
            .iter()
            .map(|col| {
                let name = col.name().to_string();
                let dtype = format!("{:?}", col.physical_type());
                let nullable = col.self_type().is_optional();
                (name, dtype, nullable)
            })
            .collect();

        match output {
            OutputFormat::Table => table::print_schema_table(&columns, quiet),
            OutputFormat::Json | OutputFormat::Jsonl => json::print_schema(&columns),
            OutputFormat::Csv => csv::print_schema(&columns, !quiet),
        }
    }
    Ok(())
}
