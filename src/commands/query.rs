//! SQL query command using `DataFusion`

use crate::error::PqError;
use crate::output::{csv, json, table};
use crate::OutputFormat;
use anyhow::Result;
use datafusion::prelude::*;
use std::path::PathBuf;

pub async fn run(paths: &[PathBuf], sql: &str, output: OutputFormat, quiet: bool) -> Result<()> {
    let ctx = SessionContext::new();

    // Register each file as a table
    // If single file, use "tbl" as the table name
    // If multiple files, use the file stem as the table name
    if paths.len() == 1 {
        ctx.register_parquet(
            "tbl",
            paths[0].to_string_lossy().as_ref(),
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("not found") || msg.contains("no such file") {
                PqError::file_not_found(&paths[0])
            } else if msg.contains("parquet") || msg.contains("magic") {
                PqError::invalid_parquet(&paths[0], &e)
            } else {
                PqError::read_error(&paths[0], &e)
            }
        })?;
    } else {
        for path in paths {
            let table_name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("tbl");
            ctx.register_parquet(
                table_name,
                path.to_string_lossy().as_ref(),
                ParquetReadOptions::default(),
            )
            .await
            .map_err(|e| {
                let msg = e.to_string().to_lowercase();
                if msg.contains("not found") || msg.contains("no such file") {
                    PqError::file_not_found(path)
                } else if msg.contains("parquet") || msg.contains("magic") {
                    PqError::invalid_parquet(path, &e)
                } else {
                    PqError::read_error(path, &e)
                }
            })?;
        }
    }

    // Execute the SQL query
    let df = ctx.sql(sql).await.map_err(|e| PqError::invalid_sql(&e))?;
    let batches = df.collect().await.map_err(|e| PqError::query_failed(&e))?;

    // Output the results
    match output {
        OutputFormat::Table => table::print_batches(&batches, quiet)?,
        OutputFormat::Json => json::print_batches(&batches)?,
        OutputFormat::Jsonl => json::print_batches_jsonl(&batches)?,
        OutputFormat::Csv => csv::print_batches(&batches, !quiet)?,
    }

    Ok(())
}
