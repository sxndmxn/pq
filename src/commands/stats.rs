//! Column statistics command

use crate::OutputFormat;
use anyhow::Result;
use comfy_table::{Cell, Table};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde::Serialize;
use std::fs::File;
use std::path::PathBuf;

/// Column statistics data
struct ColumnStats {
    name: String,
    physical_type: String,
    null_count: u64,
    min: Option<String>,
    max: Option<String>,
}

/// Serializable representation for JSON/CSV output
#[derive(Serialize)]
struct StatRow {
    column: String,
    #[serde(rename = "type")]
    dtype: String,
    null_count: u64,
    min: Option<String>,
    max: Option<String>,
}

pub fn run(
    paths: &[PathBuf],
    column_filter: Option<&str>,
    output: OutputFormat,
    quiet: bool,
) -> Result<()> {
    for path in paths {
        if paths.len() > 1 && !quiet {
            println!("==> {} <==", path.display());
        }

        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let schema = metadata.file_metadata().schema_descr();

        // Collect stats per column across all row groups
        let num_columns = schema.num_columns();
        let mut column_stats: Vec<ColumnStats> = (0..num_columns)
            .map(|i| ColumnStats {
                name: schema.column(i).name().to_string(),
                physical_type: format!("{:?}", schema.column(i).physical_type()),
                null_count: 0,
                min: None,
                max: None,
            })
            .collect();

        // Aggregate stats from all row groups
        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            for (col_idx, cs) in column_stats.iter_mut().enumerate().take(rg.num_columns()) {
                if let Some(stats) = rg.column(col_idx).statistics() {
                    cs.null_count += stats.null_count_opt().unwrap_or(0);

                    // Update min/max (format as strings for display)
                    update_min_max(cs, stats);
                }
            }
        }

        // Filter by column name if specified
        let stats_to_show: Vec<_> = if let Some(col_name) = column_filter {
            column_stats
                .into_iter()
                .filter(|s| s.name == col_name)
                .collect()
        } else {
            column_stats
        };

        // Output based on format
        output_stats(&stats_to_show, output, quiet);
    }
    Ok(())
}

/// Update min/max values from statistics based on physical type
fn update_min_max(cs: &mut ColumnStats, stats: &Statistics) {
    match stats {
        Statistics::Int32(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(min.to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(max.to_string());
            }
        }
        Statistics::Int64(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(min.to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(max.to_string());
            }
        }
        Statistics::Float(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(min.to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(max.to_string());
            }
        }
        Statistics::Double(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(min.to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(max.to_string());
            }
        }
        Statistics::ByteArray(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(String::from_utf8_lossy(min.data()).to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(String::from_utf8_lossy(max.data()).to_string());
            }
        }
        Statistics::Boolean(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(min.to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(max.to_string());
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if let Some(min) = s.min_opt() {
                cs.min = Some(String::from_utf8_lossy(min.data()).to_string());
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(String::from_utf8_lossy(max.data()).to_string());
            }
        }
        Statistics::Int96(s) => {
            // Int96 is a deprecated type used for timestamps in older Parquet files
            if let Some(min) = s.min_opt() {
                cs.min = Some(format!("{min:?}"));
            }
            if let Some(max) = s.max_opt() {
                cs.max = Some(format!("{max:?}"));
            }
        }
    }
}

/// Output statistics in the requested format
fn output_stats(stats: &[ColumnStats], output: OutputFormat, quiet: bool) {
    match output {
        OutputFormat::Table => {
            let mut tbl = Table::new();
            if !quiet {
                tbl.set_header(vec!["Column", "Type", "Nulls", "Min", "Max"]);
            }
            for s in stats {
                tbl.add_row(vec![
                    Cell::new(&s.name),
                    Cell::new(&s.physical_type),
                    Cell::new(s.null_count),
                    Cell::new(s.min.as_deref().unwrap_or("N/A")),
                    Cell::new(s.max.as_deref().unwrap_or("N/A")),
                ]);
            }
            println!("{tbl}");
        }
        OutputFormat::Json => {
            let rows: Vec<StatRow> = stats
                .iter()
                .map(|s| StatRow {
                    column: s.name.clone(),
                    dtype: s.physical_type.clone(),
                    null_count: s.null_count,
                    min: s.min.clone(),
                    max: s.max.clone(),
                })
                .collect();
            // Safe: StatRow is always serializable
            #[allow(clippy::expect_used)]
            let json = serde_json::to_string_pretty(&rows).expect("StatRow is always serializable");
            println!("{json}");
        }
        OutputFormat::Jsonl => {
            for s in stats {
                let row = StatRow {
                    column: s.name.clone(),
                    dtype: s.physical_type.clone(),
                    null_count: s.null_count,
                    min: s.min.clone(),
                    max: s.max.clone(),
                };
                // Safe: StatRow is always serializable
                #[allow(clippy::expect_used)]
                let json = serde_json::to_string(&row).expect("StatRow is always serializable");
                println!("{json}");
            }
        }
        OutputFormat::Csv => {
            if !quiet {
                println!("column,type,null_count,min,max");
            }
            for s in stats {
                println!(
                    "{},{},{},{},{}",
                    escape_csv(&s.name),
                    escape_csv(&s.physical_type),
                    s.null_count,
                    escape_csv(s.min.as_deref().unwrap_or("")),
                    escape_csv(s.max.as_deref().unwrap_or(""))
                );
            }
        }
    }
}

/// Escape a string for CSV output
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
