//! File metadata command

use crate::cli::args::{InfoArgs, OutputFormat};
use crate::dataset::Dataset;
use crate::error::ResultExt;
use crate::output::table;
use crate::{engine, PqError, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;
use std::fs::File;

#[derive(Serialize)]
struct FileInfo {
    file: String,
    file_size_bytes: u64,
    num_rows: i64,
    num_columns: usize,
    num_row_groups: usize,
    compression: String,
    created_by: String,
    version: i32,
}

pub fn run(args: InfoArgs) -> Result<()> {
    let mut all_info = Vec::new();
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        if dataset.is_multi_source() && !args.quiet && matches!(args.output, OutputFormat::Table) {
            println!("==> {} <==", source.path().display());
        }

        let path = source.path();
        let file = File::open(path).with_path_context(path)?;
        let file_size = engine::parquet::file_size(path)?;
        let reader = SerializedFileReader::new(file).map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("magic") || msg.contains("not a valid parquet") {
                PqError::invalid_parquet(path, &e)
            } else if msg.contains("eof") || msg.contains("truncat") {
                PqError::corrupted(path, &e)
            } else {
                PqError::read_error(path, &e)
            }
        })?;
        let metadata = reader.metadata();
        let file_meta = metadata.file_metadata();

        let num_row_groups = metadata.num_row_groups();
        let num_rows = file_meta.num_rows();
        let num_columns = file_meta.schema_descr().num_columns();
        let created_by = file_meta.created_by().unwrap_or("unknown").to_string();
        let version = file_meta.version();

        // Get compression info from first row group if available
        let compression = if num_row_groups > 0 {
            let rg = metadata.row_group(0);
            if rg.num_columns() > 0 {
                format!("{:?}", rg.column(0).compression())
            } else {
                "N/A".to_string()
            }
        } else {
            "N/A".to_string()
        };

        let info = FileInfo {
            file: path.display().to_string(),
            file_size_bytes: file_size,
            num_rows,
            num_columns,
            num_row_groups,
            compression: compression.clone(),
            created_by: created_by.clone(),
            version,
        };

        match args.output {
            OutputFormat::Table => {
                let rows = [
                    ("File", path.display().to_string()),
                    ("File Size", format_size(file_size)),
                    ("Rows", num_rows.to_string()),
                    ("Columns", num_columns.to_string()),
                    ("Row Groups", num_row_groups.to_string()),
                    ("Compression", compression),
                    ("Created By", created_by),
                    ("Version", version.to_string()),
                ];
                table::print_key_value(&rows, args.quiet);
            }
            OutputFormat::Json | OutputFormat::Csv => {
                all_info.push(info);
            }
            OutputFormat::Jsonl => {
                let json = serde_json::to_string(&info)?;
                println!("{json}");
            }
        }
    }

    // Print collected JSON/CSV output
    match args.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&all_info)?;
            println!("{json}");
        }
        OutputFormat::Csv if !all_info.is_empty() => {
            print_info_csv(&all_info, !args.quiet);
        }
        _ => {}
    }

    Ok(())
}

/// Format file size in human-readable form
#[allow(clippy::cast_precision_loss)]
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Print file info as CSV
fn print_info_csv(info: &[FileInfo], include_header: bool) {
    if include_header {
        println!("file,file_size_bytes,num_rows,num_columns,num_row_groups,compression,created_by,version");
    }
    for i in info {
        // Escape created_by field in case it contains commas or quotes
        let created_by_escaped = if i.created_by.contains(',') || i.created_by.contains('"') {
            format!("\"{}\"", i.created_by.replace('"', "\"\""))
        } else {
            i.created_by.clone()
        };
        println!(
            "{},{},{},{},{},{},{},{}",
            i.file,
            i.file_size_bytes,
            i.num_rows,
            i.num_columns,
            i.num_row_groups,
            i.compression,
            created_by_escaped,
            i.version
        );
    }
}
