use crate::model::FileInfo;
use crate::output::csv_support::escape_csv;
use crate::output::table;
use anyhow::Result;
use std::io::Write;

pub fn write_table<W: Write>(mut writer: W, rows: &[FileInfo], quiet: bool) -> Result<()> {
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            writeln!(writer)?;
        }

        let entries = [
            ("File", row.path().display().to_string()),
            ("File Size", format_size(row.file_size_bytes)),
            ("Rows", row.num_rows.to_string()),
            ("Columns", row.num_columns.to_string()),
            ("Row Groups", row.num_row_groups.to_string()),
            (
                "Compression",
                row.compression
                    .map_or_else(|| "N/A".to_string(), |compression| compression.to_string()),
            ),
            (
                "Created By",
                row.created_by
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
            ),
            ("Version", row.version.to_string()),
        ];
        table::write_key_value(&mut writer, &entries, quiet)?;
    }

    Ok(())
}

pub fn write_csv<W: Write>(
    mut writer: W,
    rows: &[FileInfo],
    include_header: bool,
) -> std::io::Result<()> {
    if include_header {
        writeln!(writer, "file,file_size_bytes,num_rows,num_columns,num_row_groups,compression,created_by,version")?;
    }

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{}",
            escape_csv(&row.path().display().to_string()),
            row.file_size_bytes,
            row.num_rows,
            row.num_columns,
            row.num_row_groups,
            escape_csv(
                &row.compression
                    .map_or_else(String::new, |compression| compression.to_string())
            ),
            escape_csv(row.created_by.as_deref().unwrap_or("")),
            row.version,
        )?;
    }

    Ok(())
}

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
