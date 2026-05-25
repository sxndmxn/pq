use crate::model::FileInfo;
use crate::output::table;

pub fn print_table(rows: &[FileInfo], quiet: bool) {
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            println!();
        }

        let entries = [
            ("File", row.file.clone()),
            ("File Size", format_size(row.file_size_bytes)),
            ("Rows", row.num_rows.to_string()),
            ("Columns", row.num_columns.to_string()),
            ("Row Groups", row.num_row_groups.to_string()),
            ("Compression", row.compression.clone()),
            ("Created By", row.created_by.clone()),
            ("Version", row.version.to_string()),
        ];
        table::print_key_value(&entries, quiet);
    }
}

pub fn print_csv(rows: &[FileInfo], include_header: bool) {
    if include_header {
        println!("file,file_size_bytes,num_rows,num_columns,num_row_groups,compression,created_by,version");
    }

    for row in rows {
        println!(
            "{},{},{},{},{},{},{},{}",
            escape_csv(&row.file),
            row.file_size_bytes,
            row.num_rows,
            row.num_columns,
            row.num_row_groups,
            escape_csv(&row.compression),
            escape_csv(&row.created_by),
            row.version,
        );
    }
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

fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
