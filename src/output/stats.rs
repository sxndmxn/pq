use crate::model::ColumnStats;
use comfy_table::{Cell, Table};

pub fn print_table(rows: &[ColumnStats], quiet: bool) {
    let mut table = Table::new();
    if !quiet {
        table.set_header(vec!["Column", "Type", "Nulls", "Min", "Max"]);
    }

    for row in rows {
        table.add_row(vec![
            Cell::new(&row.column),
            Cell::new(&row.dtype),
            Cell::new(row.null_count),
            Cell::new(row.min.as_deref().unwrap_or("N/A")),
            Cell::new(row.max.as_deref().unwrap_or("N/A")),
        ]);
    }

    println!("{table}");
}

pub fn print_csv(rows: &[ColumnStats], include_header: bool) {
    if include_header {
        println!("column,type,null_count,min,max");
    }

    for row in rows {
        println!(
            "{},{},{},{},{}",
            escape_csv(&row.column),
            escape_csv(&row.dtype),
            row.null_count,
            escape_csv(row.min.as_deref().unwrap_or("")),
            escape_csv(row.max.as_deref().unwrap_or("")),
        );
    }
}

fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
