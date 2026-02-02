//! Pretty table formatting using comfy-table

use anyhow::Result;
use arrow::array::RecordBatch;
use comfy_table::{Cell, Table};

/// Print record batches as a formatted table
pub fn print_batches(batches: &[RecordBatch], quiet: bool) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let mut table = Table::new();

    if !quiet {
        table.set_header(schema.fields().iter().map(|f| Cell::new(f.name())));
    }

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = arrow::util::display::array_value_to_string(col, row_idx)?;
                row.push(Cell::new(value));
            }
            table.add_row(row);
        }
    }

    println!("{table}");
    Ok(())
}

/// Print a simple key-value table
pub fn print_key_value(rows: &[(&str, String)], quiet: bool) {
    let mut table = Table::new();

    if !quiet {
        table.set_header(vec![Cell::new("Key"), Cell::new("Value")]);
    }

    for (key, value) in rows {
        table.add_row(vec![Cell::new(*key), Cell::new(value)]);
    }

    println!("{table}");
}

/// Print schema information as a table
pub fn print_schema_table(columns: &[(String, String, bool)], quiet: bool) {
    let mut table = Table::new();

    if !quiet {
        table.set_header(vec![
            Cell::new("Column"),
            Cell::new("Type"),
            Cell::new("Nullable"),
        ]);
    }

    for (name, dtype, nullable) in columns {
        table.add_row(vec![
            Cell::new(name),
            Cell::new(dtype),
            Cell::new(if *nullable { "Yes" } else { "No" }),
        ]);
    }

    println!("{table}");
}
