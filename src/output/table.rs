//! Pretty table formatting using comfy-table

use crate::model::ColumnInfo;
use crate::Result;
use arrow::array::RecordBatch;
use comfy_table::{Cell, Table};
use std::io::Write;

pub fn write_batches<W: Write>(mut writer: W, batches: &[RecordBatch], quiet: bool) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let mut table = Table::new();

    if !quiet {
        table.set_header(schema.fields().iter().map(|field| Cell::new(field.name())));
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

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_key_value<W: Write>(
    mut writer: W,
    rows: &[(&str, String)],
    quiet: bool,
) -> Result<()> {
    let mut table = Table::new();

    if !quiet {
        table.set_header(vec![Cell::new("Key"), Cell::new("Value")]);
    }

    for (key, value) in rows {
        table.add_row(vec![Cell::new(*key), Cell::new(value)]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_schema_table<W: Write>(
    mut writer: W,
    columns: &[ColumnInfo],
    quiet: bool,
) -> Result<()> {
    let mut table = Table::new();

    if !quiet {
        table.set_header(vec![
            Cell::new("Column"),
            Cell::new("Type"),
            Cell::new("Nullable"),
        ]);
    }

    for column in columns {
        table.add_row(vec![
            Cell::new(&column.name),
            Cell::new(column.display_type()),
            Cell::new(if column.nullable { "Yes" } else { "No" }),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}
