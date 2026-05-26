use crate::model::ColumnStats;
use crate::output::csv_support::escape_csv;
use comfy_table::{Cell, Table};
use std::io::Write;

pub fn write_table<W: Write>(
    mut writer: W,
    rows: &[ColumnStats],
    quiet: bool,
) -> std::io::Result<()> {
    let mut table = Table::new();
    if !quiet {
        table.set_header(vec!["Column", "Type", "Nulls", "Min", "Max"]);
    }

    for row in rows {
        table.add_row(vec![
            Cell::new(&row.column),
            Cell::new(row.display_type()),
            Cell::new(row.null_count),
            Cell::new(
                row.min
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |value| row.display_stat_value(value)),
            ),
            Cell::new(
                row.max
                    .as_ref()
                    .map_or_else(|| "N/A".to_string(), |value| row.display_stat_value(value)),
            ),
        ]);
    }

    writeln!(writer, "{table}")
}

pub fn write_csv<W: Write>(
    mut writer: W,
    rows: &[ColumnStats],
    include_header: bool,
) -> std::io::Result<()> {
    if include_header {
        writeln!(writer, "column,type,null_count,min,max")?;
    }

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{}",
            escape_csv(&row.column),
            escape_csv(&row.display_type()),
            row.null_count,
            escape_csv(
                &row.min
                    .as_ref()
                    .map_or_else(String::new, |value| row.display_stat_value(value))
            ),
            escape_csv(
                &row.max
                    .as_ref()
                    .map_or_else(String::new, |value| row.display_stat_value(value))
            ),
        )?;
    }

    Ok(())
}
