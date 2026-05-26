use crate::model::{ColumnInfo, SchemaResult};
use crate::output::csv_support::escape_csv;
use crate::Result;
use std::io::Write;

pub fn write_csv<W: Write>(
    mut writer: W,
    columns: &[ColumnInfo],
    include_header: bool,
) -> Result<()> {
    if include_header {
        writeln!(writer, "column,type,nullable")?;
    }

    for column in columns {
        writeln!(
            writer,
            "{},{},{}",
            escape_csv(&column.name),
            escape_csv(&column.display_type()),
            column.nullable
        )?;
    }

    Ok(())
}

pub fn write_csv_results<W: Write>(
    mut writer: W,
    results: &[SchemaResult],
    include_header: bool,
) -> Result<()> {
    if include_header {
        writeln!(writer, "file,column,type,nullable")?;
    }

    for result in results {
        for column in &result.columns {
            writeln!(
                writer,
                "{},{},{},{}",
                escape_csv(&result.path.display().to_string()),
                escape_csv(&column.name),
                escape_csv(&column.display_type()),
                column.nullable
            )?;
        }
    }

    Ok(())
}
