use crate::model::ColumnInfo;
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
