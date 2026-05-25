//! CSV output formatting

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder;
use std::io::Write;

pub fn write_batches<W: Write>(
    mut writer: W,
    batches: &[RecordBatch],
    include_header: bool,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    for (index, batch) in batches.iter().enumerate() {
        let mut csv_writer = WriterBuilder::new()
            .with_header(include_header && index == 0)
            .build(&mut writer);
        csv_writer.write(batch)?;
    }

    writer.flush()?;
    Ok(())
}

/// Write record batches as CSV to a file
pub fn write_batches_to_file(batches: &[RecordBatch], path: &std::path::Path) -> Result<()> {
    if batches.is_empty() {
        std::fs::File::create(path)?;
        return Ok(());
    }

    let file = std::fs::File::create(path)?;
    write_batches(file, batches, true)
}
