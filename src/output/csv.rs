//! CSV output formatting

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder;
use std::io::{self, Write};

/// Print record batches as CSV to stdout
pub fn print_batches(batches: &[RecordBatch], include_header: bool) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let stdout = io::stdout();
    let mut handle = stdout.lock();

    for (i, batch) in batches.iter().enumerate() {
        let mut writer = WriterBuilder::new()
            .with_header(include_header && i == 0)
            .build(&mut handle);

        writer.write(batch)?;
    }

    handle.flush()?;
    Ok(())
}

/// Write record batches as CSV to a file
pub fn write_batches_to_file(batches: &[RecordBatch], path: &std::path::Path) -> Result<()> {
    if batches.is_empty() {
        // Create empty file
        std::fs::File::create(path)?;
        return Ok(());
    }

    let file = std::fs::File::create(path)?;
    let mut writer = WriterBuilder::new().with_header(true).build(file);

    for batch in batches {
        writer.write(batch)?;
    }

    Ok(())
}

/// Print schema as CSV
pub fn print_schema(columns: &[(String, String, bool)], include_header: bool) {
    if include_header {
        println!("column,type,nullable");
    }
    for (name, dtype, nullable) in columns {
        println!("{name},{dtype},{nullable}");
    }
}
