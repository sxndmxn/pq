//! CSV output formatting

use crate::Result;
use arrow::array::RecordBatch;
use arrow::csv::{Writer, WriterBuilder};
use arrow::error::ArrowError;
use std::fs::File;
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

pub struct BatchFileWriter {
    writer: Writer<File>,
}

impl BatchFileWriter {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: WriterBuilder::new().with_header(true).build(file),
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> std::result::Result<(), ArrowError> {
        self.writer.write(batch)
    }

    pub fn finish(&mut self) {}
}
