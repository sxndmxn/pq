//! JSON and JSONL output formatting

use crate::Result;
use arrow::array::RecordBatch;
use arrow::json::writer::{JsonArray, LineDelimited, Writer};
use arrow::json::WriterBuilder;
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

pub fn write_json<W: Write>(writer: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(writer);

    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn write_jsonl<W: Write>(writer: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(writer);

    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn write_value<W: Write, T: Serialize + ?Sized>(mut writer: W, value: &T) -> Result<()> {
    serde_json::to_writer_pretty(&mut writer, value)?;
    writeln!(writer)?;
    Ok(())
}

pub fn write_json_lines<W: Write, T: Serialize>(mut writer: W, values: &[T]) -> Result<()> {
    for value in values {
        serde_json::to_writer(&mut writer, value)?;
        writeln!(writer)?;
    }

    Ok(())
}

pub struct JsonBatchFileWriter {
    writer: Writer<BufWriter<File>, JsonArray>,
}

impl JsonBatchFileWriter {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        let file = File::create(path)?;
        let writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(BufWriter::new(file));
        Ok(Self { writer })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish()?;
        Ok(())
    }
}

pub struct JsonlBatchFileWriter {
    writer: Writer<BufWriter<File>, LineDelimited>,
}

impl JsonlBatchFileWriter {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        let file = File::create(path)?;
        let writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(BufWriter::new(file));
        Ok(Self { writer })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish()?;
        Ok(())
    }
}
