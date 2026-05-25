//! JSON and JSONL output formatting

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::json::writer::{JsonArray, LineDelimited};
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

pub fn write_batches_to_file(batches: &[RecordBatch], path: &std::path::Path) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    write_json(writer, batches)
}

pub fn write_batches_jsonl_to_file(batches: &[RecordBatch], path: &std::path::Path) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    write_jsonl(writer, batches)
}
