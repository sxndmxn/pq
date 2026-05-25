//! JSON and JSONL output formatting

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::json::writer::{JsonArray, LineDelimited};
use arrow::json::WriterBuilder;
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::io::{self, Write};

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

/// Print record batches as a JSON array
pub fn print_batches(batches: &[RecordBatch]) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    write_json(&mut handle, batches)?;
    writeln!(handle)?;
    Ok(())
}

/// Print record batches as JSONL (one JSON object per line)
pub fn print_batches_jsonl(batches: &[RecordBatch]) -> Result<()> {
    let stdout = io::stdout();
    let handle = stdout.lock();
    write_jsonl(handle, batches)
}

/// Print a single value as JSON
pub fn print_value<T: Serialize + ?Sized>(value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    println!("{json}");
    Ok(())
}

pub fn print_json_lines<T: Serialize>(values: &[T]) -> Result<()> {
    for value in values {
        println!("{}", serde_json::to_string(value)?);
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
