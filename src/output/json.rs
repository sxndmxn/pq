//! JSON and JSONL output formatting

use crate::engine::parquet::ColumnInfo;
use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::json::writer::{JsonArray, LineDelimited};
use arrow::json::WriterBuilder;
use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};

fn encode_json_array(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());

    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;

    Ok(writer.into_inner())
}

pub fn write_json<W: Write>(mut writer: W, batches: &[RecordBatch]) -> Result<()> {
    let json = encode_json_array(batches)?;
    let value: Value = serde_json::from_slice(&json)?;
    serde_json::to_writer_pretty(&mut writer, &value)?;
    writer.flush()?;
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
    writer.into_inner().flush()?;
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
pub fn print_value<T: Serialize>(value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    println!("{json}");
    Ok(())
}

/// Print schema as JSON array
pub fn print_schema(columns: &[ColumnInfo]) -> Result<()> {
    #[derive(Serialize)]
    struct Column {
        name: String,
        #[serde(rename = "type")]
        dtype: String,
        nullable: bool,
    }

    let cols: Vec<_> = columns
        .iter()
        .map(|column| Column {
            name: column.name.clone(),
            dtype: column.type_name.clone(),
            nullable: column.nullable,
        })
        .collect();

    let json = serde_json::to_string_pretty(&cols)?;
    println!("{json}");
    Ok(())
}

/// Print schema as JSONL (one JSON object per line)
pub fn print_schema_jsonl(columns: &[ColumnInfo]) -> Result<()> {
    #[derive(Serialize)]
    struct Column {
        name: String,
        #[serde(rename = "type")]
        dtype: String,
        nullable: bool,
    }

    for column in columns {
        let row = Column {
            name: column.name.clone(),
            dtype: column.type_name.clone(),
            nullable: column.nullable,
        };
        println!("{}", serde_json::to_string(&row)?);
    }

    Ok(())
}
