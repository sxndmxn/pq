//! JSON and JSONL output formatting

use anyhow::Result;
use arrow::array::RecordBatch;
use serde::Serialize;
use serde_json::{Map, Value};

/// Convert a record batch to JSON rows
fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Map<String, Value>>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut row = Map::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let value_str = arrow::util::display::array_value_to_string(col, row_idx)?;

            // Try to parse as number or bool, otherwise keep as string
            let value = if value_str == "null" || value_str.is_empty() {
                Value::Null
            } else if let Ok(n) = value_str.parse::<i64>() {
                Value::Number(n.into())
            } else if let Ok(n) = value_str.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map_or_else(|| Value::String(value_str.clone()), Value::Number)
            } else if value_str == "true" {
                Value::Bool(true)
            } else if value_str == "false" {
                Value::Bool(false)
            } else {
                Value::String(value_str)
            };

            row.insert(field.name().clone(), value);
        }
        rows.push(row);
    }

    Ok(rows)
}

/// Print record batches as a JSON array
pub fn print_batches(batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        println!("[]");
        return Ok(());
    }

    let mut all_rows = Vec::new();
    for batch in batches {
        all_rows.extend(batch_to_json_rows(batch)?);
    }

    let json = serde_json::to_string_pretty(&all_rows)?;
    println!("{json}");
    Ok(())
}

/// Print record batches as JSONL (one JSON object per line)
pub fn print_batches_jsonl(batches: &[RecordBatch]) -> Result<()> {
    for batch in batches {
        for row in batch_to_json_rows(batch)? {
            let json = serde_json::to_string(&row)?;
            println!("{json}");
        }
    }
    Ok(())
}

/// Print a single value as JSON
#[allow(dead_code)]
pub fn print_value<T: Serialize>(value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    println!("{json}");
    Ok(())
}

/// Print schema as JSON array
pub fn print_schema(columns: &[(String, String, bool)]) {
    #[derive(Serialize)]
    struct Column {
        name: String,
        #[serde(rename = "type")]
        dtype: String,
        nullable: bool,
    }

    let cols: Vec<_> = columns
        .iter()
        .map(|(name, dtype, nullable)| Column {
            name: name.clone(),
            dtype: dtype.clone(),
            nullable: *nullable,
        })
        .collect();

    // Safe to use expect here as we control the input - it's always serializable
    #[allow(clippy::expect_used)]
    let json = serde_json::to_string_pretty(&cols).expect("schema is always serializable");
    println!("{json}");
}
