use anyhow::Result;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("test.parquet")
}

fn temp_path(name: &str, extension: &str) -> Result<std::path::PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("system clock error: {error}"))?
        .as_nanos();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(std::env::temp_dir().join(format!("pq_{name}_{unique}_{counter}.{extension}")))
}

fn write_parquet(
    path: &std::path::Path,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<()> {
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

#[test]
fn file_info_comes_from_public_api() -> Result<()> {
    let dataset = pq::dataset_from_inputs(vec![fixture_path()])?;
    let infos = pq::info(&dataset)?;
    let info = &infos[0];

    assert_eq!(info.num_rows, 5);
    assert_eq!(info.num_columns, 4);
    assert_eq!(info.num_row_groups, 1);
    assert!(info
        .path
        .display()
        .to_string()
        .ends_with("tests/fixtures/test.parquet"));

    Ok(())
}

#[test]
fn column_stats_come_from_public_api() -> Result<()> {
    let dataset = pq::dataset_from_inputs(vec![fixture_path()])?;
    let results = pq::stats(&dataset, Some("id"))?;
    let rows = &results[0].rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column, "id");
    assert_eq!(
        rows[0].min.as_ref().map(ToString::to_string).as_deref(),
        Some("1")
    );
    assert_eq!(
        rows[0].max.as_ref().map(ToString::to_string).as_deref(),
        Some("5")
    );

    Ok(())
}

#[test]
fn library_api_exposes_typed_schema_results() -> Result<()> {
    let dataset = pq::dataset_from_inputs(vec![fixture_path()])?;
    let schema = pq::schema(&dataset)?;

    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].columns[0].name, "id");
    assert_eq!(
        schema[0].columns[0].column_type.physical,
        pq::PhysicalType::Int64
    );

    Ok(())
}

#[test]
fn merge_comes_from_public_api() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )?;
    let left = temp_path("merge_left", "parquet")?;
    let right = temp_path("merge_right", "parquet")?;
    let output = temp_path("merge_output", "parquet")?;

    write_parquet(&left, Arc::clone(&schema), std::slice::from_ref(&batch))?;
    write_parquet(&right, schema, &[batch])?;

    let merge_dataset = pq::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    pq::merge(&merge_dataset, &output)?;

    let output_dataset = pq::dataset_from_inputs(vec![output.clone()])?;
    let count = pq::count(&output_dataset)?;
    assert_eq!(count.total_rows, 4);

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    fs::remove_file(output)?;
    Ok(())
}
