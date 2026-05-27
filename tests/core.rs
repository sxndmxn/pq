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

fn temp_dir(name: &str) -> Result<std::path::PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("system clock error: {error}"))?
        .as_nanos();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pq_{name}_{unique}_{counter}"));
    fs::create_dir_all(&dir)?;
    Ok(dir)
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
fn empty_dataset_input_is_typed_error() -> Result<()> {
    let Err(error) = pq::dataset_from_inputs(Vec::new()) else {
        return Err(anyhow::anyhow!("empty dataset input should fail"));
    };

    assert!(matches!(error, pq::PqError::NoInputFiles));
    Ok(())
}

#[test]
fn dataset_glob_expansion_is_sorted() -> Result<()> {
    let dir = temp_dir("dataset_glob_order")?;
    let first = dir.join("a.parquet");
    let second = dir.join("b.parquet");
    fs::write(&second, b"PAR1")?;
    fs::write(&first, b"PAR1")?;

    let dataset = pq::dataset_from_inputs(vec![dir.join("*.parquet")])?;
    let paths = dataset.paths().collect::<Vec<_>>();

    assert_eq!(paths, vec![first.as_path(), second.as_path()]);

    fs::remove_file(first)?;
    fs::remove_file(second)?;
    fs::remove_dir(dir)?;
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
    assert_eq!(
        info.compression,
        pq::CompressionSummary::Single(pq::CompressionCodec::Snappy)
    );
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
fn missing_stats_column_is_typed_error() -> Result<()> {
    let dataset = pq::dataset_from_inputs(vec![fixture_path()])?;
    let Err(error) = pq::stats(&dataset, Some("missing_column")) else {
        return Err(anyhow::anyhow!("missing stats column should fail"));
    };

    assert!(matches!(error, pq::PqError::ColumnNotFound { .. }));

    Ok(())
}

#[test]
fn binary_stats_display_is_deterministic() {
    let binary_stats = pq::ColumnStats {
        column: "payload".to_string(),
        column_type: pq::ColumnType {
            physical: pq::PhysicalType::ByteArray,
            logical: None,
        },
        null_count: 0,
        min: None,
        max: None,
    };
    let string_stats = pq::ColumnStats {
        column: "name".to_string(),
        column_type: pq::ColumnType {
            physical: pq::PhysicalType::ByteArray,
            logical: Some(pq::LogicalTypeKind::String),
        },
        null_count: 0,
        min: None,
        max: None,
    };

    assert_eq!(
        binary_stats.display_stat_value(&pq::StatValue::Binary(vec![0xff, b'a'])),
        "ff61"
    );
    assert_eq!(
        string_stats.display_stat_value(&pq::StatValue::Binary(b"Alice".to_vec())),
        "Alice"
    );
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

#[test]
fn merge_schema_mismatch_does_not_truncate_existing_output() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let right_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        false,
    )]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )?;
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
    )?;
    let left = temp_path("mismatch_left", "parquet")?;
    let right = temp_path("mismatch_right", "parquet")?;
    let output = temp_path("mismatch_output", "parquet")?;

    write_parquet(&left, left_schema, &[left_batch])?;
    write_parquet(&right, right_schema, &[right_batch])?;
    fs::write(&output, b"sentinel")?;

    let dataset = pq::dataset_from_inputs(vec![left.clone(), right.clone()])?;
    let Err(error) = pq::merge(&dataset, &output) else {
        fs::remove_file(left)?;
        fs::remove_file(right)?;
        fs::remove_file(output)?;
        return Err(anyhow::anyhow!("schema mismatch should fail"));
    };

    assert!(matches!(error, pq::PqError::SchemaMismatch { .. }));
    assert_eq!(fs::read(&output)?, b"sentinel");

    fs::remove_file(left)?;
    fs::remove_file(right)?;
    fs::remove_file(output)?;
    Ok(())
}
