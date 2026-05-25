//! CLI integration tests for pq

use anyhow::Result;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn pq() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pq"))
}

fn fixture_path() -> String {
    format!("{}/tests/fixtures/test.parquet", env!("CARGO_MANIFEST_DIR"))
}

fn temp_path(name: &str, extension: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("pq_{name}_{unique}.{extension}"))
}

fn write_parquet(
    path: &Path,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    max_row_group_size: Option<usize>,
) -> Result<()> {
    let file = fs::File::create(path)?;
    let props = max_row_group_size.map(|size| {
        WriterProperties::builder()
            .set_max_row_group_size(size)
            .build()
    });
    let mut writer = ArrowWriter::try_new(file, schema, props)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

#[test]
fn test_help() -> Result<()> {
    let output = pq().arg("--help").output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pq"));
    assert!(stdout.contains("schema"));
    assert!(stdout.contains("head"));
    assert!(stdout.contains("stats"));
    Ok(())
}

#[test]
fn test_version() -> Result<()> {
    let output = pq().arg("--version").output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pq"));
    Ok(())
}

#[test]
fn test_schema() -> Result<()> {
    let output = pq().args(["schema", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Column"));
    assert!(stdout.contains("Type"));
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
    Ok(())
}

#[test]
fn test_schema_json() -> Result<()> {
    let output = pq()
        .args(["schema", &fixture_path(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"name\""));
    assert!(stdout.contains("\"type\""));
    Ok(())
}

#[test]
fn test_head() -> Result<()> {
    let output = pq().args(["head", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("Bob"));
    Ok(())
}

#[test]
fn test_head_with_limit() -> Result<()> {
    let output = pq().args(["head", &fixture_path(), "-n", "2"]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("Bob"));
    Ok(())
}

#[test]
fn test_head_json() -> Result<()> {
    let output = pq()
        .args(["head", &fixture_path(), "-o", "json"])
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with('['));
    assert!(stdout.contains("\"name\""));
    Ok(())
}

#[test]
fn test_tail() -> Result<()> {
    let output = pq().args(["tail", &fixture_path(), "-n", "2"]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Diana") || stdout.contains("Eve"));
    Ok(())
}

#[test]
fn test_count() -> Result<()> {
    let output = pq().args(["count", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim(), "5");
    Ok(())
}

#[test]
fn test_stats() -> Result<()> {
    let output = pq().args(["stats", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Column"));
    assert!(stdout.contains("Min"));
    assert!(stdout.contains("Max"));
    assert!(stdout.contains("id"));
    Ok(())
}

#[test]
fn test_info() -> Result<()> {
    let output = pq().args(["info", &fixture_path()]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Rows"));
    assert!(stdout.contains("Columns"));
    assert!(stdout.contains("Compression"));
    Ok(())
}

#[test]
fn test_convert_csv() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("pq_test_output.csv");

    let output = pq()
        .args([
            "convert",
            &fixture_path(),
            &output_path.display().to_string(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(output_path.exists());

    let contents = fs::read_to_string(&output_path)?;
    assert!(contents.contains("id,name,amount,active"));
    assert!(contents.contains("Alice"));

    let _ignored = fs::remove_file(&output_path);
    Ok(())
}

#[test]
fn test_convert_json() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("pq_test_output.json");

    let output = pq()
        .args([
            "convert",
            &fixture_path(),
            &output_path.display().to_string(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(output_path.exists());

    let contents = fs::read_to_string(&output_path)?;
    assert!(contents.starts_with('['));
    assert!(contents.contains("\"name\""));

    let _ignored = fs::remove_file(&output_path);
    Ok(())
}

#[test]
fn test_merge() -> Result<()> {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("pq_test_merged.parquet");

    let output = pq()
        .args([
            "merge",
            &fixture_path(),
            &fixture_path(),
            "-o",
            &output_path.display().to_string(),
        ])
        .output()?;
    assert!(output.status.success());
    assert!(output_path.exists());

    let count_output = pq()
        .args(["count", &output_path.display().to_string()])
        .output()?;
    let stdout = String::from_utf8_lossy(&count_output.stdout);
    assert_eq!(stdout.trim(), "10");

    let _ignored = fs::remove_file(&output_path);
    Ok(())
}

#[test]
fn test_convert_json_preserves_types() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("0012"),
                Some(""),
                Some("true"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(12), None, Some(-7), Some(0)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])) as ArrayRef,
        ],
    )?;

    let input_path = temp_path("typed_input", "parquet");
    let json_path = temp_path("typed_output", "json");
    let jsonl_path = temp_path("typed_output", "jsonl");
    write_parquet(&input_path, schema, &[batch], None)?;

    let json_output = pq()
        .args([
            "convert",
            &input_path.display().to_string(),
            &json_path.display().to_string(),
        ])
        .output()?;
    assert!(json_output.status.success());

    let rows: serde_json::Value = serde_json::from_str(&fs::read_to_string(&json_path)?)?;
    let rows = rows.as_array().expect("json output should be an array");
    assert_eq!(
        rows[0]["code"],
        serde_json::Value::String("0012".to_string())
    );
    assert_eq!(rows[1]["code"], serde_json::Value::String(String::new()));
    assert_eq!(
        rows[2]["code"],
        serde_json::Value::String("true".to_string())
    );
    assert_eq!(rows[0]["qty"], serde_json::json!(12));
    assert_eq!(rows[1]["qty"], serde_json::Value::Null);
    assert_eq!(rows[0]["flag"], serde_json::json!(true));
    assert_eq!(rows[2]["flag"], serde_json::Value::Null);

    let jsonl_output = pq()
        .args([
            "convert",
            &input_path.display().to_string(),
            &jsonl_path.display().to_string(),
        ])
        .output()?;
    assert!(jsonl_output.status.success());

    let lines: Vec<serde_json::Value> = fs::read_to_string(&jsonl_path)?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(lines[1]["code"], serde_json::Value::String(String::new()));
    assert_eq!(lines[1]["qty"], serde_json::Value::Null);
    assert_eq!(
        lines[2]["code"],
        serde_json::Value::String("true".to_string())
    );
    assert_eq!(lines[2]["flag"], serde_json::Value::Null);

    let _ignored = fs::remove_file(&input_path);
    let _ignored = fs::remove_file(&json_path);
    let _ignored = fs::remove_file(&jsonl_path);
    Ok(())
}

#[test]
fn test_stats_aggregates_across_row_groups() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![5, 10, 1, 3])) as ArrayRef],
    )?;
    let input_path = temp_path("stats_groups", "parquet");
    write_parquet(&input_path, schema, &[batch], Some(2))?;

    let output = pq()
        .args(["stats", &input_path.display().to_string(), "-o", "json"])
        .output()?;
    assert!(output.status.success());

    let rows: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        rows[0]["column"],
        serde_json::Value::String("value".to_string())
    );
    assert_eq!(rows[0]["min"], serde_json::Value::String("1".to_string()));
    assert_eq!(rows[0]["max"], serde_json::Value::String("10".to_string()));

    let _ignored = fs::remove_file(&input_path);
    Ok(())
}

#[test]
fn test_schema_jsonl_outputs_one_object_per_line() -> Result<()> {
    let output = pq()
        .args(["schema", &fixture_path(), "-o", "jsonl"])
        .output()?;
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<_> = stdout.lines().collect();
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|line| line.starts_with('{')));
    assert!(lines.iter().all(|line| !line.starts_with('[')));

    Ok(())
}
