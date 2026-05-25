//! CLI integration tests for pq

use anyhow::Result;
use std::fs;
use std::process::Command;

fn pq() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pq"))
}

fn fixture_path() -> String {
    format!("{}/tests/fixtures/test.parquet", env!("CARGO_MANIFEST_DIR"))
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
    let output = pq()
        .args(["head", &fixture_path(), "-n", "2"])
        .output()?;
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
    let output = pq()
        .args(["tail", &fixture_path(), "-n", "2"])
        .output()?;
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
        .args(["convert", &fixture_path(), &output_path.display().to_string()])
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
        .args(["convert", &fixture_path(), &output_path.display().to_string()])
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
