//! Integration tests for pq CLI

use std::process::Command;

fn pq() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pq"))
}

fn fixture_path() -> String {
    format!("{}/tests/fixtures/test.parquet", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn test_help() {
    let output = pq().arg("--help").output().expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pq"));
    assert!(stdout.contains("schema"));
    assert!(stdout.contains("head"));
    assert!(stdout.contains("query"));
}

#[test]
fn test_version() {
    let output = pq().arg("--version").output().expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pq"));
}

#[test]
fn test_schema() {
    let output = pq()
        .args(["schema", &fixture_path()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Column"));
    assert!(stdout.contains("Type"));
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
}

#[test]
fn test_schema_json() {
    let output = pq()
        .args(["schema", &fixture_path(), "-o", "json"])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"name\""));
    assert!(stdout.contains("\"type\""));
}

#[test]
fn test_head() {
    let output = pq()
        .args(["head", &fixture_path()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("Bob"));
}

#[test]
fn test_head_with_limit() {
    let output = pq()
        .args(["head", &fixture_path(), "-n", "2"])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("Bob"));
    // Should not contain Charlie if we only asked for 2 rows
}

#[test]
fn test_head_json() {
    let output = pq()
        .args(["head", &fixture_path(), "-o", "json"])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should be valid JSON array
    assert!(stdout.starts_with('['));
    assert!(stdout.contains("\"name\""));
}

#[test]
fn test_tail() {
    let output = pq()
        .args(["tail", &fixture_path(), "-n", "2"])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Diana") || stdout.contains("Eve"));
}

#[test]
fn test_count() {
    let output = pq()
        .args(["count", &fixture_path()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.trim() == "5");
}

#[test]
fn test_stats() {
    let output = pq()
        .args(["stats", &fixture_path()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Column"));
    assert!(stdout.contains("Min"));
    assert!(stdout.contains("Max"));
    assert!(stdout.contains("id"));
}

#[test]
fn test_info() {
    let output = pq()
        .args(["info", &fixture_path()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Rows"));
    assert!(stdout.contains("Columns"));
    assert!(stdout.contains("Compression"));
}

#[test]
fn test_query() {
    let output = pq()
        .args([
            "query",
            "SELECT name, amount FROM tbl WHERE amount > 200",
            &fixture_path(),
        ])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("name"));
    assert!(stdout.contains("amount"));
}

#[test]
fn test_query_json() {
    let output = pq()
        .args([
            "query",
            "SELECT COUNT(*) as cnt FROM tbl",
            &fixture_path(),
            "-o",
            "json",
        ])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cnt"));
}

#[test]
fn test_convert_csv() {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("pq_test_output.csv");

    let output = pq()
        .args(["convert", &fixture_path(), output_path.to_str().unwrap()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    assert!(output_path.exists());

    let contents = std::fs::read_to_string(&output_path).unwrap();
    assert!(contents.contains("id,name,amount,active"));
    assert!(contents.contains("Alice"));

    // Cleanup
    std::fs::remove_file(&output_path).ok();
}

#[test]
fn test_convert_json() {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("pq_test_output.json");

    let output = pq()
        .args(["convert", &fixture_path(), output_path.to_str().unwrap()])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    assert!(output_path.exists());

    let contents = std::fs::read_to_string(&output_path).unwrap();
    assert!(contents.starts_with('['));
    assert!(contents.contains("\"name\""));

    // Cleanup
    std::fs::remove_file(&output_path).ok();
}

#[test]
fn test_merge() {
    let temp_dir = std::env::temp_dir();
    let output_path = temp_dir.join("pq_test_merged.parquet");

    let output = pq()
        .args([
            "merge",
            &fixture_path(),
            &fixture_path(),
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to execute");
    assert!(output.status.success());
    assert!(output_path.exists());

    // Verify merged file has doubled row count
    let count_output = pq()
        .args(["count", output_path.to_str().unwrap()])
        .output()
        .expect("failed to execute");
    let stdout = String::from_utf8_lossy(&count_output.stdout);
    assert!(stdout.trim() == "10");

    // Cleanup
    std::fs::remove_file(&output_path).ok();
}
