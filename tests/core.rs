use anyhow::Result;

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("test.parquet")
}

#[test]
fn file_info_comes_from_shared_engine() -> Result<()> {
    let info = pq::engine::parquet::file_info(&fixture_path())?;

    assert_eq!(info.num_rows, 5);
    assert_eq!(info.num_columns, 4);
    assert_eq!(info.num_row_groups, 1);
    assert!(info.file.ends_with("tests/fixtures/test.parquet"));

    Ok(())
}

#[test]
fn column_stats_come_from_shared_engine() -> Result<()> {
    let rows = pq::engine::stats::column_stats(&fixture_path(), Some("id"))?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column, "id");
    assert_eq!(rows[0].min.as_deref(), Some("1"));
    assert_eq!(rows[0].max.as_deref(), Some("5"));

    Ok(())
}
