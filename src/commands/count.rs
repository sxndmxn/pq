//! Row count command

use crate::error::{PqError, ResultExt};
use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::PathBuf;

pub fn run(paths: &[PathBuf], quiet: bool) -> Result<()> {
    let mut grand_total: i64 = 0;

    for path in paths {
        let file = File::open(path).with_path_context(path)?;
        let reader = SerializedFileReader::new(file).map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("magic") || msg.contains("not a valid parquet") {
                PqError::invalid_parquet(path, &e)
            } else if msg.contains("eof") || msg.contains("truncat") {
                PqError::corrupted(path, &e)
            } else {
                PqError::read_error(path, &e)
            }
        })?;
        let count = reader.metadata().file_metadata().num_rows();

        if quiet {
            println!("{count}");
        } else if paths.len() > 1 {
            println!("{}: {count}", path.display());
        } else {
            println!("{count}");
        }

        grand_total += count;
    }

    if paths.len() > 1 && !quiet {
        println!("Total: {grand_total}");
    }

    Ok(())
}
