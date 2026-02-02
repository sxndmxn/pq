//! Row count command

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::PathBuf;

pub fn run(paths: &[PathBuf], quiet: bool) -> Result<()> {
    let mut grand_total: i64 = 0;

    for path in paths {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
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
