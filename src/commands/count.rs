//! Row count command

use crate::cli::args::CountArgs;
use crate::dataset::Dataset;
use crate::{engine, Result};

pub fn run(args: CountArgs) -> Result<()> {
    let mut grand_total: i64 = 0;
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        let count = engine::parquet::row_count(source.path())?;

        if args.quiet {
            println!("{count}");
        } else if dataset.is_multi_source() {
            println!("{}: {count}", source.path().display());
        } else {
            println!("{count}");
        }

        grand_total += count;
    }

    if dataset.is_multi_source() && !args.quiet {
        println!("Total: {grand_total}");
    }

    Ok(())
}
