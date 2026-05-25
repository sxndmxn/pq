//! Row count command

use crate::api;
use crate::cli::args::CountArgs;
use crate::dataset::Dataset;
use crate::Result;

pub fn run(args: CountArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let counts = api::count(&dataset)?;

    for entry in &counts.entries {
        if args.quiet || !dataset.is_multi_source() {
            println!("{}", entry.rows);
        } else {
            println!("{}: {}", entry.path.display(), entry.rows);
        }
    }

    if dataset.is_multi_source() && !args.quiet {
        println!("Total: {}", counts.total_rows);
    }

    Ok(())
}
