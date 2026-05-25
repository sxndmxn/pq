//! Head and tail commands

use crate::cli::args::{HeadArgs, TailArgs};
use crate::dataset::Dataset;
use crate::{engine, output, plan, Result};

pub fn run(args: HeadArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let plan = plan::scan::ScanPlan::head(args.rows);

    for source in dataset.sources() {
        if dataset.is_multi_source() && !args.quiet {
            println!("==> {} <==", source.path().display());
        }

        let single_source = Dataset::from_source(source.clone());
        let batches = engine::scan::scan(&single_source, &plan)?;
        output::write_batches(args.output, args.quiet, &batches)?;
    }

    Ok(())
}

pub fn run_tail(args: TailArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let plan = plan::scan::ScanPlan::tail(args.rows);

    for source in dataset.sources() {
        if dataset.is_multi_source() && !args.quiet {
            println!("==> {} <==", source.path().display());
        }

        let single_source = Dataset::from_source(source.clone());
        let batches = engine::scan::scan(&single_source, &plan)?;
        output::write_batches(args.output, args.quiet, &batches)?;
    }

    Ok(())
}
