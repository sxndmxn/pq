//! Column statistics command

use crate::api;
use crate::cli::args::StatsArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: StatsArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;

    for result in api::stats(&dataset, args.column.as_deref())? {
        commands::print_source_header(&dataset, &result.path, args.quiet);
        output::write_stats(args.output, args.quiet, &result.rows)?;
    }
    Ok(())
}
