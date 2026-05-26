//! Column statistics command

use crate::api;
use crate::cli::args::StatsArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: StatsArgs) -> Result<()> {
    let StatsArgs {
        inputs,
        column,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let output = output.into();

    for result in api::stats(&dataset, column.as_deref())? {
        commands::print_source_header(&dataset, &result.path, quiet);
        output::write_stats(output, quiet, &result.rows)?;
    }
    Ok(())
}
