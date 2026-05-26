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
    let output_format: output::OutputFormat = output.into();
    let results = api::stats(&dataset, column.as_deref())?;

    if output_format.is_table() {
        for result in results {
            commands::print_source_header(&dataset, &result.path, quiet);
            output::write_stats(output_format, quiet, &result.rows)?;
        }
    } else {
        output::write_stats_results(output_format, quiet, &results)?;
    }

    Ok(())
}
