//! Column statistics command

use crate::cli::args::StatsArgs;
use crate::dataset::Dataset;
use crate::{commands, engine, output, Result};

pub fn run(args: StatsArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        let path = source.path();
        commands::print_source_header(&dataset, path, args.quiet);
        let rows = engine::stats::column_stats(path, args.column.as_deref())?;
        output::write_stats(args.output, args.quiet, &rows)?;
    }
    Ok(())
}
