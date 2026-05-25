//! Schema display command

use crate::api;
use crate::cli::args::SchemaArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: SchemaArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;

    for result in api::schema(&dataset)? {
        commands::print_source_header(&dataset, &result.path, args.quiet);
        output::write_schema(args.output, args.quiet, &result.columns)?;
    }
    Ok(())
}
