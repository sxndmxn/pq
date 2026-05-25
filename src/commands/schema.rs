//! Schema display command

use crate::api;
use crate::cli::args::SchemaArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: SchemaArgs) -> Result<()> {
    let SchemaArgs {
        inputs,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let output = output.into();

    for result in api::schema(&dataset)? {
        commands::print_source_header(&dataset, &result.path, quiet);
        output::write_schema(output, quiet, &result.columns)?;
    }
    Ok(())
}
