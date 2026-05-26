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
    let output_format: output::OutputFormat = output.into();
    let results = api::schema(&dataset)?;

    if output_format.is_table() {
        for result in results {
            commands::print_source_header(&dataset, &result.path, quiet);
            output::write_schema(output_format, quiet, &result.columns)?;
        }
    } else {
        output::write_schema_results(output_format, quiet, &results)?;
    }

    Ok(())
}
