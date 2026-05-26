//! File metadata command

use crate::api;
use crate::cli::args::InfoArgs;
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: InfoArgs) -> Result<()> {
    let InfoArgs {
        inputs,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let output_format: output::OutputFormat = output.into();
    let infos = api::info(&dataset)?;
    if output_format.is_table() {
        for info in &infos {
            commands::print_source_header(&dataset, info.path(), quiet);
            output::write_file_infos(output_format, quiet, std::slice::from_ref(info))?;
        }
        Ok(())
    } else {
        output::write_file_infos(output_format, quiet, &infos)
    }
}
