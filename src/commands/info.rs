//! File metadata command

use crate::api;
use crate::cli::args::InfoArgs;
use crate::dataset::Dataset;
use crate::{output, Result};

pub fn run(args: InfoArgs) -> Result<()> {
    let InfoArgs {
        inputs,
        output,
        quiet,
    } = args;
    let dataset = Dataset::from_inputs(inputs)?;
    let infos = api::info(&dataset)?;
    output::write_file_infos(output.into(), quiet, &infos)
}
