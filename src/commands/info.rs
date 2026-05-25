//! File metadata command

use crate::api;
use crate::cli::args::InfoArgs;
use crate::dataset::Dataset;
use crate::{output, Result};

pub fn run(args: InfoArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let infos = api::info(&dataset)?;
    output::write_file_infos(args.output, args.quiet, &infos)
}
