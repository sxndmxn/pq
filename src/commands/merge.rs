//! File merging command

use crate::api;
use crate::cli::args::MergeArgs;
use crate::dataset::Dataset;
use crate::Result;

pub fn run(args: MergeArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    api::merge(&dataset, &args.output)
}
