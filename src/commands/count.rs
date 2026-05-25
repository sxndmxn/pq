//! Row count command

use crate::api;
use crate::cli::args::CountArgs;
use crate::dataset::Dataset;
use crate::{output, Result};

pub fn run(args: CountArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let counts = api::count(&dataset)?;
    output::write_counts(args.quiet, dataset.is_multi_source(), &counts)
}
