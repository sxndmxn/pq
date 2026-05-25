//! Format conversion command

use crate::cli::args::ConvertArgs;
use crate::dataset::Dataset;
use crate::Result;

pub fn run(args: ConvertArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(vec![args.input])?;
    let input = dataset.sources()[0].path();
    let batches = crate::engine::parquet::read_batches(input)?;
    crate::output::write_batches_to_path(args.output_path.as_path(), &batches)
}
