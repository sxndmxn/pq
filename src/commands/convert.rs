//! Format conversion command

use crate::api;
use crate::cli::args::ConvertArgs;
use crate::dataset::Dataset;
use crate::Result;

pub fn run(args: ConvertArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(vec![args.input])?;
    let input = dataset
        .paths()
        .next()
        .ok_or(crate::PqError::NoInputFiles)?;
    api::convert(input, args.output_path.as_path())
}
