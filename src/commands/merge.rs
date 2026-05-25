//! File merging command

use crate::cli::args::MergeArgs;
use crate::dataset::Dataset;
use crate::Result;

pub fn run(args: MergeArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let paths: Vec<_> = dataset
        .sources()
        .iter()
        .map(|source| source.path())
        .collect();
    crate::engine::parquet::merge_files(&paths, &args.output)
}
