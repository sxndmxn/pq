//! Head and tail commands

use crate::cli::args::{HeadArgs, TailArgs};
use crate::dataset::Dataset;
use crate::{commands, engine, output, Result};
use std::path::Path;

pub fn run_head(args: HeadArgs) -> Result<()> {
    run_scan(
        args.inputs,
        args.rows,
        args.output,
        args.quiet,
        engine::parquet::read_head,
    )
}

pub fn run_tail(args: TailArgs) -> Result<()> {
    run_scan(
        args.inputs,
        args.rows,
        args.output,
        args.quiet,
        engine::parquet::read_tail,
    )
}

fn run_scan(
    inputs: Vec<std::path::PathBuf>,
    rows: usize,
    output_format: crate::OutputFormat,
    quiet: bool,
    scan: fn(&Path, usize) -> Result<Vec<arrow::array::RecordBatch>>,
) -> Result<()> {
    let dataset = Dataset::from_inputs(inputs)?;

    for source in dataset.sources() {
        commands::print_source_header(&dataset, source.path(), quiet);
        let batches = scan(source.path(), rows)?;
        output::write_batches(output_format, quiet, &batches)?;
    }

    Ok(())
}
