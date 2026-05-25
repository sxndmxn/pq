//! Head and tail commands

use crate::api::{self, ScanKind, ScanOptions};
use crate::cli::args::{HeadArgs, TailArgs};
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run_head(args: HeadArgs) -> Result<()> {
    run_scan(
        args.inputs,
        ScanKind::Head,
        args.rows,
        args.output.into(),
        args.quiet,
    )
}

pub fn run_tail(args: TailArgs) -> Result<()> {
    run_scan(
        args.inputs,
        ScanKind::Tail,
        args.rows,
        args.output.into(),
        args.quiet,
    )
}

fn run_scan(
    inputs: Vec<std::path::PathBuf>,
    kind: ScanKind,
    rows: usize,
    output_format: crate::output::OutputFormat,
    quiet: bool,
) -> Result<()> {
    let dataset = Dataset::from_inputs(inputs)?;

    for result in api::scan(&dataset, kind, ScanOptions { rows })? {
        commands::print_source_header(&dataset, &result.path, quiet);
        output::write_batches(output_format, quiet, &result.batches)?;
    }

    Ok(())
}
