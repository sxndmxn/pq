//! Head and tail commands

use crate::api;
use crate::cli::args::{HeadArgs, TailArgs};
use crate::dataset::Dataset;
use crate::{commands, output, PqError, Result, ScanKind, ScanOptions, ScanResult};
use arrow::datatypes::SchemaRef;
use std::path::PathBuf;

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
    let results = api::scan(&dataset, kind, ScanOptions { rows })?;

    if output_format.is_table() {
        for result in results {
            commands::print_source_header(&dataset, &result.path, quiet);
            output::write_batches(output_format, quiet, &result.batches)?;
        }
    } else {
        validate_compatible_schemas(&results)?;
        let batches = results
            .into_iter()
            .flat_map(|result| result.batches)
            .collect::<Vec<_>>();
        output::write_batches(output_format, quiet, &batches)?;
    }

    Ok(())
}

fn validate_compatible_schemas(results: &[ScanResult]) -> Result<()> {
    let mut first_schema: Option<(PathBuf, SchemaRef)> = None;

    for result in results {
        for batch in &result.batches {
            let schema = batch.schema();
            if let Some((first_path, expected_schema)) = &first_schema {
                if schema.as_ref() != expected_schema.as_ref() {
                    return Err(PqError::SchemaMismatch {
                        file1: first_path.display().to_string(),
                        file2: result.path.display().to_string(),
                        details: "Cannot combine scan results with different schemas for structured output"
                            .to_string(),
                    });
                }
            } else {
                first_schema = Some((result.path.clone(), schema));
            }
        }
    }

    Ok(())
}
