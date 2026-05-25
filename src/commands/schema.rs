//! Schema display command

use crate::cli::args::SchemaArgs;
use crate::dataset::Dataset;
use crate::output::{csv, json, table};
use crate::{engine, Result};

pub fn run(args: SchemaArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        if dataset.is_multi_source() && !args.quiet {
            println!("==> {} <==", source.path().display());
        }

        let columns = engine::parquet::schema_columns(source.path())?;

        match args.output {
            crate::cli::args::OutputFormat::Table => table::print_schema_table(&columns, args.quiet),
            crate::cli::args::OutputFormat::Json | crate::cli::args::OutputFormat::Jsonl => {
                json::print_schema(&columns)?
            }
            crate::cli::args::OutputFormat::Csv => csv::print_schema(&columns, !args.quiet),
        }
    }
    Ok(())
}
