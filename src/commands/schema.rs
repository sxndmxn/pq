//! Schema display command

use crate::cli::args::SchemaArgs;
use crate::dataset::Dataset;
use crate::{commands, engine, output, Result};

pub fn run(args: SchemaArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        commands::print_source_header(&dataset, source.path(), args.quiet);
        let columns = engine::parquet::schema_columns(source.path())?;
        output::write_schema(args.output, args.quiet, &columns)?;
    }
    Ok(())
}
