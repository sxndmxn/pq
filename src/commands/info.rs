//! File metadata command

use crate::cli::args::{InfoArgs, OutputFormat};
use crate::dataset::Dataset;
use crate::{commands, engine, output, Result};

pub fn run(args: InfoArgs) -> Result<()> {
    let mut all_info = Vec::new();
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        let path = source.path();
        let info = engine::parquet::file_info(path)?;

        match args.output {
            OutputFormat::Table => {
                commands::print_source_header(&dataset, path, args.quiet);
                output::write_file_infos(args.output, args.quiet, &[info])?;
            }
            OutputFormat::Json | OutputFormat::Csv | OutputFormat::Jsonl => all_info.push(info),
        }
    }

    if matches!(
        args.output,
        OutputFormat::Json | OutputFormat::Jsonl | OutputFormat::Csv
    ) && !all_info.is_empty()
    {
        output::write_file_infos(args.output, args.quiet, &all_info)?;
    }

    Ok(())
}
