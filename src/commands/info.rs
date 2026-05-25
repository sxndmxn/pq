//! File metadata command

use crate::api;
use crate::cli::args::{InfoArgs, OutputFormat};
use crate::dataset::Dataset;
use crate::{commands, output, Result};

pub fn run(args: InfoArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;
    let infos = api::info(&dataset)?;

    match args.output {
        OutputFormat::Table => {
            for info in infos {
                commands::print_source_header(&dataset, info.path(), args.quiet);
                output::write_file_infos(args.output, args.quiet, &[info])?;
            }
        }
        OutputFormat::Json | OutputFormat::Csv | OutputFormat::Jsonl => {
            if !infos.is_empty() {
                output::write_file_infos(args.output, args.quiet, &infos)?;
            }
        }
    }

    Ok(())
}
