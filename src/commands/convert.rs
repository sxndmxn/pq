//! Format conversion command

use crate::api;
use crate::cli::args::ConvertArgs;
use crate::dataset::InputFile;
use crate::Result;

pub fn run(args: ConvertArgs) -> Result<()> {
    let input = InputFile::from_input(args.input)?;
    api::convert(input.path(), args.output_path.as_path())
}
