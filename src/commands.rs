use crate::cli::args::Command;
use crate::dataset::Dataset;
use crate::Result;
use std::path::Path;

pub mod convert;
pub mod count;
pub mod info;
pub mod merge;
pub mod scan;
pub mod schema;
pub mod stats;

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::Schema(args) => schema::run(args),
        Command::Head(args) => scan::run_head(args),
        Command::Tail(args) => scan::run_tail(args),
        Command::Count(args) => count::run(args),
        Command::Stats(args) => stats::run(args),
        Command::Convert(args) => convert::run(args),
        Command::Merge(args) => merge::run(args),
        Command::Info(args) => info::run(args),
    }
}

fn print_source_header(dataset: &Dataset, path: &Path, quiet: bool) {
    if dataset.is_multi_source() && !quiet {
        println!("==> {} <==", path.display());
    }
}
