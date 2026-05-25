use crate::cli::args::Command;
use crate::Result;

pub mod convert;
pub mod count;
pub mod head;
pub mod info;
pub mod merge;
pub mod schema;
pub mod stats;

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::Schema(args) => schema::run(args),
        Command::Head(args) => head::run(args),
        Command::Tail(args) => head::run_tail(args),
        Command::Count(args) => count::run(args),
        Command::Stats(args) => stats::run(args),
        Command::Convert(args) => convert::run(args),
        Command::Merge(args) => merge::run(args),
        Command::Info(args) => info::run(args),
    }
}
