mod cli;
mod commands;
pub mod dataset;
pub mod engine;
mod error;
pub mod model;
mod output;

pub use anyhow::Result;
pub use cli::args::{Cli, Command, OutputFormat};
pub use dataset::{Dataset, DatasetSource};
pub use error::PqError;
pub use model::{ColumnInfo, ColumnStats, FileInfo};

pub fn run(command: Command) -> Result<()> {
    commands::run(command)
}
