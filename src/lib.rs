mod api;
mod cli;
mod commands;
mod dataset;
mod engine;
mod error;
pub mod model;
mod output;

pub use anyhow::Result;
pub use api::{
    count, dataset_from_inputs, info, merge, scan, schema, stats, CountEntry, CountResult,
    ScanKind, ScanOptions, ScanResult, SchemaResult, StatsResult,
};
use clap::Parser;
pub use dataset::Dataset;
pub use error::PqError;
pub use model::{
    ColumnInfo, ColumnStats, ColumnType, CompressionCodec, FileInfo, LogicalTypeKind, PhysicalType,
    StatValue, TimeUnit,
};
pub use output::OutputFormat;

pub fn run_cli() -> Result<()> {
    let cli = cli::args::Cli::parse();
    run(cli.command)
}

fn run(command: cli::args::Command) -> Result<()> {
    commands::run(command)
}
