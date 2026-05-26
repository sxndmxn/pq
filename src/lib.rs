mod api;
mod cli;
mod commands;
mod dataset;
mod engine;
mod error;
mod model;
mod output;

pub use anyhow::Result;
pub use api::{count, dataset_from_inputs, info, merge, scan, schema, stats};
use clap::Parser;
pub use dataset::Dataset;
pub use error::PqError;
pub use model::{
    ColumnInfo, ColumnStats, ColumnType, CompressionCodec, CountEntry, CountResult, FileInfo,
    LogicalTypeKind, PhysicalType, ScanKind, ScanOptions, ScanResult, SchemaResult, StatValue,
    StatsResult, TimeUnit,
};

pub fn run_cli() -> Result<()> {
    let cli = cli::args::Cli::parse();
    run(cli.command)
}

fn run(command: cli::args::Command) -> Result<()> {
    commands::run(command)
}
