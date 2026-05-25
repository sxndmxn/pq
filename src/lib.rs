pub mod api;
mod cli;
mod commands;
pub mod dataset;
pub mod engine;
mod error;
pub mod model;
mod output;

pub use anyhow::Result;
pub use api::{
    count, dataset_from_inputs, info, merge, scan, schema, stats, CountEntry, CountResult,
    ScanKind, ScanOptions, ScanResult, SchemaResult, StatsResult,
};
pub use cli::args::{Cli, Command, OutputFormat};
pub use dataset::{Dataset, DatasetSource};
pub use error::PqError;
pub use model::{
    ColumnInfo, ColumnStats, ColumnType, CompressionCodec, FileInfo, LogicalTypeKind, PhysicalType,
    StatValue, TimeUnit,
};

pub fn run(command: Command) -> Result<()> {
    commands::run(command)
}
