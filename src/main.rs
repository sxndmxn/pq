//! pq - A jq-like CLI for Parquet files

mod commands;
mod output;
mod utils;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "pq")]
#[command(
    about = "A jq-like CLI for Parquet files. Fast startup, pretty output, sensible defaults."
)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show schema (column names, types, nullability)
    Schema {
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Output format
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
    /// Show first N rows
    Head {
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Number of rows to show
        #[arg(short = 'n', long = "rows", default_value = "10")]
        rows: usize,
        /// Output format
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
    /// Show last N rows
    Tail {
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Number of rows to show
        #[arg(short = 'n', long = "rows", default_value = "10")]
        rows: usize,
        /// Output format
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
    /// Count total rows
    Count {
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
    /// Column statistics (min, max, nulls, distinct)
    Stats {
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Specific column to show stats for
        #[arg(short, long)]
        column: Option<String>,
        /// Output format
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
    /// Run SQL query against file
    Query {
        /// SQL query to execute
        #[arg(required = true)]
        sql: String,
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Output format
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
    /// Convert to CSV, JSON, or JSONL
    Convert {
        /// Input parquet file
        #[arg(required = true)]
        input: PathBuf,
        /// Output file path
        #[arg(required = true)]
        output_path: PathBuf,
    },
    /// Merge multiple parquet files
    Merge {
        /// Input parquet files
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Output file path
        #[arg(short, long, required = true)]
        output: PathBuf,
    },
    /// File metadata (row groups, compression, size)
    Info {
        /// Parquet file(s) to read
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Output format
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,
        /// Suppress headers and formatting
        #[arg(short, long)]
        quiet: bool,
    },
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Jsonl,
    Csv,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Schema {
            files,
            output,
            quiet,
        } => {
            let paths = utils::expand_globs(&files)?;
            commands::schema::run(&paths, output, quiet)?;
        }
        Commands::Head {
            files,
            rows,
            output,
            quiet,
        } => {
            let paths = utils::expand_globs(&files)?;
            commands::head::run(&paths, rows, output, quiet)?;
        }
        Commands::Tail {
            files,
            rows,
            output,
            quiet,
        } => {
            let paths = utils::expand_globs(&files)?;
            commands::head::run_tail(&paths, rows, output, quiet)?;
        }
        Commands::Count { files, quiet } => {
            let paths = utils::expand_globs(&files)?;
            commands::count::run(&paths, quiet)?;
        }
        Commands::Stats {
            files,
            column,
            output,
            quiet,
        } => {
            let paths = utils::expand_globs(&files)?;
            commands::stats::run(&paths, column.as_deref(), output, quiet)?;
        }
        Commands::Query {
            sql,
            files,
            output,
            quiet,
        } => {
            let paths = utils::expand_globs(&files)?;
            commands::query::run(&paths, &sql, output, quiet).await?;
        }
        Commands::Convert { input, output_path } => {
            commands::convert::run(&input, &output_path)?;
        }
        Commands::Merge { files, output } => {
            let paths = utils::expand_globs(&files)?;
            commands::merge::run(&paths, &output)?;
        }
        Commands::Info {
            files,
            output,
            quiet,
        } => {
            let paths = utils::expand_globs(&files)?;
            commands::info::run(&paths, output, quiet)?;
        }
    }

    Ok(())
}
