use clap::Parser;
use pq::cli::args::Cli;
use pq::{commands, Result};

fn main() {
    if let Err(err) = run() {
        // Print user-friendly error message
        eprintln!("error: {err}");

        // Print cause chain without internal details
        let mut source = err.source();
        while let Some(cause) = source {
            // Skip internal library error types that aren't helpful
            let msg = cause.to_string();
            if !msg.contains("ArrowError") && !msg.contains("ParquetError") {
                eprintln!("  caused by: {msg}");
            }
            source = cause.source();
        }

        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    commands::run(cli.command)
}
