use clap::Parser;
use pq::{Cli, PqError, Result};

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");

        for cause in err.chain().skip(1) {
            if PqError::should_hide_cause(cause) {
                continue;
            }
            eprintln!("  caused by: {cause}");
        }

        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    pq::run(cli.command)
}
