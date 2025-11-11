use clap::{arg, command, Parser};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Name of the RDB directory
    #[arg(long)]
    dir: String,

    /// Name of the RDB file
    #[arg(long)]
    dbfilename: String,
}
