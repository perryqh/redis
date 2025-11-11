use clap::{arg, command, Parser};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Name of the RDB directory
    #[arg(long, default_value = "~/.redis-rust")]
    pub dir: String,

    /// Name of the RDB file
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,
}
