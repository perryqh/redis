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

    /// Server port
    #[arg(long, default_value = "6379")]
    pub port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let args = Args::parse_from(["redis-rust"]);
        assert_eq!(args.dir, "~/.redis-rust");
        assert_eq!(args.dbfilename, "dump.rdb");
        assert_eq!(args.port, 6379);
    }
}
