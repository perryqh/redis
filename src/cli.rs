use anyhow::Result;
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

    #[arg(long)]
    pub replicaof: Option<String>,
}

impl Args {
    pub fn replicaof_host_port(&self) -> Result<Option<(String, u16)>> {
        if let Some(replicaof) = &self.replicaof {
            let parts: Vec<&str> = replicaof.split_whitespace().collect();
            if parts.len() == 2 {
                let host = parts[0].to_string();
                let port = parts[1].parse::<u16>()?;
                Ok(Some((host, port)))
            } else {
                Err(anyhow::anyhow!(
                    "Invalid replicaof format. Expecting `<host> <port>`"
                ))
            }
        } else {
            Ok(None)
        }
    }
}

pub fn validate_replicaof(replicaof: &str) -> Result<String, String> {
    if !replicaof.is_empty() {
        Ok(replicaof.to_string())
    } else {
        Err("Replicaof must contain only alphabetic characters and not be empty".to_string())
    }
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
        assert_eq!(args.replicaof, None);
    }

    #[test]
    fn test_replicaof_host_port_not_provided() -> Result<()> {
        let args = Args::parse_from(["redis-rust"]);
        let result = args.replicaof_host_port()?;
        assert_eq!(result, None);

        let args = Args::parse_from(["redis-rust", "--replicaof", "localhost 6379"]);
        let result = args.replicaof_host_port()?;
        assert_eq!(result, Some(("localhost".to_string(), 6379)));

        let args = Args::parse_from(["redis-rust", "--replicaof", "localhost:6379"]);
        assert!(args.replicaof_host_port().is_err());

        let args = Args::parse_from(["redis-rust", "--replicaof", "127.0.0.1  6379"]);
        let result = args.replicaof_host_port()?;

        assert_eq!(result, Some(("127.0.0.1".to_string(), 6379)));

        let args = Args::parse_from(["redis-rust", "--replicaof", "127.0.0.1"]);
        assert!(args.replicaof_host_port().is_err());

        Ok(())
    }
}
