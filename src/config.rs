use crate::cli::Args;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: String,
    pub dbfilename: String,
    pub server_address: String,
    pub server_port: u16,
}

impl Config {
    pub fn new(args: Args) -> Result<Self> {
        Ok(Config {
            dir: args.dir,
            dbfilename: args.dbfilename,
            server_port: args.port,
            ..Default::default()
        })
    }

    pub fn full_rdb_path(&self) -> String {
        format!("{}/{}", self.dir, self.dbfilename)
    }

    pub fn server_bind_address(&self) -> String {
        format!("{}:{}", self.server_address, self.server_port)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dir: String::from("~/redis-rust"),
            dbfilename: String::from("dump.rdb"),
            server_address: String::from("127.0.0.1"),
            server_port: 6379,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_new() {
        let args = Args {
            dir: String::from("/tmp"),
            dbfilename: String::from("test.rdb"),
            port: 6379,
            replicaof: None,
        };

        let config = Config::new(args).unwrap();

        assert_eq!(config.dir, "/tmp");
        assert_eq!(config.dbfilename, "test.rdb");
        assert_eq!(config.server_address, "127.0.0.1");
        assert_eq!(config.server_port, 6379);
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();

        assert_eq!(config.dir, "~/redis-rust");
        assert_eq!(config.dbfilename, "dump.rdb");
        assert_eq!(config.server_address, "127.0.0.1");
        assert_eq!(config.server_port, 6379);
    }

    #[test]
    fn test_server_bind_address() {
        let config = Config::default();

        assert_eq!(config.server_bind_address(), "127.0.0.1:6379");
    }

    #[test]
    fn test_server_bind_address_with_custom_port() {
        let config = Config {
            server_address: String::from("192.168.1.1"),
            server_port: 8080,
            ..Default::default()
        };

        assert_eq!(config.server_bind_address(), "192.168.1.1:8080");
    }
}
