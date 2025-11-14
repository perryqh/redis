use std::fs;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use codecrafters_redis::cli::Args;
use codecrafters_redis::config::Config;
use codecrafters_redis::connection::handle_connection;
use codecrafters_redis::context::AppContext;
use codecrafters_redis::rdb::parse_rdb_file;
use codecrafters_redis::store::Store;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::new(args)?;
    let store = build_store(&config).await?;

    let listener = TcpListener::bind(&config.server_bind_address()).await?;
    println!(
        "Redis server listening on {}",
        &config.server_bind_address()
    );

    // Accept connections in a loop
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from: {}", peer_addr);

        // Clone the Arc for the spawned task
        let store_clone = Arc::clone(&store);
        let config_clone = config.clone();

        // Spawn a new task to handle this connection
        tokio::spawn(async move {
            let app_context = AppContext::new(&store_clone, &config_clone);
            if let Err(e) = handle_connection(socket, &app_context).await {
                eprintln!("Error handling connection from {}: {}", peer_addr, e);
            }
        });
    }

    async fn build_store(config: &Config) -> Result<Arc<Store>> {
        let contents = fs::read(config.full_rdb_path());
        if let Ok(contents) = contents {
            let rdb = parse_rdb_file(contents);
            if let Ok(rdb) = rdb {
                return Ok(Arc::new(Store::from_rdb(rdb.to_store_values())?));
            }
        }
        Ok(Arc::new(Store::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codecrafters_redis::cli::Args;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_build_store_with_nonexistent_file() {
        let config = Config {
            dir: "/nonexistent/directory".to_string(),
            dbfilename: "nonexistent.rdb".to_string(),
            ..Default::default()
        };

        let result = build_store(&config).await;
        assert!(result.is_ok());

        let store = result.unwrap();
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn test_build_store_with_invalid_rdb_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "invalid rdb content").unwrap();
        let temp_path = temp_file.path();

        let dir = temp_path.parent().unwrap().to_str().unwrap().to_string();
        let filename = temp_path.file_name().unwrap().to_str().unwrap().to_string();

        let config = Config {
            dir,
            dbfilename: filename,
            ..Default::default()
        };

        let result = build_store(&config).await;
        assert!(result.is_ok());

        let store = result.unwrap();
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn test_build_store_creates_empty_store_on_error() {
        let config = Config {
            dir: "/tmp".to_string(),
            dbfilename: "nonexistent_test_file_12345.rdb".to_string(),
            ..Default::default()
        };

        let result = build_store(&config).await;
        assert!(result.is_ok());

        let store = result.unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn test_config_from_args() {
        let args = Args {
            dir: "/custom/dir".to_string(),
            dbfilename: "custom.rdb".to_string(),
            port: 6380,
        };

        let config = Config::new(args).unwrap();
        assert_eq!(config.dir, "/custom/dir");
        assert_eq!(config.dbfilename, "custom.rdb");
        assert_eq!(config.server_port, 6380);
    }

    #[test]
    fn test_config_full_rdb_path() {
        let config = Config {
            dir: "/var/redis".to_string(),
            dbfilename: "dump.rdb".to_string(),
            ..Default::default()
        };

        assert_eq!(config.full_rdb_path(), "/var/redis/dump.rdb");
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.dir, "~/redis-rust");
        assert_eq!(config.dbfilename, "dump.rdb");
    }

    #[test]
    fn test_config_clone() {
        let config1 = Config {
            dir: "/test/dir".to_string(),
            dbfilename: "test.rdb".to_string(),
            ..Default::default()
        };

        let config2 = config1.clone();
        assert_eq!(config1.dir, config2.dir);
        assert_eq!(config1.dbfilename, config2.dbfilename);
    }

    #[tokio::test]
    async fn test_build_store_with_empty_directory_path() {
        let config = Config {
            dir: "".to_string(),
            dbfilename: "dump.rdb".to_string(),
            ..Default::default()
        };

        let result = build_store(&config).await;
        assert!(result.is_ok());

        let store = result.unwrap();
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn test_build_store_returns_arc_store() {
        let config = Config {
            dir: "/nonexistent".to_string(),
            dbfilename: "test.rdb".to_string(),
            ..Default::default()
        };

        let result = build_store(&config).await;
        assert!(result.is_ok());

        let store = result.unwrap();
        // Verify it's an Arc by cloning
        let store_clone = Arc::clone(&store);
        assert_eq!(Arc::strong_count(&store), 2);
        drop(store_clone);
        assert_eq!(Arc::strong_count(&store), 1);
    }

    #[test]
    fn test_config_full_rdb_path_with_special_chars() {
        let config = Config {
            dir: "/path/with spaces/and-dashes".to_string(),
            dbfilename: "my-dump_file.rdb".to_string(),
            ..Default::default()
        };

        assert_eq!(
            config.full_rdb_path(),
            "/path/with spaces/and-dashes/my-dump_file.rdb"
        );
    }

    #[tokio::test]
    async fn test_build_store_with_relative_path() {
        let config = Config {
            dir: "./tests/fixtures".to_string(),
            dbfilename: "nonexistent_file.rdb".to_string(),
            ..Default::default()
        };

        let result = build_store(&config).await;
        assert!(result.is_ok());
    }

    async fn build_store(config: &Config) -> Result<Arc<Store>> {
        let contents = fs::read(config.full_rdb_path());
        if let Ok(contents) = contents {
            let rdb = parse_rdb_file(contents);
            if let Ok(rdb) = rdb {
                return Ok(Arc::new(Store::from_rdb(rdb.to_store_values())?));
            }
        }
        Ok(Arc::new(Store::new()))
    }
}
