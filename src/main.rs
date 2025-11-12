use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use codecrafters_redis::cli::Args;
use codecrafters_redis::commands::CommandExecuteInput;
use codecrafters_redis::config::Config;
use codecrafters_redis::connection::handle_connection;
use codecrafters_redis::store::Store;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::new(args)?;
    // Create a shared store wrapped in Arc for thread-safe access across tasks
    let store = Arc::new(Store::from_config(&config));

    // Bind to the Redis default port
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Redis server listening on 127.0.0.1:6379");

    // Accept connections in a loop
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from: {}", peer_addr);

        // Clone the Arc for the spawned task
        let store_clone = Arc::clone(&store);
        let config_clone = config.clone();

        // Spawn a new task to handle this connection
        tokio::spawn(async move {
            let command_input = CommandExecuteInput::new(&store_clone, &config_clone);
            if let Err(e) = handle_connection(socket, &command_input).await {
                eprintln!("Error handling connection from {}: {}", peer_addr, e);
            }
        });
    }
}
