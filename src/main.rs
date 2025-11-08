use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from: {}", peer_addr);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Error handling connection: {}", e);
            }
        });
    }
}

/// Handles a single client connection by sending a PONG response
async fn handle_connection(mut socket: TcpStream) -> Result<()> {
    let (_reader, mut writer) = socket.split();
    writer.write_all(b"+PONG\r\n").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_handle_connection_sends_pong() -> Result<()> {
        // Create a pair of connected sockets for testing
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // Spawn a task to accept the connection
        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle_connection(socket).await
        });

        // Connect as a client
        let mut client = TcpStream::connect(addr).await?;

        // Wait for the server to handle the connection
        server_task.await??;

        // Read the response from the server
        let mut buffer = vec![0; 8];
        let n = client.read(&mut buffer).await?;
        buffer.truncate(n);

        // Verify we received the expected PONG response
        assert_eq!(&buffer, b"+PONG\r\n");

        Ok(())
    }
}
