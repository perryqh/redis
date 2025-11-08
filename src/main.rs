use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
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
    let (_reader, writer) = socket.split();
    handle_connection_impl(_reader, writer).await
}

/// Generic connection handler that works with any async reader/writer
async fn handle_connection_impl<R, W>(mut _reader: R, mut writer: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // For now, we just send PONG without reading anything
    writer.write_all(b"+PONG\r\n").await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_handle_connection_impl_sends_pong() -> Result<()> {
        // Use in-memory buffers for testing
        let reader = Cursor::new(Vec::new());
        let mut writer = Vec::new();

        // Call the generic handler
        handle_connection_impl(reader, &mut writer).await?;

        // Verify the output
        assert_eq!(writer, b"+PONG\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_connection_with_real_socket() -> Result<()> {
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

    #[tokio::test]
    async fn test_multiple_writers() -> Result<()> {
        // Test with multiple different writer types to ensure generics work
        let reader1 = Cursor::new(Vec::new());
        let mut buffer1 = Vec::new();
        handle_connection_impl(reader1, &mut buffer1).await?;
        assert_eq!(buffer1, b"+PONG\r\n");

        // Test with a cursor as writer
        let reader2 = Cursor::new(Vec::new());
        let mut cursor_writer = Cursor::new(Vec::new());
        handle_connection_impl(reader2, &mut cursor_writer).await?;
        assert_eq!(cursor_writer.into_inner(), b"+PONG\r\n");

        Ok(())
    }
}
