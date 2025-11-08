use std::io::Cursor;

use anyhow::Result;

use codecrafters_redis::input_command_parser::parse_command;
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
async fn handle_connection_impl<R, W>(mut reader: R, mut writer: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use tokio::io::AsyncReadExt;

    // Continuously read and process commands
    loop {
        let mut buffer = vec![0; 1024];
        let n = reader.read(&mut buffer).await?;

        if n == 0 {
            // Connection closed
            break;
        }

        buffer.truncate(n);
        let mut cursor = Cursor::new(buffer.as_slice());

        while let Ok(Some(command)) = parse_command(&mut cursor) {
            let response = command.response()?;
            writer.write_all(&response).await?;
            writer.flush().await?;
        }
    }
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
        let mut writer = Vec::new();

        // Write PING command to the reader
        let reader = Cursor::new(b"+PING\r\n".to_vec());

        // Call the generic handler
        handle_connection_impl(reader, &mut writer).await?;

        // Verify the output
        assert_eq!(writer, b"+PONG\r\n");

        Ok(())
    }
    #[tokio::test]
    async fn test_multiple_pings_in_memory() -> Result<()> {
        // Test with in-memory buffers sending multiple PING commands at once
        let reader = Cursor::new(b"+PING\r\n+PING\r\n+PING\r\n".to_vec());
        let mut writer = Vec::new();

        // Call the handler
        handle_connection_impl(reader, &mut writer).await?;

        // Verify we received three PONG responses
        assert_eq!(writer, b"+PONG\r\n+PONG\r\n+PONG\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_pings_sent_separately() -> Result<()> {
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Collect all responses
        let mut all_responses = Vec::new();

        // Send first PING and read response
        client.write_all(b"+PING\r\n").await?;
        client.flush().await?;

        // Give server time to process and respond
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Read first PONG
        let mut buffer = vec![0; 8];
        let n = client.read(&mut buffer).await?;
        all_responses.extend_from_slice(&buffer[..n]);

        // Send second PING and read response
        client.write_all(b"+PING\r\n").await?;
        client.flush().await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Read second PONG
        let mut buffer = vec![0; 8];
        let n = client.read(&mut buffer).await?;
        all_responses.extend_from_slice(&buffer[..n]);

        // Send third PING and read response
        client.write_all(b"+PING\r\n").await?;
        client.flush().await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Read third PONG
        let mut buffer = vec![0; 8];
        let n = client.read(&mut buffer).await?;
        all_responses.extend_from_slice(&buffer[..n]);

        // Verify we received three PONG responses
        assert_eq!(
            &all_responses,
            b"+PONG\r\n+PONG\r\n+PONG\r\n",
            "Expected 3 PONGs but got: {:?}",
            String::from_utf8_lossy(&all_responses)
        );

        // Shutdown the connection
        client.shutdown().await?;

        // Clean up - server task should complete after client shutdown
        server_task.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_pings_sent_together() -> Result<()> {
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Send multiple PING commands
        client.write_all(b"+PING\r\n+PING\r\n+PING\r\n").await?;
        client.flush().await?;

        // Small delay to ensure all commands are processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Read multiple PONG responses
        let mut buffer = vec![0; 24]; // 8 bytes per PONG * 3
        let n = client.read(&mut buffer).await?;
        buffer.truncate(n);

        // Verify we received three PONG responses
        assert_eq!(
            &buffer,
            b"+PONG\r\n+PONG\r\n+PONG\r\n",
            "Expected 3 PONGs but got: {:?}",
            String::from_utf8_lossy(&buffer)
        );

        // Clean up
        drop(client);
        server_task.await??;

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
        use tokio::io::AsyncWriteExt;

        // Send a PING command to the server
        client.write_all(b"+PING\r\n").await?;
        client.flush().await?;

        // Read the response from the server
        let mut buffer = vec![0; 8];
        let n = client.read(&mut buffer).await?;
        buffer.truncate(n);

        // Verify we received the expected PONG response
        assert_eq!(&buffer, b"+PONG\r\n");

        // Shutdown the connection to signal no more data
        client.shutdown().await?;

        // Wait for the server to handle the connection
        server_task.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_writers() -> Result<()> {
        // Test with multiple different writer types to ensure generics work
        let reader1 = Cursor::new(b"+PING\r\n".to_vec());
        let mut buffer1 = Vec::new();
        handle_connection_impl(reader1, &mut buffer1).await?;
        assert_eq!(buffer1, b"+PONG\r\n");

        // Test with a cursor as writer
        let reader2 = Cursor::new(b"+PING\r\n".to_vec());
        let mut cursor_writer = Cursor::new(Vec::new());
        handle_connection_impl(reader2, &mut cursor_writer).await?;
        assert_eq!(cursor_writer.into_inner(), b"+PONG\r\n");

        Ok(())
    }
}
