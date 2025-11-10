use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;

use codecrafters_redis::input_command_parser::parse_command;
use codecrafters_redis::store::Store;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    let store = Arc::new(Store::new());
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from: {}", peer_addr);

        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, &store_clone).await {
                eprintln!("Error handling connection: {}", e);
            }
        });
    }
}

/// Handles a single client connection by sending a PONG response
async fn handle_connection(mut socket: TcpStream, store: &Store) -> Result<()> {
    let (_reader, writer) = socket.split();
    handle_connection_impl(_reader, writer, store).await
}

/// Generic connection handler that works with any async reader/writer
async fn handle_connection_impl<R, W>(mut reader: R, mut writer: W, store: &Store) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

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
            let response = command.execute(store)?;
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

    fn ping_command() -> Vec<u8> {
        b"*1\r\n$4\r\nPING\r\n".to_vec()
    }

    #[tokio::test]
    async fn test_handle_connection_impl_sends_pong() -> Result<()> {
        // Use in-memory buffers for testing
        let mut writer = Vec::new();

        let reader = Cursor::new(ping_command());

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &Store::new()).await?;

        // Verify the output
        assert_eq!(writer, b"+PONG\r\n");

        Ok(())
    }

    struct ConnectionTestInput {
        send: Vec<u8>,
        expected_response: Vec<u8>,
    }

    async fn test_handle_connection_commands(input: ConnectionTestInput) -> Result<()> {
        // Use in-memory buffers for testing instead of real TCP connections
        let reader = Cursor::new(input.send);
        let mut writer = Vec::new();

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &Store::new()).await?;

        // Verify the output
        assert_eq!(writer, input.expected_response);

        Ok(())
    }

    #[tokio::test]
    async fn test_real_ping() -> Result<()> {
        test_handle_connection_commands(ConnectionTestInput {
            send: b"*1\r\n$4\r\nPING\r\n".to_vec(),
            expected_response: b"+PONG\r\n".to_vec(),
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set_get() -> Result<()> {
        let store = Store::new();
        // Use in-memory buffers for testing instead of real TCP connections
        let reader = Cursor::new(b"*3\r\n$3\r\nSET\r\n$4\r\ntaco\r\n$5\r\nsmell\r\n".to_vec());
        let mut writer = Vec::new();

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &store).await?;

        // Verify the output
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(store.get("taco"), Some("smell".to_string()));

        let reader = Cursor::new(
            b"*3\r\n$3\r\nSET\r\n$6\r\nphrase\r\n$28\r\nshould have been a rake task\r\n".to_vec(),
        );
        let mut writer = Vec::new();

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &store).await?;

        // Verify the output
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(
            store.get("phrase"),
            Some("should have been a rake task".to_string())
        );

        let reader = Cursor::new(b"*2\r\n$3\r\nGET\r\n$6\r\nphrase\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &store).await?;

        // Verify the output
        assert_eq!(writer, b"$28\r\nshould have been a rake task\r\n".to_vec());

        let reader = Cursor::new(b"*2\r\n$3\r\nGET\r\n$6\r\nunknown\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &store).await?;

        // Verify the output
        assert_eq!(writer, b"$-1\r\n".to_vec());

        Ok(())
    }

    #[tokio::test]
    async fn test_real_echo() -> Result<()> {
        test_handle_connection_commands(ConnectionTestInput {
            send: b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".to_vec(),
            expected_response: b"$3\r\nhey\r\n".to_vec(),
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_writers() -> Result<()> {
        let store = Store::new();
        // Test with multiple different writer types to ensure generics work
        let reader1 = Cursor::new(ping_command());
        let mut buffer1 = Vec::new();
        handle_connection_impl(reader1, &mut buffer1, &store).await?;
        assert_eq!(buffer1, b"+PONG\r\n");

        // Test with a cursor as writer
        let reader2 = Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec());
        let mut cursor_writer = Cursor::new(Vec::new());
        handle_connection_impl(reader2, &mut cursor_writer, &store).await?;
        assert_eq!(cursor_writer.into_inner(), b"+PONG\r\n");

        Ok(())
    }
}
