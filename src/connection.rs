use std::io::Cursor;

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::input_command_parser::parse_command;
use crate::store::Store;

/// Handles a single client connection
///
/// This function splits the TCP stream and delegates to the generic handler.
///
/// # Arguments
/// * `socket` - The TCP stream for the client connection
/// * `store` - A reference to the shared key-value store
///
/// # Errors
/// Returns an error if there's an I/O failure or command parsing error
pub async fn handle_connection(mut socket: TcpStream, store: &Store) -> Result<()> {
    let (reader, writer) = socket.split();
    handle_connection_impl(reader, writer, store).await
}

/// Generic connection handler that works with any async reader/writer
///
/// This function continuously reads commands from the reader, parses them,
/// executes them against the store, and writes responses back to the writer.
///
/// # Arguments
/// * `reader` - An async reader implementing AsyncRead
/// * `writer` - An async writer implementing AsyncWrite
/// * `store` - A reference to the shared key-value store
///
/// # Type Parameters
/// * `R` - The reader type (must implement AsyncRead + Unpin)
/// * `W` - The writer type (must implement AsyncWrite + Unpin)
///
/// # Errors
/// Returns an error if there's an I/O failure or command parsing error
///
/// # Example
/// ```no_run
/// use std::io::Cursor;
/// use codecrafters_redis::connection::handle_connection_impl;
/// use codecrafters_redis::store::Store;
///
/// # async fn example() -> anyhow::Result<()> {
/// let store = Store::new();
/// let reader = Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec());
/// let mut writer = Vec::new();
/// handle_connection_impl(reader, &mut writer, &store).await?;
/// assert_eq!(writer, b"+PONG\r\n");
/// # Ok(())
/// # }
/// ```
pub async fn handle_connection_impl<R, W>(mut reader: R, mut writer: W, store: &Store) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
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
        let store = Store::new();

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &store).await?;

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
        let store = Store::new();

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &store).await?;

        // Verify the output
        assert_eq!(writer, input.expected_response);

        Ok(())
    }

    #[tokio::test]
    async fn test_ping_command() -> Result<()> {
        test_handle_connection_commands(ConnectionTestInput {
            send: b"*1\r\n$4\r\nPING\r\n".to_vec(),
            expected_response: b"+PONG\r\n".to_vec(),
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_echo_command() -> Result<()> {
        test_handle_connection_commands(ConnectionTestInput {
            send: b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".to_vec(),
            expected_response: b"$3\r\nhey\r\n".to_vec(),
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set_get_commands() -> Result<()> {
        let store = Store::new();

        // Test SET command
        let reader = Cursor::new(b"*3\r\n$3\r\nSET\r\n$4\r\ntaco\r\n$5\r\nsmell\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &store).await?;
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(store.get("taco"), Some("smell".to_string()));

        // Test another SET command with longer value
        let reader = Cursor::new(
            b"*3\r\n$3\r\nSET\r\n$6\r\nphrase\r\n$28\r\nshould have been a rake task\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &store).await?;
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(
            store.get("phrase"),
            Some("should have been a rake task".to_string())
        );

        // Test GET command for existing key
        let reader = Cursor::new(b"*2\r\n$3\r\nGET\r\n$6\r\nphrase\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &store).await?;
        assert_eq!(writer, b"$28\r\nshould have been a rake task\r\n".to_vec());

        // Test GET command for non-existing key
        let reader = Cursor::new(b"*2\r\n$3\r\nGET\r\n$7\r\nunknown\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &store).await?;
        assert_eq!(writer, b"$-1\r\n".to_vec());

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_commands_in_buffer() -> Result<()> {
        let store = Store::new();

        // Send multiple commands in one buffer
        let commands = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let reader = Cursor::new(commands.to_vec());
        let mut writer = Vec::new();

        handle_connection_impl(reader, &mut writer, &store).await?;

        // Should receive both responses
        assert_eq!(writer, b"+PONG\r\n$5\r\nhello\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_writer_types() -> Result<()> {
        let store = Store::new();

        // Test with Vec<u8> as writer
        let reader1 = Cursor::new(ping_command());
        let mut buffer1 = Vec::new();
        handle_connection_impl(reader1, &mut buffer1, &store).await?;
        assert_eq!(buffer1, b"+PONG\r\n");

        // Test with Cursor as writer
        let reader2 = Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec());
        let mut cursor_writer = Cursor::new(Vec::new());
        handle_connection_impl(reader2, &mut cursor_writer, &store).await?;
        assert_eq!(cursor_writer.into_inner(), b"+PONG\r\n");

        Ok(())
    }
}
