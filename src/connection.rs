use std::io::Cursor;

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::context::AppContext;
use crate::resp::parse_command;

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
pub async fn handle_connection<'a>(
    mut socket: TcpStream,
    app_context: &'a AppContext<'a>,
) -> Result<()> {
    let (reader, writer) = socket.split();
    handle_connection_impl(reader, writer, app_context).await
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
/// use codecrafters_redis::config::Config;
/// use codecrafters_redis::context::AppContext;
/// use codecrafters_redis::replication::Role;
///
/// # async fn example() -> anyhow::Result<()> {
/// let store = Store::new();
/// let config = Config::default();
/// let replication_role = Role::default();
/// let app_context = AppContext::new(&store, &config, &replication_role);
/// let reader = Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec());
/// let mut writer = Vec::new();
/// handle_connection_impl(reader, &mut writer, &app_context).await?;
/// assert_eq!(writer, b"+PONG\r\n");
/// # Ok(())
/// # }
/// ```
pub async fn handle_connection_impl<'a, R, W>(
    mut reader: R,
    mut writer: W,
    app_context: &'a AppContext<'a>,
) -> Result<()>
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
            let response = command.execute(app_context)?;
            writer.write_all(&response).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::config::Config;
    use crate::replication::Role;
    use crate::store::Store;
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
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &app_context).await?;

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
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // Call the generic handler
        handle_connection_impl(reader, &mut writer, &app_context).await?;

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
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // Test SET command
        let reader = Cursor::new(b"*3\r\n$3\r\nSET\r\n$4\r\ntaco\r\n$5\r\nsmell\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(store.get_string("taco"), Some("smell".to_string()));

        // Test another SET command with longer value
        let reader = Cursor::new(
            b"*3\r\n$3\r\nSET\r\n$6\r\nphrase\r\n$28\r\nshould have been a rake task\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(
            store.get_string("phrase"),
            Some("should have been a rake task".to_string())
        );

        // Test GET command for existing key
        let reader = Cursor::new(b"*2\r\n$3\r\nGET\r\n$6\r\nphrase\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"$28\r\nshould have been a rake task\r\n".to_vec());

        // Test GET command for existing key
        let reader = Cursor::new(b"*2\r\n$3\r\nGET\r\n$4\r\ntaco\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"$5\r\nsmell\r\n".to_vec());

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_commands_in_buffer() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // Send multiple commands in one buffer
        let commands = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let reader = Cursor::new(commands.to_vec());
        let mut writer = Vec::new();

        handle_connection_impl(reader, &mut writer, &app_context).await?;

        // Should receive both responses
        assert_eq!(writer, b"+PONG\r\n$5\r\nhello\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_writer_types() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // Test with Vec<u8> as writer
        let reader1 = Cursor::new(ping_command());
        let mut buffer1 = Vec::new();
        handle_connection_impl(reader1, &mut buffer1, &app_context).await?;
        assert_eq!(buffer1, b"+PONG\r\n");

        // Test with Cursor as writer
        let reader2 = Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec());
        let mut cursor_writer = Cursor::new(Vec::new());
        handle_connection_impl(reader2, &mut cursor_writer, &app_context).await?;
        assert_eq!(cursor_writer.into_inner(), b"+PONG\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_set_with_ex_option() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // SET key value EX 1 (expire in 1 second)
        let reader = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$6\r\ntestex\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n");
        assert_eq!(store.get_string("testex"), Some("value".to_string()));

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;
        assert_eq!(store.get_string("testex"), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_with_px_option() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // SET key value PX 500 (expire in 500 milliseconds)
        let reader = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$6\r\ntestpx\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n500\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n");
        assert_eq!(store.get_string("testpx"), Some("value".to_string()));

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
        assert_eq!(store.get_string("testpx"), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_get_with_expiration_workflow() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // SET with expiration
        let set_reader = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$7\r\nmykey99\r\n$7\r\nmyval99\r\n$2\r\nPX\r\n$3\r\n200\r\n"
                .to_vec(),
        );
        let mut set_writer = Vec::new();
        handle_connection_impl(set_reader, &mut set_writer, &app_context).await?;
        assert_eq!(set_writer, b"+OK\r\n");

        // GET immediately - should exist
        let get_reader1 = Cursor::new(b"*2\r\n$3\r\nGET\r\n$7\r\nmykey99\r\n".to_vec());
        let mut get_writer1 = Vec::new();
        handle_connection_impl(get_reader1, &mut get_writer1, &app_context).await?;
        assert_eq!(get_writer1, b"$7\r\nmyval99\r\n");

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

        // GET after expiration - should return null
        let reader2 = Cursor::new(b"*2\r\n$3\r\nGET\r\n$7\r\nmykey99\r\n".to_vec());
        let mut writer2 = Vec::new();
        handle_connection_impl(reader2, &mut writer2, &app_context).await?;
        assert_eq!(writer2, b"$-1\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_set_overwrites_expiration() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // SET with short expiration
        let set1_reader = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$8\r\noverride\r\n$2\r\nv1\r\n$2\r\nPX\r\n$3\r\n100\r\n".to_vec(),
        );
        let mut set1_writer = Vec::new();
        handle_connection_impl(set1_reader, &mut set1_writer, &app_context).await?;
        assert_eq!(set1_writer, b"+OK\r\n");

        // Immediately SET without expiration
        let set2_reader =
            Cursor::new(b"*3\r\n$3\r\nSET\r\n$8\r\noverride\r\n$2\r\nv2\r\n".to_vec());
        let mut set2_writer = Vec::new();
        handle_connection_impl(set2_reader, &mut set2_writer, &app_context).await?;
        assert_eq!(set2_writer, b"+OK\r\n");

        // Wait past original expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // GET should still return value (no expiration on second SET)
        let reader3 = Cursor::new(b"*2\r\n$3\r\nGET\r\n$8\r\noverride\r\n".to_vec());
        let mut writer3 = Vec::new();
        handle_connection_impl(reader3, &mut writer3, &app_context).await?;
        assert_eq!(writer3, b"$2\r\nv2\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_keys_with_different_expirations() -> Result<()> {
        let store = Store::new();
        let config = Config::default();
        let replication_role = Role::default();
        let app_context = AppContext::new(&store, &config, &replication_role);

        // SET key1 with long expiration
        let reader1 = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$2\r\nPX\r\n$4\r\n1000\r\n".to_vec(),
        );
        let mut writer1 = Vec::new();
        handle_connection_impl(reader1, &mut writer1, &app_context).await?;

        // SET key2 with short expiration
        let reader2 = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$4\r\nval2\r\n$2\r\nPX\r\n$3\r\n100\r\n".to_vec(),
        );
        let mut writer2 = Vec::new();
        handle_connection_impl(reader2, &mut writer2, &app_context).await?;

        // SET key3 without expiration
        let reader3 = Cursor::new(b"*3\r\n$3\r\nSET\r\n$4\r\nkey3\r\n$4\r\nval3\r\n".to_vec());
        let mut writer3 = Vec::new();
        handle_connection_impl(reader3, &mut writer3, &app_context).await?;

        // Wait for key2 to expire
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // Check all keys
        assert_eq!(store.get_string("key1"), Some("val1".to_string())); // Still valid
        assert_eq!(store.get_string("key2"), None); // Expired
        assert_eq!(store.get_string("key3"), Some("val3".to_string())); // No expiration

        Ok(())
    }
}
