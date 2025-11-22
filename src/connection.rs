use std::io::Cursor;

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::commands::CommandAction;
use crate::context::AppContext;
use crate::datatypes::{Integer, RedisDataType};
use crate::resp::{parse_command, parse_data_type};

/// Handles a single client connection
///
/// This function splits the TCP stream and delegates to the generic handler.
///
/// # Arguments
/// * `socket` - The TCP stream for the client connection
/// * `app_context` - A reference to the application context
///
/// # Errors
/// Returns an error if there's an I/O failure or command parsing error
pub async fn handle_connection(socket: TcpStream, app_context: AppContext) -> Result<()> {
    handle_connection_with_stream(socket, &app_context).await
}

/// Handles a connection with access to the full TcpStream for follower registration
async fn handle_connection_with_stream(
    mut socket: TcpStream,
    app_context: &AppContext,
) -> Result<()> {
    let mut buffer = vec![0; 1024];

    loop {
        let n = socket.read(&mut buffer).await?;

        if n == 0 {
            break;
        }

        buffer.truncate(n);
        let mut cursor = Cursor::new(buffer.as_slice());

        while let Ok(Some(command)) = parse_command(&mut cursor) {
            match command.execute(app_context)? {
                CommandAction::Response(response) => {
                    socket.write_all(&response).await?;
                    socket.flush().await?;
                }
                CommandAction::PsyncHandshake { response, rdb_data } => {
                    // Send FULLRESYNC response
                    socket.write_all(&response).await?;
                    socket.flush().await?;

                    // Send RDB file
                    socket.write_all(&rdb_data).await?;
                    socket.flush().await?;

                    // Register follower and become replication stream
                    if let Some(ref replication_manager) = app_context.replication_manager {
                        let (reader, writer) = socket.into_split();
                        let (follower_id, ack_sender) =
                            replication_manager.register_follower(writer).await;

                        // Keep connection open, reading any follower data
                        keep_follower_connected(reader, ack_sender, follower_id).await?;
                    }
                    return Ok(());
                }
                CommandAction::ReplicaHealthCheck {
                    timeout_milliseconds,
                    num_replicas,
                } => {
                    let acknowledged_count =
                        if let Some(ref replication_manager) = app_context.replication_manager {
                            replication_manager
                                .wait_for_replicas(num_replicas, timeout_milliseconds)
                                .await
                        } else {
                            0
                        };

                    let integer_response = Integer::new(acknowledged_count as i32);
                    socket.write_all(&integer_response.to_bytes()?).await?;
                    socket.flush().await?;
                }
            }
        }

        buffer.resize(1024, 0);
    }
    Ok(())
}

/// Keeps a follower connection alive by reading until disconnect
/// Parses REPLCONF ACK messages and sends offsets through the channel
async fn keep_follower_connected<R>(
    mut reader: R,
    ack_sender: mpsc::UnboundedSender<u64>,
    follower_id: String,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0; 1024];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                eprintln!("Follower {} disconnected", follower_id);
                break;
            }
            Ok(n) => {
                // Parse REPLCONF ACK responses
                let mut cursor = Cursor::new(&buffer[..n]);
                while let Ok(Some(data)) = parse_data_type(&mut cursor) {
                    // Check if it's a REPLCONF ACK response
                    if let Some(array) = data.as_any().downcast_ref::<crate::datatypes::Array>() {
                        if array.values.len() == 3 {
                            let cmd = array.values[0]
                                .as_any()
                                .downcast_ref::<crate::datatypes::BulkString>();
                            let subcmd = array.values[1]
                                .as_any()
                                .downcast_ref::<crate::datatypes::BulkString>();
                            let offset_bulk = array.values[2]
                                .as_any()
                                .downcast_ref::<crate::datatypes::BulkString>();

                            if let (Some(cmd), Some(subcmd), Some(offset_bulk)) =
                                (cmd, subcmd, offset_bulk)
                            {
                                if cmd.value.to_uppercase() == "REPLCONF"
                                    && subcmd.value.to_uppercase() == "ACK"
                                {
                                    if let Ok(offset) = offset_bulk.value.parse::<u64>() {
                                        eprintln!(
                                            "Follower {} sent ACK with offset {}",
                                            follower_id, offset
                                        );
                                        let _ = ack_sender.send(offset);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading from follower {}: {}", follower_id, e);
                break;
            }
        }
    }
    Ok(())
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
/// use codecrafters_redis::replication::ReplicationRole;
///
/// # async fn example() -> anyhow::Result<()> {
/// let store = Store::new();
/// let config = Config::default();
/// let replication_role = ReplicationRole::default();
/// let app_context = AppContext::new(store, config, replication_role);
/// let reader = Cursor::new(b"*1\r\n$4\r\nPING\r\n".to_vec());
/// let mut writer = Vec::new();
/// handle_connection_impl(reader, &mut writer, &app_context).await?;
/// assert_eq!(writer, b"+PONG\r\n");
/// # Ok(())
/// # }
/// ```
pub async fn handle_connection_impl<R, W>(
    mut reader: R,
    mut writer: W,
    app_context: &AppContext,
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
            match command.execute(app_context)? {
                CommandAction::Response(response) => {
                    writer.write_all(&response).await?;
                    writer.flush().await?;
                }
                CommandAction::PsyncHandshake { response, rdb_data } => {
                    // Send FULLRESYNC response
                    writer.write_all(&response).await?;
                    writer.flush().await?;

                    // Send RDB file
                    writer.write_all(&rdb_data).await?;
                    writer.flush().await?;

                    // For generic streams, just keep connection open
                    // (Real follower registration happens in handle_connection_with_stream)
                    eprintln!("PSYNC handshake complete (generic stream)");
                    return Ok(());
                }
                CommandAction::ReplicaHealthCheck {
                    timeout_milliseconds,
                    num_replicas,
                } => {
                    dbg!(timeout_milliseconds, num_replicas);
                    todo!()
                }
            }
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
        let app_context = AppContext::default();

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
        let app_context = AppContext::default();

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
        let app_context = AppContext::default();

        // Test SET command
        let reader = Cursor::new(b"*3\r\n$3\r\nSET\r\n$4\r\ntaco\r\n$5\r\nsmell\r\n".to_vec());
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(
            app_context.store.get_string("taco"),
            Some("smell".to_string())
        );

        // Test another SET command with longer value
        let reader = Cursor::new(
            b"*3\r\n$3\r\nSET\r\n$6\r\nphrase\r\n$28\r\nshould have been a rake task\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n".to_vec());
        assert_eq!(
            app_context.store.get_string("phrase"),
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
        let app_context = AppContext::default();

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
        let app_context = AppContext::default();

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
        let app_context = AppContext::default();

        // SET key value EX 1 (expire in 1 second)
        let reader = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$6\r\ntestex\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("testex"),
            Some("value".to_string())
        );

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;
        assert_eq!(app_context.store.get_string("testex"), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_with_px_option() -> Result<()> {
        let app_context = AppContext::default();

        // SET key value PX 500 (expire in 500 milliseconds)
        let reader = Cursor::new(
            b"*5\r\n$3\r\nSET\r\n$6\r\ntestpx\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n500\r\n".to_vec(),
        );
        let mut writer = Vec::new();
        handle_connection_impl(reader, &mut writer, &app_context).await?;
        assert_eq!(writer, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("testpx"),
            Some("value".to_string())
        );

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
        assert_eq!(app_context.store.get_string("testpx"), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_get_with_expiration_workflow() -> Result<()> {
        let app_context = AppContext::default();

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
        let app_context = AppContext::default();

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
        let app_context = AppContext::default();

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
        assert_eq!(
            app_context.store.get_string("key1"),
            Some("val1".to_string())
        ); // Still valid
        assert_eq!(app_context.store.get_string("key2"), None); // Expired
        assert_eq!(
            app_context.store.get_string("key3"),
            Some("val3".to_string())
        ); // No expiration

        Ok(())
    }
}
