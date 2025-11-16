use std::{io::Cursor, time::Duration};

use crate::{
    commands::CommandAction,
    context::AppContext,
    datatypes::{Array, BulkString, RedisDataType, SimpleString},
    replication::ReplicationRole,
    resp::{parse_command, parse_data_type},
};
use anyhow::{bail, ensure, Result};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

pub struct Follower {
    app_context: AppContext,
}

impl Follower {
    pub fn new(app_context: AppContext) -> Self {
        Self { app_context }
    }

    pub async fn start(&self) -> Result<()> {
        let ReplicationRole::Follower(follower_replication) =
            &self.app_context.replication_role.as_ref()
        else {
            bail!("Not a follower role");
        };
        // connect to leader_host:leader_port
        let leader_addr = format!(
            "{}:{}",
            follower_replication.leader_host, follower_replication.leader_port
        );
        let mut stream = TcpStream::connect(&leader_addr).await?;
        let (mut reader, mut writer) = stream.split();
        self.ping_leader(&mut reader, &mut writer).await?;
        self.repl_conf_listening(&mut reader, &mut writer).await?;
        self.repl_conf_capa(&mut reader, &mut writer).await?;
        self.psync(&mut reader, &mut writer).await?;
        self.listen(&mut reader, &mut writer).await
    }

    async fn listen<R, W>(&self, reader: &mut R, writer: &mut W) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        dbg!("in listen......loop");
        // sleep for a bit to let handshaking complete
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut buf = [0; 1024];
        let mut offset: usize = 0;
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                // No more data, exit cleanly
                break;
            }

            // Parse and execute commands from the buffer
            let mut cursor = Cursor::new(&buf[..n]);
            loop {
                let position_before = cursor.position() as usize;
                let command = parse_command(&mut cursor)?;

                match command {
                    Some(command) => {
                        let position_after = cursor.position() as usize;
                        let bytes_consumed = position_after - position_before;

                        dbg!(command.command_name());
                        if let Some(CommandAction::Response(response)) = command
                            .execute_leader_command_from_replica(&self.app_context, offset)?
                        {
                            writer.write_all(&response).await?;
                            writer.flush().await?;
                        }
                        offset += bytes_consumed;
                    }
                    None => {
                        // No more commands in the buffer, exit the loop
                        break;
                    }
                }
            }
        }
        dbg!("Exiting follower listen loop");
        Ok(())
    }

    async fn psync<Reader, Writer>(
        &self,
        reader: &mut Reader,
        writer: &mut Writer,
    ) -> anyhow::Result<()>
    where
        Reader: AsyncReadExt + Unpin,
        Writer: AsyncWriteExt + Unpin,
    {
        let conf_array = Array::new(vec![
            Box::new(BulkString::new("PSYNC".to_string())),
            Box::new(BulkString::new("?".to_string())),
            Box::new(BulkString::new("-1".to_string())),
        ]);
        writer.write_all(&conf_array.to_bytes()?).await?;

        let response_string = read_simple_string_line(reader).await?;
        let (replication_id, offset) =
            psync_response_to_replication_id_and_offset(&response_string)?;
        dbg!("psync", replication_id, offset);

        read_rdb_file(reader).await?;
        Ok(())
    }

    async fn repl_conf_capa<Reader, Writer>(
        &self,
        reader: &mut Reader,
        writer: &mut Writer,
    ) -> anyhow::Result<()>
    where
        Reader: AsyncReadExt + Unpin,
        Writer: AsyncWriteExt + Unpin,
    {
        let conf_array = Array::new(vec![
            Box::new(BulkString::new("REPLCONF".to_string())),
            Box::new(BulkString::new("capa".to_string())),
            Box::new(BulkString::new("psync2".to_string())),
        ]);
        writer.write_all(&conf_array.to_bytes()?).await?;
        dbg!("capa psync2");
        let response_string = response_as_simple_string(reader).await?;
        ensure!(
            response_string == "OK",
            "conf-capa Unexpected response from master. Expected 'OK', got '{}'",
            response_string
        );
        Ok(())
    }

    async fn repl_conf_listening<Reader, Writer>(
        &self,
        reader: &mut Reader,
        writer: &mut Writer,
    ) -> anyhow::Result<()>
    where
        Reader: AsyncReadExt + Unpin,
        Writer: AsyncWriteExt + Unpin,
    {
        let conf_array = Array::new(vec![
            Box::new(BulkString::new("REPLCONF".to_string())),
            Box::new(BulkString::new("listening-port".to_string())),
            Box::new(BulkString::new(
                self.app_context.config.server_port.to_string(),
            )),
        ]);
        writer.write_all(&conf_array.to_bytes()?).await?;
        dbg!("repl config listening");
        let response_string = response_as_simple_string(reader).await?;
        ensure!(
            response_string == "OK",
            format!(
                "repl-conf-listening - Unexpected response from master. Expected 'OK', got '{}'",
                response_string
            )
        );
        Ok(())
    }

    async fn ping_leader<Reader, Writer>(
        &self,
        reader: &mut Reader,
        writer: &mut Writer,
    ) -> anyhow::Result<()>
    where
        Reader: AsyncReadExt + Unpin,
        Writer: AsyncWriteExt + Unpin,
    {
        let ping_bulk = BulkString::new("PING".to_string());
        let ping_array = Array::new(vec![Box::new(ping_bulk)]);
        writer.write_all(&ping_array.to_bytes()?).await?;

        let response_string = response_as_simple_string(reader).await?;
        ensure!(
            response_string == "PONG",
            format!(
                "ping - Unexpected response from master expected PONG, got: {}",
                response_string
            )
        );
        Ok(())
    }
}

async fn read_rdb_file<Reader>(reader: &mut Reader) -> Result<()>
where
    Reader: AsyncReadExt + Unpin,
{
    // $<length_of_file>\r\n<binary_contents_of_file>

    // Read the '$' prefix
    let mut prefix = [0u8; 1];
    reader.read_exact(&mut prefix).await?;
    ensure!(prefix[0] == b'$', "Expected '$' prefix for RDB file");

    // Read the length until \r\n
    let mut length_bytes = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\r' {
            let mut next_byte = [0u8; 1];
            reader.read_exact(&mut next_byte).await?;
            ensure!(next_byte[0] == b'\n', "Expected '\\n' after '\\r'");
            break;
        }
        length_bytes.push(byte[0]);
    }

    // Parse the length
    let length_str = String::from_utf8(length_bytes)?;
    let length: usize = length_str.parse()?;

    // Read and discard the RDB file contents
    let mut rdb_data = vec![0u8; length];
    reader.read_exact(&mut rdb_data).await?;

    Ok(())
}

async fn read_simple_string_line<Reader>(reader: &mut Reader) -> Result<String>
where
    Reader: AsyncReadExt + Unpin,
{
    // Read '+' prefix
    let mut prefix = [0u8; 1];
    reader.read_exact(&mut prefix).await?;
    ensure!(prefix[0] == b'+', "Expected '+' prefix for simple string");

    // Read until \r\n
    let mut line_bytes = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\r' {
            let mut next_byte = [0u8; 1];
            reader.read_exact(&mut next_byte).await?;
            ensure!(next_byte[0] == b'\n', "Expected '\\n' after '\\r'");
            break;
        }
        line_bytes.push(byte[0]);
    }

    Ok(String::from_utf8(line_bytes)?)
}

async fn response_as_simple_string<Reader>(reader: &mut Reader) -> Result<String>
where
    Reader: AsyncReadExt + Unpin,
{
    let mut buf = [0; 512];
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    let mut bytes = Cursor::new(&buf[..byte_count]);
    let response = parse_data_type(&mut bytes)?;
    simple_string_from_response(response)
}

fn simple_string_from_response(response: Option<Box<dyn RedisDataType>>) -> Result<String> {
    let data_type = response.ok_or_else(|| anyhow::anyhow!("No response received"))?;

    let simple_string = data_type
        .as_any()
        .downcast_ref::<SimpleString>()
        .ok_or_else(|| anyhow::anyhow!("Unexpected response type"))?;

    Ok(simple_string.value.clone())
}

fn psync_response_to_replication_id_and_offset(response: &str) -> Result<(String, u64)> {
    let mut parts = response.split(' ');
    let action = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("psync - expected FULLRESYNC"))?;
    ensure!(
        action == "FULLRESYNC",
        "psync - expected FULLRESYNC, got {}",
        action
    );
    let replication_id = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("psync - expected replication ID"))?;
    ensure!(
        replication_id.len() == 40,
        format!(
            "psync - expected replication ID of length 40, got: {}",
            replication_id
        )
    );
    let offset = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("psync - expected offset"))?;
    let offset = offset
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("Invalid offset"))?;

    Ok((replication_id.to_string(), offset))
}

#[cfg(test)]
mod tests {
    use base64::{engine::general_purpose::STANDARD, Engine};

    use crate::rdb::EMPTY_RDB;

    use super::*;

    #[tokio::test]
    async fn test_ping_leader() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = tokio::io::empty();
        let mut writer = tokio::io::sink();
        let result = follower.ping_leader(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ping_leader_success() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"+PONG\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.ping_leader(&mut reader, &mut writer).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ping_leader_failure() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"-ERR\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.ping_leader(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_psync_success() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);

        // Prepare FULLRESYNC response with RDB file data
        let mut response = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".to_vec();

        // Add RDB file in bulk string format
        let rdb_data = STANDARD.decode(EMPTY_RDB).unwrap();
        let rdb_bulk_string = format!("${}\r\n", rdb_data.len()).into_bytes();
        response.extend_from_slice(&rdb_bulk_string);
        response.extend_from_slice(&rdb_data);

        let mut reader = std::io::Cursor::new(response);
        let mut writer = tokio::io::sink();
        let result = follower.psync(&mut reader, &mut writer).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_psync_empty_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = tokio::io::empty();
        let mut writer = tokio::io::sink();
        let result = follower.psync(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_psync_invalid_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"+INVALID\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.psync(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_to_replication_id_and_offset_success() {
        let response = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_ok());
        let (replication_id, offset) = result.unwrap();
        assert_eq!(replication_id, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_psync_response_to_replication_id_and_offset_with_nonzero_offset() {
        let response = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 12345";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_ok());
        let (replication_id, offset) = result.unwrap();
        assert_eq!(replication_id, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        assert_eq!(offset, 12345);
    }

    #[test]
    fn test_psync_response_missing_action() {
        let response = "";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_wrong_action() {
        let response = "WRONGACTION 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_missing_replication_id() {
        let response = "FULLRESYNC";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_invalid_replication_id_length() {
        let response = "FULLRESYNC tooshort 0";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_missing_offset() {
        let response = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_invalid_offset() {
        let response = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb notanumber";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_psync_response_negative_offset() {
        let response = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb -1";
        let result = psync_response_to_replication_id_and_offset(response);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repl_conf_listening_success() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"+OK\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_listening(&mut reader, &mut writer).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_repl_conf_listening_empty_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = tokio::io::empty();
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_listening(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repl_conf_listening_invalid_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"+ERROR\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_listening(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repl_conf_listening_error_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"-ERR unknown command\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_listening(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repl_conf_capa_success() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"+OK\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_capa(&mut reader, &mut writer).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_repl_conf_capa_empty_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = tokio::io::empty();
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_capa(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repl_conf_capa_invalid_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"+NOTOK\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_capa(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repl_conf_capa_error_response() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let mut reader = std::io::Cursor::new(b"-ERR invalid capability\r\n");
        let mut writer = tokio::io::sink();
        let result = follower.repl_conf_capa(&mut reader, &mut writer).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_listen_set() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());
        let array = Array::from_strs(vec!["SET", "key", "value"]);
        let mut reader = std::io::Cursor::new(array.to_bytes()?);
        let mut writer = tokio::io::sink();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());
        let value = app_context.store.get_string("key");
        assert_eq!(value, Some("value".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn test_listen_set_multiple_commands() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());
        let array = Array::from_strs(vec!["SET", "baz", "bop"]);
        let another_array = Array::from_strs(vec!["SET", "foo", "bar"]);
        let mut bytes = array.to_bytes()?;
        bytes.extend_from_slice(&another_array.to_bytes()?);
        let mut reader = std::io::Cursor::new(bytes);
        let mut writer = tokio::io::sink();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());
        let value = app_context.store.get_string("baz");
        assert_eq!(value, Some("bop".to_string()));
        let value = app_context.store.get_string("foo");
        assert_eq!(value, Some("bar".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn test_listen_replconf_getack() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());
        let array = Array::from_strs(vec!["REPLCONF", "GETACK", "*"]);
        let mut reader = std::io::Cursor::new(array.to_bytes()?);
        let mut writer = Vec::new();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());

        // Should respond with REPLCONF ACK 0 (initial offset, no commands processed before GETACK)
        let expected = Array::from_strs(vec!["REPLCONF", "ACK", "0"]).to_bytes()?;
        assert_eq!(writer, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_listen_replconf_getack_after_set() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());

        // Send SET and GETACK in the same buffer
        let set_array = Array::from_strs(vec!["SET", "key", "value"]);
        let getack_array = Array::from_strs(vec!["REPLCONF", "GETACK", "*"]);
        let mut bytes = set_array.to_bytes()?;
        let set_size = bytes.len();
        bytes.extend_from_slice(&getack_array.to_bytes()?);

        let mut reader = std::io::Cursor::new(bytes);
        let mut writer = Vec::new();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());

        // Offset should only include the SET command bytes, not the GETACK
        let expected =
            Array::from_strs(vec!["REPLCONF", "ACK", &set_size.to_string()]).to_bytes()?;
        assert_eq!(writer, expected);

        // Verify SET was executed
        let value = app_context.store.get_string("key");
        assert_eq!(value, Some("value".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn test_listen_replconf_getack_multiple_commands() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());

        // Send multiple SET commands, then REPLCONF GETACK
        let set1 = Array::from_strs(vec!["SET", "key1", "value1"]);
        let set2 = Array::from_strs(vec!["SET", "key2", "value2"]);
        let getack = Array::from_strs(vec!["REPLCONF", "GETACK", "*"]);

        let mut bytes = set1.to_bytes()?;
        bytes.extend_from_slice(&set2.to_bytes()?);
        let total_size = bytes.len();
        bytes.extend_from_slice(&getack.to_bytes()?);

        let mut reader = std::io::Cursor::new(bytes);
        let mut writer = Vec::new();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());

        // Offset should only include the SET commands bytes, not the GETACK
        let expected =
            Array::from_strs(vec!["REPLCONF", "ACK", &total_size.to_string()]).to_bytes()?;
        assert_eq!(writer, expected);

        // Verify both SETs were executed
        assert_eq!(
            app_context.store.get_string("key1"),
            Some("value1".to_string())
        );
        assert_eq!(
            app_context.store.get_string("key2"),
            Some("value2".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_listen_replconf_non_getack() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());

        // REPLCONF with non-GETACK action should not respond
        let array = Array::from_strs(vec!["REPLCONF", "listening-port", "6380"]);
        let mut reader = std::io::Cursor::new(array.to_bytes()?);
        let mut writer = Vec::new();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());

        // Should not write anything to writer (only GETACK responds)
        assert!(writer.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_listen_replconf_getack_response_format() -> Result<()> {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context.clone());

        // Test that GETACK returns proper RESP array format
        let array = Array::from_strs(vec!["REPLCONF", "GETACK", "*"]);
        let mut reader = std::io::Cursor::new(array.to_bytes()?);
        let mut writer = Vec::new();
        let result = follower.listen(&mut reader, &mut writer).await;
        assert!(result.is_ok());

        // Parse response to verify it's a valid RESP array
        let mut cursor = std::io::Cursor::new(&writer[..]);
        let response = crate::resp::parse_data_type(&mut cursor)?;
        assert!(response.is_some());

        // Verify it's an array with 3 elements: REPLCONF, ACK, offset
        let data = response.unwrap();
        let array = data
            .as_any()
            .downcast_ref::<Array>()
            .expect("Response should be an Array");
        assert_eq!(array.values.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_rdb_file() -> Result<()> {
        let data = STANDARD.decode(EMPTY_RDB).unwrap();
        let mut rdb_data = format!("${}\r\n", data.len()).into_bytes();
        rdb_data.extend_from_slice(&data);
        let mut reader = std::io::Cursor::new(rdb_data);
        let result = read_rdb_file(&mut reader).await;
        assert!(result.is_ok());
        Ok(())
    }
}
