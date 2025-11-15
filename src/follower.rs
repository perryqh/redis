use std::io::Cursor;

use crate::{
    context::AppContext,
    datatypes::{Array, BulkString, RedisDataType, SimpleString},
    replication::ReplicationRole,
    resp::parse_data_type,
};
use anyhow::{bail, ensure, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
        self.psync(&mut reader, &mut writer).await
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

        let response_string = response_as_simple_string(reader).await?;
        let (replication_id, offset) =
            psync_response_to_replication_id_and_offset(&response_string)?;
        dbg!(replication_id, offset);
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
        let response = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
        let mut reader = std::io::Cursor::new(response.as_slice());
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
}
