use crate::{
    context::AppContext,
    datatypes::{Array, BulkString, RedisDataType},
    replication::ReplicationRole,
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
        self.repl_conf_capa(&mut reader, &mut writer).await
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

        let mut buf = [0; 512];
        let byte_count = reader.read(&mut buf).await?;
        ensure!(byte_count > 0, "No data received from master");
        let bytes = &buf[..byte_count];
        ensure!(bytes == b"+OK\r\n", "Unexpected response from master");
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

        let mut buf = [0; 512];
        let byte_count = reader.read(&mut buf).await?;
        ensure!(byte_count > 0, "No data received from master");
        let bytes = &buf[..byte_count];
        ensure!(bytes == b"+OK\r\n", "Unexpected response from master");
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

        let mut buf = [0; 512];
        let byte_count = reader.read(&mut buf).await?;
        ensure!(byte_count > 0, "No data received from master");
        let bytes = &buf[..byte_count];
        ensure!(bytes == b"+PONG\r\n", "Unexpected response from master");
        Ok(())
    }
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
}
