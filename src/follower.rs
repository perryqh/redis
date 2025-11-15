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
        let (reader, writer) = stream.split();
        self.ping_leader(reader, writer).await
    }

    async fn ping_leader<Reader, Writer>(
        &self,
        mut reader: Reader,
        mut writer: Writer,
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
        let result = follower
            .ping_leader(tokio::io::empty(), tokio::io::sink())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ping_leader_success() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let result = follower
            .ping_leader(std::io::Cursor::new(b"+PONG\r\n"), tokio::io::sink())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ping_leader_failure() {
        let app_context = AppContext::default();
        let follower = Follower::new(app_context);
        let result = follower
            .ping_leader(std::io::Cursor::new(b"-ERR\r\n"), tokio::io::sink())
            .await;
        assert!(result.is_err());
    }
}
