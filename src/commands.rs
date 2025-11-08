use crate::datatypes::{RedisDataType, SimpleString};
use anyhow::Result;

pub trait RedisCommand: Send {
    fn response(&self) -> Result<Vec<u8>>;
}

pub struct PingCommand {}
impl RedisCommand for PingCommand {
    fn response(&self) -> Result<Vec<u8>> {
        SimpleString::new("PONG".to_string()).to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_command() {
        let command = PingCommand {};
        let response = command.response().unwrap();
        assert_eq!(response, b"+PONG\r\n");
    }
}
