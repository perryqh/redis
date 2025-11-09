use crate::datatypes::{BulkString, RedisDataType, SimpleString};
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

pub struct EchoCommand {
    message: String,
}

impl EchoCommand {
    pub fn new(echo_message: &[Box<dyn RedisDataType>]) -> Self {
        let message = if let Some(first) = echo_message.first() {
            if let Some(bulk_string) = first.as_any().downcast_ref::<BulkString>() {
                bulk_string.value.clone()
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        EchoCommand { message }
    }
}

impl RedisCommand for EchoCommand {
    fn response(&self) -> Result<Vec<u8>> {
        BulkString::new(self.message.clone()).to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::BulkString;

    use super::*;

    #[test]
    fn test_ping_command() {
        let command = PingCommand {};
        let response = command.response().unwrap();
        assert_eq!(response, b"+PONG\r\n");
    }

    #[test]
    fn test_echo_command() {
        let bulk_string: Box<dyn RedisDataType> = Box::new(BulkString::new("Hello".to_string()));
        let command = EchoCommand::new(&[bulk_string]);
        let response = command.response().unwrap();
        assert_eq!(response, b"$5\r\nHello\r\n");
    }
}
