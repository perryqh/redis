use crate::{
    datatypes::{BulkString, NullBulkString, RedisDataType, SimpleString},
    store::{self, Store},
};
use anyhow::{bail, Result};

pub trait RedisCommand: Send {
    fn execute(&self, store: &mut Store) -> Result<Vec<u8>>;
}

pub struct PingCommand {}
impl RedisCommand for PingCommand {
    fn execute(&self, _store: &mut Store) -> Result<Vec<u8>> {
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
    fn execute(&self, _store: &mut Store) -> Result<Vec<u8>> {
        BulkString::new(self.message.clone()).to_bytes()
    }
}

pub struct SetCommand {
    pub key: String,
    pub value: String,
}

impl SetCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let key = if let Some(first) = input_array.first() {
            if let Some(bulk_string) = first.as_any().downcast_ref::<BulkString>() {
                bulk_string.value.clone()
            } else {
                bail!("Expected bulk string key")
            }
        } else {
            bail!("Expected key")
        };
        let value = if let Some(second) = input_array.get(1) {
            if let Some(bulk_string) = second.as_any().downcast_ref::<BulkString>() {
                bulk_string.value.clone()
            } else {
                bail!("Expected bulk string value")
            }
        } else {
            bail!("Expected value")
        };
        Ok(SetCommand { key, value })
    }
}

impl RedisCommand for SetCommand {
    fn execute(&self, store: &mut Store) -> Result<Vec<u8>> {
        store.set(self.key.clone(), self.value.clone());
        SimpleString::new("OK".to_string()).to_bytes()
    }
}

pub struct GetCommand {
    pub key: String,
}

impl GetCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let key = if let Some(first) = input_array.first() {
            if let Some(bulk_string) = first.as_any().downcast_ref::<BulkString>() {
                bulk_string.value.clone()
            } else {
                bail!("Expected bulk string key")
            }
        } else {
            bail!("Expected key")
        };
        Ok(GetCommand { key })
    }
}

impl RedisCommand for GetCommand {
    fn execute(&self, store: &mut Store) -> Result<Vec<u8>> {
        match store.get(&self.key) {
            Some(value) => BulkString::new(value.clone()).to_bytes(),
            None => NullBulkString {}.to_bytes(),
        }
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
