use std::time::Duration;

use crate::{
    datatypes::{BulkString, NullBulkString, RedisDataType, SimpleString},
    store::Store,
};
use anyhow::{bail, Context, Result};

/// Helper function to extract a BulkString value from an input array at the specified index
fn extract_bulk_string(
    input_array: &[Box<dyn RedisDataType>],
    index: usize,
    field_name: &str,
) -> Result<String> {
    input_array
        .get(index)
        .context(format!("Expected {}", field_name))?
        .as_any()
        .downcast_ref::<BulkString>()
        .map(|bs| bs.value.clone())
        .context(format!("Expected bulk string for {}", field_name))
}

pub trait RedisCommand: Send {
    fn execute(&self, store: &Store) -> Result<Vec<u8>>;
}

pub struct PingCommand {}
impl RedisCommand for PingCommand {
    fn execute(&self, _store: &Store) -> Result<Vec<u8>> {
        SimpleString::new("PONG".to_string()).to_bytes()
    }
}

pub struct EchoCommand {
    message: String,
}

impl EchoCommand {
    pub fn new(echo_message: &[Box<dyn RedisDataType>]) -> Self {
        let message = extract_bulk_string(echo_message, 0, "message").unwrap_or_default();
        EchoCommand { message }
    }
}

impl RedisCommand for EchoCommand {
    fn execute(&self, _store: &Store) -> Result<Vec<u8>> {
        BulkString::new(self.message.clone()).to_bytes()
    }
}

#[derive(Debug)]
pub struct SetCommand {
    pub key: String,
    pub value: String,
    pub ttl: Option<Duration>,
}

impl SetCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        // Extract required arguments
        let key = extract_bulk_string(input_array, 0, "key")?;
        let value = extract_bulk_string(input_array, 1, "value")?;

        // Parse optional TTL arguments
        let ttl = Self::parse_ttl_options(input_array)?;

        Ok(SetCommand { key, value, ttl })
    }

    /// Parse optional TTL options (EX or PX) from the input array
    fn parse_ttl_options(input_array: &[Box<dyn RedisDataType>]) -> Result<Option<Duration>> {
        // Check if we have both option and value arguments
        if input_array.len() < 4 {
            return Ok(None);
        }

        // Extract the option name (EX or PX)
        let option_name = extract_bulk_string(input_array, 2, "TTL option")?.to_uppercase();

        // Extract the TTL value as a string and parse it
        let ttl_string = extract_bulk_string(input_array, 3, "TTL value")?;
        let ttl_value = ttl_string.parse::<i64>().context(format!(
            "Invalid TTL value: '{}'. Expected a positive integer",
            ttl_string
        ))?;

        // Validate TTL value is positive
        if ttl_value <= 0 {
            bail!("TTL value must be positive, got: {}", ttl_value);
        }

        // Convert to Duration based on option
        let duration = match option_name.as_str() {
            "EX" => Duration::from_secs(ttl_value as u64),
            "PX" => Duration::from_millis(ttl_value as u64),
            _ => bail!(
                "Unknown TTL option: '{}'. Expected 'EX' or 'PX'",
                option_name
            ),
        };

        Ok(Some(duration))
    }
}

impl RedisCommand for SetCommand {
    fn execute(&self, store: &Store) -> Result<Vec<u8>> {
        if let Some(ttl) = self.ttl {
            store.set_with_expiration(self.key.clone(), self.value.clone(), ttl);
        } else {
            store.set(self.key.clone(), self.value.clone());
        }
        SimpleString::new("OK".to_string()).to_bytes()
    }
}

#[derive(Debug)]
pub struct GetCommand {
    pub key: String,
}

impl GetCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let key = extract_bulk_string(input_array, 0, "key")?;
        Ok(GetCommand { key })
    }
}

impl RedisCommand for GetCommand {
    fn execute(&self, store: &Store) -> Result<Vec<u8>> {
        match store.get(&self.key) {
            Some(value) => BulkString::new(value).to_bytes(),
            None => NullBulkString {}.to_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::{BulkString, Integer, SimpleString};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_ping_command() {
        let store = Store::new();
        let command = PingCommand {};
        let response = command.execute(&store).unwrap();
        assert_eq!(response, b"+PONG\r\n");
    }

    #[test]
    fn test_echo_command() {
        let store = Store::new();
        let bulk_string: Box<dyn RedisDataType> = Box::new(BulkString::new("Hello".to_string()));
        let command = EchoCommand::new(&[bulk_string]);
        let response = command.execute(&store).unwrap();
        assert_eq!(response, b"$5\r\nHello\r\n");
    }

    #[test]
    fn test_set_command_basic() {
        let store = Store::new();
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("mykey".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("myvalue".to_string()));

        let command = SetCommand::new(&[key, value]).unwrap();
        let response = command.execute(&store).unwrap();

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(store.get("mykey"), Some("myvalue".to_string()));
    }

    #[test]
    fn test_set_command_overwrite() {
        let store = Store::new();

        // Set initial value
        let key1: Box<dyn RedisDataType> = Box::new(BulkString::new("key1".to_string()));
        let value1: Box<dyn RedisDataType> = Box::new(BulkString::new("value1".to_string()));
        let command1 = SetCommand::new(&[key1, value1]).unwrap();
        command1.execute(&store).unwrap();
        assert_eq!(store.get("key1"), Some("value1".to_string()));

        // Overwrite with new value
        let key2: Box<dyn RedisDataType> = Box::new(BulkString::new("key1".to_string()));
        let value2: Box<dyn RedisDataType> = Box::new(BulkString::new("value2".to_string()));
        let command2 = SetCommand::new(&[key2, value2]).unwrap();
        command2.execute(&store).unwrap();
        assert_eq!(store.get("key1"), Some("value2".to_string()));
    }

    #[test]
    fn test_set_command_with_ex_option() {
        let store = Store::new();
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("tempkey".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("tempvalue".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("EX".to_string()));
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("1".to_string())); // 1 second

        let command = SetCommand::new(&[key, value, option, ttl]).unwrap();
        let response = command.execute(&store).unwrap();

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(store.get("tempkey"), Some("tempvalue".to_string()));

        // Wait for expiration
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(store.get("tempkey"), None);
    }

    #[test]
    fn test_set_command_with_px_option() {
        let store = Store::new();
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("tempkey2".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("tempvalue2".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("PX".to_string()));
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("500".to_string())); // 500 milliseconds

        let command = SetCommand::new(&[key, value, option, ttl]).unwrap();
        let response = command.execute(&store).unwrap();

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(store.get("tempkey2"), Some("tempvalue2".to_string()));

        // Wait for expiration
        thread::sleep(Duration::from_millis(600));
        assert_eq!(store.get("tempkey2"), None);
    }

    #[test]
    fn test_set_command_ex_lowercase() {
        let _store = Store::new();
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key_ex".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("val_ex".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("ex".to_string())); // lowercase
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("1".to_string()));

        let command = SetCommand::new(&[key, value, option, ttl]).unwrap();
        assert!(command.ttl.is_some());
        assert_eq!(command.ttl.unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn test_set_command_px_uppercase() {
        let _store = Store::new();
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key_px".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("val_px".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("PX".to_string())); // uppercase
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("1000".to_string()));

        let command = SetCommand::new(&[key, value, option, ttl]).unwrap();
        assert!(command.ttl.is_some());
        assert_eq!(command.ttl.unwrap(), Duration::from_millis(1000));
    }

    #[test]
    fn test_set_command_without_ttl() {
        let store = Store::new();
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("persistent".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("forever".to_string()));

        let command = SetCommand::new(&[key, value]).unwrap();
        assert!(command.ttl.is_none());

        command.execute(&store).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert_eq!(store.get("persistent"), Some("forever".to_string()));
    }

    #[test]
    fn test_set_command_replaces_ttl() {
        let store = Store::new();

        // Set with expiration
        let key1: Box<dyn RedisDataType> = Box::new(BulkString::new("key_ttl".to_string()));
        let value1: Box<dyn RedisDataType> = Box::new(BulkString::new("val1".to_string()));
        let option1: Box<dyn RedisDataType> = Box::new(BulkString::new("PX".to_string()));
        let ttl1: Box<dyn RedisDataType> = Box::new(BulkString::new("200".to_string()));
        let command1 = SetCommand::new(&[key1, value1, option1, ttl1]).unwrap();
        command1.execute(&store).unwrap();

        // Overwrite without expiration
        let key2: Box<dyn RedisDataType> = Box::new(BulkString::new("key_ttl".to_string()));
        let value2: Box<dyn RedisDataType> = Box::new(BulkString::new("val2".to_string()));
        let command2 = SetCommand::new(&[key2, value2]).unwrap();
        command2.execute(&store).unwrap();

        // Wait past original expiration time
        thread::sleep(Duration::from_millis(250));
        // Should still exist because we overwrote without TTL
        assert_eq!(store.get("key_ttl"), Some("val2".to_string()));
    }

    #[test]
    fn test_get_command_existing_key() {
        let store = Store::new();
        store.set("existing".to_string(), "value".to_string());

        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("existing".to_string()));
        let command = GetCommand::new(&[key]).unwrap();
        let response = command.execute(&store).unwrap();

        assert_eq!(response, b"$5\r\nvalue\r\n");
    }

    #[test]
    fn test_get_command_nonexistent_key() {
        let store = Store::new();

        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("nonexistent".to_string()));
        let command = GetCommand::new(&[key]).unwrap();
        let response = command.execute(&store).unwrap();

        assert_eq!(response, b"$-1\r\n"); // Null bulk string
    }

    #[test]
    fn test_get_command_expired_key() {
        let store = Store::new();
        store.set_with_expiration(
            "expired".to_string(),
            "value".to_string(),
            Duration::from_millis(50),
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(100));

        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("expired".to_string()));
        let command = GetCommand::new(&[key]).unwrap();
        let response = command.execute(&store).unwrap();

        assert_eq!(response, b"$-1\r\n"); // Should return null for expired key
    }

    #[test]
    fn test_set_command_missing_key() {
        let result = SetCommand::new(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected key"));
    }

    #[test]
    fn test_set_command_missing_value() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let result = SetCommand::new(&[key]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected value"));
    }

    #[test]
    fn test_get_command_missing_key() {
        let result = GetCommand::new(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected key"));
    }

    #[test]
    fn test_set_command_invalid_ttl_option() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("value".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("INVALID".to_string()));
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("100".to_string()));

        let result = SetCommand::new(&[key, value, option, ttl]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown TTL option"));
    }

    #[test]
    fn test_set_command_negative_ttl() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("value".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("EX".to_string()));
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("-5".to_string()));

        let result = SetCommand::new(&[key, value, option, ttl]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("TTL value must be positive"));
    }

    #[test]
    fn test_set_command_zero_ttl() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("value".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("PX".to_string()));
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("0".to_string()));

        let result = SetCommand::new(&[key, value, option, ttl]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("TTL value must be positive"));
    }

    #[test]
    fn test_set_command_wrong_type_for_key() {
        let key: Box<dyn RedisDataType> = Box::new(Integer::new(123)); // Wrong type
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("value".to_string()));

        let result = SetCommand::new(&[key, value]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected bulk string for key"));
    }

    #[test]
    fn test_set_command_wrong_type_for_value() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(SimpleString::new("value".to_string())); // Wrong type

        let result = SetCommand::new(&[key, value]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected bulk string for value"));
    }

    #[test]
    fn test_set_command_wrong_type_for_ttl_option() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("value".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(Integer::new(100)); // Wrong type
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("100".to_string()));

        let result = SetCommand::new(&[key, value, option, ttl]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected bulk string for TTL option"));
    }

    #[test]
    fn test_set_command_wrong_type_for_ttl_value() {
        let key: Box<dyn RedisDataType> = Box::new(BulkString::new("key".to_string()));
        let value: Box<dyn RedisDataType> = Box::new(BulkString::new("value".to_string()));
        let option: Box<dyn RedisDataType> = Box::new(BulkString::new("EX".to_string()));
        let ttl: Box<dyn RedisDataType> = Box::new(BulkString::new("not_a_number".to_string())); // Invalid format

        let result = SetCommand::new(&[key, value, option, ttl]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid TTL value"));
    }
}
