use std::time::Duration;

use crate::{
    context::AppContext,
    datatypes::{Array, BulkString, Integer, NullBulkString, RedisDataType, SimpleString},
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
    fn execute(&self, app_context: &AppContext) -> Result<Vec<u8>>;
}

pub struct PingCommand {}
impl RedisCommand for PingCommand {
    fn execute<'a>(&self, _app_context: &AppContext) -> Result<Vec<u8>> {
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
    fn execute<'a>(&self, _app_context: &AppContext) -> Result<Vec<u8>> {
        BulkString::new(self.message.clone()).to_bytes()
    }
}

pub struct RpushCommand {
    pub key: String,
    pub values: Vec<String>,
}

impl RpushCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let key = extract_bulk_string(input_array, 0, "key")?;
        let mut values = Vec::new();
        for i in 1..input_array.len() {
            let value = extract_bulk_string(input_array, i, &format!("value{}", i))?;
            values.push(value);
        }
        if values.is_empty() {
            bail!("RPUSH requires at least one value");
        }
        Ok(RpushCommand { key, values })
    }
}

impl RedisCommand for RpushCommand {
    fn execute<'a>(&self, app_context: &AppContext) -> Result<Vec<u8>> {
        let mut len = 0;
        for value in &self.values {
            len = app_context.store.rpush(self.key.clone(), value.clone());
        }
        Integer::new(len as i32).to_bytes()
    }
}

pub struct RpopCommand {
    pub key: String,
}

impl RpopCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let key = extract_bulk_string(input_array, 0, "key")?;
        Ok(RpopCommand { key })
    }
}

impl RedisCommand for RpopCommand {
    fn execute<'a>(&self, app_context: &AppContext) -> Result<Vec<u8>> {
        match app_context.store.rpop(&self.key) {
            Some(value) => BulkString::new(value).to_bytes(),
            None => NullBulkString {}.to_bytes(),
        }
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
    fn execute<'a>(&self, app_context: &AppContext) -> Result<Vec<u8>> {
        if let Some(ttl) = self.ttl {
            app_context
                .store
                .set_string_with_expiration(self.key.clone(), self.value.clone(), ttl);
        } else {
            app_context
                .store
                .set_string(self.key.clone(), self.value.clone());
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
    fn execute<'a>(&self, app_context: &AppContext) -> Result<Vec<u8>> {
        match app_context.store.get_string(&self.key) {
            Some(value) => BulkString::new(value).to_bytes(),
            None => NullBulkString {}.to_bytes(),
        }
    }
}

#[derive(Debug)]
pub enum ConfigAction {
    Get(Vec<String>),
}

#[derive(Debug)]
pub struct ConfigCommand {
    pub action: ConfigAction,
}

impl ConfigCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        // Extract required arguments
        let action = extract_bulk_string(input_array, 0, "action")?;
        if action.to_uppercase() != "GET" {
            bail!("Unsupported config action {}", action)
        }
        let key = extract_bulk_string(input_array, 1, "key")?;
        Ok(Self {
            action: ConfigAction::Get(vec![key]),
        })
    }
}

impl RedisCommand for ConfigCommand {
    fn execute<'a>(&self, app_context: &AppContext) -> Result<Vec<u8>> {
        match self.action {
            ConfigAction::Get(ref keys) => {
                let mut values: Vec<Box<dyn RedisDataType>> = Vec::new();
                for key in keys {
                    values.push(Box::new(BulkString::new(key.clone())));
                    match key.to_lowercase().as_str() {
                        "dir" => {
                            values.push(Box::new(BulkString::new(app_context.config.dir.clone())))
                        }
                        "dbfilename" => values.push(Box::new(BulkString::new(
                            app_context.config.dbfilename.clone(),
                        ))),
                        _ => values.push(Box::new(NullBulkString {})),
                    }
                }
                Array::new(values).to_bytes()
            }
        }
    }
}

#[derive(Debug)]
pub struct KeysCommand {
    pub pattern: String,
}

impl KeysCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let pattern = extract_bulk_string(input_array, 0, "pattern")?
            .trim_matches('"')
            .to_string();
        Ok(KeysCommand { pattern })
    }
}

impl RedisCommand for KeysCommand {
    fn execute<'a>(&self, app_context: &AppContext) -> Result<Vec<u8>> {
        let keys: Vec<String> = app_context.store.keys(&self.pattern)?;
        let bulk_strings = keys
            .into_iter()
            .map(|key| Box::new(BulkString::new(key)) as Box<dyn RedisDataType>)
            .collect();
        Array::new(bulk_strings).to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::{BulkString, Integer, SimpleString};
    use std::thread;
    use std::time::Duration;

    use super::*;

    // Helper function to create a BulkString
    fn bulk_string(s: &str) -> Box<dyn RedisDataType> {
        Box::new(BulkString::new(s.to_string()))
    }

    // Helper function to create SET command args
    fn set_command_args(key: &str, value: &str) -> Vec<Box<dyn RedisDataType>> {
        vec![bulk_string(key), bulk_string(value)]
    }

    // Helper function to create SET command with expiration args
    fn set_command_with_expiration(
        key: &str,
        value: &str,
        option: &str,
        ttl: &str,
    ) -> Vec<Box<dyn RedisDataType>> {
        vec![
            bulk_string(key),
            bulk_string(value),
            bulk_string(option),
            bulk_string(ttl),
        ]
    }

    #[test]
    fn test_config_get_command() -> Result<()> {
        let app_context = AppContext::default();
        let command = ConfigCommand::new(&[bulk_string("GET"), bulk_string("dir")])?;
        let response = command.execute(&app_context)?;
        assert_eq!(response, b"*2\r\n$3\r\ndir\r\n$12\r\n~/redis-rust\r\n");

        let command = ConfigCommand::new(&[bulk_string("GET"), bulk_string("dbfilename")])?;
        let response = command.execute(&app_context)?;
        assert_eq!(response, b"*2\r\n$10\r\ndbfilename\r\n$8\r\ndump.rdb\r\n");

        Ok(())
    }

    #[test]
    fn test_ping_command() {
        let app_context = AppContext::default();
        let command = PingCommand {};
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b"+PONG\r\n");
    }

    #[test]
    fn test_echo_command() {
        let app_context = AppContext::default();
        let command = EchoCommand::new(&[bulk_string("Hello")]);
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b"$5\r\nHello\r\n");
    }

    #[test]
    fn test_set_command_basic() {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_args("mykey", "myvalue")).unwrap();
        let response = command.execute(&app_context).unwrap();

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("mykey"),
            Some("myvalue".to_string())
        );
    }

    #[test]
    fn test_set_command_overwrite() {
        let app_context = AppContext::default();

        // Set initial value
        let command1 = SetCommand::new(&set_command_args("key1", "value1")).unwrap();
        command1.execute(&app_context).unwrap();
        assert_eq!(
            app_context.store.get_string("key1"),
            Some("value1".to_string())
        );

        // Overwrite with new value
        let command2 = SetCommand::new(&set_command_args("key1", "value2")).unwrap();
        command2.execute(&app_context).unwrap();
        assert_eq!(
            app_context.store.get_string("key1"),
            Some("value2".to_string())
        );
    }

    #[test]
    fn test_set_command_with_ex_option() {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_with_expiration(
            "tempkey",
            "tempvalue",
            "EX",
            "1",
        ))
        .unwrap();
        let response = command.execute(&app_context).unwrap();

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("tempkey"),
            Some("tempvalue".to_string())
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(app_context.store.get_string("tempkey"), None);
    }

    #[test]
    fn test_set_command_with_px_option() {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_with_expiration(
            "tempkey2",
            "tempvalue2",
            "PX",
            "500",
        ))
        .unwrap();
        let response = command.execute(&app_context).unwrap();

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("tempkey2"),
            Some("tempvalue2".to_string())
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(600));
        assert_eq!(app_context.store.get_string("tempkey2"), None);
    }

    #[test]
    fn test_set_command_ex_lowercase() {
        let command =
            SetCommand::new(&set_command_with_expiration("key_ex", "val_ex", "ex", "1")).unwrap();
        assert!(command.ttl.is_some());
        assert_eq!(command.ttl.unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn test_set_command_px_uppercase() {
        let command = SetCommand::new(&set_command_with_expiration(
            "key_px", "val_px", "PX", "500",
        ))
        .unwrap();
        assert!(command.ttl.is_some());
        assert_eq!(command.ttl.unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn test_set_command_without_ttl() {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_args("persistent", "forever")).unwrap();
        assert!(command.ttl.is_none());

        command.execute(&app_context).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            app_context.store.get_string("persistent"),
            Some("forever".to_string())
        );
    }

    #[test]
    fn test_keys_command() -> Result<()> {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_args("foo", "bar"))?;
        command.execute(&app_context)?;

        let command = KeysCommand::new(&[bulk_string("\"foo\"")]).unwrap();
        let result = command.execute(&app_context)?;
        assert_eq!(result, b"*1\r\n$3\r\nfoo\r\n");

        Ok(())
    }

    #[test]
    fn test_set_command_replaces_ttl() {
        let app_context = AppContext::default();

        // Set with TTL
        let command1 =
            SetCommand::new(&set_command_with_expiration("key_ttl", "val1", "EX", "1")).unwrap();
        command1.execute(&app_context).unwrap();

        // Immediately overwrite without TTL
        let command2 = SetCommand::new(&set_command_args("key_ttl", "val2")).unwrap();
        command2.execute(&app_context).unwrap();

        // Wait past original expiration
        thread::sleep(Duration::from_millis(1100));
        // Should still exist because second SET removed TTL
        assert_eq!(
            app_context.store.get_string("key_ttl"),
            Some("val2".to_string())
        );
    }

    #[test]
    fn test_get_command_existing_key() {
        let app_context = AppContext::default();
        app_context
            .store
            .set_string("existing".to_string(), "value".to_string());

        let command = GetCommand::new(&[bulk_string("existing")]).unwrap();
        let response = command.execute(&app_context).unwrap();

        assert_eq!(response, b"$5\r\nvalue\r\n");
    }

    #[test]
    fn test_get_command_nonexistent_key() {
        let app_context = AppContext::default();

        let command = GetCommand::new(&[bulk_string("nonexistent")]).unwrap();
        let response = command.execute(&app_context).unwrap();

        assert_eq!(response, b"$-1\r\n"); // Null bulk string
    }

    #[test]
    fn test_get_command_expired_key() {
        let app_context = AppContext::default();
        app_context.store.set_string_with_expiration(
            "expired".to_string(),
            "value".to_string(),
            Duration::from_millis(50),
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(100));

        let command = GetCommand::new(&[bulk_string("expired")]).unwrap();
        let response = command.execute(&app_context).unwrap();

        assert_eq!(response, b"$-1\r\n"); // Null bulk string for expired key
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
    // RPUSH command tests
    #[test]
    fn test_rpush_command_single_value() {
        let app_context = AppContext::default();
        let command = RpushCommand::new(&[bulk_string("mylist"), bulk_string("value1")]).unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b":1\r\n");
    }

    #[test]
    fn test_rpush_command_multiple_values() {
        let app_context = AppContext::default();
        let command = RpushCommand::new(&[
            bulk_string("mylist"),
            bulk_string("a"),
            bulk_string("b"),
            bulk_string("c"),
        ])
        .unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b":3\r\n");
    }

    #[test]
    fn test_rpush_command_append() {
        let app_context = AppContext::default();
        app_context
            .store
            .rpush("mylist".to_string(), "existing".to_string());
        let command = RpushCommand::new(&[bulk_string("mylist"), bulk_string("new")]).unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b":2\r\n");
    }

    #[test]
    fn test_rpush_command_missing_value() {
        let result = RpushCommand::new(&[bulk_string("mylist")]);
        assert!(result.is_err());
    }

    // RPOP command tests
    #[test]
    fn test_rpop_command_existing_list() {
        let app_context = AppContext::default();
        app_context
            .store
            .rpush("mylist".to_string(), "value1".to_string());
        app_context
            .store
            .rpush("mylist".to_string(), "value2".to_string());

        let command = RpopCommand::new(&[bulk_string("mylist")]).unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b"$6\r\nvalue2\r\n");
    }

    #[test]
    fn test_rpop_command_nonexistent_key() {
        let app_context = AppContext::default();
        let command = RpopCommand::new(&[bulk_string("nonexistent")]).unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn test_rpop_command_empty_list() {
        let app_context = AppContext::default();
        app_context
            .store
            .rpush("mylist".to_string(), "only".to_string());
        app_context.store.rpop("mylist");

        let command = RpopCommand::new(&[bulk_string("mylist")]).unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn test_rpop_command_on_string_key() {
        let app_context = AppContext::default();
        app_context
            .store
            .set_string("stringkey".to_string(), "value".to_string());

        let command = RpopCommand::new(&[bulk_string("stringkey")]).unwrap();
        let response = command.execute(&app_context).unwrap();
        assert_eq!(response, b"$-1\r\n"); // Wrong type
    }
}
