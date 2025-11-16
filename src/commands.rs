use std::time::Duration;

use crate::{
    context::AppContext,
    datatypes::{
        Array, BulkString, Integer, NullBulkString, RedisDataType, SimpleError, SimpleString,
    },
    rdb::EMPTY_RDB,
    replication::ReplicationRole,
};
use anyhow::{bail, Context, Result};

/// Represents the action to take after executing a command
#[derive(Debug)]
pub enum CommandAction {
    /// Regular response to send back to the client
    Response(Vec<u8>),
    /// PSYNC handshake: send response, then RDB file, then become replication stream
    PsyncHandshake {
        response: Vec<u8>,
        rdb_data: Vec<u8>,
    },
}

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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction>;
}

pub struct PingCommand {}
impl RedisCommand for PingCommand {
    fn execute(&self, _app_context: &AppContext) -> Result<CommandAction> {
        let response = SimpleString::new("PONG".to_string()).to_bytes()?;
        Ok(CommandAction::Response(response))
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
    fn execute(&self, _app_context: &AppContext) -> Result<CommandAction> {
        let response = BulkString::new(self.message.clone()).to_bytes()?;
        Ok(CommandAction::Response(response))
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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        let mut len = 0;
        for value in &self.values {
            len = app_context.store.rpush(self.key.clone(), value.clone());
        }
        let response = Integer::new(len as i32).to_bytes()?;
        Ok(CommandAction::Response(response))
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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        let response = match app_context.store.rpop(&self.key) {
            Some(value) => BulkString::new(value).to_bytes()?,
            None => NullBulkString {}.to_bytes()?,
        };
        Ok(CommandAction::Response(response))
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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        if let Some(ttl) = self.ttl {
            app_context
                .store
                .set_string_with_expiration(self.key.clone(), self.value.clone(), ttl);
        } else {
            app_context
                .store
                .set_string(self.key.clone(), self.value.clone());
        }
        let response = SimpleString::new("OK".to_string()).to_bytes()?;
        Ok(CommandAction::Response(response))
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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        let response = match app_context.store.get_string(&self.key) {
            Some(value) => BulkString::new(value).to_bytes()?,
            None => NullBulkString {}.to_bytes()?,
        };
        Ok(CommandAction::Response(response))
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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        let response = match self.action {
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
                        _ => values.push(Box::new(BulkString::new("".to_string()))),
                    }
                }
                Array::new(values).to_bytes()?
            }
        };
        Ok(CommandAction::Response(response))
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
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        let keys: Vec<String> = app_context.store.keys(&self.pattern)?;
        let bulk_strings = keys
            .into_iter()
            .map(|key| Box::new(BulkString::new(key)) as Box<dyn RedisDataType>)
            .collect();
        let response = Array::new(bulk_strings).to_bytes()?;
        Ok(CommandAction::Response(response))
    }
}

#[derive(Debug)]
pub enum InfoSection {
    Replication,
}

#[derive(Debug)]
pub struct InfoCommand {
    pub sections: Vec<InfoSection>,
}

impl InfoCommand {
    pub fn new(_input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        Ok(InfoCommand {
            sections: vec![InfoSection::Replication],
        })
    }
}

impl RedisCommand for InfoCommand {
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        let mut info = String::new();

        for section in &self.sections {
            match section {
                InfoSection::Replication => match app_context.replication_role.as_ref() {
                    ReplicationRole::Leader(leader_replication) => {
                        info.push_str("role:master\n");
                        info.push_str("master_replid:");
                        info.push_str(leader_replication.replication_id.as_str());
                        info.push('\n');
                        info.push_str("master_repl_offset:");
                        info.push_str(leader_replication.replication_offset.to_string().as_str());
                        info.push('\n');
                    }
                    ReplicationRole::Follower(_) => {
                        info.push_str("role:slave\n");
                    }
                },
            }
        }

        let response = BulkString::new(info).to_bytes()?;
        Ok(CommandAction::Response(response))
    }
}

pub struct ReplConfCommand {}
impl RedisCommand for ReplConfCommand {
    fn execute(&self, _app_context: &AppContext) -> Result<CommandAction> {
        let response = SimpleString::new("OK".to_string()).to_bytes()?;
        Ok(CommandAction::Response(response))
    }
}

pub struct PsyncCommand {
    // I believe that the follower sends this as a sanity check. It should be
    // "this" replication ID
    pub replication_id: String,
    pub replication_offset: i64,
}

impl PsyncCommand {
    pub fn new(input_array: &[Box<dyn RedisDataType>]) -> Result<Self> {
        let replication_id = extract_bulk_string(input_array, 0, "replication_id")?;
        let offset = extract_bulk_string(input_array, 1, "offset")?;
        Ok(Self {
            replication_id,
            replication_offset: offset.parse()?,
        })
    }
}

impl RedisCommand for PsyncCommand {
    fn execute(&self, app_context: &AppContext) -> Result<CommandAction> {
        if let ReplicationRole::Leader(leader_replication) = app_context.replication_role.as_ref() {
            let response_text = format!(
                "FULLRESYNC {} {}",
                leader_replication.replication_id, leader_replication.replication_offset
            );
            dbg!("response_text: {}", &response_text);
            let response = SimpleString::new(response_text).to_bytes()?;
            use base64::Engine;
            let data = base64::engine::general_purpose::STANDARD
                .decode(EMPTY_RDB)
                .unwrap();
            let mut rdb_data = format!("${}\r\n", data.len()).into_bytes();
            rdb_data.extend_from_slice(&data);
            Ok(CommandAction::PsyncHandshake { response, rdb_data })
        } else {
            dbg!("PSYNC not supported in follower mode");
            let error = SimpleError::new("PSYNC not supported in follower mode".to_string());
            let response = error.to_bytes()?;
            Ok(CommandAction::Response(response))
        }
    }
}

/// Helper function for tests to extract response bytes from CommandAction
#[cfg(test)]
fn extract_response(action: CommandAction) -> Vec<u8> {
    match action {
        CommandAction::Response(bytes) => bytes,
        CommandAction::PsyncHandshake { response, .. } => response,
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::{BulkString, Integer, SimpleString};
    use crate::replication::{FollowerReplication, LeaderReplication};
    use std::sync::Arc;
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
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"*2\r\n$3\r\ndir\r\n$12\r\n~/redis-rust\r\n");

        let command = ConfigCommand::new(&[bulk_string("GET"), bulk_string("dbfilename")])?;
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"*2\r\n$10\r\ndbfilename\r\n$8\r\ndump.rdb\r\n");

        Ok(())
    }

    #[test]
    fn test_ping_command() -> Result<()> {
        let app_context = AppContext::default();
        let command = PingCommand {};
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"+PONG\r\n");
        Ok(())
    }

    #[test]
    fn test_echo_command() -> Result<()> {
        let app_context = AppContext::default();
        let command = EchoCommand::new(&[bulk_string("Hello")]);
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"$5\r\nHello\r\n");
        Ok(())
    }

    #[test]
    fn test_set_command_basic() -> Result<()> {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_args("mykey", "myvalue")).unwrap();
        let response = extract_response(command.execute(&app_context)?);

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("mykey"),
            Some("myvalue".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_set_command_overwrite() -> Result<()> {
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
        Ok(())
    }

    #[test]
    fn test_set_command_with_ex_option() -> Result<()> {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_with_expiration(
            "tempkey",
            "tempvalue",
            "EX",
            "1",
        ))
        .unwrap();
        let response = extract_response(command.execute(&app_context)?);

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("tempkey"),
            Some("tempvalue".to_string())
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(app_context.store.get_string("tempkey"), None);
        Ok(())
    }

    #[test]
    fn test_set_command_with_px_option() -> Result<()> {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_with_expiration(
            "tempkey2",
            "tempvalue2",
            "PX",
            "500",
        ))
        .unwrap();
        let response = extract_response(command.execute(&app_context)?);

        assert_eq!(response, b"+OK\r\n");
        assert_eq!(
            app_context.store.get_string("tempkey2"),
            Some("tempvalue2".to_string())
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(600));
        assert_eq!(app_context.store.get_string("tempkey2"), None);
        Ok(())
    }

    #[test]
    fn test_set_command_ex_lowercase() -> Result<()> {
        let command =
            SetCommand::new(&set_command_with_expiration("key_ex", "val_ex", "ex", "1")).unwrap();
        assert!(command.ttl.is_some());
        Ok(())
    }

    #[test]
    fn test_set_command_px_uppercase() -> Result<()> {
        let command = SetCommand::new(&set_command_with_expiration(
            "key_px", "val_px", "PX", "500",
        ))
        .unwrap();
        assert!(command.ttl.is_some());
        Ok(())
    }

    #[test]
    fn test_set_command_without_ttl() -> Result<()> {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_args("persistent", "forever")).unwrap();
        assert!(command.ttl.is_none());
        command.execute(&app_context).unwrap();
        assert_eq!(
            app_context.store.get_string("persistent"),
            Some("forever".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_keys_command() -> Result<()> {
        let app_context = AppContext::default();
        let command = SetCommand::new(&set_command_args("foo", "bar"))?;
        command.execute(&app_context)?;

        let command = KeysCommand::new(&[bulk_string("\"foo\"")]).unwrap();
        let result = extract_response(command.execute(&app_context)?);
        assert_eq!(result, b"*1\r\n$3\r\nfoo\r\n");

        Ok(())
    }

    #[test]
    fn test_set_command_replaces_ttl() -> Result<()> {
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
        Ok(())
    }

    #[test]
    fn test_get_command_existing_key() -> Result<()> {
        let app_context = AppContext::default();
        app_context
            .store
            .set_string("mykey".to_string(), "value".to_string());
        let command = GetCommand::new(&[bulk_string("mykey")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"$5\r\nvalue\r\n");
        Ok(())
    }

    #[test]
    fn test_get_command_nonexistent_key() -> Result<()> {
        let app_context = AppContext::default();

        let command = GetCommand::new(&[bulk_string("nonexistent")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);

        assert_eq!(response, b"$-1\r\n"); // Null bulk string
        Ok(())
    }

    #[test]
    fn test_get_command_expired_key() -> Result<()> {
        let app_context = AppContext::default();
        app_context.store.set_string_with_expiration(
            "expired".to_string(),
            "value".to_string(),
            Duration::from_millis(50),
        );

        // Wait for expiration
        thread::sleep(Duration::from_millis(600));
        let command = GetCommand::new(&[bulk_string("tempkey")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);

        assert_eq!(response, b"$-1\r\n"); // Null bulk string after expiration
        Ok(())
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
    fn test_rpush_command_single_value() -> Result<()> {
        let app_context = AppContext::default();
        let command = RpushCommand::new(&[bulk_string("mylist"), bulk_string("value1")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b":1\r\n");
        Ok(())
    }

    #[test]
    fn test_rpush_command_multiple_values() -> Result<()> {
        let app_context = AppContext::default();
        let command = RpushCommand::new(&[
            bulk_string("mylist"),
            bulk_string("a"),
            bulk_string("b"),
            bulk_string("c"),
        ])
        .unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b":3\r\n");
        Ok(())
    }

    #[test]
    fn test_rpush_command_append() -> Result<()> {
        let app_context = AppContext::default();
        app_context
            .store
            .rpush("mylist".to_string(), "existing".to_string());
        let command = RpushCommand::new(&[bulk_string("mylist"), bulk_string("new")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b":2\r\n");
        Ok(())
    }

    #[test]
    fn test_rpush_command_missing_value() {
        let result = RpushCommand::new(&[bulk_string("mylist")]);
        assert!(result.is_err());
    }

    // RPOP command tests
    #[test]
    fn test_rpop_command_existing_list() -> Result<()> {
        let app_context = AppContext::default();
        app_context
            .store
            .rpush("mylist".to_string(), "value".to_string());
        app_context
            .store
            .rpush("mylist".to_string(), "value2".to_string());

        let command = RpopCommand::new(&[bulk_string("mylist")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"$6\r\nvalue2\r\n");
        Ok(())
    }

    #[test]
    fn test_rpop_command_nonexistent_key() -> Result<()> {
        let app_context = AppContext::default();
        let command = RpopCommand::new(&[bulk_string("nonexistent")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"$-1\r\n");
        Ok(())
    }

    #[test]
    fn test_rpop_command_empty_list() -> Result<()> {
        let app_context = AppContext::default();
        app_context
            .store
            .rpush("mylist".to_string(), "value".to_string());
        // Pop the only element
        app_context.store.rpop("mylist");

        let command = RpopCommand::new(&[bulk_string("mylist")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"$-1\r\n");
        Ok(())
    }

    #[test]
    fn test_rpop_command_on_string_key() -> Result<()> {
        let app_context = AppContext::default();
        // Set a string key
        app_context
            .store
            .set_string("stringkey".to_string(), "value".to_string());

        let command = RpopCommand::new(&[bulk_string("stringkey")]).unwrap();
        let response = extract_response(command.execute(&app_context)?);
        assert_eq!(response, b"$-1\r\n"); // Wrong type
        Ok(())
    }

    #[test]
    fn test_info_command() -> Result<()> {
        let leader_replication = LeaderReplication::default();
        let app_context = AppContext {
            replication_role: Arc::new(ReplicationRole::Leader(leader_replication.clone())),
            ..Default::default()
        };
        let command = InfoCommand::new(&[])?;
        let response = extract_response(command.execute(&app_context)?);
        let expected_string = format!(
            "role:master\nmaster_replid:{}\nmaster_repl_offset:0\n",
            leader_replication.replication_id
        );
        let expected = format!("${}\r\n{}\r\n", expected_string.len(), expected_string);
        let expected = expected.as_bytes();
        assert_eq!(response, expected);
        Ok(())
    }

    #[test]
    fn test_info_command_with_replication_master() -> Result<()> {
        let master_replication = LeaderReplication::default();
        let app_context = AppContext {
            replication_role: Arc::new(ReplicationRole::Leader(master_replication.clone())),
            ..Default::default()
        };
        let command = InfoCommand::new(&[])?;
        let response = extract_response(command.execute(&app_context)?);
        let expected_string = format!(
            "role:master\nmaster_replid:{}\nmaster_repl_offset:0\n",
            master_replication.replication_id
        );
        let expected = format!("${}\r\n{}\r\n", expected_string.len(), expected_string);
        let expected = expected.as_bytes();
        assert_eq!(response, expected);
        Ok(())
    }

    #[test]
    fn test_info_command_with_replication_follower() -> Result<()> {
        let follower_replication = FollowerReplication::default();
        let app_context = AppContext {
            replication_role: Arc::new(ReplicationRole::Follower(follower_replication.clone())),
            ..Default::default()
        };
        let command = InfoCommand::new(&[])?;
        let response = extract_response(command.execute(&app_context)?);
        let expected_string = "role:slave\n".to_string();
        let expected = format!("${}\r\n{}\r\n", expected_string.len(), expected_string);
        let expected = expected.as_bytes();
        assert_eq!(response, expected);
        Ok(())
    }
}
