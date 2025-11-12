use std::io::{Cursor, Read};

use anyhow::Result;

use crate::commands::{
    EchoCommand, GetCommand, PingCommand, RedisCommand, RpopCommand, RpushCommand, SetCommand,
};
use crate::datatypes::{Array, BulkString, Integer, RedisDataType, SimpleError, SimpleString};

/// Parse a Redis data type from the cursor
pub fn parse_data_type(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];

    // Try to read the first byte
    if cursor.read_exact(&mut byte).is_err() {
        return Ok(None);
    }

    match byte[0] {
        b'*' => parse_array(cursor),
        b'$' => parse_bulk_string(cursor),
        b'+' => parse_simple_string(cursor),
        b':' => parse_integer(cursor),
        b'-' => parse_error(cursor),
        _ => Ok(None),
    }
}

pub fn parse_command(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisCommand>>> {
    // Parse the data type
    if let Some(data_type) = parse_data_type(cursor)? {
        // Check if it's an Array with a command
        if let Some(array) = data_type.as_any().downcast_ref::<Array>() {
            if !array.values.is_empty() {
                if let Some(bulk_string) = array.values[0].as_any().downcast_ref::<BulkString>() {
                    match bulk_string.value.to_uppercase().as_str() {
                        "PING" if array.values.len() == 1 => {
                            return Ok(Some(Box::new(PingCommand {})));
                        }
                        "ECHO" if array.values.len() >= 2 => {
                            let echo_args = &array.values[1..];
                            return Ok(Some(Box::new(EchoCommand::new(echo_args))));
                        }
                        "SET" if array.values.len() >= 3 => {
                            let set_command = SetCommand::new(&array.values[1..])?;
                            return Ok(Some(Box::new(set_command)));
                        }
                        "GET" if array.values.len() >= 2 => {
                            let get_command = GetCommand::new(&array.values[1..])?;
                            return Ok(Some(Box::new(get_command)));
                        }
                        "RPUSH" if array.values.len() >= 3 => {
                            let rpush_command = RpushCommand::new(&array.values[1..])?;
                            return Ok(Some(Box::new(rpush_command)));
                        }
                        "RPOP" if array.values.len() >= 2 => {
                            let rpop_command = RpopCommand::new(&array.values[1..])?;
                            return Ok(Some(Box::new(rpop_command)));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(None)
}
/// Helper function to convert a byte to its ASCII character representation
///
/// Examples:
/// - byte_to_ascii(43) returns '+'
/// - byte_to_ascii(65) returns 'A'
/// - byte_to_ascii(97) returns 'a'
/// - byte_to_ascii(48) returns '0'
#[allow(dead_code)]
fn byte_to_ascii(byte: u8) -> char {
    byte as char
}

/// Alternative ways to work with bytes and ASCII:
///
/// 1. Direct comparison with byte literals:
///    if byte == b'+' { ... }  // b'+' equals 43u8
///
/// 2. Convert byte array to string:
///    let bytes = [72, 101, 108, 108, 111]; // "Hello"
///    let text = String::from_utf8(bytes.to_vec()).unwrap();
///
/// 3. Convert single byte to string:
///    let byte = 65u8; // 'A'
///    let text = (byte as char).to_string();
///
/// 4. Check if byte is ASCII:
///    if byte.is_ascii() { ... }
///    if byte.is_ascii_alphabetic() { ... }
///    if byte.is_ascii_digit() { ... }
///
/// Parse an array from the cursor
/// Format: *<count>\r\n<element1><element2>...<elementN>
fn parse_array(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];
    let mut buffer = Vec::new();

    // Read until \r\n to get the count
    loop {
        if cursor.read_exact(&mut byte).is_err() {
            return Ok(None);
        }

        buffer.push(byte[0]);

        if buffer.len() >= 2 && &buffer[buffer.len() - 2..] == b"\r\n" {
            break;
        }
    }

    // Parse the count
    let count_str = std::str::from_utf8(&buffer[..buffer.len() - 2])?;
    let count = count_str.parse::<usize>()?;

    // Parse each element
    let mut values = Vec::new();
    for _ in 0..count {
        if let Some(element) = parse_data_type(cursor)? {
            values.push(element);
        } else {
            return Ok(None);
        }
    }

    Ok(Some(Box::new(Array { values })))
}

/// Parse a bulk string from the cursor
/// Format: $<length>\r\n<data>\r\n
fn parse_bulk_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];
    let mut buffer = Vec::new();

    // Read until \r\n to get the length
    loop {
        if cursor.read_exact(&mut byte).is_err() {
            return Ok(None);
        }

        buffer.push(byte[0]);

        if buffer.len() >= 2 && &buffer[buffer.len() - 2..] == b"\r\n" {
            break;
        }
    }

    // Parse the length
    let length_str = std::str::from_utf8(&buffer[..buffer.len() - 2])?;
    let length = length_str.parse::<usize>()?;

    // Read the data
    let mut data = vec![0u8; length];
    if cursor.read_exact(&mut data).is_err() {
        return Ok(None);
    }

    // Skip the trailing \r\n
    let mut crlf = [0u8; 2];
    if cursor.read_exact(&mut crlf).is_err() {
        return Ok(None);
    }

    let value = String::from_utf8(data)?;
    Ok(Some(Box::new(BulkString::new(value))))
}

/// Parse a simple string from the cursor
/// Format: +<data>\r\n
fn parse_simple_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];
    let mut buffer = Vec::new();
    buffer.push(b'+');

    loop {
        if cursor.read_exact(&mut byte).is_err() {
            return Ok(None);
        }

        buffer.push(byte[0]);

        if buffer.len() >= 2 && &buffer[buffer.len() - 2..] == b"\r\n" {
            break;
        }
    }

    let string = String::from_utf8(buffer.to_vec())?;
    let (_, value) = string.split_at(1);
    let simple_string = SimpleString::new(value.trim_end_matches("\r\n").to_string());

    Ok(Some(Box::new(simple_string)))
}

/// Parse an integer from the cursor
/// Format: :<integer>\r\n
fn parse_integer(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];
    let mut buffer = Vec::new();

    // Read until \r\n to get the integer value
    loop {
        if cursor.read_exact(&mut byte).is_err() {
            return Ok(None);
        }

        buffer.push(byte[0]);

        if buffer.len() >= 2 && &buffer[buffer.len() - 2..] == b"\r\n" {
            break;
        }
    }

    // Parse the integer value
    let integer_str = std::str::from_utf8(&buffer[..buffer.len() - 2])?;
    let value = integer_str.parse::<i32>()?;

    Ok(Some(Box::new(Integer { value })))
}

/// Parse an error from the cursor
/// Format: -<error message>\r\n
fn parse_error(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];
    let mut buffer = Vec::new();

    // Read until \r\n to get the error message
    loop {
        if cursor.read_exact(&mut byte).is_err() {
            return Ok(None);
        }

        buffer.push(byte[0]);

        if buffer.len() >= 2 && &buffer[buffer.len() - 2..] == b"\r\n" {
            break;
        }
    }

    // Extract the error message (without the \r\n)
    let error_str = std::str::from_utf8(&buffer[..buffer.len() - 2])?;

    Ok(Some(Box::new(SimpleError {
        value: error_str.to_string(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandExecuteInput;
    use crate::config::Config;
    use crate::store::{DataType, Store};

    fn redis_array_of_bulk_strings(strs: Vec<&str>) -> Vec<u8> {
        let mut result = Vec::new();
        result.push(b'*');
        result.extend_from_slice(strs.len().to_string().as_bytes());
        result.extend_from_slice(b"\r\n");

        for s in strs {
            result.push(b'$');
            result.extend_from_slice(s.len().to_string().as_bytes());
            result.extend_from_slice(b"\r\n");
            result.extend_from_slice(s.as_bytes());
            result.extend_from_slice(b"\r\n");
        }

        result
    }

    // Helper function to execute a command from string arguments
    fn execute_command(args: Vec<&str>, store: &Store) -> Result<Vec<u8>> {
        let data = redis_array_of_bulk_strings(args);
        let mut cursor = Cursor::new(data.as_ref());
        let command = parse_command(&mut cursor)?
            .ok_or_else(|| anyhow::anyhow!("Failed to parse command"))?;
        let config = Config::default();
        let command_input = CommandExecuteInput::new(store, &config);
        command.execute(&command_input)
    }

    // Helper function to assert a list value in the store
    fn assert_list_value(store: &Store, key: &str, expected: Vec<&str>) {
        match store.get(key) {
            Some(DataType::List(list)) => {
                assert_eq!(list, expected, "List values don't match");
            }
            Some(DataType::String(_)) => {
                panic!("Expected list but got string for key '{}'", key);
            }
            None => {
                panic!("Expected list but key '{}' not found", key);
            }
        }
    }

    // Helper function to assert a string value in the store
    fn assert_string_value(store: &Store, key: &str, expected: &str) {
        match store.get(key) {
            Some(DataType::String(s)) => {
                assert_eq!(s, expected, "String values don't match");
            }
            Some(DataType::List(_)) => {
                panic!("Expected string but got list for key '{}'", key);
            }
            None => {
                panic!("Expected string but key '{}' not found", key);
            }
        }
    }

    #[test]
    fn test_rpush_single_value() -> Result<()> {
        let store = Store::new();
        let response = execute_command(vec!["rpush", "mykey", "value"], &store)?;
        assert_eq!(response, b":1\r\n");
        assert_list_value(&store, "mykey", vec!["value"]);
        Ok(())
    }

    #[test]
    fn test_rpush_multiple_values() -> Result<()> {
        let store = Store::new();
        execute_command(vec!["rpush", "mykey", "value"], &store)?;
        let response = execute_command(vec!["rpush", "mykey", "one", "two"], &store)?;
        assert_eq!(response, b":3\r\n");
        assert_list_value(&store, "mykey", vec!["value", "one", "two"]);
        Ok(())
    }

    #[test]
    fn test_rpush_multiple_values_at_once() -> Result<()> {
        let store = Store::new();
        let response = execute_command(vec!["rpush", "mylist", "a", "b", "c"], &store)?;
        assert_eq!(response, b":3\r\n");
        assert_list_value(&store, "mylist", vec!["a", "b", "c"]);
        Ok(())
    }

    #[test]
    fn test_rpop_from_list() -> Result<()> {
        let store = Store::new();
        execute_command(vec!["rpush", "mykey", "one", "two", "three"], &store)?;

        let response = execute_command(vec!["rpop", "mykey"], &store)?;
        assert_eq!(response, b"$5\r\nthree\r\n");
        assert_list_value(&store, "mykey", vec!["one", "two"]);
        Ok(())
    }

    #[test]
    fn test_rpop_from_empty_list() -> Result<()> {
        let store = Store::new();
        execute_command(vec!["rpush", "mykey", "only"], &store)?;
        execute_command(vec!["rpop", "mykey"], &store)?;

        // Pop from now-empty list
        let response = execute_command(vec!["rpop", "mykey"], &store)?;
        assert_eq!(response, b"$-1\r\n"); // Null bulk string
        Ok(())
    }

    #[test]
    fn test_rpop_nonexistent_key() -> Result<()> {
        let store = Store::new();
        let response = execute_command(vec!["rpop", "nonexistent"], &store)?;
        assert_eq!(response, b"$-1\r\n"); // Null bulk string
        Ok(())
    }

    #[test]
    fn test_rpush_missing_value() -> Result<()> {
        let data = redis_array_of_bulk_strings(vec!["rpush", "mykey"]);
        let mut cursor = Cursor::new(data.as_ref());
        let result = parse_command(&mut cursor);

        // Should fail to parse because RPUSH requires at least one value
        assert!(result.is_err() || result.unwrap().is_none());
        Ok(())
    }

    #[test]
    fn test_rpop_missing_key() -> Result<()> {
        let data = redis_array_of_bulk_strings(vec!["rpop"]);
        let mut cursor = Cursor::new(data.as_ref());
        let command = parse_command(&mut cursor)?;

        // Should not parse with missing key
        assert!(command.is_none());
        Ok(())
    }

    #[test]
    fn test_list_string_type_separation() -> Result<()> {
        let store = Store::new();

        // Set a string value
        execute_command(vec!["set", "mykey", "stringvalue"], &store)?;
        assert_string_value(&store, "mykey", "stringvalue");

        // RPUSH should replace the string with a list
        execute_command(vec!["rpush", "mykey", "listvalue"], &store)?;
        assert_list_value(&store, "mykey", vec!["listvalue"]);

        Ok(())
    }

    #[test]
    fn test_get_on_list_returns_none() -> Result<()> {
        let store = Store::new();
        execute_command(vec!["rpush", "listkey", "value"], &store)?;

        // GET should return None for a list key
        let response = execute_command(vec!["get", "listkey"], &store)?;
        assert_eq!(response, b"$-1\r\n"); // Null bulk string
        Ok(())
    }

    #[test]
    fn test_multiple_rpop_operations() -> Result<()> {
        let store = Store::new();
        execute_command(vec!["rpush", "stack", "first", "second", "third"], &store)?;

        let response1 = execute_command(vec!["rpop", "stack"], &store)?;
        assert_eq!(response1, b"$5\r\nthird\r\n");

        let response2 = execute_command(vec!["rpop", "stack"], &store)?;
        assert_eq!(response2, b"$6\r\nsecond\r\n");

        let response3 = execute_command(vec!["rpop", "stack"], &store)?;
        assert_eq!(response3, b"$5\r\nfirst\r\n");

        let response4 = execute_command(vec!["rpop", "stack"], &store)?;
        assert_eq!(response4, b"$-1\r\n"); // Empty now

        Ok(())
    }

    #[test]
    fn test_parse_array_with_bulk_string_ping_lowercase() -> Result<()> {
        // Test parsing a lowercase PING command sent as an Array with BulkString
        let data = redis_array_of_bulk_strings(vec!["ping"]);
        let mut cursor = Cursor::new(data.as_ref());

        // Parse as a command
        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_some(),
            "Expected to parse ping command from array"
        );

        // Verify the command returns the expected PONG response
        let store = Store::new();
        let config = Config::default();
        let command_input = CommandExecuteInput::new(&store, &config);
        let response = command.unwrap().execute(&command_input)?;
        assert_eq!(response, b"+PONG\r\n");

        // Also test the data type parser directly
        let mut cursor = Cursor::new(data.as_ref());
        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        // Assert it's an Array with one BulkString of value "ping"
        let data_type = data_type.unwrap();
        let array = data_type
            .as_any()
            .downcast_ref::<Array>()
            .expect("Expected Array type");
        assert_eq!(array.values.len(), 1, "Expected array with 1 element");

        let bulk_string = array.values[0]
            .as_any()
            .downcast_ref::<BulkString>()
            .expect("Expected BulkString in array");
        assert_eq!(
            bulk_string.value, "ping",
            "Expected BulkString value to be 'ping'"
        );

        Ok(())
    }

    #[test]
    fn test_parse_empty_input() -> Result<()> {
        // Test parsing empty input returns None
        let data = b"";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(command.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_simple_string_ping_not_accepted() -> Result<()> {
        // Test that SimpleString PING is NOT accepted as a command
        // Commands must be sent as arrays with bulk strings
        let data = b"+PING\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_none(),
            "SimpleString PING should not be accepted as a command"
        );

        Ok(())
    }

    #[test]
    fn test_parse_bulk_string_ping_not_accepted() -> Result<()> {
        // Test that BulkString PING alone is NOT accepted as a command
        // Commands must be sent as arrays
        let data = b"$4\r\nPING\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_none(),
            "BulkString PING alone should not be accepted as a command"
        );

        Ok(())
    }

    #[test]
    fn test_echo_command() -> Result<()> {
        // Test parsing an ECHO command sent as an Array with BulkString
        let data = redis_array_of_bulk_strings(vec!["ECHO", "hey"]);
        let mut cursor = Cursor::new(data.as_ref());

        // Parse as a command
        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_some(),
            "Expected to parse echo command from array"
        );

        // Verify the command returns the expected PONG response
        let store = Store::new();
        let config = Config::default();
        let command_input = CommandExecuteInput::new(&store, &config);
        let response = command.unwrap().execute(&command_input)?;
        assert_eq!(response, b"$3\r\nhey\r\n");

        // Also test the data type parser directly
        let mut cursor = Cursor::new(data.as_ref());
        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        // Assert it's an Array with two BulkStrings: "ECHO" and "hey"
        let data_type = data_type.unwrap();
        let array = data_type
            .as_any()
            .downcast_ref::<Array>()
            .expect("Expected Array type");
        assert_eq!(array.values.len(), 2, "Expected array with 2 elements");

        let echo_command = array.values[0]
            .as_any()
            .downcast_ref::<BulkString>()
            .expect("Expected BulkString for command");
        assert_eq!(
            echo_command.value, "ECHO",
            "Expected first BulkString value to be 'ECHO'"
        );

        let echo_arg = array.values[1]
            .as_any()
            .downcast_ref::<BulkString>()
            .expect("Expected BulkString for argument");
        assert_eq!(
            echo_arg.value, "hey",
            "Expected second BulkString value to be 'hey'"
        );

        Ok(())
    }

    #[test]
    fn test_byte_to_ascii_conversion() {
        // Test converting various bytes to ASCII characters
        assert_eq!(byte_to_ascii(43), '+');
        assert_eq!(byte_to_ascii(42), '*');
        assert_eq!(byte_to_ascii(36), '$');
        assert_eq!(byte_to_ascii(58), ':');
        assert_eq!(byte_to_ascii(45), '-');

        // Letters
        assert_eq!(byte_to_ascii(65), 'A');
        assert_eq!(byte_to_ascii(97), 'a');

        // Numbers
        assert_eq!(byte_to_ascii(48), '0');
        assert_eq!(byte_to_ascii(57), '9');

        // Special characters
        assert_eq!(byte_to_ascii(13), '\r');
        assert_eq!(byte_to_ascii(10), '\n');

        // Using byte literals
        assert_eq!(b'+', 43u8);
        assert_eq!(b'*', 42u8);
        assert_eq!(b'$', 36u8);
        assert_eq!(b':', 58u8);
        assert_eq!(b'-', 45u8);
    }

    #[test]
    fn test_parse_integer() -> Result<()> {
        // Test parsing a positive integer
        let data = b":42\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        let data_type = data_type.unwrap();
        let integer = data_type
            .as_any()
            .downcast_ref::<Integer>()
            .expect("Expected Integer type");
        assert_eq!(integer.value, 42);

        // Test parsing a negative integer
        let data = b":-123\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        let data_type = data_type.unwrap();
        let integer = data_type
            .as_any()
            .downcast_ref::<Integer>()
            .expect("Expected Integer type");
        assert_eq!(integer.value, -123);

        // Test parsing zero
        let data = b":0\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        let data_type = data_type.unwrap();
        let integer = data_type
            .as_any()
            .downcast_ref::<Integer>()
            .expect("Expected Integer type");
        assert_eq!(integer.value, 0);

        Ok(())
    }

    #[test]
    fn test_parse_error() -> Result<()> {
        // Test parsing a simple error
        let data = b"-Error message\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        let data_type = data_type.unwrap();
        let error = data_type
            .as_any()
            .downcast_ref::<SimpleError>()
            .expect("Expected SimpleError type");
        assert_eq!(error.value, "Error message");

        // Test parsing an error with special characters
        let data = b"-ERR unknown command 'foobar'\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        let data_type = data_type.unwrap();
        let error = data_type
            .as_any()
            .downcast_ref::<SimpleError>()
            .expect("Expected SimpleError type");
        assert_eq!(error.value, "ERR unknown command 'foobar'");

        Ok(())
    }

    #[test]
    fn test_parse_array_with_mixed_types() -> Result<()> {
        // Test parsing an array with different data types
        // Array with: BulkString, Integer, SimpleString
        let data = b"*3\r\n$5\r\nhello\r\n:42\r\n+OK\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        let data_type = data_type.unwrap();
        let array = data_type
            .as_any()
            .downcast_ref::<Array>()
            .expect("Expected Array type");
        assert_eq!(array.values.len(), 3);

        // Check first element is BulkString
        let bulk_string = array.values[0]
            .as_any()
            .downcast_ref::<BulkString>()
            .expect("Expected BulkString at index 0");
        assert_eq!(bulk_string.value, "hello");

        // Check second element is Integer
        let integer = array.values[1]
            .as_any()
            .downcast_ref::<Integer>()
            .expect("Expected Integer at index 1");
        assert_eq!(integer.value, 42);

        // Check third element is SimpleString
        let simple_string = array.values[2]
            .as_any()
            .downcast_ref::<SimpleString>()
            .expect("Expected SimpleString at index 2");
        assert_eq!(simple_string.value, "OK");

        Ok(())
    }
}
