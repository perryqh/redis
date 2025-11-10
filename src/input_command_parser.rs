use std::io::{Cursor, Read};

use anyhow::Result;

use crate::commands::{EchoCommand, GetCommand, PingCommand, RedisCommand, SetCommand};
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
            dbg!(&array.values);
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
    use crate::store::Store;

    #[test]
    fn test_parse_array_with_bulk_string_ping_lowercase() -> Result<()> {
        // Test parsing a lowercase PING command sent as an Array with BulkString
        let data = b"*1\r\n$4\r\nping\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        // Parse as a command
        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_some(),
            "Expected to parse ping command from array"
        );

        // Verify the command returns the expected PONG response
        let store = Store::new();
        let response = command.unwrap().execute(&store)?;
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
        let data = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        // Parse as a command
        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_some(),
            "Expected to parse echo command from array"
        );

        // Verify the command returns the expected PONG response
        let store = Store::new();
        let response = command.unwrap().execute(&store)?;
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
