use std::io::{Cursor, Read};

use anyhow::Result;

use crate::commands::{PingCommand, RedisCommand};
use crate::datatypes::{Array, BulkString, RedisDataType, SimpleString};

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
/// Parse a Redis data type from the cursor
pub fn parse_data_type(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisDataType>>> {
    let mut byte = [0u8; 1];

    // Try to read the first byte
    if cursor.read_exact(&mut byte).is_err() {
        return Ok(None);
    }

    match byte[0] {
        b'*' => {
            // Parse array
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
        b'$' => {
            // Parse bulk string
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
        b'+' => {
            // Parse simple string
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
        _ => Ok(None),
    }
}

pub fn parse_command(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisCommand>>> {
    // Parse the data type
    if let Some(data_type) = parse_data_type(cursor)? {
        // Check if it's an Array with a PING command
        if let Some(array) = data_type.as_any().downcast_ref::<Array>() {
            if array.values.len() == 1 {
                if let Some(bulk_string) = array.values[0].as_any().downcast_ref::<BulkString>() {
                    if bulk_string.value.to_uppercase() == "PING" {
                        return Ok(Some(Box::new(PingCommand {})));
                    }
                }
            }
        }
        // Check if it's a SimpleString PING command
        else if let Some(simple_string) = data_type.as_any().downcast_ref::<SimpleString>() {
            if simple_string.value.to_uppercase() == "PING" {
                return Ok(Some(Box::new(PingCommand {})));
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string_ping() -> Result<()> {
        // Test parsing a PING command sent as a SimpleString
        let data = b"+PING\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(command.is_some());

        // Verify the command returns the expected PONG response
        let response = command.unwrap().response()?;
        assert_eq!(response, b"+PONG\r\n");

        Ok(())
    }

    #[test]
    fn test_parse_simple_string_ping_lowercase() -> Result<()> {
        // Test parsing a lowercase ping command
        let data = b"+ping\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(command.is_some());

        // Verify the command returns the expected PONG response
        let response = command.unwrap().response()?;
        assert_eq!(response, b"+PONG\r\n");

        Ok(())
    }

    #[test]
    fn test_parse_array_with_bulk_string_ping() -> Result<()> {
        // Test parsing a PING command sent as an Array with BulkString
        let data = b"*1\r\n$4\r\nPING\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        // Parse as a command
        let command = parse_command(&mut cursor)?;
        assert!(
            command.is_some(),
            "Expected to parse PING command from array"
        );

        // Verify the command returns the expected PONG response
        let response = command.unwrap().response()?;
        assert_eq!(response, b"+PONG\r\n");

        // Also test the data type parser directly
        let mut cursor = Cursor::new(data.as_ref());
        let data_type = parse_data_type(&mut cursor)?;
        assert!(data_type.is_some());

        // Assert it's an Array with one BulkString of value "PING"
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
            bulk_string.value, "PING",
            "Expected BulkString value to be 'PING'"
        );

        Ok(())
    }

    #[test]
    fn test_parse_simple_string_unknown_command() -> Result<()> {
        // Test parsing an unknown command returns None
        let data = b"+HELLO\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(command.is_none());

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
        let response = command.unwrap().response()?;
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
    fn test_parse_incomplete_simple_string() -> Result<()> {
        // Test parsing incomplete SimpleString (missing \r\n) returns None
        let data = b"+PING";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(command.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_unknown_type() -> Result<()> {
        // Test parsing unknown Redis type prefix returns None
        let data = b"!SOMETHING\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        let command = parse_command(&mut cursor)?;
        assert!(command.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_multiple_commands_in_buffer() -> Result<()> {
        // Test parsing multiple PING commands from a single buffer
        let data = b"+PING\r\n+PING\r\n+PING\r\n";
        let mut cursor = Cursor::new(data.as_ref());

        // Parse first command
        let command1 = parse_command(&mut cursor)?;
        assert!(command1.is_some());
        assert_eq!(command1.unwrap().response()?, b"+PONG\r\n");

        // Parse second command
        let command2 = parse_command(&mut cursor)?;
        assert!(command2.is_some());
        assert_eq!(command2.unwrap().response()?, b"+PONG\r\n");

        // Parse third command
        let command3 = parse_command(&mut cursor)?;
        assert!(command3.is_some());
        assert_eq!(command3.unwrap().response()?, b"+PONG\r\n");

        // No more commands
        let command4 = parse_command(&mut cursor)?;
        assert!(command4.is_none());

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
}
