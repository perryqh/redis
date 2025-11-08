use std::io::{Cursor, Read};

use anyhow::Result;

use crate::commands::{PingCommand, RedisCommand};
use crate::datatypes::{RedisDataType, SimpleString};

pub fn parse_command(cursor: &mut Cursor<&[u8]>) -> Result<Option<Box<dyn RedisCommand>>> {
    // if next character is '+', grab the u8s through \r\n and create a SimpleString
    let mut byte = [0u8; 1];

    // Try to read the first byte
    if cursor.read_exact(&mut byte).is_err() {
        return Ok(None);
    }

    match byte[0] {
        b'+' => {
            // Read until we find \r\n
            let mut buffer = Vec::new();
            buffer.push(b'+'); // Include the '+' prefix

            loop {
                if cursor.read_exact(&mut byte).is_err() {
                    return Ok(None);
                }

                buffer.push(byte[0]);

                // Check if we've found \r\n
                if buffer.len() >= 2 && &buffer[buffer.len() - 2..] == b"\r\n" {
                    break;
                }
            }

            // Parse the SimpleString from the collected bytes
            let simple_string = SimpleString::from_bytes(&buffer)?;

            // Check if this is a PING command
            if simple_string.value.to_uppercase() == "PING" {
                Ok(Some(Box::new(PingCommand {})))
            } else {
                // Other commands will be handled here in the future
                Ok(None)
            }
        }
        _ => {
            // Other Redis data types will be handled here
            Ok(None)
        }
    }
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
}
