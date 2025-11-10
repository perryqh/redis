use anyhow::Result;
use std::any::Any;

pub trait RedisDataType: Any {
    fn to_bytes(&self) -> Result<Vec<u8>>;
    fn as_any(&self) -> &dyn Any;
}

// +OK\r\n
#[derive(Debug, PartialEq)]
pub struct SimpleString {
    pub value: String,
}

impl SimpleString {
    pub fn new(value: String) -> Self {
        SimpleString { value }
    }
}

// -Error message\r\n
#[derive(Debug, PartialEq)]
pub struct SimpleError {
    pub value: String,
}

impl RedisDataType for SimpleError {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(format!("-{}\r\n", self.value).into_bytes())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// *<number-of-elements>\r\n<element-1>...<element-n>
// *0\r\n   empty array
// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
pub struct Array {
    pub values: Vec<Box<dyn RedisDataType>>,
}

// *-1\r\n
pub struct NullArray {}

impl RedisDataType for NullArray {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(b"*-1\r\n".to_vec())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// :[<+|->]<value>\r\n
// unsigned base 10
#[derive(Debug, PartialEq)]
pub struct Integer {
    pub value: i32,
}

impl RedisDataType for Integer {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(format!(":{}\r\n", self.value).into_bytes())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// $<length>\r\n<data>\r\n
#[derive(Debug, PartialEq)]
pub struct BulkString {
    pub value: String,
}

impl BulkString {
    pub fn new(value: String) -> Self {
        BulkString { value }
    }
}

impl RedisDataType for BulkString {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(format!("${}\r\n{}\r\n", self.value.len(), self.value).into_bytes())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// $-1\r\n
pub struct NullBulkString {
}

impl RedisDataType for NullBulkString {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(b"$-1\r\n".to_vec())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RedisDataType for SimpleString {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(format!("+{}\r\n", self.value).into_bytes())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RedisDataType for Array {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        // it might not make sense to implement to_bytes on datatypes because
        // business logic might be different for each datatype
        // Example: when should the response also be an array?
        let mut bytes = vec![];
        for value in &self.values {
            bytes.extend(value.to_bytes()?);
        }
        Ok(bytes)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string_to_bytes() -> Result<()> {
        let simple_string = SimpleString {
            value: "Hello, World!".to_string(),
        };
        let bytes = simple_string.to_bytes()?;
        assert_eq!(bytes, "+Hello, World!\r\n".as_bytes());
        Ok(())
    }
}
