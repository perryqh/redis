use anyhow::Result;

pub trait RedisDataType {
    fn to_bytes(&self) -> Result<Vec<u8>>;
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

// +
#[derive(Debug, PartialEq)]
pub struct SimpleString {
    pub value: String,
}

// :
#[derive(Debug, PartialEq)]
pub struct Integer {
    pub value: i32,
}

// $
pub struct BulkStrings {
    pub values: Vec<String>,
}

impl SimpleString {
    pub fn new(value: String) -> Self {
        SimpleString { value }
    }
}

impl RedisDataType for SimpleString {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(format!("+{}\r\n", self.value).into_bytes())
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from(bytes)
    }
}

impl TryFrom<&[u8]> for SimpleString {
    type Error = anyhow::Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        let string = String::from_utf8(bytes.to_vec())?;
        let (_, value) = string.split_at(1);
        Ok(SimpleString {
            value: value.trim_end_matches("\r\n").to_string(),
        })
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

    #[test]
    fn test_simple_string_from_bytes() -> Result<()> {
        let bytes = "+Hello, World!\r\n".as_bytes();
        let simple_string = SimpleString::from_bytes(bytes)?;
        assert_eq!(simple_string.value, "Hello, World!");
        Ok(())
    }
}
