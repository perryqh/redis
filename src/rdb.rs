use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::store::{DataType, StoreValue};
use anyhow::Result;

pub fn parse_rdb_file(bytes: Vec<u8>) -> Result<Rdb> {
    let version = String::from_utf8(bytes[5..9].to_vec())?;
    // skip bytes to b"FE00"
    Ok(Rdb { version })
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Rdb {
    version: String,
}

impl Rdb {
    pub fn to_store_values(&self) -> Arc<RwLock<HashMap<String, StoreValue<DataType>>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rdb_file() -> Result<()> {
        let bytes = vec![0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31];
        let rdb = parse_rdb_file(bytes)?;
        assert_eq!(rdb.version, "0011");

        Ok(())
    }

    fn fixture_bytes() -> Result<Vec<u8>> {
        let file_path = "tests/fixtures/dump.rdb";
        let bytes = std::fs::read(file_path)?;
        Ok(bytes)
    }

    #[test]
    fn test_fixture_bytes() -> Result<()> {
        let bytes = fixture_bytes()?;
        assert!(!bytes.is_empty());
        let rdb = parse_rdb_file(bytes)?;
        assert_eq!(rdb.version, "0012");

        Ok(())
    }
}
