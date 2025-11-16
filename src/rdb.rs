use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::store::{DataType, StoreValue};
use anyhow::{Context, Result};
use bytes::Bytes;

type ByteIndex = usize;
// https://rdb.fnordig.de/file_format.html#length-encoding
pub fn parse_rdb_file(bytes: Vec<u8>) -> Result<Rdb> {
    let bytes = Bytes::from(bytes);
    let version = String::from_utf8(bytes[5..9].to_vec()).context("Failed to parse version")?;
    let mut current_index = find_data_begin_index(&bytes)?;

    let mut data: HashMap<String, StoreValue<DataType>> = HashMap::new();
    while let Some((key, value, index)) = parse_key_value(&bytes, current_index)? {
        current_index = index;
        data.insert(key, value);
    }

    Ok(Rdb { version, data })
}

fn parse_key_value(
    bytes: &Bytes,
    current_index: ByteIndex,
) -> Result<Option<(String, StoreValue<DataType>, ByteIndex)>> {
    let mut current_index = current_index;
    let flag = bytes[current_index];
    current_index += 1;
    if flag == 0xFF {
        return Ok(None);
    }
    let expires_at: Option<SystemTime> = match flag {
        0xFC => {
            let length_bytes = &bytes[current_index..current_index + 8].try_into()?;
            let expiration_timestamp_in_milliseconds = u64::from_le_bytes(*length_bytes);
            let duration = Duration::from_millis(expiration_timestamp_in_milliseconds);

            current_index += 8;
            if bytes[current_index] == 0x00 {
                current_index += 1;
            } else {
                return Ok(None);
            }
            Some(system_time_from_duration_since_unix_epoch(duration))
        }
        0xFD => {
            let length_bytes = &bytes[current_index..current_index + 4].try_into()?;
            let expiration_timestamp_in_seconds = u32::from_le_bytes(*length_bytes);
            let duration = Duration::from_secs(expiration_timestamp_in_seconds as u64);

            current_index += 4;
            if bytes[current_index] == 0x00 {
                current_index += 1;
            } else {
                return Ok(None);
            }
            Some(system_time_from_duration_since_unix_epoch(duration))
        }
        _ => None,
    };

    let (string_length, mut current_index) = length_encoded_int(bytes, current_index)?;
    let key = String::from_utf8(bytes[current_index..current_index + string_length].to_vec())
        .context("Failed to parse key")?;
    current_index += string_length;
    let (string_length, mut current_index) = length_encoded_int(bytes, current_index)?;
    let value = String::from_utf8(bytes[current_index..current_index + string_length].to_vec())
        .context("Failed to parse value")?;
    let value = StoreValue::new(DataType::String(value), expires_at);
    current_index += string_length;
    Ok(Some((key, value, current_index)))
}

fn system_time_from_duration_since_unix_epoch(duration: Duration) -> SystemTime {
    UNIX_EPOCH + duration
}

// Skipping over the FA and FE sections
// Return index at the start of the "data" sections
fn find_data_begin_index(bytes: &Bytes) -> Result<ByteIndex> {
    let fe_index =
        index_of(bytes, &[0xFE, 0x00, 0xFB]).context("Failed to find FE 00 FB marker")?;
    let current_index = fe_index + 3;
    // skip over the subsequent 2 length-encoded-int
    let (_, current_index) =
        length_encoded_int(bytes, current_index).context("Size of the corresponding hash table")?;
    let (_, current_index) = length_encoded_int(bytes, current_index)
        .context("Size of the corresponding expire hash table")?;

    Ok(current_index)
}

// https://rdb.fnordig.de/file_format.html#length-encoding
// Examine first two bits of the byte at current_index
// `00` - The next 6 bits represent the length
// `01` - Read one additional byte. The combined 14 bits represent the length
// `10` - Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
// Return (length, the index after the length bytes)
fn length_encoded_int(bytes: &Bytes, current_index: ByteIndex) -> Result<(usize, ByteIndex)> {
    let byte = bytes[current_index];
    let length = match (byte >> 6) & 0b11 {
        0 => {
            let length = byte & 0x3F;
            (length as usize, current_index + 1)
        }
        1 => {
            let length = ((byte & 0x3F) as usize) << 8 | bytes[current_index + 1] as usize;
            (length, current_index + 2)
        }
        2 => {
            let length_bytes = &bytes[current_index + 1..current_index + 5].try_into()?;
            let length = u32::from_be_bytes(*length_bytes);
            (length as usize, current_index + 5)
        }
        _ => return Err(anyhow::anyhow!("Invalid length encoding")),
    };
    Ok(length)
}

fn index_of(bytes: &Bytes, pattern: &[u8]) -> Option<usize> {
    bytes
        .windows(pattern.len())
        .position(|window| window == pattern)
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Rdb {
    version: String,
    data: HashMap<String, StoreValue<DataType>>,
}

impl Rdb {
    pub fn to_store_values(&self) -> Arc<RwLock<HashMap<String, StoreValue<DataType>>>> {
        Arc::new(RwLock::new(self.data.clone()))
    }
}

pub const EMPTY_RDB: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn fixture_bytes() -> Result<Vec<u8>> {
        let file_path = "tests/fixtures/dump.rdb";
        let bytes = std::fs::read(file_path)?;
        Ok(bytes)
    }

    #[test]
    fn test_index_of() {
        let bytes = Bytes::from(vec![0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31]);
        let pattern = b"FE00";
        assert_eq!(index_of(&bytes, pattern), None);

        let bytes = Bytes::from(vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0x46, 0x45, 0x30, 0x30,
        ]);
        let pattern = b"FE00";
        assert_eq!(index_of(&bytes, pattern), Some(9));

        let remainder = &bytes[9..];
        assert_eq!(remainder, &[0x46, 0x45, 0x30, 0x30]);
    }

    #[test]
    fn test_with_bytes() -> Result<()> {
        let bytes: Vec<u8> = vec![
            82, 69, 68, 73, 83, 48, 48, 49, 50, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 56, 46, 50, 46, 51, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 250, 5, 99, 116, 105, 109, 101, 194, 148, 102, 18, 105, 250, 8, 117, 115, 101, 100,
            45, 109, 101, 109, 194, 80, 90, 13, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192,
            0, 254, 0, 251, 2, 0, 0, 3, 116, 111, 111, 3, 116, 97, 114, 0, 3, 102, 111, 111, 3, 98,
            97, 114, 255, 214, 124, 113, 82, 225, 160, 123, 7,
        ];
        let hex_string: &String = &bytes
            .iter()
            .skip(79)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");
        dbg!(&hex_string);
        // 84
        // let bytes = Bytes::from(bytes);
        // let fe_index = index_of(&bytes, &[0xFE, 0x00, 0xFB]).context("Failed to find FB marker")?;
        // assert_eq!(fe_index, 79);
        // 00 03 74 6f 6f 03 74 61 72 00 03 66 6f 6f 03 62 61 72 ff d6 7c 71 52 e1 a0 7b 07
        Ok(())
    }

    #[test]
    fn test_key_value_no_expiration() -> Result<()> {
        let bytes = Bytes::from(vec![0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78]);
        let (key, store_value, index) = parse_key_value(&bytes, 0)?.unwrap();
        assert_eq!(key, "baz");
        assert_eq!(&store_value.data, &DataType::String("qux".to_string()));
        assert_eq!(&store_value.expires_at, &None);
        assert_eq!(index, 9);

        Ok(())
    }

    #[test]
    fn test_key_value_milliseconds_expiration() -> Result<()> {
        let bytes = Bytes::from(vec![
            0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x03, 0x66, 0x6F, 0x6F,
            0x03, 0x62, 0x61, 0x72,
        ]);
        let (key, store_value, index) = parse_key_value(&bytes, 0)?.unwrap();
        assert_eq!(key, "foo");
        assert_eq!(&store_value.data, &DataType::String("bar".to_string()));
        assert_eq!(
            &store_value.expires_at,
            &Some(UNIX_EPOCH + Duration::from_secs(1713824559) + Duration::from_nanos(637000000))
        );
        assert_eq!(index, 18);

        Ok(())
    }

    #[test]
    fn test_key_value_seconds_expiration() -> Result<()> {
        let bytes = Bytes::from(vec![
            0xFD, 0x52, 0xED, 0x2A, 0x66, 0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78,
        ]);
        let (key, store_value, index) = parse_key_value(&bytes, 0)?.unwrap();
        assert_eq!(key, "baz");
        assert_eq!(&store_value.data, &DataType::String("qux".to_string()));
        assert_eq!(
            &store_value.expires_at,
            &Some(UNIX_EPOCH + Duration::from_secs(1714089298))
        );
        assert_eq!(index, 14);

        Ok(())
    }

    #[test]
    fn test_fixture_bytes() -> Result<()> {
        let bytes = fixture_bytes()?;
        assert!(!bytes.is_empty());
        let rdb = parse_rdb_file(bytes)?;
        assert_eq!(rdb.version, "0012");
        assert_eq!(rdb.data.len(), 2);

        dbg!(&rdb.data);
        Ok(())
    }

    #[test]
    fn test_length_encoded_int_simple() -> Result<()> {
        // first two bits are 0b00
        // The next 6 bits represent the length
        let bytes = Bytes::from(vec![0x0A, 0x00]);
        let current_index = 0;
        let (length, new_current_index) = length_encoded_int(&bytes, current_index)?;
        assert_eq!(length, 10);
        assert_eq!(new_current_index, 1);

        let bytes = Bytes::from(vec![0x00, 0x0A, 0x00]);
        let current_index = 1;
        let (length, new_current_index) = length_encoded_int(&bytes, current_index)?;
        assert_eq!(length, 10);
        assert_eq!(new_current_index, 2);
        Ok(())
    }

    #[test]
    fn test_length_encoded_int_additional_byte() -> Result<()> {
        // first two bits are 0b01
        // Read one additional byte. The combined 14 bits represent the length
        let bytes = Bytes::from(vec![0x00, 0x0A, 0x00, 0x42, 0xBC, 0x00]);
        let current_index = 3;
        let (length, new_current_index) = length_encoded_int(&bytes, current_index)?;
        assert_eq!(length, 700);
        assert_eq!(new_current_index, 5);
        Ok(())
    }

    #[test]
    fn test_length_encoded_int_additional_four_bytes() -> Result<()> {
        // first two bits are 0b10
        // Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
        let bytes = Bytes::from(vec![0x80, 0x00, 0x00, 0x42, 0x68, 0x00]);
        let current_index = 0;
        let (length, new_current_index) = length_encoded_int(&bytes, current_index)?;
        assert_eq!(length, 17000);
        assert_eq!(new_current_index, 5);

        Ok(())
    }

    #[test]
    fn bit_tests() -> Result<()> {
        let byte = 0x0A;
        let result = (byte >> 6) & 0b11;
        assert_eq!(result, 0);

        let byte = 0x42;
        let result = (byte >> 6) & 0b11;
        assert_eq!(result, 1);

        let byte = 0x80;
        let result = (byte >> 6) & 0b11;
        assert_eq!(result, 2);

        Ok(())
    }

    #[test]
    fn test_instant_from_duration_since_unix_epoch() {
        let duration = Duration::from_secs(0);
        let result = system_time_from_duration_since_unix_epoch(duration);
        assert_eq!(result, UNIX_EPOCH);

        let duration = Duration::from_secs(1763056572);
        let result = system_time_from_duration_since_unix_epoch(duration);
        assert_eq!(result, UNIX_EPOCH + duration);
    }
}
