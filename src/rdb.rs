use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::store::{DataType, StoreValue};

pub fn parse_rdb_file(_config: &Config) -> Arc<RwLock<HashMap<String, StoreValue<DataType>>>> {
    Arc::new(RwLock::new(HashMap::new()))
}
