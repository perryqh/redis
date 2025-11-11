use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// The type of data that can be stored in Redis
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    String(String),
    List(Vec<String>),
}

/// A value stored in the key-value store with optional expiration
#[derive(Clone, Debug)]
struct StoreValue<V> {
    data: V,
    expires_at: Option<Instant>,
}

impl<V> StoreValue<V> {
    /// Creates a new value without expiration
    fn new(data: V) -> Self {
        Self {
            data,
            expires_at: None,
        }
    }

    /// Creates a new value with expiration
    fn new_with_expiration(data: V, ttl: Duration) -> Self {
        Self {
            data,
            expires_at: Some(Instant::now() + ttl),
        }
    }

    /// Checks if the value has expired
    fn is_expired(&self) -> bool {
        self.expires_at
            .map(|expires_at| Instant::now() >= expires_at)
            .unwrap_or(false)
    }
}

/// A thread-safe key-value store with expiration support
///
/// This store uses RwLock to allow multiple concurrent reads while ensuring
/// exclusive access for writes. Values can optionally expire after a specified duration.
#[derive(Clone)]
pub struct Store<V = DataType> {
    inner: Arc<RwLock<HashMap<String, StoreValue<V>>>>,
}

impl<V: Clone> Store<V> {
    /// Creates a new empty store
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Sets a key-value pair without expiration
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// store.set("key1".to_string(), "value1".to_string());
    /// assert_eq!(store.get("key1"), Some("value1".to_string()));
    /// ```
    pub fn set(&self, key: String, value: V) {
        let mut map = self.inner.write().unwrap();
        map.insert(key, StoreValue::new(value));
    }

    /// Sets a key-value pair with expiration
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    /// * `ttl` - Time-to-live duration after which the value expires
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    /// use std::time::Duration;
    /// use std::thread;
    ///
    /// let store = Store::new();
    /// store.set_with_expiration("key1".to_string(), "value1".to_string(), Duration::from_millis(100));
    /// assert_eq!(store.get("key1"), Some("value1".to_string()));
    /// thread::sleep(Duration::from_millis(150));
    /// assert_eq!(store.get("key1"), None);
    /// ```
    pub fn set_with_expiration(&self, key: String, value: V, ttl: Duration) {
        let mut map = self.inner.write().unwrap();
        map.insert(key, StoreValue::new_with_expiration(value, ttl));
    }

    /// Gets a value by key, returning None if the key doesn't exist or has expired
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    /// * `Some(V)` - The value if it exists and hasn't expired
    /// * `None` - If the key doesn't exist or has expired
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// store.set("key1".to_string(), "value1".to_string());
    /// assert_eq!(store.get("key1"), Some("value1".to_string()));
    /// assert_eq!(store.get("nonexistent"), None);
    /// ```
    pub fn get(&self, key: &str) -> Option<V> {
        let map = self.inner.read().unwrap();
        map.get(key).and_then(|value| {
            if value.is_expired() {
                None
            } else {
                Some(value.data.clone())
            }
        })
    }

    /// Deletes a key from the store
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `true` - If the key existed and was deleted
    /// * `false` - If the key didn't exist
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// store.set("key1".to_string(), "value1".to_string());
    /// assert_eq!(store.delete("key1"), true);
    /// assert_eq!(store.delete("key1"), false);
    /// ```
    pub fn delete(&self, key: &str) -> bool {
        let mut map = self.inner.write().unwrap();
        map.remove(key).is_some()
    }

    /// Checks if a key exists and hasn't expired
    ///
    /// # Arguments
    /// * `key` - The key to check
    ///
    /// # Returns
    /// * `true` - If the key exists and hasn't expired
    /// * `false` - If the key doesn't exist or has expired
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// store.set("key1".to_string(), "value1".to_string());
    /// assert_eq!(store.exists("key1"), true);
    /// assert_eq!(store.exists("nonexistent"), false);
    /// ```
    pub fn exists(&self, key: &str) -> bool {
        let map = self.inner.read().unwrap();
        map.get(key)
            .map(|value| !value.is_expired())
            .unwrap_or(false)
    }

    /// Removes all expired entries from the store
    ///
    /// This is useful for periodic cleanup to free memory from expired entries
    /// that haven't been accessed yet.
    ///
    /// # Returns
    /// The number of entries removed
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    /// use std::time::Duration;
    /// use std::thread;
    ///
    /// let store = Store::new();
    /// store.set_with_expiration("key1".to_string(), "value1".to_string(), Duration::from_millis(50));
    /// store.set_with_expiration("key2".to_string(), "value2".to_string(), Duration::from_millis(50));
    /// thread::sleep(Duration::from_millis(100));
    /// let removed = store.cleanup_expired();
    /// assert_eq!(removed, 2);
    /// ```
    pub fn cleanup_expired(&self) -> usize {
        let mut map = self.inner.write().unwrap();
        let initial_size = map.len();
        map.retain(|_, value| !value.is_expired());
        initial_size - map.len()
    }

    /// Returns the number of non-expired entries in the store
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// store.set("key1".to_string(), "value1".to_string());
    /// store.set("key2".to_string(), "value2".to_string());
    /// assert_eq!(store.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        let map = self.inner.read().unwrap();
        map.iter().filter(|(_, value)| !value.is_expired()).count()
    }

    /// Returns true if the store has no non-expired entries
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// assert!(store.is_empty());
    /// store.set("key1".to_string(), "value1".to_string());
    /// assert!(!store.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all entries from the store
    ///
    /// # Examples
    /// ```
    /// use codecrafters_redis::store::Store;
    ///
    /// let store = Store::new();
    /// store.set("key1".to_string(), "value1".to_string());
    /// store.set("key2".to_string(), "value2".to_string());
    /// store.clear();
    /// assert!(store.is_empty());
    /// ```
    pub fn clear(&self) {
        let mut map = self.inner.write().unwrap();
        map.clear();
    }
}

impl<V: Clone> Default for Store<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl Store<DataType> {
    /// Sets a string value without expiration
    pub fn set_string(&self, key: String, value: String) {
        self.set(key, self::DataType::String(value));
    }

    /// Sets a string value with expiration
    pub fn set_string_with_expiration(&self, key: String, value: String, ttl: Duration) {
        self.set_with_expiration(key, self::DataType::String(value), ttl);
    }

    /// Gets a string value by key, returns None if key doesn't exist or holds wrong type
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.get(key).and_then(|dt| match dt {
            self::DataType::String(s) => Some(s),
            _ => None, // Wrong type - key exists but holds a list
        })
    }

    /// Pushes a value to the right of a list
    ///
    /// # Arguments
    /// * `key` - The key of the list
    /// * `value` - The value to push
    ///
    /// # Returns
    /// The new length of the list
    pub fn rpush(&self, key: String, value: String) -> usize {
        let mut map = self.inner.write().unwrap();
        let entry = map
            .entry(key)
            .or_insert_with(|| StoreValue::new(self::DataType::List(Vec::new())));

        match &mut entry.data {
            self::DataType::List(list) => {
                list.push(value);
                list.len()
            }
            self::DataType::String(_) => {
                // Replace string with list - this matches Redis behavior
                // when a key holding a string gets an RPUSH operation
                entry.data = self::DataType::List(vec![value]);
                1
            }
        }
    }

    /// Pops a value from the right of a list
    ///
    /// # Arguments
    /// * `key` - The key of the list
    ///
    /// # Returns
    /// The popped value, or None if the list is empty, doesn't exist, or holds wrong type
    pub fn rpop(&self, key: &str) -> Option<String> {
        let mut map = self.inner.write().unwrap();
        map.get_mut(key).and_then(|value| {
            if value.is_expired() {
                None
            } else {
                match &mut value.data {
                    self::DataType::List(list) => list.pop(),
                    self::DataType::String(_) => None, // Wrong type
                }
            }
        })
    }

    /// Gets the length of a list
    ///
    /// # Arguments
    /// * `key` - The key of the list
    ///
    /// # Returns
    /// The length of the list, or 0 if it doesn't exist or holds wrong type
    pub fn llen(&self, key: &str) -> usize {
        let map = self.inner.read().unwrap();
        map.get(key).map_or(0, |value| {
            if value.is_expired() {
                0
            } else {
                match &value.data {
                    self::DataType::List(list) => list.len(),
                    self::DataType::String(_) => 0, // Wrong type
                }
            }
        })
    }

    /// Gets a range of elements from a list
    ///
    /// # Arguments
    /// * `key` - The key of the list
    /// * `start` - The start index (inclusive)
    /// * `stop` - The stop index (inclusive)
    ///
    /// # Returns
    /// A vector of elements in the specified range
    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> Vec<String> {
        let map = self.inner.read().unwrap();
        map.get(key).map_or(Vec::new(), |value| {
            if value.is_expired() {
                Vec::new()
            } else {
                match &value.data {
                    self::DataType::List(list) => {
                        let len = list.len() as isize;
                        let start = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start.min(len)
                        } as usize;
                        let stop = if stop < 0 {
                            (len + stop).max(-1)
                        } else {
                            stop.min(len - 1)
                        } as usize;

                        if start > stop || start >= list.len() {
                            Vec::new()
                        } else {
                            list[start..=stop.min(list.len() - 1)].to_vec()
                        }
                    }
                    self::DataType::String(_) => Vec::new(), // Wrong type
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_set_and_get() {
        let store = Store::new();
        store.set_string("key1".to_string(), "value1".to_string());
        assert_eq!(store.get_string("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_get_nonexistent() {
        let store = Store::new();
        assert_eq!(store.get_string("nonexistent"), None);
    }

    #[test]
    fn test_overwrite() {
        let store = Store::new();
        store.set_string("key1".to_string(), "value1".to_string());
        store.set_string("key1".to_string(), "value2".to_string());
        assert_eq!(store.get_string("key1"), Some("value2".to_string()));
    }

    #[test]
    fn test_delete() {
        let store = Store::new();
        store.set_string("key1".to_string(), "value1".to_string());
        assert!(store.delete("key1"));
        assert!(store.get_string("key1").is_none());
        assert!(!store.delete("key1"));
    }

    #[test]
    fn test_exists() {
        let store = Store::new();
        assert!(!store.exists("key1"));
        store.set_string("key1".to_string(), "value1".to_string());
        assert!(store.exists("key1"));
        store.delete("key1");
        assert!(!store.exists("key1"));
    }

    #[test]
    fn test_expiration() {
        let store = Store::new();
        store.set_string_with_expiration(
            "key1".to_string(),
            "value1".to_string(),
            Duration::from_millis(100),
        );
        assert_eq!(store.get_string("key1"), Some("value1".to_string()));
        thread::sleep(Duration::from_millis(150));
        assert_eq!(store.get_string("key1"), None);
    }

    #[test]
    fn test_expiration_with_exists() {
        let store = Store::new();
        store.set_string_with_expiration(
            "key1".to_string(),
            "value1".to_string(),
            Duration::from_millis(100),
        );
        assert!(store.exists("key1"));
        thread::sleep(Duration::from_millis(150));
        assert!(!store.exists("key1"));
    }

    #[test]
    fn test_no_expiration() {
        let store = Store::new();
        store.set_string("key1".to_string(), "value1".to_string());
        thread::sleep(Duration::from_millis(100));
        assert_eq!(store.get_string("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_cleanup_expired() {
        let store = Store::new();
        store.set_string_with_expiration(
            "key1".to_string(),
            "value1".to_string(),
            Duration::from_millis(50),
        );
        store.set_string_with_expiration(
            "key2".to_string(),
            "value2".to_string(),
            Duration::from_millis(50),
        );
        store.set_string("key3".to_string(), "value3".to_string());
        thread::sleep(Duration::from_millis(100));
        let removed = store.cleanup_expired();
        assert_eq!(removed, 2);
        assert_eq!(store.get_string("key3"), Some("value3".to_string()));
    }

    #[test]
    fn test_len() {
        let store = Store::new();
        assert_eq!(store.len(), 0);
        store.set_string("key1".to_string(), "value1".to_string());
        assert_eq!(store.len(), 1);
        store.set_string("key2".to_string(), "value2".to_string());
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_len_with_expired() {
        let store = Store::new();
        store.set_string_with_expiration(
            "key1".to_string(),
            "value1".to_string(),
            Duration::from_millis(50),
        );
        store.set_string("key2".to_string(), "value2".to_string());
        assert_eq!(store.len(), 2);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_is_empty() {
        let store = Store::new();
        assert!(store.is_empty());
        store.set_string("key1".to_string(), "value1".to_string());
        assert!(!store.is_empty());
    }

    #[test]
    fn test_clear() {
        let store = Store::new();
        store.set_string("key1".to_string(), "value1".to_string());
        store.set_string("key2".to_string(), "value2".to_string());
        store.clear();
        assert!(store.is_empty());
        assert_eq!(store.get_string("key1"), None);
    }

    #[test]
    fn test_concurrent_reads() {
        let store = Store::new();
        store.set_string("key1".to_string(), "value1".to_string());

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let store_clone = store.clone();
                thread::spawn(move || {
                    assert_eq!(store_clone.get_string("key1"), Some("value1".to_string()));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_writes() {
        let store = Store::new();

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let store_clone = store.clone();
                thread::spawn(move || {
                    store_clone.set_string(format!("key{}", i), format!("value{}", i));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..10 {
            assert_eq!(
                store.get_string(&format!("key{}", i)),
                Some(format!("value{}", i))
            );
        }
    }

    #[test]
    fn test_concurrent_mixed_operations() {
        let store = Store::new();
        store.set_string("shared".to_string(), "initial".to_string());

        let handles: Vec<_> = (0..20)
            .map(|i| {
                let store_clone = store.clone();
                thread::spawn(move || {
                    if i % 2 == 0 {
                        store_clone.set_string(format!("key{}", i), format!("value{}", i));
                    } else {
                        store_clone.get_string("shared");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.get_string("shared"), Some("initial".to_string()));
    }

    #[test]
    fn test_overwrite_removes_expiration() {
        let store = Store::new();
        store.set_string_with_expiration(
            "key1".to_string(),
            "value1".to_string(),
            Duration::from_millis(100),
        );
        thread::sleep(Duration::from_millis(50));
        store.set_string("key1".to_string(), "value2".to_string());
        thread::sleep(Duration::from_millis(100));
        // Should still exist because we overwrote with no expiration
        assert_eq!(store.get_string("key1"), Some("value2".to_string()));
    }

    #[test]
    fn test_overwrite_with_new_expiration() {
        let store = Store::new();
        store.set_string_with_expiration(
            "key1".to_string(),
            "value1".to_string(),
            Duration::from_millis(50),
        );
        thread::sleep(Duration::from_millis(30));
        store.set_string_with_expiration(
            "key1".to_string(),
            "value2".to_string(),
            Duration::from_millis(100),
        );
        thread::sleep(Duration::from_millis(40)); // Total 70ms, first would be expired
        assert_eq!(store.get_string("key1"), Some("value2".to_string()));
        thread::sleep(Duration::from_millis(80)); // Total 150ms
        assert_eq!(store.get_string("key1"), None);
    }
}
