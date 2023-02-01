use std::{collections::HashMap};
use crate::error::KvErr;
use std::result;
use tempfile::TempDir;
use std::path::{self, Path, PathBuf};

/// kv store result, warp kvErr
pub type Result<T> = result::Result<T, KvErr>;

/// KvStore main data structure
pub struct KvStore {
    store: HashMap<String, String>,
}

/// impl new get set remove method
impl KvStore {
    /// impl new method
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    /// Open the KvStore at a given path.
    /// Return the KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        unimplemented!()
    }

    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).map(|x| x.clone()))
    }
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = self.store.entry(key).or_insert(value.clone());
        *entry = value;
        Ok(())
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
