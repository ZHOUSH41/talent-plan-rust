use std::{collections::HashMap};
/// KvStore main data structure
pub struct KvStore {
    map: HashMap<String, String>,
}

/// impl new get set remove method
impl KvStore {

    /// impl new method
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// input key stirng
    /// output Option value
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|x| x.clone())
    }

    /// insert kv store 
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// remove key/value from kv strore
    /// key may not in kv store
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
