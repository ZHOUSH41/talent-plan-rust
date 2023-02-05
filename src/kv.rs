use serde_json::Deserializer;

use crate::{error::Result, error::KvErr, Commands};
use std::collections::HashMap;
use std::fs::{OpenOptions, create_dir_all, read_dir, File};
use std::io::{Write, BufReader, SeekFrom, Seek, Read};
use std::path::PathBuf;

/// KvStore main data structure
pub struct KvStore {
    store: HashMap<String, KvEntry>,
    current_file_id: u64,
    dir_path: PathBuf,
    current_file_offset: u64,
}

#[derive(Debug)]
/// Bitcask map entry struct
struct KvEntry {
    file_id: u64,
    value_sz: u64,
    value_pos: u64,
}

/// Bitcask file entry struct
// struct  FileEntry {
//     crc: String,
//     tstamp: String,
//     key_sz: usize,
//     value_sz: usize,
//     key: String,
//     value: String,
// }

/// impl new get set remove method
impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        create_dir_all(&dir_path)?;
        let mut store = HashMap::new();
        let (current_file_id, current_file_offset) = Self::recover(&dir_path, &mut store)?;
        let kv = KvStore { store:  store, current_file_id: current_file_id, dir_path:dir_path, current_file_offset: current_file_offset};
        Ok(kv)
    }

    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(t) => {
                // println!("pos: {}, sz: {}", t.value_pos,t.value_sz);
                let reader_path = self.dir_path.join(format!("store_file_{}.txt", t.file_id));
                let mut file = OpenOptions::new().read(true).open(&reader_path)?;
                let mut buf_reader = BufReader::new(file);
                buf_reader.seek(SeekFrom::Start(t.value_pos))?;
                let mut read_file_with_cap = buf_reader.take(t.value_sz);
                if let Some(Commands::Set { key, value }) = serde_json::from_reader(read_file_with_cap)? {
                    Ok(Some(value))
                } else {
                    Err(KvErr::UnknownCommand)
                }

            },
            None => Ok(None),
        }

    }
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Commands::Set { key: key.clone(), value };
        let series_data = serde_json::to_string(&cmd)?;
        let new_file_name = self
            .dir_path
            .join(format!("store_file_{}.txt", self.current_file_id));
        let mut file = OpenOptions::new().create(true).append(true).open(&new_file_name)?;
        let len = file.write(series_data.as_bytes())?;
        let offset = self.current_file_offset;
        self.current_file_offset += len as u64;
        let entry = KvEntry {
            file_id: self.current_file_id,
            value_pos: offset,
            value_sz: len as u64,
        };
        self.store.insert(key, entry);
        // println!("store: {:?}", self.store.get(&key));
        Ok(())
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        // println!("store: {:?}", self.store.get(&key));
        match self.store.get(&key) {
            Some(t) => {
                let cmd = Commands::Rm{key: key.clone()};
                let series_data = serde_json::to_string(&cmd)?;
                let file_path = self.dir_path.join(format!("store_file_{}.txt", t.file_id));
                let mut file = OpenOptions::new().append(true).open(&file_path)?; 
                let len = file.write(series_data.as_bytes())?;
                self.current_file_offset += len as u64; 
                self.store.remove(&key);
                Ok(())
            },
            None => Err(KvErr::KeyNotFound),
        }
    }

    fn recover(dir_path: &PathBuf, store: &mut HashMap<String, KvEntry>) -> Result<(u64, u64)> {
        let mut data_files: Vec<u64> = read_dir(dir_path)?
            .flat_map(|res| res.map(|e| e.path()))
            .filter(|path| path.is_file() && path.extension() == Some("txt".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| {
                        filename
                            .trim_start_matches("store_file_")
                            .trim_end_matches(".txt")
                    })
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();
        // println!("recover: {:?}", data_files);
        data_files.sort();
        let mut current_file_offset = 0;
        for data in &data_files {
            let file_path = dir_path.join(format!("store_file_{}.txt", data));          
            let reader = BufReader::new(File::open(&file_path)?);
            let mut iter = Deserializer::from_reader(reader).into_iter::<Commands>();
            let mut before_offset = iter.byte_offset() as u64;
            while let Some(command) = iter.next() {
                let after_offset = iter.byte_offset() as u64;
                match command? {
                    Commands::Set { key, value }=> {
                        store.insert(key, KvEntry { file_id: *data, value_sz: after_offset - before_offset, value_pos: before_offset });
                    },
                    Commands::Rm { key } => {
                        store.remove(&key);
                    },
                    Commands::Get { key } => todo!(),
                }
                current_file_offset = after_offset;
                before_offset = after_offset;
            }
        }
        Ok((*data_files.last().unwrap_or(&0), current_file_offset))
    }
}
