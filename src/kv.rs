use serde_json::Deserializer;

use crate::{error::KvErr, error::Result, Commands};
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, remove_file, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const REDUNDAN_DATA_LIMIT: u64 = 1024;
/// KvStore main data structure
pub struct KvStore {
    store: HashMap<String, KvEntry>,
    current_file_id: u64,
    dir_path: PathBuf,
    current_file_offset: u64,
    redundant_data_sz: u64,
}

#[derive(Debug)]
/// Bitcask map entry struct
struct KvEntry {
    file_id: u64,
    value_sz: u64,
    value_pos: u64,
}

/// impl new get set remove method
impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        create_dir_all(&dir_path)?;
        let mut store = HashMap::new();
        let (current_file_id, current_file_offset, redundant_data_sz) =
            Self::recover(&dir_path, &mut store)?;
        if current_file_id == 0 {
            let new_file_name = dir_path.join(format!("store_file_{}.txt", current_file_id));
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_file_name)?;
        }
        let mut kv = KvStore {
            store,
            current_file_id,
            dir_path,
            current_file_offset,
            redundant_data_sz,
        };
        if redundant_data_sz > REDUNDAN_DATA_LIMIT {
            kv.compact()?
        }
        Ok(kv)
    }

    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(t) => {
                let reader_path = self.dir_path.join(format!("store_file_{}.txt", t.file_id));
                let file = OpenOptions::new().read(true).open(&reader_path)?;
                let mut buf_reader = BufReader::new(file);
                buf_reader.seek(SeekFrom::Start(t.value_pos))?;
                let read_file_with_cap = buf_reader.take(t.value_sz);
                // TODO: serde_json::from_reader slower method than from_str or simliar other method
                if let Some(Commands::Set { key: _, value }) =
                    serde_json::from_reader(read_file_with_cap)?
                {
                    Ok(Some(value))
                } else {
                    Err(KvErr::UnknownCommand)
                }
            }
            None => Ok(None),
        }
    }
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Commands::Set {
            key: key.clone(),
            value,
        };
        let series_data = serde_json::to_string(&cmd)?;
        let new_file_name = self
            .dir_path
            .join(format!("store_file_{}.txt", self.current_file_id));
        let mut file = OpenOptions::new().append(true).open(&new_file_name)?;
        let len = file.write(series_data.as_bytes())?;
        assert_ne!(len, 0);
        let offset = self.current_file_offset;
        self.current_file_offset += len as u64;
        let entry = KvEntry {
            file_id: self.current_file_id,
            value_pos: offset,
            value_sz: len as u64,
        };
        self.redundant_data_sz += self
            .store
            .insert(key, entry)
            .map(|entry| entry.value_sz)
            .unwrap_or(0);
        if self.redundant_data_sz > REDUNDAN_DATA_LIMIT {
            self.compact()?
        }
        Ok(())
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.store.get(&key) {
            Some(t) => {
                let cmd = Commands::Rm { key: key.clone() };
                let series_data = serde_json::to_string(&cmd)?;
                let file_path = self.dir_path.join(format!("store_file_{}.txt", t.file_id));
                let mut file = OpenOptions::new().append(true).open(&file_path)?;
                let len = file.write(series_data.as_bytes())?;
                self.current_file_offset += len as u64;
                self.redundant_data_sz += len as u64;
                self.redundant_data_sz += self
                    .store
                    .remove(&key)
                    .map(|entry| entry.value_sz)
                    .unwrap_or(0);

                if self.redundant_data_sz > REDUNDAN_DATA_LIMIT {
                    self.compact()?
                }
                Ok(())
            }
            None => Err(KvErr::KeyNotFound),
        }
    }

    /// 恢复流程：
    /// 1. 读取对应文件夹下面的data files，并排序
    /// 2. 对每个文件进行恢复，KvEntry
    /// 3. 返回最后一个文件的编号和offset
    fn recover(
        dir_path: &PathBuf,
        store: &mut HashMap<String, KvEntry>,
    ) -> Result<(u64, u64, u64)> {
        let data_files = Self::find_dir_data_files(&dir_path)?;
        let mut current_file_offset = 0;
        let mut redundant_data_sz = 0;
        for data in &data_files {
            let file_path = dir_path.join(format!("store_file_{}.txt", data));
            let reader = BufReader::new(File::open(&file_path)?);
            let mut iter = Deserializer::from_reader(reader).into_iter::<Commands>();
            let mut before_offset = iter.byte_offset() as u64;
            while let Some(command) = iter.next() {
                let after_offset = iter.byte_offset() as u64;
                assert_ne!(after_offset, before_offset);
                match command? {
                    Commands::Set { key, value: _ } => {
                        redundant_data_sz += store
                            .insert(
                                key,
                                KvEntry {
                                    file_id: *data,
                                    value_sz: after_offset - before_offset,
                                    value_pos: before_offset,
                                },
                            )
                            .map(|entry| entry.value_sz)
                            .unwrap_or(0);
                    }
                    Commands::Rm { key } => {
                        redundant_data_sz +=
                            store.remove(&key).map(|entry| entry.value_sz).unwrap_or(0);
                        redundant_data_sz += after_offset - before_offset;
                    }
                    _ => continue,
                }
                current_file_offset = after_offset;
                // 需要更新before offset，这是value pos的值
                before_offset = after_offset;
            }
        }
        Ok((
            *data_files.last().unwrap_or(&0),
            current_file_offset,
            redundant_data_sz,
        ))
    }

    /// 当冗余的数据超过一定的量之后，需要进行压缩
    /// 压缩流程：
    /// 1.
    fn compact(&mut self) -> Result<()> {
        self.create_new_file()?;
        let new_file_name = self
            .dir_path
            .join(format!("store_file_{}.txt", self.current_file_id));
        let mut before_offset = 0;
        let mut buf_writer = BufWriter::new(OpenOptions::new().append(true).open(&new_file_name)?);
        for entry in self.store.values_mut() {
            let file_path = self
                .dir_path
                .join(format!("store_file_{}.txt", entry.file_id));
            let mut reader = BufReader::new(File::open(&file_path)?);
            reader.seek(SeekFrom::Start(entry.value_pos))?;
            let mut data_reader = reader.take(entry.value_sz);
            let len = io::copy(&mut data_reader, &mut buf_writer)?;
            assert_ne!(len, 0);
            *entry = KvEntry {
                file_id: self.current_file_id,
                value_sz: len,
                value_pos: before_offset,
            };
            before_offset += len;
        }
        buf_writer.flush()?;

        let data_files = Self::find_dir_data_files(&self.dir_path)?;
        for data in data_files.into_iter().filter(|x| *x < self.current_file_id) {
            remove_file(self.dir_path.join(format!("store_file_{}.txt", data)))?;
        }

        self.create_new_file()?;
        Ok(())
    }

    fn create_new_file(&mut self) -> Result<()> {
        self.current_file_id += 1;
        let new_file_name = self
            .dir_path
            .join(format!("store_file_{}.txt", self.current_file_id));
        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_file_name)?;
        //
        self.current_file_offset = 0;
        Ok(())
    }

    fn find_dir_data_files(dir_path: &PathBuf) -> Result<Vec<u64>> {
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
        data_files.sort();
        Ok(data_files)
    }
}
