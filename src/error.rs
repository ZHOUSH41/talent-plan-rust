use std::io;
use std::result;
use failure::Fail;

/// kv store result, warp kvErr
pub type Result<T> = result::Result<T, KvErr>;

/// talent plan docs require use failure crates
/// but now failure crates not update anymore and
/// Rust ecosystem error handling use anyhow or thiserror
/// TODO: use thiserror define and handle kv error
///
#[derive(Fail, Debug)]
pub enum KvErr {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    SerializeErr(#[cause] serde_json::Error),
    
    #[fail(display = "Key not found")]
    KeyNotFound,
    
    #[fail(display = "unknown command")]
    UnknownCommand,
}


/// impl std::io::Error convert to KvErr
impl From<io::Error> for KvErr {
    fn from(value: io::Error) -> Self {
        KvErr::Io(value)
    }
}

impl From<serde_json::Error> for KvErr {
    fn from(value: serde_json::Error) -> Self {
        KvErr::SerializeErr(value)
    }
}