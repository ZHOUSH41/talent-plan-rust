use std::io;

use failure::Fail;

/// talent plan docs require use failure crates
/// but now failure crates not update anymore and
/// Rust ecosystem error handling use anyhow or thiserror
/// TODO: use thiserror define and handle kv error
///
#[derive(Fail, Debug)]
pub enum KvErr {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
}
