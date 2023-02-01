/// KvStore crate
#[deny(missing_docs)]
mod kv;
mod error;
pub use kv::{Result, KvStore};
