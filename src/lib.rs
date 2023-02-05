/// KvStore crate
#[deny(missing_docs)]
mod kv;
mod error;
mod command;
pub use kv::KvStore;
pub use error::Result;
pub use error::KvErr;
pub use command::Commands;
