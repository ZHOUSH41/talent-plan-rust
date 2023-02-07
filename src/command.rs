use clap::{Subcommand};
use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Debug, Subcommand)]
pub enum Commands {
    Get{ key: String},
    /// set key and value in kv store
    Set{key: String, value: String},
    /// remove key from kv store
    Rm{key: String},
}
