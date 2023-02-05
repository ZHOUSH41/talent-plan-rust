use clap::{Parser, Subcommand, ValueEnum};
use kvs::{Commands, KvErr, KvStore, Result};
use serde::{Deserialize, Serialize};
use std::env;
#[derive(Deserialize, Serialize, Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Commands>,
}
fn main() -> Result<()> {
    let cli = Cli::parse();
    // println!("current dir {:?}", env::current_dir()?);
    let mut store = KvStore::open(env::current_dir()?)?;
    match cli.cmd {
        Some(Commands::Get { key }) => match store.get(key)? {
            Some(val) => println!("{}", val),
            None => println!("Key not found"),
        },
        Some(Commands::Set { key, value } )=> {
            if let Err(err) = store.set(key, value) {
                println!("{}", err);
                std::process::exit(1);
            }
        }
        Some(Commands::Rm { key }) => {
            if let Err(KvErr::KeyNotFound) = store.remove(key) {
                println!("Key not found");
                std::process::exit(1);
            }
        }
        _ => {
            std::process::exit(1);
        }
    }
    Ok(())
}
