use clap::{Parser, Subcommand};
use kvs::KvStore;
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// get key related value from kv store
    Get {
        /// lists test values
        key: String,
    },
    /// set key and value in kv store 
    Set {
        key: String,
        value: String,
    },
    /// remove key from kv store
    Rm {
        key: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Get{key}) => {
            eprintln!("unimplemented");
            std::process::exit(1);
        },
        Some(Commands::Set { key, value }) => {
            eprintln!("unimplemented");
            std::process::exit(1);
        },
        Some(Commands::Rm { key }) => {
            eprintln!("unimplemented");
            std::process::exit(1);
        },
        None => {
            eprintln!("command don't implement!");
            std::process::exit(1);
        },
    }
}
