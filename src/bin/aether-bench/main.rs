mod common;
mod put;
mod range;
mod report;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "aether-bench", about = "Aether cluster benchmark tool")]
struct Cli {
    /// Server endpoint address
    #[arg(long, default_value = "127.0.0.1:2379")]
    endpoint: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Benchmark PUT operations
    Put {
        /// Total number of operations
        #[arg(long, default_value_t = 10000)]
        total: usize,

        /// Number of concurrent clients
        #[arg(long, default_value_t = 1)]
        clients: usize,

        /// Key size in bytes
        #[arg(long, default_value_t = 64)]
        key_size: usize,

        /// Value size in bytes
        #[arg(long, default_value_t = 256)]
        val_size: usize,

        /// Use sequential keys instead of random
        #[arg(long)]
        sequential: bool,
    },

    /// Benchmark RANGE/GET operations (single key)
    Range {
        /// Total number of operations
        #[arg(long, default_value_t = 10000)]
        total: usize,

        /// Number of concurrent clients
        #[arg(long, default_value_t = 1)]
        clients: usize,

        /// Key size in bytes
        #[arg(long, default_value_t = 64)]
        key_size: usize,

        /// Value size in bytes (for pre-fill)
        #[arg(long, default_value_t = 256)]
        val_size: usize,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Put {
            total,
            clients,
            key_size,
            val_size,
            sequential,
        } => {
            put::run(put::PutConfig {
                endpoint: cli.endpoint,
                total,
                clients,
                key_size,
                val_size,
                sequential,
            })
            .await;
        }
        Command::Range {
            total,
            clients,
            key_size,
            val_size,
        } => {
            range::run(range::RangeConfig {
                endpoint: cli.endpoint,
                total,
                clients,
                key_size,
                val_size,
            })
            .await;
        }
    }
}
