use std::path::PathBuf;

use clap::Parser;

use aether::config::AetherConfig;
use aether::storage::RocksStorage;

#[derive(Parser, Debug)]
#[command(name = "aether", about = "A distributed key-value store")]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "aether.toml")]
    config: String,

    /// Node ID
    #[arg(short, long)]
    node_id: Option<u64>,

    /// Listen address
    #[arg(short, long)]
    addr: Option<String>,

    /// Data directory
    #[arg(short, long)]
    data_dir: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    tracing::info!("Aether starting...");

    // Load config
    let mut config = if std::path::Path::new(&cli.config).exists() {
        AetherConfig::load(&cli.config)?
    } else {
        tracing::info!("Config file not found, using defaults");
        AetherConfig::default()
    };

    // Override config with CLI args
    if let Some(node_id) = cli.node_id {
        config.node_id = node_id;
    }
    if let Some(addr) = cli.addr {
        config.addr = addr;
    }
    if let Some(data_dir) = cli.data_dir {
        config.data_dir = PathBuf::from(data_dir);
    }

    tracing::info!(
        node_id = config.node_id,
        addr = %config.addr,
        data_dir = %config.data_dir.display(),
        "Initializing storage"
    );

    // Initialize storage
    let _storage = RocksStorage::open(&config.data_dir)?;
    tracing::info!("Storage initialized");

    // TODO: start Raft, start gRPC server

    tracing::info!("Aether stopped");
    Ok(())
}
