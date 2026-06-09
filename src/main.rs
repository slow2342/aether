use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;

use aether::api::{ClusterService, KvService};
use aether::config::AetherConfig;
use aether::proto::aether_cluster_server::AetherClusterServer;
use aether::proto::aether_kv_server::AetherKvServer;
use aether::proto::raft_rpc::raft_rpc_server::RaftRpcServer;
use aether::raft::log_store::AetherLogStore;
use aether::raft::network::AetherNetwork;
use aether::raft::rpc::RaftRpcImpl;
use aether::raft::state_machine::AetherStateMachine;
use aether::raft::{RaftNode, TypeConfig, WatchEvent};
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

    // Initialize storage (creates default, raft_log, raft_state CFs)
    let storage = Arc::new(RocksStorage::open(&config.data_dir)?);
    tracing::info!("Storage initialized");

    // Create Raft log store sharing the same RocksDB instance
    let log_store = AetherLogStore::new(storage.db().clone())?;

    // Create state machine
    let (watch_tx, _watch_rx) = tokio::sync::broadcast::channel::<WatchEvent>(1024);
    let state_machine = AetherStateMachine::new(watch_tx);

    // Build Raft config
    let raft_config = Arc::new(openraft::Config {
        cluster_name: "aether".to_string(),
        election_timeout_min: config.cluster.election_timeout_ms,
        election_timeout_max: config.cluster.election_timeout_ms * 2,
        heartbeat_interval: config.cluster.heartbeat_interval_ms,
        ..Default::default()
    });

    // Build initial member set from config
    let mut members = BTreeMap::new();
    members.insert(
        config.node_id,
        RaftNode {
            addr: config.addr.clone(),
            data: String::new(),
        },
    );
    for peer in &config.cluster.peers {
        members.insert(
            peer.node_id,
            RaftNode {
                addr: peer.addr.clone(),
                data: String::new(),
            },
        );
    }

    // Create network layer and Raft instance
    let network = AetherNetwork::new(config.node_id);
    let raft = Arc::new(
        openraft::Raft::<TypeConfig>::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine,
        )
        .await?,
    );

    tracing::info!("Raft instance created");

    // Bootstrap cluster if needed
    match raft.initialize(members).await {
        Ok(()) => {
            tracing::info!("Cluster initialized");
        }
        Err(openraft::error::RaftError::APIError(
            openraft::error::InitializeError::NotAllowed(_),
        )) => {
            tracing::info!("Cluster already initialized, skipping bootstrap");
        }
        Err(e) => {
            tracing::error!(error = %e, "Cluster initialization failed");
            return Err(anyhow::anyhow!("cluster initialization failed: {e}"));
        }
    }

    // Create services
    let kv_service = KvService::new(storage);
    let cluster_service = ClusterService::new(raft.clone(), config.node_id);
    let raft_rpc_service = RaftRpcImpl::new(raft);

    // Start gRPC server
    let addr = config.addr.parse()?;
    tracing::info!(addr = %config.addr, "Starting gRPC server");

    Server::builder()
        .add_service(AetherKvServer::new(kv_service))
        .add_service(AetherClusterServer::new(cluster_service))
        .add_service(RaftRpcServer::new(raft_rpc_service))
        .serve(addr)
        .await?;

    tracing::info!("Aether stopped");
    Ok(())
}
