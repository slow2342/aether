use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use tonic::transport::Server;

use aether::api::{ClusterService, KvService, LeaseService, WatchService};
use aether::config::AetherConfig;
use aether::lease::{LeaseManager, LeaseStore};
use aether::proto::aether_cluster_server::AetherClusterServer;
use aether::proto::aether_kv_server::AetherKvServer;
use aether::proto::aether_lease_server::AetherLeaseServer;
use aether::proto::aether_watch_server::AetherWatchServer;
use aether::proto::raft_rpc::raft_rpc_server::RaftRpcServer;
use aether::raft::node;
use aether::raft::raftrs_handle::RaftRsHandle;
use aether::raft::raftrs_store::RaftRsStore;
use aether::raft::rpc::RaftRpcImpl;
use aether::raft::state_machine::AetherStateMachine;
use aether::raft::{RaftHandle, WatchEvent};
use aether::storage::RocksStorage;
use aether::watch::WatchManager;

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

    // Create raft-rs log store sharing the same RocksDB instance
    let raft_store = RaftRsStore::new(storage.db().clone());

    // Create state machine
    let (watch_tx, _watch_rx) = tokio::sync::broadcast::channel::<WatchEvent>(1024);
    let lease_store = LeaseStore::new(storage.clone());
    let (mut lease_manager, expiry_rx) = LeaseManager::new(config.lease.max_leases, 1);
    // Restore lease state from persistent storage
    if let Err(e) = lease_manager.restore(&lease_store) {
        tracing::error!(error = %e, "failed to restore lease state from storage");
    }
    let lease_manager = Arc::new(Mutex::new(lease_manager));
    let lease_store_for_sm = lease_store.clone();
    let lease_manager_for_sm = lease_manager.clone();
    let state_machine = Arc::new(Mutex::new(AetherStateMachine::new(
        watch_tx.clone(),
        storage.clone(),
        lease_manager_for_sm,
        lease_store_for_sm,
    )));

    // Build raft-rs config
    let raft_config = raft::Config {
        id: config.node_id,
        election_tick: 10,             // 10 ticks = 1 second (100ms per tick)
        heartbeat_tick: 1,             // 1 tick = 100ms heartbeat
        max_size_per_msg: 1024 * 1024, // 1MB
        max_inflight_msgs: 256,
        ..Default::default()
    };

    // Build node address map
    let mut node_addrs = std::collections::HashMap::new();
    node_addrs.insert(config.node_id, config.addr.clone());
    for peer in &config.cluster.peers {
        node_addrs.insert(peer.node_id, peer.addr.clone());
    }

    // Build initial peers list
    let mut initial_peers = vec![(config.node_id, config.addr.clone())];
    for peer in &config.cluster.peers {
        initial_peers.push((peer.node_id, peer.addr.clone()));
    }

    // Channel for outgoing raft messages (event loop → network sender)
    let (msg_out_tx, msg_out_rx) = tokio::sync::mpsc::channel(1024);

    // Start raft event loop on dedicated thread
    let node::RaftNodeHandle {
        thread_handle: _raft_handle,
        msg_tx: msg_in_tx,
        propose_tx,
        conf_change_tx,
        shared_state,
    } = node::start_raft_node(
        raft_config,
        raft_store,
        state_machine,
        msg_out_tx,
        initial_peers.clone(),
    )?;

    tracing::info!("Raft event loop started");

    // Create network sender task
    let mut network_sender =
        aether::raft::network::NetworkSender::new(msg_out_rx, config.node_id, node_addrs);
    tokio::spawn(async move {
        network_sender.run().await;
    });

    // Create services
    let watch_manager = WatchManager::new(watch_tx);
    let watch_service = WatchService::new(watch_manager);

    // Create raft handle for API layer
    let raft_handle: Arc<dyn RaftHandle> = Arc::new(RaftRsHandle::new(
        propose_tx,
        conf_change_tx,
        shared_state,
        initial_peers.clone(),
    ));

    let kv_service = KvService::new(storage, raft_handle.clone(), config.node_id);
    let cluster_service = ClusterService::new(raft_handle.clone(), config.node_id);
    let lease_service = LeaseService::new(
        raft_handle.clone(),
        config.node_id,
        lease_manager.clone(),
        config.lease.max_ttl,
    );

    // Create RPC server (receives messages from other nodes)
    let raft_rpc_service = RaftRpcImpl::new(msg_in_tx);

    // Start gRPC server
    let addr = config.addr.parse()?;
    tracing::info!(addr = %config.addr, "Starting gRPC server");

    // Spawn lease expiry task.
    //
    // NOTE: There is an inherent race between the expiry task checking for expired
    // leases and in-flight KeepAlive proposals. A KeepAlive proposed but not yet
    // committed may be overridden by a Revoke. Clients mitigate this by sending
    // KeepAlive at TTL/3 frequency, well before the actual expiry.
    {
        let expiry_raft = raft_handle.clone();
        let expiry_lease_manager = lease_manager.clone();
        let mut expiry_rx = expiry_rx;
        let (check_tx, mut check_rx) = tokio::sync::mpsc::channel::<()>(1);

        tokio::spawn(async move {
            loop {
                let earliest = *expiry_rx.borrow_and_update();
                let now = aether::lease::now_millis();
                let sleep_ms = (earliest - now).max(100) as u64;

                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)) => {
                        let _ = check_tx.send(()).await;
                    }
                    result = expiry_rx.changed() => {
                        if result.is_err() { break; }
                    }
                }
            }
        });

        tokio::spawn(async move {
            while check_rx.recv().await.is_some() {
                let expired_ids = {
                    match expiry_lease_manager.lock() {
                        Ok(mgr) => mgr.expired_ids(),
                        Err(e) => {
                            tracing::warn!(error = %e, "lease manager mutex poisoned, skipping expiry check");
                            continue;
                        }
                    }
                };
                for id in expired_ids {
                    // Re-check: lease may have been renewed between expired_ids() and now
                    let still_expired = expiry_lease_manager.lock().map_or(true, |mgr| {
                        mgr.get(id)
                            .is_none_or(|l| l.expiry_time <= aether::lease::now_millis())
                    });
                    if !still_expired {
                        continue;
                    }
                    tracing::info!(lease_id = id, "lease expired, proposing revoke");
                    if let Err(e) = expiry_raft
                        .propose(aether::raft::RaftRequest::LeaseRevoke { id })
                        .await
                    {
                        tracing::error!(lease_id = id, error = %e, "failed to propose lease revoke");
                    }
                }
            }
        });
    }

    tracing::info!("Lease expiry task started");

    Server::builder()
        .add_service(AetherKvServer::new(kv_service))
        .add_service(AetherWatchServer::new(watch_service))
        .add_service(AetherLeaseServer::new(lease_service))
        .add_service(AetherClusterServer::new(cluster_service))
        .add_service(RaftRpcServer::new(raft_rpc_service))
        .serve(addr)
        .await?;

    tracing::info!("Aether stopped");
    Ok(())
}
