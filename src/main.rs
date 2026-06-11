use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use clap::Parser;
use rand_core::{OsRng, RngCore};
use tonic::transport::Server;

use aether::api::{AuthService, ClusterService, KvService, LeaseService, WatchService};
use aether::auth::AuthLayer;
use aether::config::AetherConfig;
use aether::lease::{LeaseManager, LeaseStore};
use aether::proto::aether_auth_server::AetherAuthServer;
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
use aether::storage::{RocksStorage, StorageEngine};
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
    let auth_cache = Arc::new(aether::auth::AuthCache::new());
    let auth_enabled = Arc::new(AtomicBool::new(config.auth.enabled));
    let auth_bootstrapped = Arc::new(AtomicBool::new(false));

    // Resolve JWT signing key: use configured value, or load/generate a random key.
    // All nodes in a cluster MUST share the same signing_key — configure it explicitly.
    let signing_key = if config.auth.signing_key.is_empty() {
        let key_path = config.data_dir.join(".signing_key");
        if key_path.exists() {
            std::fs::read_to_string(&key_path)
                .map(|s| s.trim().to_string())
                .unwrap_or_default()
        } else {
            let mut bytes = [0u8; 32];
            OsRng.fill_bytes(&mut bytes);
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            // Write with restrictive permissions (owner-only read/write)
            match std::fs::File::create(&key_path) {
                Ok(f) => {
                    use std::io::Write;
                    use std::os::unix::fs::PermissionsExt;
                    let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
                    let mut writer = std::io::BufWriter::new(f);
                    if let Err(e) = writer.write_all(hex.as_bytes()) {
                        tracing::error!(error = %e, "failed to write signing key");
                    } else if let Err(e) = writer.flush() {
                        tracing::error!(error = %e, "failed to flush signing key");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to create signing key file");
                }
            }
            hex
        }
    } else {
        config.auth.signing_key.clone()
    };
    if signing_key.is_empty() {
        anyhow::bail!(
            "JWT signing key is empty; configure auth.signing_key or ensure data directory is writable"
        );
    }

    let token_validator = Arc::new(aether::auth::TokenValidator::new(
        &signing_key,
        config.auth.token_expiry_hours,
    ));
    let token_validator_for_api = token_validator.clone();
    let auth_interceptor = Arc::new(aether::auth::AuthInterceptor::new(
        auth_enabled.clone(),
        token_validator,
        auth_cache.clone(),
        auth_bootstrapped.clone(),
    ));

    // Load existing auth data from storage into cache (handles node restarts)
    {
        let users = storage
            .scan(aether::auth::USER_KEY_PREFIX, usize::MAX)
            .unwrap_or_default();
        let mut loaded_users = Vec::new();
        for kv in &users {
            if let Ok(user) =
                rkyv::from_bytes::<aether::auth::User, rkyv::rancor::BoxedError>(&kv.value)
            {
                loaded_users.push(user);
            }
        }
        auth_cache.load_users(loaded_users);

        let roles = storage
            .scan(aether::auth::ROLE_KEY_PREFIX, usize::MAX)
            .unwrap_or_default();
        let mut loaded_roles = Vec::new();
        for kv in &roles {
            if let Ok(role) =
                rkyv::from_bytes::<aether::auth::Role, rkyv::rancor::BoxedError>(&kv.value)
            {
                loaded_roles.push(role);
            }
        }
        auth_cache.load_roles(loaded_roles);

        // Restore auth enabled state from previous run
        if storage
            .get(aether::auth::AUTH_ENABLED_KEY)
            .unwrap_or(None)
            .is_some()
        {
            auth_enabled.store(true, Ordering::Release);
        }

        // Restore bootstrapped flag
        if storage
            .get(aether::auth::AUTH_BOOTSTRAPPED_KEY)
            .unwrap_or(None)
            .is_some()
        {
            auth_bootstrapped.store(true, Ordering::Release);
        }
    }

    let auth_cache_for_api = auth_cache.clone();
    let auth_interceptor_for_api = auth_interceptor.clone();

    let state_machine = Arc::new(Mutex::new(AetherStateMachine::new(
        watch_tx.clone(),
        storage.clone(),
        lease_manager_for_sm,
        lease_store_for_sm,
        auth_cache,
        auth_enabled.clone(),
    )));
    // auth_enabled is cloned above; keep a reference for ClusterService below

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
        read_index_tx,
        shared_state,
    } = node::start_raft_node(
        raft_config,
        raft_store,
        state_machine,
        msg_out_tx,
        initial_peers.clone(),
        config.cluster.snapshot_trigger_log_entries,
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
    let watch_service = WatchService::new(
        watch_manager,
        auth_enabled.clone(),
        auth_interceptor_for_api.clone(),
    );

    // Create raft handle for API layer
    let raft_handle: Arc<dyn RaftHandle> = Arc::new(RaftRsHandle::new(
        propose_tx,
        conf_change_tx,
        read_index_tx,
        shared_state,
        initial_peers.clone(),
    ));

    let auth_enabled_for_api = auth_enabled.clone();
    let kv_service = KvService::new(
        storage,
        raft_handle.clone(),
        config.node_id,
        auth_enabled.clone(),
        auth_interceptor,
    );
    let cluster_service =
        ClusterService::new(raft_handle.clone(), config.node_id, auth_enabled.clone());
    let lease_service = LeaseService::new(
        raft_handle.clone(),
        config.node_id,
        lease_manager.clone(),
        config.lease.max_ttl,
        auth_enabled,
    );

    let auth_service = AuthService::new(
        raft_handle.clone(),
        config.node_id,
        auth_cache_for_api,
        token_validator_for_api,
        auth_enabled_for_api,
        auth_interceptor_for_api.clone(),
        auth_bootstrapped,
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

    let auth_layer = AuthLayer::new(auth_interceptor_for_api);

    Server::builder()
        .layer(auth_layer)
        .add_service(AetherAuthServer::new(auth_service))
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
