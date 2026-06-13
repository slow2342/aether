mod auth_proxy;
mod barrier_proxy;
mod cluster_proxy;
mod election_proxy;
mod kv_proxy;
mod lease_proxy;
mod lock_proxy;
mod maintenance_proxy;
mod queue_proxy;
mod session_proxy;
mod watch_proxy;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::RwLock;
use tonic::Request;
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::proto::MemberListRequest;
use crate::proto::aether_auth_client::AetherAuthClient;
use crate::proto::aether_barrier_client::AetherBarrierClient;
use crate::proto::aether_cluster_client::AetherClusterClient;
use crate::proto::aether_election_client::AetherElectionClient;
use crate::proto::aether_kv_client::AetherKvClient;
use crate::proto::aether_lease_client::AetherLeaseClient;
use crate::proto::aether_lock_client::AetherLockClient;
use crate::proto::aether_maintenance_client::AetherMaintenanceClient;
use crate::proto::aether_queue_client::AetherQueueClient;
use crate::proto::aether_session_client::AetherSessionClient;
use crate::proto::aether_watch_client::AetherWatchClient;

pub use self::auth_proxy::AuthProxy;
pub use self::barrier_proxy::BarrierProxy;
pub use self::cluster_proxy::ClusterProxy;
pub use self::election_proxy::ElectionProxy;
pub use self::kv_proxy::KvProxy;
pub use self::lease_proxy::LeaseProxy;
pub use self::lock_proxy::LockProxy;
pub use self::maintenance_proxy::MaintenanceProxy;
pub use self::queue_proxy::QueueProxy;
pub use self::session_proxy::SessionProxy;
pub use self::watch_proxy::WatchProxy;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5000;

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub listen_addr: String,
    pub backend_addrs: Vec<String>,
    pub request_timeout_ms: u64,
    pub tls: Option<TlsConfig>,
    pub health_addr: String,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub ca_cert: String,
    pub client_cert: String,
    pub client_key: String,
}

impl GatewayConfig {
    pub fn new(listen_addr: String, backend_addrs: Vec<String>) -> Self {
        Self {
            listen_addr,
            backend_addrs,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            tls: None,
            health_addr: "127.0.0.1:9091".to_string(),
        }
    }
}

/// Create a new `Request` preserving the `authorization` metadata.
pub(super) fn forward_request<T>(metadata: &tonic::metadata::MetadataMap, inner: T) -> Request<T> {
    let mut req = Request::new(inner);
    if let Some(auth) = metadata.get("authorization") {
        req.metadata_mut().insert("authorization", auth.clone());
    }
    req
}

#[derive(Debug, Clone)]
pub(super) struct BackendConnection {
    kv: AetherKvClient<Channel>,
    cluster: AetherClusterClient<Channel>,
    maintenance: AetherMaintenanceClient<Channel>,
    watch: AetherWatchClient<Channel>,
    lease: AetherLeaseClient<Channel>,
    auth: AetherAuthClient<Channel>,
    lock: AetherLockClient<Channel>,
    election: AetherElectionClient<Channel>,
    barrier: AetherBarrierClient<Channel>,
    queue: AetherQueueClient<Channel>,
    session: AetherSessionClient<Channel>,
}

pub struct BackendPool {
    backends: Vec<(String, BackendConnection)>,
    addr_to_idx: HashMap<String, usize>,
    leader_idx: Option<usize>,
    rr_counter: AtomicUsize,
    request_timeout: Duration,
    tls: Option<TlsConfig>,
}

impl BackendPool {
    async fn connect(
        addrs: &[String],
        request_timeout_ms: u64,
        tls: &Option<TlsConfig>,
    ) -> Result<Self, tonic::Status> {
        let mut backends = Vec::new();
        let mut addr_to_idx = HashMap::new();
        let scheme = if tls.is_some() { "https" } else { "http" };

        for addr in addrs {
            let uri = format!("{scheme}://{addr}");
            match connect_backend(&uri, tls).await {
                Ok(conn) => {
                    info!(addr = %addr, "connected to backend");
                    addr_to_idx.insert(addr.clone(), backends.len());
                    backends.push((addr.clone(), conn));
                }
                Err(e) => {
                    warn!(addr = %addr, error = %e, "failed to connect to backend");
                }
            }
        }

        if backends.is_empty() {
            return Err(tonic::Status::unavailable(
                "failed to connect to any backend node",
            ));
        }

        let mut pool = Self {
            backends,
            addr_to_idx,
            leader_idx: None,
            rr_counter: AtomicUsize::new(0),
            tls: tls.clone(),
            request_timeout: Duration::from_millis(request_timeout_ms),
        };

        pool.discover_leader().await;
        Ok(pool)
    }

    async fn discover_leader(&mut self) {
        for (i, (addr, conn)) in self.backends.iter_mut().enumerate() {
            let mut client = conn.cluster.clone();
            match tokio::time::timeout(
                self.request_timeout,
                client.member_list(Request::new(MemberListRequest {})),
            )
            .await
            {
                Ok(Ok(resp)) => {
                    let members = &resp.get_ref().members;
                    info!(addr = %addr, member_count = members.len(), "discovered cluster members, setting as initial leader candidate");
                    self.leader_idx = Some(i);
                    return;
                }
                Ok(Err(e)) => {
                    warn!(addr = %addr, error = %e, "member_list failed");
                }
                Err(_) => {
                    warn!(addr = %addr, "member_list timed out");
                }
            }
        }
        warn!("no backend responded to member_list, leader unknown");
    }

    pub fn update_leader(&mut self, addr: &str) {
        if let Some(&idx) = self.addr_to_idx.get(addr)
            && self.leader_idx != Some(idx)
        {
            info!(addr = %addr, "leader updated");
            self.leader_idx = Some(idx);
        }
    }

    pub(super) fn add_redirect_connection(&mut self, addr: String, conn: BackendConnection) {
        let idx = self.backends.len();
        info!(addr = %addr, idx = idx, "caching redirect connection");
        self.addr_to_idx.insert(addr.clone(), idx);
        self.leader_idx = Some(idx);
        self.backends.push((addr, conn));
    }

    fn timeout(&self) -> Duration {
        self.request_timeout
    }

    pub(super) fn tls(&self) -> &Option<TlsConfig> {
        &self.tls
    }

    pub fn backends_count(&self) -> usize {
        self.backends.len()
    }

    // --- KV ---
    pub fn get_leader_kv(&self) -> Option<(String, AetherKvClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.kv.clone()));
        }
        self.get_any_kv_inner()
    }
    pub fn get_any_kv(&self) -> Option<(String, AetherKvClient<Channel>)> {
        self.get_any_kv_inner()
    }
    fn get_any_kv_inner(&self) -> Option<(String, AetherKvClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.kv.clone()))
    }

    // --- Cluster ---
    pub fn get_leader_cluster(&self) -> Option<(String, AetherClusterClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.cluster.clone()));
        }
        self.get_any_cluster_inner()
    }
    pub fn get_any_cluster(&self) -> Option<(String, AetherClusterClient<Channel>)> {
        self.get_any_cluster_inner()
    }
    fn get_any_cluster_inner(&self) -> Option<(String, AetherClusterClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.cluster.clone()))
    }

    // --- Maintenance ---
    pub fn get_leader_maintenance(&self) -> Option<(String, AetherMaintenanceClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.maintenance.clone()));
        }
        self.get_any_maintenance_inner()
    }
    pub fn get_any_maintenance(&self) -> Option<(String, AetherMaintenanceClient<Channel>)> {
        self.get_any_maintenance_inner()
    }
    fn get_any_maintenance_inner(&self) -> Option<(String, AetherMaintenanceClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.maintenance.clone()))
    }

    // --- Watch ---
    pub fn get_leader_watch(&self) -> Option<(String, AetherWatchClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.watch.clone()));
        }
        self.get_any_watch_inner()
    }
    pub fn get_any_watch(&self) -> Option<(String, AetherWatchClient<Channel>)> {
        self.get_any_watch_inner()
    }
    fn get_any_watch_inner(&self) -> Option<(String, AetherWatchClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.watch.clone()))
    }

    // --- Lease ---
    pub fn get_leader_lease(&self) -> Option<(String, AetherLeaseClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.lease.clone()));
        }
        self.get_any_lease_inner()
    }
    pub fn get_any_lease(&self) -> Option<(String, AetherLeaseClient<Channel>)> {
        self.get_any_lease_inner()
    }
    fn get_any_lease_inner(&self) -> Option<(String, AetherLeaseClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.lease.clone()))
    }

    // --- Auth ---
    pub fn get_leader_auth(&self) -> Option<(String, AetherAuthClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.auth.clone()));
        }
        self.get_any_auth_inner()
    }
    pub fn get_any_auth(&self) -> Option<(String, AetherAuthClient<Channel>)> {
        self.get_any_auth_inner()
    }
    fn get_any_auth_inner(&self) -> Option<(String, AetherAuthClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.auth.clone()))
    }

    // --- Lock ---
    pub fn get_leader_lock(&self) -> Option<(String, AetherLockClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.lock.clone()));
        }
        self.get_any_lock_inner()
    }
    pub fn get_any_lock(&self) -> Option<(String, AetherLockClient<Channel>)> {
        self.get_any_lock_inner()
    }
    fn get_any_lock_inner(&self) -> Option<(String, AetherLockClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.lock.clone()))
    }

    // --- Election ---
    pub fn get_leader_election(&self) -> Option<(String, AetherElectionClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.election.clone()));
        }
        self.get_any_election_inner()
    }
    pub fn get_any_election(&self) -> Option<(String, AetherElectionClient<Channel>)> {
        self.get_any_election_inner()
    }
    fn get_any_election_inner(&self) -> Option<(String, AetherElectionClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.election.clone()))
    }

    // --- Barrier ---
    pub fn get_leader_barrier(&self) -> Option<(String, AetherBarrierClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.barrier.clone()));
        }
        self.get_any_barrier_inner()
    }
    pub fn get_any_barrier(&self) -> Option<(String, AetherBarrierClient<Channel>)> {
        self.get_any_barrier_inner()
    }
    fn get_any_barrier_inner(&self) -> Option<(String, AetherBarrierClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.barrier.clone()))
    }

    // --- Queue ---
    pub fn get_leader_queue(&self) -> Option<(String, AetherQueueClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.queue.clone()));
        }
        self.get_any_queue_inner()
    }
    pub fn get_any_queue(&self) -> Option<(String, AetherQueueClient<Channel>)> {
        self.get_any_queue_inner()
    }
    fn get_any_queue_inner(&self) -> Option<(String, AetherQueueClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.queue.clone()))
    }

    // --- Session ---
    pub fn get_leader_session(&self) -> Option<(String, AetherSessionClient<Channel>)> {
        if let Some(idx) = self.leader_idx
            && let Some((addr, conn)) = self.backends.get(idx)
        {
            return Some((addr.clone(), conn.session.clone()));
        }
        self.get_any_session_inner()
    }
    pub fn get_any_session(&self) -> Option<(String, AetherSessionClient<Channel>)> {
        self.get_any_session_inner()
    }
    fn get_any_session_inner(&self) -> Option<(String, AetherSessionClient<Channel>)> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        let (addr, conn) = self.backends.get(idx % self.backends.len())?;
        Some((addr.clone(), conn.session.clone()))
    }
}

/// Extract leader redirect address from a gRPC status, if present.
pub fn extract_leader_redirect(status: &tonic::Status) -> Option<String> {
    match status.metadata().get("x-aether-leader") {
        Some(val) => match val.to_str() {
            Ok(s) => Some(s.to_string()),
            Err(e) => {
                warn!(error = %e, "x-aether-leader header is not valid UTF-8");
                None
            }
        },
        None => None,
    }
}

/// Create a gRPC channel, optionally configured with TLS.
pub(super) async fn create_channel(
    uri: &str,
    tls: &Option<TlsConfig>,
) -> Result<Channel, tonic::Status> {
    let mut endpoint = Channel::from_shared(uri.to_string())
        .map_err(|e| tonic::Status::unavailable(format!("invalid uri: {e}")))?;

    if let Some(tls_cfg) = tls {
        let ca = std::fs::read(&tls_cfg.ca_cert)
            .map_err(|e| tonic::Status::unavailable(format!("read ca cert: {e}")))?;
        let cert = std::fs::read(&tls_cfg.client_cert)
            .map_err(|e| tonic::Status::unavailable(format!("read client cert: {e}")))?;
        let key = std::fs::read(&tls_cfg.client_key)
            .map_err(|e| tonic::Status::unavailable(format!("read client key: {e}")))?;

        let ca_cert = tonic::transport::Certificate::from_pem(ca);
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls_config = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(ca_cert)
            .identity(identity);

        endpoint = endpoint
            .tls_config(tls_config)
            .map_err(|e| tonic::Status::unavailable(format!("tls config: {e}")))?;
    }

    endpoint
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(format!("connect failed: {e}")))
}

async fn connect_backend(
    uri: &str,
    tls: &Option<TlsConfig>,
) -> Result<BackendConnection, tonic::Status> {
    let channel = create_channel(uri, tls).await?;
    Ok(connection_from_channel(channel))
}

fn connection_from_channel(channel: Channel) -> BackendConnection {
    BackendConnection {
        kv: AetherKvClient::new(channel.clone()),
        cluster: AetherClusterClient::new(channel.clone()),
        maintenance: AetherMaintenanceClient::new(channel.clone()),
        watch: AetherWatchClient::new(channel.clone()),
        lease: AetherLeaseClient::new(channel.clone()),
        auth: AetherAuthClient::new(channel.clone()),
        lock: AetherLockClient::new(channel.clone()),
        election: AetherElectionClient::new(channel.clone()),
        barrier: AetherBarrierClient::new(channel.clone()),
        queue: AetherQueueClient::new(channel.clone()),
        session: AetherSessionClient::new(channel),
    }
}

/// Connect to a redirected leader, cache the connection in the pool, and return it.
pub(super) async fn redirect_connection(
    pool: &Arc<RwLock<BackendPool>>,
    leader: &str,
) -> Result<BackendConnection, tonic::Status> {
    let tls = { pool.read().await.tls().clone() };
    let scheme = if tls.is_some() { "https" } else { "http" };
    let channel = create_channel(&format!("{scheme}://{leader}"), &tls).await?;
    let conn = connection_from_channel(channel);
    pool.write()
        .await
        .add_redirect_connection(leader.to_string(), conn.clone());
    Ok(conn)
}

pub async fn run_gateway(config: GatewayConfig) -> anyhow::Result<()> {
    info!(
        listen_addr = %config.listen_addr,
        backends = ?config.backend_addrs,
        timeout_ms = config.request_timeout_ms,
        "starting gateway"
    );

    let pool = BackendPool::connect(
        &config.backend_addrs,
        config.request_timeout_ms,
        &config.tls,
    )
    .await?;
    let pool = Arc::new(RwLock::new(pool));

    // Spawn health check HTTP server
    let health_pool = pool.clone();
    let health_addr: std::net::SocketAddr = config.health_addr.parse()?;
    tokio::spawn(async move {
        if let Err(e) = serve_health(health_addr, health_pool).await {
            tracing::error!(error = %e, "gateway health server failed");
        }
    });
    info!(addr = %config.health_addr, "gateway health server started");

    let addr = config.listen_addr.parse()?;
    info!(addr = %config.listen_addr, "gateway listening");

    tonic::transport::Server::builder()
        .add_service(crate::proto::aether_kv_server::AetherKvServer::new(
            KvProxy::new(pool.clone()),
        ))
        .add_service(
            crate::proto::aether_cluster_server::AetherClusterServer::new(ClusterProxy::new(
                pool.clone(),
            )),
        )
        .add_service(
            crate::proto::aether_maintenance_server::AetherMaintenanceServer::new(
                MaintenanceProxy::new(pool.clone()),
            ),
        )
        .add_service(crate::proto::aether_watch_server::AetherWatchServer::new(
            WatchProxy::new(pool.clone()),
        ))
        .add_service(crate::proto::aether_lease_server::AetherLeaseServer::new(
            LeaseProxy::new(pool.clone()),
        ))
        .add_service(crate::proto::aether_auth_server::AetherAuthServer::new(
            AuthProxy::new(pool.clone()),
        ))
        .add_service(crate::proto::aether_lock_server::AetherLockServer::new(
            LockProxy::new(pool.clone()),
        ))
        .add_service(
            crate::proto::aether_election_server::AetherElectionServer::new(ElectionProxy::new(
                pool.clone(),
            )),
        )
        .add_service(
            crate::proto::aether_barrier_server::AetherBarrierServer::new(BarrierProxy::new(
                pool.clone(),
            )),
        )
        .add_service(crate::proto::aether_queue_server::AetherQueueServer::new(
            QueueProxy::new(pool.clone()),
        ))
        .add_service(
            crate::proto::aether_session_server::AetherSessionServer::new(SessionProxy::new(
                pool.clone(),
            )),
        )
        .serve(addr)
        .await?;

    info!("gateway stopped");
    Ok(())
}

/// Gateway health check HTTP server.
///
/// - `GET /health/live` — always 200 if the process is running.
/// - `GET /health/ready` — 200 if at least one backend is reachable.
async fn serve_health(
    addr: std::net::SocketAddr,
    pool: Arc<RwLock<BackendPool>>,
) -> anyhow::Result<()> {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr).await?;
    info!(addr = %addr, "gateway health server listening");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "health server accept error");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let pool = pool.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                let pool = pool.clone();
                async move {
                    if req.method() != hyper::Method::GET {
                        return Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(405)
                                .body(Full::new(Bytes::from("METHOD NOT ALLOWED")))
                                .unwrap(),
                        );
                    }
                    let response = match req.uri().path() {
                        "/health/live" => Response::builder()
                            .status(200)
                            .body(Full::new(Bytes::from("OK")))
                            .unwrap(),
                        "/health/ready" => {
                            let p = pool.read().await;
                            if p.backends_count() > 0 {
                                Response::builder()
                                    .status(200)
                                    .body(Full::new(Bytes::from("OK")))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(503)
                                    .body(Full::new(Bytes::from("NOT READY")))
                                    .unwrap()
                            }
                        }
                        _ => Response::builder()
                            .status(404)
                            .body(Full::new(Bytes::from("NOT FOUND")))
                            .unwrap(),
                    };
                    Ok::<_, hyper::Error>(response)
                }
            });

            let conn_result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                http1::Builder::new().serve_connection(io, service),
            )
            .await;
            match conn_result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::debug!(error = %err, "health connection error");
                }
                Err(_) => {
                    tracing::debug!("health connection timed out");
                }
            }
        });
    }
}
