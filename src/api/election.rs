use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::election::{ElectionManager, election_key, election_name, validate_election_name};
use crate::proto::aether_election_server::AetherElection;
use crate::proto::{
    CampaignRequest, CampaignResponse, KeyValue, LeaderRequest, LeaderResponse, ObserveRequest,
    ObserveResponse, ResignRequest, ResignResponse, ResponseHeader,
};
use crate::raft::{self, RaftHandle, WatchEventType, require_leader};
use crate::watch::WatchManager;

/// Maximum value size for election campaign (64 KB).
const MAX_ELECTION_VALUE_SIZE: usize = 64 * 1024;

/// Lease ID size in bytes (i64 = 8 bytes).
const LEASE_ID_SIZE: usize = 8;

/// Maximum time to wait for leadership acquisition (30 seconds).
const CAMPAIGN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ElectionService {
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    election_manager: Arc<Mutex<ElectionManager>>,
    storage: Arc<crate::storage::RocksStorage>,
    watch_manager: Arc<WatchManager>,
}

impl ElectionService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        election_manager: Arc<Mutex<ElectionManager>>,
        storage: Arc<crate::storage::RocksStorage>,
        watch_manager: Arc<WatchManager>,
    ) -> Self {
        Self {
            raft,
            node_id,
            election_manager,
            storage,
            watch_manager,
        }
    }

    fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: 0,
            member_id: self.node_id,
            revision: 0,
            raft_term: self.raft.term(),
        }
    }

    async fn propose(&self, request: raft::RaftRequest) -> Result<raft::RaftResponse, Status> {
        require_leader(self.raft.as_ref(), self.node_id)?;
        self.raft
            .propose(request)
            .await
            .map_err(|e| Status::internal(format!("raft write failed: {e}")))
    }

    /// Get the leader's value from storage.
    /// Value format in KV store: `[lease_id: i64 BE][value_bytes]`
    fn get_leader_value(&self, key: &[u8]) -> Vec<u8> {
        use crate::storage::StorageEngine;
        match self.storage.get(key) {
            Ok(Some(value)) => {
                if value.len() > LEASE_ID_SIZE {
                    value[LEASE_ID_SIZE..].to_vec()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    /// Build a KeyValue response for a leader.
    fn build_leader_kv(&self, key: &[u8], lease_id: i64) -> KeyValue {
        let value = self.get_leader_value(key);
        KeyValue {
            key: key.to_vec(),
            value,
            create_revision: 0,
            mod_revision: 0,
            version: 1,
            lease: lease_id,
        }
    }

    /// Try to campaign and wait for leadership if election already has a leader.
    async fn campaign_with_wait(
        &self,
        name: Vec<u8>,
        lease_id: i64,
        value: Vec<u8>,
    ) -> Result<Vec<u8>, Status> {
        // Create a watch on the election key BEFORE proposing
        // This ensures we don't miss any Delete events
        let election_key_val = election_key(&name);
        let (watch_id, mut watch_rx) = self
            .watch_manager
            .create(
                election_key_val.clone(),
                Vec::new(), // exact key match
                vec![WatchEventType::Delete],
                false,
            )
            .await;

        // Helper to cancel watch
        let cancel_watch = || {
            let wm = self.watch_manager.clone();
            async move {
                wm.cancel(watch_id, "campaign completed".to_string()).await;
            }
        };

        loop {
            // Propose campaign to Raft
            let resp = self
                .propose(raft::RaftRequest::ElectionCampaign {
                    name: name.clone(),
                    lease_id,
                    value: value.clone(),
                })
                .await?;

            match resp {
                raft::RaftResponse::ElectionCampaign { leader_key } => {
                    cancel_watch().await;
                    return Ok(leader_key);
                }
                raft::RaftResponse::ElectionAlreadyHasLeader { .. } => {
                    // Wait for the leader key to be deleted
                    tracing::debug!(
                        election = %String::from_utf8_lossy(&name),
                        "election has leader, waiting for resign or expiry"
                    );

                    // Wait for the key to be deleted or timeout
                    let result = tokio::time::timeout(CAMPAIGN_TIMEOUT, async {
                        while let Some(resp) = watch_rx.recv().await {
                            if resp.canceled {
                                break;
                            }
                            for event in &resp.events {
                                if event.event_type == WatchEventType::Delete {
                                    return true;
                                }
                            }
                        }
                        false
                    })
                    .await;

                    match result {
                        Ok(true) => {
                            // Leader key was deleted, retry campaign
                            tracing::debug!(
                                election = %String::from_utf8_lossy(&name),
                                "leader resigned, retrying campaign"
                            );
                            continue;
                        }
                        Ok(false) => {
                            // Watch was canceled unexpectedly
                            cancel_watch().await;
                            return Err(Status::internal("watch canceled unexpectedly"));
                        }
                        Err(_) => {
                            // Timeout
                            cancel_watch().await;
                            return Err(Status::deadline_exceeded(format!(
                                "campaign timed out after {} seconds: election already has a leader",
                                CAMPAIGN_TIMEOUT.as_secs()
                            )));
                        }
                    }
                }
                raft::RaftResponse::Error { message } => {
                    cancel_watch().await;
                    return Err(Status::internal(message));
                }
                _ => {
                    cancel_watch().await;
                    return Err(Status::internal("unexpected response type"));
                }
            }
        }
    }
}

type ObserveStream = Pin<Box<dyn Stream<Item = Result<ObserveResponse, Status>> + Send>>;

#[tonic::async_trait]
impl AetherElection for ElectionService {
    async fn campaign(
        &self,
        request: Request<CampaignRequest>,
    ) -> Result<Response<CampaignResponse>, Status> {
        let req = request.into_inner();

        // Validate election name
        validate_election_name(&req.name).map_err(Status::invalid_argument)?;

        // Validate lease_id (must be positive)
        if req.lease_id <= 0 {
            return Err(Status::invalid_argument("lease_id must be positive"));
        }

        // Validate value size
        if req.value.len() > MAX_ELECTION_VALUE_SIZE {
            return Err(Status::invalid_argument(format!(
                "value size {} exceeds maximum {}",
                req.value.len(),
                MAX_ELECTION_VALUE_SIZE
            )));
        }

        // Campaign with wait for leadership
        let leader_key = self
            .campaign_with_wait(req.name, req.lease_id, req.value)
            .await?;

        Ok(Response::new(CampaignResponse {
            header: Some(self.header()),
            leader_key,
        }))
    }

    async fn leader(
        &self,
        request: Request<LeaderRequest>,
    ) -> Result<Response<LeaderResponse>, Status> {
        let req = request.into_inner();

        // Validate election name
        validate_election_name(&req.name).map_err(Status::invalid_argument)?;

        // Require leader for linearizable read
        require_leader(self.raft.as_ref(), self.node_id)?;

        // Extract data from lock, then release before I/O
        let (leader_key, lease_id) = {
            let mgr = self
                .election_manager
                .lock()
                .map_err(|e| Status::internal(format!("election manager lock poisoned: {e}")))?;

            let key = mgr.get_leader_key(&req.name).map(|k| k.to_vec());
            let lease = key.as_ref().and_then(|k| mgr.get_lease_id(k)).unwrap_or(0);
            (key, lease)
        };

        // Build KeyValue response (I/O happens here, outside the lock)
        let kv = leader_key.map(|key| self.build_leader_kv(&key, lease_id));

        Ok(Response::new(LeaderResponse {
            header: Some(self.header()),
            kv,
        }))
    }

    async fn resign(
        &self,
        request: Request<ResignRequest>,
    ) -> Result<Response<ResignResponse>, Status> {
        let req = request.into_inner();

        // Validate leader key
        if req.leader_key.is_empty() {
            return Err(Status::invalid_argument("leader_key must not be empty"));
        }

        // Validate it's an election key
        if election_name(&req.leader_key).is_none() {
            return Err(Status::invalid_argument("key is not an election key"));
        }

        let resp = self
            .propose(raft::RaftRequest::ElectionResign {
                leader_key: req.leader_key,
            })
            .await?;

        match resp {
            raft::RaftResponse::ElectionResign {} => Ok(Response::new(ResignResponse {
                header: Some(self.header()),
            })),
            raft::RaftResponse::ElectionResignNotFound {} => {
                Err(Status::not_found("leader key not found"))
            }
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    type ObserveStream = ObserveStream;

    async fn observe(
        &self,
        request: Request<ObserveRequest>,
    ) -> Result<Response<Self::ObserveStream>, Status> {
        let req = request.into_inner();

        // Validate election name
        validate_election_name(&req.name).map_err(Status::invalid_argument)?;

        // Require leader for linearizable read
        require_leader(self.raft.as_ref(), self.node_id)?;

        let (tx, rx) = mpsc::channel::<Result<ObserveResponse, Status>>(256);

        // Get current leader and send initial response
        let election_name = req.name.clone();
        let election_manager = self.election_manager.clone();
        let watch_manager = self.watch_manager.clone();
        let header = self.header();

        // Send initial leader info
        {
            let mgr = election_manager
                .lock()
                .map_err(|e| Status::internal(format!("election manager lock poisoned: {e}")))?;

            let leader_key = mgr.get_leader_key(&election_name).map(|k| k.to_vec());
            let kv = leader_key.map(|key| {
                let lease_id = mgr.get_lease_id(&key).unwrap_or(0);
                KeyValue {
                    key: key.clone(),
                    value: Vec::new(),
                    create_revision: 0,
                    mod_revision: 0,
                    version: 1,
                    lease: lease_id,
                }
            });

            let resp = ObserveResponse {
                header: Some(header),
                kv,
            };

            // Send initial leader info
            if tx.try_send(Ok(resp)).is_err() {
                return Err(Status::internal("failed to send initial observe response"));
            }
        }

        // Spawn task to watch for leadership changes
        let election_key_exact = election_key(&election_name);
        tokio::spawn(async move {
            // Create a watch on the specific election key (exact match)
            let (watch_id, mut watch_rx) = watch_manager
                .create(
                    election_key_exact.clone(),
                    Vec::new(), // empty = exact key match
                    vec![WatchEventType::Put, WatchEventType::Delete],
                    true, // include prev_kv
                )
                .await;

            // Forward watch events as ObserveResponse
            while let Some(resp) = watch_rx.recv().await {
                if resp.canceled {
                    break;
                }

                for event in &resp.events {
                    let kv = Some(KeyValue {
                        key: event.kv.key.clone(),
                        value: event.kv.value.clone(),
                        create_revision: event.kv.create_revision,
                        mod_revision: event.kv.mod_revision,
                        version: event.kv.version,
                        lease: event.kv.lease,
                    });

                    let observe_resp = ObserveResponse {
                        header: None, // No header for subsequent events
                        kv,
                    };

                    if tx.send(Ok(observe_resp)).await.is_err() {
                        // Client disconnected
                        watch_manager
                            .cancel(watch_id, "client disconnected".to_string())
                            .await;
                        return;
                    }
                }
            }

            // Clean up watch if stream ends
            watch_manager
                .cancel(watch_id, "observe stream ended".to_string())
                .await;
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as ObserveStream))
    }
}
