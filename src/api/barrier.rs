use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::barrier::{BarrierManager, barrier_key, validate_barrier_name};
use crate::proto::aether_barrier_server::AetherBarrier;
use crate::proto::{
    BarrierCreateRequest, BarrierCreateResponse, BarrierQueryRequest, BarrierQueryResponse,
    BarrierReleaseRequest, BarrierReleaseResponse, ResponseHeader,
};
use crate::raft::{self, RaftHandle, WatchEventType, require_leader};
use crate::watch::WatchManager;

/// Maximum time to wait for barrier acquisition (30 seconds).
const BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

pub struct BarrierService {
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    barrier_manager: Arc<Mutex<BarrierManager>>,
    watch_manager: Arc<WatchManager>,
}

impl BarrierService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        barrier_manager: Arc<Mutex<BarrierManager>>,
        watch_manager: Arc<WatchManager>,
    ) -> Self {
        Self {
            raft,
            node_id,
            barrier_manager,
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

    /// Try to create a barrier, blocking if it's already held.
    /// Creates a watch BEFORE proposing to avoid missing release events.
    async fn create_with_wait(&self, name: Vec<u8>, lease_id: i64) -> Result<Vec<u8>, Status> {
        // Create a watch on the barrier key BEFORE proposing
        // This ensures we don't miss any Delete events
        let key = barrier_key(&name);
        let (watch_id, mut watch_rx) = self
            .watch_manager
            .create(
                key,
                Vec::new(), // exact key match
                vec![WatchEventType::Delete],
                false,
            )
            .await;

        // Helper to cancel watch
        let cancel_watch = || {
            let wm = self.watch_manager.clone();
            async move {
                wm.cancel(watch_id, "barrier create completed".to_string())
                    .await;
            }
        };

        loop {
            // Propose barrier create to Raft
            let resp = match self
                .propose(raft::RaftRequest::BarrierCreate {
                    name: name.clone(),
                    lease_id,
                })
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    cancel_watch().await;
                    return Err(e);
                }
            };

            match resp {
                raft::RaftResponse::BarrierCreate { key } => {
                    cancel_watch().await;
                    return Ok(key);
                }
                raft::RaftResponse::BarrierAlreadyHeld { .. } => {
                    // Wait for the barrier key to be deleted
                    tracing::debug!(
                        barrier = %String::from_utf8_lossy(&name),
                        "barrier is held, waiting for release"
                    );

                    // Wait for the key to be deleted or timeout
                    let result = tokio::time::timeout(BARRIER_TIMEOUT, async {
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
                            // Barrier was released, retry create
                            tracing::debug!(
                                barrier = %String::from_utf8_lossy(&name),
                                "barrier released, retrying create"
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
                                "barrier create timed out after {} seconds: barrier is held",
                                BARRIER_TIMEOUT.as_secs()
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

#[tonic::async_trait]
impl AetherBarrier for BarrierService {
    async fn create(
        &self,
        request: Request<BarrierCreateRequest>,
    ) -> Result<Response<BarrierCreateResponse>, Status> {
        let req = request.into_inner();

        validate_barrier_name(&req.name).map_err(Status::invalid_argument)?;

        if req.lease_id < 0 {
            return Err(Status::invalid_argument("lease_id must be non-negative"));
        }

        // Use create_with_wait to block if barrier is already held
        let key = self.create_with_wait(req.name, req.lease_id).await?;

        Ok(Response::new(BarrierCreateResponse {
            header: Some(self.header()),
            key,
        }))
    }

    async fn release(
        &self,
        request: Request<BarrierReleaseRequest>,
    ) -> Result<Response<BarrierReleaseResponse>, Status> {
        let req = request.into_inner();

        validate_barrier_name(&req.name).map_err(Status::invalid_argument)?;

        let resp = self
            .propose(raft::RaftRequest::BarrierRelease { name: req.name })
            .await?;

        match resp {
            raft::RaftResponse::BarrierRelease {} => Ok(Response::new(BarrierReleaseResponse {
                header: Some(self.header()),
            })),
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn query(
        &self,
        request: Request<BarrierQueryRequest>,
    ) -> Result<Response<BarrierQueryResponse>, Status> {
        let req = request.into_inner();

        validate_barrier_name(&req.name).map_err(Status::invalid_argument)?;

        require_leader(self.raft.as_ref(), self.node_id)?;

        let mgr = self
            .barrier_manager
            .lock()
            .map_err(|e| Status::internal(format!("barrier manager lock poisoned: {e}")))?;

        let key = mgr
            .get_key(&req.name)
            .map(|k| k.to_vec())
            .unwrap_or_default();
        let held = !key.is_empty();
        let lease_id = if held {
            mgr.get_lease_id(&key).unwrap_or(0)
        } else {
            0
        };

        Ok(Response::new(BarrierQueryResponse {
            header: Some(self.header()),
            held,
            key,
            lease_id,
        }))
    }
}
