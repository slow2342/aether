use std::sync::Arc;
use std::sync::Mutex;

use tonic::{Request, Response, Status};

use crate::lock::{LockManager, lock_name, validate_lock_name};
use crate::proto::aether_lock_server::AetherLock;
use crate::proto::{
    LockQueryRequest, LockQueryResponse, LockRequest, LockResponse, ResponseHeader, UnlockRequest,
    UnlockResponse,
};
use crate::raft::{self, RaftHandle, require_leader};

pub struct LockService {
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    lock_manager: Arc<Mutex<LockManager>>,
}

impl LockService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        lock_manager: Arc<Mutex<LockManager>>,
    ) -> Self {
        Self {
            raft,
            node_id,
            lock_manager,
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
}

#[tonic::async_trait]
impl AetherLock for LockService {
    async fn lock(&self, request: Request<LockRequest>) -> Result<Response<LockResponse>, Status> {
        let req = request.into_inner();

        // Validate lock name
        validate_lock_name(&req.name).map_err(Status::invalid_argument)?;

        // Validate lease_id (must be non-negative)
        if req.lease_id < 0 {
            return Err(Status::invalid_argument("lease_id must be non-negative"));
        }

        // Propose to Raft - the state machine will check if lock is already held
        let resp = self
            .propose(raft::RaftRequest::LockAcquire {
                name: req.name,
                lease_id: req.lease_id,
            })
            .await?;

        match resp {
            raft::RaftResponse::LockAcquire { key } => Ok(Response::new(LockResponse {
                header: Some(self.header()),
                key,
            })),
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        let req = request.into_inner();

        // Validate lock key
        if req.key.is_empty() {
            return Err(Status::invalid_argument("lock key must not be empty"));
        }

        // Validate it's a lock key
        if lock_name(&req.key).is_none() {
            return Err(Status::invalid_argument("key is not a lock key"));
        }

        let resp = self
            .propose(raft::RaftRequest::LockRelease { key: req.key })
            .await?;

        match resp {
            raft::RaftResponse::LockRelease {} => Ok(Response::new(UnlockResponse {
                header: Some(self.header()),
            })),
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn lock_query(
        &self,
        request: Request<LockQueryRequest>,
    ) -> Result<Response<LockQueryResponse>, Status> {
        let req = request.into_inner();

        // Validate lock name
        validate_lock_name(&req.name).map_err(Status::invalid_argument)?;

        // Require leader for linearizable read
        require_leader(self.raft.as_ref(), self.node_id)?;

        let mgr = self
            .lock_manager
            .lock()
            .map_err(|e| Status::internal(format!("lock manager lock poisoned: {e}")))?;

        let key = mgr
            .get_key(&req.name)
            .map(|k| k.to_vec())
            .unwrap_or_default();
        let lease_id = if key.is_empty() {
            0
        } else {
            mgr.get_lease_id(&key).unwrap_or(0)
        };

        Ok(Response::new(LockQueryResponse {
            header: Some(self.header()),
            owner: key,
            lease_id,
        }))
    }
}
