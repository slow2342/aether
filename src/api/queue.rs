use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto::aether_queue_server::AetherQueue;
use crate::proto::{
    QueueDequeueRequest, QueueDequeueResponse, QueueEnqueueRequest, QueueEnqueueResponse,
    QueuePeekRequest, QueuePeekResponse, ResponseHeader,
};
use crate::queue::{queue_scan_prefix, validate_queue_name};
use crate::raft::{self, RaftHandle, require_leader};
use crate::storage::StorageEngine;

/// Maximum value size for queue items (1 MB).
const MAX_QUEUE_VALUE_SIZE: usize = 1024 * 1024;

pub struct QueueService {
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    storage: Arc<crate::storage::RocksStorage>,
}

impl QueueService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        storage: Arc<crate::storage::RocksStorage>,
    ) -> Self {
        Self {
            raft,
            node_id,
            storage,
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
impl AetherQueue for QueueService {
    async fn enqueue(
        &self,
        request: Request<QueueEnqueueRequest>,
    ) -> Result<Response<QueueEnqueueResponse>, Status> {
        let req = request.into_inner();

        validate_queue_name(&req.name).map_err(Status::invalid_argument)?;

        if req.value.is_empty() {
            return Err(Status::invalid_argument("value must not be empty"));
        }
        if req.value.len() > MAX_QUEUE_VALUE_SIZE {
            return Err(Status::invalid_argument(format!(
                "value size {} exceeds maximum {}",
                req.value.len(),
                MAX_QUEUE_VALUE_SIZE
            )));
        }

        let resp = self
            .propose(raft::RaftRequest::QueueEnqueue {
                name: req.name,
                value: req.value,
            })
            .await?;

        match resp {
            raft::RaftResponse::QueueEnqueue { key } => Ok(Response::new(QueueEnqueueResponse {
                header: Some(self.header()),
                key,
            })),
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn dequeue(
        &self,
        request: Request<QueueDequeueRequest>,
    ) -> Result<Response<QueueDequeueResponse>, Status> {
        let req = request.into_inner();

        validate_queue_name(&req.name).map_err(Status::invalid_argument)?;

        let resp = self
            .propose(raft::RaftRequest::QueueDequeue { name: req.name })
            .await?;

        match resp {
            raft::RaftResponse::QueueDequeue { key, value } => {
                Ok(Response::new(QueueDequeueResponse {
                    header: Some(self.header()),
                    key,
                    value,
                }))
            }
            raft::RaftResponse::QueueDequeueEmpty {} => Err(Status::not_found("queue is empty")),
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn peek(
        &self,
        request: Request<QueuePeekRequest>,
    ) -> Result<Response<QueuePeekResponse>, Status> {
        let req = request.into_inner();

        validate_queue_name(&req.name).map_err(Status::invalid_argument)?;

        require_leader(self.raft.as_ref(), self.node_id)?;

        // Read the front item directly from storage (without proposing through Raft)
        let prefix = queue_scan_prefix(&req.name);
        let entries = self
            .storage
            .scan(&prefix, 1)
            .map_err(|e| Status::internal(format!("storage scan failed: {e}")))?;

        if entries.is_empty() {
            return Err(Status::not_found("queue is empty"));
        }

        let front = &entries[0];
        Ok(Response::new(QueuePeekResponse {
            header: Some(self.header()),
            key: front.key.clone(),
            value: front.value.clone(),
        }))
    }
}
