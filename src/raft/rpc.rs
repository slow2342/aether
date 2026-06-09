use std::sync::Arc;

use openraft::Raft;
use tonic::{Request, Response, Status};

use super::TypeConfig;
use crate::proto::raft_rpc as pb;
use crate::proto::raft_rpc::raft_rpc_server::RaftRpc;

/// Raft RPC server implementation
pub struct RaftRpcImpl {
    raft: Arc<Raft<TypeConfig>>,
}

impl RaftRpcImpl {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self { raft }
    }
}

#[async_trait::async_trait]
impl RaftRpc for RaftRpcImpl {
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let ae_req: openraft::raft::AppendEntriesRequest<TypeConfig> =
            serde_json::from_slice(&req.payload)
                .map_err(|e| Status::internal(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .append_entries(ae_req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let payload = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(pb::AppendEntriesResponse { payload }))
    }

    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let req = request.into_inner();
        let vote_req: openraft::raft::VoteRequest<u64> = serde_json::from_slice(&req.payload)
            .map_err(|e| Status::internal(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .vote(vote_req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let payload = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(pb::VoteResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotRequest>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let is_req: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice(&req.payload)
                .map_err(|e| Status::internal(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let payload = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(pb::InstallSnapshotResponse { payload }))
    }
}
