use prost011::Message as ProstMessage;
use raft::eraftpb::Message;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::proto::raft_rpc as pb;
use crate::proto::raft_rpc::raft_rpc_server::RaftRpc;

/// Raft RPC server that receives messages from other nodes
/// and forwards them to the event loop.
pub struct RaftRpcImpl {
    msg_tx: mpsc::Sender<Message>,
}

impl RaftRpcImpl {
    pub fn new(msg_tx: mpsc::Sender<Message>) -> Self {
        Self { msg_tx }
    }
}

#[async_trait::async_trait]
impl RaftRpc for RaftRpcImpl {
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let msg = Message::decode(req.payload.as_slice())
            .map_err(|e| Status::internal(format!("decode error: {e}")))?;

        self.msg_tx
            .send(msg)
            .await
            .map_err(|e| Status::internal(format!("channel error: {e}")))?;

        Ok(Response::new(pb::AppendEntriesResponse { payload: vec![] }))
    }

    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let req = request.into_inner();
        let msg = Message::decode(req.payload.as_slice())
            .map_err(|e| Status::internal(format!("decode error: {e}")))?;

        self.msg_tx
            .send(msg)
            .await
            .map_err(|e| Status::internal(format!("channel error: {e}")))?;

        Ok(Response::new(pb::VoteResponse { payload: vec![] }))
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotRequest>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let msg = Message::decode(req.payload.as_slice())
            .map_err(|e| Status::internal(format!("decode error: {e}")))?;

        self.msg_tx
            .send(msg)
            .await
            .map_err(|e| Status::internal(format!("channel error: {e}")))?;

        Ok(Response::new(pb::InstallSnapshotResponse {
            payload: vec![],
        }))
    }
}
