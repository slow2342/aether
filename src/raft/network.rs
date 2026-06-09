use std::time::Duration;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use tonic::transport::Channel;

use super::{NodeId, RaftNode, TypeConfig};
use crate::proto::raft_rpc as pb;
use crate::proto::raft_rpc::raft_rpc_client::RaftRpcClient;

/// Aether Raft network implementation
pub struct AetherNetwork {
    node_id: NodeId,
}

impl AetherNetwork {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

impl RaftNetworkFactory<TypeConfig> for AetherNetwork {
    type Network = AetherRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &RaftNode) -> Self::Network {
        tracing::debug!(
            source = self.node_id,
            target,
            addr = %node.addr,
            "creating raft network client"
        );
        AetherRaftNetwork {
            target,
            target_addr: node.addr.clone(),
            client: None,
        }
    }
}

/// Raft network client for a specific target node
pub struct AetherRaftNetwork {
    target: NodeId,
    target_addr: String,
    client: Option<RaftRpcClient<Channel>>,
}

impl AetherRaftNetwork {
    async fn get_client(&mut self) -> Result<&mut RaftRpcClient<Channel>, NetworkError> {
        if self.client.is_none() {
            let uri = format!("http://{}", self.target_addr);
            let client = RaftRpcClient::connect(uri)
                .await
                .map_err(|e| NetworkError::new(&e))?;
            self.client = Some(client);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl RaftNetwork<TypeConfig> for AetherRaftNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, RaftNode, RaftError<u64>>> {
        tracing::debug!(
            target = self.target,
            target_addr = %self.target_addr,
            "sending AppendEntries"
        );

        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let payload =
            serde_json::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let request = tonic::Request::new(pb::AppendEntriesRequest { payload });

        let response = tokio::time::timeout(Duration::from_secs(5), client.append_entries(request))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();
        serde_json::from_slice(&resp.payload).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, RaftNode, RaftError<u64>>> {
        tracing::debug!(
            target = self.target,
            target_addr = %self.target_addr,
            "sending Vote"
        );

        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let payload =
            serde_json::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let request = tonic::Request::new(pb::VoteRequest { payload });

        let response = tokio::time::timeout(Duration::from_secs(2), client.vote(request))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();
        serde_json::from_slice(&resp.payload).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, RaftNode, RaftError<u64, InstallSnapshotError>>,
    > {
        tracing::debug!(
            target = self.target,
            target_addr = %self.target_addr,
            "sending InstallSnapshot"
        );

        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let payload =
            serde_json::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let request = tonic::Request::new(pb::InstallSnapshotRequest { payload });

        let response =
            tokio::time::timeout(Duration::from_secs(60), client.install_snapshot(request))
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();
        serde_json::from_slice(&resp.payload).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}
