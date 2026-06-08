use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use tokio::sync::RwLock;

use super::{NodeId, RaftNode, TypeConfig};

/// Aether Raft network implementation
pub struct AetherNetwork {
    /// Node ID of this node
    #[allow(dead_code)]
    node_id: NodeId,
    /// Cluster members
    #[allow(dead_code)]
    members: Arc<RwLock<BTreeMap<NodeId, RaftNode>>>,
}

impl AetherNetwork {
    /// Create a new network instance
    pub fn new(node_id: NodeId, members: Arc<RwLock<BTreeMap<NodeId, RaftNode>>>) -> Self {
        Self { node_id, members }
    }
}

impl RaftNetworkFactory<TypeConfig> for AetherNetwork {
    type Network = AetherRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &RaftNode) -> Self::Network {
        AetherRaftNetwork {
            target,
            target_addr: node.addr.clone(),
        }
    }
}

/// Raft network client for a specific target node
pub struct AetherRaftNetwork {
    /// Target node ID
    target: NodeId,
    /// Target node address
    target_addr: String,
}

impl RaftNetwork<TypeConfig> for AetherRaftNetwork {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, RaftNode, RaftError<u64>>> {
        tracing::debug!(
            target = self.target,
            target_addr = %self.target_addr,
            "sending AppendEntries"
        );

        // TODO: Implement gRPC client for AppendEntries using tonic
        Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other("not implemented"),
        )))
    }

    async fn vote(
        &mut self,
        _req: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, RaftNode, RaftError<u64>>> {
        tracing::debug!(
            target = self.target,
            target_addr = %self.target_addr,
            "sending Vote"
        );

        // TODO: Implement gRPC client for Vote using tonic
        Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other("not implemented"),
        )))
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<TypeConfig>,
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

        // TODO: Implement gRPC client for InstallSnapshot using tonic
        Err(RPCError::Network(NetworkError::new(
            &std::io::Error::other("not implemented"),
        )))
    }
}
