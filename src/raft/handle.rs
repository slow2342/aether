use async_trait::async_trait;
use tonic::Status;

use super::{NodeId, RaftRequest, RaftResponse};

/// Raft error type for the handle trait.
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("not leader, redirect to node {leader:?}")]
    NotLeader { leader: Option<u64> },
    #[error("no leader elected")]
    NoLeader,
    #[error("raft internal: {0}")]
    Internal(String),
    #[error("channel closed")]
    ChannelClosed,
}

/// Abstraction over Raft consensus operations.
/// KvService and ClusterService depend on this trait, not on a specific Raft library.
#[async_trait]
pub trait RaftHandle: Send + Sync + 'static {
    /// Propose a write request. Returns response after commit.
    async fn propose(&self, request: RaftRequest) -> Result<RaftResponse, RaftError>;

    /// Current leader's node_id.
    fn leader_id(&self) -> Option<u64>;

    /// Whether this node is the current leader.
    fn is_leader(&self, node_id: NodeId) -> bool {
        self.leader_id() == Some(node_id)
    }

    /// Confirm leadership and return the current commit index.
    /// The caller should wait until the state machine has applied up to this
    /// index before reading, to guarantee linearizable reads.
    fn commit_index(&self) -> u64;

    /// Return the last applied index of the state machine.
    fn applied_index(&self) -> u64;

    /// All cluster members: (node_id, addr).
    fn members(&self) -> Vec<(u64, String)>;

    /// Add a learner node to the cluster.
    async fn add_learner(&self, id: u64, addr: String) -> Result<(), RaftError>;

    /// Change voter configuration. `voters` is the complete set of voter node IDs.
    async fn change_membership(&self, voters: Vec<u64>) -> Result<(), RaftError>;
}

/// Shared helper for leader-required operations.
/// Returns `Ok(())` if this node is the leader, `Err` with leader redirect info otherwise.
pub fn require_leader(raft: &dyn RaftHandle, node_id: NodeId) -> Result<(), Status> {
    match raft.leader_id() {
        Some(id) if id == node_id => Ok(()),
        Some(_) => {
            let leader_addr = raft
                .members()
                .iter()
                .find(|(id, _)| Some(*id) == raft.leader_id())
                .map(|(_, addr)| addr.clone());

            let mut status = Status::unavailable("not leader");
            if let Some(addr) = leader_addr {
                let mut metadata = tonic::metadata::MetadataMap::new();
                metadata.insert(
                    "x-aether-leader",
                    addr.parse()
                        .map_err(|_| Status::internal("invalid leader addr"))?,
                );
                status = Status::with_metadata(status.code(), status.message(), metadata);
            }
            Err(status)
        }
        None => Err(Status::unavailable("no leader elected")),
    }
}

/// Confirm leadership and return the commit index for a linearizable read.
/// The caller must wait until `raft.applied_index() >= commit_index` before reading.
pub fn ensure_linearizable(raft: &dyn RaftHandle, node_id: NodeId) -> Result<u64, Status> {
    require_leader(raft, node_id)?;
    Ok(raft.commit_index())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockRaftHandle {
        leader: Option<u64>,
        members: Vec<(u64, String)>,
    }

    #[async_trait::async_trait]
    impl RaftHandle for MockRaftHandle {
        async fn propose(&self, _: RaftRequest) -> Result<RaftResponse, RaftError> {
            unimplemented!()
        }
        fn leader_id(&self) -> Option<u64> {
            self.leader
        }
        fn commit_index(&self) -> u64 {
            0
        }
        fn applied_index(&self) -> u64 {
            0
        }
        fn members(&self) -> Vec<(u64, String)> {
            self.members.clone()
        }
        async fn add_learner(&self, _: u64, _: String) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn change_membership(&self, _: Vec<u64>) -> Result<(), RaftError> {
            unimplemented!()
        }
    }

    #[test]
    fn test_require_leader_ok_when_is_leader() {
        let raft = MockRaftHandle {
            leader: Some(1),
            members: vec![(1, "127.0.0.1:2380".into())],
        };
        assert!(require_leader(&raft, 1).is_ok());
    }

    #[test]
    fn test_require_leader_err_when_not_leader() {
        let raft = MockRaftHandle {
            leader: Some(2),
            members: vec![(1, "127.0.0.1:2380".into()), (2, "127.0.0.2:2380".into())],
        };
        let err = require_leader(&raft, 1).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not leader"));
        // Should include leader address in metadata
        let leader_header = err.metadata().get("x-aether-leader");
        assert!(leader_header.is_some());
    }

    #[test]
    fn test_require_leader_err_when_no_leader() {
        let raft = MockRaftHandle {
            leader: None,
            members: vec![],
        };
        let err = require_leader(&raft, 1).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("no leader"));
    }
}
