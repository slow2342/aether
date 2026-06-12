use async_trait::async_trait;
use tonic::Status;

use super::{NodeId, RaftRequest, RaftResponse};

/// Cluster member with role information.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub id: NodeId,
    pub addr: String,
    pub is_learner: bool,
}

/// Raft error type for the handle trait.
#[derive(Debug, Clone, thiserror::Error)]
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

    /// Return the current commit index (without leader confirmation).
    fn commit_index(&self) -> u64;

    /// Return the last applied index of the state machine.
    fn applied_index(&self) -> u64;

    /// Wait until applied_index may have changed. Used by linearizable_read
    /// to avoid polling.
    async fn wait_for_apply(&self);

    /// Request a ReadIndex from the Raft leader. Sends a heartbeat to confirm
    /// leadership and returns the commit index at the time of confirmation.
    /// The caller must wait until `applied_index() >= returned_index` before reading.
    async fn read_index(&self) -> Result<u64, RaftError>;

    /// All cluster members with role information.
    fn members(&self) -> Vec<MemberInfo>;

    /// Add a learner node to the cluster.
    async fn add_learner(&self, id: u64, addr: String) -> Result<(), RaftError>;

    /// Remove a node (voter or learner) from the cluster.
    async fn remove_node(&self, id: u64) -> Result<(), RaftError>;

    /// Change voter configuration. `voters` is the complete set of voter node IDs.
    async fn change_membership(&self, voters: Vec<u64>) -> Result<(), RaftError>;
}

/// Shared helper for leader-required operations.
/// Returns `Ok(())` if this node is the leader, `Err` with leader redirect info otherwise.
pub fn require_leader(raft: &dyn RaftHandle, node_id: NodeId) -> Result<(), Status> {
    let current_leader = raft.leader_id();
    match current_leader {
        Some(id) if id == node_id => Ok(()),
        Some(leader_id) => {
            let leader_addr = raft
                .members()
                .into_iter()
                .find(|m| m.id == leader_id)
                .map(|m| m.addr);

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

/// Confirm leadership via ReadIndex and return the commit index for a
/// linearizable read. The caller must wait until
/// `raft.applied_index() >= commit_index` before reading.
pub async fn ensure_linearizable(raft: &dyn RaftHandle) -> Result<u64, Status> {
    raft.read_index().await.map_err(|e| match e {
        RaftError::NotLeader { leader } => {
            let mut status = Status::unavailable("not leader");
            if let Some(leader_id) = leader {
                let leader_addr = raft
                    .members()
                    .into_iter()
                    .find(|m| m.id == leader_id)
                    .map(|m| m.addr);
                if let Some(addr) = leader_addr {
                    let mut metadata = tonic::metadata::MetadataMap::new();
                    if let Ok(val) = addr.parse() {
                        metadata.insert("x-aether-leader", val);
                    }
                    status = Status::with_metadata(status.code(), status.message(), metadata);
                }
            }
            status
        }
        RaftError::NoLeader => Status::unavailable("no leader elected"),
        _ => Status::internal(format!("read index failed: {e}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct MockRaftHandle {
        leader: Option<u64>,
        members: Vec<MemberInfo>,
        commit_idx: u64,
        applied_idx: AtomicU64,
        read_index_result: Option<Result<u64, RaftError>>,
    }

    impl MockRaftHandle {
        fn new(leader: Option<u64>, members: Vec<MemberInfo>) -> Self {
            Self {
                leader,
                members,
                commit_idx: 0,
                applied_idx: AtomicU64::new(0),
                read_index_result: None,
            }
        }

        fn with_commit_index(mut self, idx: u64) -> Self {
            self.commit_idx = idx;
            self
        }

        fn with_read_index_result(mut self, result: Result<u64, RaftError>) -> Self {
            self.read_index_result = Some(result);
            self
        }
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
            self.commit_idx
        }
        fn applied_index(&self) -> u64 {
            self.applied_idx.load(Ordering::Relaxed)
        }
        async fn wait_for_apply(&self) {
            // No-op in mock.
        }
        async fn read_index(&self) -> Result<u64, RaftError> {
            if let Some(result) = &self.read_index_result {
                return result.clone();
            }
            if self.leader.is_some() {
                Ok(self.commit_idx)
            } else {
                Err(RaftError::NoLeader)
            }
        }
        fn members(&self) -> Vec<MemberInfo> {
            self.members.clone()
        }
        async fn add_learner(&self, _: u64, _: String) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn remove_node(&self, _: u64) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn change_membership(&self, _: Vec<u64>) -> Result<(), RaftError> {
            unimplemented!()
        }
    }

    fn voter(id: NodeId, addr: &str) -> MemberInfo {
        MemberInfo {
            id,
            addr: addr.into(),
            is_learner: false,
        }
    }

    // --- require_leader tests ---

    #[test]
    fn test_require_leader_ok_when_is_leader() {
        let raft = MockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]);
        assert!(require_leader(&raft, 1).is_ok());
    }

    #[test]
    fn test_require_leader_err_when_not_leader() {
        let raft = MockRaftHandle::new(
            Some(2),
            vec![voter(1, "127.0.0.1:2380"), voter(2, "127.0.0.2:2380")],
        );
        let err = require_leader(&raft, 1).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not leader"));
        // Should include leader address in metadata
        let leader_header = err.metadata().get("x-aether-leader");
        assert!(leader_header.is_some());
    }

    #[test]
    fn test_require_leader_err_when_no_leader() {
        let raft = MockRaftHandle::new(None, vec![]);
        let err = require_leader(&raft, 1).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("no leader"));
    }

    // --- read_index tests ---

    #[tokio::test]
    async fn test_read_index_returns_commit_index_when_leader() {
        let raft =
            MockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]).with_commit_index(42);
        let idx = raft.read_index().await.unwrap();
        assert_eq!(idx, 42);
    }

    #[tokio::test]
    async fn test_read_index_fails_when_no_leader() {
        let raft = MockRaftHandle::new(None, vec![]);
        let err = raft.read_index().await.unwrap_err();
        assert!(matches!(err, RaftError::NoLeader));
    }

    #[tokio::test]
    async fn test_read_index_fails_when_not_leader() {
        let raft = MockRaftHandle::new(None, vec![])
            .with_read_index_result(Err(RaftError::NotLeader { leader: Some(2) }));
        let err = raft.read_index().await.unwrap_err();
        assert!(matches!(err, RaftError::NotLeader { leader: Some(2) }));
    }

    // --- wait_for_apply tests ---

    #[tokio::test]
    async fn test_wait_for_apply_completes() {
        let raft = MockRaftHandle::new(Some(1), vec![]);
        // Mock is a no-op; just verify it doesn't hang or panic.
        raft.wait_for_apply().await;
    }

    // --- ensure_linearizable tests ---

    #[tokio::test]
    async fn test_ensure_linearizable_ok_when_leader() {
        let raft =
            MockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]).with_commit_index(10);
        let idx = ensure_linearizable(&raft).await.unwrap();
        assert_eq!(idx, 10);
    }

    #[tokio::test]
    async fn test_ensure_linearizable_err_when_no_leader() {
        let raft = MockRaftHandle::new(None, vec![]);
        let err = ensure_linearizable(&raft).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("no leader"));
    }

    #[tokio::test]
    async fn test_ensure_linearizable_err_when_not_leader_with_redirect() {
        let raft = MockRaftHandle::new(None, vec![])
            .with_read_index_result(Err(RaftError::NotLeader { leader: Some(2) }))
            .with_commit_index(0);
        // Need members so redirect metadata can be populated.
        let raft = MockRaftHandle {
            members: vec![voter(1, "127.0.0.1:2380"), voter(2, "127.0.0.2:2380")],
            ..raft
        };
        let err = ensure_linearizable(&raft).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not leader"));
        // Should include leader address in metadata for redirect.
        let leader_header = err.metadata().get("x-aether-leader");
        assert!(leader_header.is_some());
        assert_eq!(leader_header.unwrap().to_str().unwrap(), "127.0.0.2:2380");
    }

    #[tokio::test]
    async fn test_ensure_linearizable_err_when_not_leader_no_leader_id() {
        let raft = MockRaftHandle::new(None, vec![])
            .with_read_index_result(Err(RaftError::NotLeader { leader: None }));
        let err = ensure_linearizable(&raft).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("not leader"));
        // No leader ID → no redirect metadata.
        assert!(err.metadata().get("x-aether-leader").is_none());
    }

    #[tokio::test]
    async fn test_ensure_linearizable_err_when_channel_closed() {
        let raft =
            MockRaftHandle::new(None, vec![]).with_read_index_result(Err(RaftError::ChannelClosed));
        let err = ensure_linearizable(&raft).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("read index failed"));
    }

    // --- Full mock with learner support ---

    use std::sync::RwLock;

    struct FullMockRaftHandle {
        leader: Option<u64>,
        members: RwLock<Vec<MemberInfo>>,
    }

    impl FullMockRaftHandle {
        fn new(leader: Option<u64>, members: Vec<MemberInfo>) -> Self {
            Self {
                leader,
                members: RwLock::new(members),
            }
        }
    }

    #[async_trait::async_trait]
    impl RaftHandle for FullMockRaftHandle {
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
        async fn wait_for_apply(&self) {}
        async fn read_index(&self) -> Result<u64, RaftError> {
            Ok(0)
        }
        fn members(&self) -> Vec<MemberInfo> {
            self.members.read().unwrap().clone()
        }
        async fn add_learner(&self, id: u64, addr: String) -> Result<(), RaftError> {
            let mut members = self.members.write().unwrap();
            if members.iter().any(|m| m.id == id) {
                return Err(RaftError::Internal(format!("node {id} already exists")));
            }
            members.push(MemberInfo {
                id,
                addr,
                is_learner: true,
            });
            Ok(())
        }
        async fn remove_node(&self, id: u64) -> Result<(), RaftError> {
            self.members.write().unwrap().retain(|m| m.id != id);
            Ok(())
        }
        async fn change_membership(&self, voters: Vec<u64>) -> Result<(), RaftError> {
            let mut members = self.members.write().unwrap();
            let old_voters: Vec<u64> = members
                .iter()
                .filter(|m| !m.is_learner)
                .map(|m| m.id)
                .collect();
            let to_add: Vec<u64> = voters
                .iter()
                .filter(|id| !old_voters.contains(id))
                .copied()
                .collect();
            for node_id in to_add {
                if let Some(m) = members.iter_mut().find(|m| m.id == node_id) {
                    m.is_learner = false;
                } else {
                    members.push(MemberInfo {
                        id: node_id,
                        addr: String::new(),
                        is_learner: false,
                    });
                }
            }
            members.retain(|m| voters.contains(&m.id));
            Ok(())
        }
    }

    fn learner(id: NodeId, addr: &str) -> MemberInfo {
        MemberInfo {
            id,
            addr: addr.into(),
            is_learner: true,
        }
    }

    // --- learner lifecycle tests ---

    #[tokio::test]
    async fn test_add_learner_sets_is_learner() {
        let raft = FullMockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]);
        raft.add_learner(2, "127.0.0.2:2380".into()).await.unwrap();

        let members = raft.members();
        assert_eq!(members.len(), 2);
        let node2 = members.iter().find(|m| m.id == 2).unwrap();
        assert!(node2.is_learner);
    }

    #[tokio::test]
    async fn test_add_learner_rejects_duplicate() {
        let raft = FullMockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]);
        raft.add_learner(2, "127.0.0.2:2380".into()).await.unwrap();
        let err = raft
            .add_learner(2, "127.0.0.2:2380".into())
            .await
            .unwrap_err();
        assert!(matches!(err, RaftError::Internal(_)));
    }

    #[tokio::test]
    async fn test_promote_learner_to_voter() {
        let raft = FullMockRaftHandle::new(
            Some(1),
            vec![voter(1, "127.0.0.1:2380"), learner(2, "127.0.0.2:2380")],
        );
        // Promote: change_membership with both IDs.
        raft.change_membership(vec![1, 2]).await.unwrap();

        let members = raft.members();
        let node2 = members.iter().find(|m| m.id == 2).unwrap();
        assert!(!node2.is_learner);
    }

    #[tokio::test]
    async fn test_remove_learner() {
        let raft = FullMockRaftHandle::new(
            Some(1),
            vec![voter(1, "127.0.0.1:2380"), learner(2, "127.0.0.2:2380")],
        );
        raft.remove_node(2).await.unwrap();

        let members = raft.members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, 1);
    }

    #[tokio::test]
    async fn test_remove_voter() {
        let raft = FullMockRaftHandle::new(
            Some(1),
            vec![voter(1, "127.0.0.1:2380"), voter(2, "127.0.0.2:2380")],
        );
        raft.remove_node(2).await.unwrap();

        let members = raft.members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, 1);
    }

    #[tokio::test]
    async fn test_add_learner_then_remove() {
        let raft = FullMockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]);
        raft.add_learner(2, "127.0.0.2:2380".into()).await.unwrap();
        assert_eq!(raft.members().len(), 2);

        raft.remove_node(2).await.unwrap();
        assert_eq!(raft.members().len(), 1);
        assert!(raft.members().iter().all(|m| m.id != 2));
    }

    #[tokio::test]
    async fn test_add_learner_then_promote_then_remove() {
        let raft = FullMockRaftHandle::new(Some(1), vec![voter(1, "127.0.0.1:2380")]);

        // Add as learner.
        raft.add_learner(2, "127.0.0.2:2380".into()).await.unwrap();
        assert!(
            raft.members()
                .iter()
                .find(|m| m.id == 2)
                .unwrap()
                .is_learner
        );

        // Promote to voter.
        raft.change_membership(vec![1, 2]).await.unwrap();
        assert!(
            !raft
                .members()
                .iter()
                .find(|m| m.id == 2)
                .unwrap()
                .is_learner
        );

        // Remove.
        raft.remove_node(2).await.unwrap();
        assert_eq!(raft.members().len(), 1);
    }
}
