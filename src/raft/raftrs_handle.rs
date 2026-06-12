use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use raft::eraftpb::{ConfChange, ConfChangeType};
use std::sync::RwLock;
use tokio::sync::{mpsc, oneshot};

use super::handle::{MemberInfo, RaftError, RaftHandle};
use super::node::{ConfChangeRequest, ProposeRequest, RaftSharedState, ReadIndexRequest};
use super::{NodeId, RaftRequest, RaftResponse};

/// raft-rs based implementation of RaftHandle.
///
/// Communicates with the event loop via channels.
pub struct RaftRsHandle {
    propose_tx: mpsc::Sender<ProposeRequest>,
    conf_change_tx: mpsc::Sender<ConfChangeRequest>,
    read_index_tx: mpsc::Sender<ReadIndexRequest>,
    shared_state: Arc<RaftSharedState>,
    // Uses std::sync::RwLock (not tokio::sync::RwLock) because members() is a
    // sync method on the trait, and write operations (add_learner, change_membership)
    // only hold the lock during synchronous mutations — no .await while guarded.
    members: RwLock<Vec<MemberInfo>>,
    read_index_counter: AtomicU64,
}

impl RaftRsHandle {
    pub fn new(
        propose_tx: mpsc::Sender<ProposeRequest>,
        conf_change_tx: mpsc::Sender<ConfChangeRequest>,
        read_index_tx: mpsc::Sender<ReadIndexRequest>,
        shared_state: Arc<RaftSharedState>,
        initial_peers: Vec<(u64, String)>,
    ) -> Self {
        // Bootstrap peers are all voters.
        let members = initial_peers
            .into_iter()
            .map(|(id, addr)| MemberInfo {
                id,
                addr,
                is_learner: false,
            })
            .collect();
        Self {
            propose_tx,
            conf_change_tx,
            read_index_tx,
            shared_state,
            members: RwLock::new(members),
            read_index_counter: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl RaftHandle for RaftRsHandle {
    async fn propose(&self, request: RaftRequest) -> Result<RaftResponse, RaftError> {
        let data = bincode::serialize(&request)
            .map_err(|e| RaftError::Internal(format!("serialize failed: {e}")))?;

        let (tx, rx) = oneshot::channel();
        self.propose_tx
            .send(ProposeRequest { data, tx })
            .await
            .map_err(|_| RaftError::ChannelClosed)?;

        let resp_data = rx.await.map_err(|_| RaftError::ChannelClosed)??;

        let resp = bincode::deserialize(&resp_data)
            .map_err(|e| RaftError::Internal(format!("deserialize failed: {e}")))?;

        if let RaftResponse::Error { message } = &resp {
            return Err(RaftError::Internal(message.clone()));
        }

        Ok(resp)
    }

    fn leader_id(&self) -> Option<NodeId> {
        let id = self
            .shared_state
            .leader_id
            .load(std::sync::atomic::Ordering::Relaxed);
        if id == 0 { None } else { Some(id) }
    }

    fn commit_index(&self) -> u64 {
        self.shared_state
            .commit_index
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn applied_index(&self) -> u64 {
        self.shared_state
            .applied_index
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn term(&self) -> u64 {
        self.shared_state
            .term
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn wait_for_apply(&self) {
        self.shared_state.applied_notify.notified().await;
    }

    async fn read_index(&self) -> Result<u64, RaftError> {
        let id = self.read_index_counter.fetch_add(1, Ordering::Relaxed);
        let context = id.to_be_bytes().to_vec();
        let (tx, rx) = oneshot::channel();
        self.read_index_tx
            .send(ReadIndexRequest { context, tx })
            .await
            .map_err(|_| RaftError::ChannelClosed)?;

        rx.await.map_err(|_| RaftError::ChannelClosed)?
    }

    fn members(&self) -> Vec<MemberInfo> {
        self.members.read().unwrap().clone()
    }

    async fn add_learner(&self, id: u64, addr: String) -> Result<(), RaftError> {
        // Reject if the node is already a member (voter or learner).
        {
            let members = self.members.read().unwrap();
            if members.iter().any(|m| m.id == id) {
                return Err(RaftError::Internal(format!(
                    "node {id} is already a cluster member"
                )));
            }
        }

        let mut cc = ConfChange::default();
        cc.set_change_type(ConfChangeType::AddLearnerNode);
        cc.node_id = id;

        let (tx, rx) = oneshot::channel();
        self.conf_change_tx
            .send(ConfChangeRequest { cc, tx })
            .await
            .map_err(|_| RaftError::ChannelClosed)?;

        rx.await.map_err(|_| RaftError::ChannelClosed)??;

        // Update local member list so require_leader can find the new node's address.
        self.members.write().unwrap().push(MemberInfo {
            id,
            addr,
            is_learner: true,
        });
        Ok(())
    }

    async fn remove_node(&self, id: u64) -> Result<(), RaftError> {
        let mut cc = ConfChange::default();
        cc.set_change_type(ConfChangeType::RemoveNode);
        cc.node_id = id;

        let (tx, rx) = oneshot::channel();
        self.conf_change_tx
            .send(ConfChangeRequest { cc, tx })
            .await
            .map_err(|_| RaftError::ChannelClosed)?;

        rx.await.map_err(|_| RaftError::ChannelClosed)??;

        // Remove from local member list.
        self.members.write().unwrap().retain(|m| m.id != id);
        Ok(())
    }

    async fn change_membership(&self, voters: Vec<u64>) -> Result<(), RaftError> {
        let old_voters: Vec<u64> = self
            .members
            .read()
            .unwrap()
            .iter()
            .filter(|m| !m.is_learner)
            .map(|m| m.id)
            .collect();

        let to_add: Vec<u64> = voters
            .iter()
            .filter(|id| !old_voters.contains(id))
            .copied()
            .collect();
        let to_remove: Vec<u64> = old_voters
            .into_iter()
            .filter(|id| !voters.contains(id))
            .collect();

        // Apply changes one at a time (Raft requires serial config changes).
        for node_id in &to_add {
            let mut cc = ConfChange::default();
            cc.set_change_type(ConfChangeType::AddNode);
            cc.node_id = *node_id;

            let (tx, rx) = oneshot::channel();
            self.conf_change_tx
                .send(ConfChangeRequest { cc, tx })
                .await
                .map_err(|_| RaftError::ChannelClosed)?;

            rx.await.map_err(|_| RaftError::ChannelClosed)??;
        }
        for node_id in &to_remove {
            let mut cc = ConfChange::default();
            cc.set_change_type(ConfChangeType::RemoveNode);
            cc.node_id = *node_id;

            let (tx, rx) = oneshot::channel();
            self.conf_change_tx
                .send(ConfChangeRequest { cc, tx })
                .await
                .map_err(|_| RaftError::ChannelClosed)?;

            rx.await.map_err(|_| RaftError::ChannelClosed)??;
        }

        // Update local member list: add new voters, promote learners, remove old ones.
        let mut members = self.members.write().unwrap();
        for node_id in to_add {
            if let Some(m) = members.iter_mut().find(|m| m.id == node_id) {
                // Promote existing learner to voter.
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
