use std::sync::Arc;

use async_trait::async_trait;
use raft::eraftpb::{ConfChange, ConfChangeType};
use std::sync::RwLock;
use tokio::sync::{mpsc, oneshot};

use super::handle::{RaftError, RaftHandle};
use super::node::{ConfChangeRequest, ProposeRequest, RaftSharedState};
use super::{NodeId, RaftRequest, RaftResponse};

/// raft-rs based implementation of RaftHandle.
///
/// Communicates with the event loop via channels.
pub struct RaftRsHandle {
    propose_tx: mpsc::Sender<ProposeRequest>,
    conf_change_tx: mpsc::Sender<ConfChangeRequest>,
    shared_state: Arc<RaftSharedState>,
    // Uses std::sync::RwLock (not tokio::sync::RwLock) because members() is a
    // sync method on the trait, and write operations (add_learner, change_membership)
    // only hold the lock during synchronous mutations — no .await while guarded.
    members: RwLock<Vec<(u64, String)>>,
}

impl RaftRsHandle {
    pub fn new(
        propose_tx: mpsc::Sender<ProposeRequest>,
        conf_change_tx: mpsc::Sender<ConfChangeRequest>,
        shared_state: Arc<RaftSharedState>,
        members: Vec<(u64, String)>,
    ) -> Self {
        Self {
            propose_tx,
            conf_change_tx,
            shared_state,
            members: RwLock::new(members),
        }
    }
}

#[async_trait]
impl RaftHandle for RaftRsHandle {
    async fn propose(&self, request: RaftRequest) -> Result<RaftResponse, RaftError> {
        let data = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&request)
            .map(|b| b.into_vec())
            .map_err(|e| RaftError::Internal(format!("serialize failed: {e}")))?;

        let (tx, rx) = oneshot::channel();
        self.propose_tx
            .send(ProposeRequest { data, tx })
            .await
            .map_err(|_| RaftError::ChannelClosed)?;

        let resp_data = rx.await.map_err(|_| RaftError::ChannelClosed)??;

        let resp = rkyv::from_bytes::<RaftResponse, rkyv::rancor::BoxedError>(&resp_data)
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

    fn members(&self) -> Vec<(u64, String)> {
        self.members.read().unwrap().clone()
    }

    async fn add_learner(&self, id: u64, addr: String) -> Result<(), RaftError> {
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
        self.members.write().unwrap().push((id, addr));
        Ok(())
    }

    async fn change_membership(&self, voters: Vec<u64>) -> Result<(), RaftError> {
        let old_voters: Vec<u64> = self
            .members
            .read()
            .unwrap()
            .iter()
            .map(|(id, _)| *id)
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

        // Update local member list: add new voters, remove old ones.
        let mut members = self.members.write().unwrap();
        for node_id in to_add {
            if !members.iter().any(|(id, _)| *id == node_id) {
                members.push((node_id, String::new()));
            }
        }
        members.retain(|(id, _)| voters.contains(id));
        Ok(())
    }
}
