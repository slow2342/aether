use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::ChangeMembers;
use openraft::Raft;
use tonic::{Request, Response, Status};

use super::super::raft::{NodeId, RaftNode, TypeConfig};
use crate::proto::aether_cluster_server::AetherCluster;
use crate::proto::{self as pb, ResponseHeader};

/// Cluster membership service
pub struct ClusterService {
    raft: Arc<Raft<TypeConfig>>,
    node_id: NodeId,
}

impl ClusterService {
    pub fn new(raft: Arc<Raft<TypeConfig>>, node_id: NodeId) -> Self {
        Self { raft, node_id }
    }

    fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: 0,
            member_id: self.node_id,
            revision: 0,
            raft_term: 0,
        }
    }

    /// Returns `Ok(())` if this node is the leader, `Err` with leader info otherwise.
    fn require_leader(&self) -> Result<(), Status> {
        let rx = self.raft.metrics();
        let metrics = rx.borrow();

        match metrics.current_leader {
            Some(id) if id == self.node_id => Ok(()),
            Some(_) => {
                let leader_addr = metrics.current_leader.and_then(|id| {
                    metrics
                        .membership_config
                        .membership()
                        .get_node(&id)
                        .map(|n| n.addr.clone())
                });
                drop(metrics);

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

    fn list_members(&self) -> Vec<pb::Member> {
        let rx = self.raft.metrics();
        let metrics = rx.borrow();
        let membership = metrics.membership_config.membership();
        let voter_ids: BTreeSet<NodeId> = membership.voter_ids().collect();
        let mut members = Vec::new();

        for (id, node) in membership.nodes() {
            members.push(pb::Member {
                id: *id,
                addr: node.addr.clone(),
                is_learner: !voter_ids.contains(id),
                data: node.data.clone(),
            });
        }

        members
    }
}

#[async_trait::async_trait]
impl AetherCluster for ClusterService {
    async fn member_list(
        &self,
        _request: Request<pb::MemberListRequest>,
    ) -> Result<Response<pb::MemberListResponse>, Status> {
        let members = self.list_members();
        Ok(Response::new(pb::MemberListResponse {
            header: Some(self.header()),
            members,
        }))
    }

    async fn member_add(
        &self,
        request: Request<pb::MemberAddRequest>,
    ) -> Result<Response<pb::MemberAddResponse>, Status> {
        self.require_leader()?;

        let req = request.into_inner();
        if req.addr.is_empty() {
            return Err(Status::invalid_argument("addr must not be empty"));
        }

        // Generate a new node ID: max existing ID + 1
        let new_id = {
            let rx = self.raft.metrics();
            let metrics = rx.borrow();
            let membership = metrics.membership_config.membership();
            membership.nodes().map(|(id, _)| *id).max().unwrap_or(0) + 1
        };

        let node = RaftNode {
            addr: req.addr,
            data: String::new(),
        };

        // Add as learner first
        self.raft
            .add_learner(new_id, node.clone(), false)
            .await
            .map_err(|e| Status::internal(format!("add_learner failed: {e}")))?;

        // Promote to voter if not learner
        if !req.is_learner {
            let mut voters = BTreeSet::new();
            voters.insert(new_id);
            self.raft
                .change_membership(ChangeMembers::AddVoterIds(voters), true)
                .await
                .map_err(|e| Status::internal(format!("change_membership failed: {e}")))?;
        }

        let members = self.list_members();
        let member = pb::Member {
            id: new_id,
            addr: node.addr,
            is_learner: req.is_learner,
            data: node.data,
        };

        Ok(Response::new(pb::MemberAddResponse {
            header: Some(self.header()),
            member: Some(member),
            members,
        }))
    }

    async fn member_remove(
        &self,
        request: Request<pb::MemberRemoveRequest>,
    ) -> Result<Response<pb::MemberRemoveResponse>, Status> {
        self.require_leader()?;

        let req = request.into_inner();
        if req.id == 0 {
            return Err(Status::invalid_argument("member id must not be 0"));
        }

        if req.id == self.node_id {
            return Err(Status::invalid_argument("cannot remove self"));
        }

        // Remove from voters first (RemoveNodes requires the node to not be a voter)
        let mut remove_voters = BTreeSet::new();
        remove_voters.insert(req.id);
        self.raft
            .change_membership(ChangeMembers::RemoveVoters(remove_voters), false)
            .await
            .map_err(|e| Status::internal(format!("change_membership failed: {e}")))?;

        // Then remove from cluster entirely
        let mut remove_nodes = BTreeSet::new();
        remove_nodes.insert(req.id);
        if let Err(e) = self
            .raft
            .change_membership(ChangeMembers::RemoveNodes(remove_nodes), false)
            .await
        {
            tracing::warn!(
                node_id = req.id,
                error = %e,
                "RemoveNodes failed after RemoveVoters succeeded; \
                 node is now a learner. Retry member_remove to complete removal."
            );
            return Err(Status::internal(format!("remove node failed: {e}")));
        }

        let members = self.list_members();
        Ok(Response::new(pb::MemberRemoveResponse {
            header: Some(self.header()),
            members,
        }))
    }
}
