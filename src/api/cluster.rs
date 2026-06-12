use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::{Request, Response, Status};

use crate::proto::aether_cluster_server::AetherCluster;
use crate::proto::{self as pb, ResponseHeader};
use crate::raft::{NodeId, RaftHandle, require_leader};

/// Cluster membership service
pub struct ClusterService {
    raft: Arc<dyn RaftHandle>,
    node_id: NodeId,
    auth_enabled: Arc<AtomicBool>,
}

impl ClusterService {
    pub fn new(raft: Arc<dyn RaftHandle>, node_id: NodeId, auth_enabled: Arc<AtomicBool>) -> Self {
        Self {
            raft,
            node_id,
            auth_enabled,
        }
    }

    /// Require root user for admin operations when auth is enabled
    fn require_root(
        req: &Request<impl std::fmt::Debug>,
        auth_enabled: &AtomicBool,
    ) -> Result<(), Status> {
        if !auth_enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let username = req
            .extensions()
            .get::<String>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("no user in context"))?;
        if username != "root" {
            return Err(Status::permission_denied("root user required"));
        }
        Ok(())
    }

    fn header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: 0,
            member_id: self.node_id,
            revision: 0,
            raft_term: 0,
        }
    }

    fn list_members(&self) -> Vec<pb::Member> {
        self.raft
            .members()
            .into_iter()
            .map(|m| pb::Member {
                id: m.id,
                addr: m.addr,
                is_learner: m.is_learner,
                data: String::new(),
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl AetherCluster for ClusterService {
    async fn member_list(
        &self,
        request: Request<pb::MemberListRequest>,
    ) -> Result<Response<pb::MemberListResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
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
        Self::require_root(&request, &self.auth_enabled)?;
        require_leader(self.raft.as_ref(), self.node_id)?;

        let req = request.into_inner();
        if req.addr.is_empty() {
            return Err(Status::invalid_argument("addr must not be empty"));
        }

        // Generate a new node ID: max existing ID + 1
        let new_id = self.raft.members().iter().map(|m| m.id).max().unwrap_or(0) + 1;

        // Add as learner first
        self.raft
            .add_learner(new_id, req.addr.clone())
            .await
            .map_err(|e| Status::internal(format!("add_learner failed: {e}")))?;

        // Promote to voter if not learner
        if !req.is_learner {
            let mut voters: Vec<u64> = self.raft.members().iter().map(|m| m.id).collect();
            voters.push(new_id);
            self.raft
                .change_membership(voters)
                .await
                .map_err(|e| Status::internal(format!("change_membership failed: {e}")))?;
        }

        let members = self.list_members();
        let member = pb::Member {
            id: new_id,
            addr: req.addr,
            is_learner: req.is_learner,
            data: String::new(),
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
        Self::require_root(&request, &self.auth_enabled)?;
        require_leader(self.raft.as_ref(), self.node_id)?;

        let req = request.into_inner();
        if req.id == 0 {
            return Err(Status::invalid_argument("member id must not be 0"));
        }

        if req.id == self.node_id {
            return Err(Status::invalid_argument("cannot remove self"));
        }

        // Check whether the target is a learner or voter.
        let is_learner = self
            .raft
            .members()
            .iter()
            .find(|m| m.id == req.id)
            .map(|m| m.is_learner)
            .ok_or_else(|| Status::not_found(format!("member {} not found", req.id)))?;

        if is_learner {
            // Learners are not in the voter set — remove directly via ConfChange.
            self.raft
                .remove_node(req.id)
                .await
                .map_err(|e| Status::internal(format!("remove_node failed: {e}")))?;
        } else {
            // Remove voter by setting voter list to all current voters minus the target.
            let voters: Vec<u64> = self
                .raft
                .members()
                .iter()
                .filter(|m| m.id != req.id && !m.is_learner)
                .map(|m| m.id)
                .collect();
            self.raft
                .change_membership(voters)
                .await
                .map_err(|e| Status::internal(format!("change_membership failed: {e}")))?;
        }

        let members = self.list_members();
        Ok(Response::new(pb::MemberRemoveResponse {
            header: Some(self.header()),
            members,
        }))
    }

    async fn member_promote(
        &self,
        request: Request<pb::MemberPromoteRequest>,
    ) -> Result<Response<pb::MemberPromoteResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
        require_leader(self.raft.as_ref(), self.node_id)?;

        let req = request.into_inner();
        if req.id == 0 {
            return Err(Status::invalid_argument("member id must not be 0"));
        }

        // Verify the target is a learner.
        let members = self.raft.members();
        let target = members
            .iter()
            .find(|m| m.id == req.id)
            .ok_or_else(|| Status::not_found(format!("member {} not found", req.id)))?;
        if !target.is_learner {
            return Err(Status::failed_precondition(format!(
                "member {} is already a voter",
                req.id
            )));
        }

        // Build voter list: current voters + the learner being promoted.
        let mut voters: Vec<u64> = members
            .iter()
            .filter(|m| !m.is_learner)
            .map(|m| m.id)
            .collect();
        voters.push(req.id);

        self.raft
            .change_membership(voters)
            .await
            .map_err(|e| Status::internal(format!("change_membership failed: {e}")))?;

        let members = self.list_members();
        Ok(Response::new(pb::MemberPromoteResponse {
            header: Some(self.header()),
            members,
        }))
    }
}
