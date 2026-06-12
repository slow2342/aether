use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::proto::aether_shard_server::AetherShard;
use crate::proto::{self as pb, ResponseHeader};
use crate::raft::{NodeId, RaftHandle, require_leader};
use crate::shard::Region;
use crate::shard::manager::ShardManager;

/// Shard management gRPC service.
pub struct ShardService {
    raft: Arc<dyn RaftHandle>,
    node_id: NodeId,
    auth_enabled: Arc<AtomicBool>,
    shard_manager: Arc<Mutex<ShardManager>>,
}

impl ShardService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: NodeId,
        auth_enabled: Arc<AtomicBool>,
        shard_manager: Arc<Mutex<ShardManager>>,
    ) -> Self {
        Self {
            raft,
            node_id,
            auth_enabled,
            shard_manager,
        }
    }

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
}

fn region_to_proto(r: &Region) -> pb::Region {
    pb::Region {
        id: r.id,
        start_key: r.start_key.clone(),
        end_key: r.end_key.clone(),
        region_epoch: Some(pb::RegionEpoch {
            conf_ver: r.region_epoch.conf_ver,
            version: r.region_epoch.version,
        }),
        leader: r.leader,
        replicas: r.replicas.clone(),
    }
}

#[async_trait::async_trait]
impl AetherShard for ShardService {
    async fn get_region(
        &self,
        request: Request<pb::GetRegionRequest>,
    ) -> Result<Response<pb::GetRegionResponse>, Status> {
        let req = request.into_inner();
        let mgr = self.shard_manager.lock().unwrap();
        let region = mgr
            .find_region(&req.key)
            .ok_or_else(|| Status::not_found("no region found for key"))?;

        Ok(Response::new(pb::GetRegionResponse {
            header: Some(self.header()),
            region: Some(region_to_proto(region)),
        }))
    }

    async fn list_regions(
        &self,
        _request: Request<pb::ListRegionsRequest>,
    ) -> Result<Response<pb::ListRegionsResponse>, Status> {
        let mgr = self.shard_manager.lock().unwrap();
        let regions = mgr
            .list_regions()
            .into_iter()
            .map(region_to_proto)
            .collect();

        Ok(Response::new(pb::ListRegionsResponse {
            header: Some(self.header()),
            regions,
        }))
    }

    async fn split_region(
        &self,
        request: Request<pb::SplitRegionRequest>,
    ) -> Result<Response<pb::SplitRegionResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
        require_leader(self.raft.as_ref(), self.node_id)?;

        let req = request.into_inner();
        if req.region_id == 0 {
            return Err(Status::invalid_argument("region_id must not be 0"));
        }
        if req.split_key.is_empty() {
            return Err(Status::invalid_argument("split_key must not be empty"));
        }

        let response = self
            .raft
            .propose(crate::raft::RaftRequest::RegionSplit {
                region_id: req.region_id,
                split_key: req.split_key,
            })
            .await
            .map_err(|e| Status::internal(format!("propose failed: {e}")))?;

        match response {
            crate::raft::RaftResponse::RegionSplit { parent, child } => {
                Ok(Response::new(pb::SplitRegionResponse {
                    header: Some(self.header()),
                    parent: Some(region_to_proto(&parent)),
                    child: Some(region_to_proto(&child)),
                }))
            }
            crate::raft::RaftResponse::Error { message } => {
                Err(Status::failed_precondition(message))
            }
            _ => Err(Status::internal("unexpected response type")),
        }
    }
}
