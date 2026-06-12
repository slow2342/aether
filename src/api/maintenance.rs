use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::{Request, Response, Status};

use crate::cluster::{AlarmManager, AlarmType};
use crate::proto::aether_maintenance_server::AetherMaintenance;
use crate::proto::{self as pb, AlarmAction, ResponseHeader};
use crate::raft::{NodeId, RaftHandle};
use crate::storage::RocksStorage;

const AETHER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maintenance service providing defrag, alarm, and status operations.
pub struct MaintenanceService {
    raft: Arc<dyn RaftHandle>,
    storage: Arc<RocksStorage>,
    alarm_manager: Arc<AlarmManager>,
    node_id: NodeId,
    auth_enabled: Arc<AtomicBool>,
}

impl MaintenanceService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        storage: Arc<RocksStorage>,
        alarm_manager: Arc<AlarmManager>,
        node_id: NodeId,
        auth_enabled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            raft,
            storage,
            alarm_manager,
            node_id,
            auth_enabled,
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
            raft_term: self.raft.term(),
        }
    }
}

#[async_trait::async_trait]
impl AetherMaintenance for MaintenanceService {
    async fn defrag(
        &self,
        request: Request<pb::DefragRequest>,
    ) -> Result<Response<pb::DefragResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;

        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || storage.defrag())
            .await
            .map_err(|e| Status::internal(format!("defrag task failed: {e}")))?
            .map_err(|e| Status::internal(format!("defrag failed: {e}")))?;

        Ok(Response::new(pb::DefragResponse {
            header: Some(self.header()),
        }))
    }

    async fn alarm(
        &self,
        request: Request<pb::AlarmRequest>,
    ) -> Result<Response<pb::AlarmResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;

        let req = request.into_inner();
        let action = AlarmAction::try_from(req.action)
            .map_err(|_| Status::invalid_argument("invalid alarm action"))?;
        let alarm_type = AlarmType::from_proto(req.alarm)
            .ok_or_else(|| Status::invalid_argument("invalid alarm type"))?;

        match action {
            AlarmAction::Get => {
                let alarms: Vec<pb::AlarmMember> = self
                    .alarm_manager
                    .get_all()
                    .into_iter()
                    .map(|(member_id, alarm_type)| pb::AlarmMember {
                        member_id,
                        alarm: alarm_type.to_proto(),
                    })
                    .collect();

                Ok(Response::new(pb::AlarmResponse {
                    header: Some(self.header()),
                    alarms,
                }))
            }
            AlarmAction::Activate => {
                if alarm_type == AlarmType::None {
                    return Err(Status::invalid_argument("cannot activate ALARM_TYPE_NONE"));
                }
                self.alarm_manager.activate(self.node_id, alarm_type);

                Ok(Response::new(pb::AlarmResponse {
                    header: Some(self.header()),
                    alarms: vec![pb::AlarmMember {
                        member_id: self.node_id,
                        alarm: alarm_type.to_proto(),
                    }],
                }))
            }
            AlarmAction::Acknowledge => {
                if alarm_type == AlarmType::None {
                    return Err(Status::invalid_argument(
                        "cannot acknowledge ALARM_TYPE_NONE",
                    ));
                }
                let member_id = if req.member_id != 0 {
                    req.member_id
                } else {
                    self.node_id
                };
                self.alarm_manager.acknowledge(member_id, alarm_type);

                Ok(Response::new(pb::AlarmResponse {
                    header: Some(self.header()),
                    alarms: vec![],
                }))
            }
        }
    }

    async fn status(
        &self,
        request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;

        let leader = self.raft.leader_id().unwrap_or(0);
        let raft_index = self.raft.commit_index();
        let raft_term = self.raft.term();
        let raft_applied_index = self.raft.applied_index();
        let db_size = self.storage.approximate_size();
        let db_in_use = self.storage.approximate_mem_usage();
        let is_learner = self
            .raft
            .members()
            .iter()
            .any(|m| m.id == self.node_id && m.is_learner);

        Ok(Response::new(pb::StatusResponse {
            header: Some(self.header()),
            version: AETHER_VERSION.to_string(),
            db_size,
            leader,
            raft_index,
            raft_term,
            raft_applied_index,
            db_in_use,
            is_learner,
        }))
    }
}
