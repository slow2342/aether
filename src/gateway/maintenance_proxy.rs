use super::{BackendPool, extract_leader_redirect, forward_request};
use crate::proto::aether_maintenance_server::AetherMaintenance;
use crate::proto::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct MaintenanceProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl MaintenanceProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<
        crate::proto::aether_maintenance_client::AetherMaintenanceClient<tonic::transport::Channel>,
        Status,
    > {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.maintenance.clone())
    }
}

#[tonic::async_trait]
impl AetherMaintenance for MaintenanceProxy {
    async fn defrag(
        &self,
        request: Request<DefragRequest>,
    ) -> Result<Response<DefragResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_maintenance()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.defrag(forward_request(&metadata, req))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.defrag(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn alarm(
        &self,
        request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_maintenance()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.alarm(forward_request(&metadata, req))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.alarm(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_maintenance()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.status(forward_request(&metadata, req))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.status(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }
}
