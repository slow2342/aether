use super::{BackendPool, extract_leader_redirect, forward_request};
use crate::proto::aether_lock_server::AetherLock;
use crate::proto::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct LockProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl LockProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<crate::proto::aether_lock_client::AetherLockClient<tonic::transport::Channel>, Status>
    {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.lock.clone())
    }
}

#[tonic::async_trait]
impl AetherLock for LockProxy {
    async fn lock(&self, request: Request<LockRequest>) -> Result<Response<LockResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_lock()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.lock(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.lock(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_lock()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.unlock(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.unlock(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn lock_query(
        &self,
        request: Request<LockQueryRequest>,
    ) -> Result<Response<LockQueryResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_lock()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.lock_query(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.lock_query(forward_request(&metadata, req)))
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
