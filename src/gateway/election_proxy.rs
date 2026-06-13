use super::{BackendPool, extract_leader_redirect, forward_request};
use crate::proto::aether_election_server::AetherElection;
use crate::proto::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct ElectionProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl ElectionProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<
        crate::proto::aether_election_client::AetherElectionClient<tonic::transport::Channel>,
        Status,
    > {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.election.clone())
    }
}

#[tonic::async_trait]
impl AetherElection for ElectionProxy {
    async fn campaign(
        &self,
        request: Request<CampaignRequest>,
    ) -> Result<Response<CampaignResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_election()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.campaign(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.campaign(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn leader(
        &self,
        request: Request<LeaderRequest>,
    ) -> Result<Response<LeaderResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_election()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.leader(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.leader(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn resign(
        &self,
        request: Request<ResignRequest>,
    ) -> Result<Response<ResignResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_election()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.resign(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.resign(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    type ObserveStream = tonic::Streaming<ObserveResponse>;

    async fn observe(
        &self,
        request: Request<ObserveRequest>,
    ) -> Result<Response<Self::ObserveStream>, Status> {
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_election()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        let req = request.into_inner();
        match tokio::time::timeout(timeout, client.observe(Request::new(req.clone()))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    Err(Status::unavailable(format!(
                        "leader redirect to {leader}, please retry"
                    )))
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }
}
