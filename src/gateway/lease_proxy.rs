use super::{BackendPool, extract_leader_redirect, forward_request};
use crate::proto::aether_lease_server::AetherLease;
use crate::proto::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct LeaseProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl LeaseProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<
        crate::proto::aether_lease_client::AetherLeaseClient<tonic::transport::Channel>,
        Status,
    > {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.lease.clone())
    }
}

#[tonic::async_trait]
impl AetherLease for LeaseProxy {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_lease()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.lease_grant(forward_request(&metadata, req)))
            .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.lease_grant(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_lease()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.lease_revoke(forward_request(&metadata, req)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.lease_revoke(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    type LeaseKeepAliveStream = tonic::Streaming<LeaseKeepAliveResponse>;

    async fn lease_keep_alive(
        &self,
        request: Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_lease()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        let mut client_stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            while let Ok(Some(msg)) = client_stream.message().await {
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        match tokio::time::timeout(timeout, client.lease_keep_alive(Request::new(stream))).await {
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

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_lease()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.lease_time_to_live(forward_request(&metadata, req)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(
                        timeout,
                        c.lease_time_to_live(forward_request(&metadata, req)),
                    )
                    .await
                    .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_lease()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.lease_leases(forward_request(&metadata, req)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.lease_leases(forward_request(&metadata, req)))
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
