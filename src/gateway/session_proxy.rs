use super::{BackendPool, extract_leader_redirect, forward_request};
use crate::proto::aether_session_server::AetherSession;
use crate::proto::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct SessionProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl SessionProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<
        crate::proto::aether_session_client::AetherSessionClient<tonic::transport::Channel>,
        Status,
    > {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.session.clone())
    }
}

#[tonic::async_trait]
impl AetherSession for SessionProxy {
    async fn create(
        &self,
        request: Request<SessionCreateRequest>,
    ) -> Result<Response<SessionCreateResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_session()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.create(forward_request(&metadata, req))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.create(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    async fn close(
        &self,
        request: Request<SessionCloseRequest>,
    ) -> Result<Response<SessionCloseResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_session()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.close(forward_request(&metadata, req))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.close(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    type KeepAliveStream = tonic::Streaming<SessionKeepAliveResponse>;

    async fn keep_alive(
        &self,
        request: Request<tonic::Streaming<SessionKeepAliveRequest>>,
    ) -> Result<Response<Self::KeepAliveStream>, Status> {
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_session()
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
        match tokio::time::timeout(timeout, client.keep_alive(Request::new(stream))).await {
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

    async fn query(
        &self,
        request: Request<SessionQueryRequest>,
    ) -> Result<Response<SessionQueryResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_any_session()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(timeout, client.query(forward_request(&metadata, req))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.query(forward_request(&metadata, req)))
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
