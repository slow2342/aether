use super::{BackendPool, extract_leader_redirect};
use crate::proto::aether_watch_server::AetherWatch;
use crate::proto::{WatchRequest, WatchResponse};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct WatchProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl WatchProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<
        crate::proto::aether_watch_client::AetherWatchClient<tonic::transport::Channel>,
        Status,
    > {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.watch.clone())
    }
}

#[tonic::async_trait]
impl AetherWatch for WatchProxy {
    type WatchStream = tonic::Streaming<WatchResponse>;

    async fn watch(
        &self,
        request: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (timeout, _addr, mut client) = {
            let p = self.pool.read().await;
            let c = p
                .get_leader_watch()
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
        match tokio::time::timeout(timeout, client.watch(Request::new(stream))).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let _ = self.redirect_and_cache(&leader).await;
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
