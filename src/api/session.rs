use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::lease::{LeaseManager, now_millis};
use crate::proto::aether_session_server::AetherSession;
use crate::proto::{
    ResponseHeader, SessionCloseRequest, SessionCloseResponse, SessionCreateRequest,
    SessionCreateResponse, SessionKeepAliveRequest, SessionKeepAliveResponse, SessionQueryRequest,
    SessionQueryResponse,
};
use crate::raft::{self, RaftHandle, require_leader};
use crate::session::SessionManager;

pub struct SessionService {
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    lease_manager: Arc<Mutex<LeaseManager>>,
    session_manager: Arc<Mutex<SessionManager>>,
    max_ttl: i64,
    auth_enabled: Arc<AtomicBool>,
}

impl SessionService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        lease_manager: Arc<Mutex<LeaseManager>>,
        session_manager: Arc<Mutex<SessionManager>>,
        max_ttl: i64,
        auth_enabled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            raft,
            node_id,
            lease_manager,
            session_manager,
            max_ttl,
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

    async fn propose(&self, request: raft::RaftRequest) -> Result<raft::RaftResponse, Status> {
        require_leader(self.raft.as_ref(), self.node_id)?;
        self.raft
            .propose(request)
            .await
            .map_err(|e| Status::internal(format!("raft write failed: {e}")))
    }
}

#[tonic::async_trait]
impl AetherSession for SessionService {
    async fn create(
        &self,
        request: Request<SessionCreateRequest>,
    ) -> Result<Response<SessionCreateResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
        let req = request.into_inner();

        // Negotiate TTL: clamp to [1, max_ttl]
        let ttl = req.ttl.clamp(1, self.max_ttl);
        if ttl != req.ttl {
            tracing::info!(requested = req.ttl, granted = ttl, "session TTL negotiated");
        }

        // Compute expiry_time on the leader for deterministic state machine apply
        let expiry_time = now_millis() + ttl * 1000;

        // Create session via lease grant
        let resp = self
            .propose(raft::RaftRequest::LeaseGrant { ttl, expiry_time })
            .await?;

        match resp {
            raft::RaftResponse::LeaseGrant { id, ttl } => {
                // Register session in session manager
                {
                    let mut mgr = self.session_manager.lock().map_err(|e| {
                        Status::internal(format!("session manager lock poisoned: {e}"))
                    })?;
                    mgr.create(id);
                }

                Ok(Response::new(SessionCreateResponse {
                    header: Some(self.header()),
                    id,
                    granted_ttl: ttl,
                }))
            }
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    async fn close(
        &self,
        request: Request<SessionCloseRequest>,
    ) -> Result<Response<SessionCloseResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
        let req = request.into_inner();

        if req.id <= 0 {
            return Err(Status::invalid_argument("session ID must be positive"));
        }

        // Verify session exists before attempting revoke
        {
            let mgr = self
                .session_manager
                .lock()
                .map_err(|e| Status::internal(format!("session manager lock poisoned: {e}")))?;
            if !mgr.exists(req.id) {
                return Err(Status::not_found(format!("session not found: {}", req.id)));
            }
        }

        // Revoke the underlying lease (deletes all attached keys)
        let resp = self
            .propose(raft::RaftRequest::LeaseRevoke { id: req.id })
            .await?;

        match resp {
            raft::RaftResponse::LeaseRevoke {} => {
                // Remove from session manager after successful revoke
                {
                    let mut mgr = self.session_manager.lock().map_err(|e| {
                        Status::internal(format!("session manager lock poisoned: {e}"))
                    })?;
                    let _ = mgr.close(req.id);
                }

                Ok(Response::new(SessionCloseResponse {
                    header: Some(self.header()),
                }))
            }
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response type")),
        }
    }

    type KeepAliveStream = ReceiverStream<Result<SessionKeepAliveResponse, Status>>;

    async fn keep_alive(
        &self,
        request: Request<Streaming<SessionKeepAliveRequest>>,
    ) -> Result<Response<Self::KeepAliveStream>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
        require_leader(self.raft.as_ref(), self.node_id)?;

        let raft = self.raft.clone();
        let header = self.header();
        let lease_manager = self.lease_manager.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let mut stream = request.into_inner();

        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                if req.id <= 0 {
                    let _ = tx
                        .send(Err(Status::invalid_argument("session ID must be positive")))
                        .await;
                    continue;
                }

                // Look up granted_ttl from lease manager (source of truth)
                let granted_ttl = lease_manager
                    .lock()
                    .ok()
                    .and_then(|mgr| mgr.get(req.id).map(|l| l.granted_ttl));
                let expiry_time = match granted_ttl {
                    Some(ttl) if ttl > 0 => now_millis() + ttl * 1000,
                    _ => {
                        let _ = tx
                            .send(Err(Status::not_found(format!(
                                "session not found: {}",
                                req.id
                            ))))
                            .await;
                        continue;
                    }
                };

                let resp = raft
                    .propose(raft::RaftRequest::LeaseKeepAlive {
                        id: req.id,
                        expiry_time,
                    })
                    .await;

                match resp {
                    Ok(raft::RaftResponse::LeaseKeepAlive { ttl }) => {
                        let _ = tx
                            .send(Ok(SessionKeepAliveResponse {
                                header: Some(header),
                                ttl,
                            }))
                            .await;
                    }
                    Ok(raft::RaftResponse::Error { message }) => {
                        let _ = tx.send(Err(Status::internal(message))).await;
                    }
                    Err(e) => {
                        let status = match &e {
                            crate::raft::RaftError::NotLeader { .. } => {
                                let mut s = Status::unavailable("not leader");
                                if let Some(addr) = raft
                                    .members()
                                    .into_iter()
                                    .find(|m| Some(m.id) == raft.leader_id())
                                    .map(|m| m.addr)
                                {
                                    let mut metadata = tonic::metadata::MetadataMap::new();
                                    if let Ok(val) = addr.parse() {
                                        metadata.insert("x-aether-leader", val);
                                        s = Status::with_metadata(s.code(), s.message(), metadata);
                                    }
                                }
                                s
                            }
                            crate::raft::RaftError::NoLeader => {
                                Status::unavailable("no leader elected")
                            }
                            _ => Status::internal(format!("raft error: {e}")),
                        };
                        let _ = tx.send(Err(status)).await;
                    }
                    _ => {
                        let _ = tx.send(Err(Status::internal("unexpected response"))).await;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn query(
        &self,
        request: Request<SessionQueryRequest>,
    ) -> Result<Response<SessionQueryResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled)?;
        require_leader(self.raft.as_ref(), self.node_id)?;
        let req = request.into_inner();

        if req.id <= 0 {
            return Err(Status::invalid_argument("session ID must be positive"));
        }

        let mgr = self
            .lease_manager
            .lock()
            .map_err(|e| Status::internal(format!("lease manager lock poisoned: {e}")))?;

        let lease = mgr
            .get(req.id)
            .ok_or_else(|| Status::not_found(format!("session not found: {}", req.id)))?;

        let remaining_ttl = ((lease.expiry_time - now_millis()) / 1000).max(0);
        let keys_count = mgr.get_keys(req.id).map(|k| k.len()).unwrap_or(0) as i64;

        Ok(Response::new(SessionQueryResponse {
            header: Some(self.header()),
            id: req.id,
            ttl: remaining_ttl,
            granted_ttl: lease.granted_ttl,
            keys: keys_count,
        }))
    }
}
