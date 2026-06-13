use super::{BackendPool, extract_leader_redirect, forward_request};
use crate::proto::aether_auth_server::AetherAuth;
use crate::proto::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub struct AuthProxy {
    pool: Arc<RwLock<BackendPool>>,
}
impl AuthProxy {
    pub fn new(pool: Arc<RwLock<BackendPool>>) -> Self {
        Self { pool }
    }
    async fn redirect_and_cache(
        &self,
        leader: &str,
    ) -> Result<crate::proto::aether_auth_client::AetherAuthClient<tonic::transport::Channel>, Status>
    {
        let conn = super::redirect_connection(&self.pool, leader).await?;
        Ok(conn.auth.clone())
    }
}

macro_rules! unary_auth {
    ($self:ident, $request:ident, $method:ident, $getter:ident) => {{
        let metadata = $request.metadata().clone();
        let req = $request.into_inner();
        let (timeout, _addr, mut client) = {
            let p = $self.pool.read().await;
            let c = p
                .$getter()
                .ok_or_else(|| Status::unavailable("no backends available"))?;
            (p.timeout(), c.0, c.1)
        };
        match tokio::time::timeout(
            timeout,
            client.$method(forward_request(&metadata, req.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(status)) => {
                if let Some(leader) = extract_leader_redirect(&status) {
                    let mut c = $self.redirect_and_cache(&leader).await?;
                    tokio::time::timeout(timeout, c.$method(forward_request(&metadata, req)))
                        .await
                        .map_err(|_| Status::deadline_exceeded("request timed out"))?
                } else {
                    Err(status)
                }
            }
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }};
}

#[tonic::async_trait]
impl AetherAuth for AuthProxy {
    async fn authenticate(
        &self,
        request: Request<AuthenticateRequest>,
    ) -> Result<Response<AuthenticateResponse>, Status> {
        unary_auth!(self, request, authenticate, get_any_auth)
    }
    async fn auth_enable(
        &self,
        request: Request<AuthEnableRequest>,
    ) -> Result<Response<AuthEnableResponse>, Status> {
        unary_auth!(self, request, auth_enable, get_leader_auth)
    }
    async fn auth_disable(
        &self,
        request: Request<AuthDisableRequest>,
    ) -> Result<Response<AuthDisableResponse>, Status> {
        unary_auth!(self, request, auth_disable, get_leader_auth)
    }
    async fn auth_status(
        &self,
        request: Request<AuthStatusRequest>,
    ) -> Result<Response<AuthStatusResponse>, Status> {
        unary_auth!(self, request, auth_status, get_any_auth)
    }
    async fn user_add(
        &self,
        request: Request<UserAddRequest>,
    ) -> Result<Response<UserAddResponse>, Status> {
        unary_auth!(self, request, user_add, get_leader_auth)
    }
    async fn user_delete(
        &self,
        request: Request<UserDeleteRequest>,
    ) -> Result<Response<UserDeleteResponse>, Status> {
        unary_auth!(self, request, user_delete, get_leader_auth)
    }
    async fn user_get(
        &self,
        request: Request<UserGetRequest>,
    ) -> Result<Response<UserGetResponse>, Status> {
        unary_auth!(self, request, user_get, get_any_auth)
    }
    async fn user_list(
        &self,
        request: Request<UserListRequest>,
    ) -> Result<Response<UserListResponse>, Status> {
        unary_auth!(self, request, user_list, get_any_auth)
    }
    async fn user_change_password(
        &self,
        request: Request<UserChangePasswordRequest>,
    ) -> Result<Response<UserChangePasswordResponse>, Status> {
        unary_auth!(self, request, user_change_password, get_leader_auth)
    }
    async fn user_grant_role(
        &self,
        request: Request<UserGrantRoleRequest>,
    ) -> Result<Response<UserGrantRoleResponse>, Status> {
        unary_auth!(self, request, user_grant_role, get_leader_auth)
    }
    async fn user_revoke_role(
        &self,
        request: Request<UserRevokeRoleRequest>,
    ) -> Result<Response<UserRevokeRoleResponse>, Status> {
        unary_auth!(self, request, user_revoke_role, get_leader_auth)
    }
    async fn role_add(
        &self,
        request: Request<RoleAddRequest>,
    ) -> Result<Response<RoleAddResponse>, Status> {
        unary_auth!(self, request, role_add, get_leader_auth)
    }
    async fn role_delete(
        &self,
        request: Request<RoleDeleteRequest>,
    ) -> Result<Response<RoleDeleteResponse>, Status> {
        unary_auth!(self, request, role_delete, get_leader_auth)
    }
    async fn role_get(
        &self,
        request: Request<RoleGetRequest>,
    ) -> Result<Response<RoleGetResponse>, Status> {
        unary_auth!(self, request, role_get, get_any_auth)
    }
    async fn role_list(
        &self,
        request: Request<RoleListRequest>,
    ) -> Result<Response<RoleListResponse>, Status> {
        unary_auth!(self, request, role_list, get_any_auth)
    }
    async fn role_grant_permission(
        &self,
        request: Request<RoleGrantPermissionRequest>,
    ) -> Result<Response<RoleGrantPermissionResponse>, Status> {
        unary_auth!(self, request, role_grant_permission, get_leader_auth)
    }
    async fn role_revoke_permission(
        &self,
        request: Request<RoleRevokePermissionRequest>,
    ) -> Result<Response<RoleRevokePermissionResponse>, Status> {
        unary_auth!(self, request, role_revoke_permission, get_leader_auth)
    }
}
