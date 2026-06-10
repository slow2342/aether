use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::{Request, Response, Status};

use crate::auth::{AuthCache, AuthInterceptor, TokenValidator, User};
use crate::proto::aether_auth_server::AetherAuth;
use crate::proto::*;
use crate::raft::{self, RaftHandle, require_leader};

fn auth_header(node_id: u64) -> ResponseHeader {
    ResponseHeader {
        cluster_id: 0,
        member_id: node_id,
        revision: 0,
        raft_term: 0,
    }
}

pub struct AuthService {
    raft: Arc<dyn RaftHandle>,
    node_id: u64,
    auth_cache: Arc<AuthCache>,
    token_validator: Arc<TokenValidator>,
    auth_enabled: Arc<AtomicBool>,
    auth_interceptor: Arc<AuthInterceptor>,
    /// Whether auth has ever been enabled — set once, never cleared.
    /// When true, auth_enable requires a valid root token.
    bootstrapped: Arc<AtomicBool>,
}

impl AuthService {
    pub fn new(
        raft: Arc<dyn RaftHandle>,
        node_id: u64,
        auth_cache: Arc<AuthCache>,
        token_validator: Arc<TokenValidator>,
        auth_enabled: Arc<AtomicBool>,
        auth_interceptor: Arc<AuthInterceptor>,
        bootstrapped: Arc<AtomicBool>,
    ) -> Self {
        Self {
            raft,
            node_id,
            auth_cache,
            token_validator,
            auth_enabled,
            auth_interceptor,
            bootstrapped,
        }
    }

    async fn propose(&self, request: raft::RaftRequest) -> Result<raft::RaftResponse, Status> {
        require_leader(self.raft.as_ref(), self.node_id)?;
        self.raft
            .propose(request)
            .await
            .map_err(|e| Status::internal(format!("raft write failed: {e}")))
    }

    /// Get the authenticated username from request extensions
    fn current_user<T>(req: &Request<T>) -> Option<String> {
        req.extensions().get::<String>().cloned()
    }

    /// Require root user for admin operations when auth is enabled.
    /// When auth is disabled and bootstrapped, still require root to prevent
    /// admin operations during the AuthDisable→AuthEnable window.
    fn require_root<T>(
        req: &Request<T>,
        auth_enabled: &AtomicBool,
        bootstrapped: &AtomicBool,
    ) -> Result<(), Status> {
        if !auth_enabled.load(Ordering::Acquire) && !bootstrapped.load(Ordering::Acquire) {
            return Ok(());
        }
        let user =
            Self::current_user(req).ok_or_else(|| Status::unauthenticated("no user in context"))?;
        if user != "root" {
            return Err(Status::permission_denied("root user required"));
        }
        Ok(())
    }

    /// Validate a user or role name: no null bytes, max 128 bytes, no reserved prefix.
    fn validate_name(name: &str) -> Result<(), Status> {
        if name.is_empty() {
            return Err(Status::invalid_argument("name must not be empty"));
        }
        if name.len() > 128 {
            return Err(Status::invalid_argument("name must not exceed 128 bytes"));
        }
        if name.as_bytes().contains(&0) {
            return Err(Status::invalid_argument("name must not contain null bytes"));
        }
        if name.starts_with("_aether_") {
            return Err(Status::invalid_argument(
                "name must not start with reserved prefix _aether_",
            ));
        }
        Ok(())
    }

    const MAX_PASSWORD_LEN: usize = 1024;
}

#[tonic::async_trait]
impl AetherAuth for AuthService {
    async fn user_add(
        &self,
        request: Request<UserAddRequest>,
    ) -> Result<Response<UserAddResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        Self::validate_name(&req.name)?;
        if req.password.len() < 8 {
            return Err(Status::invalid_argument(
                "password must be at least 8 characters",
            ));
        }
        if req.password.len() > Self::MAX_PASSWORD_LEN {
            return Err(Status::invalid_argument(
                "password must not exceed 1024 bytes",
            ));
        }
        let password_hash = User::hash_password(&req.password).map_err(Status::internal)?;
        let resp = self
            .propose(raft::RaftRequest::AuthUserAdd {
                name: req.name.into_bytes(),
                password_hash: password_hash.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthUserAdd {} => Ok(Response::new(UserAddResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => Err(Status::already_exists(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn user_delete(
        &self,
        request: Request<UserDeleteRequest>,
    ) -> Result<Response<UserDeleteResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        if req.name == "root" {
            return Err(Status::permission_denied("cannot delete root user"));
        }
        let resp = self
            .propose(raft::RaftRequest::AuthUserDelete {
                name: req.name.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthUserDelete {} => Ok(Response::new(UserDeleteResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => Err(Status::not_found(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn user_get(
        &self,
        request: Request<UserGetRequest>,
    ) -> Result<Response<UserGetResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let user = self
            .auth_cache
            .get_user(&req.name)
            .ok_or_else(|| Status::not_found("user not found"))?;
        Ok(Response::new(UserGetResponse {
            header: Some(auth_header(self.node_id)),
            user: Some(AuthUser {
                name: user.name,
                roles: user.roles,
                enabled: user.enabled,
            }),
        }))
    }

    async fn user_list(
        &self,
        request: Request<UserListRequest>,
    ) -> Result<Response<UserListResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let users = self
            .auth_cache
            .list_users()
            .into_iter()
            .map(|u| AuthUser {
                name: u.name,
                roles: u.roles,
                enabled: u.enabled,
            })
            .collect();
        Ok(Response::new(UserListResponse {
            header: Some(auth_header(self.node_id)),
            users,
        }))
    }

    async fn user_change_password(
        &self,
        request: Request<UserChangePasswordRequest>,
    ) -> Result<Response<UserChangePasswordResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        Self::validate_name(&req.name)?;
        if req.password.len() < 8 {
            return Err(Status::invalid_argument(
                "password must be at least 8 characters",
            ));
        }
        if req.password.len() > Self::MAX_PASSWORD_LEN {
            return Err(Status::invalid_argument(
                "password must not exceed 1024 bytes",
            ));
        }
        let password_hash = User::hash_password(&req.password).map_err(Status::internal)?;
        let resp = self
            .propose(raft::RaftRequest::AuthUserChangePassword {
                name: req.name.into_bytes(),
                password_hash: password_hash.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthUserChangePassword {} => {
                Ok(Response::new(UserChangePasswordResponse {
                    header: Some(auth_header(self.node_id)),
                }))
            }
            raft::RaftResponse::Error { message } => Err(Status::not_found(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn user_grant_role(
        &self,
        request: Request<UserGrantRoleRequest>,
    ) -> Result<Response<UserGrantRoleResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let resp = self
            .propose(raft::RaftRequest::AuthUserGrantRole {
                user: req.user.into_bytes(),
                role: req.role.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthUserGrantRole {} => Ok(Response::new(UserGrantRoleResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => Err(Status::not_found(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn user_revoke_role(
        &self,
        request: Request<UserRevokeRoleRequest>,
    ) -> Result<Response<UserRevokeRoleResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let resp = self
            .propose(raft::RaftRequest::AuthUserRevokeRole {
                user: req.user.into_bytes(),
                role: req.role.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthUserRevokeRole {} => {
                Ok(Response::new(UserRevokeRoleResponse {
                    header: Some(auth_header(self.node_id)),
                }))
            }
            raft::RaftResponse::Error { message } => Err(Status::not_found(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn role_add(
        &self,
        request: Request<RoleAddRequest>,
    ) -> Result<Response<RoleAddResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        Self::validate_name(&req.name)?;
        let resp = self
            .propose(raft::RaftRequest::AuthRoleAdd {
                name: req.name.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthRoleAdd {} => Ok(Response::new(RoleAddResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => Err(Status::already_exists(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn role_delete(
        &self,
        request: Request<RoleDeleteRequest>,
    ) -> Result<Response<RoleDeleteResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let resp = self
            .propose(raft::RaftRequest::AuthRoleDelete {
                name: req.name.into_bytes(),
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthRoleDelete {} => Ok(Response::new(RoleDeleteResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => Err(Status::failed_precondition(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn role_get(
        &self,
        request: Request<RoleGetRequest>,
    ) -> Result<Response<RoleGetResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let role = self
            .auth_cache
            .get_role(&req.name)
            .ok_or_else(|| Status::not_found("role not found"))?;
        Ok(Response::new(RoleGetResponse {
            header: Some(auth_header(self.node_id)),
            role: Some(AuthRole {
                name: role.name,
                permissions: role
                    .permissions
                    .into_iter()
                    .map(|p| Permission {
                        perm_type: match p.perm_type {
                            crate::auth::PermissionType::Read => PermissionType::Read.into(),
                            crate::auth::PermissionType::Write => PermissionType::Write.into(),
                            crate::auth::PermissionType::Readwrite => {
                                PermissionType::Readwrite.into()
                            }
                        },
                        key: p.key,
                        range_end: p.range_end,
                    })
                    .collect(),
            }),
        }))
    }

    async fn role_list(
        &self,
        request: Request<RoleListRequest>,
    ) -> Result<Response<RoleListResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let roles = self
            .auth_cache
            .list_roles()
            .into_iter()
            .map(|r| AuthRole {
                name: r.name,
                permissions: r
                    .permissions
                    .into_iter()
                    .map(|p| Permission {
                        perm_type: match p.perm_type {
                            crate::auth::PermissionType::Read => PermissionType::Read.into(),
                            crate::auth::PermissionType::Write => PermissionType::Write.into(),
                            crate::auth::PermissionType::Readwrite => {
                                PermissionType::Readwrite.into()
                            }
                        },
                        key: p.key,
                        range_end: p.range_end,
                    })
                    .collect(),
            })
            .collect();
        Ok(Response::new(RoleListResponse {
            header: Some(auth_header(self.node_id)),
            roles,
        }))
    }

    async fn role_grant_permission(
        &self,
        request: Request<RoleGrantPermissionRequest>,
    ) -> Result<Response<RoleGrantPermissionResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let perm = req
            .permission
            .ok_or_else(|| Status::invalid_argument("permission required"))?;
        let perm_type = match PermissionType::try_from(perm.perm_type) {
            Ok(PermissionType::Read) => crate::auth::PermissionType::Read,
            Ok(PermissionType::Write) => crate::auth::PermissionType::Write,
            Ok(PermissionType::Readwrite) => crate::auth::PermissionType::Readwrite,
            _ => return Err(Status::invalid_argument("invalid permission type")),
        };
        let resp = self
            .propose(raft::RaftRequest::AuthRoleGrantPermission {
                role: req.role.into_bytes(),
                permission: crate::auth::Permission {
                    perm_type,
                    key: perm.key,
                    range_end: perm.range_end,
                },
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthRoleGrantPermission {} => {
                Ok(Response::new(RoleGrantPermissionResponse {
                    header: Some(auth_header(self.node_id)),
                }))
            }
            raft::RaftResponse::Error { message } => Err(Status::not_found(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn role_revoke_permission(
        &self,
        request: Request<RoleRevokePermissionRequest>,
    ) -> Result<Response<RoleRevokePermissionResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let req = request.into_inner();
        let perm = req
            .permission
            .ok_or_else(|| Status::invalid_argument("permission required"))?;
        let perm_type = match PermissionType::try_from(perm.perm_type) {
            Ok(PermissionType::Read) => crate::auth::PermissionType::Read,
            Ok(PermissionType::Write) => crate::auth::PermissionType::Write,
            Ok(PermissionType::Readwrite) => crate::auth::PermissionType::Readwrite,
            _ => return Err(Status::invalid_argument("invalid permission type")),
        };
        let resp = self
            .propose(raft::RaftRequest::AuthRoleRevokePermission {
                role: req.role.into_bytes(),
                permission: crate::auth::Permission {
                    perm_type,
                    key: perm.key,
                    range_end: perm.range_end,
                },
            })
            .await?;
        match resp {
            raft::RaftResponse::AuthRoleRevokePermission {} => {
                Ok(Response::new(RoleRevokePermissionResponse {
                    header: Some(auth_header(self.node_id)),
                }))
            }
            raft::RaftResponse::Error { message } => Err(Status::not_found(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn authenticate(
        &self,
        request: Request<AuthenticateRequest>,
    ) -> Result<Response<AuthenticateResponse>, Status> {
        if !self.auth_enabled.load(Ordering::Acquire) {
            return Err(Status::failed_precondition("auth is not enabled"));
        }
        let req = request.into_inner();
        if req.password.len() > Self::MAX_PASSWORD_LEN {
            return Err(Status::invalid_argument(
                "password must not exceed 1024 bytes",
            ));
        }

        // Check rate limit before attempting authentication
        if let Some(remaining) = self.auth_interceptor.is_locked_out(&req.name) {
            return Err(Status::resource_exhausted(format!(
                "account locked, try again in {remaining}s"
            )));
        }

        let user = match self.auth_cache.get_user(&req.name) {
            Some(u) => u,
            None => {
                self.auth_interceptor.record_failure(&req.name);
                return Err(Status::unauthenticated("invalid credentials"));
            }
        };
        // Use same error for disabled users and wrong password to prevent
        // username enumeration via timing or error message differences.
        if !user.enabled || !user.verify_password(&req.password) {
            self.auth_interceptor.record_failure(&req.name);
            return Err(Status::unauthenticated("invalid credentials"));
        }

        // Authentication succeeded — clear failure count
        self.auth_interceptor.clear_failures(&req.name);
        let token = self
            .token_validator
            .create_token(&req.name)
            .map_err(Status::internal)?;
        Ok(Response::new(AuthenticateResponse {
            header: Some(auth_header(self.node_id)),
            token,
        }))
    }

    async fn auth_enable(
        &self,
        request: Request<AuthEnableRequest>,
    ) -> Result<Response<AuthEnableResponse>, Status> {
        // After first bootstrap, re-enabling auth requires root credentials
        if self.bootstrapped.load(Ordering::Acquire) {
            Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        }
        // Rate-limit auth_enable to prevent argon2 CPU exhaustion (M5)
        if let Some(remaining) = self.auth_interceptor.is_locked_out("__auth_enable__") {
            return Err(Status::resource_exhausted(format!(
                "too many attempts, try again in {remaining}s"
            )));
        }
        let req = request.into_inner();
        if req.root_password.len() < 8 {
            return Err(Status::invalid_argument(
                "root password must be at least 8 characters",
            ));
        }
        if req.root_password.len() > Self::MAX_PASSWORD_LEN {
            return Err(Status::invalid_argument(
                "root password must not exceed 1024 bytes",
            ));
        }
        let password_hash = User::hash_password(&req.root_password).map_err(Status::internal)?;
        let resp = self
            .propose(raft::RaftRequest::AuthEnable {
                root_password_hash: password_hash.into_bytes(),
            })
            .await;
        // Record failure for rate limiting (argon2 was already computed)
        if resp.is_err() {
            self.auth_interceptor.record_failure("__auth_enable__");
        }
        match resp? {
            raft::RaftResponse::AuthEnable {} => Ok(Response::new(AuthEnableResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => {
                self.auth_interceptor.record_failure("__auth_enable__");
                Err(Status::internal(message))
            }
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn auth_disable(
        &self,
        request: Request<AuthDisableRequest>,
    ) -> Result<Response<AuthDisableResponse>, Status> {
        Self::require_root(&request, &self.auth_enabled, &self.bootstrapped)?;
        let resp = self.propose(raft::RaftRequest::AuthDisable {}).await?;
        match resp {
            raft::RaftResponse::AuthDisable {} => Ok(Response::new(AuthDisableResponse {
                header: Some(auth_header(self.node_id)),
            })),
            raft::RaftResponse::Error { message } => Err(Status::internal(message)),
            _ => Err(Status::internal("unexpected response")),
        }
    }

    async fn auth_status(
        &self,
        _request: Request<AuthStatusRequest>,
    ) -> Result<Response<AuthStatusResponse>, Status> {
        Ok(Response::new(AuthStatusResponse {
            header: Some(auth_header(self.node_id)),
            enabled: self.auth_enabled.load(Ordering::Acquire),
        }))
    }
}
