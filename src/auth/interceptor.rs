use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use http::{Request, Response};
use tower::{Layer, Service};

use super::cache::AuthCache;
use super::role::PermissionType;
use super::token::TokenValidator;

/// Auth failure record for rate limiting
struct FailureRecord {
    count: u64,
    first_failure: Instant,
    locked_until: Option<Instant>,
}

/// Core auth logic shared between the layer and service handlers.
///
/// Validates JWT tokens, enforces rate limiting on failed attempts,
/// and provides RBAC permission checking helpers for use in service handlers.
pub struct AuthInterceptor {
    auth_enabled: Arc<AtomicBool>,
    token_validator: Arc<TokenValidator>,
    auth_cache: Arc<AuthCache>,
    /// Per-username failure tracking for rate limiting
    failures: Mutex<HashMap<String, FailureRecord>>,
    /// Whether auth has ever been enabled (prevents unauthenticated re-enable)
    bootstrapped: Arc<AtomicBool>,
}

const MAX_FAILURES: u64 = 5;
const FAILURE_WINDOW_SECS: u64 = 300; // 5 minutes
const LOCKOUT_SECS: u64 = 900; // 15 minutes

impl AuthInterceptor {
    pub fn new(
        auth_enabled: Arc<AtomicBool>,
        token_validator: Arc<TokenValidator>,
        auth_cache: Arc<AuthCache>,
        bootstrapped: Arc<AtomicBool>,
    ) -> Self {
        Self {
            auth_enabled,
            token_validator,
            auth_cache,
            failures: Mutex::new(HashMap::new()),
            bootstrapped,
        }
    }

    /// Set auth enabled state (called by state machine on AuthEnable/AuthDisable)
    pub fn set_enabled(&self, enabled: bool) {
        self.auth_enabled.store(enabled, Ordering::Release);
    }

    /// Record a failed authentication attempt.
    /// Also cleans up expired entries to prevent unbounded memory growth.
    pub fn record_failure(&self, username: &str) {
        let mut failures = self.failures.lock().unwrap();

        // Clean up expired entries to prevent memory exhaustion
        failures.retain(|_, record| {
            if let Some(locked_until) = record.locked_until {
                // Keep if still locked
                locked_until > Instant::now()
            } else {
                // Keep if within failure window
                record.first_failure.elapsed().as_secs() <= FAILURE_WINDOW_SECS
            }
        });

        let record = failures
            .entry(username.to_string())
            .or_insert(FailureRecord {
                count: 0,
                first_failure: Instant::now(),
                locked_until: None,
            });

        // Reset if outside failure window
        if record.first_failure.elapsed().as_secs() > FAILURE_WINDOW_SECS {
            record.count = 0;
            record.first_failure = Instant::now();
            record.locked_until = None;
        }

        record.count += 1;
        if record.count >= MAX_FAILURES {
            record.locked_until =
                Some(Instant::now() + std::time::Duration::from_secs(LOCKOUT_SECS));
        }
    }

    /// Clear failure record on successful authentication
    pub fn clear_failures(&self, username: &str) {
        self.failures.lock().unwrap().remove(username);
    }

    /// Check if a username is currently locked out.
    /// Returns `Some(remaining_secs)` if locked, `None` otherwise.
    pub fn is_locked_out(&self, username: &str) -> Option<u64> {
        let failures = self.failures.lock().unwrap();
        if let Some(record) = failures.get(username)
            && let Some(locked_until) = record.locked_until
        {
            let remaining = locked_until
                .checked_duration_since(Instant::now())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if remaining > 0 {
                return Some(remaining);
            }
        }
        None
    }

    /// Check permission for a single key.
    /// Called by service handlers after extracting the username from request extensions.
    pub fn check_permission(
        &self,
        username: &str,
        key: &[u8],
        required: PermissionType,
    ) -> Result<(), tonic::Status> {
        // Root user bypasses permission check
        if username == "root" {
            return Ok(());
        }

        let user = self
            .auth_cache
            .get_user(username)
            .ok_or_else(|| tonic::Status::unauthenticated("user not found"))?;

        if !user.enabled {
            return Err(tonic::Status::unauthenticated("user is disabled"));
        }

        for role_name in &user.roles {
            if let Some(role) = self.auth_cache.get_role(role_name) {
                for perm in &role.permissions {
                    if perm.covers_key(key, required) {
                        return Ok(());
                    }
                }
            }
        }

        Err(tonic::Status::permission_denied(
            "permission denied for the requested resource",
        ))
    }

    /// Check permission for a key range.
    /// Called by service handlers for Range/Delete-range operations.
    pub fn check_range_permission(
        &self,
        username: &str,
        key: &[u8],
        range_end: &[u8],
        required: PermissionType,
    ) -> Result<(), tonic::Status> {
        if username == "root" {
            return Ok(());
        }

        let user = self
            .auth_cache
            .get_user(username)
            .ok_or_else(|| tonic::Status::unauthenticated("user not found"))?;

        if !user.enabled {
            return Err(tonic::Status::unauthenticated("user is disabled"));
        }

        for role_name in &user.roles {
            if let Some(role) = self.auth_cache.get_role(role_name) {
                for perm in &role.permissions {
                    if perm.covers_range(key, range_end, required) {
                        return Ok(());
                    }
                }
            }
        }

        Err(tonic::Status::permission_denied(
            "permission denied for the requested resource",
        ))
    }

    /// Validate token and return the username.
    /// Returns `Ok(username)` on success, or `Ok(String::new())` when auth is
    /// disabled or the request targets a public endpoint.
    fn authenticate<B>(&self, req: &Request<B>) -> Result<String, tonic::Status> {
        // If auth is disabled, allow all requests
        if !self.auth_enabled.load(Ordering::Acquire) {
            return Ok(String::new());
        }

        let path = req.uri().path();

        // Public endpoints: Authenticate only.
        // AuthEnable is only public when auth has never been bootstrapped (first-time setup).
        // Once bootstrapped, AuthEnable requires a valid token (root).
        // AuthStatus requires auth to prevent information leakage.
        if path.ends_with("/Authenticate") {
            return Ok(String::new());
        }
        if path.ends_with("/AuthEnable") && !self.bootstrapped.load(Ordering::Acquire) {
            return Ok(String::new());
        }

        // Extract token from HTTP headers
        let raw_token = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| tonic::Status::unauthenticated("missing authorization token"))?;

        // Strip "Bearer " prefix if present
        let token = raw_token.strip_prefix("Bearer ").unwrap_or(raw_token);

        // Validate token
        let claims = self
            .token_validator
            .validate_token(token)
            .map_err(|_| tonic::Status::unauthenticated("invalid or expired token"))?;

        // Check rate limit
        if let Some(remaining) = self.is_locked_out(&claims.sub) {
            return Err(tonic::Status::resource_exhausted(format!(
                "account locked, try again in {remaining}s"
            )));
        }

        Ok(claims.sub)
    }
}

// --- Tower Layer / Service for HTTP-level auth ---
//
// tonic's `Interceptor` trait receives `Request<()>` which does not include
// the URI path. To perform path-based public-endpoint bypass we must operate
// at the HTTP level via a tower Layer/Service.

/// Tower layer that wraps a service with JWT validation and rate limiting.
#[derive(Clone)]
pub struct AuthLayer {
    interceptor: Arc<AuthInterceptor>,
}

impl AuthLayer {
    pub fn new(interceptor: Arc<AuthInterceptor>) -> Self {
        Self { interceptor }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            interceptor: self.interceptor.clone(),
        }
    }
}

/// Tower service that validates JWT tokens and stores the authenticated
/// username in request extensions. Downstream handlers use the username
/// for RBAC permission checks.
#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    interceptor: Arc<AuthInterceptor>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for AuthService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = S::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        match self.interceptor.authenticate(&req) {
            Ok(username) => {
                // Store username in request extensions for service handlers
                req.extensions_mut().insert(username);
                let fut = self.inner.call(req);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Err(status) => {
                // Return the auth error as an HTTP response with gRPC status
                let response = status.into_http();
                Box::pin(async move { Ok(response) })
            }
        }
    }
}
