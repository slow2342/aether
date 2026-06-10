pub mod cache;
pub mod interceptor;
pub mod role;
pub mod token;
pub mod user;

pub use self::cache::AuthCache;
pub use self::interceptor::{AuthInterceptor, AuthLayer, AuthService};
pub use self::role::{Permission, PermissionType, Role, extract_txn_keys};
pub use self::token::TokenValidator;
pub use self::user::User;

/// Key prefix for user data in storage
pub const USER_KEY_PREFIX: &[u8] = b"_aether_auth/user/";
/// Key prefix for role data in storage
pub const ROLE_KEY_PREFIX: &[u8] = b"_aether_auth/role/";
/// Key for the auth enabled flag in storage
pub const AUTH_ENABLED_KEY: &[u8] = b"_aether_auth/enabled";
/// Key for the auth bootstrapped flag — set once on first AuthEnable,
/// never cleared. Prevents unauthenticated re-enable after AuthDisable.
pub const AUTH_BOOTSTRAPPED_KEY: &[u8] = b"_aether_auth/bootstrapped";

/// Encode a user storage key
pub fn user_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(USER_KEY_PREFIX.len() + name.len());
    key.extend_from_slice(USER_KEY_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}

/// Encode a role storage key
pub fn role_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(ROLE_KEY_PREFIX.len() + name.len());
    key.extend_from_slice(ROLE_KEY_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}
