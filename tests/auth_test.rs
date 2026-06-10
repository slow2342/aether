use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use aether::auth::cache::AuthCache;
use aether::auth::interceptor::AuthInterceptor;
use aether::auth::role::{Permission, PermissionType, Role};
use aether::auth::token::TokenValidator;
use aether::auth::user::User;

#[test]
fn test_full_auth_flow() {
    // 1. Create cache and validator
    let cache = AuthCache::new();
    let validator = TokenValidator::new("test-secret", 24);

    // 2. Create root user
    let root_hash = User::hash_password(b"root-password-123").unwrap();
    let root_user = User::new("root".to_string(), root_hash);
    cache.insert_user(root_user);

    // 3. Create a role with key-range permission
    let mut app_role = Role::new("app-writer".to_string());
    app_role.permissions.push(Permission {
        perm_type: PermissionType::Write,
        key: b"/app/".to_vec(),
        range_end: b"/app0".to_vec(),
    });
    cache.insert_role(app_role);

    // 4. Create a user and grant the role
    let alice_hash = User::hash_password(b"alice-password").unwrap();
    let mut alice = User::new("alice".to_string(), alice_hash);
    alice.roles.push("app-writer".to_string());
    cache.insert_user(alice);

    // 5. Verify authentication
    let alice = cache.get_user("alice").unwrap();
    assert!(alice.verify_password(b"alice-password"));

    // 6. Create token
    let token = validator.create_token("alice").unwrap();
    let claims = validator.validate_token(&token).unwrap();
    assert_eq!(claims.sub, "alice");

    // 7. Test permission check
    let interceptor = AuthInterceptor::new(
        Arc::new(AtomicBool::new(true)),
        Arc::new(validator),
        Arc::new(cache),
        Arc::new(AtomicBool::new(true)),
    );

    // Alice can write to /app/ keys
    assert!(
        interceptor
            .check_permission("alice", b"/app/config", PermissionType::Write)
            .is_ok()
    );

    // Alice cannot write to /other/ keys
    assert!(
        interceptor
            .check_permission("alice", b"/other/key", PermissionType::Write)
            .is_err()
    );

    // Alice cannot read (only write permission)
    assert!(
        interceptor
            .check_permission("alice", b"/app/config", PermissionType::Read)
            .is_err()
    );

    // Root bypasses all checks
    assert!(
        interceptor
            .check_permission("root", b"/any/key", PermissionType::Write)
            .is_ok()
    );
}

#[test]
fn test_role_delete_in_use() {
    let cache = AuthCache::new();

    let mut user = User::new("bob".to_string(), "hash".to_string());
    user.roles.push("admin".to_string());
    cache.insert_user(user);

    assert!(cache.is_role_in_use("admin"));
    assert!(!cache.is_role_in_use("other"));
}

#[test]
fn test_permission_covers_range() {
    let p = Permission {
        perm_type: PermissionType::Read,
        key: b"/app/".to_vec(),
        range_end: b"/app0".to_vec(),
    };

    // Covers whole range
    assert!(p.covers_range(b"/app/", b"/app0", PermissionType::Read));

    // Partial range within
    assert!(p.covers_range(b"/app/a", b"/app/b", PermissionType::Read));

    // Extends beyond
    assert!(!p.covers_range(b"/app/", b"/zzz", PermissionType::Read));
}

#[test]
fn test_rate_limiting() {
    let cache = AuthCache::new();
    let validator = TokenValidator::new("test-secret", 24);
    let interceptor = AuthInterceptor::new(
        Arc::new(AtomicBool::new(true)),
        Arc::new(validator),
        Arc::new(cache),
        Arc::new(AtomicBool::new(false)),
    );

    // Record 5 failures
    for _ in 0..5 {
        interceptor.record_failure("attacker");
    }

    // Should be locked out
    assert!(interceptor.is_locked_out("attacker").is_some());

    // Clear on success
    interceptor.clear_failures("attacker");
    assert!(interceptor.is_locked_out("attacker").is_none());
}
