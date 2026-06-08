# Security Conventions

## Input Validation

### API Layer

Validate all external input at the API boundary, before it reaches storage or Raft:

```rust
fn validate_put_request(req: &PutRequest) -> Result<(), Status> {
    if req.key.is_empty() {
        return Err(Status::invalid_argument("key must not be empty"));
    }
    if req.key.len() > MAX_KEY_SIZE {
        return Err(Status::invalid_argument(format!(
            "key size {} exceeds maximum {}",
            req.key.len(), MAX_KEY_SIZE
        )));
    }
    if req.value.len() > MAX_VALUE_SIZE {
        return Err(Status::invalid_argument(format!(
            "value size {} exceeds maximum {}",
            req.value.len(), MAX_VALUE_SIZE
        )));
    }
    Ok(())
}
```

### Size Limits

| Resource | Limit | Configurable |
|----------|-------|-------------|
| Key size | 1 KB | Yes |
| Value size | 1 MB | Yes |
| Range scan | 1000 keys | Yes |
| Watch streams per client | 10 | No |
| Lease TTL | 1s - 24h | No |

### Key Validation

- Keys must not be empty
- Keys must not contain null bytes unless using range encoding
- Keys starting with `_aether_` are reserved for internal use

## Authentication

### Token Security

- JWT tokens signed with HS256 (upgradeable to RS256)
- Token expiry: 24h default, configurable
- Store signing key securely (environment variable, not config file)
- Never log tokens

### Password Storage

- Hash passwords with argon2id before storage
- Never store or log plaintext passwords
- Password complexity: minimum 8 characters

### RBAC

- Root role has full access, cannot be deleted
- Default: deny all
- Permissions: `read`, `write`, `readwrite`, `admin`
- Key-range permissions: `read("/app/"), write("/app/config/")`

## Secrets Management

```rust
// BAD: logging secrets
info!(token = %token, "authenticated user");

// GOOD: log action, not secret
info!(user = %username, "authenticated successfully");

// BAD: including secrets in error messages
return Err(format!("invalid token: {}", token));

// GOOD: generic error
return Err("invalid or expired token");
```

## Network Security

- Support TLS for gRPC (tonic TLS configuration)
- mTLS for inter-node Raft communication
- Validate peer certificates during cluster formation
- Default: plaintext (must opt-in to TLS)

```rust
let tls_config = ServerTlsConfig::new()
    .identity(server_identity)
    .client_ca_root(client_ca);

Server::builder()
    .tls_config(tls_config)?
    .add_service(service)
    .serve(addr)
    .await?;
```

## Unsafe Code

- No `unsafe` code without explicit `// SAFETY:` comment
- Each `unsafe` block must explain why it's sound
- Reviewer must approve `unsafe` usage in PR
- Prefer safe abstractions from well-tested crates

## Dependency Security

See [dependency-conventions.md](dependency-conventions.md) for full dependency management and auditing rules.

## Quota & Resource Protection

- Space quota: configurable max storage size
- Request rate limiting: per-client and global
- Connection limits: max concurrent gRPC connections
- Watch limits: max watches per client to prevent resource exhaustion

```rust
// Space quota check before write
if self.storage.size_bytes() + value.len() as u64 > self.config.max_storage_bytes {
    return Err(AetherError::QuotaExceeded {
        current: self.storage.size_bytes(),
        limit: self.config.max_storage_bytes,
    });
}
```

## See Also

- [api-design.md](api-design.md) — gRPC auth interceptor and error code mapping
- [dependency-conventions.md](dependency-conventions.md) — Dependency auditing and vulnerability checks
- [docker-conventions.md](docker-conventions.md) — Container security and image scanning
