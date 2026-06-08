# Logging & Observability Conventions

## Tracing Setup

- Use `tracing` crate for all logging — never `println!` or `log` crate
- Use `tracing-subscriber` with structured JSON output in production
- Use `tracing-subscriber` with pretty output in development

## Log Levels

| Level | When to use |
|-------|-------------|
| `error!` | Unrecoverable or data-loss situations. Requires human attention. |
| `warn!` | Recoverable anomalies. Degraded but functional. |
| `info!` | Key lifecycle events: startup, shutdown, leader change, membership change. |
| `debug!` | Request-level tracing: individual RPC calls, state transitions. |
| `trace!` | Extremely verbose: every Raft message, every storage operation. |

## Structured Logging

Always use structured fields, never string interpolation:

```rust
// BAD: string interpolation
info!("Received put request for key {}", String::from_utf8_lossy(&key));

// GOOD: structured fields
info!(key = %String::from_utf8_lossy(&key), "received put request");
```

## Span Usage

Create spans for operations that involve multiple steps:

```rust
// BAD: no span context
async fn handle_put(&self, req: PutRequest) -> PutResponse {
    info!("handling put");
    self.raft.propose(req).await;
    info!("put complete");
}

// GOOD: span with context
#[instrument(skip(self, req), fields(key = %String::from_utf8_lossy(&req.key)))]
async fn handle_put(&self, req: PutRequest) -> PutResponse {
    info!("handling put request");
    self.raft.propose(req).await;
    info!("put complete");
}
```

## What to Log

### Always Log

- Server startup/shutdown (info)
- Leader elections (info)
- Membership changes (info)
- Snapshot creation/restore (info)
- Fatal errors that cause shutdown (error)

### Log at Debug

- Individual RPC requests (with key/range info)
- Raft state transitions
- Watch events dispatched
- Lease grant/revoke/expire

### Log at Trace

- Raft messages sent/received (AppendEntries, Vote)
- Storage read/write operations
- Codec encode/decode

### Never Log

- Passwords, tokens, secrets
- Full values of large data — log key + length instead
- In tight loops — will flood output

## Error Logging

```rust
// BAD: logging error and returning it (double reporting)
error!("failed to get key: {}", err);
Err(err)

// GOOD: log at the caller, return at the site
// At the error site:
Err(StorageError::KeyNotFound { key: key.to_vec() })

// At the handler (caller) level:
match storage.get(&key) {
    Ok(val) => ...,
    Err(e) => {
        error!(key = %String::from_utf8_lossy(&key), error = %e, "get failed");
        return Err(e.into());
    }
}
```

## Metrics Naming

Follow Prometheus naming conventions:

- `aether_<subsystem>_<name>_<unit>`
- Use `_total` suffix for counters
- Use `_seconds` suffix for duration histograms
- Examples:
  - `aether_raft_leader_changes_total`
  - `aether_request_duration_seconds`
  - `aether_storage_size_bytes`
  - `aether_watch_active_count`

## Health Check

- Liveness: is the process alive? (always 200 if running)
- Readiness: is the node ready to serve traffic? (200 only if Raft is initialized and storage is accessible)

## See Also

- [api-design.md](api-design.md) — gRPC service structure and error codes
- [ci-conventions.md](ci-conventions.md) — CI pipeline configuration
