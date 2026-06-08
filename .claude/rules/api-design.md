# gRPC API Design Conventions

## Service Structure

One service per concern:

```protobuf
service AetherKV { ... }         // KV operations
service AetherWatch { ... }      // Watch streaming
service AetherLease { ... }      // Lease management
service AetherAuth { ... }       // Authentication
service AetherCluster { ... }    // Cluster management
service AetherMaintenance { ... } // Defrag, Alarm, Status
service AetherLock { ... }       // Distributed lock
service AetherElection { ... }   // Leader election
```

## Request Validation

Validate all requests at the API layer before forwarding to Raft. See [security-conventions.md](security-conventions.md) for input validation rules and size limits.

## Authentication Interceptor

Apply auth check via tonic interceptor:

```rust
pub struct AuthInterceptor {
    auth_enabled: bool,
    token_validator: Arc<TokenValidator>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if !self.auth_enabled {
            return Ok(req);
        }

        let token = req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing token"))?;

        let claims = self.token_validator.validate(token)
            .map_err(|_| Status::unauthenticated("invalid token"))?;

        req.extensions_mut().insert(claims);
        Ok(req)
    }
}
```

## Error Code Mapping

Map internal errors to gRPC status codes:

```rust
impl From<AetherError> for Status {
    fn from(err: AetherError) -> Self {
        match err {
            AetherError::Storage(StorageError::KeyNotFound { .. }) => {
                Status::not_found(err.to_string())
            }
            AetherError::Storage(StorageError::RocksDb(_)) => {
                Status::internal("storage error")
            }
            AetherError::Auth(_) => {
                Status::permission_denied(err.to_string())
            }
            AetherError::NotLeader { leader } => {
                // Include leader info in metadata for client redirect
                let mut status = Status::unavailable("not leader");
                if let Some(leader_addr) = leader {
                    if let Ok(addr) = leader_addr.parse() {
                        let mut metadata = MetadataMap::new();
                        metadata.insert("x-aether-leader", addr);
                        status = Status::with_metadata(status.code(), status.message(), metadata);
                    }
                }
                status
            }
            _ => Status::internal(err.to_string()),
        }
    }
}
```

## Streaming Responses

For Watch and KeepAlive, use server streaming:

```protobuf
service AetherWatch {
    rpc Watch(WatchRequest) returns (stream WatchResponse);
}

service AetherLease {
    rpc KeepAlive(stream LeaseKeepAliveRequest) returns (stream LeaseKeepAliveResponse);
}
```

## Metadata Conventions

Custom metadata headers:

| Header | Direction | Purpose |
|--------|-----------|---------|
| `x-aether-cluster-id` | Both | Cluster identification |
| `x-aether-member-id` | Response | Node that handled request |
| `x-aether-revision` | Response | Store revision at response time |
| `x-aether-raft-term` | Response | Current Raft term |
| `x-aether-leader` | Error response | Leader address for redirect |

## Rate Limiting

- Apply per-key rate limiting for write-heavy workloads
- Use bounded channels for request queuing
- Return `Status::resource_exhausted` when queue is full

## Pagination

Use `limit` + `count_only` for large range scans:

```protobuf
message RangeRequest {
    bytes key = 1;
    bytes range_end = 2;
    int64 limit = 3;      // max keys to return, 0 = all
    bool count_only = 4;  // return count without values
    bytes start_key = 5;  // for continuation (last key from previous response)
}
```

## See Also

- [security-conventions.md](security-conventions.md) — Input validation, size limits, auth patterns
- [proto-conventions.md](proto-conventions.md) — Protobuf naming and structure rules
- [logging-conventions.md](logging-conventions.md) — Structured logging and metrics naming
