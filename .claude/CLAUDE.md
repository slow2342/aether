# Aether — Distributed KV Store

Aether is a distributed key-value storage engine built in Rust, inspired by etcd and ZooKeeper. It uses Raft consensus for replication, RocksDB for storage, and gRPC for client communication.

## Architecture

```
src/
├── main.rs          # CLI entry point (clap)
├── lib.rs           # Module declarations
├── config.rs        # Configuration structs
├── error.rs         # Unified error types
├── raft/            # Raft consensus (raft-rs)
│   ├── mod.rs       # Type definitions (NodeId, RaftRequest, RaftResponse)
│   ├── handle.rs    # RaftHandle trait abstraction
│   ├── node.rs      # Raft event loop (dedicated thread)
│   ├── raftrs_handle.rs  # RaftHandle impl for raft-rs
│   ├── raftrs_store.rs   # RocksDB-backed raft log store
│   ├── state_machine.rs  # Apply log → storage engine
│   ├── network.rs        # Inter-node RPC (tonic)
│   └── rpc.rs            # Raft RPC server
├── storage/         # Storage engine layer
│   ├── mod.rs       # StorageEngine trait
│   ├── rocksdb.rs   # RocksDB implementation
│   ├── codec.rs     # rkyv serialization
│   ├── mvcc.rs      # Multi-version concurrency control
│   └── txn.rs       # Transaction execution
├── api/             # gRPC service layer
│   ├── mod.rs       # Module entry
│   ├── server.rs    # gRPC server bootstrap
│   ├── kv.rs        # KV service (Put/Get/Delete/Range)
│   ├── watch.rs     # Watch streaming service
│   ├── lease.rs     # Lease service (Grant/Revoke/KeepAlive)
│   ├── auth.rs      # Auth service (RBAC)
│   ├── txn.rs       # Transaction service
│   ├── cluster.rs   # Cluster management service
│   ├── member.rs    # Member/Learner management
│   ├── maintenance.rs  # Defrag/Alarm/Status
│   ├── metrics.rs   # Prometheus metrics
│   ├── health.rs    # Health check endpoint
│   └── admin.rs     # Admin interface
├── cluster/         # Cluster management
│   ├── mod.rs       # Cluster entry
│   ├── membership.rs    # Member add/remove (Raft ConfChange)
│   ├── alarm.rs     # Alarm management
│   ├── downgrade.rs # Version upgrade/downgrade
│   └── discovery.rs # Discovery service
├── shard/           # Data sharding
│   ├── mod.rs       # Shard manager
│   ├── region.rs    # Region definition (key range + replicas)
│   └── scheduler.rs # Split/Merge/Migration scheduler
├── watch/           # Watch mechanism
│   ├── mod.rs       # Watch manager
│   └── watcher.rs   # Watcher lifecycle
├── lease/           # Lease/TTL
│   ├── mod.rs       # Lease manager
│   └── lease.rs     # Lease struct (ID, TTL, keys)
├── auth/            # Authentication & authorization
│   ├── mod.rs       # Auth entry
│   ├── user.rs      # User management
│   ├── role.rs      # Role management (RBAC)
│   └── token.rs     # JWT token handling
├── session/         # Session management
│   ├── mod.rs       # Session manager
│   └── session.rs   # Session lifecycle
├── lock/            # Distributed lock
│   └── mod.rs       # Lock implementation
├── election/        # Leader election
│   └── mod.rs       # Election implementation
├── primitive/       # Distributed primitives
│   ├── barrier.rs   # Distributed barrier
│   └── queue.rs     # Distributed FIFO queue
├── gateway/         # Stateless proxy
│   ├── mod.rs       # Gateway logic
│   └── routing.rs   # Request routing
└── client/          # Rust client SDK
    ├── mod.rs       # Client entry
    ├── kv.rs        # KV client
    ├── watch.rs     # Watch client
    ├── lease.rs     # Lease client
    ├── lock.rs      # Lock client
    ├── election.rs  # Election client
    └── cluster.rs   # Cluster client
proto/
├── aether.proto     # Main service definitions
└── raft.proto       # Raft RPC definitions
```

### Data Flow

```
Client → gRPC API → Raft Leader → Log Entry → State Machine → RocksDB
                         ↓
                    Replicate to Followers
```

### Key Dependencies

- **raft**: Raft consensus protocol (crates.io `raft` crate v0.7)
- **rocksdb**: Embedded KV storage engine
- **rkyv**: Zero-copy serialization for internal data
- **tonic/prost**: gRPC framework
- **tokio**: Async runtime
- **clap**: CLI argument parsing
- **tracing**: Structured logging

## Rules

All coding conventions are in `rules/`. Read the relevant file before making changes:

| Topic | File |
|-------|------|
| Git commits, branches, PRs | [git-conventions.md](rules/git-conventions.md) |
| Rust coding style, naming, errors, async | [rust-conventions.md](rules/rust-conventions.md) |
| Common mistakes to avoid | [anti-patterns.md](rules/anti-patterns.md) |
| Logging, metrics, health checks | [logging-conventions.md](rules/logging-conventions.md) |
| Protobuf definitions | [proto-conventions.md](rules/proto-conventions.md) |
| RocksDB storage layer | [storage-conventions.md](rules/storage-conventions.md) |
| Raft integration (raft-rs) | [raft-conventions.md](rules/raft-conventions.md) |
| gRPC API design | [api-design.md](rules/api-design.md) |
| Testing & benchmarks | [testing-conventions.md](rules/testing-conventions.md) |
| Security & auth | [security-conventions.md](rules/security-conventions.md) |
| Dependency management | [dependency-conventions.md](rules/dependency-conventions.md) |
| Versioning & releases | [versioning-conventions.md](rules/versioning-conventions.md) |
| CI/CD pipeline | [ci-conventions.md](rules/ci-conventions.md) |
| Docker & containers | [docker-conventions.md](rules/docker-conventions.md) |

## Build & Test

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo test                     # Run all tests
cargo test -p aether --lib     # Unit tests only
cargo test --test <name>       # Run specific integration test
cargo clippy -- -D warnings    # Clippy with warnings as errors
cargo fmt -- --check           # Check formatting
cargo bench                    # Run benchmarks (criterion)
```

### Pre-commit Checklist

1. `cargo build` succeeds
2. `cargo test` passes
3. `cargo clippy -- -D warnings` passes
4. `cargo fmt -- --check` passes

## PR Implementation Order

See the implementation plan at `plans/giggly-singing-owl.md` for the full 23-PR breakdown with dependencies.
