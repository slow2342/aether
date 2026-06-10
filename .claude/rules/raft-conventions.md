# Raft Integration Conventions

## raft-rs Type Configuration

Uses raft-rs 0.7 (TiKV fork) with a dedicated event loop thread:

```rust
pub type NodeId = u64;

// Event loop runs on a dedicated std::thread, communicates via channels.
// RaftHandle trait abstracts over the raft implementation for the API layer.
```

## Event Loop Architecture

- Raft event loop runs on a dedicated `std::thread` (not tokio)
- Uses `crossbeam-channel` for blocking multi-channel recv with timeout
- Tokio ↔ crossbeam bridge tasks forward messages between async and sync worlds
- `RaftSharedState` (AtomicU64) exposes leader_id to the async API layer

## State Machine Rules

### Apply Order

- State machine applies log entries in strict index order
- Each apply must be idempotent (safe to replay)
- Never assume ordering beyond Raft's guarantee

### Request/Response Types

```rust
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RaftRequest {
    Put { key: Vec<u8>, value: Vec<u8>, lease_id: i64 },
    Delete { key: Vec<u8>, range_end: Vec<u8> },
    Txn { compare: Vec<Compare>, success: Vec<RequestOp>, failure: Vec<RequestOp> },
    // Cluster operations
    MemberAdd { member: RaftNode },
    MemberRemove { node_id: NodeId },
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RaftResponse {
    Put { prev_kv: Option<KeyValue> },
    Delete { deleted: i64, prev_kvs: Vec<KeyValue> },
    Txn { succeeded: bool, responses: Vec<ResponseOp> },
    MemberAdd { member: RaftNode },
    MemberRemove {},
}
```

### State Machine Structure

```rust
pub struct AetherStateMachine {
    /// Applied log index (must be persisted)
    last_applied: u64,
    /// Storage engine for user data
    storage: Arc<dyn StorageEngine>,
    /// Watch event notifier
    watch_tx: tokio::sync::broadcast::Sender<WatchEvent>,
    /// Lease manager
    lease_manager: Arc<LeaseManager>,
}
```

## Log Store Rules

### Persistence

- `HardState` (current_term, voted_for) must be persisted before responding to Vote
- Log entries must be persisted before AppendEntries response
- Use `WriteOptions::set_sync(true)` for all Raft log writes

### Column Families

- `raft_log`: log entries indexed by `[index: u64 BE]` → protobuf Entry
- `raft_state`: HardState, ConfState, last_purged, snapshot_index, snapshot_term

### Snapshot

- Snapshot includes full state machine dump
- Use RocksDB checkpoint for consistent snapshot
- Snapshot metadata must include: last_included_index, last_included_term
- Snapshot transfer uses streaming RPC

## Network Layer Rules

### RPC Timeouts

| RPC | Timeout |
|-----|---------|
| AppendEntries | 5s |
| Vote | 2s |
| InstallSnapshot | 60s (large data) |

### Error Handling

- Network errors → return `RPCError::Network` (triggers retry)
- Mismatched term → return `RPCError::HigherTerm` (triggers step-down)
- Never panic on network errors

### Membership Changes

- One change at a time (add OR remove, not both)
- Wait for previous change to commit before proposing new one
- Learner → Voter requires explicit promote request

## Leader Rules

- Send heartbeat within `heartbeat_interval` (default 1s)
- Do not serve linearizable reads until ReadIndex confirmed
- Track follower match indexes for commit calculation
- On becoming leader: commit a no-op entry to establish leadership

## Follower Rules

- Reject requests from stale leader (term < current_term)
- Reply with current_term in all responses
- On receiving higher term: step down immediately
- Do not serve reads without ReadIndex or lease confirmation

## Candidate Rules

- Vote for self on entering candidate state
- Set election timeout with jitter: `base + random(0, base)`
- On receiving majority votes: become leader
- On receiving AppendEntries from valid leader: become follower
- On election timeout: increment term, start new election

## See Also

- [storage-conventions.md](storage-conventions.md) — RocksDB column families for Raft log and state
- [api-design.md](api-design.md) — How client requests reach the Raft layer
