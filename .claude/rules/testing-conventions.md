# Testing Conventions

## Test Organization

```
tests/
├── storage_test.rs       # Storage engine integration tests
├── raft_test.rs          # Raft consensus tests
├── api_test.rs           # gRPC API end-to-end tests
├── cluster_test.rs       # Multi-node cluster tests
├── watch_test.rs         # Watch mechanism tests
├── lease_test.rs         # Lease/TTL tests
├── auth_test.rs          # Authentication tests
├── mvcc_test.rs          # MVCC tests
├── txn_test.rs           # Transaction tests
├── lock_test.rs          # Distributed lock tests
├── election_test.rs      # Leader election tests
└── common/
    ├── mod.rs            # Shared test utilities
    ├── cluster.rs        # Test cluster bootstrap helper
    └── client.rs         # Test client helper
```

## Unit Test Rules

- Place in `#[cfg(test)] mod tests` at the bottom of the source file
- Test the module's public API, not internal implementation details
- Each test function tests exactly one behavior
- Test name format: `test_<what>_<condition>_<expected>`
  - `test_put_returns_prev_kv_when_requested`
  - `test_scan_returns_empty_for_missing_prefix`
  - `test_lease_expires_after_ttl`

## Integration Test Rules

- Place in `tests/` directory
- Each test file focuses on one feature area
- Use shared test utilities from `tests/common/`
- Tests must be runnable in parallel (no shared mutable state)
- Clean up resources (temp dirs, ports) in test teardown

## Test Cluster Helper

```rust
// tests/common/cluster.rs
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub temp_dir: tempfile::TempDir,
}

pub struct TestNode {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft<TypeConfig>,
    pub storage: Arc<RocksStorage>,
    pub shutdown: tokio::sync::watch::Sender<bool>,
}

impl TestCluster {
    /// Bootstrap a cluster of `n` nodes for testing
    pub async fn start(n: usize) -> Self { ... }

    /// Stop a specific node (simulate crash)
    pub async fn stop_node(&mut self, id: NodeId) { ... }

    /// Restart a previously stopped node
    pub async fn restart_node(&mut self, id: NodeId) { ... }

    /// Get the current leader node
    pub async fn leader(&self) -> &TestNode { ... }

    /// Wait until a leader is elected
    pub async fn wait_for_leader(&self, timeout: Duration) -> NodeId { ... }

    /// Write a key through the leader
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> { ... }

    /// Read a key from the leader
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { ... }
}
```

## Async Test Patterns

```rust
#[tokio::test]
async fn test_put_get_roundtrip() {
    let cluster = TestCluster::start(1).await;
    cluster.put(b"key", b"value").await.unwrap();
    let result = cluster.get(b"key").await.unwrap();
    assert_eq!(result, Some(b"value".to_vec()));
}

#[tokio::test]
async fn test_replication_to_followers() {
    let cluster = TestCluster::start(3).await;
    let leader = cluster.leader().await;

    // Write through leader
    cluster.put(b"key", b"value").await.unwrap();

    // Read from each follower to verify replication
    for node in &cluster.nodes {
        if node.id != leader.id {
            // Wait for replication
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if node.storage.get(b"key").unwrap() == Some(b"value".to_vec()) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }).await.expect("replication timed out");
        }
    }
}
```

## Waiting for Conditions

Never use `sleep` to wait for async conditions. Use polling with timeout:

```rust
pub async fn wait_for<F, Fut>(condition: F, timeout: Duration) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("condition not met within {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

## Test Data

- Use deterministic test data: `b"key-001"`, `b"value-001"`
- For random data, use a seeded RNG for reproducibility
- Use `tempfile::tempdir()` for all disk-based tests — auto-cleaned on drop

## Property-Based Testing

For complex logic (encoding, serialization), consider property-based tests:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn mvcc_key_encoding_roundtrip(key in prop::collection::vec(any::<u8>(), 0..1000), rev in 0u64..u64::MAX) {
        let encoded = mvcc_key(&key, rev);
        let (decoded_key, decoded_rev) = decode_mvcc_key(&encoded);
        prop_assert_eq!(decoded_key, key);
        prop_assert_eq!(decoded_rev, rev);
    }
}
```

## Benchmark Rules

- Place in `benches/` directory using `criterion`
- Benchmark the critical path: storage read/write, Raft proposal, serialization
- Name benchmarks: `<operation>_<data_size>`, e.g., `put_1kb`, `scan_1000_keys`
- Include baseline comparison when optimizing

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_put(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();
    let value = vec![0u8; 1024]; // 1KB value

    c.bench_function("put_1kb", |b| {
        b.iter(|| {
            storage.put(b"bench-key", &value).unwrap();
        })
    });
}
```

## See Also

- [rust-conventions.md](rust-conventions.md) — General Rust coding style
- [anti-patterns.md](anti-patterns.md) — Testing anti-patterns (sleep, order dependency)
