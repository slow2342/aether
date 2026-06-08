# Anti-Patterns & Common Mistakes

## Rust Anti-Patterns

### Error Handling

```rust
// BAD: unwrap in library code
let value = storage.get(key).unwrap();

// BAD: swallowing errors
let _ = storage.put(key, value);

// BAD: stringly-typed errors
return Err("something went wrong".into());

// GOOD: typed errors with context
storage.put(key, value).map_err(|e| StorageError::WriteFailed { key: key.to_vec(), source: e })?;
```

### Cloning

```rust
// BAD: unnecessary clone when ownership not needed
fn process(data: Vec<u8>) { ... }
process(value.clone()); // value not used after

// BAD: cloning Arc contents
let inner = arc.lock().unwrap().clone(); // clones the whole data

// GOOD: clone only when needed
fn process(data: &[u8]) { ... }
process(&value);

// GOOD: clone Arc, not contents
let arc_clone = Arc::clone(&arc);
```

### Locking

```rust
// BAD: holding lock across .await
let guard = rwlock.read().await;
let result = some_async_fn(guard).await; // other writers blocked
drop(guard);

// BAD: nested locks (potential deadlock)
let a = lock_a.lock().await;
let b = lock_b.lock().await; // if another task holds b and waits for a → deadlock

// GOOD: hold lock briefly, extract data
let data = {
    let guard = rwlock.read().await;
    guard.get_value()
}; // lock released here
some_async_fn(data).await;

// GOOD: always acquire locks in consistent order
let a = lock_a.lock().await;
let b = lock_b.lock().await;
```

### Async Pitfalls

```rust
// BAD: blocking in async context
std::thread::sleep(Duration::from_secs(1)); // blocks the tokio runtime thread
std::fs::read_to_string("file")?;           // blocking I/O

// GOOD: async equivalents
tokio::time::sleep(Duration::from_secs(1)).await;
tokio::fs::read_to_string("file").await?;

// BAD: spawning without JoinHandle tracking
tokio::spawn(async { ... }); // panic is silently swallowed

// GOOD: handle spawn result
let handle = tokio::spawn(async { ... });
// Later: handle.await?? to propagate panics

// BAD: unbounded channel (memory leak risk)
let (tx, rx) = mpsc::unbounded_channel();

// GOOD: bounded channel with backpressure
let (tx, mut rx) = mpsc::channel(1024);
```

### Iterator Patterns

```rust
// BAD: collect then iterate
let items: Vec<_> = iter.filter(|x| ...).collect();
for item in &items { ... }

// GOOD: chain directly
for item in iter.filter(|x| ...) { ... }

// BAD: indexing into Vec
for i in 0..vec.len() {
    process(&vec[i]);
}

// GOOD: iterate directly
for item in &vec {
    process(item);
}
```

## Distributed Systems Anti-Patterns

### Consensus

```rust
// BAD: reading from follower without ReadIndex
// Follower may have stale data
let value = local_storage.get(key)?;

// GOOD: use ReadIndex for linearizable reads
let read_index = raft.ensure_linearizable().await?;
// wait until state machine applied up to read_index
let value = local_storage.get(key)?;

// BAD: modifying state machine directly
state_machine.put(key, value);

// GOOD: always go through Raft log
raft.client_write(request).await?;
```

### Storage

```rust
// BAD: using default column family for everything
db.put(key, value)?;

// GOOD: separate column families for different data types
let cf = db.cf_handle("raft_logs").unwrap();
db.put_cf(cf, key, value)?;

// BAD: encoding keys without prefix separation
let key = format!("{}:{}", name, id); // "user:1" could collide with "us:er1"

// GOOD: use length-prefixed or null-separated encoding
let key = encode_key(name, id); // [len(name)][name][id]
```

### Serialization

```rust
// BAD: using serde for hot-path Raft log entries
// serde has significant overhead for high-frequency operations
#[derive(Serialize, Deserialize)]
struct LogEntry { ... }

// GOOD: use rkyv for zero-copy deserialization
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct LogEntry { ... }
```

### Networking

```rust
// BAD: no timeout on RPC calls
let response = client.get(request).await?;

// GOOD: always set timeout
let response = tokio::time::timeout(
    Duration::from_secs(5),
    client.get(request)
).await??;

// BAD: retrying without backoff
loop {
    match client.connect().await {
        Ok(conn) => break conn,
        Err(_) => continue, // tight loop, potential DoS
    }
}

// GOOD: exponential backoff with jitter
let mut backoff = Duration::from_millis(100);
loop {
    match client.connect().await {
        Ok(conn) => break conn,
        Err(_) => {
            tokio::time::sleep(backoff + random_jitter()).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}
```

## Testing Anti-Patterns

```rust
// BAD: test depends on execution order
#[test]
fn test_step_1() { DB.put("key", "v1"); }
#[test]
fn test_step_2() { assert_eq!(DB.get("key"), "v1"); } // assumes step_1 ran first

// GOOD: each test sets up its own state
#[test]
fn test_get_after_put() {
    let db = create_test_db();
    db.put("key", "v1").unwrap();
    assert_eq!(db.get("key").unwrap(), Some("v1".into()));
}

// BAD: sleeping to wait for async condition
tokio::time::sleep(Duration::from_secs(1)).await;
assert!(condition_met());

// GOOD: polling with timeout
tokio::time::timeout(Duration::from_secs(5), async {
    loop {
        if condition_met() { return; }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}).await.expect("timed out waiting for condition");
```

## See Also

- [rust-conventions.md](rust-conventions.md) — Canonical coding rules
- [storage-conventions.md](storage-conventions.md) — Key encoding and column family patterns
- [raft-conventions.md](raft-conventions.md) — Raft consensus rules
