# Storage Layer Conventions

## Column Family Strategy

Use separate RocksDB column families for different data categories:

| CF Name | Data | Key Encoding | Value |
|---------|------|--------------|-------|
| `default` | User KV data | raw key bytes | raw value bytes |
| `meta` | Internal metadata (revision counter, cluster config) | key prefix | rkyv encoded |
| `raft_log` | Raft log entries | `[term][index]` big-endian | rkyv encoded LogEntry |
| `raft_state` | HardState, ConfState | fixed key names | rkyv encoded |
| `mvcc` | MVCC versioned data | `[key][revision]` | rkyv encoded versioned value |
| `lease` | Lease data | lease_id | rkyv encoded LeaseInfo |

## Key Encoding

### User Keys

Store raw bytes as-is in the `default` CF. No encoding overhead for the common path.

### Raft Log Keys

```rust
fn raft_log_key(term: u64, index: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&term.to_be_bytes());
    key[8..16].copy_from_slice(&index.to_be_bytes());
    key
}
```

Big-endian encoding ensures lexicographic order matches logical order.

### MVCC Keys

Key length must be encoded to avoid ambiguity between key bytes and revision bytes:

```rust
fn mvcc_key(key: &[u8], revision: u64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8 + key.len() + 8);
    encoded.extend_from_slice(&(key.len() as u64).to_be_bytes());
    encoded.extend_from_slice(key);
    encoded.extend_from_slice(&revision.to_be_bytes());
    encoded
}

fn decode_mvcc_key(encoded: &[u8]) -> (&[u8], u64) {
    let key_len = u64::from_be_bytes(encoded[0..8].try_into().unwrap()) as usize;
    let key = &encoded[8..8 + key_len];
    let revision = u64::from_be_bytes(encoded[8 + key_len..16 + key_len].try_into().unwrap());
    (key, revision)
}
```

### Range Key Encoding

For range operations, follow etcd convention:
- Empty `range_end` → single key operation
- `range_end = "\0"` → all keys from `key` to end of keyspace
- Prefix scan for key `"foo"` → set `range_end = "fop"` (next lexicographic key)
- `range_end = b"key\x00"` → exclusive end (scan `[key, key\x00)`)

## Write Operations

### Batch Writes

Always use `WriteBatch` for multi-key atomic operations:

```rust
// BAD: multiple individual puts (not atomic)
storage.put(key1, val1)?;
storage.put(key2, val2)?; // if this fails, key1 is already written

// GOOD: atomic batch
let batch = vec![
    WriteOp::Put(key1, val1),
    WriteOp::Put(key2, val2),
];
storage.batch_write(batch)?; // all or nothing
```

### Sync Policy

- Raft log writes: `WriteOptions::set_sync(true)` — durability is critical
- State machine writes: can be async (`set_sync(false)`) since Raft log provides durability
- Snapshot: sync before reporting success

## Read Operations

### Prefix Scan

```rust
fn scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<KvPair>> {
    let mut results = Vec::new();
    let iter = self.db.iterator_cf(
        self.default_cf(),
        IteratorMode::From(prefix, Direction::Forward),
    );
    for item in iter {
        let (key, value) = item?;
        if !key.starts_with(prefix) || results.len() >= limit {
            break;
        }
        results.push(KvPair { key: key.to_vec(), value: value.to_vec() });
    }
    Ok(results)
}
```

### Consistent Snapshot Reads

For consistent reads across multiple keys, use a RocksDB snapshot:

```rust
let snapshot = db.snapshot();
let cf = db.cf_handle("default").unwrap();
let val1 = snapshot.get_cf(cf, key1)?;
let val2 = snapshot.get_cf(cf, key2)?;
// val1 and val2 are consistent as of the snapshot point
```

## TTL / Lease Integration

- Do NOT store TTL in the storage engine directly
- TTL is managed by the Lease layer
- When a lease expires, the Lease layer issues Delete commands through Raft
- Storage engine only stores raw key-value pairs

## Compaction Strategy

- Raft log: compact after snapshot (delete log entries older than snapshot index)
- MVCC: compact old revisions based on user-specified revision
- User data: use RocksDB's built-in compaction (leveled compaction)
- Run manual compaction after bulk deletes to reclaim space

## Error Recovery

- RocksDB corruption: attempt `RepairDB` first, then restore from snapshot
- Always log RocksDB errors with full context (cf name, key, operation)
- Map RocksDB errors to `StorageError` variants with context

## See Also

- [raft-conventions.md](raft-conventions.md) — Raft log storage and state machine integration
- [anti-patterns.md](anti-patterns.md) — Storage anti-patterns (column family misuse, key encoding)
