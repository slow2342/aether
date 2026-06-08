# Rust Coding Conventions

## General

- Rust Edition 2024
- All code must pass `cargo clippy -- -D warnings`
- All code must be formatted with `cargo fmt`
- No `#![allow(...)]` at crate level — fix the warning instead

## Naming

- `snake_case`: functions, methods, variables, modules, file names
- `CamelCase` (PascalCase): types, traits, enums
- `SCREAMING_SNAKE_CASE`: constants, statics
- Abbreviations longer than 2 chars use normal casing: `RaftState` not `RAFTState`
- Prefix boolean methods with `is_`, `has_`, `can_`, `should_`

## Error Handling

- Use `thiserror` for library error types — one enum per module
- Use `anyhow::Result` only in `main.rs` and test code
- Never `.unwrap()` in library code
- Use `.expect("reason")` only when the invariant is truly impossible
- Use `?` for all fallible operations
- Implement `From<InnerError>` for outer error types to enable `?` chaining
- Include context in errors — use `.context("doing X")` from anyhow in top-level code

```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("key not found: {key:?}")]
    KeyNotFound { key: Vec<u8> },

    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("codec error: {0}")]
    Codec(String),
}
```

## Async

- All I/O must be async using tokio
- Use `tokio::sync::RwLock` / `tokio::sync::Mutex` for shared state in async code
- Use `std::sync::RwLock` / `std::sync::Mutex` only for CPU-bound data with no `.await` while held
- Prefer `tokio::select!` over polling loops
- Use `tokio::spawn` for independent concurrent tasks
- Use `#[tokio::main]` in main, `#[tokio::test]` in async tests
- Always specify timeout for network operations

## Ownership & Borrowing

- Prefer `&T` over `T` in function parameters when ownership isn't needed
- Use `Arc<T>` for shared ownership across tasks — never `Rc<T>` in async
- Use `Arc<str>` or `Arc<[u8]>` for shared immutable data to avoid cloning
- Prefer returning owned types from public APIs; use references for internal helpers

## Generics & Traits

- Use `impl Trait` for simple return types
- Use explicit generic parameters when the type appears in multiple positions
- Prefer trait objects (`dyn Trait`) when type erasure is acceptable and avoids monomorphization bloat
- Define traits at the consumer site, not the provider site

## Serialization

- `rkyv` for hot-path internal data (Raft logs, state machine entries)
- `serde` for config, API payloads, external formats
- Always derive `Debug` on serialized types
- Use `#[serde(default)]` for optional config fields
- Use `#[serde(skip_serializing_if = "Option::is_none")]` for optional response fields

## Module Structure

- One responsibility per file
- Split files exceeding ~500 lines
- `mod.rs` exports public types and re-exports — no logic in `mod.rs`
- Keep internal types private; expose only what callers need
- Use `pub(crate)` for types shared within the crate but not externally

```rust
// src/storage/mod.rs
mod rocksdb;
mod codec;

pub use self::rocksdb::RocksStorage;
pub use self::codec::Codec;

pub trait StorageEngine: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;
    fn scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<KvPair>, StorageError>;
    fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), StorageError>;
}
```

## Testing

See [testing-conventions.md](testing-conventions.md) for full testing rules including test naming format, organization, and async test patterns.

## Documentation

- No doc comments on private items unless the logic is non-obvious
- Public items: one-line doc comment max, only if the name doesn't explain itself
- Never write doc comments that just repeat the function signature
- Use `// SAFETY:` comments for unsafe blocks explaining why it's sound

## Dependencies

See [dependency-conventions.md](dependency-conventions.md) for full dependency management rules.

## Performance

- Avoid unnecessary allocations — use `Bytes` from `bytes` crate for shared buffers
- Use `SmallVec` or stack arrays for small, bounded collections
- Profile before optimizing — use `criterion` benchmarks in `benches/`
- Prefer iterators over manual loops for clarity and potential auto-vectorization

## See Also

- [anti-patterns.md](anti-patterns.md) — Common Rust mistakes with bad/good examples
- [testing-conventions.md](testing-conventions.md) — Test organization, naming, async patterns
- [dependency-conventions.md](dependency-conventions.md) — Adding and managing crate dependencies
