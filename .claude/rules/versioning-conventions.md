# Versioning & Release Conventions

## Semantic Versioning

Follow SemVer: `MAJOR.MINOR.PATCH`

```toml
[package]
version = "0.1.0"
```

### Version Bump Rules

| Change Type | Bump | Example |
|-------------|------|---------|
| New feature (backward compatible) | MINOR | 0.1.0 → 0.2.0 |
| Bug fix (backward compatible) | PATCH | 0.1.0 → 0.1.1 |
| Breaking API change | MAJOR | 0.9.0 → 1.0.0 |
| New module added | MINOR | 0.1.0 → 0.2.0 |
| Proto definition change (wire compatible) | MINOR | 0.1.0 → 0.2.0 |
| Proto definition change (wire incompatible) | MAJOR | 0.9.0 → 1.0.0 |
| Dependency upgrade (no API change) | PATCH | 0.1.0 → 0.1.1 |
| Internal refactor (no API change) | PATCH | 0.1.0 → 0.1.1 |

### Pre-1.0 Rules

- During 0.x.y, breaking changes bump MINOR: 0.1.0 → 0.2.0
- PATCH is reserved for bug fixes only
- Each MINOR release may have breaking changes — document them in CHANGELOG

## CHANGELOG Format

Use Keep a Changelog format:

```markdown
# Changelog

## [Unreleased]

## [0.2.0] - 2026-06-15

### Added
- MVCC multi-version concurrency control
- Transaction support (compare-and-swap)

### Changed
- StorageEngine trait now includes revision parameter in get()

### Fixed
- Raft election timeout jitter calculation

### Deprecated
- `StorageEngine::get_simple()` — use `StorageEngine::get()` instead

### Removed
- (nothing removed)

### Security
- Upgraded tonic to 0.14.1 to fix CVE-2026-XXXX

## [0.1.0] - 2026-06-01

### Added
- Initial release
- RocksDB storage engine
- Raft consensus (single node)
- gRPC API (Put/Get/Delete/Range)
```

### Rules

- Every PR that changes behavior gets a CHANGELOG entry under `[Unreleased]`
- On release: move `[Unreleased]` entries to the new version with date
- Group entries by: Added, Changed, Fixed, Deprecated, Removed, Security
- Reference issue/PR numbers: `(#123)`
- Keep entries concise — one line per change

## Release Process

### 1. Prepare Release

```bash
# Ensure all tests pass
cargo test
cargo clippy -- -D warnings

# Update version in Cargo.toml
# Update CHANGELOG.md — move Unreleased to new version

# Commit
git commit -m "chore(release): prepare v0.2.0"
```

### 2. Tag

```bash
# Tag format: v{version}
git tag -a v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0
```

### 3. Publish (if library)

```bash
cargo publish --dry-run  # verify first
cargo publish
```

### 4. GitHub Release

Create GitHub release from tag with:
- Release notes from CHANGELOG
- Binary artifacts (if applicable)
- Migration guide (for breaking changes)

## Proto Versioning

Proto files are versioned with the crate — no separate versioning.

- Wire-compatible changes (adding optional fields): MINOR bump
- Wire-incompatible changes (removing fields, changing types): MAJOR bump
- Never remove or reuse field numbers
- Use `reserved` to mark removed fields

```protobuf
message PutResponse {
    ResponseHeader header = 1;
    KeyValue prev_kv = 2;
    reserved 3;  // was: bool created (removed in v0.3.0)
}
```

## Compatibility Matrix

Document supported versions:

| Component | Supported Versions |
|-----------|-------------------|
| Rust | 1.85+ (Edition 2024) |
| OS | Linux, macOS |
| Arch | x86_64, aarch64 |
| RocksDB | 0.24.x |
| gRPC protocol | tonic 0.14.x |

## Breaking Change Migration

For breaking changes, provide a migration guide in the CHANGELOG:

````markdown
### Migration Guide: v0.1.x → v0.2.0

**StorageEngine trait change:**
```rust
// Before (v0.1.x)
fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

// After (v0.2.0)
fn get(&self, key: &[u8], revision: Option<u64>) -> Result<Option<Vec<u8>>>;
// Pass None for latest revision (equivalent to old behavior)
```
````

## See Also

- [git-conventions.md](git-conventions.md) — Commit message format and branch naming
- [ci-conventions.md](ci-conventions.md) — CI checks required before release
