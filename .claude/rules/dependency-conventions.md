# Dependency Management Conventions

## Adding New Dependencies

Before adding a new crate, answer these questions:

1. **Is it necessary?** Can we solve this with stdlib or existing dependencies?
2. **Is it maintained?** Last commit within 6 months, active maintainer(s), open issues being addressed.
3. **Is it trusted?** Download count, organization backing, known security audits.
4. **Is it lightweight?** Check transitive dependency count with `cargo tree -p <crate> --depth 1`.
5. **Is it compatible?** License must be MIT OR Apache-2.0. No GPL, AGPL, or proprietary licenses.

## Version Pinning Strategy

```toml
[dependencies]
# Major version pinned (default for most crates)
serde = "1.0"

# Minor version pinned for unstable crates
raft = "0.7"

# Exact pin only for known-buggy versions
# BAD: some-crate = "=1.2.3"  (unless documenting a known issue)
```

### Rules

- Pin to major version: `"1"` or `"1.0"` — allows patch updates
- For pre-1.0 crates: pin to minor version: `"0.9"` — allows patch updates within 0.9.x
- Never use `*` (any version)
- Document why a specific version constraint was chosen if non-obvious

## Feature Flags

```toml
# Enable only needed features
tokio = { version = "1", features = ["full"] }  # OK for tokio — we use everything
raft = { version = "0.7", default-features = false, features = ["prost-codec", "default-logger"] }  # OK — specific features

# BAD: enabling default features when only one is needed
some-crate = "1.0"  # pulls in 20 features, we only need 2

# GOOD: disable defaults, enable only what's needed
some-crate = { version = "1.0", default-features = false, features = ["feature-we-need"] }
```

## Dependency Categories

Organize `Cargo.toml` dependencies by category with comments:

```toml
[dependencies]
# --- Raft consensus ---
raft = { version = "0.7", default-features = false, features = ["prost-codec", "default-logger"] }

# --- Storage ---
rocksdb = "0.24.0"

# --- Serialization ---
serde = { version = "1.0", features = ["derive"] }
rkyv = "0.8"
prost = "0.14"

# --- gRPC ---
tonic = "0.14"

# --- Async runtime ---
tokio = { version = "1", features = ["full"] }

# --- Logging ---
tracing = "0.1"
tracing-subscriber = "0.3"

# --- CLI ---
clap = { version = "4", features = ["derive"] }

# --- Error handling ---
anyhow = "1"
thiserror = "2"
```

## Auditing Dependencies

```bash
# Check for known vulnerabilities
cargo audit

# Check for duplicate dependencies
cargo tree -d

# Check dependency licenses
cargo deny check licenses

# Check for unused dependencies
cargo +nightly udeps
```

Run `cargo audit` in CI on every PR. Block merge on critical/high vulnerabilities.

## Upgrading Dependencies

1. Check changelog for breaking changes
2. Run `cargo update` for patch versions (safe)
3. For major/minor bumps: update Cargo.toml, fix compilation, run full test suite
4. One dependency upgrade per commit — do not batch unrelated upgrades
5. Commit message: `chore(deps): upgrade <crate> from X to Y`

## What NOT to Add

- Crates with fewer than 1000 downloads/month (unless niche and critical)
- Crates that pull in OpenSSL when rustls is sufficient
- Crates that depend on C/C++ libraries unless no pure-Rust alternative exists
- Utility crates for trivial functions (random string generation, basic formatting)
- Abandoned crates (no commits in 12+ months, no fork/maintainer)

## Internal Crate Organization

If the project grows, split into workspace members:

```
aether/
├── Cargo.toml          # workspace root
├── aether-core/        # storage, raft, state machine
├── aether-api/         # gRPC service definitions
├── aether-client/      # client SDK
└── aether-proto/       # proto compilation
```

Only split when a single crate exceeds ~10k lines or has distinct versioning needs.

## See Also

- [security-conventions.md](security-conventions.md) — Security requirements for dependencies
- [versioning-conventions.md](versioning-conventions.md) — Version pinning and release process
