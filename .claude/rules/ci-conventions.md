# CI/CD Conventions

## Pipeline Overview

```yaml
# .github/workflows/ci.yml — runs on every push and PR
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: "-D warnings"
```

## Required Checks (all must pass to merge)

### 1. Build

```yaml
build:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: dtolnay/rust-toolchain@stable
    - uses: Swatinem/rust-cache@v2
      with:
        cache-targets: true
    - run: cargo build --all-targets
```

Build is Linux-only (x86_64 + aarch64). macOS is not tested — production runs on Linux.

### 2. Test

```yaml
test:
  needs: build
  steps:
    - run: cargo test --all-targets
    - run: cargo test --doc  # doc tests
```

### 3. Clippy

```yaml
clippy:
  needs: build
  steps:
    - run: cargo clippy --all-targets -- -D warnings
```

### 4. Format Check

```yaml
fmt:
  steps:
    - run: cargo fmt -- --check
```

### 5. Security Audit

```yaml
audit:
  steps:
    - uses: rustsec/audit-check@v2.0.0
      with:
        token: ${{ secrets.GITHUB_TOKEN }}
```

### 6. Proto Compilation

```yaml
proto:
  steps:
    - run: cargo build  # verifies proto compilation via build.rs
    - run: |
        # Verify generated code is up to date
        git diff --exit-code src/generated/
```

## Cache Strategy

```yaml
- uses: Swatinem/rust-cache@v2
  with:
    # Cache key based on Cargo.lock
    key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
    # Cache target/ for faster builds
    cache-targets: true
```

## Merge Requirements

| Check | Must Pass | Block Merge |
|-------|-----------|-------------|
| build (stable, linux) | Yes | Yes |
| build (aarch64) | Yes | Yes |
| test | Yes | Yes |
| clippy | Yes (0 warnings) | Yes |
| fmt | Yes | Yes |
| audit | Yes (no critical/high) | Yes |
| build (nightly) | Best effort | No |

## Branch Protection Rules (GitHub Settings)

- Require pull request reviews before merging (1 minimum)
- Require status checks to pass before merging
- Require branches to be up to date before merging
- Do not allow bypassing the above settings
- Allow force pushes: No
- Allow deletions: No

## Release CI

```yaml
# .github/workflows/release.yml — runs on tag push
name: Release

on:
  push:
    tags: ['v*']

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo build --release
      - uses: softprops/action-gh-release@v2
        with:
          files: target/release/aether
          generate_release_notes: true
```

## Benchmark CI (optional)

```yaml
bench:
  if: github.event_name == 'push' && github.ref == 'refs/heads/main'
  steps:
    - run: cargo bench -- --output-format bencher | tee output.txt
    - uses: benchmark-action/github-action@v1
      with:
        name: Benchmarks
        tool: cargo
        output-file-path: output.txt
        auto-push: true
```

## Local CI Simulation

Before pushing, run locally:

```bash
# Full CI check (add to Makefile or just script)
cargo fmt -- --check && \
cargo clippy --all-targets -- -D warnings && \
cargo test --all-targets && \
cargo test --doc && \
cargo audit
```

## Concurrency

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true  # cancel old runs on same branch
```

## Notifications

- Failed builds on `main`: notify via Slack/email
- Security audit failures: create GitHub issue automatically
- Release published: notify via GitHub release event

## See Also

- [docker-conventions.md](docker-conventions.md) — Docker build in CI, image scanning
- [versioning-conventions.md](versioning-conventions.md) — Release process and tagging
