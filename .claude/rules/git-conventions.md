# Git Conventions

## Commit Message Format

Use Conventional Commits: `<type>(<scope>): <description>`

### Types

| Type | When to use |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `refactor` | Restructure code without changing behavior |
| `test` | Add or update tests |
| `docs` | Documentation only |
| `chore` | Build, CI, tooling, dependencies |
| `perf` | Performance improvement |
| `style` | Formatting, whitespace (no logic change) |

### Scopes

Use the module name as scope: `storage`, `raft`, `api`, `cluster`, `watch`, `lease`, `auth`, `shard`, `client`, `config`, `error`, `session`, `lock`, `election`, `gateway`, `proto`

Use `deps` for dependency updates, `ci` for CI changes.

### Rules

- Subject line: imperative mood, lowercase after type/scope, no period at end, max 72 chars
- Body: wrap at 72 chars, explain **why** not **what** (the diff shows what)
- Footer: reference issues with `Closes #123` or `Refs #123`

### Examples

```
feat(storage): implement RocksDB storage engine with prefix scan

Add StorageEngine trait with get/put/delete/scan operations.
RocksDB implementation uses default column family with rkyv codec.

Closes #12
```

```
fix(raft): handle stale vote response during leader election

Previously a stale vote from a previous term could cause a node
to incorrectly believe it won the election. Now we validate the
term in the vote response before proceeding.
```

```
refactor(api): extract KV service into separate module

The api/server.rs was growing beyond 500 lines. Split KV-specific
handlers into api/kv.rs following the existing module pattern.
```

### What NOT to commit

- `.env` files, credentials, secrets
- IDE-specific files (.idea/, .vscode/)
- Build artifacts (target/)
- `.claude/settings.local.json` (personal local settings)
- **AI/Spec generated files** — never commit spec files, plan files, or any AI-generated artifacts. Do not move them to another directory to bypass this rule.

### What MUST be committed

- `Cargo.lock` — required for reproducible builds (this is a binary application)

## Branch Naming

- `feat/<feature>` — New features (e.g., `feat/storage-engine`)
- `fix/<issue>` — Bug fixes (e.g., `fix/raft-vote-stale`)
- `refactor/<area>` — Refactoring (e.g., `refactor/api-modules`)
- `test/<area>` — Test additions (e.g., `test/storage-integration`)
- `chore/<task>` — Tooling/config (e.g., `chore/claude-config`)
- `docs/<area>` — Documentation (e.g., `docs/architecture`)

## Commit Author

- Never include `Co-Authored-By` or any AI attribution in commit messages
- Commits are authored by the human user only

## PR Conventions

- One logical change per PR
- PR title follows commit message format
- PR body format:

```markdown
## Summary

Brief description of what this PR does and why.

## Changes

- **path/to/file.rs** — What changed and why.
- **path/to/other.rs** — What changed and why.

## Testing

Describe any tests added or modified. Skip this section if no test changes.

## How to verify

Steps to verify the changes work correctly, e.g.:
- Commands to run (cargo build, cargo test, etc.)
- Manual verification steps if applicable

## Related issues

Refs #123, #456
```

- Reference related issues with `Refs #<issue>`
- Skip this section if no related issues
- Do not use `Closes` — let the user decide when to close issues manually

- List every changed file with a short explanation of what was modified and the reason
- All CI checks must pass before merge
- `cargo clippy -- -D warnings` must pass
- No `unsafe` code without explicit justification

## See Also

- [versioning-conventions.md](versioning-conventions.md) — Version bump rules, CHANGELOG format, release process
- [ci-conventions.md](ci-conventions.md) — Required CI checks before merge
- [rust-conventions.md](rust-conventions.md) — Code style and naming conventions
