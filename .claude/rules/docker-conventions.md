# Docker Conventions

## Dockerfile Best Practices

### Multi-stage Build

Use multi-stage builds to separate compilation from runtime, minimizing final image size:

```dockerfile
# === Build stage ===
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Copy dependency manifests first to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./
# Create empty src to trigger dependency download
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src

# Copy source and rebuild
COPY . .
RUN cargo build --release

# === Runtime stage ===
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run as non-root user
RUN groupadd -r aether && useradd -r -g aether -d /data aether

COPY --from=builder /app/target/release/aether /usr/local/bin/aether

# Create data directory
RUN mkdir -p /data && chown aether:aether /data

USER aether
WORKDIR /data

EXPOSE 2379 2380 9090

ENTRYPOINT ["aether"]
CMD ["--config", "/etc/aether/config.toml"]
```

### Rules

- Base images must use official images with pinned version tags (`rust:1.85-bookworm`, not `rust:latest`)
- Runtime image: use `debian:bookworm-slim` or `distroless`, not full `debian`
- Must run as non-root user
- Each `RUN` instruction must clean up caches (`rm -rf /var/lib/apt/lists/*`)
- Leverage build cache: copy `Cargo.toml` + `Cargo.lock` first, then source
- Never store secrets, certificates, or config files in the image (inject via mounts or env vars)

## Port Allocation

| Port | Purpose |
|------|---------|
| 2379 | Client gRPC API |
| 2380 | Inter-node Raft communication |
| 9090 | Metrics (Prometheus) |
| 9091 | Health check (HTTP) |

## .dockerignore

```
/target
.git
.github
.claude
.idea
.vscode
*.swp
*.swo
.DS_Store
.env
.env.*
benches/
tests/
criterion/
README.md
CHANGELOG.md
LICENSE
```

## Docker Compose (Development)

```yaml
# docker-compose.yml — development/testing
version: "3.8"

services:
  aether-1:
    build: .
    ports:
      - "2379:2379"
      - "2380:2380"
    volumes:
      - aether-1-data:/data
    command: ["--node-id", "1", "--addr", "aether-1:2380", "--join", "aether-1:2380,aether-2:2380,aether-3:2380"]

  aether-2:
    build: .
    ports:
      - "2381:2379"
      - "2382:2380"
    volumes:
      - aether-2-data:/data
    command: ["--node-id", "2", "--addr", "aether-2:2380", "--join", "aether-1:2380,aether-2:2380,aether-3:2380"]

  aether-3:
    build: .
    ports:
      - "2383:2379"
      - "2384:2380"
    volumes:
      - aether-3-data:/data
    command: ["--node-id", "3", "--addr", "aether-3:2380", "--join", "aether-1:2380,aether-2:2380,aether-3:2380"]

volumes:
  aether-1-data:
  aether-2-data:
  aether-3-data:
```

### Compose Rules

- Development environment provides 3-node cluster configuration
- Use named volumes for data persistence
- Avoid port conflicts in host mapping (2379/2381/2383)
- Do not hardcode config in compose file — use CLI args or mount config files

## Image Tag Strategy

### Tag Format

| Tag | Purpose | Example |
|-----|---------|---------|
| `v{version}` | Official release | `v0.1.0`, `v1.2.3` |
| `{version}` | Without v prefix | `0.1.0`, `1.2.3` |
| `{major}.{minor}` | Latest patch | `0.1`, `1.2` |
| `{major}` | Latest minor | `0`, `1` |
| `latest` | Latest stable | Points to latest official release only |
| `sha-{commit}` | Specific commit | `sha-abc1234` |
| `pr-{number}` | PR build | `pr-42` (CI only, never pushed) |

### Rules

- `latest` must point to the latest official release, never to pre-release or nightly
- Push full version tag + major.minor + major on every release
- CI builds use `sha-{commit}` tags for traceability
- Never overwrite published tags (immutable)

## CI Docker Build

```yaml
# .github/workflows/docker.yml
docker:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4

    - uses: docker/setup-buildx-action@v3

    - uses: docker/login-action@v3
      with:
        registry: ghcr.io
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    - uses: docker/build-push-action@v5
      with:
        context: .
        push: ${{ github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v') }}
        tags: |
          ghcr.io/${{ github.repository }}:${{ github.ref_name }}
          ghcr.io/${{ github.repository }}:latest
        cache-from: type=gha
        cache-to: type=gha,mode=max
        platforms: linux/amd64,linux/arm64
```

### CI Rules

- Use GitHub Actions cache to speed up builds
- Multi-platform build: `linux/amd64` + `linux/arm64`
- Push image to registry only on tag push
- PR builds only verify compilation, never push

## Health Check

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["aether", "health"]
```

Or via HTTP:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:9091/health || exit 1
```

### Health Check Rules

- HEALTHCHECK must be configured
- `start-period` must allow sufficient startup time (Raft election takes time)
- Check Raft state and storage availability, not just process liveness

## Resource Limits

```yaml
# docker-compose.yml
services:
  aether-1:
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 2G
        reservations:
          cpus: "0.5"
          memory: 512M
```

### Resource Rules

- Production must set memory limits (RocksDB memory usage is unbounded without limits)
- CPU limits depend on cluster size
- Reserve resources to ensure minimum availability

## Logging

```dockerfile
# Log to stdout/stderr for Docker log collection
ENV RUST_LOG=info
ENV RUST_LOG_FORMAT=json
```

### Logging Rules

- Container logs must output to stdout/stderr
- Use JSON format for log aggregation system parsing
- Never write log files inside the container
- Control log level via environment variables

## Security

### Image Scanning

```yaml
# Scan image for vulnerabilities in CI
- uses: aquasecurity/trivy-action@master
  with:
    image-ref: ghcr.io/${{ github.repository }}:${{ github.ref_name }}
    severity: CRITICAL,HIGH
    exit-code: 1
```

### Security Rules

- CI must scan images for vulnerabilities (Trivy or Grype)
- CRITICAL/HIGH vulnerabilities block release
- Never store secrets in the image (use Docker secrets or K8s secrets)
- Use distroless or minimal base images to reduce attack surface
- Regularly update base images for security patches

## See Also

- [ci-conventions.md](ci-conventions.md) — CI pipeline for Docker builds
- [security-conventions.md](security-conventions.md) — Security rules and auth patterns
