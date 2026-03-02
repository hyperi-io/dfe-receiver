<!--
  Project:      dfe-receiver
  File:         docs/CONTAINER-PUBLISHING.md
  Purpose:      Spec for GHCR container image publishing via CI
  Language:     Markdown

  License:      FSL-1.1-ALv2
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Container Image Publishing — dfe-receiver

## Goal

Publish multi-arch container images for dfe-receiver to GitHub Container
Registry (GHCR) on every release, so downstream projects (dfe-docker,
dfe-operator) can reference `ghcr.io/hyperi-io/dfe-receiver:x.y.z` instead of
building from source or downloading binaries from JFrog.

## How It Works

The CI submodule (v1.59.0+) has built-in container publishing support. On
release, it:

1. Builds the Dockerfile for `linux/amd64` and `linux/arm64`
2. Pushes to `ghcr.io/hyperi-io/dfe-receiver` with semantic version tags
3. Verifies the image exists in GHCR

No extra secrets needed — uses `GITHUB_TOKEN` automatically.

## Tags Generated

For a release `v1.7.0`:

- `ghcr.io/hyperi-io/dfe-receiver:1.7.0`
- `ghcr.io/hyperi-io/dfe-receiver:1.7`
- `ghcr.io/hyperi-io/dfe-receiver:1`
- `ghcr.io/hyperi-io/dfe-receiver:latest`
- `ghcr.io/hyperi-io/dfe-receiver:sha-<commit>`

Pre-release versions (e.g. `1.7.0-beta.1`) only get the full version tag.

## Implementation

### 1. Create Dockerfile

Create `Dockerfile` in the repo root. The binary is already cross-compiled in
CI for both amd64 and arm64, so the Dockerfile wraps the pre-built binary
rather than compiling from source.

**Option A — Download from JFrog at build time:**

```dockerfile
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG VERSION

# Map Docker platform arch to binary suffix
RUN case "${TARGETARCH}" in \
      amd64) SUFFIX="linux-amd64" ;; \
      arm64) SUFFIX="linux-arm64" ;; \
      *) echo "Unsupported: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -sf -o /usr/local/bin/dfe-receiver \
        "https://github.com/hyperi-io/dfe-receiver/releases/download/v${VERSION}/dfe-receiver-v${VERSION}-${SUFFIX}" && \
    chmod +x /usr/local/bin/dfe-receiver

RUN useradd --create-home --uid 1000 appuser && \
    mkdir -p /var/spool/dfe-receiver && \
    chown appuser:appuser /var/spool/dfe-receiver

USER appuser

EXPOSE 8080 6000 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/metrics > /dev/null || exit 1

ENTRYPOINT ["dfe-receiver"]
CMD ["--config", "/etc/dfe/receiver.yaml"]
```

**Option B — COPY from CI build artifact:**

If CI builds the binary first and passes it to the Docker build step, use
`COPY` instead of downloading. This avoids the GitHub release chicken-and-egg
problem (release must exist before image build can download it).

```dockerfile
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY dfe-receiver /usr/local/bin/dfe-receiver
RUN chmod +x /usr/local/bin/dfe-receiver

RUN useradd --create-home --uid 1000 appuser && \
    mkdir -p /var/spool/dfe-receiver && \
    chown appuser:appuser /var/spool/dfe-receiver

USER appuser

EXPOSE 8080 6000 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/metrics > /dev/null || exit 1

ENTRYPOINT ["dfe-receiver"]
CMD ["--config", "/etc/dfe/receiver.yaml"]
```

**Recommended:** Option B (COPY from CI artifact). The CI publish workflow
already builds the binary before the container step runs — no circular
dependency.

### 2. Update `.hyperi-ci.yaml`

Add the container publishing section:

```yaml
publish:
  enabled: true
  binaries: both

  container:
    enabled: true
    registry: ghcr
    dockerfile: Dockerfile
    platforms:
      - linux/amd64
      - linux/arm64
```

### 3. Update CI submodule

Ensure the ci submodule is at v1.59.0 or later:

```bash
git submodule update --remote ci
```

### 4. Update publish workflow

Regenerate or update `.github/workflows/publish.yml` to include container
publishing inputs. The CI actions/jobs/publish composite action already handles
container publishing when the config is detected.

## Verification

After a release:

```bash
# Pull and verify
docker pull ghcr.io/hyperi-io/dfe-receiver:1.7.0
docker run --rm ghcr.io/hyperi-io/dfe-receiver:1.7.0 --help

# Check multi-arch
docker manifest inspect ghcr.io/hyperi-io/dfe-receiver:1.7.0
```

## Consumer Usage

### Docker Compose (dfe-docker)

```yaml
services:
  dfe-receiver:
    image: ghcr.io/hyperi-io/dfe-receiver:${DFE_RECEIVER_VERSION:-latest}
    ports:
      - "8080:8080"
      - "9090:9090"
    volumes:
      - ./config/receiver.yaml:/etc/dfe/receiver.yaml:ro
```

### Kubernetes (dfe-operator)

```yaml
containers:
  - name: dfe-receiver
    image: ghcr.io/hyperi-io/dfe-receiver:1.7.0
    ports:
      - containerPort: 8080
```

## Container Checklist

Per HyperI Docker standards:

- [ ] Non-root user (UID 1000)
- [ ] Debug utilities included (curl, netcat)
- [ ] HEALTHCHECK defined
- [ ] EXPOSE ports declared
- [ ] Exec form ENTRYPOINT (receives SIGTERM)
- [ ] No secrets baked in
- [ ] Multi-arch (amd64 + arm64)
