# Deployment

Wruntime provides CLI commands for packaging and deploying services to remote hosts. The workflow is **bundle once, deploy anywhere** — a single tarball contains everything needed for both systemd and Docker deployments.

## Prerequisites

Cross-compilation of host binaries uses `cargo-zigbuild`, which bundles a Linux sysroot via Zig:

```bash
brew install zig
cargo install cargo-zigbuild
```

## Overview

| Command | Purpose |
|---------|---------|
| `wr managers bundle` | Package the manager binary + config into a tarball |
| `wr managers deploy` | Push bundle to a remote host and start the service |
| `wr managers status` | Inspect a bundle without deploying |
| `wr managers list` | List active managers in the cluster |
| `wr node bundle` | Package proxy + engine binaries, WASM modules, and schemas |
| `wr node deploy` | Push node bundle to a remote host and start services |
| `wr node status` | Inspect a node bundle without deploying |
| `wr logs node` | View logs from services on a remote node (systemd or Docker) |

## Bundle structure

Bundles are gzip'd tarballs containing cross-compiled binaries, config templates, WASM modules (with pre-compiled `.cwasm` native artifacts), schemas, migrations, and deployment descriptors for both systemd and Docker.

**Manager bundle:**

```
wr-manager/
├── bin/wr-manager
├── config/manager.toml          # template with {db_url}, {advertise_address} placeholders
├── systemd/wr-manager.service   # template with {secret_key} placeholder
├── docker/
│   ├── Dockerfile.manager
│   └── docker-compose.yml
└── manifest.json
```

**Node bundle:**

```
wr-node/
├── bin/
│   ├── wr-proxy
│   └── wr-engine
├── config/
│   ├── proxy.toml               # template generated or sourced from --proxy-config; {db_url}, {host}
│   └── engine.toml              # template with {db_url}
├── modules/
│   ├── order-service.wasm
│   └── order-service.cwasm      # pre-compiled native (Cranelift)
├── schemas/
│   └── order-service.binpb
├── migrations/
│   └── order-service/
│       └── V1__create_tables.sql
├── systemd/
│   ├── wr-proxy.service
│   ├── wr-engine-order-service.service
│   └── 99-wruntime.conf         # sysctl tuning
├── docker/
│   ├── Dockerfile.proxy
│   ├── Dockerfile.engine-order-service
│   └── docker-compose.yml
└── manifest.json
```

## Deploy configuration (`wr-deploy.toml`)

Instead of passing every flag on the command line, you can create a `wr-deploy.toml` in your working directory. Both bundle and deploy commands auto-discover it (or accept `--config <path>` to load a specific file).

**Precedence:** CLI flag > config file > environment variable > default

```toml
# wr-deploy.toml — shared settings for bundle and deploy commands
format     = "systemd"
target     = "aarch64-unknown-linux-gnu"
workdir    = "/opt/wruntime"
proxy_config = "examples/config/proxy.toml"
db_url     = "postgres://postgres@10.0.1.1:5432/wruntime"
secret_key = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
ssh_key    = "~/.ssh/deploy_key"
seed_nodes = ["10.0.1.11:9010", "10.0.1.12:9010"] # metadata/reserved; not emitted into manager runtime TOML
cert_dir   = "./certs"    # CA + node certs from `wr cert`
peer_port  = 9443         # mTLS peer listener port
# ssh_port     = 22
# image_prefix = "wr"
```

All fields are optional. Fields that only apply to specific commands (e.g. `secret_key` for managers) are silently ignored when unused. CLI flags always override the config file.

`proxy_config` applies to `wr node bundle`: when set (or passed as `--proxy-config` / `WR_PROXY_CONFIG`), the node bundle uses that source proxy TOML, templates deploy-varying database/node/TLS values, and preserves proxy runtime sections such as `[circuit_breaker]`, `[egress]`, and `[external]`. When omitted, the CLI keeps generating a minimal proxy config from the engine node settings.

`seed_nodes` is deployment metadata/reserved for future gossip bootstrapping UX. It is accepted from `wr-deploy.toml` / `--seed-node` for compatibility, but the deploy flow does not write `cluster.seed_nodes` into runtime `manager.toml` by default because the runtime manager config has no such field.

**Environment variables** are also supported for all deploy-related fields:

| Flag | Env var | Default |
|------|---------|---------|
| `--format` | `WR_FORMAT` | `systemd` |
| `--db-url` | `WR_DB_URL` | — |
| `--secret-key` | `WR_SECRET_KEY` | — |
| `--ssh-key` | `WR_SSH_KEY` | — |
| `--ssh-port` | `WR_SSH_PORT` | SSH default |
| `--target` | `WR_TARGET` | `x86_64-unknown-linux-gnu` |
| `--proxy-config` | `WR_PROXY_CONFIG` | — |
| `--advertise-address` | `WR_ADVERTISE_ADDRESS` | derived from remote host |
| `--manager` | `WR_MANAGER` | — |
| `--cert-dir` | `WR_CERT_DIR` | — |
| `--peer-port` | `WR_PEER_PORT` | `9443` |

## Template variables

Config files use placeholders that are resolved at deploy time:

| Variable | Resolved from | Used in |
|----------|---------------|---------|
| `{db_url}` | `--db-url` / `WR_DB_URL` / config | manager, proxy, engine configs |
| `{host}` | deploy target (`user@host`) | proxy/engine `[node]` addresses |
| `{secret_key}` | `--secret-key` / `WR_SECRET_KEY` / config | manager systemd unit / Dockerfile |
| `{peer_port}` | `--peer-port` / `WR_PEER_PORT` / config (default: 9443) | proxy config (`peer_port`) |
| `{advertise_address}` | `--advertise-address` / `WR_ADVERTISE_ADDRESS` (auto-derived from remote host if omitted) | manager config (`advertise_grpc_address`) |

Unresolved placeholders cause deployment to fail.

## Single-node deployment (systemd)

The simplest approach is a `wr-deploy.toml` alongside your engine configs:

```toml
# wr-deploy.toml
target     = "aarch64-unknown-linux-gnu"
db_url     = "postgres://postgres@localhost:5432/wruntime"
secret_key = "<64-char-hex-key>"
```

```bash
# 1. Bundle manager (target and output have defaults)
wr-cli managers bundle --manager-config examples/config/manager.toml

# 2. Deploy manager (format defaults to systemd, advertise-address derived from host)
wr-cli managers deploy wr-manager-bundle.tar.gz deploy@10.0.1.1

# 3. Bundle node
wr-cli node bundle --engine-config engine.toml
```

Add `--proxy-config examples/config/proxy.toml` (or set `proxy_config` in `wr-deploy.toml`) when the source proxy config has runtime sections such as egress allowlists, external routes, or non-default circuit-breaker settings that must be preserved in the bundle.

```bash
# 4. Deploy node
wr-cli node deploy wr-node-bundle.tar.gz deploy@10.0.1.1 --manager http://10.0.1.1:9000

# 5. Verify
wr-cli engines list --manager http://10.0.1.1:9000
```

Without the config file, pass all values as flags:

```bash
wr-cli managers bundle \
    --manager-config examples/config/manager.toml \
    --target aarch64-unknown-linux-gnu \
    --output manager.tar.gz

wr-cli managers deploy manager.tar.gz deploy@10.0.1.1 \
    --db-url "postgres://postgres@localhost:5432/wruntime" \
    --secret-key "<64-char-hex-key>"

wr-cli node bundle \
    --engine-config engine.toml \
    --target aarch64-unknown-linux-gnu \
    --output myapp.tar.gz

wr-cli node deploy myapp.tar.gz deploy@10.0.1.1 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000
```

Deploy steps (systemd): SCP tarball, unpack to `--workdir` (default `/opt/wruntime`), install static units, resolve and upload config templates, provision TLS certificates, start services once with the final runtime files in place, then poll manager until the engine registers (60s timeout).

## Multi-node cluster setup

With a shared `wr-deploy.toml`:

```toml
# wr-deploy.toml
target     = "aarch64-unknown-linux-gnu"
db_url     = "postgres://postgres@10.0.1.1:5432/wruntime"
secret_key = "<64-char-hex-key>"
```

```bash
export WR_MANAGER=http://10.0.1.1:9000

# --- Manager (once per cluster) ---

wr-cli managers bundle --manager-config examples/config/manager.toml --output manager.tar.gz
wr-cli managers deploy manager.tar.gz deploy@10.0.1.1

# --- Node A ---

wr-cli node bundle --engine-config examples/multi-node/node-a/engine-1.toml --output node-a.tar.gz
wr-cli node deploy node-a.tar.gz deploy@10.0.1.50

# --- Node B ---

wr-cli node bundle --engine-config examples/multi-node/node-b/engine-1.toml --output node-b.tar.gz
wr-cli node deploy node-b.tar.gz deploy@10.0.1.51
```

Each node's proxy/engine internal listeners (`listen_address`, `control_address`) bind loopback; only the proxy's mTLS peer listener (`peer_port`, default 9443) is reachable across nodes. The example configs above bind loopback accordingly.

Without the config file, pass all values explicitly:

```bash
wr-cli managers bundle \
    --manager-config examples/config/manager.toml \
    --target aarch64-unknown-linux-gnu \
    --output manager.tar.gz

wr-cli managers deploy manager.tar.gz deploy@10.0.1.1 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --secret-key "<64-char-hex-key>"

wr-cli node bundle \
    --engine-config examples/multi-node/node-a/engine-1.toml \
    --target aarch64-unknown-linux-gnu \
    --output node-a.tar.gz

wr-cli node deploy node-a.tar.gz deploy@10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000
```

## Docker deployment

The same bundle works for Docker — override the format via flag, env, or config:

```bash
# Via flag
wr-cli node deploy myapp.tar.gz deploy@10.0.1.50 --format docker

# Via wr-deploy.toml
# format = "docker"

# Or extract locally and run with Docker Compose
tar xzf myapp.tar.gz
cd wr-node && docker compose -f docker/docker-compose.yml up -d
```

## TLS certificates

All inter-service communication uses mTLS. Generate certificates before deployment:

```bash
# 1. Create a CA (once per cluster)
wr-cli cert init-ca --output ./certs/

# 2. Generate per-node certificates (hostname must match the deploy target IP)
wr-cli cert generate 10.0.1.1 --ca-dir ./certs/    # manager
wr-cli cert generate 10.0.1.50 --ca-dir ./certs/   # node A
wr-cli cert generate 10.0.1.51 --ca-dir ./certs/   # node B
```

During `node deploy`, pass `--cert-dir ./certs/` (or set `cert_dir` in `wr-deploy.toml`). The deploy command SCPs `ca.crt`, `<host>.crt`, and `<host>.key` to `{workdir}/wr-node/certs/` on the remote host. The bundled proxy config references these paths.

For local development, run `just certs` to generate a CA and localhost certificates.

## Remote host requirements

Deploy commands run privileged operations over SSH via `sudo`. The deploy user must have **passwordless sudo** configured on each target host:

```bash
echo "deploy ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/deploy
```

## SSH options

Both `managers deploy` and `node deploy` accept:

- `--ssh-key <PATH>` — private key for authentication (env: `WR_SSH_KEY`, config: `ssh_key`)
- `--ssh-port <PORT>` — SSH port (env: `WR_SSH_PORT`, config: `ssh_port`)

## NAT / port-forwarding environments

When VMs are behind NAT (e.g., QEMU emulated VLAN with port forwarding), services cannot reach each other by their bind addresses. Use `--advertise-address` on the manager deploy so that proxies discover a routable address from the `wr_managers` database table. Use `--ssh-port` to target forwarded SSH ports on the host.

By default, `--advertise-address` is auto-derived from the deploy target host and the manager's listen port. You only need to set it explicitly when the externally-reachable address differs from the deploy target (e.g., NAT).

```bash
# Example: QEMU VMs with port forwarding through the host
wr-cli managers deploy manager.tar.gz example@localhost \
    --ssh-port 2201 \
    --db-url "postgres://postgres@localhost:5432/wruntime" \
    --secret-key "<64-char-hex-key>" \
    --advertise-address "http://10.0.2.2:9000"

wr-cli node deploy node.tar.gz example@localhost \
    --ssh-port 2202 \
    --db-url "postgres://postgres@10.0.2.2:5432/wruntime" \
    --manager http://10.0.2.2:9000
```

In QEMU user-mode networking, `10.0.2.2` is the host gateway address reachable from all VMs.

## Startup retry behavior

Services use automatic retries to tolerate startup ordering and transient failures during deployment. This means services can be started in any order — the engine will wait for the proxy and manager to become available rather than crashing immediately.

| Operation | Retry strategy | Total window |
|-----------|---------------|--------------|
| Engine → proxy connection | Exponential backoff (200ms → 5s cap), 10 attempts | ~30s |
| Engine → manager registration (via proxy) | Exponential backoff (500ms → 5s cap), 10 attempts | ~30s |
| Engine heartbeat (per cycle) | 3 attempts, 50ms apart; reconnects on total failure | 100ms per cycle |
| Proxy heartbeat flush (per engine) | 3 attempts, 50ms apart; clears manager affinity on failure | 100ms per engine |
| Manager routing table lock | Exponential backoff (10ms → 80ms), 4 attempts | ~150ms |

If all retries are exhausted during startup, the engine exits with a descriptive error. Heartbeat retries are best-effort — a failed cycle is skipped and retried on the next interval (3s).

The CLI `node deploy` command polls the manager for up to 60 seconds waiting for the engine to register. With the retry windows above, a healthy cluster typically completes registration within 10–15 seconds of service start.

## Pre-compilation

During `node bundle`, WASM modules are pre-compiled to native `.cwasm` artifacts via Cranelift cross-compilation for the target architecture. The engine loads `.cwasm` files when available, eliminating JIT compilation at startup.

## Inspecting bundles

Use `status` to inspect a bundle without deploying:

```bash
wr-cli managers status manager.tar.gz
wr-cli node status myapp.tar.gz
```

Prints: target triple, workdir, modules, template variables, config files, and checksums.

## Viewing logs

Stream logs from remote nodes over SSH:

```bash
# All services on a systemd node
wr-cli logs node deploy@10.0.1.50 --format systemd

# Single service, follow mode
wr-cli logs node deploy@10.0.1.50 --format systemd --service wr-proxy --follow

# Docker node, last 50 lines from the last hour
wr-cli logs node deploy@10.0.1.50 --format docker --tail 50 --since 1h
```

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | — | `systemd` or `docker` (required) |
| `--service` | all wr-* units | Filter to a specific service (e.g. `wr-proxy`, `wr-engine-inventory`) |
| `--tail` | `100` | Number of recent log lines to show |
| `--since` | `5m` | Lookback window, e.g. `5m`, `1h` (systemd only) |
| `--follow` | off | Stream new lines as they arrive |
| `--workdir` | `/opt/wruntime` | Base directory for installed files |
| `--ssh-key` | — | SSH private key path |
| `--ssh-port` | — | SSH port |
