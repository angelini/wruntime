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
│   ├── proxy.toml               # template with {db_url}, {host}
│   └── engine.toml              # template with {db_url}, {guest_db_url}
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

## Template variables

Config files use placeholders that are resolved at deploy time:

| Variable | Resolved from | Used in |
|----------|---------------|---------|
| `{db_url}` | `--db-url` flag | manager, proxy, engine configs |
| `{guest_db_url}` | `--guest-db-url` flag | engine config (module DB access) |
| `{host}` | deploy target (`user@host`) | proxy/engine `[node]` addresses |
| `{secret_key}` | `--secret-key` flag | manager systemd unit / Dockerfile |
| `{advertise_address}` | `--advertise-address` flag | manager config (`advertise_grpc_address`) |

Unresolved placeholders cause deployment to fail.

## Single-node deployment (systemd)

```bash
# 1. Bundle manager
wr-cli managers bundle \
    --manager-config examples/config/manager.toml \
    --target aarch64-unknown-linux-gnu \
    --output manager.tar.gz

# 2. Deploy manager
wr-cli managers deploy manager.tar.gz deploy@10.0.1.1 \
    --format systemd \
    --db-url "postgres://postgres@localhost:5432/wruntime" \
    --secret-key "<64-char-hex-key>" \
    --advertise-address "http://10.0.1.1:9000"

# 3. Bundle node — cross-compile for the target architecture
wr-cli node bundle \
    --engine-config engine.toml \
    --target aarch64-unknown-linux-gnu \
    --output myapp.tar.gz

# 4. Deploy node
wr-cli node deploy myapp.tar.gz deploy@10.0.1.1 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000

# 5. Verify
wr-cli engines list --manager http://10.0.1.1:9000
```

Deploy steps (systemd): SCP tarball, unpack to `--workdir` (default `/opt/wruntime`), install systemd units, resolve config templates, restart services, poll manager until the engine registers (60s timeout).

## Multi-node cluster setup

```bash
# --- Manager (once per cluster) ---

wr-cli managers bundle \
    --manager-config examples/config/manager.toml \
    --target aarch64-unknown-linux-gnu \
    --output manager.tar.gz

wr-cli managers deploy manager.tar.gz deploy@10.0.1.1 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --secret-key "<64-char-hex-key>" \
    --advertise-address "http://10.0.1.1:9000"

# --- Node A ---

wr-cli node bundle \
    --engine-config examples/multi-node/node-a/engine-1.toml \
    --target aarch64-unknown-linux-gnu \
    --output node-a.tar.gz

wr-cli node deploy node-a.tar.gz deploy@10.0.1.50 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000

# --- Node B ---

wr-cli node bundle \
    --engine-config examples/multi-node/node-b/engine-1.toml \
    --target aarch64-unknown-linux-gnu \
    --output node-b.tar.gz

wr-cli node deploy node-b.tar.gz deploy@10.0.1.51 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000
```

## Docker deployment

The same bundle works for Docker — just change the `--format` flag:

```bash
wr-cli node deploy myapp.tar.gz deploy@10.0.1.50 \
    --format docker \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000

# Or extract locally and run with Docker Compose
tar xzf myapp.tar.gz
cd wr-node && docker compose -f docker/docker-compose.yml up -d
```

## Remote host requirements

Deploy commands run privileged operations over SSH via `sudo`. The deploy user must have **passwordless sudo** configured on each target host:

```bash
echo "deploy ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/deploy
```

## SSH options

Both `managers deploy` and `node deploy` accept:

- `--ssh-key <PATH>` — private key for authentication
- `--ssh-port <PORT>` — SSH port (default: 22)

## NAT / port-forwarding environments

When VMs are behind NAT (e.g., QEMU emulated VLAN with port forwarding), services cannot reach each other by their bind addresses. Use `--advertise-address` on the manager deploy so that proxies discover a routable address from the `wr_managers` database table. Use `--ssh-port` to target forwarded SSH ports on the host.

```bash
# Example: QEMU VMs with port forwarding through the host
wr-cli managers deploy manager.tar.gz example@localhost \
    --ssh-port 2201 \
    --format systemd \
    --db-url "postgres://postgres@localhost:5432/wruntime" \
    --secret-key "<64-char-hex-key>" \
    --advertise-address "http://10.0.2.2:9000"

wr-cli node deploy node.tar.gz example@localhost \
    --ssh-port 2202 \
    --format systemd \
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
