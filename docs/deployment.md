# Deployment

Wruntime provides CLI commands for packaging and deploying services to remote hosts. The workflow is **bundle once, deploy anywhere** — a single tarball contains everything needed for both systemd and Docker deployments.

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
├── config/manager.toml          # template with {db_url} placeholder
├── systemd/wr-manager.service
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

Unresolved placeholders cause deployment to fail.

## Single-node deployment (systemd)

```bash
# 1. Bundle — cross-compile for the target architecture
wr-cli node bundle \
    --engine-config engine.toml \
    --target x86_64-unknown-linux-gnu \
    --output myapp.tar.gz

# 2. Deploy to remote host
wr-cli node deploy myapp.tar.gz deploy@10.0.1.50 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000

# 3. Verify
wr-cli engines list --manager http://10.0.1.1:9000
```

Deploy steps (systemd): SCP tarball, unpack to `--workdir` (default `/opt/wruntime`), install systemd units, resolve config templates, restart services, poll manager until the engine registers (60s timeout).

## Multi-node cluster setup

```bash
# --- Manager (once per cluster) ---

wr-cli managers bundle \
    --manager-config examples/config/manager.toml \
    --target x86_64-unknown-linux-gnu \
    --output manager.tar.gz

wr-cli managers deploy manager.tar.gz deploy@10.0.1.1 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime"

# --- Node A ---

wr-cli node bundle \
    --engine-config examples/multi-node/node-a/engine-1.toml \
    --target x86_64-unknown-linux-gnu \
    --output node-a.tar.gz

wr-cli node deploy node-a.tar.gz deploy@10.0.1.50 \
    --format systemd \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager http://10.0.1.1:9000

# --- Node B ---

wr-cli node bundle \
    --engine-config examples/multi-node/node-b/engine-1.toml \
    --target x86_64-unknown-linux-gnu \
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

## SSH options

Both `managers deploy` and `node deploy` accept:

- `--ssh-key <PATH>` — private key for authentication
- `--ssh-port <PORT>` — SSH port (default: 22)

## Pre-compilation

During `node bundle`, WASM modules are pre-compiled to native `.cwasm` artifacts via Cranelift cross-compilation for the target architecture. The engine loads `.cwasm` files when available, eliminating JIT compilation at startup.

## Inspecting bundles

Use `status` to inspect a bundle without deploying:

```bash
wr-cli managers status manager.tar.gz
wr-cli node status myapp.tar.gz
```

Prints: target triple, workdir, modules, template variables, config files, and checksums.
