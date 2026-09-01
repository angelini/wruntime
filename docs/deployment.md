# Deployment

Wruntime provides CLI commands for packaging and deploying services to remote hosts. The workflow is **bundle once, deploy anywhere** — a single tarball contains everything needed for both systemd and Docker deployments.

Maintainers changing deployment generation or lifecycle behavior must run the
protected lifecycle qualification described in [Testing](testing.md) and the
[maintainer validation matrix](agents/wruntime-maintainer/validation.md). The
public deployment workflow below is not a substitute for that disposable-VM
systemd/Docker validation.

## Prerequisites

Cross-compilation of host binaries uses `cargo-zigbuild`, which bundles a Linux sysroot via Zig:

```bash
brew install zig
cargo install cargo-zigbuild
```

## Overview

| Command | Purpose |
| --------- | --------- |
| `wr-cli managers bundle` | Package the manager binary + config into a tarball |
| `wr-cli managers deploy` | Push bundle to a remote host and start the service |
| `wr-cli managers inspect-bundle` | Inspect a manager bundle without deploying |
| `wr-cli managers list` | List active managers in the cluster |
| `wr-cli node bundle` | Package proxy + engine binaries, WASM modules, and schemas |
| `wr-cli node deploy` | Push node bundle to a remote host and start services |
| `wr-cli node rollback` | Activate a retained prior successful bundle as a new revision |
| `wr-cli node stop` | Stop one deployed engine through its systemd or Docker owner and prove final exit |
| `wr-cli node inspect-bundle` | Verify and inspect a node bundle without deploying |
| `wr-cli cluster status` | Show the authoritative cluster-wide runtime snapshot |
| `wr-cli logs node` | View logs from services on a remote node (systemd or Docker) |

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
secret_key = "<64-hex-character-key>"
ssh_key    = "~/.ssh/deploy_key"
seed_nodes = ["10.0.1.11:9010", "10.0.1.12:9010"] # reserved; not emitted into manager runtime TOML
gossip_address = "10.0.1.10:9010" # optional manager UDP bind/advertise address
cert_dir   = "./certs"    # CA + node certs from `wr-cli cert`
peer_port  = 9443         # mTLS peer listener port
# ssh_port     = 22
# image_prefix = "wr"
```

All fields are optional. Fields that only apply to specific commands (e.g. `secret_key` for managers) are silently ignored when unused. CLI flags always override the config file.

`proxy_config` applies to `wr-cli node bundle`: when set (or passed as `--proxy-config` / `WR_PROXY_CONFIG`), the node bundle uses that source proxy TOML, templates deploy-varying database/node/TLS values, and preserves proxy runtime sections such as `[circuit_breaker]`, `[egress]`, and `[external]`. When omitted, the CLI keeps generating a minimal proxy config from the engine node settings.

`seed_nodes` is reserved deployment metadata. The deploy flow does not write it into runtime `manager.toml` because the runtime manager config has no such field. Managers discover fresh peers through the shared database, then use each peer's deployed, routable gossip address to form the chitchat mesh.

**Environment variables** are also supported for all deploy-related fields:

| Flag | Env var | Default |
| ------ | --------- | --------- |
| `--format` | `WR_FORMAT` | `systemd` |
| `--db-url` | `WR_DB_URL` | — |
| `--secret-key` | `WR_SECRET_KEY` | — |
| `--ssh-key` | `WR_SSH_KEY` | — |
| `--ssh-port` | `WR_SSH_PORT` | SSH default |
| `--target` | `WR_TARGET` | `x86_64-unknown-linux-gnu` |
| `--proxy-config` | `WR_PROXY_CONFIG` | — |
| `--advertise-address` | `WR_ADVERTISE_ADDRESS` | derived from remote host |
| `--gossip-address` | `WR_GOSSIP_ADDRESS` | resolved remote IP + bundled gossip port |
| `--manager` | `WR_MANAGER` | — |
| `--cert-dir` | `WR_CERT_DIR` | — |
| `--peer-port` | `WR_PEER_PORT` | `9443` |

Deployment ports must be valid non-zero TCP ports. Malformed or zero CLI/config/environment values fail immediately; defaults apply only when a value is absent. Likewise, malformed ports in source manager, proxy, or engine addresses fail bundle/deploy generation instead of becoming port `0`.

## Template variables

Config files use placeholders that are resolved at deploy time:

| Variable | Resolved from | Used in |
| ---------- | --------------- | --------- |
| `{db_url}` | `--db-url` / `WR_DB_URL` / config | manager, proxy, engine configs |
| `{host}` | deploy target (`user@host`) | proxy/engine `[node]` addresses |
| `{secret_key}` | `--secret-key` / `WR_SECRET_KEY` / config | manager systemd unit / Dockerfile |
| `{peer_port}` | `--peer-port` / `WR_PEER_PORT` / config (default: 9443) | explicit proxy/engine `peer_address` templates |
| `{advertise_address}` | `--advertise-address` / `WR_ADVERTISE_ADDRESS` (auto-derived from remote host if omitted) | manager config (`advertise_grpc_address`) |
| `{gossip_address}` | `--gossip-address` / `WR_GOSSIP_ADDRESS` / config (resolved remote IP plus bundled port if omitted) | manager config (`gossip_listen_address`) |

Unresolved placeholders cause deployment to fail. A supplied manager gossip address must be a socket address and retain the gossip port recorded by the bundle. Each manager needs a unique address that is both bindable on its host and reachable over UDP by every other manager. Manager Docker deployments use host networking so this address has the same meaning for systemd and Docker.

### Manager deploy readiness contract

After starting the manager, `wr-cli managers deploy` connects to the resolved SSH-host poll endpoint over mTLS using `ca.crt`, `<ssh-host>.crt`, and `<ssh-host>.key` from that deploy's resolved `cert_dir`. This connection does not use the CLI process's default/global certificate paths. The client certificate must cover the poll endpoint's IP in its SANs.

Deploy exits zero only when the contacted process's `LifecycleService` reports `READY` with the exact activation identity installed into the systemd unit or container image for that deployment. Manager membership remains a later cluster-health assertion and cannot satisfy startup readiness. TLS, transport, malformed lifecycle evidence, process-instance replacement, terminal-before-ready, and 60-second timeout outcomes are distinct non-zero failures with the last typed observation. No `ListManagers` visibility or advertised-address polling is used as the startup gate.

Live startup logs are stopped and both output-reader tasks are joined before final diagnostics. A bounded startup-log dump is attempted on both readiness success and failure; diagnostic collection reports its own outcome without replacing the primary readiness failure. Manager Docker log collection uses privileged Compose, matching the passwordless-sudo deployment prerequisite.

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
wr-cli node deploy --node-id node-a wr-node-bundle.tar.gz deploy@10.0.1.1 --manager https://10.0.1.1:9000

# 5. Inspect the immutable bundle (runtime health is verified by deploy)
wr-cli node inspect-bundle wr-node-bundle.tar.gz
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

wr-cli node deploy --node-id node-a myapp.tar.gz deploy@10.0.1.1 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager https://10.0.1.1:9000
```

Deploy steps (systemd): the CLI creates a manager-owned deployment attempt for the explicit stable `--node-id`, verifies and uploads the immutable bundle, resolves revision metadata into every engine config, provisions TLS, daemon-reloads, restarts all desired units, and removes obsolete engine units. It exits zero only after the manager verifies the exact current node/revision/digest/engine-slot/module inventory has fresh heartbeats and healthy default routes; timeout or any staging/activation/readiness failure is non-zero. A bundle has a deterministic SHA-256 digest while every deploy attempt receives a new monotonic per-node revision.

On the remote host, immutable bundle content is retained under `{workdir}/wr-node/bundles/<digest>/`, activation instances under `releases/<revision>/`, and `current` is switched atomically only after staging is complete. A pre-switch failure leaves the previous release running. A post-switch failure remains a failed attempt and requires an explicit rollback; it is never silently reported as success.

```bash
# Select a successful historical revision explicitly, or omit --to for the previous successful revision.
wr-cli node rollback deploy@10.0.1.1 --node-id node-a --to 3 \
  --manager https://10.0.1.1:9000
```

Rollback verifies that the retained bundle and source release exist, reads the recorded systemd/Docker backend, allocates a new revision, copies the historical desired inventory/digest, injects the new revision metadata, force-restarts/recreates services, and runs the same exact readiness verification. Revisions never move backward. Bundles and releases are retained indefinitely in this initial lifecycle; garbage collection and automatic rollback are intentionally out of scope. An interrupted CLI can leave a pending/active attempt, which history records truthfully rather than inferring remote failure.

## Multi-node cluster setup

With a shared `wr-deploy.toml`:

```toml
# wr-deploy.toml
target     = "aarch64-unknown-linux-gnu"
db_url     = "postgres://postgres@10.0.1.1:5432/wruntime"
secret_key = "<64-char-hex-key>"
```

```bash
export WR_MANAGER=https://10.0.1.1:9000

# --- Manager (once per cluster) ---

wr-cli managers bundle --manager-config examples/config/manager.toml --output manager.tar.gz
wr-cli managers deploy manager.tar.gz deploy@10.0.1.1

# --- Node A ---

wr-cli node bundle --engine-config examples/multi-node/node-a/engine-1.toml --output node-a.tar.gz
wr-cli node deploy --node-id node-a node-a.tar.gz deploy@10.0.1.50

# --- Node B ---

wr-cli node bundle --engine-config examples/multi-node/node-b/engine-1.toml --output node-b.tar.gz
wr-cli node deploy --node-id node-b node-b.tar.gz deploy@10.0.1.51
```

Each node's proxy/engine internal listeners and `[node]` data/control URLs bind loopback. Only the proxy's explicitly advertised `[node].peer_address` mTLS listener is reachable across nodes; `--peer-port` fills that target-specific URL in a staged release.

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

wr-cli node deploy --node-id node-a node-a.tar.gz deploy@10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager https://10.0.1.1:9000
```

## Disposable first-deployment and two-manager acceptance

Use this procedure only with disposable hosts, database, and CA. It exercises a failed first start, a systemd manager, a Docker manager, a non-default certificate directory, and cross-seed identity/address convergence.

1. Provision clean Linux hosts `${HOST_A}` and `${HOST_B}` plus an empty shared PostgreSQL database `${DB_URL}` reachable from both. Host A must use systemd; Host B must have Docker Compose. Configure passwordless sudo. Allow manager gRPC TCP port 9000 and gossip UDP port 9010 between hosts. Resolve routable addresses `${IP_A}` and `${IP_B}`; each host must be able to bind its own IP. Use the same bundle/`cluster_id`, database, secret key, and CA on both hosts.
2. Generate a disposable CA and host certificates in a deliberately non-default directory. The certificate SAN must cover the IP used by deploy's readiness poll:

   ```bash
   export CERT_DIR="$PWD/disposable-manager-certs"
   wr-cli cert init-ca --output "$CERT_DIR"
   wr-cli cert generate "${HOST_A}" --ca-dir "$CERT_DIR" --ip "${IP_A}"
   wr-cli cert generate "${HOST_B}" --ca-dir "$CERT_DIR" --ip "${IP_B}"
   ```

3. Build one reusable manager bundle:

   ```bash
   wr-cli managers bundle \
     --manager-config examples/config/manager.toml \
     --output manager.tar.gz
   ```

4. On a throwaway snapshot or third disposable host, deploy with an unreachable `${BAD_DB_URL}` and capture the status. Acceptance requires a non-zero lifecycle wait with last typed evidence and the bounded startup-log dump. Reset that host/snapshot and the database before continuing:

   ```bash
   if wr-cli managers deploy manager.tar.gz "${USER}@${HOST_A}" \
     --format systemd --db-url "${BAD_DB_URL}" --secret-key "${SECRET_KEY}" \
     --advertise-address "https://${IP_A}:9000" \
     --gossip-address "${IP_A}:9010" --cert-dir "$CERT_DIR"; then
     echo "invalid deployment unexpectedly succeeded" >&2
     exit 1
   else
     status=$?
   fi
   test "$status" -ne 0
   ```

5. Deploy the first manager on Host A. Deployment explicitly restarts the systemd service (or force-recreates the Compose container). Zero is valid only after output reports the replacement process instance `READY` at Host A's lifecycle endpoint; when a prior instance was observable, the new ID must differ:

   ```bash
   wr-cli managers deploy manager.tar.gz "${USER}@${HOST_A}" \
     --format systemd --db-url "${DB_URL}" --secret-key "${SECRET_KEY}" \
     --advertise-address "https://${IP_A}:9000" \
     --gossip-address "${IP_A}:9010" --cert-dir "$CERT_DIR"
   ```

6. Query Host A and record `${MANAGER_A_ID}` from the exact ID/address pair:

   ```bash
   wr-cli --manager "https://${IP_A}:9000" \
     --ca-cert "$CERT_DIR/ca.crt" \
     --client-cert "$CERT_DIR/${HOST_A}.crt" \
     --client-key "$CERT_DIR/${HOST_A}.key" managers list
   ```

7. Deploy the same bundle to Host B with Docker and the same database, cluster, secret, and CA. Manager A cannot satisfy this process-local readiness gate; success must report Host B's newly observed process instance `READY`:

   ```bash
   wr-cli managers deploy manager.tar.gz "${USER}@${HOST_B}" \
     --format docker --db-url "${DB_URL}" --secret-key "${SECRET_KEY}" \
     --advertise-address "https://${IP_B}:9000" \
     --gossip-address "${IP_B}:9010" --cert-dir "$CERT_DIR"
   ```

8. Query through both seeds. Each result must contain exactly `${MANAGER_A_ID}` at `https://${IP_A}:9000` and `${MANAGER_B_ID}` at `https://${IP_B}:9000`:

   ```bash
   wr-cli --manager "https://${IP_A}:9000" \
     --ca-cert "$CERT_DIR/ca.crt" \
     --client-cert "$CERT_DIR/${HOST_A}.crt" \
     --client-key "$CERT_DIR/${HOST_A}.key" managers list

   wr-cli --manager "https://${IP_B}:9000" \
     --ca-cert "$CERT_DIR/ca.crt" \
     --client-cert "$CERT_DIR/${HOST_B}.crt" \
     --client-key "$CERT_DIR/${HOST_B}.key" managers list
   ```

   `cluster status --detail` is optional additional evidence; it is not the deploy readiness contract.

9. Retain CLI transcripts and remote service/container logs, then destroy both hosts, drop the disposable database, and delete the disposable CA. Reverse the backend assignment when release qualification requires Docker coverage for the first manager itself.

## Docker deployment

The same bundle works for Docker — override the format via flag, env, or config:

```bash
# Via flag
wr-cli node deploy --node-id node-a myapp.tar.gz deploy@10.0.1.50 --format docker

# Via wr-deploy.toml
# format = "docker"

Docker deployments use Linux host networking so proxy/engine loopback trust boundaries match systemd. The CLI builds and force-recreates every desired container for the activated revision; direct Compose startup from an unresolved bundle is not supported.
```

## TLS certificates

Manager gRPC and cross-node peer-proxy traffic use mTLS. Local engine-to-proxy data-plane and engine-to-proxy control-plane traffic use plain HTTP on loopback listeners; manager liveness gossip uses its separate UDP listener. Generate certificates for the mTLS boundaries before deployment:

```bash
# 1. Create a CA (once per cluster)
wr-cli cert init-ca --output ./certs/

# 2. Generate per-node certificates (hostname must match the deploy target IP)
wr-cli cert generate 10.0.1.1 --ca-dir ./certs/    # manager
wr-cli cert generate 10.0.1.50 --ca-dir ./certs/   # node A
wr-cli cert generate 10.0.1.51 --ca-dir ./certs/   # node B
```

During `managers deploy`, pass `--cert-dir <dir>` (or set `cert_dir` in `wr-deploy.toml`). The command provisions `ca.crt`, `<host>.crt`, and `<host>.key` for the remote manager and uses those same local files explicitly for its readiness connection. Docker mounts the provisioned remote certificate directory read-only into the manager container.

During `node deploy`, the same option stages the files inside the revision release before switching `current`. The resolved proxy/engine configs reference those release-relative paths.

For local development, run `just certs` to generate a CA and localhost certificates.

## Remote host requirements

Deploy commands run privileged operations over SSH via `sudo`. The deploy user must have **passwordless sudo** configured on each target host:

```bash
echo "deploy ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/deploy
```

## SSH options

Both `managers deploy` and `node deploy` accept:

- `--ssh-key <PATH>` — private key for authentication (env: `WR_SSH_KEY`, config: `ssh_key`)
- `--ssh-port <PORT>` — non-zero SSH port (env: `WR_SSH_PORT`, config: `ssh_port`); malformed configured values are errors

## NAT / port-forwarding environments

When VMs are behind NAT (e.g., QEMU emulated VLAN with port forwarding), services cannot reach each other by their bind addresses. Use `--advertise-address` on the manager deploy so that proxies discover a routable address from the `wr_managers` database table. Use `--ssh-port` to target forwarded SSH ports on the host.

By default, `--advertise-address` is auto-derived from the deploy target host and the manager's listen port. You only need to set it explicitly when the externally-reachable address differs from the deploy target (e.g., NAT).

```bash
# Example: QEMU VMs with port forwarding through the host
wr-cli managers deploy manager.tar.gz example@localhost \
    --ssh-port 2201 \
    --db-url "postgres://postgres@localhost:5432/wruntime" \
    --secret-key "<64-char-hex-key>" \
    --advertise-address "https://10.0.2.2:9000" \
    --gossip-address "10.0.2.15:9010"

wr-cli node deploy --node-id node-a node.tar.gz example@localhost \
    --ssh-port 2202 \
    --db-url "postgres://postgres@10.0.2.2:5432/wruntime" \
    --manager https://10.0.2.2:9000
```

In QEMU user-mode networking, `10.0.2.2` is the host gateway address reachable from all VMs.

## Local foreground development

Build guest artifacts separately with `wr-cli dev build`, then run already-built service binaries and an optional one-shot scenario under one foreground owner:

```bash
wr-cli dev run \
  --manager-config path/to/manager.toml \
  --proxy-config primary=path/to/proxy.toml \
  --proxy-config peer=path/to/peer-proxy.toml \
  --engine-config path/to/engine.toml \
  -- sh path/to/scenario.sh
```

Exactly one manager and one or more uniquely named proxies are required; engine configs are repeatable and may be omitted. The command starts manager, proxy, and engine waves in that order, with concurrency inside proxy and engine waves. It rejects occupied control endpoints, supplies a unique activation identity to every service, and requires READY to report that identity plus the exact service kind. After engine readiness it captures the manager routing version once and waits under one absolute deadline until every supplied proxy's activation-bound routing status reaches it. Only then is the command after `--` started.

The scenario owns an isolated process group. Scenario completion, service exit, SIGINT, or SIGTERM first terminates that whole group, then cleanup signals and reaps engines concurrently, proxies concurrently, and the manager. Each service gets the existing 30-second internal period, 10-second TERM grace, and 5-second KILL/reap policy period. At that 45-second boundary the owner latches and emits `deadline exceeded, awaiting reap` evidence exactly once, then remains alive in reap-only mode with the sole `Child` handle until exit is actually proven; 45 seconds bounds failure classification, not command return for an uninterruptible OS process. A second signal may escalate. Scenario failure remains the primary result; cleanup failure is also printed and makes an otherwise successful run fail. Service output is prefixed and bounded tails are retained in failure evidence. No socket, lock, PID file, or persistent supervisor state is created.

Repository examples keep artifact construction outside foreground execution: their Just recipes run the matching `dev build` group before invoking a run script, and each run script renders configuration before making one `dev run` call with its scenario.

## Backend-owned remote stop

Stop one deployed engine by its stable bundle slot; arbitrary remote commands and proxy selectors are not accepted:

```bash
wr-cli node stop deploy@10.0.1.50 \
  --component engine:inventory \
  --format systemd \
  --json
```

The command derives `wr-engine-<slot>.service` for systemd or `engine-<slot>` for Docker Compose, asks that backend owner to stop the process, and inspects the same backend until exit is proven. It owns one absolute 45-second budget across SSH actions and polls. If graceful exit does not complete with enough budget remaining, it uses the backend's fixed force action and still requires final exit evidence; a timeout or unproved exit is non-zero.

`--json` emits the selected component, backend and derived target, graceful disposition and action evidence, force-action evidence, final backend state, `final_exited`, elapsed milliseconds, and whether force succeeded. The protected deployment qualification retains this record, then keeps the co-located proxy running while `cluster wait` observes the stopped engine's node become unhealthy before rollback.

## Semantic startup and bounded shutdown

Generated systemd units use `Type=notify`; each process sends `READY=1` only after its semantic startup barriers and sends `STOPPING=1` when final shutdown begins. Units use `SIGTERM`, `TimeoutStopSec=45s`, and final `SIGKILL` only after that external grace period. Generated Compose services use the same binary-native lifecycle probe, `stop_signal: SIGTERM`, and `stop_grace_period: 45s`. Engines depend on the proxy with `condition: service_healthy`, so startup waits for proxy semantic readiness and reverse dependency order stops engines before the proxy.

Each signal-driven stop has one absolute 30-second internal deadline; route convergence, admission waits, deregistration, and task joins consume that deadline without resetting it. The foreground runner's 45-second termination-policy boundary leaves 15 seconds after internal shutdown for process exit and escalation; needing SIGKILL or crossing the boundary is a failed graceful shutdown, while the owner still waits to reap before returning. Healthchecks execute the service binary with `--lifecycle-probe <config>` and succeed only in `READY`; a successful TCP connection while `STARTING` is not readiness.

Startup remains tolerant only through bounded, owned retries. A proxy must reach a manager and install an initial routing snapshot before readiness. An engine retries its proxy connection and registration, but startup fails non-zero if those attempts expire, a configured module is unhealthy, or readiness publication cannot converge. The first healthy engine heartbeat is synchronous: the manager returns the routing version containing the atomic readiness update, and the local proxy replies only after installing at least that version. Callers do not need fixed sleeps or manager-health polling.

On engine shutdown, route withdrawal and local proxy convergence happen before HTTP admission closes and before final deregistration. Existing HTTP requests and claimed jobs drain to the shared deadline; new work is rejected deterministically. Proxy shutdown closes data-plane admission and every data listener before joining control and background tasks. Manager shutdown rejects new administrative mutations but retains read-only status and required engine drain/deregister operations during teardown. These are internal `STOPPING` phases; deadline expiry or failed required deregistration is a non-zero process outcome.

The CLI `node deploy` and `node rollback` commands use one absolute deadline around `VerifyDeployment`, require the exact node/revision/digest record, and retain every typed condition as timeout evidence. `node stop` separately owns backend stop and final-exit proof under its single 45-second budget; deployment callers do not open a lifecycle tunnel or poll the backend themselves. A ready result is sufficient because engine readiness already proves local proxy route convergence; callers perform no post-ready sleep or invoke retry. Process lifecycle readiness and cluster availability remain distinct contracts.

## Pre-compilation

During `node bundle`, WASM modules are pre-compiled to native `.cwasm` artifacts via Cranelift cross-compilation for the target architecture. The engine loads `.cwasm` files when available, eliminating JIT compilation at startup.

## Inspecting bundles

Inspect bundles without querying runtime status:

```bash
wr-cli managers inspect-bundle manager.tar.gz
wr-cli node inspect-bundle myapp.tar.gz
```

Node inspection recomputes every payload checksum and the canonical digest before printing the target, digest, stable engine slots, exact module versions, template variables, config files, and checksums. Bundle inspection is deliberately separate from runtime status.

## Cluster runtime status

Query any seed manager for one coherent manager-known snapshot:

```bash
wr-cli --manager https://manager-1:9000 cluster status
wr-cli cluster status --output json
wr-cli cluster status --node node-a --detail
wr-cli cluster status --service ecommerce.inventory@1.0.0
wr-cli cluster status --fail-on unhealthy
wr-cli cluster wait --node node-a --severity unhealthy --timeout-secs 30
```

The default table prints aggregate counts and problem rows; `--detail` expands healthy and unknown records. JSON always emits the complete typed snapshot DTO with `schema_version: 1`, raw observation/heartbeat/deployment timestamps, server-computed ages, desired and actual identities, routing version, route evidence, and stable condition codes. Human `detail` text is explanatory; automation must use severity and code.

A healthy rollout reports the exact current node revision and digest with one authoritative fresh registration per desired slot, fresh module heartbeats, and healthy routes. Common failures are `REVISION_MISMATCH` for an old activated revision and `STALE_ENGINE_HEARTBEAT`/`STALE_MODULE_HEARTBEAT` for expired observations. A service remains available but becomes degraded with `PARTIAL_ROUTE_AVAILABILITY` when only some desired routes are healthy; zero healthy desired routes is unhealthy. Manager DB/gossip convergence and disagreement use `BOOTSTRAP_CONVERGING`, `GOSSIP_DEAD`, and `MANAGER_DB_GOSSIP_DISAGREEMENT` with separately stamped DB and gossip observation times.

No direct proxy or host scrape occurs. Routing-sync age, circuit-breaker state, CPU, and memory therefore remain `SIGNAL_NOT_REPORTED`; stale/unmanaged registrations remain visible but cannot satisfy a desired revision. The default command never acts as a monitoring gate. `--fail-on degraded` and `--fail-on unhealthy` retain display-gate behavior. For scripts, `cluster wait` returns zero only when a non-empty filtered target reaches the exact requested severity and writes the matching typed snapshot; timeout, transport/query failure, malformed evidence, and an empty/impossible filter remain distinct non-zero outcomes. Lifecycle state expectations use the separate `wr-cli lifecycle` command.

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
| ------ | --------- | ------------- |
| `--format` | — | `systemd` or `docker` (required) |
| `--service` | all wr-* units | Filter to a specific service (e.g. `wr-proxy`, `wr-engine-inventory`) |
| `--tail` | `100` | Number of recent log lines to show |
| `--since` | `5m` | Lookback window, e.g. `5m`, `1h` (systemd only) |
| `--follow` | off | Stream new lines as they arrive |
| `--workdir` | `/opt/wruntime` | Base directory for installed files |
| `--ssh-key` | — | SSH private key path |
| `--ssh-port` | — | SSH port |
