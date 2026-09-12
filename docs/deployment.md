# Deployment

Wruntime provides CLI commands for packaging and deploying services to remote hosts. The workflow is **bundle once, deploy anywhere** — a single tarball contains everything needed for both systemd and Docker deployments.

Maintainers changing deployment generation or lifecycle behavior must run the
protected lifecycle qualification described in [Testing](testing.md) and the
[maintainer validation matrix](agents/wruntime-maintainer/validation.md). The
public deployment workflow below is not a substitute for that disposable-VM
systemd/Docker validation. The protected fixture deploys database-enabled engines and exercises job administration on both backends: generated delegation certificate mounts and advertised engine admin addresses must start successfully, runtime credentials must fail on the operator listener, queue discovery must report one fresh delegate, and a manager-mediated queue summary must reach the engine.

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
| `wr-cli node agent install/update` | Atomically update only an already provisioned host-agent binary, restart it, and wait for a new compatible attestation |
| `wr-cli node deploy` | Stage/finalize a complete desired inventory and reconcile initial, replacement, scale, or mixed changes |
| `wr-cli node rollback` | Stage a retained successful bundle as a new revision and submit explicit rollback |
| `wr-cli engines status` | Compose lifecycle, availability, revision authority, backend evidence, and active operation status |
| `wr-cli engines restart` | Restart one committed desired slot without changing its revision or inventory |
| `wr-cli operations get/list/resume/cancel` | Inspect and administer durable operation history |
| `wr-cli node abandon` | Remove an unsubmitted inactive allocation after manager safety checks |
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
├── systemd/wr-manager.service   # stable activation-launcher unit
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
├── agent/
│   └── wr-cli                    # independently installed host agent binary
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
│   ├── wr-node-agent.service
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
cert_dir   = "./certs"    # CA + node certs from `wr-cli cert`
peer_port  = 9443         # mTLS peer listener port
# ssh_port     = 22
# image_prefix = "wr"
```

All fields are optional. Fields that only apply to specific commands (e.g. `secret_key` for managers) are silently ignored when unused. CLI flags always override the config file.

`proxy_config` applies to `wr-cli node bundle`: when set (or passed as `--proxy-config` / `WR_PROXY_CONFIG`), the node bundle uses that source proxy TOML, templates deploy-varying database/node/TLS values, and preserves proxy runtime sections such as `[circuit_breaker]`, `[egress]`, and `[external]`. When omitted, the CLI keeps generating a minimal proxy config from the engine node settings.

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
| `{secret_key}` | `--secret-key` / `WR_SECRET_KEY` / config | protected manager runtime environment / Dockerfile |
| `{peer_port}` | `--peer-port` / `WR_PEER_PORT` / config (default: 9443) | explicit proxy/engine `peer_address` templates |
| `{operation_id}` | manager-derived deployment allocation identity | engine deployment metadata |
| `{revision_digest}` | manager-derived canonical revision identity | engine deployment metadata |
| `{advertise_address}` | `--advertise-address` / `WR_ADVERTISE_ADDRESS` (auto-derived from remote host if omitted) | manager config (`advertise_grpc_address`) |

Unresolved placeholders cause deployment to fail. Systemd manager deployment atomically installs the encryption key in the root-only `/var/lib/wruntime/manager-secrets/runtime.env` environment file referenced by the stable unit; the key is not embedded in that unit. The unit is installed at `/usr/local/lib/systemd/system/wr-manager.service`, below `/run/systemd/system` in the unit load path, so the runtime mask used during manager-set cutover takes precedence. Manager Docker deployments retain host networking for systemd parity and direct listener addressing.

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
# 4. After provisioning config, credentials, directories, backend, service unit,
#    executable, and initial manager policy, update the agent binary.
wr-cli node agent install --node-id node-a --format systemd wr-node-bundle.tar.gz deploy@10.0.1.1 --manager https://10.0.1.1:9000

# 5. Stage/finalize the complete desired inventory and submit reconciliation
wr-cli node deploy --node-id node-a wr-node-bundle.tar.gz deploy@10.0.1.1 --manager https://10.0.1.1:9000 --request-token node-a-initial

# 6. Inspect immutable bundle content without querying runtime health
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

wr-cli node agent install --node-id node-a --format systemd myapp.tar.gz deploy@10.0.1.1 \
    --manager https://10.0.1.1:9000
wr-cli node deploy --node-id node-a myapp.tar.gz deploy@10.0.1.1 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --manager https://10.0.1.1:9000 --request-token node-a-initial
```

Node deploy creates an inactive manager allocation for the stable `--node-id`; that allocation reserves the operation UUID and canonical revision digest embedded in the prelaunch engine configuration. The CLI verifies the bundle, resolves host and manager-derived values, uploads bytes without touching running workloads, writes digest-covered release metadata, and calls `FinalizeDeployment`. It then requires a fresh compatible agent attestation and submits one durable operation. The manager and agent—not the CLI's SSH session—select, stop, start, verify, switch authority, commit, and clean up. A CLI wait timeout is nonzero but does not cancel durable work.

Immutable bundle content is retained under `{workdir}/wr-node/bundles/<digest>/` and revisions under `wr-node/releases/<revision>/`. Each proxy/engine slot selects its own release through `wr-node/slots/<slot>`; no node-wide engine `current` or aggregate activation exists. A staging/finalization interruption leaves serving authority unchanged. An allocation finalized but not submitted is explicit and may be removed only with `node abandon`; once submitted, recovery uses operation status/resume or rollback rather than inference from CLI exit.

```bash
# Select a successful historical revision explicitly, or omit --to for the previous successful revision.
wr-cli node rollback deploy@10.0.1.1 --node-id node-a --to 3 \
  --manager https://10.0.1.1:9000
```

Rollback verifies retained source content, allocates and finalizes a new monotonic revision, and submits a rollback operation. The agent performs only manager-issued per-slot effects; manager evidence gates authority and commit. Revisions never move backward. After commit, the rollout is already terminally successful. Independent periodic cleanup deletes only the manager-derived revision/digest allow-list while preserving configured retention, every pending/active unabandoned allocation, committed/staged revisions, observations, active authority, and rollback sources. Automatic post-commit rollback is never inferred from cleanup delay or failure. Operators inspect `wr-cli node cleanup status <node-id>` and compare-and-set a paused generation with `wr-cli node cleanup retry <node-id> --generation <n>`; retry only queues the next periodic pass.

## Operator lifecycle operations

Maintenance addresses stable `node_id` plus engine slot, never ephemeral `engine_id`:

```bash
wr-cli engines status --node-id node-a --slot blue --json
wr-cli engines restart --node-id node-a --slot blue --wait-timeout 300
wr-cli operations list --node-id node-a --include-terminal --json
wr-cli operations resume <operation-id>
wr-cli operations cancel <operation-id>
```

Restart defaults to a five-minute durable deadline and preserves the committed revision and desired inventory. Restarting the final serving slot requires explicit `--allow-downtime`. Waiting is default. `--wait-timeout` limits only the CLI; timeout is nonzero and prints the operation ID and last observation while durable work continues. `--no-wait` returns after submission. Omitting `--request-token` generates and prints a UUID. Reusing the same actor/token/payload follows the same operation; conflicting reuse fails.

Use `node deploy` for every complete desired-inventory change—first deployment, revision replacement, expansion, contraction, or a mixed transition:

```bash
wr-cli node deploy --node-id node-a wr-node-v2.tar.gz deploy@10.0.1.1 \
  --request-token node-a-v2 --max-unavailable 1
wr-cli node deploy --node-id node-a wr-node-scaled.tar.gz deploy@10.0.1.1 \
  --request-token node-a-inventory-change --max-unavailable 1
```

The bundle's finalized inventory is authoritative; callers do not select initial, upgrade, scale, or canary mechanics. The manager progresses additions, retained replacements, and removals sequentially in deterministic groups. The default is `max_unavailable=1` and a 30-minute durable deadline. Reducing the final serving slot or targeting an empty inventory requires `--allow-downtime`. Submitting the exact committed revision with a new authenticated actor/token creates an immediately successful operation and performs no host, deployment, or authority mutation. `--wait-timeout` is caller-only and `--no-wait` returns after submission. Cancellation before commit restores the complete source inventory and authority without the expired forward deadline; committed work is corrected only by separately authorized rollback.

The host agent verifies digest-covered release metadata, confines paths below `deployment_root`, atomically selects `wr-node/slots/<slot>`, and invokes only fixed systemd units or Compose services. It has no listener and no root-owned recovery-state directory or journal. The generated unit grants write access to workload state, the runtime directory, and backend-owned unit/socket paths only. Stop sends SIGTERM through that backend; the engine's 30-second shutdown emits `STOPPING`, withdraws routes, converges the proxy, drains, and deregisters. The manager commits delivery ambiguity before returning a mutation. A live agent retries an unacknowledged exact tagged result in memory before claiming more work; after agent replacement, the fresh activation never replays that result and instead receives typed backend inspection from durable manager state. Endpoint disappearance alone is never final-exit proof, and missing or query-error inspection evidence is unknown and pauses rather than authorizing another mutation.

### Clean-slate persistence baseline

The embedded manager schema and engine job queue each have one V1 migration. Existing manager/job persistence and old Refinery history or checksums are unsupported. During development or testing, stop wruntime and run `just dev-reset-db` to destroy and recreate both stores before starting binaries containing the clean baseline. Guest-authored module migrations remain independent forward migration chains and are not reset by this contract.

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
wr-cli node agent install --node-id node-a --format systemd node-a.tar.gz deploy@10.0.1.50
wr-cli node deploy --node-id node-a node-a.tar.gz deploy@10.0.1.50 --request-token node-a-initial

# --- Node B ---

wr-cli node bundle --engine-config examples/multi-node/node-b/engine-1.toml --output node-b.tar.gz
wr-cli node agent install --node-id node-b --format systemd node-b.tar.gz deploy@10.0.1.51
wr-cli node deploy --node-id node-b node-b.tar.gz deploy@10.0.1.51 --request-token node-b-initial
```

Each node's proxy/engine internal listeners and `[node]` data/control URLs bind loopback. The proxy's explicitly advertised `[node].peer_address` mTLS listener is reachable across nodes; a database-enabled engine also exposes its configured `job_admin.advertise_address` directly to managers over delegation mTLS. `--peer-port` fills the proxy target-specific URL, while the job-admin port comes from each engine config and is checked for bundle-wide conflicts.

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

## Manager-set deployment

Use one replay-stable TOML manifest with `wr-cli managers deploy-set --manifest <path>`. It names the caller operation UUID, cluster, existing endpoint (omitted only for pristine bootstrap), exact target policy file, deployment certificate set, ordered source and target manager/host sets, and every executable/image, backend-spec, config, credential, and selector digest. The authenticated deployment principal plus operation UUID is the immutable owner; the manifest has no transferable executor or predecessor-recovery identity. Target entries use either a local systemd binary with its SHA-256 or an immutable Compose `name@sha256:...` image. The CLI validates all local bytes and reruns the shared policy/deployment-identity validator before contacting a host. An exact Begin retry returns its original `PREPARED` receipt.

Systemd binaries are retained at `/opt/wruntime/manager-artifacts/binaries/<sha256>/wr-manager`; Compose images are pulled and inspected by repo digest. Backend specs are also digest-qualified. During `STAGING`, config is only owner-readable `next.tmp`, credential sets remain immutable below `/etc/wruntime/pki`, and the target descriptor remains unselected. A spontaneous restart therefore uses the source descriptor. `FAILED_PRE_CLOSE` changes no active selector.

After the manager reports `OLD_CLOSED`, fenced target actions maintain only `current.toml` and `previous.toml`, atomically replace `/var/lib/wruntime/manager-activation/current-activation.json`, and start each target with privileged admission closed. The stable launcher verifies executable/image, backend-spec, config, and credential digests before start. An exact `READY_CLOSED` target becomes the control endpoint before each remaining source is selector-fenced, stopped, and masked without changing its selector. Targets open only after all declared sources have typed `STOPPED` evidence and no live non-target membership remains. Owner-only phase transitions are lease-free; digest mismatch, conflicting replay, or any post-closure ambiguity becomes durable `FAILED_CLOSED`. There is no automatic rollback or generic artifact cleanup.

Recovery is explicit: `wr-cli managers reset-failed-rollout --manifest <path> --rollout-id <id>` first matches the exact durable failed declaration, disconnects, and read-only inspects every declared source and target. The manager accepts only exact role coverage with all processes conclusively stopped and one uniform nonzero installed policy generation/digest. The stable reset receipt clears only the active guard and grants a one-use permit to the same authenticated principal. It does not certify success, change accepted policy, select artifacts, or open admission; a restarted manager reports `CLOSED_STARTUP`. Operators must repair stopped hosts explicitly, then submit a distinct fresh rollout under the same principal to consume the permit and reopen admission on successful completion.

Protected qualification covers node lifecycle under systemd and Compose, but manager deploy-set only under systemd. It proves successful A→B→A, deterministic post-close digest refusal and `FAILED_CLOSED`, active and mixed-policy reset rejection, complete stopped/uniform reset, closed admission after reset, and a separate fresh rollout. Compose manager deploy-set remains explicitly unqualified until immutable image distribution is available.

## Docker deployment

The same bundle works for Docker — override the format via flag, env, or config:

```bash
# Via flag
wr-cli node deploy --node-id node-a myapp.tar.gz deploy@10.0.1.50 --format docker

# Via wr-deploy.toml
# format = "docker"

Docker deployments use Linux host networking so proxy/engine loopback trust boundaries match systemd. Inactive images and Compose metadata are staged first; only the node agent invokes fixed Compose services after manager authorization. Direct Compose startup from an unresolved bundle is not supported.
```

## TLS certificates

Manager gRPC, peer-proxy traffic, and manager-to-engine job administration use mTLS; loopback engine/proxy traffic remains plain HTTP. Server and client roots are disjoint. Every client leaf has `clientAuth` plus exactly one project URI SAN (`urn:wruntime:<cluster>:<kind>:<name>`), and every server leaf has only `serverAuth` plus its endpoint SANs. A manager's workload leaf is distinct from its endpoint leaf.

Production roots are installed under `/etc/wruntime/pki/roots/`. Immutable credential sets are installed under `/etc/wruntime/pki/<service-or-slot>/sets/<version>`; they remain root-owned and are read-only to the workload group used by systemd. Private keys and runtime config never live in a release or image. Generate roots and profile leaves with `wr-cli cert init-root` and `wr-cli cert issue`, and validate an uncertain installation with `wr-cli cert verify`. Authorization and revocation use the leaf SHA-256 fingerprint recorded in immutable set metadata. Job access uses the same manager endpoint and policy; there is no operator-admin listener, delegation persona, or CA-wide capability.

## Remote host requirements

External provisioning owns bootstrap, node-agent config and credentials, directories, backend prerequisites, and the hardened service unit. Systemd node hosts require Linux 5.3 or newer so the agent can use `pidfd_open` for exact process-exit evidence. The product's agent install/update helper uses privileged SSH only to stage, verify, atomically replace the existing agent executable, and restart the existing service; it never transfers private material or repairs missing topology. Inactive release staging and diagnostic commands also use bounded privileged operations. Workload effects do not. The deploy user must have **passwordless sudo** configured on each target host:

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
    --advertise-address "https://10.0.2.2:9000"

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

## Agent-owned deployed process effects

Operators address stable deployment identity and submit durable intent:

```bash
wr-cli engines restart --node-id node-a --slot inventory --request-token restart-42 --json
```

Only the continuously fenced node agent maps the typed target to `wr-engine-<slot>.service` or the fixed Compose service. It records backend instance identity, sends the backend's graceful SIGTERM action, and inspects until the exact instance exits. For systemd, it pins and revalidates the activation's `MainPID` before stop so exact process-exit proof survives systemd unloading the inactive unit and clearing `InvocationID`. Manager reconciliation separately requires `STOPPING`, route withdrawal, deregistration, and backend final exit where the action calls for them. A restarted process must have the requested revision/digests and a fresh process/backend identity before authority can return.

SSH remains available for binary-only host-agent updates on an existing provisioned baseline, pre-staging immutable bytes, and bounded diagnostics. It is never used to execute workload stop/start/select/cleanup effects. If the agent loses its activation lease or cannot inspect the backend, the operation pauses with explicit evidence; a replacement activation begins with inspection and cannot blindly repeat the prior effect.

## Semantic startup and bounded shutdown

Generated systemd units use `Type=notify`; each process sends `READY=1` only after its semantic startup barriers and sends `STOPPING=1` when final shutdown begins. Units use `SIGTERM`, `TimeoutStopSec=45s`, and final `SIGKILL` only after that external grace period. Generated Compose services use the same binary-native lifecycle probe, `stop_signal: SIGTERM`, and `stop_grace_period: 45s`. Engines depend on the proxy with `condition: service_healthy`, so startup waits for proxy semantic readiness and reverse dependency order stops engines before the proxy.

Each signal-driven stop has one absolute 30-second internal deadline; route convergence, admission waits, deregistration, and task joins consume that deadline without resetting it. The foreground runner's 45-second termination-policy boundary leaves 15 seconds after internal shutdown for process exit and escalation; needing SIGKILL or crossing the boundary is a failed graceful shutdown, while the owner still waits to reap before returning. Healthchecks execute the service binary with `--lifecycle-probe <config>` and succeed only in `READY`; a successful TCP connection while `STARTING` is not readiness.

Startup remains tolerant only through bounded, owned retries. Generated proxy configuration carries the same exact node/revision/bundle/operation/revision-digest identity as its release marker (with no resolved-release digest, avoiding a circular input), plus the bounded report cadence. A proxy must reach a manager, register its STARTING process identity, publish an initial status snapshot, and install an initial routing snapshot before readiness. An engine retries its proxy connection and registration, but startup fails non-zero if those attempts expire, a configured module is unhealthy, or readiness publication cannot converge. The first healthy engine heartbeat is synchronous: the manager returns the routing version containing the atomic readiness update, and the local proxy replies only after installing at least that version. If the proxy process is replaced while an engine remains running, the next heartbeat can rebuild the empty proxy cache only after the manager accepts that engine's exact ownership fence; the replacement proxy also converges the returned routing version before acknowledging it. Callers do not need fixed sleeps or manager-health polling.

On engine shutdown, route withdrawal and local proxy convergence happen before HTTP admission closes and before final deregistration. Existing HTTP requests and claimed jobs drain to the shared deadline; new work is rejected deterministically. Proxy shutdown closes data-plane admission and every data listener before joining control and background tasks. Manager shutdown rejects new administrative mutations but retains read-only status and required engine drain/deregister operations during teardown. These are internal `STOPPING` phases; deadline expiry or failed required deregistration is a non-zero process outcome.

`node deploy` and `rollback` stage/finalize exact node/revision/bundle/resolved-release identity and then submit durable work. The manager owns the absolute operation deadline; the CLI's wait deadline is separate. Backend inspection, lifecycle READY/STOPPING, registration, routing convergence, slot authority, and commit remain distinct evidence, and callers add no readiness sleep. A completed verification report waits for its matching fresh observation instead of treating normal result-before-observation delivery as invalid evidence. Post-commit cleanup is visible through the dedicated node cleanup status and never changes the already-committed operation result. Overdue or paused cleanup projects `DEGRADED`, not `UNHEALTHY`, while serving remains healthy.

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

The default table prints aggregate counts (including proxies) and problem rows; `--detail` expands selected/expected proxy identity, lifecycle/admission, report and routing ages, routing source/version, listener states, aggregate breaker counts, and healthy/unknown records. JSON uses the strict current `schema_version: 1` and emits canonical top-level plus node-embedded proxy inventory alongside raw observation/heartbeat/deployment timestamps, server-computed ages, desired and actual identities, routing version, route evidence, and stable condition codes. Human `detail` text is explanatory; automation must use severity and code.

A healthy rollout reports the exact current node revision and digest with one authoritative fresh registration per desired slot, fresh module heartbeats, and healthy routes. Common failures are `REVISION_MISMATCH` for an old activated revision and `STALE_ENGINE_HEARTBEAT`/`STALE_MODULE_HEARTBEAT` for expired observations. A service remains available but becomes degraded with `PARTIAL_ROUTE_AVAILABILITY` when only some desired routes are healthy; zero healthy desired routes is unhealthy. A retained manager row whose lease is older than the configured live threshold is dead with `STALE_MANAGER_HEARTBEAT`; the database observation timestamp is the authoritative clock evidence.

No direct proxy or host scrape occurs. Each proxy pushes complete authenticated snapshots to the manager; status selects exactly one fresh process matching operation-aware deployment identity and interprets routing/listener/admission/breaker evidence. During a rollout, source remains expected until target selection/start, target remains expected through engine rollout and commit, and failed-forward restoration expects source. Duplicate or irreducibly ambiguous evidence degrades instead of guessing. A stale replaced predecessor remains diagnostic but does not poison recovery. Only CPU and memory remain `SIGNAL_NOT_REPORTED`; stale/unmanaged registrations remain visible but cannot satisfy a desired revision. The default command never acts as a monitoring gate. `--fail-on degraded` and `--fail-on unhealthy` retain display-gate behavior. For scripts, `cluster wait` returns zero only when a non-empty filtered target reaches the exact requested severity and writes the matching typed snapshot; timeout, transport/query failure, malformed evidence, and an empty/impossible filter remain distinct non-zero outcomes. Lifecycle state expectations use the separate `wr-cli lifecycle` command.

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
