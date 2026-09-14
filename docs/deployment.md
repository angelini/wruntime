# Deployment

Wruntime packages managers and nodes as deterministic Systemd bundles. A bundle
is host-agnostic: deploy resolves host, database, and identity placeholders
before finalizing an immutable release.

## Prerequisites

Remote builds require Rust, the selected Rust target, Zig, and
`cargo-zigbuild`. Hosts require Linux 5.3 or newer, Systemd, and a deployment
account with password-free `sudo`. Database-enabled deployments also require
PostgreSQL 18 and a database-host-local operator able to update `pg_ident.conf`.

```bash
rustup target add x86_64-unknown-linux-gnu
cargo install cargo-zigbuild
```

Run `wr-cli --help`, `wr-cli managers --help`, or `wr-cli node --help` for the
installed CLI's complete option reference.

## Deployment configuration

Commands auto-discover `wr-deploy.toml` in the current directory or accept
`--config <path>`. Precedence is CLI flag, config file, environment variable,
then command default.

```toml
# wr-deploy.toml
target       = "x86_64-unknown-linux-gnu"
workdir      = "/opt/wruntime"
proxy_config = "examples/config/proxy.toml"
db_url       = "postgres://postgres@10.0.1.1:5432/wruntime"
secret_key   = "<64-hex-character-key>"
ssh_key      = "~/.ssh/deploy_key"
cert_dir     = "./certs"
peer_port    = 9443

# Required only when the node bundle contains DB-enabled modules.
[tenant_database]
server_name         = "postgres.internal"
host_addr           = "10.0.0.15"
port                = 5432
connect_timeout_secs = 10
```

Important environment equivalents include `WR_MANAGER`, `WR_DB_URL`,
`WR_SECRET_KEY`, `WR_SSH_KEY`, `WR_SSH_PORT`, `WR_TARGET`, `WR_PROXY_CONFIG`,
`WR_ADVERTISE_ADDRESS`, `WR_CERT_DIR`, `WR_PEER_PORT`, and the
`WR_TENANT_DB_*` settings.

`db_url` is the manager/proxy/platform-job database URL. It is never used to
infer tenant database settings. Tenant `server_name` is the DNS identity in the
PostgreSQL server certificate; optional `host_addr` changes only the private TCP
destination.

## Certificates

Server and client roots are separate. Server leaves carry `serverAuth` and DNS
or IP endpoint SANs. Client leaves carry `clientAuth` and one project URI SAN,
for example `urn:wruntime:production:human:deployer`.

```bash
wr-cli cert init-root server --output certs/runtime-server-root
wr-cli cert init-root client --output certs/runtime-client-root
wr-cli cert issue manager-endpoint --endpoint manager.example --ip 10.0.1.10 \
  --ca-dir certs/runtime-server-root --destination certs/runtime-manager-endpoint
wr-cli cert issue human --cluster-id production --name deployer \
  --ca-dir certs/runtime-client-root --destination certs/runtime-human-client
wr-cli cert verify certs/runtime-human-client
```

Production roots live under `/etc/wruntime/pki/roots/`. Immutable credential
sets live under `/etc/wruntime/pki/<service-or-slot>/sets/<version>` and stay
outside release directories and images. Runtime authorization and revocation
use the leaf SHA-256 fingerprint.

Manager-facing CLI commands default to:

- `--ca-cert certs/runtime-server-root/ca.crt`
- `--client-cert certs/runtime-human-client/leaf.pem`
- `--client-key certs/runtime-human-client/key.pem`

Override them with `WR_CA_CERT`, `WR_CLIENT_CERT`, and `WR_CLIENT_KEY`. Job
commands use the same manager endpoint and global TLS options; there is no
separate operator job-admin listener or certificate workflow.

## Deploy managers

Build one manager bundle and deploy it to each manager host:

```bash
wr-cli managers bundle --manager-config examples/config/manager.toml \
  --output wr-manager-bundle.tar.gz
wr-cli managers inspect-bundle wr-manager-bundle.tar.gz
wr-cli managers deploy wr-manager-bundle.tar.gz deploy@10.0.1.10 \
  --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
  --secret-key "<64-hex-character-key>"
```

`--advertise-address` defaults from the remote host and configured manager port.
Set it when NAT or a load balancer gives clients a different reachable address.
All managers in one cluster use the same PostgreSQL database and authorization
policy.

### Manager readiness

Deploy polls the resolved manager endpoint over mTLS with credentials from that
deployment's `cert_dir`. Success requires `LifecycleService.GetStatus` to report
`READY` for service kind `manager` and the exact activation identity installed
in the Systemd unit. Manager membership and `ListManagers` are not startup
gates. TLS failure, process replacement, terminal-before-ready state, malformed
lifecycle evidence, and timeout are non-zero outcomes.

## Database-free nodes

A database-free node can be allocated and deployed directly:

```bash
wr-cli node bundle \
  --engine-config examples/multi-node/node-b/engine-1.toml \
  --output wr-node-bundle.tar.gz
wr-cli node inspect-bundle wr-node-bundle.tar.gz

# The host-agent config, credentials, directories, and Systemd unit are
# provisioned separately. This command installs or updates only its binary.
wr-cli node agent install --node-id node-a wr-node-bundle.tar.gz deploy@10.0.1.20 \
  --manager https://10.0.1.10:9000

wr-cli node deploy --node-id node-a wr-node-bundle.tar.gz deploy@10.0.1.20 \
  --manager https://10.0.1.10:9000 \
  --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
  --request-token node-a-initial
```

`node agent install|update` is a binary updater for an already provisioned,
outbound-only agent. It does not create agent config, credentials, directories,
or backend topology.

A node bundle may contain several `--engine-config` values. Add
`--proxy-config <path>` when preserving egress, public routes, or non-default
proxy settings. A proxy-only bundle with no engine configs describes an empty
desired engine inventory and requires `--allow-downtime` when removing final
capacity.

## Database-enabled nodes

Database-enabled nodes require reservation, offline provisioning and migration,
then deployment with the matching receipt. No engine or manager receives the
PostgreSQL admin URL.

```bash
# 1. Bundle the node.
wr-cli node bundle --engine-config engine.toml --output node.tar.gz

# 2. Create PostgreSQL endpoint and node-client credentials from explicit roots.
wr-cli cert init-root server --output postgres-pki/server-root
wr-cli cert init-root client --output postgres-pki/client-root
wr-cli cert issue postgres-server --endpoint postgres.internal --ip 10.0.0.15 \
  --ca-dir postgres-pki/server-root --destination postgres-pki/server
wr-cli cert issue postgres-client --cluster-id production --name node-a \
  --ca-dir postgres-pki/client-root --destination postgres-pki/node-a

# 3. Reserve manager-owned revision identity without changing the node.
wr-cli node reserve-deployment node.tar.gz deploy@node-a \
  --node-id node-a --request-token node-a-v7 --output prepared/node-a \
  --postgres-client-set postgres-pki/node-a

# 4. Run on the database host (or an equivalent co-located one-shot job).
wr-cli postgres provision --manifest provisioning.toml \
  --admin-url-file /run/operator/postgres-admin-url \
  --pg-ident-target /var/lib/postgresql/data/pg_ident.conf
wr-cli postgres migrate --manifest provisioning.toml \
  --bundle-manifest prepared/node-a/migration-bundle.json \
  --bundle-root prepared/node-a/migrations \
  --admin-url-file /run/operator/postgres-admin-url \
  --node-bundle node.tar.gz \
  --deployment-reservation prepared/node-a/reservation.json \
  --receipt-out prepared/node-a/tenant-state.json

# 5. Install credentials outside the release and submit only matching state.
wr-cli node deploy node.tar.gz deploy@node-a --node-id node-a \
  --reservation prepared/node-a/reservation.json \
  --tenant-state-manifest prepared/node-a/tenant-state.json \
  --postgres-client-set postgres-pki/node-a
```

The reservation and tenant-state receipt bind the manager, node, revision,
operation, inventory, tenant endpoint, node certificate, provisioning
generation, bundle digest, and ordered migration hashes. A mismatch fails before
release activation. Failed or ambiguous migration attempts remain blocked until
an operator uses `wr-cli postgres approve-retry` with the exact artifact and
attempt evidence.

Namespace databases, roles, and mappings are retained. Their retirement and
access revocation belong to PostgreSQL administration, not node deployment.

Development setup is host-owned rather than a sandbox deployment workflow. Each
linked worktree has an isolated Compose project, retained volumes, PKI, fixture
artifacts, and readiness records. Pi consumes the internally consistent
published state without requiring current source-hash equality and performs no
Docker, provisioning, migration, or topology discovery. When rerun after
provisioning or migration changes, host `just dev-up` converges the retained
volumes through the immutable migration ledger and never deletes them
automatically. Intentionally incompatible local data requires an explicit
manual reset. See [Testing](testing.md#development-services) for details.

## Operations and recovery

`node deploy` always submits a complete desired inventory. It handles initial
deployment, additions, replacements, and removals. `--max-unavailable` defaults
to one; final-capacity or empty-inventory changes require `--allow-downtime`.
`--wait-timeout` limits only the CLI wait, while the durable manager operation
continues. `--no-wait` returns after submission.

```bash
wr-cli node deploy --node-id node-a wr-node-v2.tar.gz deploy@10.0.1.20 \
  --request-token node-a-v2 --max-unavailable 1
wr-cli engines status --node-id node-a --slot blue --json
wr-cli operations list --node-id node-a --include-terminal --json
wr-cli operations get OPERATION_ID --json
```

A timeout or disconnected CLI does not imply failure. Inspect the operation and
cluster state first. If the manager pauses on uncertain backend evidence, repair
the underlying host or connectivity issue, then resume:

```bash
wr-cli operations resume OPERATION_ID
```

Cancelling queued or paused work before commit requests restoration of the
complete source inventory. Committed deployments are corrected with an explicit
rollback:

```bash
wr-cli operations cancel OPERATION_ID
wr-cli node rollback deploy@10.0.1.20 --node-id node-a --to 3
```

Rollback creates a new monotonic revision from retained content; revision
numbers do not move backward. Restart is separate single-slot maintenance and
preserves the committed inventory:

```bash
wr-cli engines restart --node-id node-a --slot blue --wait-timeout 300
```

An unsubmitted inactive allocation can be removed with `wr-cli node abandon`.
After submission, use operation status, resume, cancel, or rollback rather than
inferring state from local files.

Release cleanup runs independently after commit:

```bash
wr-cli node cleanup status node-a
wr-cli node cleanup retry node-a --generation 7
```

A paused cleanup degrades maintenance status but does not change serving
health. Retry queues another manager reconciliation pass; it does not delete
files directly.

## Manager-set changes

After the first manager is ready, deploy later manager-set changes from one
replay-stable manifest:

```bash
wr-cli managers deploy-set --manifest manager-rollout.toml
```

The manifest names the authenticated owner, operation ID, manager endpoint,
source and target members, authorization policy, credentials, executables,
Systemd units, configs, selectors, and their digests. Targets start with
privileged admission closed. The rollout opens them only after replacement
membership and stopped-source evidence are complete.

Post-closure ambiguity remains `FAILED_CLOSED`; it does not automatically roll
back or reopen admission. After stopping and repairing every declared host, the
same owner may submit complete stopped/uniform-policy evidence:

```bash
wr-cli managers reset-failed-rollout \
  --manifest manager-rollout.toml --rollout-id ROLLOUT_ID
```

Reset clears the failed guard and grants permission for one new rollout. It does
not itself start managers or open admission. Submit a distinct fresh rollout
after repair.

## Cluster and lifecycle status

Use cluster status for composed runtime health:

```bash
wr-cli --manager https://manager.example:9000 cluster status --detail
wr-cli --manager https://manager.example:9000 cluster status --output json
wr-cli --manager https://manager.example:9000 \
  cluster wait --node node-a --severity healthy --timeout-secs 60
```

The default status command is display-only. `--fail-on degraded|unhealthy|unknown`
turns it into an exit gate. Automation should use severity and condition codes,
not human-readable detail.

Use lifecycle status for one trusted process endpoint:

```bash
wr-cli lifecycle status --endpoint https://manager.example:9000 --tls
wr-cli lifecycle wait --endpoint https://manager.example:9000 --tls \
  --state ready --service-kind manager --process-instance ACTIVATION_ID
```

Lifecycle state is not process-exit proof. Local foreground owners prove exit by
reaping their child; deployed exit evidence comes from the node agent's typed
Systemd inspection.

## Local foreground development

Build guest artifacts first, then run services and an optional scenario under
one foreground owner:

```bash
wr-cli dev build multi-node
wr-cli dev run \
  --manager-config path/to/manager.toml \
  --proxy-config primary=path/to/proxy.toml \
  --engine-config path/to/engine.toml \
  -- sh path/to/scenario.sh
```

The runner starts manager, proxy, and engine waves in order and waits for typed
readiness and routing convergence. Service exit, SIGINT, or SIGTERM stops the
scenario process group and reaps engines, proxies, then the manager. It creates
no persistent supervisor state.

## Logs and troubleshooting

```bash
wr-cli logs node deploy@10.0.1.20 --service wr-proxy --tail 200
wr-cli logs node deploy@10.0.1.20 --service wr-engine-blue --follow
```

| Symptom | Check |
| --- | --- |
| Manager deploy never becomes ready | TLS endpoint/SANs, exact activation ID, service kind, and bounded startup diagnostics. |
| Node operation pauses | `operations get`, agent attestation, backend inspection, and host connectivity. Do not repeat a mutation blindly. |
| Engine remains unhealthy | Offline tenant receipt, `database.url` versus `database.tenant`, module health, and local proxy routing version. |
| Peer traffic fails | Proxy `[endpoint_tls]`, `[client_tls]`, advertised `peer_address`, and policy enrollment. |
| Job commands are denied | Global manager TLS identity, policy role/capability, and queue scope. |
| NAT deployment is unreachable | Set manager `--advertise-address`; use `--ssh-port` only for the SSH transport. |

For local and protected validation commands, see [Testing](testing.md). Maintainer
change-class requirements live in the
[validation matrix](agents/wruntime-maintainer/validation.md).
