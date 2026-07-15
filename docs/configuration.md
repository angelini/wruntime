# Configuration

Each service reads a TOML config file. Start the three components **in order**: manager first, then proxy, then engines.

```bash
just manager   # dev (cargo run)
just proxy
just engine

just manager-release   # release binaries
just proxy-release
just engine-release
```

## wr-manager

```bash
just manager
```

`manager.toml`:

```toml
listen_address                = "0.0.0.0:9000"
engine_heartbeat_timeout_secs = 30
local_proxy_address           = "http://127.0.0.1:9001"  # required — scheduler posts jobs here

[tls]
cert_path    = "certs/manager.crt"
key_path     = "certs/manager.key"
ca_cert_path = "certs/ca.crt"

[database]
url             = "postgres://postgres@localhost:5433/wruntime_example"
max_connections = 10

[cluster]
cluster_id            = "default"           # all managers in the same cluster must match
gossip_listen_address = "0.0.0.0:9010"      # UDP address for chitchat gossip
# advertise_grpc_address = "http://manager-1:9000"  # optional; defaults to listen_address
# gossip_interval_ms = 500                           # optional; default 500

# scheduler_lease_secs       = 30    # optional; lease before another manager may reclaim
# scheduler_retry_base_secs  = 5     # optional; base backoff, doubles per failure
# scheduler_retry_cap_secs   = 300   # optional; max backoff cap
```

The `[tls]` section is required. All gRPC clients must present a certificate signed by the same CA.

The `[database]` section is required. The manager persists engines, routing rules,
and schemas to Postgres. Embedded SQL migrations run automatically on startup via
refinery, serialized across active-active managers by a Postgres advisory lock.

The `[cluster]` section is required. Multiple managers can run active-active against the same Postgres database. Each manager registers itself in the `wr_managers` table, heartbeats every 15 seconds, and participates in a chitchat gossip mesh for failure detection. Chitchat is now load-bearing for manager liveness — `gossip_listen_address` must be a reachable UDP address; a manager fails to start (fail-fast) if gossip cannot bind. Concurrent writes are serialized via Postgres row locks. Set `advertise_grpc_address` when the manager is behind a load balancer or NAT.

The manager also runs a Postgres-backed claim/lease job scheduler that submits scheduled jobs through `local_proxy_address` (the local proxy loopback) using the same routing/mTLS path as normal traffic; delivery is **at-least-once** (jobs must be idempotent). `local_proxy_address` is **required** — startup fails if it is unset or empty. `scheduler_lease_secs` must exceed the worst-case per-tick submission time, or a schedule can be reclaimed by another manager while still legitimately in flight.

## wr-proxy

```bash
just proxy
```

`proxy.toml`:

```toml
listen_address  = "127.0.0.1:9001"         # loopback only — engines on same host
control_address = "127.0.0.1:9002"         # gRPC control plane for engine registration/heartbeats

[node]
proxy_address = "http://127.0.0.1:9001"   # engines use this for outbound HTTP calls
peer_port     = 9443                       # mTLS peer listener port

[node.tls]
cert_path    = "certs/127.0.0.1.crt"
key_path     = "certs/127.0.0.1.key"
ca_cert_path = "certs/ca.crt"

[database]
url = "postgres://postgres@localhost:5433/wruntime_example"

[cache]
routing_table_ttl_secs = 5   # how often to poll the manager for routing updates
```

`listen_address` and `control_address` **must** bind to loopback (`127.0.0.1`, `::1`, or `localhost`) — only engines on the same host reach them, and the proxy now rejects a non-loopback value at config load. Cross-node traffic uses the mTLS peer listener on `peer_port` (default 9443, binds `0.0.0.0`). The peer address is derived automatically from `proxy_address` host + `peer_port`. The routing layer uses the derived peer address to distinguish local vs. remote rules.

`control_address` exposes a gRPC `NodeService` that engines on the same node use for registration and heartbeats instead of connecting directly to the manager. This decouples engines from the manager address and enables local-first orchestration.

The proxy is a streaming header-based router — it inspects only HTTP headers for routing decisions and streams request and response bodies through without buffering. It connects to the manager at startup, then polls for routing table updates in the background.

### Circuit breaker

The proxy can protect downstream engines from cascading failures with per-engine circuit breakers. Each engine address gets its own independent breaker — one open circuit does not affect routing to other engines.

```toml
[circuit_breaker]
failure_threshold  = 5    # consecutive failures (5xx / 429 / network error) before opening
open_duration_secs = 30   # seconds the breaker stays open before probing again
```

Both fields are optional and default to the values shown above. Omitting the `[circuit_breaker]` section keeps circuit breaking enabled with these defaults; there is currently no config flag to disable it.

### External routes (public API)

Expose a subset of internal module routes to external callers on a separate port. All `x-wr-*` headers are stripped from incoming requests before routing, preventing header spoofing.

```toml
[external]
listen_address = "0.0.0.0:8080"

[[external.route]]
path      = "/items"
methods   = ["GET", "POST"]
module    = "inventory"
namespace = "ecommerce"

[[external.route]]
path      = "/items/{id}"
methods   = ["GET"]
module    = "inventory"
namespace = "ecommerce"
```

Omit the `[external]` section to keep all routes internal-only.

## wr-engine

```bash
just engine
```

`engine.toml`:

```toml
listen_address = "127.0.0.1:9100"

[node]
proxy_address   = "http://127.0.0.1:9001"  # local proxy; WASM outbound calls rewrite to this
control_address = "http://127.0.0.1:9002"  # proxy's gRPC control plane for registration/heartbeats
peer_port       = 9443                      # mTLS peer port (peer address derived from proxy_address host)

[node.tls]
cert_path    = "certs/127.0.0.1.crt"
key_path     = "certs/127.0.0.1.key"
ca_cert_path = "certs/ca.crt"

[[module]]
name                 = "order-service"
namespace            = "ecommerce"
version              = "1.0.0"
wasm_path            = "modules/order_service.wasm"
schema_path          = "schemas/order_service.binpb"
request_timeout_secs = 10   # optional; default 30

[[module]]
name        = "inventory-service"
namespace   = "ecommerce"
version     = "1.0.0"
wasm_path   = "modules/inventory_service.wasm"
schema_path = "schemas/inventory_service.binpb"
# request_timeout_secs omitted — uses the default of 30 seconds
```

`listen_address` **must** bind to loopback (`127.0.0.1`, `::1`, or `localhost`); the engine rejects a non-loopback value at config load. To bind a routable interface anyway, set `allow_non_loopback_internal = true` at the top level of `engine.toml` (defaults to `false`; omitting it keeps existing configs valid). When enabled with a `0.0.0.0` bind, the engine still advertises `127.0.0.1` to peers, so the address is only reachable same-host — operators enabling this flag own end-to-end reachability.

> **`schema_path` is required on the first occurrence of each unique module tuple.** The first config occurrence of each unique `(namespace, name, version)` must declare a non-empty existing compiled `FileDescriptorSet`; later duplicate instances may omit `schema_path`. Schemas are uploaded to the manager on registration for discovery purposes.

On startup the engine:

1. Starts an inbound HTTP server on `listen_address`.
2. Registers itself and its modules with the manager to obtain requested secrets and DB credentials.
3. The manager creates schemas and default routes as unhealthy and resets module readiness for advertised tuples.
4. Provisions schemas and the job schema, runs migrations, builds pools, resolves secrets, and loads modules.
5. Sends an immediate readiness heartbeat after module load, then every 3 seconds, reporting healthy loaded modules.
6. Deregisters cleanly on `Ctrl+C`, which immediately marks its routing rules as unhealthy.

### Database migrations

Modules that use a database can declare a `migrations_path` pointing to a directory of SQL migration files. Migrations run on the engine (host side) at startup — after the Postgres schema is provisioned and before the WASM module loads. Default route rows may already be registered, but they remain unhealthy and unroutable until migrations, secret resolution, module load, readiness heartbeat, and manager health recomputation succeed.

```toml
[database]
url             = "postgres://user:pass@localhost:5432/mydb"
max_connections = 20

[[module]]
name            = "inventory"
namespace       = "ecommerce"
version         = "1.0.0"
wasm_path       = "modules/inventory.wasm"
schema_path     = "schemas/inventory.binpb"
database        = true
migrations_path = "modules/inventory/migrations"
```

Migration files follow the [refinery](https://github.com/rust-db/refinery) naming convention:

```
modules/inventory/migrations/
    V1__create_tables.sql
    V2__add_indexes.sql
```

Key behaviors:

- **Per-namespace DB roles:** The manager automatically generates and stores a random password for each namespace that needs database access. At engine registration, the manager returns per-namespace credentials (`wr_ns_{namespace}` roles). The engine creates these roles, grants them access to the relevant schemas, and connects module pools using the namespace role — modules never see the DB password. This provides namespace-level privilege isolation without any manual role or secret configuration.
- **Schema isolation:** `search_path` is set to the module's own schema before migrations run. A migration cannot modify tables belonging to another module.
- **Advisory locking:** An engine acquires a Postgres advisory lock before running migrations, preventing concurrent execution across engine replicas for the same module.
- **Idempotent:** Refinery tracks applied migrations in a `refinery_schema_history` table inside the module's schema. Already-applied migrations are skipped on subsequent startups.
- **Fail-fast:** If any migration fails, the engine exits before the module becomes routable. Registered route rows remain unhealthy, so the module never receives traffic.

### Per-module request timeout

`request_timeout_secs` sets a hard deadline on every request dispatched to a module. If the WASM handler does not produce a response within that window, the engine cancels the request and returns `504 Gateway Timeout` to the proxy. The proxy treats a `504` as a terminal error and does not retry on another instance.

The default is **30 seconds**. Set it lower for latency-sensitive modules, or higher for modules that perform long-running work such as batch imports.

```toml
[[module]]
name                 = "batch-processor"
namespace            = "pipeline"
version              = "1.0.0"
wasm_path            = "modules/batch_processor.wasm"
schema_path          = "schemas/batch_processor.binpb"
request_timeout_secs = 120
```

### LLM inference

Modules can call the Claude API (or other LLM providers) through a host binding. The engine holds the API key — guests never see credentials.

```toml
[llm]
provider        = "anthropic"
api_key_env     = "ANTHROPIC_API_KEY"   # env var read at startup
base_url        = "https://api.anthropic.com"  # optional, this is the default
max_tokens_limit = 8192                 # host-enforced ceiling per request

[[module]]
name        = "my-agent"
namespace   = "example"
version     = "1.0.0"
wasm_path   = "modules/my_agent.wasm"
schema_path = "schemas/my_agent.binpb"
llm         = true
```

Key behaviors:

- **Credential isolation:** The API key is resolved from an environment variable at engine startup and never enters the WASM sandbox.
- **Host-enforced token limit:** `max_tokens_limit` caps the `max_tokens` field on every request before forwarding to the API, preventing runaway generation.
- **Provider mapping:** Currently only `"anthropic"` is supported. The WIT interface is provider-agnostic so future providers can be added without changing guest code.
- **Streaming:** Guests can use `complete-stream` to get a cursor that yields typed stream events — zero or more text deltas, then one final usage event, then one stop event — following the same resource pattern as `row-cursor` for database queries. Tool-enabled requests cannot be streamed (they are rejected with `invalid-request`); use `complete` for tool calls.

### Blobstore

Modules can read and write objects in an S3-compatible store through a host binding. Add a `[blobstore]` section and set `blobstore = true` on each module that needs access.

```toml
[blobstore]
endpoint          = "http://127.0.0.1:8900"   # S3-compatible endpoint
access_key_id     = "..."
secret_access_key = "..."
region            = "us-east-1"   # optional; this is the default
max_object_size   = 16777216      # optional; bytes, default 16 MiB
max_list_objects  = 1000          # optional; default 1000

[[module]]
name        = "report-service"
namespace   = "example"
version     = "1.0.0"
wasm_path   = "modules/report_service.wasm"
schema_path = "schemas/report_service.binpb"
blobstore   = true
```

Key behaviors:

- **`max_object_size`** (default **16 MiB**) is enforced on both `put-object` (checked before upload) and `get-object` (the download is aborted mid-stream once the running total would exceed the limit — an oversized object is never fully buffered). Exceeding it returns `blob-error::too-large`.
- **`max_list_objects`** (default **1000**) caps a single `list-objects` call; exceeding it returns `blob-error::too-large` rather than silently truncating.

### Resource limits

Every request has per-store ceilings on the number of concurrently live guest-created host resources. They are enforced live (one running count per kind) so a guest cannot exhaust the wasmtime resource table and crash the engine. Omit the `[limits]` section to use the defaults.

```toml
[limits]
max_spans           = 1024   # concurrent guest-created tracing spans
max_db_transactions = 64     # concurrent open DB transactions
max_db_cursors      = 256    # concurrent open DB row cursors
max_llm_streams     = 32     # concurrent open LLM completion streams
```

| Key | Default | Resource | Over-cap behavior |
|-----|---------|----------|-------------------|
| `max_spans` | 1024 | tracing spans (`start`/`start-root`) | guest instance is **trapped** (request fails); engine survives |
| `max_db_transactions` | 64 | DB transactions (`begin-transaction`) | returns `db-error::connection` |
| `max_db_cursors` | 256 | DB row cursors (`query-stream`) | returns `db-error::connection` |
| `max_llm_streams` | 32 | LLM completion streams (`complete-stream`) | returns `llm-error::api` |

A resource frees its slot when the guest drops it, so long-lived requests should drop transactions, cursors, spans, and streams promptly to stay under the caps.

### Outbound HTTP body limit

`max_outbound_body_bytes` (top-level engine key, default **16 MiB**) bounds the size of an outbound HTTP request body a guest may send. The body is buffered incrementally up to this bound; a request whose body exceeds it is aborted (never fully buffered) and the guest's outbound call fails with an `HttpRequestBodySize` error. Response bodies are not affected (they stream through).

```toml
listen_address          = "127.0.0.1:9100"
max_outbound_body_bytes = 16777216   # optional; default 16 MiB
```

### Module health checks

After module load immediately, then every 3 seconds, the engine sends `GET /__health` to each loaded module instance. If the module responds with a `2xx` status within 5 seconds it is reported as healthy in the next heartbeat; otherwise it is omitted, and the manager marks its routing rule unhealthy so the proxy stops sending traffic to it.

By default a module does not need to handle `/__health` at all — the `wasi:http/incoming-handler` export just needs to exist. The engine treats any `2xx` as healthy and anything else (including a timeout or a dropped connection) as unhealthy.

To run custom checks — verifying database connectivity, warming caches, or validating internal state — handle the path explicitly in your module:

```rust
impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();

        if path == "/__health" {
            // Run any checks that make sense for this module.
            let ok = database::query("SELECT 1", &[]).is_ok();
            let status = if ok { 200 } else { 503 };
            return send_response(response_out, status, vec![]);
        }

        // ... normal request handling
    }
}
```

If the health handler returns a non-`2xx` status or does not respond within 5 seconds, the module is excluded from that heartbeat. The routing rule is marked unhealthy by the manager and will not receive traffic until a subsequent heartbeat reports the module healthy again.

### Routing rules

When an engine registers, the manager automatically creates one default routing rule per module that carries a schema, in the same transaction as the registration (and only after the engine's requested secrets and per-namespace DB credentials resolve successfully). Default rules start with `healthy = false`; the proxy indexes only healthy rules, so they are not routable until module readiness is reported and recomputed. You do not need to create these rules manually. The `UpsertRoutingRule` RPC remains available as an admin override — for example to add an extra rule or force a rule healthy:

```
# example using grpcurl
grpcurl -plaintext -d '{
  "rule_id": "r1",
  "source_module": "order-service",
  "source_namespace": "ecommerce",
  "destination_module": "inventory-service",
  "destination_namespace": "ecommerce",
  "destination_version": "1.0.0",
  "engine_id": "<engine-uuid>",
  "engine_address": "http://127.0.0.1:9100",
  "peer_address": "https://127.0.0.1:9443"
}' 127.0.0.1:9000 wruntime.ManagerService/UpsertRoutingRule
```

`peer_address` tells every proxy which node owns this rule. A proxy whose own derived `[node]` peer address matches will route directly to `engine_address`; all other proxies relay to `peer_address` and let that node route locally. The old `proxy_address` routing-rule field is reserved in `proto/wruntime.proto` and must not be used in new rules.

Current worker modules use the same descriptor/default-route path as service modules, so direct worker routes remain available but are gated by the same unhealthy-until-ready lifecycle. Queue-only workers with no default route require a future proto/control-plane route-publication flag or module mode.

To run **multiple instances** of the same module version across different engines (on the same or different nodes), create one rule per engine pointing at the same `(destination_module, destination_namespace, destination_version)`. The proxy round-robins across all healthy rules for that tuple.

To deploy a **new version** alongside the old one, register a new engine with `version = "2.0.0"` and add a corresponding rule. Callers that omit `x-wr-version` are load-balanced across all healthy versions; the proxy injects the concrete selected version into `x-wr-version` before forwarding. Callers that pin a version with the `x-wr-version` request header continue to reach the older instance.

### Multi-node deployment

Each proxy and engine binds its internal `listen_address`/`control_address` to loopback. Cross-node traffic uses the proxy mTLS peer listener on `peer_port`; each node's peer address is derived from `[node].proxy_address` host + `peer_port`.

```toml
# examples/multi-node/node-a/proxy.toml
listen_address  = "127.0.0.1:9001"
control_address = "127.0.0.1:9002"

[node]
proxy_address = "http://node-a-host:9001"
peer_port     = 9443

[node.tls]
cert_path    = "certs/127.0.0.1.crt"
key_path     = "certs/127.0.0.1.key"
ca_cert_path = "certs/ca.crt"

[database]
url = "postgres://postgres@db-host:5432/wruntime"

# examples/multi-node/node-a/engine-1.toml
listen_address = "127.0.0.1:9100"

[node]
proxy_address   = "http://node-a-host:9001"
control_address = "http://127.0.0.1:9002"
peer_port       = 9443

[node.tls]
cert_path    = "certs/127.0.0.1.crt"
key_path     = "certs/127.0.0.1.key"
ca_cert_path = "certs/ca.crt"
```

```toml
# examples/multi-node/node-b/proxy.toml
listen_address  = "127.0.0.1:9003"
control_address = "127.0.0.1:9004"

[node]
proxy_address = "http://node-b-host:9003"
peer_port     = 9443

[node.tls]
cert_path    = "certs/127.0.0.1.crt"
key_path     = "certs/127.0.0.1.key"
ca_cert_path = "certs/ca.crt"

[database]
url = "postgres://postgres@db-host:5432/wruntime"

# examples/multi-node/node-b/engine-1.toml
listen_address = "127.0.0.1:9200"

[node]
proxy_address   = "http://node-b-host:9003"
control_address = "http://127.0.0.1:9004"
peer_port       = 9443

[node.tls]
cert_path    = "certs/127.0.0.1.crt"
key_path     = "certs/127.0.0.1.key"
ca_cert_path = "certs/ca.crt"
```

When a module on Node A calls a module whose routing rule has `peer_address = "https://node-b-host:9443"`, Node A's proxy adds `x-wr-via-proxy: 1` and forwards the request to Node B's mTLS peer listener. Node B's `RoutingLayer` resolves the destination as a local engine and forwards to it.

### Manager high availability

Run multiple managers against the same Postgres database for active-active HA. Each manager needs a unique `gossip_listen_address` and should set `advertise_grpc_address` to its externally-reachable gRPC address:

```toml
# manager-1.toml
listen_address       = "0.0.0.0:9000"
local_proxy_address  = "http://127.0.0.1:9001"

[database]
url = "postgres://postgres@db-host:5432/wruntime"

[cluster]
cluster_id             = "prod"
gossip_listen_address  = "0.0.0.0:9010"
advertise_grpc_address = "http://manager-1:9000"

# manager-2.toml
listen_address       = "0.0.0.0:9000"
local_proxy_address  = "http://127.0.0.1:9001"

[database]
url = "postgres://postgres@db-host:5432/wruntime"

[cluster]
cluster_id             = "prod"
gossip_listen_address  = "0.0.0.0:9010"
advertise_grpc_address = "http://manager-2:9000"
```

Proxies and engines can point at any single manager — they all share the same Postgres state. For production, use a load balancer or DNS round-robin in front of the managers.

### CLI access

The CLI requires a manager address and does **not** require database access:

```bash
# Via flag
wr-cli --manager http://manager-1:9000 engines list

# Via environment variable
export WR_MANAGER=http://manager-1:9000
wr-cli engines list

# Discover all managers in the cluster from any seed
wr-cli --manager http://manager-1:9000 engines list
# Proxies and clients discover managers via ListManagers, which reconciles the
# DB-fresh set against chitchat; direct wr_managers reads are a documented
# bootstrap-only fallback when no manager RPC is reachable.
```

### Remote deployment via CLI

The CLI provides `wr managers` and `wr node` command groups for deploying to remote hosts via SSH. Both support systemd and Docker deployment formats. Bundles are **host-agnostic** — they contain template placeholders like `{host}` and `{db_url}` that are resolved at deploy time.

Both bundle and deploy commands auto-discover a `wr-deploy.toml` file in the current directory (or accept `--config <path>`). This file provides defaults for flags like `target`, `db_url`, `format`, etc. — see [deployment.md](deployment.md) for the full config reference.

#### Deploying a manager

```bash
# 1. Build a host-agnostic manager bundle (target defaults to x86_64-unknown-linux-gnu)
wr-cli managers bundle --manager-config examples/config/manager.toml

# 2. Deploy to the remote host (resolves {db_url} at deploy time)
wr-cli managers deploy wr-manager-bundle.tar.gz deploy@10.0.1.10 \
  --db-url "postgres://postgres@localhost:5432/wruntime" \
  --secret-key "<64-char-hex-key>"

# 3. Inspect a bundle to see template variables and checksums
wr-cli managers status wr-manager-bundle.tar.gz
```

The same bundle can be deployed to multiple managers with different `--db-url` and `--seed-node` values. With a `wr-deploy.toml`, deploy reduces to just the positional args:

```bash
wr-cli managers deploy wr-manager-bundle.tar.gz deploy@10.0.1.10
```

#### Deploying engine+proxy nodes

```bash
# 1. Build a host-agnostic node bundle (one build for all nodes)
wr-cli node bundle --engine-config examples/codegen/engine.toml

# 2. Deploy to each node (resolves {host}, {db_url})
export WR_MANAGER=http://10.0.1.10:9000

wr-cli node deploy wr-node-bundle.tar.gz deploy@10.0.1.20 \
  --db-url "postgres://postgres@10.0.1.10:5432/wruntime"

wr-cli node deploy wr-node-bundle.tar.gz deploy@10.0.1.30 \
  --db-url "postgres://postgres@10.0.1.10:5432/wruntime"
```

Use `--skip-build` to reuse compiled artifacts when only rebuilding the bundle metadata.
