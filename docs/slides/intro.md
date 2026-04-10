# WRuntime

WASM + WASI Runtime

Sandboxed execution with a built in GRPC API

With integrated systems: Postgres, S3, OTEL Tracing

And operational tools: load balancing, versioned APIs, circuit breakers

---

# WebAssembly Beyond the Browser

**WASM started as a browser compile target — it's now a portable, sandboxed runtime for server-side code**

- Bytecode format: compile once (Rust, Go, C, JS), run anywhere with a WASM runtime
- No direct syscalls, no filesystem, no network — the module only sees what the host explicitly provides
- Near-native speed with memory isolation — each module gets its own linear memory, no shared address space

**The host exposes features through typed interfaces:**

- **Database** — Postgres queries, transactions, streaming cursors — schema-isolated per module
- **Blobstore** — S3-compatible object storage with namespace-scoped keys
- **LLM inference** — Claude API access, host manages credentials, module never sees API keys
- **Tracing** — OpenTelemetry spans from inside WASM, structured attributes and error recording
- **Filesystem** — optional ephemeral tempdir per request

wruntime builds on [wasmtime](https://wasmtime.dev) and the WASI Preview 2 component model.

```wit
package wruntime:db@0.4.0;

interface database {
    query: func(
        sql:    string,
        params: list<pg-value>,
    ) -> result<list<row>, db-error>;
}
```

---

# Why Not Containers?

**Containers share a lot of the host by default**

- Shared kernel — container escapes are real
- Coarse network policies, ambient env vars
- No per-module, per-capability isolation
- Cold-start overhead vs WASM instantiation

**WASM sandboxing gives you**

- No syscalls, no ambient authority
- Memory-safe by construction
- Module calls only APIs it imports (WIT) and the host enables (TOML)
- Ideal for untrusted code: external teams or LLM-generated modules

---

# Built for LLM-Generated Guests

LLMs / Customers write module code that you can't trust. wruntime makes that safe:

- **Structured APIs only** — `.proto` defines the contract, codegen produces boilerplate, LLM fills in traits
- **Sandboxed capabilities** — module uses only what WIT imports + engine.toml allow, no surprise network calls or filesystem escape
- **Secrets stay in the host** — `DB_URL`, `ANTHROPIC_API_KEY` live in `engine.toml`, module never sees them
- **Blast radius is bounded** — a buggy module can't affect other modules, read their memory, or access their DB

The host is the trust boundary, not the guest code.

```rust
impl WasiHttpHooks for ModuleHttpHooks {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let original_uri = request.uri().to_string();
        let client = self.http_pool.get().clone();
```

---

# Architecture: Three Services

```
                          ┌───────────────┐
                          │  wr-manager   │
                          │  :9000 (gRPC) │
                          │  :9010 gossip │
                          └───────┬───────┘
                  routing table   │   heartbeat
               ┌──────────────────┼─────────────────┐
               │                  │                 │
       ┌───────▼───────┐  ┌───────▼──────┐  ┌───────▼───────┐
       │   wr-proxy    │  │   wr-proxy   │  │   wr-proxy    │
       │ :9001 (local) │  │ :9001 (local)│  │ :9001 (local) │
       │ :9443 (mTLS)  │  │ :9443 (mTLS) │  │ :9443 (mTLS)  │
       └───────┬───────┘  └──────┬───────┘  └───────┬───────┘
               │ HTTP            │ HTTP             │─ HTTP ───────────┐
       ┌───────▼───────┐  ┌──────▼───────┐  ┌───────▼───────┐  ┌───────▼───────┐
       │  wr-engine    │  │  wr-engine   │  │  wr-engine    │  │  wr-engine    │
       │  :9100        │  │  :9100       │  │  :9100        │  │  :9100        │
       │  [inventory]  │  │  [client]    │  │  [client]     │  │  [client]     │
       └───────────────┘  └──────────────┘  └───────────────┘  └───────────────┘
```

- **Manager**: module registry, routing table, schema store, secrets
- **Proxy**: streaming header-based router, cross-node mTLS forwarding
- **Engine**: wasmtime host, runs WASM modules with capability bindings
- Module identity: `(namespace, name, version)`

```bash
wr services list                          # view routing table
wr engines list                           # view registered engines
wr engines get <engine-id>                # modules on a specific engine
```

---

# Request Flow

```
Module A        Engine A        Proxy         Engine B        Module B
   │                │              │              │              │
   │ POST http://   │              │              │              │
   │ ns.b/Method    │              │              │              │
   │───────────────►│              │              │              │
   │                │ rewrite URL  │              │              │
   │                │ add x-wr-*   │              │              │
   │                │─────────────►│              │              │
   │                │              │ route lookup │              │
   │                │              │ inject hdrs  │              │
   │                │              │─────────────►│              │
   │                │              │              │─────────────►│
   │                │              │              │◄─────────────│
   │                │              │◄─────────────│              │
   │◄───────────────│◄─────────────│              │              │
```

- Engine intercepts outbound HTTP, rewrites to proxy, adds `x-wr-source` / `x-wr-destination`
- Proxy resolves target engine from routing table, streams body through without buffering
- **Schemas**: modules register `.proto` schemas with the manager — enables typed invocation via CLI
- **External routes**: expose modules as public APIs with path patterns and method filtering

```toml
# proxy.toml — fixed external routes
[[external.route]]
path      = "/api/items/{id}"
methods   = ["GET", "POST"]
module    = "inventory"
namespace = "ecommerce"
```

```bash
# invoke a module endpoint through the proxy (JSON auto-transcoded to protobuf)
wr invoke --destination http://ecommerce.inventory/Seed --body '{"item_count": 10}'
```

---

# Typed RPC Between Modules

```rust
let coordinator = CoordinatorServiceClient::new("codegen.coordinator");
let collector   = CollectorServiceClient::new("codegen.collector");
let agent       = AgentServiceClient::new("codegen.agent");

// Phase 1: Collect docs + source code
coordinator.update_task_status(proto::UpdateTaskStatusRequest {
    task_id: task_id.clone(),
    status: "collecting".into(),
})?;

let fetch_resp = collector.fetch_docs(proto::FetchDocsRequest { sources })?;

// Phase 2: Run agent with collected context
let agent_resp = agent.run_task(proto::RunTaskRequest {
    session_id: session_id.clone(),
    task_description: req.task_description.clone(),
    doc_prefixes: fetch_resp.doc_prefixes,
    max_turns: req.max_agent_turns,
})?;
```

- `ServiceClient::new("ns.module")` — address modules by name, proxy routes by header
- `.proto` → `wr-build` generates **trait**, **router**, and **typed client** — no hand-written serialization
- Type-safe: proto mismatch = compile error

---

# Implementing a Module

```rust
struct Component;
wr_sdk::export!(Component);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::agent_service_handle(&Component, request, response_out);
    }
}

impl proto::AgentService for Component {
    fn run_task(req: proto::RunTaskRequest) -> Result<proto::RunTaskResponse, ServiceError> {
        let span = tracing::start("agent.run_task", &[
            ("session.id", req.session_id.as_str()),
        ]);
        // ... your logic here ...
        Ok(proto::RunTaskResponse { result })
    }
}
```

`service_handle()` reads the request, routes to your trait method, serializes the response.

You only write the trait methods.

---

# Case Study: Codegen Pipeline

```
  User
   │
   ▼
┌─────────────┐  CreateTask   ┌────────────┐
│ coordinator │──(queue)─────►│   worker   │
│  DB: tasks  │               │            │
└─────────────┘               └──┬────────┬┘
      ▲                          │        │
      │ UpdateStatus        ┌────▼────┐ ┌─▼──────────┐
      │ CompleteTask        │collector│ │   agent    │
      └─────────────────────│         │ │ DB+Blob+LLM│
                            │Blob+FS  │ │ Multi-turn │
                            └─────────┘ └────────────┘
```

Worker orchestrates the pipeline: collect docs → run agent → report result

Each module isolated — collector can't touch DB, agent can't enqueue jobs.

---

# Codegen Capability Matrix

**The codegen example** is an LLM agent sandbox — it collects source code and documentation, then runs a multi-turn Claude conversation to generate code patches.

Four modules, each with only the capabilities it needs:

```
Module       DB  Blob  LLM  FS      Role
───────────  ──  ────  ───  ──────  ─────────────────────────────────────────
coordinator  x                      REST API + task state management
collector        x          temp    Fetch GitHub repos + docs into blobstore
worker       x                      Orchestrate pipeline (delegates to others)
agent        x   x     x    temp    Multi-turn LLM code generation
```

Collector can't touch the task DB. Agent can't enqueue new jobs. Worker can't call the LLM directly.

---

# LLM as a Host Capability

```rust
let resp = CompletionBuilder::sonnet()
    .system(&system_prompt)
    .max_tokens(8192)
    .user(&user_prompt)
    .complete()?;
```

- Host manages API keys — module never sees `ANTHROPIC_API_KEY`
- Rate-limit retry in module code: `complete_with_retry(|| builder)`
- OTel span per LLM turn for cost tracking

---

# Tracing Is a Capability Too

```rust
let span = tracing::start("agent.run_task", &[
    ("session.id",       session_id.as_str()),
    ("agent.max_turns",  &max_turns.to_string()),
    ("agent.doc_prefixes", &req.doc_prefixes.len().to_string()),
]);

// ... do work ...

tracing::set_attribute(&span, "agent.turns_used", &turn.to_string());
tracing::set_attribute(&turn_span, "tokens.input", &resp.usage.input_tokens.to_string());
tracing::set_attribute(&turn_span, "tokens.output", &resp.usage.output_tokens.to_string());
drop(span); // span ends on drop
```

- OTel spans from inside WASM via host binding
- Structured attributes, error recording with `tracing::set_error()`

```bash
wr metrics summary --since 1h             # view request metrics from OTel traces
```

---

# Worker Mode & Schedules

Modules can run as **job processors** instead of HTTP services:

```toml
# engine.toml
[[module]]
name       = "worker"
namespace  = "codegen"
mode       = "worker"
database   = true
worker_concurrency      = 4
worker_job_timeout_secs = 900
```

```toml
# schedules.toml — applied via `wr schedules apply --file`
[[schedule]]
worker_namespace = "codegen"
worker_name      = "worker"
job_type         = "/Cleanup/Run"
interval_secs    = 300
max_attempts     = 3
```

- Postgres-backed queue with `SKIP LOCKED` for distributed claiming
- `pg_notify` for event-driven polling (no busy-wait)
- Retry with backoff: failed jobs re-queue up to `max_attempts`

```bash
wr schedules apply --file schedules.toml  # create/update schedules
wr schedules list --namespace codegen     # view active schedules
```

---

# mTLS & Cross-Node Routing

```
        Node A                                     Node B
┌─────────────────────┐                   ┌─────────────────────┐
│  proxy :9001 local  │   mTLS :9443      │  proxy :9001 local  │
│        :9443 peer   │◄────────────────► │        :9443 peer   │
│                     │                   │                     │
│  engine :9100       │                   │  engine :9100       │
│    [inventory]      │                   │    [client]         │
└─────────────────────┘                   └─────────────────────┘
         │                                          │
         └──────────────┐  ┌────────────────────────┘
                        ▼  ▼
                  ┌──────────────┐
                  │  wr-manager  │
                  │  :9000 gRPC  │
                  │  (Postgres)  │
                  └──────────────┘
```

All inter-node traffic is mutually authenticated via TLS:

- Internal listener (`:9001`) binds loopback only — local engines talk here
- Peer listener (`:9443`) handles all cross-node traffic over mTLS
- Proxy resolves destination: local engine → direct HTTP, remote → mTLS peer forward

---

# External Routes & Egress

**External routes** — expose modules as public APIs:

```toml
# proxy.toml
[[external.route]]
path      = "/api/items/{id}"
methods   = ["GET", "POST"]
module    = "inventory"
namespace = "ecommerce"
```

**Egress** — controlled outbound access for modules:

```toml
[egress]
allowed_domains = ["api.anthropic.com", "*.github.com"]
```

- Modules hitting unrouted URLs pass through egress layer
- All `x-wr-*` headers stripped before forwarding to external hosts

---

# Deployment Workflow

**Shared config** — `wr-deploy.toml` in your working directory:

```toml
format     = "systemd"          # or "docker" or "k8s"
target     = "aarch64-unknown-linux-gnu"
workdir    = "/opt/wruntime"
db_url     = "postgres://postgres@10.0.0.5:5432/wruntime"
secret_key = "abcdef0123456789..."
cert_dir   = "./certs"
seed_nodes = ["10.0.0.1:9000", "10.0.0.2:9000"]
```

**Manager lifecycle**: `wr managers init` → `bundle` → `deploy` → `status`

**Node lifecycle**: `wr node init` → `bundle` → `deploy` → `status`

- Cross-compiles host binaries via `cargo-zigbuild` (x86 or ARM targets)
- Pre-compiled WASM (`.cwasm`) bundled for near-instant engine startup
- Streaming log tail during deploy for immediate feedback

```bash
wr managers bundle --manager-config manager.toml
wr managers deploy wr-manager-bundle.tar.gz admin@10.0.0.1

wr node bundle --engine-config engine.toml
wr node deploy wr-node-bundle.tar.gz admin@10.0.0.2

wr logs node admin@10.0.0.2 --format systemd --follow
```

---

# ARM on GCP — Cost & Throughput

**T2A (Ampere Altra) vs x86 pricing** (us-central1, on-demand):

```
Instance        Hourly    vs T2A
─────────────   ────────  ──────
t2a-standard-4  $0.090    baseline
e2-standard-4   $0.134    +49%
n2-standard-4   $0.194    +116%
```

The real advantage: **1 vCPU = 1 physical core** (no hyperthreading)

- x86 vCPU = hyperthread sharing a physical core (~0.6-0.7x throughput)
- T2A vCPU = dedicated core (1.0x throughput)
- Net effect: **~50-70% better price-performance** for parallel workloads like proxy routing and multi-instance WASM execution

**wruntime on ARM**:

- Host binaries: cross-compile with `cargo-zigbuild` → `aarch64-unknown-linux-gnu`
- WASM guests: architecture-neutral — no changes needed
- Caveat: T2A maxes at 48 vCPUs, fewer zones available

---

# Limitations & Trade-offs

**The sandbox is the feature — but it has boundaries:**

- **No native binaries** — headless browsers, FFmpeg, system tools can't run in WASM
- **Must compile to `wasm32-wasip2`** — Rust ecosystem coverage is strong, others are growing
- **Single-thread per request** — no `tokio::spawn` in guest code; scaling is done with multiple module instances
- **Proto-first** — every module boundary needs a `.proto` definition (no freeform JSON APIs)
- **Host binding surface** — DB, blobstore, LLM, tracing, filesystem — anything else requires a new WIT interface

**When to use containers instead:**

- You need native libraries (browser engines, media processing, ML inference with GPU)
- You trust the code and don't need per-module isolation
- Your team already has mature container infrastructure

wruntime is strongest for **multi-tenant, untrusted, or LLM-generated code** where the isolation guarantees outweigh the ecosystem constraints

---

# Recap

- **WASM sandboxing** — each module is a separate sandbox, no syscalls, no ambient authority
- **Capability model** — compile-time WIT declarations + deploy-time TOML configuration
- **Typed codegen** — `.proto` → traits, routers, clients — no hand-written plumbing
- **Host-managed infra** — DB, blobstore, LLM, secrets, tracing — modules never see credentials
- **Worker mode** — Postgres-backed job queues with schedules, retry, and event-driven polling
- **Cross-node mTLS** — mutual TLS for all inter-node traffic, cert CLI for easy setup
- **Deploy pipeline** — `wr-deploy.toml` + bundle/deploy commands, systemd or Docker, ARM cross-compilation
- **Built for untrusted code** — LLM-generated guests are safe by default

Standards: WASI Preview 2, WIT, wasmtime, protobuf
