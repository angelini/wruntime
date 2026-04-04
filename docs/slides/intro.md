# WRuntime

Capability-based security for multi-module systems

Rust SDK + protobuf codegen

---

# WebAssembly Beyond the Browser

**WASM started as a browser compile target — it's now a portable, sandboxed runtime for server-side code**

- Bytecode format: compile once (Rust, Go, C, JS), run anywhere with a WASM runtime
- No direct syscalls, no filesystem, no network — the module only sees what the host explicitly provides
- Near-native speed with memory isolation — each module gets its own linear memory, no shared address space

**WASI (WebAssembly System Interface)** extends WASM with standardized host capabilities:

- **WASI Preview 2** — the current standard, built on the Component Model
- **WIT (WebAssembly Interface Types)** — IDL for declaring imports/exports between host and guest
- **Components** — self-describing modules with typed interfaces, composable without shared memory
- Runtimes: [wasmtime](https://wasmtime.dev), wasmer, wazero — wruntime builds on wasmtime

wruntime uses WASI Preview 2 + custom WIT interfaces to expose DB, blobstore, LLM, and tracing to guests.

---

# Why Not Just Containers?

**Containers share too much by default**

- Shared kernel — container escapes are real
- Coarse network policies, ambient env vars
- No per-module, per-capability isolation
- Cold-start overhead vs WASM instantiation

**WASM sandboxing gives you**

- No syscalls, no ambient authority
- Memory-safe by construction
- Module calls only APIs it imports (WIT) and the host enables (TOML)
- Ideal for untrusted code: external teams or LLM-generated modules get the same guarantees without trusting the code

---

# Built for LLM-Generated Guests

LLMs write module code — you can't review every line. wruntime makes that safe:

- **Structured APIs only** — `.proto` defines the contract, codegen produces boilerplate, LLM fills in traits
- **Sandboxed capabilities** — module uses only what WIT imports + engine.toml allow, no surprise network calls or filesystem escape
- **Secrets stay in the host** — `DB_URL`, `ANTHROPIC_API_KEY` live in `engine.toml`, module never sees them
- **Blast radius is bounded** — a buggy module can't affect other modules, read their memory, or access their DB

The host is the trust boundary, not the guest code.

---

# Architecture: Three Services

```
 ┌──────────┐       gRPC        ┌──────────┐
 │ wr-proxy │◄─────────────────►│wr-manager│
 │  :9001   │   routing table   │  :9000   │
 └────┬─────┘      sync         └──────────┘
      │ HTTP                         ▲
      │ stream                       │ heartbeat
 ┌────▼─────┐                   ┌────┴─────┐
 │wr-engine │                   │wr-engine │
 │  :9100   │                   │  :9101   │
 └──────────┘                   └──────────┘
```

- **Manager**: module registry + routing table
- **Proxy**: streaming header-based router
- **Engine**: wasmtime host, runs WASM modules
- Module identity: `(namespace, name, version)`

---

# Request Flow

```
Module A                               Proxy                               Module B
   │  POST /rpc/Method                   │                                    │
   │  x-wr-source: ns.a                  │                                    │
   │  x-wr-dest:   ns.b                  │                                    │
   │  ─────────────────────────────────► │                                    │
   │                                     │  resolve dest engine from table    │
   │                                     │  inject x-wr-module headers        │
   │                                     │  stream body through               │
   │                                     │  ─────────────────────────────────►│
   │                                     │                                    │
   │                                     │◄───────────────────────────────────│
   │◄──────────────────────────────────  │              response              │
```

- Engine intercepts outbound HTTP, adds `x-wr-source` / `x-wr-destination`
- Proxy resolves target engine, streams body through without buffering
- Target engine dispatches to WASM instance via `ModuleRegistry`

---

# Capability Model

Modules declare imports at **compile time**:

```wit
world agent {
    import wruntime:db/database@0.4.0;
    import wruntime:blobstore/store@0.1.0;
    import wruntime:llm/inference@0.1.0;
    import wruntime:tracing/span;
    // + wasi:http, wasi:io, wasi:clocks, wasi:random
    export wasi:http/incoming-handler@0.2.6;
}
```

Host enables capabilities at **deploy time** via `engine.toml` per module.

Missing import = **link-time error** (fail-closed, not a runtime surprise)

---

# Capability Matrix — Codegen Example

From `engine.toml`:

```
Module       DB  Blob  LLM  FS      Role
───────────  ──  ────  ───  ──────  ─────────────────────────────────────────
coordinator  x                      REST API + task state management
collector        x          temp    Fetch GitHub repos + docs into blobstore
worker       x                      Orchestrate pipeline (delegates to others)
agent        x   x     x    temp    Multi-turn LLM code generation
```

Each module gets exactly what it needs. No more, no less.

---

# Proto Defines the Contract

```proto
service AgentService {
    rpc RunTask    (RunTaskRequest)    returns (RunTaskResponse);
    rpc GetSession (GetSessionRequest) returns (GetSessionResponse);
}
```

`.proto` → `wr-build` generates:

- **Trait**: `AgentService` with typed methods
- **Router**: `agent_service_router()` dispatches HTTP path to trait method
- **Client**: `AgentServiceClient` struct with typed RPC methods over HTTP

No hand-written routing or serialization.

---

# Implementing a Module

```rust
struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(req: IncomingRequest, out: ResponseOutparam) {
        let path = req.path_with_query().unwrap_or_default();
        let body = read_body(req.consume().unwrap());
        let (status, resp) = proto::agent_service_router(&Component, &path, &body);
        send_response(out, status, resp);
    }
}
```

Router dispatches to your trait impl. You only write the trait methods.

---

# Case Study: Codegen Pipeline

```
  User
   │
   ▼
┌─────────────┐  CreateTask   ┌────────────┐
│ coordinator │──(queue)─────►│   worker   │
│  DB: tasks  │               │            │
└─────────────┘               └─────┬──────┘
      ▲                             │
      │ UpdateStatus           ┌────▼───────┐
      │ CompleteTask           │  collector │  Fetch GitHub repos + docs.rs
      └────────────────────────│  Blobstore │  Store artifacts in S3
                               └────┬───────┘
                                    │
                               ┌────▼───────┐
                               │   agent    │  Multi-turn Claude conversation
                               │ DB+Blob+LLM│  Produces unified diff
                               └────────────┘
```

Worker orchestrates the pipeline: collect docs → run agent → report result

Each module isolated — collector can't touch DB, agent can't enqueue jobs.

---

# Typed RPC Between Modules

```rust
let coordinator = CoordinatorServiceClient::new("codegen.coordinator");
let collector   = CollectorServiceClient::new("codegen.collector");
let agent       = AgentServiceClient::new("codegen.agent");

// Phase 1: Collect docs + source code
let _ = coordinator.update_task_status(proto::UpdateTaskStatusRequest {
    task_id: task_id.clone(),
    status: "collecting".into(),
});

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
- Type-safe: proto mismatch = compile error

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
- Fluent builder API via `wr_sdk::llm`
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

---

# Recap

- **WASM sandboxing** — each module is a separate sandbox, no syscalls, no ambient authority
- **Capability model** — compile-time WIT declarations + deploy-time TOML configuration
- **Typed codegen** — `.proto` → traits, routers, clients — no hand-written plumbing
- **Host-managed infra** — DB, blobstore, LLM keys, tracing — modules never see credentials
- **Built for untrusted code** — LLM-generated guests are safe by default

Standards: WASI Preview 2, WIT, wasmtime, protobuf
