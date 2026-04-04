# WRuntime
Capability-based security for multi-module systems

Rust SDK + protobuf codegen
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
- Module calls only APIs it imports (WIT)
  and the host enables (TOML)
- Ideal for untrusted code: external teams
  or LLM-generated modules get the same
  guarantees without trusting the code

---
# Built for LLM-Generated Guests
LLMs write module code — you can't review
every line. wruntime makes that safe:
**Structured APIs only**
`.proto` defines the contract, codegen
produces boilerplate — LLM fills in traits
**Sandboxed capabilities**
Module uses only what WIT imports +
engine.toml allow — no surprise network
calls, no filesystem escape
**Secrets stay in the host**
`DB_URL`, `ANTHROPIC_API_KEY` live in
`engine.toml` — module never sees them
**Blast radius is bounded**
A buggy module can't affect other modules,
read their memory, or access their DB
The host is the trust boundary,
not the guest code.
---
# Architecture: Three Services
```
 ┌──────────┐   gRPC    ┌──────────┐
 │ wr-proxy │◄─────────►│wr-manager│
 │  :9001   │  routing   │  :9000   │
 └────┬─────┘  table     └──────────┘
      │ HTTP   sync           ▲
      │ stream                │ hb
 ┌────▼─────┐           ┌────┴─────┐
 │wr-engine │           │wr-engine │
 │  :9100   │           │  :9101   │
 └──────────┘           └──────────┘
```
- **Manager**: module registry + routing table
- **Proxy**: streaming header-based router
- **Engine**: wasmtime host, runs modules
- Module identity: `(namespace, name, version)`
---
# Request Flow
```
Module A                    Proxy
   │  POST /rpc/Method        │
   │  ──────────────────────► │
   │  x-wr-source: ns.a      │
   │  x-wr-dest:   ns.b      │
   │                          │
   │    resolve dest engine   │
   │    stream body through   │
   │                          │
   │         Module B         │
   │  ◄────────────────────── │
   │       response           │
```
- Engine intercepts outbound HTTP
- Adds `x-wr-source` / `x-wr-destination`
- Proxy resolves target, streams body
- Target engine dispatches to WASM instance
---
# Capability Model
Modules declare imports at **compile time**:
```wit
world agent {
  import wruntime:db/database@0.4.0;
  import wruntime:blobstore/store@0.1.0;
  import wruntime:llm/inference@0.1.0;
  import wruntime:tracing/span;
  // + wasi:http, wasi:io, wasi:clocks
  export wasi:http/incoming-handler@0.2.6;
}
```
Host enables capabilities at **deploy time**
via `engine.toml` per module.
Missing import = **link-time error** (fail-closed)
---
# Capability Matrix — Codegen Example
From `engine.toml`:
```
Module       DB  Blob  LLM  FS
───────────  ──  ────  ───  ────
coordinator  x
collector        x              temp
worker       x
agent        x   x     x    temp
```
- Coordinator: DB only (task state)
- Collector: blobstore + tempdir (docs)
- Worker: DB only (delegates to others)
- Agent: most privileged (DB+blob+LLM+fs)
Each module gets exactly what it needs.
No more, no less.
---
# Proto Defines the Contract
```proto
service AgentService {
  rpc RunTask (RunTaskRequest)
      returns (RunTaskResponse);
  rpc GetSession (GetSessionRequest)
      returns (GetSessionResponse);
}
```
`.proto` → `wr-build` generates:
- **Trait**: `AgentService` with typed methods
- **Router**: `agent_service_router()` fn
- **Client**: `AgentServiceClient` struct
No hand-written routing or serialization.
---
# Implementing a Module
```rust
struct Component;
wr_sdk::export!(
    Component with_types_in wr_sdk::bindings
);
impl wr_sdk::ServiceGuest for Component {
    fn handle(
        req: IncomingRequest,
        out: ResponseOutparam,
    ) {
        let path = req.path_with_query()
            .unwrap_or_default();
        let body = read_body(
            req.consume().unwrap()
        );
        let (status, resp) =
            proto::agent_service_router(
                &Component, &path, &body,
            );
        send_response(out, status, resp);
    }
}
```
Router dispatches to your trait impl.
You only write the trait methods.
---
# Case Study: Codegen Pipeline
```
  User
   │
   ▼
┌─────────────┐ CreateTask ┌────────┐
│ coordinator │──(queue)──►│ worker │
│  DB: tasks  │            │        │
└─────────────┘            └───┬────┘
      ▲                        │
      │ UpdateStatus      ┌────▼─────┐
      │ CompleteTask      │collector │
      └───────────────────│ Blobstore│
                          └────┬─────┘
                               │
                          ┌────▼─────┐
                          │  agent   │
                          │ DB+Blob  │
                          │  +LLM   │
                          └──────────┘
```
Worker orchestrates the pipeline:
  collect docs → run agent → report result
Each module isolated — collector can't
touch DB, agent can't enqueue jobs.
---
# Typed RPC Between Modules
```rust
let coordinator =
    CoordinatorServiceClient::new(
        "codegen.coordinator"
    );
let collector =
    CollectorServiceClient::new(
        "codegen.collector"
    );
let agent =
    AgentServiceClient::new(
        "codegen.agent"
    );
let fetch_resp = collector.fetch_docs(
    proto::FetchDocsRequest { sources }
)?;
```
- `ServiceClient::new("ns.module")`
- HTTP under the hood, proxy routes by header
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
- Host manages API keys — module never
  sees `ANTHROPIC_API_KEY`
- Fluent builder API via `wr_sdk::llm`
- Rate-limit retry in module code:
  `complete_with_retry(|| builder)`
- OTel span per LLM turn for cost tracking
---
# Tracing Is a Capability Too
```rust
let span = tracing::start(
    "agent.run_task",
    &[
        ("session.id", session_id.as_str()),
        ("agent.max_turns",
         &max_turns.to_string()),
    ],
);
// ... do work ...
tracing::set_attribute(
    &span, "agent.turns_used",
    &turn.to_string(),
);
drop(span); // span ends on drop
```
- OTel spans from inside WASM via host
- Structured attributes, error recording
- `tracing::set_error()` for failures
---
# Recap
**WASM sandboxing**
  Each module is a separate sandbox
**Capability model**
  Compile-time WIT + deploy-time TOML
**Typed codegen**
  `.proto` → traits, routers, clients
**Host-managed infra**
  DB, blobstore, LLM keys, tracing
**Built for untrusted code**
  LLM-generated guests are safe by default
Standards: WASI Preview 2, WIT,
wasmtime, protobuf
