# Investigation: WASM Guest-to-Guest Request Path

Traces the full code path when a WASM guest sends an HTTP request to another WASM guest through the proxy, with the most expensive operations labeled.

## Request Path: WASM Guest A → Proxy → WASM Guest B

```
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 1 — Source Engine: Outbound Interception                 │
│  wr-engine/src/state.rs:39-118                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. ModuleHttpHooks::send_request()                             │
│     - Header insertion (x-wr-destination, x-wr-source, etc.)   │
│     - URI rewrite to proxy address                              │
│                                                                 │
│  EXPENSIVE: Body buffering (state.rs:88-92)                     │
│     body.collect().await → to_bytes()                           │
│     Copies entire HyperOutgoingBody into a contiguous Bytes     │
│     allocation. This crosses the WASM→host boundary: wasmtime   │
│     copies guest linear memory out, then BodyExt::collect       │
│     re-aggregates all frames into one buffer.                   │
│                                                                 │
│  EXPENSIVE: HTTP/2 request to proxy (state.rs:95)               │
│     client.request(buffered).await                              │
│     Network round-trip. First request on this connection also   │
│     pays TCP + HTTP/2 handshake. Subsequent requests multiplex  │
│     over the pooled connection.                                 │
│                                                                 │
└────────────────────────────────┬────────────────────────────────┘
                                 │ HTTP/2
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 2 — Proxy: Tower Middleware Stack                        │
│  Request arrives as Request<Bytes> (body already buffered       │
│  by hyper server)                                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Layer 1: EgressLayer (egress.rs:79)                            │
│  EXPENSIVE: RwLock read on CachedRoutingTable (egress.rs:111)   │
│     table.read().await — checks if destination is internal.     │
│     Contention: shares the lock with routing table sync task    │
│     which takes a write lock on every poll interval.            │
│                                                                 │
│  Layer 2: SchemaValidationLayer (schema.rs:53)                  │
│  EXPENSIVE: RwLock read on SchemaCache.pools (schema.rs:111     │
│     or :154) — held while doing protobuf decode.                │
│     Contention: write lock taken when sync_schemas() inserts    │
│     a new module's schema.                                      │
│  EXPENSIVE: Protobuf decode/validate (schema.rs:166)            │
│     DynamicMessage::decode() — CPU-bound: parses the wire       │
│     format and validates field types against the descriptor.    │
│     For JSON bodies: serde_json deser + proto encode (double    │
│     transcode at schema.rs:127).                                │
│                                                                 │
│  Layer 3: RoutingLayer (routing.rs:69)                          │
│  EXPENSIVE: RwLock read on CachedRoutingTable (routing.rs:119)  │
│     table.read().await — second read lock in same request.      │
│  EXPENSIVE: Mutex lock on round-robin counters (routing.rs:227) │
│     counters.lock().unwrap() — std::sync::Mutex, brief but     │
│     contended under high fan-in to the same module.             │
│  - Semver parsing + comparison for version resolution           │
│    (routing.rs:140-169) — CPU, scales with number of rules.    │
│                                                                 │
│  Layer 4: ForwardService (forward.rs:71)                        │
│  - headers.clone() per candidate retry (forward.rs:99)          │
│  - body_bytes.clone() per retry — cheap (Bytes is refcounted)   │
│  EXPENSIVE: Circuit breaker lookup (forward.rs:136)             │
│     cb_registry.get_or_create() — DashMap lookup + possible     │
│     insert with lock.                                           │
│  EXPENSIVE: HTTP/2 request to destination engine                │
│     (forward.rs:141-144) client.request(forward_req).await      │
│     Network hop #2. Same TCP+H2 cost as hop #1.                │
│  EXPENSIVE: Response body buffering (forward.rs:147-151)        │
│     resp_body.collect().await.to_bytes()                        │
│     Full copy of engine response into contiguous buffer.        │
│                                                                 │
└────────────────────────────────┬────────────────────────────────┘
                                 │ HTTP/2
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 3 — Destination Engine: Inbound Server                   │
│  wr-engine/src/server.rs                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  EXPENSIVE: Body buffering (server.rs:112)                      │
│     BodyExt::collect(body).await.to_bytes()                     │
│     Third full-body copy on this path.                          │
│                                                                 │
│  EXPENSIVE: Registry lookup (server.rs:120)                     │
│     registry.next_sender().await — RwLock read on               │
│     HashMap<(ns,name,ver), InstanceList> (registry.rs:61)       │
│     Allocates 3 Strings for the HashMap key on every request.   │
│                                                                 │
│  - Channel send (server.rs:138)                                 │
│     sender.try_send() — bounded mpsc. Returns 429 if full.     │
│     Back-pressure point: queue depth = channel_capacity.        │
│                                                                 │
│  EXPENSIVE: oneshot::recv (server.rs:148)                       │
│     resp_rx.await — blocks until WASM finishes processing.      │
│                                                                 │
└────────────────────────────────┬────────────────────────────────┘
                                 │ mpsc channel
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 4 — Destination Engine: WASM Instantiation & Execution   │
│  wr-engine/src/engine.rs:349-409                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  MITIGATED: WASM instantiation (engine.rs:368)                  │
│     handler.pre.instantiate_async(&mut store).await             │
│     A new Store + instance is created PER REQUEST.              │
│     - Component::from_file() and ProxyPre::new() are done once │
│       at startup — pre-instantiation is amortized.              │
│     - Pooling allocator (PoolingAllocationConfig) is now        │
│       enabled by default. Linear memory slots are pre-mapped    │
│       at engine startup; instantiation reuses a slot instead    │
│       of mmap/munmap per request. On deallocation, wasmtime     │
│       resets memory via madvise (Linux) or anon mmap (other),   │
│       so no cross-request data leakage is possible.             │
│     - Still allocates per-request: Store, WasiCtx, ResourceTable│
│       These are cheap Rust-side structs, not kernel calls.      │
│                                                                 │
│  EXPENSIVE: ModuleState::new() (engine.rs:354-366)              │
│     - WasiCtxBuilder + ResourceTable allocation                 │
│     - If fs=tempdir: tempfile::tempdir() syscall (mkdtemp)      │
│       + preopened_dir() (engine.rs, state.rs:183-186)           │
│                                                                 │
│  EXPENSIVE: Request resource creation (engine.rs:379-382)       │
│     new_incoming_request() — wraps body as HyperIncomingBody,   │
│     inserts into ResourceTable.                                 │
│                                                                 │
│  EXPENSIVE: WASM execution (engine.rs:392)                      │
│     proxy.wasi_http_incoming_handler().call_handle()            │
│     Executes guest code. Cost depends entirely on guest logic.  │
│     If guest makes outbound HTTP calls, this recurses back to   │
│     Phase 1 (adds another full round-trip per call).            │
│                                                                 │
│  EXPENSIVE: Response body buffering (engine.rs:399-403)         │
│     rb.collect().await.to_bytes()                               │
│     Fourth body copy — WASM response collected into Bytes.      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Most Expensive Operations (ranked)

| Rank | Operation | Location | Type | Status |
|------|-----------|----------|------|--------|
| 1 | **Network hop: proxy → engine** | `forward.rs:141` | Network I/O | Open |
| 2 | **Network hop: source engine → proxy** | `state.rs:95` | Network I/O | Open |
| 3 | **Body buffering (4 copies total)** | `state.rs:88`, `server.rs:112`, `forward.rs:147`, `engine.rs:399` | Memory copy | Open |
| 4 | **Schema validation (protobuf decode)** | `schema.rs:166` | CPU | Open |
| 5 | **WASM instantiate_async per request** | `engine.rs:368` | Memory alloc + CPU | **Mitigated** — pooling allocator eliminates per-request mmap/munmap; slot reuse via pre-mapped slab |
| 6 | **CachedRoutingTable RwLock** (read 2x per request) | `egress.rs:111`, `routing.rs:119` | Lock contention | Open |
| 7 | **Registry RwLock + String allocs** | `registry.rs:61-62` | Lock + alloc | Open |
| 8 | **Round-robin Mutex** | `routing.rs:227` | Lock contention | Open |
| 9 | **tempdir creation** (if fs=tempdir) | `state.rs:184` | Syscall (mkdtemp) | Open |
| 10 | **Semver parsing per routing rule** | `routing.rs:140-169` | CPU | Open |

## Key Observations

**WASM instantiation cost is mitigated.** The pooling allocator (`PoolingAllocationConfig`) is now enabled by default via `[pool]` in engine config (`config.rs`). Wasmtime pre-maps a contiguous virtual memory slab at engine startup and carves it into fixed-size slots separated by guard regions. `instantiate_async` reuses a clean slot instead of calling mmap/munmap per request. On deallocation, wasmtime resets memory via `madvise(MADV_DONTNEED)` on Linux or anonymous mmap on other platforms — no cross-request data leakage is possible. Per-request cost is now dominated by Rust-side struct allocation (Store, WasiCtx, ResourceTable) rather than kernel calls. Configurable via `pool.total_component_instances` and `pool.max_memory_size` in engine TOML.

**The two network hops are now the dominant cost.** Source engine → proxy and proxy → destination engine each pay a full HTTP/2 round-trip. For co-located deployments these are loopback hops, but they still involve kernel socket buffers, syscalls, and serialization.

**Four full-body copies** occur as the request traverses source engine → proxy → destination engine → WASM store, and the response takes the same path back. Streaming the body without buffering would require the proxy's schema validation layer to work on streaming data (or be moved/skipped for trusted internal traffic).

**The CachedRoutingTable RwLock is acquired twice per request** — once in `EgressLayer` to check if the destination is internal, and again in `RoutingLayer` to resolve the destination engine. These could be collapsed into a single read.

**Registry key lookup allocates 3 Strings per request** (`registry.rs:62`) to construct the HashMap key. An interned or pre-hashed key would avoid this.
