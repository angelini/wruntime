# Investigation: Request Latency — Where Do the Most Expensive Operations Lie?

Follow-up to `wasm_guest_request_path.md`. The proxy now streams bodies end-to-end (`ProxyBody` wraps `hyper::body::Incoming` behind `Pin<Box<dyn Body + Send>>`). Schema validation was removed from the proxy. This investigation re-ranks the hot spots.

## What Changed Since the Last Investigation

| Item | Before | After |
|------|--------|-------|
| Proxy body handling | Fully buffered (`Bytes`) | **Streaming** — `ProxyBody::streaming()` wraps `Incoming` directly |
| Proxy response body | `body.collect().await.to_bytes()` | **Streaming** — `ProxyBody::streaming(resp_body)` returned from `ForwardService` |
| Schema validation layer | Present — protobuf decode per request | **Removed** — proxy never inspects bodies |
| Routing table lock | Read 2x per request (egress + routing) | **Read 1x** — single `table.read().await` in `RoutingLayer` |
| WASM instantiation | Pooling allocator added | Same — mitigated |

## Current Request Path: WASM Guest A -> Proxy -> WASM Guest B

```
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 1 — Source Engine: Outbound Interception                 │
│  wr-engine/src/state.rs:39-118                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. ModuleHttpHooks::send_request()                             │
│     - Header insertion: x-wr-destination, x-wr-source,         │
│       x-wr-source-ns (state.rs:47-59)                          │
│     - URI rewrite to proxy address (state.rs:62-78)             │
│                                                                 │
│  ■ EXPENSIVE: Body buffering (state.rs:87-92)                   │
│     body.collect().await → .to_bytes()                          │
│     Required because the pooled hyper Client needs              │
│     Full<Bytes> (Send + 'static). The entire WASM outgoing      │
│     body is copied from guest linear memory into a contiguous   │
│     Bytes allocation.                                           │
│                                                                 │
│  ■ EXPENSIVE: HTTP/2 request to proxy (state.rs:95)             │
│     client.request(buffered).await                              │
│     Network round-trip #1. First request pays TCP + H2          │
│     handshake; subsequent requests multiplex on the pooled      │
│     connection.                                                 │
│                                                                 │
│  ✓ STREAMING: Response from proxy (state.rs:104-106)            │
│     resp_body.map_err(...).boxed_unsync() → HyperIncomingBody   │
│     Response body streams back to guest without buffering.      │
│                                                                 │
└────────────────────────────────┬────────────────────────────────┘
                                 │ HTTP/2
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 2 — Proxy: Tower Middleware Stack                        │
│  Body arrives as ProxyBody (streaming Incoming, not buffered)   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Layer 1: TracingLayer (tracing.rs)                             │
│     Creates OTel span per request. Cost: ~1-2 us.              │
│                                                                 │
│  Layer 2: RoutingLayer (routing.rs:81-293)                      │
│  ■ LOCK: RwLock read on CachedRoutingTable (routing.rs:132)     │
│     table.read().await — single acquisition per request.        │
│     Contention: write lock taken by sync_routing_table() on     │
│     every TTL interval.                                         │
│  ■ CPU: Version resolution (routing.rs:151-223)                 │
│     semver::VersionReq::parse() + filter + max_by over healthy  │
│     rules. Linear scan; cost scales with rule count.            │
│  [x] Round-robin: per-route AtomicUsize (item 7 done)           │
│     fetch_add(1, Relaxed) % len; no mutex, lock-free            │
│     Selection state moved into the routing table.               │
│  ■ ALLOC: HashMap key construction (routing.rs:256-260)         │
│     .entry((ns.clone(), module.clone(), version.clone()))       │
│     3 String clones per request for the round-robin counter     │
│     lookup.                                                     │
│                                                                 │
│  Layer 3: EgressLayer (egress.rs)                               │
│     Only activates if ExternalEgress extension is set (no       │
│     internal route matched and egress enabled). No lock, no     │
│     cost on normal internal traffic.                            │
│                                                                 │
│  Layer 4: ForwardService (forward.rs:51-162)                    │
│  ■ LOCK: Circuit breaker lookup (forward.rs:100)                │
│     cb_registry.get_or_create() — Mutex<HashMap>. Clones the   │
│     StateMachine on every request for the given engine address. │
│  ■ EXPENSIVE: HTTP/2 forward to destination engine              │
│     (forward.rs:105-108)                                        │
│     client.request(forward_req).await                           │
│     Network round-trip #2. Hyper internally pools H2            │
│     connections.                                                │
│                                                                 │
│  ✓ STREAMING: Request body flows through untouched.             │
│     ProxyBody wraps Incoming; all layers only inspect headers.  │
│  ✓ STREAMING: Response body returned as                         │
│     ProxyBody::streaming(resp_body) (forward.rs:136).           │
│                                                                 │
└────────────────────────────────┬────────────────────────────────┘
                                 │ HTTP/2
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 3 — Destination Engine: Inbound Server                   │
│  wr-engine/src/server.rs:74-155                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ■ EXPENSIVE: Body buffering (server.rs:112-113)                │
│     BodyExt::collect(body).await.to_bytes()                     │
│     Entire request body buffered before dispatching to WASM     │
│     module task via mpsc channel. Required because              │
│     InboundRequest carries Request<Bytes>.                      │
│                                                                 │
│  ■ LOCK: Registry lookup (registry.rs:61-62)                    │
│     inner.read().await — RwLock on HashMap.                     │
│  ■ ALLOC: HashMap key (registry.rs:62)                          │
│     .get(&(ns.to_string(), name.to_string(), ver.to_string())) │
│     3 String allocations per request for the HashMap lookup     │
│     key. These are immediately discarded.                       │
│                                                                 │
│  - Channel send: sender.try_send() (server.rs:138)              │
│     Bounded mpsc. Returns 429 if full. Cheap.                   │
│                                                                 │
│  ■ WAIT: oneshot::recv (server.rs:148)                          │
│     resp_rx.await — blocks until WASM finishes processing.      │
│     This is the full WASM execution time.                       │
│                                                                 │
└────────────────────────────────┬────────────────────────────────┘
                                 │ mpsc channel
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 4 — Destination Engine: WASM Instantiation & Execution   │
│  wr-engine/src/engine.rs:357-417                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ■ ALLOC: ModuleState::new() (engine.rs:362-374)                │
│     WasiCtxBuilder, ResourceTable, clones of proxy_uri,         │
│     http_client, db_pool, blobstore per request.                │
│     If fs=tempdir: mkdtemp() syscall (state.rs:184).            │
│                                                                 │
│  ■ MITIGATED: WASM instantiation (engine.rs:376)                │
│     handler.pre.instantiate_async(&mut store).await             │
│     Pooling allocator reuses pre-mapped memory slots.           │
│     Per-request cost: Rust-side Store + WasiCtx allocation      │
│     (no kernel calls with pooling enabled).                     │
│                                                                 │
│  ■ EXPENSIVE: WASM execution (engine.rs:398-401)                │
│     proxy.wasi_http_incoming_handler().call_handle()            │
│     Guest code execution time. If the guest makes outbound      │
│     HTTP calls, each recurses back to Phase 1 adding another    │
│     full round-trip.                                            │
│                                                                 │
│  ■ EXPENSIVE: Response body buffering (engine.rs:407-411)       │
│     rb.collect().await.to_bytes()                               │
│     WASM response collected into contiguous Bytes before        │
│     returning through the channel to server.rs, which sends     │
│     it back as Full<Bytes>.                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Most Expensive Operations (ranked)

| Rank | Operation | Location | Type | Notes |
|------|-----------|----------|------|-------|
| 1 | **Network hop: source engine -> proxy** | `state.rs:95` | Network I/O | H2 multiplexed; first call pays handshake |
| 2 | **Network hop: proxy -> dest engine** | `forward.rs:105` | Network I/O | H2 multiplexed; hyper connection pool |
| 3 | **Engine inbound body buffering** | `server.rs:112` | Memory copy | Full `collect().await.to_bytes()` — blocks dispatch until body fully received |
| 4 | **WASM outbound body buffering** | `state.rs:88-92` | Memory copy | Required by `Client<_, Full<Bytes>>` signature |
| 5 | **WASM response body buffering** | `engine.rs:407-411` | Memory copy | Collected before sending through oneshot channel |
| 6 | **WASM guest execution** | `engine.rs:398-401` | CPU (guest) | Dominates for compute-heavy modules |
| 7 | **Routing table RwLock** | `routing.rs:132` | Lock contention | Single read per request (down from 2) |
| 8 | **Round-robin counter Mutex** _(resolved, item 7)_ | `routing.rs` | — | Replaced by per-route `AtomicUsize` in `IndexedRoutingTable`; hot path is lock-free |
| 9 | **Circuit breaker Mutex + clone** | `circuit_breaker.rs:27` | Lock + alloc | `HashMap` lookup + `StateMachine` clone per request |
| 10 | **Registry key String allocs** | `registry.rs:62` | Alloc | 3 owned Strings built to query HashMap |
| 11 | **ModuleState per-request alloc** | `engine.rs:362-374` | Alloc | WasiCtx, ResourceTable, clones of Arc pools |
| 12 | **WASM instantiate_async** | `engine.rs:376` | Alloc + CPU | Mitigated by pooling allocator |
| 13 | **Semver parsing in routing** | `routing.rs:151-223` | CPU | Linear scan over rules per request |

## Key Observations

### Streaming eliminated the proxy as a buffering bottleneck

The proxy no longer copies bodies. `ProxyBody::streaming()` wraps `hyper::body::Incoming` and all four middleware layers (tracing, routing, egress, forward) only inspect headers. Bodies flow through the proxy without touching application code. The schema validation layer was also removed, eliminating the protobuf decode cost.

### Three body buffering points remain — all in the engine

| Point | Location | Direction | Why it exists |
|-------|----------|-----------|---------------|
| `state.rs:88-92` | WASM outbound hook | Request out | `Client<_, Full<Bytes>>` needs a `Send + 'static` body; `HyperOutgoingBody` is `!Send` |
| `server.rs:112` | Engine inbound server | Request in | `InboundRequest` carries `Request<Bytes>` through mpsc channel |
| `engine.rs:407-411` | WASM executor | Response out | Response must cross oneshot channel as `Response<Bytes>` |

All three stem from the same root cause: the mpsc/oneshot channel boundary between `server.rs` and the WASM executor task requires owned `Bytes`, not streaming bodies. The outbound buffering (`state.rs`) has an additional constraint: wasmtime's `HyperOutgoingBody` is `!Send`.

### The routing table lock contention was halved

The previous investigation noted two `RwLock` reads per request (egress + routing). Now the egress layer only activates when the routing layer sets the `ExternalEgress` extension — normal internal traffic takes a single read lock in `RoutingLayer`.

### The round-robin counter uses a sync Mutex

**Resolved (item 7):** the round-robin `Mutex<HashMap<_, usize>>` was replaced with per-route `AtomicUsize` counters (`all_versions_counter` plus a per-version `counter`) living inside `IndexedRoutingTable`, seeded best-effort from the previous table on each sync. The hot path is now lock-free (`fetch_add(1, Relaxed) % len`) with no per-request `HashMap` key allocation.

### Registry lookup allocates 3 throwaway Strings

`registry.rs:62` constructs `(namespace.to_string(), name.to_string(), version.to_string())` as the HashMap key on every request. These allocations are discarded immediately after the lookup.

### Circuit breaker clones a StateMachine per request

`circuit_breaker.rs:26-31` locks a `Mutex<HashMap>`, looks up or inserts a breaker, then `.clone()`s the `StateMachine`. The clone is necessary because `failsafe::StateMachine` tracks state internally via `Arc`, so the clone is cheap (atomic refcount), but the Mutex lock is real contention.

## Cost Breakdown per Module-to-Module Call

For a single A -> B call (no chaining):

```
                           Estimated Latency Contribution
                           (loopback, small payload)
────────────────────────────────────────────────────────
Network: engine A -> proxy     ~50-100 us  (H2, loopback)
Proxy middleware (no buffer)   ~5-15 us    (routing + CB lookup)
Network: proxy -> engine B     ~50-100 us  (H2, loopback)
Engine B body buffer           ~1-5 us     (small payload collect)
Registry lookup                ~1-3 us     (RwLock + alloc)
Channel dispatch               ~1 us       (mpsc try_send)
WASM instantiate               ~10-50 us   (pooled)
WASM execution                 variable    (guest logic)
Response body buffer           ~1-5 us     (small payload collect)
Response return path           ~50-100 us  (reverse network hops)
────────────────────────────────────────────────────────
Overhead (excl. WASM exec):    ~170-380 us
```

For large payloads, body buffering at the three engine points dominates — cost scales linearly with body size.

## Engine Body Streaming: Investigated, Not Worth It

Streaming bodies across the engine channel boundary was investigated in detail. The conclusion: **do not implement — complexity outweighs benefit.**

### Why it's hard

All three body types that cross channel boundaries are `!Send`:
- `hyper::body::Incoming` (HTTP/2) — contains `h2::RecvStream`, `!Send`
- `HyperIncomingBody` / `HyperOutgoingBody` — `UnsyncBoxBody`, `!Send`
- `tokio::sync::mpsc::Sender<T>` requires `T: Send`

The only viable approach is a `ChannelBody` bridge (spawn a forwarding task that reads `!Send` frames and sends them through an `mpsc` to a `Send`-compatible `Body` impl). This works mechanically, but the **WASM response path** (Point 2, `engine.rs:407`) has a critical complication: the `Store` and its pooling allocator slot must stay alive until the response body is fully drained by the downstream client. Slow clients hold instance slots open, potentially exhausting `total_component_instances`.

### Why the benefit is small

- Small JSON/protobuf payloads dominate module-to-module RPCs. Buffering costs ~1-5 us per point. Streaming adds per-frame channel overhead that makes small payloads *slower*.
- Network hops (~100-200 us on loopback) are 20-100x the buffering cost.
- The proxy already streams — the engine is only a bottleneck for large payloads that don't exist in the current workload.

### If large payloads become a requirement

Add a body-size limit on the engine (413 Payload Too Large) and route large payloads through the blobstore host binding, which already supports streaming via S3 multipart upload. ~1 day of work, no architectural changes.

## Where to Focus Next

1. **Registry key interning** — Replace `HashMap<(String, String, String), _>` with a pre-hashed or interned key to avoid 3 String allocations per lookup in both `registry.rs:62` and `routing.rs:256-260`.

2. **Atomic round-robin** — Replace `Mutex<HashMap<_, usize>>` in the routing layer with per-entry `AtomicUsize` (similar to what `registry.rs` already does with `AtomicUsize` in `InstanceList`). — **Implemented (item 7).**

3. **Circuit breaker per-entry lock** — Move from a single `Mutex<HashMap>` to a concurrent map (e.g., `DashMap`) or pre-populate breakers on routing table sync so the hot path is lock-free.
