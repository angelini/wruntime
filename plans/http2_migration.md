# HTTP/2 Migration Plan

Convert all internal HTTP traffic from HTTP/1.1 to HTTP/2 cleartext (h2c). Drop the legacy HTTP/1 connection-per-request model; HTTP/2 multiplexes many requests over a single TCP connection, eliminating the need for explicit HTTP/1 connection pooling.

## Scope

Four change sites:

| # | File | What changes |
|---|------|-------------|
| 1 | `wr-proxy/Cargo.toml` | Add `http2` to hyper-util features; drop `http1` from hyper |
| 2 | `wr-engine/Cargo.toml` | Same; add `http2` to hyper-util, drop `http1` |
| 3 | `wr-proxy/src/main.rs` | Switch `accept_loop` from `http1::Builder` to `http2::Builder` |
| 4 | `wr-engine/src/server.rs` | Switch `serve` from `http1::Builder` to `http2::Builder` |
| 5 | `wr-proxy/src/layers/forward.rs` | Switch outbound client to HTTP/2-only |
| 6 | `wr-engine/src/engine.rs` + `src/state.rs` | Switch module→proxy client to HTTP/2-only |

---

## Step 1 — Cargo.toml: wr-proxy

**File:** `wr-proxy/Cargo.toml`

```toml
# Before
hyper     = { version = "1", features = ["http1", "http2", "server"] }
hyper-util = { version = "0.1", features = ["client-legacy", "tokio"] }

# After
hyper     = { version = "1", features = ["http2", "server"] }
hyper-util = { version = "0.1", features = ["client-legacy", "http2", "tokio"] }
```

Removing `http1` from hyper prevents the compiler from including HTTP/1 codecs.
Adding `http2` to hyper-util enables `Client::builder(...).http2_only(true)`.

---

## Step 2 — Cargo.toml: wr-engine

**File:** `wr-engine/Cargo.toml`

```toml
# Before
hyper     = { version = "1", features = ["http1", "http2", "server"] }
hyper-util = { version = "0.1", features = ["tokio", "client", "client-legacy", "http1"] }

# After
hyper     = { version = "1", features = ["http2", "server"] }
hyper-util = { version = "0.1", features = ["tokio", "client", "client-legacy", "http2"] }
```

---

## Step 3 — wr-proxy server: switch to HTTP/2

**File:** `wr-proxy/src/main.rs`

Replace the import and the `serve_connection` call in `accept_loop`.

```rust
// Before
use hyper::server::conn::http1;
// ...
if let Err(e) = http1::Builder::new().serve_connection(io, hyper_svc).await {
    warn!(peer = %peer_addr, error = %e, "connection error");
}

// After
use hyper::server::conn::http2;
use hyper_util::rt::TokioExecutor;
// ...
if let Err(e) = http2::Builder::new(TokioExecutor::new())
    .serve_connection(io, hyper_svc)
    .await
{
    warn!(peer = %peer_addr, error = %e, "connection error");
}
```

`http2::Builder` requires an executor argument; `TokioExecutor` is already imported via `hyper_util::rt::TokioIo` in this file — add it to that import line.

---

## Step 4 — wr-engine server: switch to HTTP/2

**File:** `wr-engine/src/server.rs`

```rust
// Before
use hyper::server::conn::http1;
// ...
if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
    warn!(error = %e, "inbound connection error");
}

// After
use hyper::server::conn::http2;
use hyper_util::rt::TokioExecutor;
// ...
if let Err(e) = http2::Builder::new(TokioExecutor::new())
    .serve_connection(io, svc)
    .await
{
    warn!(error = %e, "inbound connection error");
}
```

---

## Step 5 — wr-proxy outbound client: HTTP/2-only

**File:** `wr-proxy/src/layers/forward.rs`

The `hyper-util` legacy client supports HTTP/2 cleartext via `.http2_only(true)`. With HTTP/2 multiplexing, a single connection handles concurrent requests to the same engine; the legacy client manages this connection internally.

```rust
// Before
let client = Client::builder(TokioExecutor::new()).build_http();

// After
let client = Client::builder(TokioExecutor::new())
    .http2_only(true)
    .build_http();
```

No type signature changes — the client is still `Client<HttpConnector, Full<Bytes>>`.

---

## Step 6 — wr-engine module→proxy client: HTTP/2-only

**File:** `wr-engine/src/engine.rs` (line ~141) and `src/state.rs` (type annotation only)

```rust
// Before (engine.rs)
let http_client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
    .build_http::<http_body_util::Full<bytes::Bytes>>();

// After (engine.rs)
let http_client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
    .http2_only(true)
    .build_http::<http_body_util::Full<bytes::Bytes>>();
```

`state.rs` holds this client as `Client<HttpConnector, Full<bytes::Bytes>>` — the type is unchanged, no edits needed there.

---

## What this changes at runtime

| Concern | HTTP/1 (current) | HTTP/2 (after) |
|---------|-----------------|----------------|
| Connections per engine | One TCP connection per in-flight request | One persistent TCP connection per engine, multiplexed |
| Connection setup overhead | Paid on every request when pool is empty | Paid once; subsequent requests reuse the connection |
| Head-of-line blocking | Per-connection (absent with pooling) | Eliminated at the transport layer |
| Body framing | Chunked transfer-encoding or Content-Length | DATA frames; length is always known |
| Header compression | None | HPACK (reduces repeated `x-wr-*` header overhead) |

The proxy already buffers request bodies into `Bytes` before forwarding and response bodies before returning, so the streaming differences between HTTP versions do not affect the existing data flow.

---

## Testing

After applying changes:

```bash
just check          # must compile clean with no http1 imports remaining
just test           # unit + integration suite
just test-integration
```

The integration tests in `wr-tests/` start all three services in-process; they will exercise the HTTP/2 paths end-to-end with no external dependencies.

Verify no residual `http1` references remain in non-test code:

```bash
grep -r "http1" wr-proxy/src wr-engine/src
```

Expected: zero matches.
