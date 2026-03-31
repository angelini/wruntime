# Plan: Streaming Proxy — Remove Schema Validation, Stream Bodies

## Context

The proxy currently buffers every request and response body in full. The investigation in `plans/investigations/wasm_guest_request_path.md` identified 5 full-body copies on the guest-to-guest path — two of which happen inside the proxy (inbound request buffering in `main.rs:153` and response buffering in `forward.rs:147`). A third buffer occurs in the egress layer (`egress.rs:175`).

Schema validation is the reason the proxy needs the full body: `DynamicMessage::decode()` requires the complete protobuf message. Every other layer (tracing, routing, egress) only inspects headers. Removing schema validation from the proxy makes end-to-end body streaming possible with no loss of routing capability.

## Goal

Make the proxy a streaming header-based router. Bodies flow through as `hyper::body::Incoming` streams — never collected into `Bytes` — for both requests and responses. Remove the `SchemaValidationLayer` and all supporting schema sync infrastructure from the proxy.

## What Changes

### 1. Remove schema validation from the proxy

**Delete:**
- `wr-proxy/src/layers/schema.rs` — the `SchemaValidationLayer` and `SchemaValidationService`
- `wr-proxy/src/schema.rs` — `SchemaCache`, `sync_schemas()`, `ValidationOutcome`, and all protobuf descriptor handling
- The `schema_trigger` / `sync_schemas` background task in `main.rs`
- The `SchemaValidationLayer` from both internal and external Tower stacks in `main.rs`
- `SchemaCache` import and construction in `main.rs`

**Update:**
- `wr-proxy/src/layers/mod.rs` — remove `mod schema`, `pub use schema::SchemaValidationLayer`
- `wr-proxy/src/main.rs` — remove `schema_trigger`, `schema_cache`, `sync_schemas` spawn, and the `SchemaValidationLayer` from `ServiceBuilder` chains
- `IngressLayer` in `ingress.rs` — if it takes a `SchemaCache` parameter, remove it
- `Cargo.toml` for `wr-proxy` — drop `prost`, `prost-reflect`, `prost-types` if they become unused

### 2. Change the Tower stack body type from `Bytes` to `Incoming`

The entire internal service stack is currently typed `Service<Request<Bytes>>`. Change it to `Service<Request<hyper::body::Incoming>>`.

**`layers/mod.rs`:**
- `ResBody` stays as `BoxBody<Bytes, Infallible>` for error responses, but the forwarded response path returns the engine's streaming body directly. Define a new response body type that is either a boxed error body or a streaming `Incoming`:

```rust
use http_body_util::Either;
use hyper::body::Incoming;

/// Response body: either a locally-generated error/empty body (Left),
/// or a streamed response from an upstream engine (Right).
pub type ResBody = Either<BoxBody<Bytes, Infallible>, Incoming>;
```

Hyper's `Either` implements `http_body::Body` when both sides do, so this composes with the existing `http::Response<ResBody>` return type. `Incoming` is `!Infallible` for errors — we may need `Either<BoxBody<Bytes, Infallible>, BoxBody<Bytes, hyper::Error>>` or just `BoxBody<Bytes, Box<dyn Error>>`. Choose the simplest type that compiles; `Either` with mapped errors is preferable to a fully-boxed body.

**Alternative (simpler):** use `BoxBody<Bytes, anyhow::Error>` everywhere. Locally-generated error responses wrap `Full<Bytes>` with `.map_err(|e| ...)`. Streamed responses wrap `Incoming` via `BoxBody::new(incoming.map_err(...))`. This avoids `Either` at the cost of one vtable indirection — acceptable since the body is I/O-bound anyway.

**`main.rs` accept_loop:**
- Stop buffering the inbound body. Pass `Request<Incoming>` directly into the Tower stack:

```rust
// Before:
let (parts, body) = req.into_parts();
let bytes = BodyExt::collect(body).await?.to_bytes();
svc.call(Request::from_parts(parts, bytes))

// After:
svc.call(req)
```

- Change `accept_loop` signature from `S: Service<Request<Bytes>, ...>` to `S: Service<Request<Incoming>, ...>`

### 3. Update each layer to be generic over the body type

Each Tower layer currently constrains `Request<Bytes>`. Since they only inspect headers and extensions, make them generic:

**`TracingLayer`** — already only reads headers. Change `Service<Request<Bytes>>` to `Service<Request<B>>` where `B: Send + 'static`.

**`RoutingLayer` / `RoutingService`:**
- Currently `Service<Request<Bytes>>`. Change to `Service<Request<B>>`.
- The layer only reads `req.headers()` and `req.extensions()`, and calls `req.headers_mut().insert(...)`. None of this touches the body. The body type `B` passes through untouched.

**`EgressLayer` / `EgressService`:**
- Currently `Service<Request<Bytes>>`. Change to `Service<Request<B>>` where `B: Body<Data = Bytes> + Send + 'static`.
- The external egress path (`req.extensions().get::<ExternalEgress>()`) currently does `let (mut parts, body_bytes) = req.into_parts()` and wraps in `Full::new(body_bytes)`. Instead, forward the streaming body directly to the egress client:

```rust
// The egress hyper client needs to accept a generic body.
// Change from Client<HttpsConnector, Full<Bytes>> to Client<HttpsConnector, Incoming>
// or use BoxBody.
let egress_req = Request::from_parts(parts, body);  // body is Incoming
let resp = client.request(egress_req).await?;
// Return the response body as a stream, not buffered:
let (resp_parts, resp_body) = resp.into_parts();
Ok(Response::from_parts(resp_parts, wrap_streaming(resp_body)))
```

The egress client type (`Client<HttpsConnector<HttpConnector>, Full<Bytes>>`) needs to change. Hyper's `Client` is generic over the request body — switch to `Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, ...>>` or `Client<HttpsConnector<HttpConnector>, Incoming>`. `Incoming` won't work directly as a client body since it's a server-side type. Use `BoxBody`:

```rust
type EgressClient = Client<HttpsConnector<HttpConnector>, BoxBody<Bytes, hyper::Error>>;
```

Then wrap `Incoming` into `BoxBody` at the call site.

**`IngressLayer`** — reads headers, injects `x-wr-destination`. Same pattern: make generic over body.

### 4. Update `ForwardService` to stream both request and response

This is the core change that eliminates buffering.

**Request body:**
- Currently receives `Request<Bytes>`, clones `body_bytes` cheaply for retries.
- With streaming, the body is `Incoming` — not cloneable. Two options:

  **Option A — No request-body retry (recommended):** On the first candidate failure, return the error immediately instead of retrying with the same body. The request body stream has already been partially or fully consumed by the first attempt. This is the simplest approach and matches what most reverse proxies do (nginx, envoy do not replay request bodies across upstreams by default).

  **Option B — Buffer on retry only:** Send the body as a stream to the first candidate. If it fails with 429/5xx, the body is gone. Return the error. If we want retry, we'd need to tee the body into a buffer as it streams — adds complexity and partially defeats the purpose. Not recommended for the initial implementation.

  Go with Option A. The retry loop in `ForwardService` becomes: try candidates in order, but only the **first** candidate gets the request body. Subsequent candidates (if the first fails) get an empty body — which is only valid for retries where the upstream returned an error status (the body was already fully sent). For transport errors where the body wasn't fully sent, we cannot retry.

  Simpler formulation: **try exactly one candidate per request.** The round-robin in `RoutingLayer` already distributes load. Circuit breakers already skip known-bad candidates. The retry-across-candidates behavior is a minor resilience feature that conflicts with streaming; drop it for now.

  Update `ForwardService`:

```rust
impl Service<Request<Incoming>> for ForwardService {
    // ...
    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        // Read ResolvedDestination, pick the first non-open-circuit candidate.
        // Build the forward request with the streaming body.
        // Send it, return the streaming response.
    }
}
```

**Response body:**
- Currently `resp_body.collect().await.to_bytes()` buffers the entire engine response.
- Instead, return the response body stream directly:

```rust
let resp = client.request(forward_req).await?;
let (resp_parts, resp_body) = resp.into_parts();
// resp_body is Incoming — wrap it for the ResBody type and return
Ok(Response::from_parts(resp_parts, wrap_streaming(resp_body)))
```

**Client type change:**
- Currently `Client<HttpConnector, Full<Bytes>>`. Change to `Client<HttpConnector, BoxBody<Bytes, hyper::Error>>` (or `Incoming` if hyper allows it as a client body — it does not; use `BoxBody`).

### 5. Update the hyper client in `ForwardService`

The internal forwarding client currently uses `Full<Bytes>` as its body type:

```rust
client: Client<HttpConnector, Full<Bytes>>
```

Change to accept a streaming body. Since `Incoming` can't be used as a client request body directly, box it:

```rust
client: Client<HttpConnector, BoxBody<Bytes, hyper::Error>>
```

At the call site, wrap the incoming body:

```rust
use http_body_util::BodyExt;
let boxed = req_body.boxed(); // Incoming -> BoxBody
let forward_req = Request::from_parts(parts, boxed);
```

### 6. Update tests

- Remove all schema-validation tests from `wr-proxy` unit tests and `wr-tests/tests/integration_test.rs`
- Remove schema validation assertions from integration tests (e.g., tests that send malformed protobuf and expect 400)
- Keep schema-related proto/binpb files if the manager still stores and serves them (it does — schemas are part of the registration API). Only remove proxy-side schema consumption
- Update any test helpers that construct `SchemaCache` or call `sync_schemas`

### 7. Clean up proxy config

- Remove `cache.schema_ttl_secs` from `ProxyConfig` and example TOML files if it becomes unused
- The `SchemaCache` and `Notify` trigger can be fully removed from `main.rs`

## Files Changed

| File | Change |
|------|--------|
| `wr-proxy/src/layers/schema.rs` | **Delete** |
| `wr-proxy/src/schema.rs` | **Delete** |
| `wr-proxy/src/layers/mod.rs` | Remove schema re-export, change `ResBody` type |
| `wr-proxy/src/main.rs` | Remove schema infra, change accept_loop to pass `Incoming`, update Tower stack types |
| `wr-proxy/src/layers/forward.rs` | Stream request+response bodies, simplify retry to single candidate, change client body type |
| `wr-proxy/src/layers/egress.rs` | Make generic over body, stream egress request+response |
| `wr-proxy/src/layers/routing.rs` | Make generic over body type |
| `wr-proxy/src/layers/tracing.rs` | Make generic over body type |
| `wr-proxy/src/layers/ingress.rs` | Remove `SchemaCache` param if present, make generic over body |
| `wr-proxy/src/config.rs` | Remove `schema_ttl_secs` if unused |
| `wr-proxy/Cargo.toml` | Drop unused protobuf deps |
| `wr-tests/tests/integration_test.rs` | Remove schema validation test cases, update helpers |
| `examples/config/proxy.toml` | Remove schema_ttl_secs |

## Sequence: Streaming Request Path (After)

```
WASM Guest A
  │ HyperOutgoingBody (buffered at engine — out of scope)
  ▼
Source Engine ──HTTP/2──► Proxy
                           │
                           │ Request<Incoming> flows through:
                           │   TracingLayer    — reads headers only
                           │   RoutingLayer    — reads headers, sets extensions
                           │   EgressLayer     — reads extensions, passes through
                           │   ForwardService  — forwards Incoming body as-is
                           │
                           ▼
                    Dest Engine ◄── HTTP/2 (body streams)
                           │
                           │ Response<Incoming> flows back:
                           │   ForwardService returns streaming response
                           │   Layers pass response through unchanged
                           │
                           ▼
                    Source Engine (buffered at engine — out of scope)
```

Zero body copies in the proxy. Headers are the only thing inspected.

## Risks

- **No request retry across candidates.** Today `ForwardService` tries up to 3 engines. With streaming, the body is consumed on the first attempt. Circuit breakers still skip known-bad engines before attempting, so the first candidate is likely healthy. If retry is needed later, it can be re-added with a tee/buffer strategy.
- **No schema validation on the hot path.** Malformed protobuf reaches the WASM guest. Guests should handle bad input gracefully (they already must for external ingress traffic which bypasses schema validation today). Schema validation can move to a dev/CI tool or an optional debug mode.
- **`Incoming` is `!Clone`.** Any middleware that needs to inspect or copy the body (logging, mirroring) won't work without re-introducing buffering. Currently no such middleware exists.

## Non-Goals

- Changing engine-side buffering (`state.rs:88`, `server.rs:112`, `engine.rs:399`). Those are WASM boundary constraints and not worth the complexity now.
- Adding streaming schema validation (tee + validate). If needed, it's a separate effort.
- Changing the manager's schema storage or registration API.
