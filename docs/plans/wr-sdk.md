# Plan: `wr-sdk` — WASM Module SDK with Generated gRPC Clients

## Overview

Two new crates that eliminate boilerplate from wruntime WASM modules:

- **`wr-sdk`** — shared runtime helpers (HTTP transport, body I/O, logging) that every module links against
- **`wr-build`** — a `build.rs` library providing a `prost-build` `ServiceGenerator` that emits typed gRPC client structs from `.proto` files

Together they reduce per-method call sites from ~10 lines of raw WASI plumbing to a single typed function call, and remove all duplicated `http_rpc` / `read_body` / `send_response` helpers from module source files.

---

## Target API

**Before** (current `client/src/lib.rs`):

```rust
let buy_bytes = proto::BuyRequest { product_id: ..., quantity }.encode_to_vec();
match http_rpc(INVENTORY, "/ecommerce.InventoryService/Buy", &buy_bytes) {
    Ok((200, body)) => proto::BuyResponse::decode(body.as_slice())?,
    Ok((status, _)) => return Err(...),
    Err(e) => return Err(e),
}
```

**After**:

```rust
let client = InventoryServiceClient::new("inventory.ecommerce");
let resp: BuyResponse = client.buy(BuyRequest { product_id: ..., quantity })?;
```

---

## Crate Layout

```
wr-sdk/
  Cargo.toml
  src/
    lib.rs        # re-exports http, io modules
    http.rs       # http_rpc() — outgoing WASI HTTP transport
    io.rs         # read_body(), send_response()
    log.rs        # log() via wasi:cli/stderr

wr-build/
  Cargo.toml
  src/
    lib.rs        # WrClientGenerator: ServiceGenerator impl
```

Both crates are added to the root workspace `members`.

---

## `wr-sdk` — Runtime Helpers

`wr-sdk` is the **single source of WASI bindings** for all wruntime modules. Modules do not generate their own bindings via `cargo-component` — they consume everything from `wr-sdk`. This means all WASI types (`IncomingBody`, `ResponseOutparam`, etc.) are the same Rust types across `wr-sdk` and every module that depends on it, so all I/O helpers can live here without any type compatibility issues.

### `bindings` module

`wr-sdk` runs `wit_bindgen::generate!` internally and re-exports the result:

```rust
// wr-sdk/src/lib.rs
pub mod bindings {
    wit_bindgen::generate!({ world: "wasi:http/proxy", ... });
}
pub use bindings::exports::wasi::http::incoming_handler::Guest;
```

Modules use `wr_sdk::export!` instead of their own `bindings::export!`:

```rust
// module src/lib.rs
wr_sdk::export!(Component with_types_in wr_sdk::bindings);
```

### `http.rs`

Extracts the existing `http_rpc()` function from `client/src/lib.rs`. Uses `wr-sdk`'s own bindings internally — no types cross the crate boundary. Signature:

```rust
pub fn http_rpc(authority: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>), String>
```

### `io.rs`

Extracts `read_body()` and `send_response()` from `inventory/src/lib.rs`. Types come from `wr-sdk`'s own bindings, which are the same types the module receives at its entry point:

```rust
pub fn read_body(incoming: IncomingBody) -> Vec<u8>
pub fn send_response(response_out: ResponseOutparam, status: u16, body: Vec<u8>)
```

### `log.rs`

Extracts the `log()` helper from `client/src/lib.rs`:

```rust
pub fn log(msg: &str)
```

### Dependencies

```toml
[dependencies]
wit-bindgen = "0.x"   # same version as modules currently use
```

### Note on `cargo-component`

Modules currently rely on `cargo-component` to embed WIT metadata in the `.wasm` output. When bindings move to `wr-sdk`, the `wit_bindgen::generate!` call in `wr-sdk` handles this — but verify that the component model metadata is correctly embedded when bindings are defined in a dependency rather than the root crate. This is supported in principle (it is how `spin-sdk` works) but needs confirmation with the `cargo-component` toolchain version in use.

---

## `wr-build` — Service Code Generator

### `ServiceGenerator` impl

`prost-build` exposes a [`ServiceGenerator`](https://docs.rs/prost-build/latest/prost_build/trait.ServiceGenerator.html) trait that receives structured [`Service`](https://docs.rs/prost-build/latest/prost_build/struct.Service.html) and [`Method`](https://docs.rs/prost-build/latest/prost_build/struct.Method.html) data at build time. `WrClientGenerator` implements this to emit a client struct per service.

For `InventoryService` with package `ecommerce`, the generator emits:

```rust
pub struct InventoryServiceClient {
    authority: String,
}

impl InventoryServiceClient {
    pub fn new(authority: impl Into<String>) -> Self {
        Self { authority: authority.into() }
    }

    pub fn seed(&self, req: SeedRequest) -> Result<SeedResponse, String> {
        let body = prost::Message::encode_to_vec(&req);
        let (status, resp_bytes) = wr_sdk::http::http_rpc(
            &self.authority,
            "/ecommerce.InventoryService/Seed",
            &body,
        )?;
        if status != 200 {
            return Err(format!("rpc error: HTTP {status}"));
        }
        prost::Message::decode(resp_bytes.as_slice()).map_err(|e| e.to_string())
    }

    // ... one method per RPC
}
```

The RPC path is derived from `/{package}.{service_name}/{method_proto_name}` — all fields available on `Service` and `Method`.

### Public API

```rust
// wr-build/src/lib.rs
pub struct WrClientGenerator;

impl prost_build::ServiceGenerator for WrClientGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) { ... }
}
```

### Dependencies

```toml
[dependencies]
prost-build = "0.x"   # same version as workspace
```

---

## Module Integration

### `build.rs` change

```rust
// before
fn main() {
    prost_build::compile_protos(&["../schemas/inventory.proto"], &["../schemas"]).unwrap();
}

// after
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(&["../schemas/inventory.proto"], &["../schemas"])
        .unwrap();
}
```

### `Cargo.toml` additions

```toml
[dependencies]
wr-sdk = { path = "../../wr-sdk" }

[build-dependencies]
wr-build = { path = "../../wr-build" }
```

### Source cleanup

Once `wr-sdk` is linked, each module removes:
- Its `mod bindings;` declaration and `cargo-component`-generated `bindings/` output
- Local copies of `http_rpc()`, `read_body()`, `send_response()`, `err_body()`, `log()`
- All direct `use bindings::wasi::...` imports, replaced with `use wr_sdk::bindings::wasi::...`

---

## Implementation Steps

1. **Create `wr-sdk` crate** — scaffold, move `http_rpc`, `read_body`, `send_response`, `log` from example modules into it, wire up WASI bindings in `build.rs`
2. **Create `wr-build` crate** — scaffold, implement `WrClientGenerator::generate()` with string formatting for struct + methods
3. **Add both to workspace** — add to root `Cargo.toml` `members`
4. **Update `ecommerce-example/client`** — add deps, update `build.rs`, replace `http_rpc` / `log` usages with `wr_sdk::*`, replace manual encode/decode call sites with generated client methods
5. **Update `ecommerce-example/inventory`** — add `wr-sdk` dep, replace `read_body` / `send_response` / `err_body` with SDK equivalents
6. **Verify** — `cargo build --release` for both example workspaces; run the ecommerce example end-to-end

---

## Open Questions

- **`cargo-component` metadata embedding**: confirm that WIT world metadata is correctly embedded in the final `.wasm` when `wit_bindgen::generate!` lives in `wr-sdk` rather than the root crate. Validate early against the `cargo-component` version in use.
- **Error type**: generated methods return `Result<T, String>` for now. A proper `wr_sdk::Error` enum could be introduced later without changing the generator.
- **Server-side codegen**: a `ServiceGenerator` for handler dispatch (routing incoming requests to typed `impl` methods) is a natural follow-on but out of scope here.
