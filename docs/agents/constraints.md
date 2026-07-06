# Constraints and Gotchas

Hard rules and common mistakes when building wruntime guest modules. Read this before writing any module code.

## Hard rules

1. **`crate-type = ["cdylib"]`** is mandatory in Cargo.toml. Without it, `cargo build --target wasm32-wasip2` produces nothing useful.

2. **Proto package name determines the generated Rust module name.** `package ecommerce;` in proto generates `ecommerce.rs` in `OUT_DIR`. Include it as:

   ```rust
   mod proto { include!(concat!(env!("OUT_DIR"), "/ecommerce.rs")); }
   ```

3. **Generated router path format** is `/{proto_package}.{ProtoServiceName}/{ProtoMethodName}`.
   - Example: `/ecommerce.InventoryService/GetStock`

4. **Generated clients use authority `namespace.module` plus canonical path `/{proto_package}.{ProtoServiceName}/{ProtoMethodName}`.**
   - Full URI example: `http://ecommerce.inventory/ecommerce.InventoryService/GetStock`

5. **`schema_path` in engine.toml is required.** The engine refuses to start without it.

6. **All host binding calls are synchronous from the guest's perspective.** The engine handles async internally. Do not attempt `async`/`await` in guest code.

7. **WASI sandbox restrictions** — these are unavailable in guest modules:
   - `std::net` (use `wr_sdk::http::http_request` for outbound calls)
   - `std::fs` (unless `fs = "tempdir"` is set in engine.toml)
   - `std::thread` (single-threaded execution)
   - `std::process` (no subprocess spawning)

8. **Outbound HTTP is transparently intercepted.** Use `wr_sdk::http::http_request` (or generated client methods) for inter-module calls. The engine rewrites the URI to the local proxy automatically.

9. **The `bindings` module must be generated in-source** in every guest module:

   ```rust
   #[allow(dead_code, unused_imports)]
   mod bindings {
       wit_bindgen::generate!({
           path: "wit",
           world: "{{MODULE_NAME}}",
           generate_all,
       });
   }
   ```

   This emits the component-type metadata section from the crate's `wit/world.wit`. Without it, the WASM component will not declare its imports/exports correctly. WIT dependencies are resolved from `wit/deps/` (symlink it to `wr-sdk/wit/deps`).

10. **`wit-bindgen` and `wit-bindgen-rt`** must both be in `[dependencies]`:

    ```toml
    wit-bindgen = "0.51.0"
    wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }
    ```

11. **Health checks are handled by the SDK.** The `wr_sdk::export!` macro intercepts `GET /__health` before calling your `handle()` method. Override `ServiceGuest::health_check()` only if you need custom logic.

12. **Dropping a Transaction without calling commit() triggers automatic rollback.** This is intentional — use it for error handling.

13. **Do not use `CREATE TABLE IF NOT EXISTS` in guest code.** Database schema setup is handled by engine-side migrations. Add SQL migration files to a `migrations/` directory and set `migrations_path` in engine.toml. Migrations run at engine startup before the module receives traffic.

## Common mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Missing `crate-type = ["cdylib"]` | `cargo build --target wasm32-wasip2` produces no `.wasm` | Add `[lib] crate-type = ["cdylib"]` |
| Using `WrServiceGenerator` in a client module | Compile error: trait not implemented | Use `WrClientGenerator` for clients |
| Using `WrClientGenerator` in a handler module | No router function generated | Use `WrServiceGenerator` for handlers |
| Missing `wit/deps` symlink | `wit_bindgen::generate!` fails: `package 'wruntime:db' not found` | Symlink `wit/deps` to `wr-sdk/wit/deps` |
| Wrong proto include path | `include!` fails at build time | The filename is `{proto_package}.rs`, not the proto filename |
| Calling `database::*` without `database = true` in engine.toml | Runtime panic when module tries to query | Set `database = true` in the module's `[[module]]` block |
| Using `CREATE TABLE` in guest code instead of migrations | Tables re-created on every request, noisy NOTICE logs | Move DDL to `migrations/V1__create_tables.sql` and set `migrations_path` in engine.toml |
| Calling blobstore without importing it in world.wit | Link error at instantiation | Add `import wruntime:blobstore/store@0.1.0;` to world.wit and the Cargo.toml dependency |
| Forgetting the `mod bindings` block | Component type section missing, engine rejects the module | Add the `wit_bindgen::generate!` block (see hard rule 9) |
| Proto field number reuse after deletion | Silent data corruption | Never reuse field numbers; use `reserved` for deleted fields |
| Keyword collision in proto method names | Compile error on generated code | `wr-build` escapes keywords with `r#` automatically (e.g. `r#return`) |
| Missing `--include_imports` in protoc | Engine fails to parse schema | Always pass `--include_imports` when generating `.binpb` |
| Using `prost = "0.13"` with `prost-build = "0.14"` (version mismatch) | Build errors | Keep `prost` and `prost-build` on the same minor version |
| Missing `[workspace]` at top of guest Cargo.toml | Build picks up parent workspace settings, may fail or produce wrong output | Add a bare `[workspace]` line before `[package]` in every guest module's Cargo.toml |
| Calling `store::*` without `blobstore = true` in engine.toml | Runtime panic when module tries to access blobstore | Set `blobstore = true` in the `[[module]]` block AND add a `[blobstore]` section with endpoint/credentials to the engine config |
| Calling `complete_stream()` with tools set | `LlmError::InvalidRequest` before any upstream call | Streaming does not support tool-use; use `complete()` for tool calls |
| Uploading/downloading an object larger than `max_object_size`, or listing more than `max_list_objects` | `BlobError::TooLarge` | Stay within the engine `[blobstore]` limits (default 16 MiB / 1000 objects) or split the work |
| Sending an outbound HTTP request body larger than `max_outbound_body_bytes` | Outbound call fails with an `HttpRequestBodySize` error | Keep outbound bodies under the engine limit (default 16 MiB) |

## Runtime limits and enforced behavior

These host-enforced limits and behaviors were introduced by the runtime cleanups. Defaults are configurable on the engine (see [configuration.md](../configuration.md#wr-engine)).

1. **Per-request resource caps.** Concurrently live guest-created host resources are capped per request: tracing spans (default 1024), DB transactions (64), DB row cursors (256), LLM completion streams (32). Exceeding the **span** cap **traps** the guest instance (the request fails); exceeding the DB caps returns `db-error::connection`; exceeding the LLM cap returns `llm-error::api`. Drop resources you no longer need — a slot frees on drop.

2. **Strict DB input conversion.** Parameter values are no longer silently coerced. Malformed JSON (`Jsonb`/`JsonbArray`), a non-numeric `Numeric` string, an out-of-range `Timestamp`/`Timestamptz`/`Time`, or invalid array elements are rejected with `DbError::Query(...)`. (Reads remain lenient: an unmapped result column type comes back as `PgValue::Null` with a host-side warning.)

3. **Blobstore size/count limits.** `put_object`/`get_object` are bounded by `max_object_size` (default 16 MiB) and `list_objects` by `max_list_objects` (default 1000); exceeding either returns `BlobError::TooLarge`. Oversized downloads are aborted mid-stream, never fully buffered.

4. **LLM streaming.** `complete_stream` yields `StreamEvent` values in a fixed order (text deltas → one usage → one stop → `None`) and **pre-rejects tool-enabled requests** with `LlmError::InvalidRequest`. Thinking/signature/citation deltas are dropped.

5. **Outbound HTTP body limit.** Outbound request bodies are bounded by `max_outbound_body_bytes` (default 16 MiB); an oversized body aborts the call with an `HttpRequestBodySize` error.

## Version pinning

Current tested versions (keep in sync across all guest modules):

```toml
prost = "0.14"
prost-build = "0.14"
wr-sdk = { path = "..." }
wr-build = { path = "..." }
wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }
```
