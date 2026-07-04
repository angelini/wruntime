# Constraints and Gotchas

Hard rules and common mistakes when building wruntime guest modules. Read this before writing any module code.

## Hard rules

1. **`crate-type = ["cdylib"]`** is mandatory in Cargo.toml. Without it, `cargo build --target wasm32-wasip2` produces nothing useful.

2. **Proto package name determines the generated Rust module name.** `package ecommerce;` in proto generates `ecommerce.rs` in `OUT_DIR`. Include it as:
   ```rust
   mod proto { include!(concat!(env!("OUT_DIR"), "/ecommerce.rs")); }
   ```

3. **Router path format** is `/{package}.{service_snake}/{MethodName}` where:
   - `{package}` = proto package name (e.g. `ecommerce`)
   - `{service_snake}` = service name in snake_case with `_service` suffix stripped (e.g. `InventoryService` becomes `inventory`)
   - `{MethodName}` = proto method name in PascalCase (e.g. `GetStock`)
   - Example: `/ecommerce.inventory/GetStock`

4. **Client RPC path format** is `/{authority}/{MethodName}` where:
   - `{authority}` = the `namespace.module` address (e.g. `ecommerce.inventory`)
   - `{MethodName}` = proto method name in PascalCase
   - Example: `/ecommerce.inventory/GetStock`

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

## Version pinning

Current tested versions (keep in sync across all guest modules):

```toml
prost = "0.14"
prost-build = "0.14"
wr-sdk = { path = "..." }
wr-build = { path = "..." }
wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }
```
