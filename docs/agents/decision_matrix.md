# Decision Matrix

Choose the right pattern based on what your module needs to do.

## Module type

| Need | Type | Export Macro | build.rs Generator | Trait to Implement |
|------|------|-------------|--------------------|--------------------|
| Handle HTTP requests from other modules | Handler | `wr_sdk::export!` | `WrServiceGenerator` | `ServiceGuest` |
| Handle HTTP requests AND call other modules | Handler+Client | `wr_sdk::export!` | `WrCombinedGenerator` | `ServiceGuest` |

## Capabilities

| Need | engine.toml setting | world.wit import | Cargo.toml dependency |
|------|--------------------|-----------------|-----------------------|
| PostgreSQL | `database = true` + `migrations_path` | `import wruntime:db/database@0.4.0;` | `"wruntime:db" = { path = ".../db.wit" }` |
| S3/Blobstore | `blobstore = true` + `[blobstore]` with `allowed_buckets` | `import wruntime:blobstore/store@0.1.0;` | `"wruntime:blobstore" = { path = ".../blobstore.wit" }` |
| OpenTelemetry tracing | *(always available)* | `import wruntime:tracing/span@0.2.0;` | `"wruntime:tracing" = { path = ".../tracing.wit" }` |
| Ephemeral filesystem | `fs = "tempdir"` | *(standard WASI)* | *(already included)* |
| Outbound HTTP to other modules | *(always available)* | `import wasi:http/outgoing-handler@0.2.6;` | *(already included)* |

## When to use WrCombinedGenerator

Use `WrCombinedGenerator` when a single module needs to:

- Expose its own RPC service (handler)
- AND call RPCs on other modules (client)

Example: an `orders` module that handles order creation requests AND calls `inventory` to check stock.

```rust
// build.rs
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrCombinedGenerator::new(
            wr_build::WrServiceGenerator,
            wr_build::WrClientGenerator,
        )))
        .compile_protos(&[
            "schemas/orders.proto",
            "schemas/inventory.proto",  // for the client stubs
        ], &["schemas"])
        .unwrap();
}
```

This generates:

- `OrdersService` trait + `orders_service_router` (from WrServiceGenerator)
- `InventoryServiceClient` struct (from WrClientGenerator)
