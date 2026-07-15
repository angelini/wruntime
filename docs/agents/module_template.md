# Module Template

Complete fill-in-the-blank skeleton for a new wruntime guest module. Replace all `{{PLACEHOLDER}}` values.

## File structure

```
{{MODULE_NAME}}/
  Cargo.toml
  build.rs
  wit/
    world.wit
    deps/          # symlink to wr-sdk/wit/deps (WIT dependency packages)
  src/
    lib.rs         # bindings generated in-source via wit_bindgen::generate!
  schemas/
    {{MODULE_NAME}}.proto
  migrations/      # optional — only if database = true
    V1__create_tables.sql
```

## schemas/{{MODULE_NAME}}.proto

```protobuf
syntax = "proto3";
package {{PROTO_PACKAGE}};

service {{ServiceName}} {
  rpc {{MethodName}} ({{MethodName}}Request) returns ({{MethodName}}Response);
}

message {{MethodName}}Request {
  // fields
}

message {{MethodName}}Response {
  // fields
}
```

Compile to FileDescriptorSet (required by engine.toml):

```bash
protoc \
  --descriptor_set_out=schemas/{{MODULE_NAME}}.binpb \
  --include_imports \
  schemas/{{MODULE_NAME}}.proto
```

## Cargo.toml

```toml
[workspace]

[package]
name = "{{MODULE_NAME}}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
prost = "0.14"
wr-sdk = { path = "{{RELATIVE_PATH_TO_WR_SDK}}" }
wit-bindgen = "0.51.0"
wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }

[build-dependencies]
prost-build = "0.14"
wr-build = { path = "{{RELATIVE_PATH_TO_WR_BUILD}}" }
```

WIT dependencies are resolved from the crate's `wit/deps/` directory. Symlink it
to the canonical set in `wr-sdk` (so interface changes stay in sync):

```bash
ln -s {{RELATIVE_PATH_TO_WR_SDK}}/wit/deps wit/deps
```

## wit/world.wit

```wit
package {{NAMESPACE}}:{{MODULE_NAME}}@1.0.0;

world {{MODULE_NAME}} {
  // Always include these base imports:
  import wruntime:tracing/span@0.1.0;
  import wasi:cli/stderr@0.2.6;
  import wasi:cli/environment@0.2.6;
  import wasi:cli/exit@0.2.6;
  import wasi:http/types@0.2.6;
  import wasi:http/outgoing-handler@0.2.6;
  import wasi:io/streams@0.2.6;
  import wasi:io/poll@0.2.6;
  import wasi:io/error@0.2.6;
  import wasi:clocks/monotonic-clock@0.2.6;
  import wasi:random/random@0.2.6;

  // Add if module uses database:
  // import wruntime:db/database@0.4.0;

  // Add if module uses blobstore:
  // import wruntime:blobstore/store@0.1.0;

  // Handler modules export incoming-handler:
  export wasi:http/incoming-handler@0.2.6;


}
```

## build.rs

### For handler modules (WrServiceGenerator)

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["schemas/{{MODULE_NAME}}.proto"], &["schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=schemas/{{MODULE_NAME}}.proto");
}
```

### For client modules (WrClientGenerator)

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(&["schemas/{{MODULE_NAME}}.proto"], &["schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=schemas/{{MODULE_NAME}}.proto");
}
```

### For modules that are both handler AND client (WrCombinedGenerator)

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrCombinedGenerator::new(
            wr_build::WrServiceGenerator,
            wr_build::WrClientGenerator,
        )))
        .compile_protos(&["schemas/{{MODULE_NAME}}.proto"], &["schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=schemas/{{MODULE_NAME}}.proto");
}
```

## src/lib.rs — Handler module

```rust
#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/{{PROTO_PACKAGE}}.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "{{MODULE_NAME}}",
        generate_all,
    });
}

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::{{service_name_snake}}_handle(&Component, request, response_out);
    }
}

impl proto::{{ServiceName}} for Component {
    fn {{method_name}}(&self, req: proto::{{MethodName}}Request) -> Result<proto::{{MethodName}}Response, ServiceError> {
        // Implementation here
        Ok(proto::{{MethodName}}Response { /* fields */ })
    }
}
```

## Database migrations

If your module uses `database = true`, create a `migrations/` directory with SQL files following the [refinery](https://github.com/rust-db/refinery) naming convention: `V{version}__{description}.sql` (double underscore).

Migrations run on the **engine (host side)** at startup, before the WASM module loads and before the module becomes routable. Default routes remain unhealthy until readiness heartbeat and manager recompute. You do **not** need `CREATE TABLE IF NOT EXISTS` in your guest code.

```
migrations/
  V1__create_tables.sql     # initial schema
  V2__add_indexes.sql       # subsequent migrations
```

Example `V1__create_tables.sql`:

```sql
CREATE TABLE items (
    item_id    BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    quantity   BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_items_name ON items (name);
```

Key rules:

- Migrations can only modify your module's own schema — `search_path` is restricted to the module's schema at migration time.
- An advisory lock prevents concurrent migration execution across engine replicas.
- Already-applied migrations are skipped automatically (tracked in `refinery_schema_history` table).
- If a migration fails, the engine exits before the module becomes routable; default routes remain unhealthy, so no traffic reaches the module.

## engine.toml entry

```toml
[[module]]
name        = "{{MODULE_NAME}}"
namespace   = "{{NAMESPACE}}"
version     = "1.0.0"
wasm_path   = "path/to/{{MODULE_NAME}}/target/wasm32-wasip2/debug/{{MODULE_NAME}}.wasm"
schema_path = "path/to/schemas/{{MODULE_NAME}}.binpb"
# database = true              # uncomment if module uses database
# migrations_path = "path/to/{{MODULE_NAME}}/migrations"  # uncomment if using migrations
# blobstore = true              # uncomment if module uses blobstore
# request_timeout_secs = 30     # default 30, range 1-3600
# channel_capacity = 128        # inbound queue depth before 429
# fs = "tempdir"                # ephemeral writable /tmp
```

If `blobstore = true`, the engine config must also include:

```toml
[blobstore]
endpoint          = "http://127.0.0.1:8900"
access_key_id     = "rustfsadmin"
secret_access_key = "rustfsadmin"
allowed_buckets   = ["{{BUCKET_NAME}}"]
```

## Build command

```bash
cargo build --target wasm32-wasip2
# Output: target/wasm32-wasip2/debug/{{MODULE_NAME}}.wasm
```
