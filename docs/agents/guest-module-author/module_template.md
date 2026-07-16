# Composable Module Template

Start with this base, then apply one bounded variant: service handler, runner/client, worker, or combined generator. Replace `{{PLACEHOLDER}}` values. Compare dependency pins with current example manifests before copying.

## Base layout

```text
{{MODULE_NAME}}/
├── Cargo.toml
├── build.rs
├── schemas/{{MODULE_NAME}}.proto
├── src/lib.rs
├── wit/
│   ├── world.wit
│   └── deps -> {{RELATIVE_PATH_TO_WR_SDK}}/wit/deps
└── migrations/                 # only with database = true
    └── V1__create_tables.sql
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

Create the WIT dependency link:

```bash
ln -s {{RELATIVE_PATH_TO_WR_SDK}}/wit/deps wit/deps
```

## Schema and descriptor

```protobuf
syntax = "proto3";
package {{PROTO_PACKAGE}};

service {{ServiceName}} {
  rpc {{MethodName}}({{MethodName}}Request) returns ({{MethodName}}Response);
}

message {{MethodName}}Request {}
message {{MethodName}}Response {}
```

```bash
protoc \
  --descriptor_set_out=schemas/{{MODULE_NAME}}.binpb \
  --include_imports \
  schemas/{{MODULE_NAME}}.proto
```

## Local WIT world

Every guest needs a local generation block and world, even though `wr_sdk::bindings` supplies convenience types.

```wit
package {{NAMESPACE}}:{{MODULE_NAME}}@1.0.0;

world {{MODULE_NAME}} {
  import wruntime:tracing/span@0.2.0;
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

  // Add only with matching module opt-ins:
  // import wruntime:db/database@0.4.0;
  // import wruntime:blobstore/store@0.1.0;
  // import wruntime:llm/inference@0.1.0;

  export wasi:http/incoming-handler@0.2.6;
}
```

Base `src/lib.rs`:

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
```

`ServiceGuest::init` runs once before the first request:

```rust
impl wr_sdk::ServiceGuest for Component {
    fn init() {
        // Optional one-time SDK setup.
    }

    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::{{service_name_snake}}_handle(&Component, request, response_out);
    }
}
```

## Variant: service handler

`build.rs`:

```rust
fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["schemas/{{MODULE_NAME}}.proto"], &["schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=schemas/{{MODULE_NAME}}.proto");
}
```

Implement the generated trait:

```rust
impl proto::{{ServiceName}} for Component {
    fn {{method_name}}(
        &self,
        _req: proto::{{MethodName}}Request,
    ) -> Result<proto::{{MethodName}}Response, ServiceError> {
        Ok(proto::{{MethodName}}Response {})
    }
}
```

## Variant: runner/client

Generate a handler for the runner's trigger service and clients for dependencies:

```rust
.service_generator(Box::new(wr_build::WrCombinedGenerator::new(
    wr_build::WrServiceGenerator,
    wr_build::WrClientGenerator,
)))
.compile_protos(&["schemas/trigger.proto", "schemas/dependency.proto"], &["schemas"])
```

The runner implements its generated trigger service and uses the base `_handle` dispatch. Inside the trigger method:

```rust
let client = proto::DependencyServiceClient::new("{{NAMESPACE}}.{{DEPENDENCY}}");
let result = client.call(proto::CallRequest {})?;
```

## Variant: worker

Name the proto service `*WorkerService`, generate its handler with `WrServiceGenerator`, and implement it like a normal service. Job delivery is at least once: use an idempotency key or transactional deduplication around side effects.

```toml
[[module]]
name                    = "{{MODULE_NAME}}"
namespace               = "{{NAMESPACE}}"
version                 = "1.0.0"
mode                    = "worker"
database                = true # required for the durable queue
worker_concurrency      = 1
worker_job_timeout_secs = 300
wasm_path               = "path/to/{{MODULE_NAME}}.wasm"
schema_path             = "path/to/{{MODULE_NAME}}.binpb"
```

Worker mode requires engine database configuration because the durable queue is Postgres-backed. A submitter generates `WrWorkerClientGenerator` and constructs the client with an exact version or `""` for name-only ad-hoc dispatch.

## Variant: combined generator

For a service that also calls ordinary services and workers:

```rust
.service_generator(Box::new(wr_build::WrCombinedGenerator::new(
    wr_build::WrServiceGenerator,
    wr_build::WrCombinedGenerator::new(
        wr_build::WrClientGenerator,
        wr_build::WrWorkerClientGenerator,
    ),
)))
```

Use the generated `_handle` for a pure protobuf entry point or `_router` when composing it with manual HTTP routes.

## Module configuration deltas

Base entry:

```toml
[[module]]
name        = "{{MODULE_NAME}}"
namespace   = "{{NAMESPACE}}"
version     = "1.0.0"
wasm_path   = "path/to/target/wasm32-wasip2/debug/{{MODULE_NAME}}.wasm"
schema_path = "path/to/schemas/{{MODULE_NAME}}.binpb"
```

Add only what is used:

```toml
database       = true
migrations_path = "path/to/migrations"
blobstore       = true
llm             = true
fs              = "tempdir"

[module.env]
LOG_LEVEL = "info"
API_TOKEN = { secret = true }
```

The referenced secret must exist under the module namespace. The guest reads only the resolved environment value. Blobstore and LLM also require engine-level `[blobstore]` and `[llm]` sections; do not copy those tables blindly—use [configuration](../../configuration.md).

## Migrations

Use refinery names such as `V1__create_tables.sql`. Migrations run on the host under the module schema/namespace role before readiness and are serialized across replicas. Do not issue schema DDL in request or worker handlers.

## Build and validation

```bash
cargo fmt --check
cargo clippy --target wasm32-wasip2 -- -D warnings
cargo build --target wasm32-wasip2
```

From the repository root, also run the focused executable proof:

```bash
just build-ecommerce       # or build-stockmarket / build-codegen
just test-wasm-one db      # when changing a capability fixture
```

For a production example change, run its inline recipe. `just validate-ecommerce` is the warning-enforcing ecommerce check. See [examples](./examples.md) and [constraints](./constraints.md).
