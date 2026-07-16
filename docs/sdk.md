# Module SDK (`wr-sdk` + `wr-build`)

Wruntime guests use two crates:

- **`wr-sdk`** provides handler lifecycle/export support, typed HTTP and host-capability helpers, response/error utilities, tracing, LLM helpers, and worker job APIs.
- **`wr-build`** integrates with `prost-build` to generate service traits/routers/handlers, ordinary clients, and worker clients from protobuf services.

For guest implementation, use the [module template](agents/guest-module-author/module_template.md), [decision matrix](agents/guest-module-author/decision_matrix.md), [API guide](agents/guest-module-author/api_guide.md), and [codegen guide](agents/guest-module-author/codegen.md). Exact signatures live in [`wr-sdk/src/`](../wr-sdk/src/) and [`wr-build/src/lib.rs`](../wr-build/src/lib.rs).

## Minimal service shape

A guest defines a local WIT world, generates its component metadata with `wit_bindgen::generate!`, exports `ServiceGuest`, and implements a generated protobuf service trait:

```rust
#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/inventory.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "inventory",
        generate_all,
    });
}

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::inventory_service_handle(&Component, request, response_out);
    }
}

impl proto::InventoryService for Component {
    fn get_items(
        &self,
        _request: proto::GetItemsRequest,
    ) -> Result<proto::GetItemsResponse, ServiceError> {
        Ok(proto::GetItemsResponse { items: vec![] })
    }
}
```

The template owns current dependency pins, manifest shape, WIT imports, descriptor generation, and build commands. Do not copy dependency versions from this conceptual overview.

## Choosing generated code

| Need | Generator |
|---|---|
| Implement protobuf service | `WrServiceGenerator` |
| Call ordinary protobuf service | `WrClientGenerator` |
| Submit/query `*WorkerService` jobs | `WrWorkerClientGenerator` |
| Produce multiple kinds | nested `WrCombinedGenerator` |

Generated service code includes both `_router` and `_handle`. Generated clients use canonical `/{package}.{Service}/{Method}` paths and `namespace.module` authorities. Prefer generated clients or typed `wr_sdk::http` helpers over the legacy `http_rpc` compatibility function.

## Host capabilities

DB, blobstore, tracing, LLM, filesystem, environment, and outbound HTTP concepts are documented in [host bindings](host-bindings.md). The guest's local world declares imports; module configuration enables matching capabilities; engine startup rejects mismatches before readiness.

Guest host calls are synchronous. Rust/WIT source remains the exact API authority; the [API guide](agents/guest-module-author/api_guide.md) records non-obvious lifecycle, resource-drop, LLM streaming, and worker semantics.

## Building

```bash
cargo fmt --check
cargo clippy --target wasm32-wasip2 -- -D warnings
cargo build --target wasm32-wasip2
```

Use the relevant repository example build and inline validation for executable proof.
