# Protobuf Code Generation

`wr-build` plugs into `prost-build`. Exact generator behavior lives in [`wr-build/src/lib.rs`](../../../wr-build/src/lib.rs); this guide explains selection and generated concepts.

## Generator outputs

| Generator | Selects | Main output |
|---|---|---|
| `WrServiceGenerator` | Every service | Trait, `{service}_router`, and `{service}_handle` |
| `WrClientGenerator` | Services not ending in `WorkerService` | Typed `{Service}Client` methods returning `wr_sdk::http::HttpError` |
| `WrWorkerClientGenerator` | Only services ending in `WorkerService` | Job submit, status, and typed result helpers |
| `WrCombinedGenerator<A, B>` | Delegates to both children | Any composable pair of outputs |

All services require a non-empty proto package. Routes and worker job types use:

```text
/{proto_package}.{ProtoServiceName}/{ProtoMethodName}
```

Proto names become Rust snake case; Rust keywords are raw identifiers such as `r#return`.

## Service handler

```rust
prost_build::Config::new()
    .service_generator(Box::new(wr_build::WrServiceGenerator))
    .compile_protos(&["schemas/orders.proto"], &["schemas"])
    .unwrap();
```

Implement the generated trait. The generated `_handle` reads and decodes the request, calls the `_router`, and sends the response:

```rust
impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::orders_service_handle(&Component, request, response_out);
    }
}
```

Use `_router` directly when combining generated protobuf RPCs with a manual JSON ingress, as in the [codegen coordinator](../../../examples/codegen/coordinator/src/lib.rs).

## Ordinary client

`WrClientGenerator` creates clients constructed with a routing authority such as `ecommerce.inventory`. Methods encode protobuf, call the canonical path, reject non-2xx status, and decode the response. Errors are `wr_sdk::http::HttpError` and convert to `ServiceError` in handlers.

## Worker client

A worker service must end in `WorkerService`:

```protobuf
package codegen;
service WorkerService {
  rpc ProcessTask(ProcessTaskRequest) returns (ProcessTaskResponse);
}
```

`WrWorkerClientGenerator` emits:

- `WorkerServiceClient::new(authority, version)`;
- one submit method and one `_with_options` method per RPC;
- `get_status(job_id)`;
- `get_{method}_result(job_id)`.

The typed result helper returns `Ok(None)` while pending/running, decodes a completed result, reports dead jobs as HTTP 500, and rejects empty or invalid completed payloads as `HttpError::Decode`. An empty client version requests name-only ad-hoc dispatch; a non-empty version pins exact matching.

## Combined generation

Nest combinators for service, ordinary-client, and worker-client output:

```rust
WrCombinedGenerator::new(
    WrServiceGenerator,
    WrCombinedGenerator::new(WrClientGenerator, WrWorkerClientGenerator),
)
```

See [`examples/codegen/coordinator/build.rs`](../../../examples/codegen/coordinator/build.rs).

## Descriptors and validation

Generated Rust belongs to Cargo `OUT_DIR`; never edit it. Regenerate checked-in descriptors whenever a proto changes:

```bash
protoc --descriptor_set_out=schemas/service.binpb --include_imports schemas/service.proto
```

Then run the guest build and relevant example/host test. Use the [API guide](api_guide.md) for preferred SDK use and inspect [`wr-build/src/lib.rs`](../../../wr-build/src/lib.rs) for exact generated signatures.
