# Protobuf Schemas

> **Building a new guest module?** See [`docs/agents/codegen.md`](agents/codegen.md) for the exact mapping from proto definitions to generated Rust code.

Every module **must** declare a protobuf schema. Schemas serve two purposes:

1. **Code generation** — `wr-build` generates service traits and client stubs from the proto definitions, giving modules type-safe RPC interfaces.
2. **Discovery** — engines upload schemas to the manager on registration. Tools like `wr-cli` can fetch schemas to inspect available RPCs and message types.

The proxy does **not** validate request bodies against schemas at runtime — it is a streaming header-based router that forwards bodies without buffering or inspection. Modules are responsible for handling malformed input gracefully.

Schemas are compiled `FileDescriptorSet` binaries produced by `protoc`.

## Writing a schema

```protobuf
// inventory_service.proto
syntax = "proto3";
package inventory;

message GetItemsRequest {
  string category = 1;
}

message GetItemsResponse {
  repeated string items = 1;
}

service InventoryService {
  rpc GetItems (GetItemsRequest) returns (GetItemsResponse);
}
```

## Compiling to a FileDescriptorSet

```bash
protoc \
  --descriptor_set_out=schemas/inventory_service.binpb \
  --include_imports \
  inventory_service.proto
```

The resulting `.binpb` file is the value of `schema_path` in `engine.toml`.

## How routing works

When the proxy receives a request with `x-wr-destination: http://ecommerce.inventory/inventory.InventoryService/GetItems` it:

1. Parses the host (`ecommerce.inventory`) as `namespace.module`.
2. Looks up healthy routing rules for that module in the cached routing table.
3. Selects a candidate via round-robin and forwards the request — body is streamed through without buffering.

Generated routers match the canonical path `/{proto_package}.{ProtoServiceName}/{ProtoMethodName}`; public ingress routes must use that path unless a manual wrapper handles a different public route.

The proxy only inspects headers — it never reads, decodes, or validates the request or response body.
