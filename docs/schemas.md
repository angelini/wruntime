# Protobuf Schemas

> **Building a new guest module?** See the guest [codegen guide](agents/guest-module-author/codegen.md) for generator selection, output concepts, and authoritative source links.

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

The manager control API uses present/absent `google.protobuf.Timestamp` values for schedule `last_fired_at` and `next_fire_at`; absence is no longer encoded as an empty RFC3339 string. Generated clients should test field presence before formatting these timestamps.

Job and schedule counts/durations use `uint32`. `SubmitJobRequest.timeout_secs` and `max_attempts` reserve zero as an explicit configured-default sentinel; schedule interval/timeout/attempt fields must be non-zero. `GetJobStatusResponse.status` is the closed `JobState` enum (`PENDING`, `RUNNING`, `COMPLETE`, `DEAD`) rather than a free-form string.

These field-type changes intentionally break wire compatibility with older control-plane clients while the API remains pre-release. Upgrade managers, proxies, engines, and CLI clients together; mixed-version rolling upgrades are not supported for this transition.
