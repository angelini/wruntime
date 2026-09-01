# Protobuf Schemas

> **Building a new guest module?** See the guest [codegen guide](agents/guest-module-author/codegen.md) for generator selection, output concepts, and authoritative source links.

Every module **must** declare a protobuf schema. Schemas serve two purposes:

1. **Code generation** — `wr-build` generates service traits and client stubs from the proto definitions, giving modules type-safe RPC interfaces.
2. **Discovery** — engines upload schemas to the manager on registration. Tools like `wr-cli` can fetch schemas to inspect available RPCs and message types.

The proxy trusts wruntime-generated module, worker, scheduler, and peer traffic and forwards those request bodies without buffering or schema validation. Public traffic entering through the optional external listener is different: each configured route names an `rpc_path`, and the proxy decodes supported protobuf, canonical protobuf JSON, or flat form input against that RPC's protobuf input type before forwarding normalized wire bytes. Modules should still handle malformed internal input gracefully.

Schemas are compiled `FileDescriptorSet` binaries produced by `protoc`.

## Writing a schema

```protobuf
// schemas/inventory_service.proto
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
  schemas/inventory_service.proto
```

The resulting `.binpb` file is the value of `schema_path` in `engine.toml`.

## How routing works

When the proxy receives a trusted internal request with `x-wr-destination: http://ecommerce.inventory/inventory.InventoryService/GetItems` it:

1. Parses the host (`ecommerce.inventory`) as `namespace.module`.
2. Looks up healthy routing rules for that module in the cached routing table.
3. Selects a candidate via round-robin and streams the request body without inspection.

Generated routers match the canonical path `/{proto_package}.{ProtoServiceName}/{ProtoMethodName}`. An external route may expose a REST-style alias in `path`; its required `rpc_path` supplies the canonical generated path. Public ingress strips reserved headers, rewrites the alias to `rpc_path`, resolves the selected module version, and buffers the request under `external.max_request_body_bytes`. It selects protobuf wire, canonical protobuf JSON, or flat URL-encoded form input from `Content-Type`, lazily caches the selected version's descriptor set, decodes the request as that RPC's input message, and forwards normalized protobuf wire bytes. Malformed or schema-incompatible input is rejected with `400`, oversized input with `413`, unsupported media types with `415`, and an unavailable schema with `503`. Responses are still streamed and are not schema-validated or transcoded.

Internal loopback and mTLS peer stacks never include this validation layer, so module-to-module and cross-node continuation traffic retains the streaming fast path.

The manager control API uses present/absent `google.protobuf.Timestamp` values for schedule `last_fired_at` and `next_fire_at`; absence is no longer encoded as an empty RFC3339 string. Generated clients should test field presence before formatting these timestamps.

Job and schedule counts/durations use `uint32`. `SubmitJobRequest.timeout_secs` and `max_attempts` reserve zero as an explicit configured-default sentinel; schedule interval/timeout/attempt fields must be non-zero. `GetJobStatusResponse.status` is the closed `JobState` enum (`PENDING`, `RUNNING`, `COMPLETE`, `DEAD`) rather than a free-form string.

These field-type changes intentionally break wire compatibility with older control-plane clients while the API remains pre-release. Upgrade managers, proxies, engines, and CLI clients together; mixed-version rolling upgrades are not supported for this transition.
