# gRPC API (`proto/wruntime.proto`)

All inter-service communication uses the `wruntime.ManagerService` gRPC service.

## Engine lifecycle

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `RegisterEngine` | `EngineRegistration` | `{ accepted }` | Engine announces itself and its modules |
| `DeregisterEngine` | `{ engine_id }` | — | Engine removes itself on shutdown |
| `Heartbeat` | `{ engine_id, healthy_modules }` | — | Sent every 10 s; carries the list of currently healthy modules; manager uses this to update per-module health and mark routing rules unhealthy when a module goes silent |
| `ListEngines` | — | `[EngineRegistration]` | Returns all currently registered engines |

## Routing table

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `GetRoutingTable` | — | `RoutingTable` | Returns the full versioned table |
| `UpsertRoutingRule` | `RoutingRule` | — | Insert or update a rule by `rule_id`; always marks the rule healthy |
| `DeleteRoutingRule` | `{ rule_id }` | — | Remove a rule; increments table version |

A `RoutingRule` has the fields:

```protobuf
message RoutingRule {
  string rule_id               = 1;   // stable identifier for this rule
  string source_module         = 2;   // module that initiates the call
  string destination_module    = 3;   // module name used as the HTTP host
  string engine_id             = 4;   // UUID of the destination engine
  string engine_address        = 5;   // HTTP base URL of the destination engine
  string destination_version   = 6;   // semver of the destination module, e.g. "1.2.0"
  bool   healthy               = 7;   // set by manager; false = proxy will not route to this rule
  string destination_namespace = 8;   // namespace of the destination module
  string source_namespace      = 9;   // namespace of the source module
  string proxy_address         = 10;  // externally-reachable address of the node's proxy
}
```

`proxy_address` is set automatically from the engine's `[node] proxy_address` when the engine registers. The routing layer on each proxy compares this field against its own `[node] proxy_address` to decide whether to forward the request directly to the local `engine_address` (`LocalEngine`) or to relay it to the peer proxy at `proxy_address` (`RemoteProxy`).

The `healthy` field is managed entirely by the manager — it is always set to `true` on `UpsertRoutingRule` and is flipped to `false` automatically when the engine's heartbeat stops reporting the module as healthy, or immediately on `DeregisterEngine`. The routing table version is incremented whenever health status changes, so proxies pick up failover events within one TTL cycle.

## Schemas

| RPC | Description |
|-----|-------------|
| `UploadSchema` | Store a `FileDescriptorSet` for `(module, version)` |
| `GetSchema` | Retrieve the stored schema bytes |

Schemas are automatically uploaded when engines register (if `schema_path` is set in `engine.toml`). You can also push a schema independently:

```bash
grpcurl -plaintext -d "{
  \"module\": \"inventory-service\",
  \"version\": \"1.0.0\",
  \"proto_schema\": \"$(base64 -i schemas/inventory_service.binpb)\"
}" 127.0.0.1:9000 wruntime.ManagerService/UploadSchema
```

## Metrics (OpenTelemetry)

Request metrics are collected via OpenTelemetry traces rather than a custom gRPC pipeline. The `TracingLayer` emits a `proxy.request` span for every request with attributes: `wr.source`, `wr.destination`, `http.response.status_code`, and `otel.status_code`. Span duration captures request latency.

Query metrics via the CLI:

```bash
wr-cli metrics summary                          # default: Tempo at localhost:3200, last 1h
wr-cli metrics summary --tempo http://tempo:3200 --since 6h
```

Or query Tempo directly with [TraceQL](https://grafana.com/docs/tempo/latest/traceql/):

```
{name = "proxy.request" && span.wr.source = "order-service"}
```
