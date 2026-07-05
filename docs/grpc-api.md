# gRPC API (`proto/wruntime.proto`)

All inter-service communication uses the `wruntime.ManagerService` gRPC service.

## Engine lifecycle

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `RegisterEngine` | `EngineRegistration` | `{ accepted }` | Engine announces itself and its modules; the manager resolves requested secrets and DB credentials, then persists the engine, its schemas, and one default routing rule per schema-bearing module in a single transaction |
| `DeregisterEngine` | `{ engine_id }` | — | Engine removes itself on shutdown |
| `Heartbeat` | `{ engine_id, healthy_modules }` | — | Bumps the engine's liveness unconditionally, then upserts a per-module heartbeat for each valid `healthy_modules` entry. Entries missing `namespace`/`name`/`version` are skipped and logged, never fatal. The background monitor marks a routing rule unhealthy when either its engine heartbeat or its specific module's heartbeat goes stale |
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

The `healthy` field is managed entirely by the manager — it is always set to `true` on `UpsertRoutingRule` and is flipped to `false` automatically when the engine's heartbeat stops reporting the module as healthy, or immediately on `DeregisterEngine`. The routing table version is incremented whenever health status changes, so proxies pick up failover events within one TTL cycle. Default routing rules created at registration start healthy = true.

## Manager discovery

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `ListManagers` | — | `[ManagerInfo]` | Returns all active managers (heartbeat within 60 s). Each entry has `manager_id` and `grpc_address`. Use for peer discovery from any seed manager — no database access required. |

A `ManagerInfo` has the fields:

```protobuf
message ManagerInfo {
  string manager_id   = 1;   // UUID assigned at startup
  string grpc_address = 2;   // externally-reachable gRPC endpoint
}
```

## NodeService (control plane)

The `wruntime.NodeService` gRPC service is exposed by `wr-proxy` on its `control_address` (default port 9002). Engines on the same node use this for registration and heartbeats instead of connecting directly to the manager.

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `RegisterEngine` | `RegisterEngineRequest` | `RegisterEngineResponse` | Engine announces itself and its modules to the local proxy |
| `DeregisterEngine` | `DeregisterEngineRequest` | `DeregisterEngineResponse` | Engine removes itself on shutdown |
| `Heartbeat` | `HeartbeatRequest` | `HeartbeatResponse` | Sent every 10 s; the proxy forwards to the manager |

This decouples engines from the manager address — engines only need to know their local proxy's control address.

## Worker job queue (HTTP RPC)

Worker jobs use HTTP RPC via the proxy (not a gRPC service). The SDK provides ergonomic wrappers in `wr_sdk::jobs`.

| Endpoint | Request | Response | Description |
|----------|---------|----------|-------------|
| `POST /wruntime.WorkerService/SubmitJob` | `SubmitJobRequest` | `SubmitJobResponse` | Submit a job to a worker module's queue |
| `POST /wruntime.WorkerService/GetJobStatus` | `GetJobStatusRequest` | `GetJobStatusResponse` | Query the status of a previously submitted job |

A `SubmitJobRequest` has the fields:

```protobuf
message SubmitJobRequest {
  string worker_namespace = 1;
  string worker_name      = 2;
  string worker_version   = 3;
  string job_type         = 4;
  bytes  payload          = 5;
  int32  timeout_secs     = 6;
  int32  max_attempts     = 7;
}
```

A `GetJobStatusResponse` has the fields:

```protobuf
message GetJobStatusResponse {
  string job_id        = 1;
  string status        = 2;   // "pending" | "running" | "complete" | "failed"
  bytes  result        = 3;
  string error_message = 4;
  int32  attempt       = 5;
  int32  max_attempts  = 6;
}
```

## Schemas

| RPC | Description |
|-----|-------------|
| `GetSchema` | Retrieve the stored schema bytes |

Schemas are automatically uploaded when engines register (if `schema_path` is set in `engine.toml`).

## Secrets

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `SetSecret` | `{ namespace, key, value }` | — | Encrypt and store a secret (AES-GCM). |
| `DeleteSecret` | `{ namespace, key }` | — | Remove a secret. |
| `ListSecrets` | `{ namespace }` | `[SecretEntry]` | List secret keys (not values) for a namespace. Empty namespace returns all. |

Secrets are encrypted at rest and delivered to engines on registration via the `secrets` field in `RegisterEngineRequest`.

## Metrics (OpenTelemetry)

Request metrics are collected via OpenTelemetry traces rather than a custom gRPC pipeline. The `TracingLayer` emits a `proxy.request` span for every request with attributes: `wr.source`, `wr.destination`, `http.response.status_code`, and `otel.status_code`. Span duration captures request latency.

Query metrics via the CLI:

```bash
wr-cli --manager http://127.0.0.1:9000 metrics summary                          # default: Tempo at localhost:3200, last 1h
wr-cli --manager http://127.0.0.1:9000 metrics summary --tempo http://tempo:3200 --since 6h
```

> **Note:** `--manager` (or the `WR_MANAGER` env var) is required for all CLI commands. The CLI does not require database access — it communicates exclusively via gRPC.

Or query Tempo directly with [TraceQL](https://grafana.com/docs/tempo/latest/traceql/):

```
{name = "proxy.request" && span.wr.source = "order-service"}
```
