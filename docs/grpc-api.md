# gRPC API (`proto/wruntime.proto`)

The mTLS `wruntime.ManagerService` is the cluster control plane. Engines use the local proxy's `wruntime.NodeService` for lifecycle calls, and worker job submission/status use HTTP RPC through the proxy rather than gRPC.

## Process lifecycle

`wruntime.LifecycleService` is a read-only observation contract mounted on each service's trusted control listener. Process stage is monotonic:

`STARTING → READY → STOPPING`

`READY` means the owning service crossed its startup barriers and admits its intended work. It does not imply dependency, module, route, or cluster health; those remain `StatusSeverity` evidence from `GetClusterStatus`. `UNSPECIFIED` is invalid wire input, and there is no remotely observable `STOPPED` state after the endpoint disappears.

| RPC | Request | Response | Semantics |
| --- | --- | --- | --- |
| `GetStatus` | empty | `LifecycleStatus` | Side-effect-free snapshot containing process state, stable service kind and process instance ID, transition timestamp, typed reason, and explanatory detail. |

Transitions never move backward. Service-specific route withdrawal, admission closure, draining, deregistration, and task joining are internal `STOPPING` phases. The process owner requests graceful shutdown with SIGTERM or SIGINT; lifecycle RPC clients cannot mutate process state. `LifecycleTransitionReason` is the machine-readable transition key; `detail` is bounded explanatory text and must not be parsed by automation.

`wr-cli lifecycle status|wait --endpoint <url>` exposes this observation contract; `--tls` selects the CLI's manager mTLS credentials. Waits require the expected service kind and activation identity and return distinct errors for transport/query failure, replacement, terminal-before-ready state, and timeout. These commands never interpret cluster health severity. Local process exit is proven by the foreground owner's `Child` handle; deployed process exit is proven by systemd or Docker through `wr-cli node stop`, not by adding a lifecycle mutation RPC.

## Engine lifecycle

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `RegisterEngine` | `EngineRegistration` | `{ accepted }` | Engine announces itself and its modules; the manager resolves requested secrets and DB credentials, then persists the engine, its schemas, and one initially-unhealthy default routing rule per schema-bearing module in a single transaction; module readiness rows are reset for advertised tuples |
| `DeregisterEngine` | `{ engine_id }` | — | Engine removes itself on shutdown |
| `Heartbeat` | `{ engine_id, healthy_modules }` | manager/proxy routing versions | Atomically records engine/module readiness and makes only matching routes serving. A draining or deregistered engine is rejected. The manager response identifies the durable routing version; `NodeService` adds the locally installed proxy version and does not acknowledge initial readiness before it converges. Invalid module identities are skipped without starving valid entries. |
| `BeginEngineDrain` | `{ engine_id }` | manager/proxy routing versions | Idempotently fences later heartbeat publication, makes the engine's routes non-serving without deleting registration, and returns manager/local-proxy convergence evidence. Final deregistration remains separate. |
| `ListEngines` | — | `[EngineRegistration]` | Returns all currently registered engines |

`EngineRegistration.deployment`, when present, carries the stable node ID, manager-assigned revision, immutable `sha256:` bundle digest, and stable engine slot. `engine_id` remains a process identity and must not be used to infer deployment history.

## Deployment lifecycle

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `BeginDeployment` | node ID, idempotency attempt token, digest, expected slot/module inventory | `DeploymentRecord` | Allocates the next monotonic per-node revision, or returns the existing attempt for the same token. |
| `VerifyDeployment` | node ID, revision | readiness and condition codes | Verifies only registrations matching the requested revision and digest, with fresh engine/module heartbeats and healthy routes. |
| `CompleteDeployment` | node ID, revision, outcome | `DeploymentRecord` | Records terminal failure, or records success only after the manager re-verifies exact readiness. |
| `BeginRollback` | node ID, historical successful revision (or zero for previous), token | `DeploymentRecord` | Copies the selected immutable desired snapshot into a new monotonic revision and reports `source_revision`. |

Verification uses one repeatable-read manager snapshot and only the current desired revision. Stable codes include `SUPERSEDED_REVISION`, `MISSING_ENGINE`, `REVISION_MISMATCH`, `DIGEST_MISMATCH`, `DUPLICATE_ENGINE_SLOT`, `MISSING_MODULE`, `STALE_ENGINE_HEARTBEAT`, `MISSING_MODULE_HEARTBEAT`, `STALE_MODULE_HEARTBEAT`, `MISSING_ROUTE`, and `UNHEALTHY_ROUTE`. `DeploymentCondition.code`, `severity`, and evidence fields are the machine-readable contract; `detail` is explanatory text and may improve without a wire-contract change. CLI deployment success is defined by an empty condition set, not by `ListEngines` visibility.

## Cluster status snapshot

| RPC | Request | Response | Description |
| --- | --- | --- | --- |
| `GetClusterStatus` | empty | `GetClusterStatusResponse` | Returns one manager-composed snapshot of manager membership, desired node revisions and history, engine/module heartbeat evidence, persisted routes/services, aggregate severity, and stable conditions. |

The manager reads deployment history, registrations, engine/module heartbeats, routes, manager records, and the routing-table version in one read-only `REPEATABLE READ` PostgreSQL transaction. Gossip cannot join that transaction, so the response reports separate `database_observed_at`, `gossip_observed_at`, and `response_at` timestamps. Records and conditions are sorted deterministically. The response never stores or returns resolved secret values or deployment configuration payloads.

`StatusSeverity` has the ordered known states `healthy < degraded < unhealthy`, plus `unknown`. Unknown evidence is not interpreted as healthy, but unsupported signals do not silently worsen supported deployment/routing status: aggregate reduction ignores `unknown` when at least one known signal exists. An all-unknown view remains `unknown`.

Aggregation rules are:

- a node is healthy only when the current desired `DeploymentRecord` passes the same verifier used by `VerifyDeployment`; a previous healthy revision and unmanaged registration never satisfy current readiness;
- an engine is authoritative only when its node/revision/digest/slot matches the current desired inventory; fresh non-authoritative engines are `degraded` with `UNMANAGED_ENGINE`;
- a service is healthy when all desired routes are healthy, degraded when at least one but not all desired routes are healthy, and unhealthy when no desired route is healthy;
- gossip-dead managers are unhealthy; DB/gossip disagreement is explicit, while DB-only membership during the startup convergence window is degraded with `BOOTSTRAP_CONVERGING`;
- proxy routing-sync age, circuit-breaker state, and host CPU/memory are currently `SIGNAL_NOT_REPORTED`. Manually inserted routes have persisted health but may use `MANUAL_ROUTE_REASON_UNAVAILABLE` because heartbeat causality is not stored.

Additional stable cluster codes include `GOSSIP_DEAD`, `MANAGER_DB_GOSSIP_DISAGREEMENT`, `BOOTSTRAP_CONVERGING`, `UNMANAGED_ENGINE`, `NO_HEALTHY_ROUTE`, `PARTIAL_ROUTE_AVAILABILITY`, `MANUAL_ROUTE_REASON_UNAVAILABLE`, and `SIGNAL_NOT_REPORTED`. Consumers must branch on code/severity and use raw timestamps, ages, desired/actual revisions, and affected identities as evidence rather than parsing `detail`.

`wr-cli cluster status` performs exactly this RPC; it does not join `ListManagers`, `ListEngines`, and `GetRoutingTable` client-side. `--output json` emits the versioned CLI DTO (`schema_version: 1`) with complete typed records; field meanings and condition codes are stable automation surfaces. Table output defaults to the summary and problem rows, while `--detail` includes healthy and unknown records. `--node` and `--service namespace.module[@version]` filter presentation. The default command is display-only; `--fail-on` remains a display gate.

`wr-cli cluster wait --severity healthy|degraded|unhealthy|unknown` is the expectation surface for automation. It returns zero only when the exact severity is observed for a present filtered node/service (or the cluster when unfiltered) and emits an `outcome: observed` object containing the matching snapshot. Empty targets, malformed filters or wire severity enums, transport/query failure, and timeout are non-zero and cannot satisfy an expected unhealthy check.

Typical condition evidence:

| Scenario | Status evidence |
| --- | --- |
| Healthy rollout | desired revision matches the sole fresh slot; module heartbeat and default route are healthy; node/service are `healthy` |
| Revision mismatch | node is `unhealthy` with `REVISION_MISMATCH`, including desired revision plus stale actual registration metadata |
| Stale heartbeat | node/engine are `unhealthy` with `STALE_ENGINE_HEARTBEAT` and raw heartbeat time/age |
| Partial service availability | service is `degraded` with `PARTIAL_ROUTE_AVAILABILITY` and healthy/desired route counts |
| Manager disagreement | `BOOTSTRAP_CONVERGING`, `GOSSIP_DEAD`, or `MANAGER_DB_GOSSIP_DISAGREEMENT` records both DB and gossip observations |

## Routing table

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `GetRoutingTable` | — | `RoutingTable` | Returns the full versioned table |
| `UpsertRoutingRule` | `RoutingRule` | — | Insert or update a rule by `rule_id`; always marks the rule healthy |
| `DeleteRoutingRule` | `{ rule_id }` | — | Remove a rule; increments table version |

A `RoutingRule` has the fields:

```protobuf
message RoutingRule {
  string rule_id               = 1;   // stable identifier for this rule
  string source_module         = 2;   // metadata reserved for future source policy
  string destination_module    = 3;   // module name used as the HTTP host
  string engine_id             = 4;   // UUID of the destination engine
  string engine_address        = 5;   // HTTP base URL of the destination engine
  string destination_version   = 6;   // semver of the destination module, e.g. "1.2.0"
  bool   healthy               = 7;   // set by manager; false = proxy will not route to this rule
  string source_namespace      = 8;   // metadata reserved for future source policy
  string destination_namespace = 9;   // namespace of the destination module
  reserved 10;
  reserved "proxy_address";
  string peer_address          = 11;  // mTLS address of the destination node's proxy
}
```

`RoutingRule.peer_address` is the sole cross-node forwarding address. Each proxy compares it with its explicit `[node].peer_address` to decide whether to forward directly to the local `engine_address` (`LocalEngine`) or relay over mTLS to `peer_address` (`RemoteProxy`). `EngineRegistration.proxy_address` remains separate plain-HTTP metadata: it is the local proxy URL used by an engine for outbound rewriting. The reserved routing-rule field 10 must not be reused.

`source_module` and `source_namespace` are persisted metadata but are not currently routing or authorization constraints. Current matching uses only destination namespace, module, and optional version.

The `healthy` field is managed entirely by the manager — it is always set to `true` on `UpsertRoutingRule`; default routing rules created at registration start `healthy = false`. Matching module heartbeats plus the background monitor's health recomputation make default routes healthy, and routes flip to `false` automatically when the engine or module heartbeat goes stale, or immediately on `DeregisterEngine`. The routing table version is incremented whenever health status changes, so proxies pick up failover events within one TTL cycle.

## Manager discovery

| RPC | Request | Response | Description |
| --- | --- | --- | --- |
| `ListManagers` | — | `[ManagerInfo]` | Reconciles DB-fresh registrations with chitchat liveness. Gossip-live managers are included, gossip-dead managers are excluded immediately, and DB-fresh managers not yet observed by gossip are included only during the startup convergence window. Use for peer discovery from any seed manager—no client database access required. |

A `ManagerInfo` has the fields:

```protobuf
message ManagerInfo {
  string manager_id     = 1; // UUID assigned at startup
  string grpc_address   = 2; // externally reachable mTLS gRPC endpoint
  string gossip_address = 3; // chitchat UDP address
}
```

## NodeService (control plane)

The `wruntime.NodeService` gRPC service is exposed by `wr-proxy` on its `control_address` (default port 9002). Engines on the same node use this for registration and heartbeats instead of connecting directly to the manager.

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `RegisterEngine` | `RegisterEngineRequest` | `RegisterEngineResponse` | Engine announces itself and its modules to the local proxy |
| `DeregisterEngine` | `DeregisterEngineRequest` | `DeregisterEngineResponse` | Engine removes itself on shutdown |
| `Heartbeat` | `HeartbeatRequest` | `HeartbeatResponse` | The first post-load heartbeat is forwarded synchronously and acknowledged only after the returned manager version is installed locally; later heartbeats are cached and aggregated every 3 s. |
| `BeginEngineDrain` | `BeginEngineDrainRequest` | `BeginEngineDrainResponse` | Fences heartbeat publication, withdraws engine route admission through the manager, and returns manager/local-proxy convergence versions without deregistration. |
| `GetProxyRoutingStatus` | empty | activation ID and installed routing-table version | Read-only local observation used by a foreground process owner to prove that this exact READY proxy activation installed a captured manager version. |

NodeService serializes heartbeat/readiness/drain/deregister forwarding behind a per-engine fence and generation; unrelated engines do not hold one another's manager RPC or convergence path. A periodic flush snapshots the generation and discards it if the per-engine generation changed before the forward fence. Drain removes an engine from periodic publication before the manager route update. Deregistration tombstones it before the manager RPC, so an older flush or later heartbeat cannot recreate serving routes. Routing status reads the same lifecycle activation handle served by `LifecycleService.GetStatus` and the version from the existing installed routing snapshot; it performs no convergence or lifecycle mutation.

This decouples engines from the manager address — engines only need to know their local proxy's loopback control address.

## Worker job queue (HTTP RPC)

Worker jobs use HTTP RPC via the proxy (not a gRPC service). The SDK provides ergonomic wrappers in `wr_sdk::jobs`.

The fully qualified endpoints are canonical. `/SubmitJob` and `/GetJobStatus` remain supported compatibility aliases; SDKs and new callers should use the canonical paths.

| Endpoint | Request | Response | Description |
| --- | --- | --- | --- |
| `POST /wruntime.WorkerService/SubmitJob` | `SubmitJobRequest` | `SubmitJobResponse` | Submit a job to a worker module's queue |
| `POST /wruntime.WorkerService/GetJobStatus` | `GetJobStatusRequest` | `GetJobStatusResponse` | Query the status of a previously submitted job |

The current worker messages are:

```protobuf
enum JobState {
  JOB_STATE_UNSPECIFIED = 0;
  JOB_STATE_PENDING     = 1;
  JOB_STATE_RUNNING     = 2;
  JOB_STATE_COMPLETE    = 3;
  JOB_STATE_DEAD        = 4;
}

message SubmitJobRequest {
  string worker_namespace = 1;
  string worker_name      = 2;
  string worker_version   = 3; // empty means name-only dispatch
  string job_type         = 4;
  bytes  payload          = 5;
  uint32 timeout_secs     = 6; // 0 uses the configured default
  uint32 max_attempts     = 7; // 0 uses the configured default
}

message SubmitJobResponse { string job_id = 1; }
message GetJobStatusRequest { string job_id = 1; }

message GetJobStatusResponse {
  string job_id        = 1;
  JobState status      = 2;
  bytes  result        = 3;
  string error_message = 4;
  uint32 attempt       = 5;
  uint32 max_attempts  = 6;
}
```

- `worker_namespace` and `worker_name` must match the proxy-routed `x-wr-namespace` and `x-wr-module` identity; missing or mismatched routed identity headers are rejected.
- `worker_version` is optional. An empty value creates a name-only job claimable by any matching namespace/name worker version; a non-empty value is claimable only by that exact version.
- For non-empty `worker_version`, an `x-wr-version` header must match or the engine returns HTTP 400. The SDK omits that header for empty versions and sends it for pinned versions.
- `max_attempts` precedence is explicit request value > configured `worker_max_attempts` for the exact body version (or proxy-routed version when the body version is empty) > hard default 3.
- Manager schedules remain version-pinned and continue to require a non-empty worker version.

## Schedules

Schedules are version-pinned control-plane resources. Their interval, timeout, and attempt fields are non-zero `uint32` values; `last_fired_at` and `next_fire_at` are optional `google.protobuf.Timestamp` fields.

| RPC | Request | Response | Description |
| --- | --- | --- | --- |
| `UpsertSchedule` | `UpsertScheduleRequest` | `{ schedule_id }` | Create or update the schedule identified by worker namespace/name/version and canonical job type |
| `DeleteSchedule` | `DeleteScheduleRequest` | — | Delete the matching version-pinned schedule |
| `ListSchedules` | `{ worker_namespace }` | `[Schedule]` | List schedules, optionally filtered by worker namespace |

Schedule delivery is at least once. Handlers must be idempotent, and `job_type` uses the canonical `/{package}.{Service}/{Method}` path.

## Schemas

| RPC | Description |
| --- | --- |
| `GetSchema` | Retrieve the stored schema bytes |

Schemas are automatically uploaded when engines register; the first occurrence of each unique `(namespace, name, version)` tuple in `engine.toml` supplies `schema_path`.

## Secrets

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `SetSecret` | `{ namespace, key, value }` | — | Encrypt and store a secret (AES-GCM). |
| `DeleteSecret` | `{ namespace, key }` | — | Remove a secret. |
| `ListSecrets` | `{ namespace }` | `[SecretEntry]` | List secret keys (not values) for a namespace. Empty namespace returns all. |

Secrets are encrypted at rest and delivered to engines on registration via the `secrets` field in `RegisterEngineRequest`.

## Metrics (OpenTelemetry)

Request metrics are collected via OpenTelemetry traces rather than a custom gRPC pipeline. The `TracingLayer` emits a `proxy.request` span for every request with attributes: `wr.source`, `wr.destination`, `http.response.status_code`, and `otel.status_code`. Span duration captures request latency.

Query metrics via the CLI:

```bash
wr-cli metrics summary                          # default: Tempo at localhost:3200, last 1h
wr-cli metrics summary --tempo http://tempo:3200 --since 6h
```

> **Note:** Manager-facing CLI commands require `--manager https://…` (or `WR_MANAGER`) plus the CA/client certificate options described in [configuration](configuration.md#cli-access). Metrics commands query Tempo directly and do not require a manager.

Or query Tempo directly with [TraceQL](https://grafana.com/docs/tempo/latest/traceql/):

```traceql
{name = "proxy.request" && span.wr.source = "order-service"}
```
