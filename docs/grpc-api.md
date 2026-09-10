# gRPC API (`proto/wruntime.proto`)

The manager's single mTLS endpoint mounts the `wruntime.ClusterService`, `InfrastructureService`, `NodeService`, `JobService`, `PolicyService`, and `LifecycleService` control-plane domains. Engines use the local proxy's `wruntime.NodeService` for lifecycle calls, and worker job submission/status use HTTP RPC through the proxy rather than gRPC.

## Process lifecycle

`wruntime.LifecycleService` is a read-only observation contract mounted on each service's trusted control listener. Process stage is monotonic:

`STARTING → READY → STOPPING`

`READY` means the owning service crossed its startup barriers and admits its intended work. It does not imply dependency, module, route, or cluster health; those remain `StatusSeverity` evidence from `GetClusterStatus`. `UNSPECIFIED` is invalid wire input, and there is no remotely observable `STOPPED` state after the endpoint disappears.

| RPC | Request | Response | Semantics |
| --- | --- | --- | --- |
| `GetStatus` | empty | `LifecycleStatus` | Side-effect-free snapshot containing process state, stable service kind and process instance ID, transition timestamp, typed reason, and explanatory detail. |

Transitions never move backward. Service-specific route withdrawal, admission closure, draining, deregistration, and task joining are internal `STOPPING` phases. The process owner requests graceful shutdown with SIGTERM or SIGINT; lifecycle RPC clients cannot mutate process state. `LifecycleTransitionReason` is the machine-readable transition key; `detail` is bounded explanatory text and must not be parsed by automation.

`wr-cli lifecycle status|wait --endpoint <url>` exposes this observation contract; `--tls` selects the CLI's manager mTLS credentials. Waits require the expected service kind and activation identity and return distinct errors for transport/query failure, replacement, terminal-before-ready state, and timeout. These commands never interpret cluster health severity. Local process exit is proven by the foreground owner's retained `Child`; deployed process exit is proven by the node agent's typed systemd or Docker backend inspection, not by a lifecycle mutation RPC.

## Engine lifecycle

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `RegisterEngine` | `EngineRegistration` | `{ accepted }` | Engine announces itself and its modules; the manager resolves requested secrets and DB credentials, then persists the engine, its schemas, and one initially-unhealthy default routing rule per schema-bearing module in a single transaction; module readiness rows are reset for advertised tuples |
| `DeregisterEngine` | `{ engine_id }` | — | Engine removes itself on shutdown |
| `Heartbeat` | `{ engine_id, healthy_modules }` | manager/proxy routing versions | Atomically records engine/module readiness and makes only matching routes serving. A draining or deregistered engine is rejected. The manager response identifies the durable routing version; `NodeService` adds the locally installed proxy version and does not acknowledge initial readiness before it converges. Invalid module identities are skipped without starving valid entries. |
| `BeginEngineDrain` | `{ engine_id }` | manager/proxy routing versions | Idempotently fences later heartbeat publication, makes the engine's routes non-serving without deleting registration, and returns manager/local-proxy convergence evidence. Final deregistration remains separate. |
| `ListEngines` | — | `[EngineRegistration]` | Returns all currently registered engines |

`EngineRegistration.deployment`, when present, carries the stable node ID, manager-assigned revision, immutable `sha256:` bundle and revision digests, the manager-derived operation UUID reserved for that revision, and the stable engine slot. `engine_id` remains a process identity and must not be used to infer deployment history. Database-enabled engines also register `job_queue_id` and `job_admin_address` as an all-or-nothing pair. The queue ID identifies one physical `wr__jobs` database; engines sharing that database must use the same ID, and engines using different databases must not.

## Deployment lifecycle

| RPC | Request | Response | Description |
| ----- | --------- | ---------- | ------------- |
| `BeginDeployment` | node ID, idempotency attempt token, digest, expected slot/module inventory | `DeploymentRecord` | Allocates one inactive monotonic per-node revision and its deterministic manager-derived operation UUID, or returns the exact existing attempt for the same token. |
| `FinalizeDeployment` | exact node/token/revision, bundle and resolved-release digests | `DeploymentRecord` | Binds completely staged, digest-covered bytes before operation submission; it does not activate them. |
| `AbandonDeployment` | exact node and attempt token | `DeploymentRecord` | Fails an unsubmitted inactive allocation only when no registration, operation, or authority evidence exists. |
| `VerifyDeployment` | node ID, revision | readiness and condition codes | Side-effect-free verification of exact registrations, fresh engine/module heartbeats, authority, and healthy routes. |
| `BeginRollback` | node ID, historical successful revision (or zero for previous), token | `DeploymentRecord` | Copies retained immutable content into a new staged revision and reports `source_revision`. |

Beginning and finalizing a deployment set `wr_nodes.target_revision`; neither replaces committed `current_revision`. `DeploymentRecord.operation_id` reserves the manager-derived UUID embedded in the digest-covered prelaunch engine configuration; submission creates the durable operation with that same identity. The durable operation owns activation and commit. Source and target may coexist, but a registration serves only through exact per-slot authority (or the committed fallback where no explicit slot authority exists). The manager derives additions, retained replacements, unchanged slots, and removals from the finalized complete target inventory, orders those groups deterministically with lexical ordering inside each group, and commits only after every required backend, lifecycle, registration, route, and authority gate passes. An exact current-revision submission by a new actor/token succeeds immediately without host instructions or deployment/authority mutation. Release cleanup is independent per-node maintenance discovered by the periodic manager reconciler. Stable verification codes include `NON_AUTHORITATIVE_REVISION`, `MISSING_ENGINE`, `REVISION_MISMATCH`, `DIGEST_MISMATCH`, `DUPLICATE_ENGINE_SLOT`, `MISSING_MODULE`, `STALE_ENGINE_HEARTBEAT`, `MISSING_MODULE_HEARTBEAT`, `STALE_MODULE_HEARTBEAT`, `MISSING_ROUTE`, and `UNHEALTHY_ROUTE`. `NodeStatus.desired_deployment` is the committed serving snapshot and `target_deployment` is the optional staged snapshot. Condition code/severity and evidence fields are machine-readable; `detail` remains explanatory.

### Unified operation targets

Operation claims and `ReportStepResult` carry the same tagged target identity: a proxy identity has no slot key, while an engine identity contains a required nonempty engine slot. `NodeOperation.targets` is deterministic (proxy first, then engines by rollout order) and combines common progress/evidence with a required `proxy_details` or `engine_details` oneof. Missing or mismatched kind, identity, details, or step combinations fail closed. `ReportNodeObservation` remains engine-slot-only; proxy proof is returned through the tagged `VerifyProxy` result path. Before returning a mutating claim, the manager commits its delivery timestamp and ambiguity flag. `ReportStepResult` atomically advances manager state and stores an indefinitely retained receipt over the authenticated principal and exact serialized tagged request. Only a byte-identical retry under the same node, activation, epoch, operation, step, and target is acknowledged idempotently. Agent restart does not replay a receipt candidate; the new activation follows manager-directed `InspectBackend` and reports fresh typed evidence.

This is a breaking contract cutover. Removed slot/proxy fields are reserved and have no empty-slot compatibility alias.

## Manager service authorization

The manager serves six domains—`ClusterService`, `InfrastructureService`, `NodeService`, `JobService`, `PolicyService`, and read-only `LifecycleService`—on one mTLS listener. Stock TLS validates the client root and profile; a shared parser then requires one same-cluster URI SAN and records the leaf SHA-256 fingerprint. Immutable policy is default-deny for every served RPC and applies role, capability, resource scope, and revocation checks. A complete generated-descriptor test prevents an unclassified RPC from being mounted.

`InfrastructureService` owns durable node operations and manager-set rollouts; `NodeService` is restricted to enrolled proxy/node-agent workloads. A valid chain or routing/source header never grants a role.

| Service/RPC | Roles | Semantics |
| --- | --- | --- |
| `InfrastructureService.GetStatus/GetOperation/ListOperations/VerifyDeployment` | scoped read role | Side-effect-free composed status, exact deployment verification, and append-only operation history. Lifecycle observation, backend running/exited evidence, and availability remain separate fields. |
| `InfrastructureService.BeginDeployment/BeginRollback/SubmitOperation` | infrastructure operator | Binds allocation, staging, and one immutable operation payload to the same authenticated `(principal, request_token)` identity. Actions are complete desired-inventory deployment, explicit monotonic rollback through the same reconciler, and single-slot restart. |
| `InfrastructureService.ResumeOperation/CancelOperation` | infrastructure operator | Resume paused work with a fresh lease. Cancelling uncommitted queued/paused work requests source restoration before terminal cancellation; committed work requires rollback. |
| `InfrastructureService.PutNodeAgentPolicy/FinalizeDeployment/AbandonDeployment` | infrastructure operator | Publish the canonical expected agent policy, bind staged bytes only for their allocating principal, or safely abandon that principal's unused allocation. |
| `NodeService.Attest` | node-bound `node-agent` | Persists claimed node/activation, process-computed binary digest, exact protocol, backend kind, normalized advertised capabilities, authenticated principal, and manager receipt time. Compatibility requires exact binary/protocol/backend plus `required capabilities ⊆ advertised`; sorted missing-capability diagnostics are stable. Local config and cleanup retention are not evidence. |
| `NodeService.ClaimNodeCleanup` | node-bound `node-agent` | Claims exact generation-bound release deletion authority after revalidating its protection fingerprint. |
| `NodeService.RenewNodeCleanupLease` | node-bound `node-agent` | Renews only the current activation/generation/lease/claim tuple; a protection fence returns terminal superseded state. |
| `NodeService.ReportNodeCleanupResult` | node-bound `node-agent` | Idempotently accounts exact deletion evidence and complete resulting inventory against immutable issued authority. |
| `InfrastructureService.GetNodeCleanupStatus` | scoped operator/read role | Returns the bounded per-node cleanup generation, state, inventory summary, next action, and diagnostic. |
| `InfrastructureService.RetryNodeCleanup` | scoped operator/write role | Compare-and-set fences a paused observed generation and queues periodic reconciliation; it never performs deletion. |
| `NodeService.ClaimOperation` | same node-bound agent | Returns one manager-derived typed target/effect and monotonically increasing lease epoch. It never returns shell text or a PID. |
| `NodeService.RenewOperationLease/ReportStepResult` | same node-bound agent | Fences by node, activation, operation, step, epoch, principal, and unexpired lease. Result fields are evidence; no success boolean advances state. |
| `NodeService.ReportObservation` | same node-bound agent | Stores exact lifecycle observation separately from backend process evidence; endpoint absence never synthesizes lifecycle `STOPPED`, and inspection failure is explicit query-error evidence. |
| `NodeService.RegisterProxy/ReportProxyInventory/DeregisterProxy` | enrolled node-bound `proxy` | Registers one process tuple, atomically replaces its bounded complete listener/admission/routing/breaker report using manager receipt time, and cleanly tombstones the exact process. The authenticated proxy principal and policy binding supply proxy/node identity; report payload never grants backend, process, release, or routing authority. |

One forward/restoration operation is allowed per node. Operations, absolute deadlines, phases, and append-only events survive client, agent, and manager restarts. Typed operation steps cover proxy and engine release verification, backend stop/start/inspection, atomic selection, lifecycle/route verification, authority switching, and source restoration. The retired operation cleanup phase, step, and fields are reserved and are not reused; cleanup uses its dedicated generation protocol. The manager derives advancement from coherent evidence, so a lost acknowledgement or manager takeover reconciles actual state without repeating a backend effect.

Deployment reconciliation progresses sequentially: additions first, then retained replacements, then removals, with lexical order inside each group. Target registration remains non-serving until exact serving evidence permits the manager-owned authority switch; removal requires pinned exit, deregistration, non-serving routes, and zero authority. Before commit, cancellation or deadline expiry fences forward effects and completes full source restoration without the expired forward deadline. Rollback is separately authorized and never automatic. Defaults are `max_unavailable=1`, 300 seconds for restart, and 1800 seconds for deployment/rollback. Restart preserves the committed inventory and revision while replacing one process. Stopping the final serving slot or deploying an empty desired inventory requires explicit `allow_downtime`.

## JobService and EngineJobAdminService

`JobService` shares the manager mTLS listener and requires an explicit policy capability plus queue scope for every RPC. The manager never connects to `wr__jobs`; it selects a fresh, non-draining registered engine for the explicit queue and delegates over `EngineJobAdminService` with its distinct manager `clientAuth` workload certificate. The engine admits only enrolled, non-revoked same-cluster manager principals from its complete snapshot. Human, proxy, node-agent, wrong-cluster, unmapped, revoked, and server-only leaves are denied. Routing headers are never authorization.

| RPC | Result | Semantics |
| --- | --- | --- |
| `ListJobQueues` | queue ID, availability, fresh/total delegates | Registration-only discovery; does not query queue databases. Unpaged discovery is bounded to 10,000 queue IDs and returns `resource_exhausted` above that ceiling. |
| `ListJobs` | metadata rows and optional `next_cursor` | One newest-first keyset page. Default 50, maximum 200. Payload, result, error text, claim ID, and claimant are omitted. |
| `GetJobQueueSummary` | observed time, four state counts, total, pending depth, oldest pending | One database statement under the same non-status filters. |
| `GetJob` | full authorized detail | Includes payload/result, `last_error`, timeout, claimant/lease, and lifecycle timestamps, but never the internal `claim_id` fence. |
| `RetryJob` | updated detail | Locks one job, accepts only `dead`, resets attempt to zero and terminal/claim artifacts, preserves `last_error`, and notifies its worker before commit. |

Every operation requires `job_queue_id`. Filters support hierarchical worker namespace/name/version and source namespace/module, plus job type and inclusive `created_at_from`/exclusive `created_at_before`; unscoped child filters and empty/inverted time ranges are `invalid_argument`. List may additionally filter one closed `JobState`. Cursors are opaque, versioned, bound to normalized filters, and provide non-snapshot traversal: concurrent inserts or status changes can change later pages, especially for mutable status filters.

Stable status mapping is `invalid_argument` for malformed scope/filter/cursor, `not_found` for an unknown queue or job, `failed_precondition` for retrying a non-dead job, `unavailable` for no fresh queue delegate/readiness, and `internal` for redacted database failures. Safe reads may select the next fresh delegate only when connection establishment fails before dispatch. `RetryJob` is never replayed; a timeout or connection loss after dispatch returns `unavailable` with “outcome unknown; inspect before retrying”.

`EngineJobAdminService.CheckJobQueue` is the manager-workload identity/readiness probe. Engine methods reject a queue mismatch and remain unavailable until embedded job migrations and the initial workload-policy snapshot complete. Snapshot expiry fails closed. The listener is bound before registration and is owned and joined with the engine process. Shutdown closes job admission, rejects new RPCs, and drains admitted RPCs under the shared absolute shutdown deadline before route withdrawal and deregistration.

Job payloads and successful results are each bounded to 1 MiB at persistence; combined identity/source/type metadata is bounded to 64 KiB and stored error text to 1 MiB. Submission body collection, worker response collection, engine delegation decoding/encoding, manager forwarding, and CLI decoding use one compatible contract. The unary admin ceiling is 4 MiB so an inspection containing both maximum payload and result remains transportable through both gRPC hops. Oversize submission or persistence is rejected rather than producing an inspectable row that cannot cross the control plane.

## Cluster status snapshot

| RPC | Request | Response | Description |
| --- | --- | --- | --- |
| `GetClusterStatus` | empty | `GetClusterStatusResponse` | Returns one manager-composed snapshot of manager membership, desired node revisions and history, operation-aware top-level and node-embedded proxy inventory, engine/module heartbeat evidence, persisted routes/services, aggregate severity, and stable conditions. |

The manager reads deployment history, active operation progress, proxy registrations/reports, engine/module heartbeats, routes, manager lease records, and the routing-table version in one read-only `REPEATABLE READ` PostgreSQL transaction. The response reports `database_observed_at` from PostgreSQL and `response_at`; manager and proxy freshness are evaluated against that database observation rather than a process clock. Routing synchronization age adds the proxy's monotonic age at report creation to elapsed database time since manager receipt. Records and conditions are sorted deterministically. The response never stores or returns resolved secret values, concrete breaker targets, TLS material, database URLs, or deployment configuration payloads.

`StatusSeverity` has the ordered known states `healthy < degraded < unhealthy`, plus `unknown`. Unknown evidence is not interpreted as healthy, but unsupported signals do not silently worsen supported deployment/routing status: aggregate reduction ignores `unknown` when at least one known signal exists. An all-unknown view remains `unknown`.

Aggregation rules are:

- a node is healthy only when the current desired `DeploymentRecord` passes the same verifier used by `VerifyDeployment`; a previous healthy revision and unmanaged registration never satisfy current readiness;
- an engine is authoritative only when its node/revision/digest/slot matches the current desired inventory; fresh non-authoritative engines are `degraded` with `UNMANAGED_ENGINE`;
- a service is healthy when all desired routes are healthy, degraded when at least one but not all desired routes are healthy, and unhealthy when no desired route is healthy;
- managers with a database heartbeat inside the configured lease threshold are live and healthy; retained stale rows are dead and unhealthy with `STALE_MANAGER_HEARTBEAT`;
- expected proxy identity follows the committed deployment without an operation, the source before target selection/start, the target afterward and through engine rollout/commit, and the source during restoration; irreducibly ambiguous paused evidence selects nothing;
- one fresh exact report is selected per desired node. Missing, stale, duplicate, mismatched, STOPPING, unknown-manager, behind/stale routing, half-open, or partially-open evidence degrades. Only fresh selected READY evidence positively showing closed admission, a required configured listener not accepting, or all targets open for a nonempty breaker class is unhealthy;
- clean tombstones are absent. A stale predecessor remains `SUPERSEDED_PROXY_INSTANCE` diagnostic evidence but is excluded after one unique fresh replacement is selected. Disabled external ingress, empty routing, zero breakers, and exact timeout boundaries are neutral;
- host CPU/memory remains `SIGNAL_NOT_REPORTED`. Manually inserted routes have persisted health but may use `MANUAL_ROUTE_REASON_UNAVAILABLE` because heartbeat causality is not stored.

Additional stable cluster codes include `STALE_MANAGER_HEARTBEAT`, `UNMANAGED_ENGINE`, `NO_HEALTHY_ROUTE`, `PARTIAL_ROUTE_AVAILABILITY`, `MANUAL_ROUTE_REASON_UNAVAILABLE`, `SIGNAL_NOT_REPORTED`, `MISSING_EXPECTED_PROXY`, `MISSING_PROXY_REPORT`, `STALE_PROXY_REPORT`, `MULTIPLE_FRESH_PROXY_INSTANCES`, `PROXY_RELEASE_MISMATCH`, `AMBIGUOUS_PROXY_OPERATION`, `PROXY_STOPPING`, `UNKNOWN_PROXY_RECEIVER_MANAGER`, `PROXY_ROUTING_NOT_SYNCHRONIZED`, `PROXY_ROUTING_BEHIND`, `STALE_PROXY_ROUTING`, `UNKNOWN_PROXY_ROUTING_MANAGER`, `PROXY_ADMISSION_CLOSED`, `PROXY_LISTENER_NOT_ACCEPTING`, `ALL_PROXY_TARGETS_OPEN`, `PARTIALLY_OPEN_PROXY_BREAKERS`, `HALF_OPEN_PROXY_BREAKERS`, `SUPERSEDED_PROXY_INSTANCE`, and `ORPHAN_PROXY_INVENTORY`. Consumers must branch on code/severity and use raw timestamps, ages, desired/actual revisions, and affected identities as evidence rather than parsing `detail`.

`wr-cli cluster status` performs exactly this RPC; it does not join `ListManagers`, `ListEngines`, and `GetRoutingTable` client-side. `--output json` retains `schema_version: 2` and additively includes canonical top-level and node-embedded proxy DTOs with selected/expected deployment identity, lifecycle/admission, report and routing ages, listener states, routing version/source, aggregate breaker counts, and nested conditions. Table output adds proxy counts and problems; `--detail` expands the bounded evidence. `--node` filters both proxy inventory locations and strict unknown traversal includes proxy/listener/routing/breaker conditions. `InfrastructureService.GetStatus` additionally applies requested-node and per-node infrastructure-read authorization to canonical top-level proxies before returning the response. The default command is display-only; `--fail-on` remains a display gate.

`wr-cli cluster wait --severity healthy|degraded|unhealthy|unknown` is the expectation surface for automation. It returns zero only when the exact severity is observed for a present filtered node/service (or the cluster when unfiltered) and emits an `outcome: observed` object containing the matching snapshot. Empty targets, malformed filters or wire severity enums, transport/query failure, and timeout are non-zero and cannot satisfy an expected unhealthy check.

Typical condition evidence:

| Scenario | Status evidence |
| --- | --- |
| Healthy rollout | desired revision matches the sole fresh slot; module heartbeat and default route are healthy; node/service are `healthy` |
| Revision mismatch | node is `unhealthy` with `REVISION_MISMATCH`, including desired revision plus stale actual registration metadata |
| Stale heartbeat | node/engine are `unhealthy` with `STALE_ENGINE_HEARTBEAT` and raw heartbeat time/age |
| Partial service availability | service is `degraded` with `PARTIAL_ROUTE_AVAILABILITY` and healthy/desired route counts |
| Stale manager lease | manager is `dead` and `unhealthy` with `STALE_MANAGER_HEARTBEAT`, raw heartbeat time, age, and database observation |

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
| `ListManagers` | — | `[ManagerInfo]` | Returns manager rows whose PostgreSQL heartbeat lease is fresh under the configured threshold, evaluated with server-side `NOW()`. Use for peer discovery from any manager—no client database access required. |

A `ManagerInfo` has the fields:

```protobuf
message ManagerInfo {
  reserved 3;
  reserved "gossip_address";
  string manager_id   = 1; // UUID assigned at startup
  string grpc_address = 2; // externally reachable mTLS gRPC endpoint
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
