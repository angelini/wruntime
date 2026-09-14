# gRPC API

The exact wire contract is [`proto/wruntime.proto`](../proto/wruntime.proto).
This page explains service ownership, authorization boundaries, and semantics
that are not obvious from message fields.

## Endpoints and authorization

The manager mounts six domains on one mTLS endpoint:

| Service | Purpose |
| --- | --- |
| `ClusterService` | Engines, routes, manager discovery, composed status, schemas, secrets, and schedules. |
| `InfrastructureService` | Deployment reservations, durable node operations, cleanup, and manager-set rollouts. |
| `NodeService` | Authenticated proxy registration/reporting, engine registration/heartbeats, and node-agent work. |
| `JobService` | Policy-scoped inspection and retry for explicit job queues. |
| `PolicyService` | Authorization-policy and workload-snapshot observation. |
| `LifecycleService` | Read-only process lifecycle observation. |

Every manager RPC is default-deny. A caller needs a valid client-profile
certificate with one same-cluster URI SAN, a mapped role and capability,
matching resource scope, and a non-revoked leaf SHA-256 fingerprint. A valid
TLS chain or `x-wr-*` routing header does not grant a role.

Two additional trusted endpoints exist:

- A proxy exposes `ProxyNodeControlService` on its loopback control listener for
  local engines.
- A database-enabled engine exposes `EngineJobAdminService` over mTLS for
  enrolled manager workload principals. Human and other workload profiles are
  rejected.

Worker submission and status are HTTP RPCs routed through the proxy, not manager
gRPC methods.

## LifecycleService

Process lifecycle is monotonic:

`STARTING → READY → STOPPING`

`GetStatus` returns process state, service kind, process/activation identity,
transition time, typed reason, and bounded explanatory detail. `READY` means
that process crossed its startup barriers; it does not imply cluster, route, or
module health. There is no observable `STOPPED` state after an endpoint exits.

Lifecycle is read-only. Process owners request graceful shutdown with SIGTERM or
SIGINT. Local exit is proved by reaping the child process; deployed exit is
proved by the node agent's Systemd backend inspection.

`wr-cli lifecycle wait` requires the expected service kind and activation
identity for a `READY` wait. Manager observations also expose privileged
admission as `OPEN`, `CLOSED_STARTUP`, `CLOSED_ROLLOUT`, or `CLOSED_MISMATCH`.

## Engine registration and readiness

`NodeService.RegisterEngine` records an engine's declared modules, schemas,
listener metadata, deployment identity, requested secrets, and database
namespace inventory. Registration creates default routes as unhealthy.

`RegisterEngineResponse` returns:

- `secrets`: only application secret values explicitly requested by the engine,
  grouped by namespace;
- `namespace_access`: deterministic, non-secret database names and node-bound
  runtime/readiness role names.

Secrets therefore flow **from the manager to the engine in the response**, not
in the request. Namespace access contains no password, provisioning generation,
migration digest, deployment digest, or PostgreSQL client certificate. Those
expected-state values and certificate paths remain node-local. The manager does
not provision tenant PostgreSQL state.

After component load and health checks, `Heartbeat` atomically records readiness
and returns the manager routing version. The local proxy does not acknowledge
the first healthy heartbeat until it has installed at least that version.
`BeginEngineDrain` prevents later heartbeat publication, makes routes
non-serving, and returns convergence evidence without deregistering. Final
`DeregisterEngine` remains separate.

A managed engine's deployment metadata identifies stable node, revision, bundle
digest, operation, revision digest, and engine slot. `engine_id` is only the
current process identity. Database-enabled engines also register a queue ID and
job-admin address together; engines sharing one physical `wr__jobs` database
must share the queue ID.

## Deployment and durable operations

Deployment reservation and execution are separate:

1. `BeginDeployment` allocates an inactive monotonic node revision and stable
   operation identity, replaying the original receipt for the same principal
   and request token.
2. `FinalizeDeployment` binds completely staged bundle and resolved-release
   digests. It does not activate the release.
3. `SubmitOperation` starts manager-owned reconciliation of the complete desired
   inventory.
4. `VerifyDeployment` observes exact registration, heartbeat, authority, and
   route readiness.

`AbandonDeployment` applies only to an unsubmitted inactive allocation with no
registration or authority evidence. `BeginRollback` copies a retained successful
release into a new monotonic revision; rollback never moves revision numbers
backward.

Node operations survive CLI, agent, and manager restarts. They are idempotent by
authenticated actor and request token. One forward/restoration operation runs per
node. `GetOperation` and `ListOperations` expose state and append-only events;
`ResumeOperation` retries paused reconciliation after repair;
`CancelOperation` requests source restoration only before commit.

Claims use tagged proxy or engine-slot targets and are fenced by node, agent
activation, operation, step, and lease epoch. A node agent receives typed
backend actions, never shell text. The manager records delivery ambiguity before
returning a mutating claim. If the result acknowledgement is lost, the same
live agent retries the exact result; a replacement agent instead receives fresh
backend inspection work. Inconclusive inspection pauses rather than authorizing
a duplicate mutation.

A finalized node revision is the complete desired inventory. The manager handles
additions, replacements, and removals and enforces `max_unavailable`. Removing
final capacity requires explicit downtime consent. Cancellation or deadline
before commit restores the source inventory. Rollback is always a separate
authorized operation.

Manager-set rollouts use `BeginManagerRollout`, `AdvanceManagerRollout`,
`GetManagerRollout`, and `ResetFailedManagerRollout`. Targets start with
privileged admission closed. Post-closure ambiguity remains `FAILED_CLOSED`.
Reset requires complete stopped-host and uniform-policy evidence from the
original owner; it only permits a distinct fresh rollout and does not open
admission itself.

## NodeService workload boundary

Manager-side `NodeService` accepts only enrolled proxy and node-agent workloads:

- proxies register, publish complete bounded inventory snapshots, and
  deregister;
- node agents attest their node, activation, binary digest, protocol, and
  capabilities, then claim/renew/report operation and cleanup work.

Authenticated certificate identity binds a proxy or agent to its node. Payload
identity is not self-authorization. Agent compatibility compares the required
protocol and capability subset; local paths, polling intervals, and cleanup
retention do not grant authority.

The proxy's loopback `ProxyNodeControlService` forwards local engine
registration, heartbeat, drain, and deregistration to managers. It serializes
those operations per engine so an older heartbeat cannot undo a drain or
recreate a deregistered route. `GetProxyRoutingStatus` is a read-only local
observation used by foreground orchestration.

## JobService and EngineJobAdminService

Every `JobService` request names a queue ID and requires policy scope for that
queue. The manager never opens `wr__jobs`; it selects a fresh registered engine
for the queue and calls `EngineJobAdminService` with its distinct manager
client certificate.

| RPC | Semantics |
| --- | --- |
| `ListJobQueues` | Bounded registration-only discovery; it does not query queue databases. |
| `ListJobs` | Newest-first keyset page, default 50 and maximum 200; payload/result/error and internal claim data are omitted. |
| `GetJobQueueSummary` | Counts and pending-depth information under the supplied filters. |
| `GetJob` | Full authorized job detail except the internal claim fence. |
| `RetryJob` | Dead-only mutation that resets attempt and claim state, preserves the last failure, and is never automatically replayed after transport uncertainty. |

Filters are hierarchical: child worker/source filters require their parent
scope. Creation time uses an inclusive lower and exclusive upper bound. Cursors
are opaque and bound to normalized filters; traversal is not a database
snapshot, so concurrent changes may affect later pages.

Safe reads may fail over only before dispatch. A mutation transport failure
after dispatch reports an unknown outcome; inspect before deciding whether to
retry. Payload and successful result are each limited to 1 MiB, combined
identity metadata to 64 KiB, stored error text to 1 MiB, and unary admin messages
to 4 MiB.

`EngineJobAdminService.CheckJobQueue` is the manager workload readiness probe.
The engine rejects a mismatched queue and remains unavailable until embedded job
migrations and the initial workload-policy snapshot complete. Shutdown closes
job admission and drains accepted admin calls before route withdrawal.

## Cluster status

`ClusterService.GetClusterStatus` returns one manager-composed snapshot from a
single read-only PostgreSQL transaction. It includes manager membership,
desired and staged node revisions, current operation context, authenticated
proxy reports, engine/module heartbeats, routes, services, and the routing
version. It never returns secret values, TLS material, database URLs, or
configuration payloads.

Managers use PostgreSQL server time for lease and observation freshness.
Proxies push reports; status does not scrape proxy or host endpoints. Aggregate
severity is derived rather than stored. A node is healthy only when its current
desired deployment passes the same verification used by `VerifyDeployment`.
Services are healthy when all desired routes are healthy, degraded when only
some are healthy, and unhealthy when none are healthy.

Consumers must branch on `StatusSeverity`, condition codes, identities,
timestamps, and ages. Human `detail` text is not stable automation input.
`wr-cli cluster status` is display-only unless `--fail-on` is supplied;
`cluster wait` succeeds only when a non-empty selected target reaches the exact
requested severity.

## Routing, schemas, secrets, and schedules

`GetRoutingTable` returns the complete versioned table. `UpsertRoutingRule` and
`DeleteRoutingRule` update durable routing state and increment its version.
Routes select a destination namespace, module, optional semantic version,
engine address, and peer-proxy address. Source fields are metadata reserved for
future policy and do not currently restrict routing or authorize callers.
Default routes begin unhealthy and become eligible only through matching module
heartbeats.

`ListManagers` returns only PostgreSQL-lease-fresh managers. `GetSchema` returns
the descriptor uploaded on first registration of a module tuple.
`SetSecret`/`DeleteSecret` mutate encrypted namespace values, while
`ListSecrets` returns keys only. `UpsertSchedule`, `DeleteSchedule`, and
`ListSchedules` manage version-pinned at-least-once jobs; scheduled handlers
must be idempotent.

## Worker HTTP RPC

Worker jobs use these canonical HTTP RPC paths through the proxy:

- `POST /wruntime.WorkerService/SubmitJob`
- `POST /wruntime.WorkerService/GetJobStatus`

The routed namespace and module headers must match the request identity. A
non-empty worker version is exact and requires a matching `x-wr-version`; an
empty version is name-only. Request timeout and attempt values use zero as the
configured-default sentinel. Manager schedules always provide a non-empty
version.
