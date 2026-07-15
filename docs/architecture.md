# Architecture

A **node** is one `wr-proxy` co-located with one or more `wr-engine` instances. Nodes are independent — each proxy handles its own inbound traffic and forwards cross-node requests directly to the peer proxy, which then routes locally to its engines.

```
              ┌────────────────────────┐    gossip    ┌────────────────────────┐
              │    wr-manager (1)     │◄───(UDP)───►│    wr-manager (2)     │
              │  Engine registry      │             │  Engine registry      │
              │  Routing table        │             │  Routing table        │
              └──────────┬───────────┘             └──────────┬───────────┘
                         │          shared Postgres           │
                         │     (serialized via row locks)     │
                         └────────────────┬───────────────────┘
                                          │ gRPC (all nodes)
               ┌──────────────────────────┴───────────────────────┐
               │                                          │
               ▼                                          ▼
┌─────────────────────────────┐        ┌─────────────────────────────┐
│           Node A            │        │           Node B            │
│                             │        │                             │
│  ┌───────────────────────┐  │        │  ┌───────────────────────┐  │
│  │      wr-proxy A       │◄─┼────────┼─►│      wr-proxy B       │  │
│  │  TracingLayer         │  │  HTTP  │  │  TracingLayer         │  │
│  │  RoutingLayer         │  │        │  │  RoutingLayer         │  │
│  │  EgressLayer          │  │        │  │  ForwardService       │  │
│  │  ForwardService       │  │        │  │                       │  │
│  └──────────┬────────────┘  │        │  │                       │  │
│             │ local         │        │  └──────────┬────────────┘  │
│             ▼               │        │             │ local         │
│  ┌───────────────────────┐  │        │  ┌──────────▼────────────┐  │
│  │      wr-engine A      │  │        │  │      wr-engine B      │  │
│  │  ┌─────────────────┐  │  │        │  │  ┌─────────────────┐  │  │
│  │  │  order-service  │  │  │        │  │  │inventory-service│  │  │
│  │  │  (WASM module)  │  │  │        │  │  │  (WASM module)  │  │  │
│  │  └─────────────────┘  │  │        │  │  └─────────────────┘  │  │
│  └───────────────────────┘  │        │  └───────────────────────┘  │
└─────────────────────────────┘        └─────────────────────────────┘
```

## Components

| Binary | Default port | Role |
|--------|-------------|------|
| `wr-manager` | `9000` (gRPC) + `9010` (gossip) | Registry — engines register here, proxies sync routing tables from here. Runs active-active behind shared Postgres; chitchat gossip provides manager-to-manager liveness detection. On registration the manager resolves the engine's requested secrets and per-namespace DB credentials, then persists the engine, its schemas, and one initially-unhealthy default routing rule per schema-bearing module in a single transaction — a failed registration leaves no routing rules. |
| `wr-proxy` | `9001` (HTTP) + `9002` (gRPC control plane) | Streaming header-based router — intercepts and routes inter-module traffic; forwards cross-node requests to peer proxies; request and response bodies flow through without buffering. The control plane (`NodeService`) handles engine registration and heartbeats |
| `wr-engine` | `9100` (HTTP) | Loads WASM modules, runs them, and receives forwarded requests |

A **node** groups one `wr-proxy` with one or more `wr-engine` instances behind a shared externally-reachable proxy address. Each node knows its own address via `[node] proxy_address` in its config files; the engine sends this value to the manager on registration so the routing table can distinguish local from remote destinations.

## Manager clustering (active-active)

Multiple `wr-manager` instances can run simultaneously for high availability. All managers share the same Postgres database — concurrent writes are serialized via `SELECT ... FOR UPDATE NOWAIT` on a lock sentinel row. Each manager:

1. Registers itself in the `wr_managers` table on startup (UUID, gRPC address, gossip address).
2. Heartbeats every 15 seconds; cleans up stale managers (60 s timeout).
3. Participates in a [chitchat](https://docs.rs/chitchat) gossip mesh (UDP), publishing its own `grpc_address`/`gossip_address` into gossip node state. Chitchat's phi-accrual failure detector is the **primary** manager liveness mechanism — `gossip_listen_address` is required and must be reachable, or the manager fails to start.
4. Deregisters itself on graceful shutdown.

`ListManagers` returns a per-manager reconciliation of the DB-heartbeat-fresh set against chitchat — peers chitchat has marked dead are dropped immediately; peers gossip has never seen are included only during a short bootstrap convergence window after a manager starts, then excluded. Proxies discover managers via `ListManagers` (chitchat-reconciled), bootstrapping and falling back to a direct `wr_managers` query only when no manager RPC is reachable. The Postgres 60s heartbeat cleanup (`cleanup_stale_managers`) remains as a secondary safety-net backstop — no behavior change.

## Scheduler (routed job control plane)

Each manager runs a background scheduler that fires `wr_schedules` rows as jobs, using Postgres as a claim/lease queue with a fencing token (`claim_id`) so active-active managers cannot double-fire or clobber each other's in-flight attempts. Every tick runs three short phases:

1. **Claim** — a short transaction claims due, unleased (or lease-expired) rows with `FOR UPDATE SKIP LOCKED`, stamping `claimed_by`, `claimed_until` (a lease), and a fresh `claim_id`, then commits immediately.
2. **Submit** — outside any transaction, the manager submits each claimed job through its own configured `local_proxy_address` (the local proxy loopback), exactly like `wr-cli invoke`: POST `/wruntime.WorkerService/SubmitJob` with `x-wr-destination: http://{namespace}.{module}/wruntime.WorkerService/SubmitJob`, using the same routing/mTLS path as normal inter-module traffic.
3. **Finalize** — a fenced `UPDATE ... WHERE claim_id = $claim_id` records success (advances `next_fire_at`, clears the lease) or failure (records `last_error`, bumps `consecutive_failures`, backs off `next_fire_at`); a finalize whose `claim_id` no longer matches (row reclaimed by another manager) affects zero rows and is dropped.

Delivery is **at-least-once** — a manager crash between submit and finalize leaves the lease to expire (`claimed_until < NOW()`), and the row becomes claimable again — so scheduled jobs must be idempotent. The manager's `/wruntime.WorkerService/SubmitJob` submission path is the one place the scheduler couples to the worker/job subsystem's endpoint contract; if that endpoint changes, only `wr_manager::scheduler::submit_job` needs to change.

## Request flow

```
WASM module makes HTTP call to "http://ecommerce.inventory/inventory.InventoryService/GetItems"
  │
  ▼  [WasiHttpView::send_request intercepts — transparent to the module]
  │  Adds headers:
  │    x-wr-source:      "order-service"
  │    x-wr-destination: "http://ecommerce.inventory/inventory.InventoryService/GetItems"
  │  Rewrites URI to the local wr-proxy (Node A)
  │
  ▼
wr-proxy A  (Node A)
  │  1. TracingLayer       — opens an OTel span (captures source, destination,
  │                          status, duration); injects W3C traceparent header
  │  2. RoutingLayer       — single routing table read per request;
  │                          reads optional x-wr-version header; when omitted,
  │                          load-balances across all healthy versions;
  │                          returns 503 if no healthy instance matches;
  │                          injects x-wr-module, x-wr-namespace, x-wr-version;
  │                          round-robins across healthy instances at the same
  │                          version; resolves destination as LocalEngine or
  │                          RemoteProxy; when egress is enabled and no internal
  │                          route matches, sets ExternalEgress extension
  │  3. EgressLayer        — handles ExternalEgress requests: enforces the domain
  │                          allowlist and forwards to external hosts;
  │                          passes internal requests through to ForwardService
  │  4. ForwardService     — strips x-wr-destination / x-wr-source, injects
  │                          traceparent; streams request body to engine and
  │                          streams engine response back — no buffering; then:
  │
  ├── destination is on Node A (LocalEngine) ──────────────────────────────────┐
  │     strips x-wr-destination / x-wr-source / x-wr-via-proxy                 │
  │     forwards directly to wr-engine A                                       │
  │                                                                            ▼
  │                                                                    wr-engine A
  │
  └── destination is on Node B (RemoteProxy) ──────────────────────────────────┐
        sets x-wr-via-proxy: 1                                                 │
        forwards to wr-proxy B                                                 │
                                                                               ▼
                                                               wr-proxy B  (Node B)
                                                                 RoutingLayer routes locally
                                                                               │
                                                                               ▼
                                                                       wr-engine B

wr-engine (destination)
  │  Inbound HTTP server reads x-wr-module + x-wr-version + x-wr-namespace,
  │  dispatches to the correct WASM instance via round-robin
  │
  ▼
inventory-service WASM module handles the request
```

## Request headers (`x-wr-*`)

All internal routing uses a set of reserved `x-wr-*` HTTP headers. The proxy strips every `x-wr-*` header from externally-originated requests (public routes) to prevent spoofing.

| Header | Set by | Read by | Description |
|--------|--------|---------|-------------|
| `x-wr-destination` | `wr-engine` (outbound WASM call), `wr-proxy` IngressLayer (public routes) | `wr-proxy` RoutingLayer, TracingLayer | Full destination URI of the original call — e.g. `http://ecommerce.inventory/inventory.InventoryService/GetItems`. The host encodes the destination as `{namespace}.{module}`; the path is the canonical generated RPC path `/{proto_package}.{ProtoServiceName}/{ProtoMethodName}`. Stripped by ForwardService before reaching the destination engine. |
| `x-wr-source` | `wr-engine` (outbound WASM call), `wr-proxy` IngressLayer (set to `"external"` for public routes) | `wr-proxy` TracingLayer | Name of the calling module. Recorded as a span attribute for metrics attribution and error reporting. Stripped by ForwardService before reaching the destination engine. |
| `x-wr-source-ns` | `wr-engine` (outbound WASM call) | — | Namespace of the calling module. Carried alongside `x-wr-source` for attribution; not used for routing decisions. Stripped by ForwardService before reaching the destination engine. |
| `x-wr-version` | Caller (optional — WASM module or `wr-cli`) | `wr-proxy` RoutingLayer | Pins the request to a specific semver of the destination module (e.g. `1.2.0`). When omitted the proxy load-balances across all healthy versions of the module. RoutingLayer overwrites the value with the resolved version before forwarding. |
| `x-wr-module` | `wr-proxy` RoutingLayer | `wr-engine` inbound server | Resolved destination module name. The engine uses this (together with `x-wr-namespace` and `x-wr-version`) to select the correct WASM instance. |
| `x-wr-namespace` | `wr-proxy` RoutingLayer | `wr-engine` inbound server | Resolved destination module namespace. |
| `x-wr-via-proxy` | `wr-proxy` ForwardService (cross-node hop) | `wr-proxy` RoutingLayer | Set to `1` when forwarding to a peer proxy on another node. Stripped by ForwardService on the local-engine path. |

### Header lifecycle per request

```
WASM module calls http://ecommerce.inventory/inventory.InventoryService/GetItems
  │
  │  WasiHttpView (wr-engine) sets:
  │    x-wr-destination: http://ecommerce.inventory/inventory.InventoryService/GetItems
  │    x-wr-source:      order-service
  │    x-wr-source-ns:   ecommerce
  │
  ▼ wr-proxy (same node)
  │  RoutingLayer injects:
  │    x-wr-module:    inventory
  │    x-wr-namespace: ecommerce
  │    x-wr-version:   1.2.0          ← resolved (or forwarded from caller)
  │
  ├─ local engine ──► ForwardService strips x-wr-destination, x-wr-source,
  │                   x-wr-source-ns, x-wr-via-proxy before sending to wr-engine
  │
  └─ peer proxy   ──► ForwardService sets x-wr-via-proxy: 1; preserves
                      x-wr-destination for peer RoutingLayer to resolve
```
