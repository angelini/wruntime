# Architecture

A **node** is one `wr-proxy` co-located with one or more `wr-engine` instances. Nodes are independent — each proxy handles its own inbound traffic and forwards cross-node requests directly to the peer proxy, which then routes locally to its engines.

```
                         ┌────────────────────────┐
                         │      wr-manager        │
                         │                        │
                         │  Engine registry       │
                         │  Routing table         │
                         └──────────┬─────────────┘
                                    │ gRPC (all nodes)
               ┌────────────────────┴─────────────────────┐
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
| `wr-manager` | `9000` (gRPC) | Central registry — engines register here, proxies sync routing tables from here |
| `wr-proxy` | `9001` (HTTP) | Streaming header-based router — intercepts and routes inter-module traffic; forwards cross-node requests to peer proxies; request and response bodies flow through without buffering |
| `wr-engine` | `9100` (HTTP) | Loads WASM modules, runs them, and receives forwarded requests |

A **node** groups one `wr-proxy` with one or more `wr-engine` instances behind a shared externally-reachable proxy address. Each node knows its own address via `[node] proxy_address` in its config files; the engine sends this value to the manager on registration so the routing table can distinguish local from remote destinations.

## Request flow

```
WASM module makes HTTP call to "http://ecommerce.inventory/GetItems"
  │
  ▼  [WasiHttpView::send_request intercepts — transparent to the module]
  │  Adds headers:
  │    x-wr-source:      "order-service"
  │    x-wr-destination: "http://ecommerce.inventory/GetItems"
  │  Rewrites URI to the local wr-proxy (Node A)
  │
  ▼
wr-proxy A  (Node A)
  │  1. TracingLayer       — opens an OTel span (captures source, destination,
  │                          status, duration); injects W3C traceparent header
  │  2. RoutingLayer       — single routing table read per request;
  │                          reads optional x-wr-version header; defaults to
  │                          highest semver among healthy rules for the module;
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
| `x-wr-destination` | `wr-engine` (outbound WASM call), `wr-proxy` IngressLayer (public routes) | `wr-proxy` RoutingLayer, TracingLayer | Full destination URI of the original call — e.g. `http://ecommerce.inventory/GetItems`. The host encodes the destination as `{namespace}.{module}`; the path is the RPC method name. Stripped by ForwardService before reaching the destination engine. |
| `x-wr-source` | `wr-engine` (outbound WASM call), `wr-proxy` IngressLayer (set to `"external"` for public routes) | `wr-proxy` TracingLayer | Name of the calling module. Recorded as a span attribute for metrics attribution and error reporting. Stripped by ForwardService before reaching the destination engine. |
| `x-wr-source-ns` | `wr-engine` (outbound WASM call) | — | Namespace of the calling module. Carried alongside `x-wr-source` for attribution; not used for routing decisions. Stripped by ForwardService before reaching the destination engine. |
| `x-wr-version` | Caller (optional — WASM module or `wr-cli`) | `wr-proxy` RoutingLayer | Pins the request to a specific semver of the destination module (e.g. `1.2.0`). When omitted the proxy routes to the highest healthy semver. RoutingLayer overwrites the value with the resolved version before forwarding. |
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
