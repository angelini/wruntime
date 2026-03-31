# Configuration

Each service reads a TOML config file. Start the three components **in order**: manager first, then proxy, then engines.

```bash
just manager   # dev (cargo run)
just proxy
just engine

just manager-release   # release binaries
just proxy-release
just engine-release
```

## wr-manager

```bash
just manager
```

`manager.toml`:

```toml
listen_address                = "0.0.0.0:9000"
engine_heartbeat_timeout_secs = 30
```

## wr-proxy

```bash
just proxy
```

`proxy.toml`:

```toml
listen_address  = "0.0.0.0:9001"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://127.0.0.1:9001"   # this proxy's own address, as reachable by peers

[cache]
routing_table_ttl_secs = 5   # how often to poll the manager for routing updates
```

`proxy_address` must match how peer nodes (and engines on this node) will reach this proxy. The routing layer uses it to distinguish rules whose `proxy_address` matches this node — those are forwarded directly to the local engine; all others are forwarded to the peer proxy that owns that address.

The proxy is a streaming header-based router — it inspects only HTTP headers for routing decisions and streams request and response bodies through without buffering. It connects to the manager at startup, then polls for routing table updates in the background.

## wr-engine

```bash
just engine
```

`engine.toml`:

```toml
listen_address  = "0.0.0.0:9100"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://127.0.0.1:9001"   # local proxy; WASM outbound calls are rewritten to
                                           # this address, and it is sent to the manager on
                                           # registration so peers can find this node

[[module]]
name                 = "order-service"
namespace            = "ecommerce"
version              = "1.0.0"
wasm_path            = "modules/order_service.wasm"
schema_path          = "schemas/order_service.binpb"
request_timeout_secs = 10   # optional; default 30

[[module]]
name        = "inventory-service"
namespace   = "ecommerce"
version     = "1.0.0"
wasm_path   = "modules/inventory_service.wasm"
schema_path = "schemas/inventory_service.binpb"
# request_timeout_secs omitted — uses the default of 30 seconds
```

> **`schema_path` is required.** Every module must declare a compiled `FileDescriptorSet`. The engine will refuse to start if the file is absent. Schemas are uploaded to the manager on registration for discovery purposes.

On startup the engine:
1. Loads every listed WASM component from disk.
2. Registers itself and its modules with the manager (including schema bytes).
3. Starts an inbound HTTP server on `listen_address`.
4. Sends a heartbeat to the manager every 10 seconds, reporting all loaded modules as healthy.
5. Deregisters cleanly on `Ctrl+C`, which immediately marks its routing rules as unhealthy.

### Per-module request timeout

`request_timeout_secs` sets a hard deadline on every request dispatched to a module. If the WASM handler does not produce a response within that window, the engine cancels the request and returns `504 Gateway Timeout` to the proxy. The proxy treats a `504` as a terminal error and does not retry on another instance.

The default is **30 seconds**. Set it lower for latency-sensitive modules, or higher for modules that perform long-running work such as batch imports.

```toml
[[module]]
name                 = "batch-processor"
namespace            = "pipeline"
version              = "1.0.0"
wasm_path            = "modules/batch_processor.wasm"
schema_path          = "schemas/batch_processor.binpb"
request_timeout_secs = 120
```

### Module health checks

Every 10 seconds the engine sends `GET /__health` to each loaded module instance. If the module responds with a `2xx` status within 5 seconds it is reported as healthy in the next heartbeat; otherwise it is omitted, and the manager marks its routing rule unhealthy so the proxy stops sending traffic to it.

By default a module does not need to handle `/__health` at all — the `wasi:http/incoming-handler` export just needs to exist. The engine treats any `2xx` as healthy and anything else (including a timeout or a dropped connection) as unhealthy.

To run custom checks — verifying database connectivity, warming caches, or validating internal state — handle the path explicitly in your module:

```rust
impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();

        if path == "/__health" {
            // Run any checks that make sense for this module.
            let ok = database::query("SELECT 1", &[]).is_ok();
            let status = if ok { 200 } else { 503 };
            return send_response(response_out, status, vec![]);
        }

        // ... normal request handling
    }
}
```

If the health handler returns a non-`2xx` status or does not respond within 5 seconds, the module is excluded from that heartbeat. The routing rule is marked unhealthy by the manager and will not receive traffic until a subsequent heartbeat reports the module healthy again.

### Routing rules

Engines register themselves but do not create routing rules automatically — you create rules via the manager's gRPC API (or a management tool) after the engine is running:

```
# example using grpcurl
grpcurl -plaintext -d '{
  "rule_id": "r1",
  "source_module": "order-service",
  "source_namespace": "ecommerce",
  "destination_module": "inventory-service",
  "destination_namespace": "ecommerce",
  "destination_version": "1.0.0",
  "engine_id": "<engine-uuid>",
  "engine_address": "http://127.0.0.1:9100",
  "proxy_address": "http://127.0.0.1:9001"
}' 127.0.0.1:9000 wruntime.ManagerService/UpsertRoutingRule
```

`proxy_address` tells every proxy which node owns this rule. A proxy whose own `[node] proxy_address` matches will route directly to `engine_address`; all other proxies will relay to `proxy_address` and let that node route locally.

To run **multiple instances** of the same module version across different engines (on the same or different nodes), create one rule per engine pointing at the same `(destination_module, destination_namespace, destination_version)`. The proxy round-robins across all healthy rules for that tuple.

To deploy a **new version** alongside the old one, register a new engine with `version = "2.0.0"` and add a corresponding rule. Callers that omit `x-wr-version` are automatically upgraded to the highest semver. Callers that pin a version with the `x-wr-version` request header continue to reach the older instance.

### Multi-node deployment

Node B config files follow the same structure — just use different ports and a matching `proxy_address`:

```toml
# examples/multi-node/node-b/proxy.toml
listen_address  = "0.0.0.0:9002"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://node-b-host:9002"

# examples/multi-node/node-b/engine.toml
listen_address  = "0.0.0.0:9200"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address = "http://node-b-host:9002"
```

When a module on Node A calls a module whose routing rule has `proxy_address = "http://node-b-host:9002"`, Node A's proxy adds `x-wr-via-proxy: 1` and forwards the request to Node B's proxy. Node B's `RoutingLayer` resolves the destination as a local engine and forwards to it.
