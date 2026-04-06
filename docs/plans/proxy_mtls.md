# Inter-Service mTLS

Encrypts and mutually authenticates all network-facing inter-service communication: proxy-to-proxy, proxy-to-manager, engine-to-proxy, and CLI-to-manager.

## Context

All inter-service traffic currently flows over plain HTTP/gRPC. Any network listener is reachable by anything on the network. This change:

1. Makes mTLS mandatory on all network-facing listeners — a shared CA signs one cert per node
2. Locks intra-node listeners (proxy→engine, engine→proxy control plane) to `127.0.0.1` loopback
3. Integrates cert generation (`wr cert`) and provisioning into the deploy pipeline
4. Uses a single port per service — no separate "peer port"

## Communication Paths

| Flow | Port | Crosses network? | Change |
|------|------|-------------------|--------|
| WASM module → Proxy (HTTP outbound) | `:9001` | No (loopback) | Keep plain HTTP, bind `127.0.0.1` |
| Proxy → Engine (forward) | `:9100` | No (loopback) | Keep plain HTTP, bind `127.0.0.1` |
| Engine → Proxy (gRPC control) | `:9002` | No (loopback) | Keep plain gRPC, bind `127.0.0.1` |
| **Proxy → Proxy (cross-node)** | `:9443` | **Yes** | **New mTLS listener** |
| **Proxy → Manager (sync/fwd)** | `:9000` | **Yes** | **Add TLS to gRPC** |
| **CLI → Manager (commands)** | `:9000` | **Yes** | **Add TLS to gRPC client** |

The proxy gets a dedicated mTLS peer listener on `:9443` (configurable via `peer_port`). The internal `:9001` listener binds to `127.0.0.1` only — unreachable from the network. The manager's gRPC listener upgrades to TLS. The CLI and proxy gRPC clients add TLS with the same CA.

---

## Architecture

```
Node A                               Node B
┌──────────────────────────────────┐  ┌──────────────────────────────────┐
│  proxy 127.0.0.1:9001 (HTTP/2)  │  │  proxy 127.0.0.1:9001 (HTTP/2)  │
│  proxy 0.0.0.0:9443 (mTLS) <────┼──┼──> proxy 0.0.0.0:9443 (mTLS)    │
│  engine 127.0.0.1:9100           │  │  engine 127.0.0.1:9200           │
│  proxy ctrl 127.0.0.1:9002      │  │  proxy ctrl 127.0.0.1:9002      │
└──────────────────────────────────┘  └──────────────────────────────────┘

Manager (0.0.0.0:9000, TLS gRPC)
  ↑ TLS from proxies (routing sync, registration forwarding)
  ↑ TLS from CLI (management commands)
```

The peer address is derived automatically: given `proxy_address = "http://10.0.1.5:9001"` and `peer_port = 9443`, the peer address becomes `https://10.0.1.5:9443`. No separate URL field needed.

**Certificate flow:**
```
wr cert init-ca              →  certs/ca.crt, certs/ca.key
wr cert generate <hostname>  →  certs/<hostname>.crt, certs/<hostname>.key
wr node deploy ... --cert-dir ./certs  →  SCPs ca.crt + node cert/key to remote
```

---

## Phase 1 — `wr cert` CLI Command

New top-level subcommand using `rcgen` (pure Rust, no openssl dependency):

```
wr cert init-ca [--output ./certs/]              # P-256 CA, 10yr, self-signed
wr cert generate <hostname> [--ca-dir ./certs/]  # Node cert signed by CA, SAN: hostname + 127.0.0.1
```

Output: PEM files — `ca.crt`, `ca.key`, `<hostname>.crt`, `<hostname>.key`.

**Files:**
| File | Change |
|------|--------|
| `wr-cli/Cargo.toml` | Add `rcgen = "0.13"` |
| `wr-cli/src/cmd/cert.rs` | **New** — `CertArgs`, `InitCa`, `Generate` subcommands |
| `wr-cli/src/cmd/mod.rs` | Add `pub mod cert;` |
| `wr-cli/src/main.rs` | Add `Cert` variant to `Commands` enum |

---

## Phase 2 — Config Changes

### `NodeConfig` (`wr-common/src/node.rs`)

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub proxy_address: String,            // plain HTTP, engines use this (loopback)
    #[serde(default)]
    pub control_address: String,          // gRPC control plane (loopback)
    #[serde(default = "default_peer_port")]
    pub peer_port: u16,                   // mTLS peer listener port (default 9443)
    pub tls: TlsConfig,
}

fn default_peer_port() -> u16 { 9443 }

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
}

impl NodeConfig {
    /// Derive the mTLS peer address from proxy_address host + peer_port.
    /// "http://10.0.1.5:9001" + peer_port=9443 → "https://10.0.1.5:9443"
    pub fn peer_address(&self) -> String {
        let uri: http::Uri = self.proxy_address.parse().expect("valid proxy_address");
        let host = uri.host().expect("proxy_address must have a host");
        format!("https://{}:{}", host, self.peer_port)
    }
}
```

### `ProxyConfig` validation (`wr-proxy/src/config.rs`)

```rust
v.check(!self.node.tls.cert_path.is_empty(), "node.tls.cert_path is required");
v.check(!self.node.tls.key_path.is_empty(), "node.tls.key_path is required");
v.check(!self.node.tls.ca_cert_path.is_empty(), "node.tls.ca_cert_path is required");
v.check(self.node.peer_port > 0, "node.peer_port must be > 0");
```

### Loopback enforcement

At proxy startup, warn if `listen_address` binds to a non-loopback interface:

```rust
if !config.listen_address.starts_with("127.0.0.1:")
    && !config.listen_address.starts_with("localhost:")
{
    warn!("listen_address should bind to loopback — network traffic uses the mTLS peer listener");
}
```

### Example proxy.toml

```toml
listen_address  = "127.0.0.1:9001"
control_address = "127.0.0.1:9002"

[node]
proxy_address = "http://10.0.1.5:9001"
peer_port     = 9443

[node.tls]
cert_path    = "certs/10.0.1.5.crt"
key_path     = "certs/10.0.1.5.key"
ca_cert_path = "certs/ca.crt"

[database]
url = "postgres://..."
```

### Example engine.toml

```toml
listen_address = "127.0.0.1:9100"

[node]
proxy_address   = "http://127.0.0.1:9001"
control_address = "http://127.0.0.1:9002"
peer_port       = 9443

[node.tls]
cert_path    = "certs/10.0.1.5.crt"
key_path     = "certs/10.0.1.5.key"
ca_cert_path = "certs/ca.crt"
```

### Manager TLS config

Add TLS config to `ManagerConfig` (`wr-manager/src/config.rs`):

```rust
pub struct ManagerConfig {
    // ... existing fields ...
    pub tls: TlsConfig,  // reuse same struct from wr-common
}
```

Example `manager.toml`:
```toml
listen_address = "0.0.0.0:9000"

[tls]
cert_path    = "certs/manager.crt"
key_path     = "certs/manager.key"
ca_cert_path = "certs/ca.crt"

[database]
url = "postgres://..."

[cluster]
cluster_id = "prod"
gossip_listen_address = "0.0.0.0:9010"
```

**Files:**
| File | Change |
|------|--------|
| `wr-common/src/node.rs` | Add `peer_port`, `tls: TlsConfig`, `TlsConfig` struct, `peer_address()` method |
| `wr-proxy/src/config.rs` | Add TLS + peer_port validation; loopback warning |
| `wr-manager/src/config.rs` | Add `tls: TlsConfig` |
| All example `*.toml` files | Add `[node.tls]` / `[tls]` sections; `listen_address` → `127.0.0.1` for loopback services |

---

## Phase 3 — TLS Module (`wr-common/src/tls.rs`)

Shared TLS utilities in `wr-common` so proxy, manager, and CLI can all use them.

**New file: `wr-common/src/tls.rs`**

### Server-side (mTLS acceptor)

```rust
pub fn build_server_config(tls: &TlsConfig) -> Result<Arc<rustls::ServerConfig>> {
    // Load cert chain, key, CA from PEM files
    // Build ServerConfig with WebPkiClientVerifier (require client certs)
}

pub fn build_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor> {
    Ok(TlsAcceptor::from(build_server_config(tls)?))
}
```

### Client-side (mTLS connector)

```rust
pub fn build_client_config(tls: &TlsConfig) -> Result<rustls::ClientConfig> {
    // Load cert chain, key, CA from PEM files
    // Build ClientConfig with client auth cert and CA-only root store
}
```

### `HttpsClientPool<B>` — mTLS HTTP/2 client pool

```rust
pub struct HttpsClientPool<B> {
    clients: Arc<Vec<Client<HttpsConnector<HttpConnector>, B>>>,
    next: Arc<AtomicUsize>,
}

impl<B> HttpsClientPool<B> {
    pub fn new(size: usize, tls_config: rustls::ClientConfig) -> Self { ... }
    pub fn get(&self) -> &Client<...> { /* round-robin */ }
}
```

### gRPC TLS helpers

```rust
/// Build a tonic ServerTlsConfig for the manager's gRPC listener.
pub fn build_tonic_server_tls(tls: &TlsConfig) -> Result<tonic::transport::ServerTlsConfig> {
    let cert_pem = std::fs::read_to_string(&tls.cert_path)?;
    let key_pem = std::fs::read_to_string(&tls.key_path)?;
    let ca_pem = std::fs::read_to_string(&tls.ca_cert_path)?;
    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let ca = tonic::transport::Certificate::from_pem(ca_pem);
    Ok(tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca))
}

/// Build a tonic Channel with mTLS for connecting to a TLS gRPC server.
pub fn build_tonic_client_tls(tls: &TlsConfig) -> Result<tonic::transport::ClientTlsConfig> {
    let cert_pem = std::fs::read_to_string(&tls.cert_path)?;
    let key_pem = std::fs::read_to_string(&tls.key_path)?;
    let ca_pem = std::fs::read_to_string(&tls.ca_cert_path)?;
    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let ca = tonic::transport::Certificate::from_pem(ca_pem);
    Ok(tonic::transport::ClientTlsConfig::new()
        .identity(identity)
        .ca_certificate(ca))
}
```

### In-memory constructors (for tests)

```rust
pub fn build_server_config_from_der(...) -> Result<Arc<rustls::ServerConfig>> { ... }
pub fn build_client_config_from_der(...) -> Result<rustls::ClientConfig> { ... }
```

**Dependencies:**
| Crate | Add to |
|-------|--------|
| `tokio-rustls = "0.26"` | `wr-common/Cargo.toml` |
| `rustls-pemfile = "2"` | `wr-common/Cargo.toml` |
| `hyper-rustls = "0.27"` | `wr-common/Cargo.toml` |

`rustls = "0.23"` is already used by wr-proxy; making it available in wr-common lets all crates share the TLS primitives.

**Files:**
| File | Change |
|------|--------|
| `wr-common/Cargo.toml` | Add `tokio-rustls`, `rustls`, `rustls-pemfile`, `hyper-rustls` |
| `wr-common/src/tls.rs` | **New** — all TLS builders, `HttpsClientPool`, gRPC TLS helpers |
| `wr-common/src/lib.rs` | Add `pub mod tls;` |

---

## Phase 4 — Proto Extension

Add `peer_address` to `EngineRegistration` and `RoutingRule`:

```proto
message EngineRegistration {
  // ... fields 1-5 unchanged ...
  string peer_address = 6;  // derived mTLS peer address (https://host:peer_port)
}

message RoutingRule {
  // ... fields 1-10 unchanged ...
  string peer_address = 11;  // mTLS peer address for cross-node routing
}
```

### Engine sends peer_address (`wr-engine/src/main.rs`)

```rust
EngineRegistration {
    peer_address: config.node.peer_address(),
    // ... existing fields ...
}
```

### Routing rule creation (`wr-proxy/src/node_service.rs`, line ~126)

Use `peer_address` in routing rules:
```rust
proxy_address: reg.peer_address.clone(),
```

### Routing layer self-address (`wr-proxy/src/main.rs`)

```rust
let self_address = config.node.peer_address();
RoutingLayer::new(routing_table.clone(), self_address)
```

`make_destination` (routing.rs:108-116) remains unchanged — when `rule.proxy_address == self_proxy_address`, it's local; otherwise `Destination::RemoteProxy` with the `https://` peer address.

### Manager DB

New migration:
```sql
ALTER TABLE wr_routing_rules ADD COLUMN IF NOT EXISTS peer_address TEXT NOT NULL DEFAULT '';
ALTER TABLE wr_engines ADD COLUMN IF NOT EXISTS peer_address TEXT NOT NULL DEFAULT '';
```

Update insert/select queries to include `peer_address`.

**Files:**
| File | Change |
|------|--------|
| `proto/wruntime.proto` | Add `peer_address` to `EngineRegistration` (6) and `RoutingRule` (11) |
| `wr-engine/src/main.rs` | Send `peer_address()` in registration |
| `wr-proxy/src/node_service.rs` | Use `reg.peer_address` in routing rule |
| `wr-proxy/src/main.rs` | Pass `peer_address()` to `RoutingLayer` |
| `wr-manager/src/db.rs` | `peer_address` in queries |
| `wr-manager/migrations/` | New migration |

---

## Phase 5 — Proxy Tower Stack Changes

### `ForwardService` (`wr-proxy/src/layers/forward.rs`)

Always holds an mTLS pool — no Option:

```rust
#[derive(Clone)]
pub struct ForwardService {
    pool: HttpClientPool<ProxyBody>,               // plain HTTP for local engines
    mtls_pool: tls::HttpsClientPool<ProxyBody>,    // mTLS for remote proxies
    cb_registry: Arc<CircuitBreakerRegistry>,
}

impl ForwardService {
    pub fn new(
        cb_registry: Arc<CircuitBreakerRegistry>,
        mtls_pool: tls::HttpsClientPool<ProxyBody>,
    ) -> Self {
        let pool = HttpClientPool::new(DEFAULT_POOL_SIZE);
        Self { pool, mtls_pool, cb_registry }
    }
}
```

In `call()`:
```rust
let result = match &destination {
    Destination::LocalEngine(_) => client.request(forward_req).await,
    Destination::RemoteProxy(_) => self.mtls_pool.get().request(forward_req).await,
};
```

### `main.rs` — mTLS setup + peer listener

```rust
// Build mTLS resources from node TLS config
let client_config = wr_common::tls::build_client_config(&config.node.tls)?;
let mtls_pool = wr_common::tls::HttpsClientPool::new(DEFAULT_POOL_SIZE, client_config);
let tls_acceptor = wr_common::tls::build_acceptor(&config.node.tls)?;

let self_address = config.node.peer_address();

// Internal stack
let internal_svc = ServiceBuilder::new()
    .layer(TracingLayer)
    .layer(RoutingLayer::new(routing_table.clone(), self_address).with_egress(egress_domains))
    .layer(EgressLayer::new(config.egress.clone()))
    .service(ForwardService::new(cb_registry.clone(), mtls_pool.clone()));

// Internal listener — loopback only
let internal_listener = TcpListener::bind(&config.listen_address).await?;
info!(address = %config.listen_address, "proxy listening (internal, loopback)");
tokio::spawn(accept_loop(internal_listener, internal_svc.clone()));

// mTLS peer listener — all interfaces
let peer_bind = format!("0.0.0.0:{}", config.node.peer_port);
let peer_listener = TcpListener::bind(&peer_bind).await?;
info!(address = %peer_bind, "proxy listening (mTLS peer)");
tokio::spawn(tls_accept_loop(peer_listener, tls_acceptor, internal_svc.clone()));
```

### `tls_accept_loop`

Mirrors `accept_loop`, wraps each TCP stream through `TlsAcceptor` before `auto::Builder::serve_connection`.

### External stack

Same — `ForwardService::new` takes the mTLS pool, `RoutingLayer` takes `peer_address()`.

**Files:**
| File | Change |
|------|--------|
| `wr-proxy/src/main.rs` | mTLS pool + acceptor; peer listener; `tls_accept_loop`; loopback warning |
| `wr-proxy/src/layers/forward.rs` | `mtls_pool` field (required); branch on `Destination` |
| `wr-proxy/Cargo.toml` | Can remove `rustls`, `hyper-rustls` direct deps (now via `wr-common`) or keep for `rustls::crypto` init |

---

## Phase 6 — Manager TLS gRPC

### Manager server (`wr-manager/src/main.rs`)

Add TLS to the gRPC listener using tonic's built-in TLS support:

```rust
let tls_config = wr_common::tls::build_tonic_server_tls(&config.tls)?;

Server::builder()
    .tls_config(tls_config)?
    .add_service(ManagerServiceServer::new(manager))
    .serve(addr)
    .await?;
```

### Proxy → Manager connection (`wr-common/src/discovery.rs`)

`ManagerDiscovery::connect_new` currently uses `ManagerServiceClient::connect(addr)`. Update to use TLS:

```rust
// ManagerDiscovery gains a tls_config field
pub struct ManagerDiscovery {
    pool: Pool,
    managers: RwLock<Vec<String>>,
    affinity: RwLock<Option<AffinityState>>,
    tls_config: tonic::transport::ClientTlsConfig,
}

// In connect_new():
let channel = Endpoint::from_shared(addr.clone())?
    .tls_config(self.tls_config.clone())?
    .connect()
    .await?;
```

The proxy passes the TLS config when creating `ManagerDiscovery`:

```rust
let client_tls = wr_common::tls::build_tonic_client_tls(&config.node.tls)?;
let discovery = Arc::new(ManagerDiscovery::new(db_pool, client_tls));
```

### CLI → Manager connection (`wr-cli/src/client.rs`)

The CLI needs a `--ca-cert` flag (or reads from `wr-deploy.toml` `cert_dir`) to verify the manager's TLS cert. Optionally also `--cert` and `--key` for mTLS client auth.

```rust
pub async fn connect(addr: &str, tls: Option<&TlsConfig>) -> Result<ManagerServiceClient<Channel>> {
    let mut endpoint = Endpoint::from_shared(addr.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10));

    if let Some(tls) = tls {
        let tls_config = wr_common::tls::build_tonic_client_tls(tls)?;
        endpoint = endpoint.tls_config(tls_config)?;
    }

    let channel = endpoint.connect().await.context("failed to connect to manager")?;
    Ok(ManagerServiceClient::new(channel))
}
```

CLI global args:

```rust
#[arg(long, env = "WR_CA_CERT")]
ca_cert: Option<String>,
#[arg(long, env = "WR_CLIENT_CERT")]
client_cert: Option<String>,
#[arg(long, env = "WR_CLIENT_KEY")]
client_key: Option<String>,
```

These can also be set in `wr-deploy.toml` alongside `cert_dir`:
```toml
ca_cert_path    = "certs/ca.crt"
client_cert_path = "certs/admin.crt"
client_key_path  = "certs/admin.key"
```

**Files:**
| File | Change |
|------|--------|
| `wr-manager/src/main.rs` | Add TLS to gRPC server |
| `wr-manager/src/config.rs` | Add `tls: TlsConfig` |
| `wr-common/src/discovery.rs` | Add `tls_config` field to `ManagerDiscovery`; use TLS in `connect_new` |
| `wr-cli/src/client.rs` | Accept optional `TlsConfig`; build TLS channel |
| `wr-cli/src/main.rs` | Add `--ca-cert`, `--client-cert`, `--client-key` global args |
| `wr-cli/src/cmd/deploy_config.rs` | Add `ca_cert_path`, `client_cert_path`, `client_key_path` |

---

## Phase 7 — Deploy Integration

### `wr-deploy.toml`

```toml
cert_dir  = "./certs"   # local dir with CA + node certs from `wr cert`
peer_port = 9443        # peer TLS port (default 9443)
```

### `DeployConfig` additions

```rust
pub cert_dir: Option<String>,
pub peer_port: Option<u16>,
```

### Bundle-time (`add_proxy_config` in node.rs)

- `listen_address` → `127.0.0.1:{proxy_port}` (loopback only)
- `control_address` → `127.0.0.1:{control_port}` (loopback only)
- Add `peer_port = {peer_port}` to `[node]`
- Add `[node.tls]` with relative paths: `certs/node.crt`, `certs/node.key`, `certs/ca.crt`

### Deploy-time (`deploy()` in node.rs)

After resolving template vars:

1. Resolve `cert_dir` — **required**, error if missing
2. Find `ca.crt` + `<host>.crt` + `<host>.key` in cert_dir
3. Create `{workdir}/wr-node/certs/` on remote
4. SCP the three files as `ca.crt`, `node.crt`, `node.key`
5. Add `peer_port` to template var map

### Systemd + Docker

- Proxy service exposes peer port alongside the main port
- Docker compose maps the peer port
- Engine/control ports only bind loopback — no need to expose

### CLI config structs (`wr-cli/src/cmd/config.rs`)

```rust
pub struct ProxyNodeConfig {
    pub proxy_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<ProxyTlsConfig>,
}
```

**Files:**
| File | Change |
|------|--------|
| `wr-cli/src/cmd/deploy_config.rs` | Add `cert_dir`, `peer_port` |
| `wr-cli/src/cmd/config.rs` | Add `peer_port`, `tls` to `ProxyNodeConfig` |
| `wr-cli/src/cmd/node.rs` | `--cert-dir`, `--peer-port`; loopback bind addresses; cert SCP; peer port in systemd/docker |

---

## Phase 8 — Test + Local Dev Ergonomics

### Test PKI helper (`wr-tests/tests/helpers.rs`)

Lazily-initialized shared PKI — cert generation happens once per test binary:

```rust
pub struct TestPki {
    pub ca_cert_der: Vec<CertificateDer<'static>>,
    pub node_cert_der: Vec<CertificateDer<'static>>,
    pub node_key_der: PrivateKeyDer<'static>,
}

pub fn generate_test_pki() -> TestPki { ... }

pub fn shared_test_pki() -> &'static TestPki {
    static PKI: OnceLock<TestPki> = OnceLock::new();
    PKI.get_or_init(generate_test_pki)
}

pub fn test_mtls_pool() -> wr_common::tls::HttpsClientPool<ProxyBody> {
    let pki = shared_test_pki();
    let config = wr_common::tls::build_client_config_from_der(...).unwrap();
    wr_common::tls::HttpsClientPool::new(2, config)
}
```

### Update all `start_proxy*` helpers

Pass `test_mtls_pool()` to `ForwardService::new`. Existing tests are unchanged — the PKI is transparent.

### Manager test helper

`start_manager` gets TLS: generate in-memory certs, build `tonic::ServerTlsConfig`, serve with TLS. `manager_client` uses matching client TLS.

### mTLS-specific test cases

1. **`test_cross_node_mtls_routing`** — two proxy nodes, request routes over mTLS peer path
2. **`test_mtls_rejects_no_client_cert`** — plain TCP to mTLS port → handshake error
3. **`test_mtls_rejects_wrong_ca`** — cert from different CA → rejected

### Local dev

```justfile
certs:
    cargo run -p wr-cli -- cert init-ca --output certs/
    cargo run -p wr-cli -- cert generate 127.0.0.1 --ca-dir certs/
    cargo run -p wr-cli -- cert generate manager --ca-dir certs/
```

All example configs reference `certs/` paths. `just certs` is a prerequisite for running examples. Add `certs/` to `.gitignore`.

**Files:**
| File | Change |
|------|--------|
| `wr-tests/Cargo.toml` | Add `rcgen = "0.13"` |
| `wr-tests/tests/helpers.rs` | `TestPki`, `shared_test_pki()`, `test_mtls_pool()`; update `start_proxy*` and `start_manager` |
| `wr-tests/tests/integration_test.rs` | mTLS test cases |
| `justfile` | Add `certs` target |
| `.gitignore` | Add `certs/` |

---

## Phase 9 — Documentation

- `CLAUDE.md` — architecture: mTLS, loopback convention, `just certs` prerequisite
- `docs/deployment.md` — `wr cert` workflow, `--cert-dir`, cert directory layout
- `docs/configuration.md` — `[node.tls]`, `[tls]` config sections, `peer_port`
- `README.md` — prerequisites: `just certs` step

---

## Implementation Order

```
Phase 1 (wr cert)    ─┐
Phase 2 (config)     ──┤
Phase 3 (tls module) ──┼─→ Phase 4 (proto) ──→ Phase 5 (proxy stack) ─┐
                       │                                                ├─→ Phase 8 (tests)
                       └─→ Phase 6 (manager TLS) ─────────────────────┘
Phase 7 (deploy)     ──┘
Phase 9 (docs) — after all other phases
```

Phases 1, 2, 7 can proceed in parallel with phase 3. Phase 5 is the critical path.

---

## Verification

1. `just certs` — generates CA + localhost + manager certs
2. `just tidy` — formatting + lints pass
3. `just test` — all tests pass (TestPki transparent)
4. `just test-wasm` — WASM host binding tests pass
5. `just ecommerce-inline` — e2e with generated certs, zero WARN lines
6. Integration tests: mTLS routing, no-cert rejection, wrong-CA rejection, manager TLS

---

## File Change Summary

| File | Change |
|------|--------|
| **wr-cli** | |
| `wr-cli/Cargo.toml` | Add `rcgen` |
| `wr-cli/src/cmd/cert.rs` | **New** — `init-ca`, `generate` |
| `wr-cli/src/cmd/mod.rs` | Add `pub mod cert;` |
| `wr-cli/src/main.rs` | Add `Cert` command; `--ca-cert`, `--client-cert`, `--client-key` global args |
| `wr-cli/src/client.rs` | TLS-aware `connect()` |
| `wr-cli/src/cmd/deploy_config.rs` | Add `cert_dir`, `peer_port` |
| `wr-cli/src/cmd/config.rs` | Add `peer_port`, `tls` to `ProxyNodeConfig` |
| `wr-cli/src/cmd/node.rs` | `--cert-dir`, `--peer-port`; loopback binds; cert SCP; peer port in generators |
| **wr-common** | |
| `wr-common/Cargo.toml` | Add `tokio-rustls`, `rustls`, `rustls-pemfile`, `hyper-rustls` |
| `wr-common/src/tls.rs` | **New** — all TLS builders, `HttpsClientPool`, gRPC TLS helpers, in-memory variants |
| `wr-common/src/lib.rs` | Add `pub mod tls;` |
| `wr-common/src/node.rs` | Add `peer_port`, `tls: TlsConfig`, `TlsConfig`, `peer_address()` |
| `wr-common/src/discovery.rs` | Add `tls_config` to `ManagerDiscovery`; TLS in `connect_new` |
| **wr-proxy** | |
| `wr-proxy/src/main.rs` | mTLS pool + acceptor; peer listener; `tls_accept_loop`; loopback warning |
| `wr-proxy/src/layers/forward.rs` | `mtls_pool` field (required); branch on `Destination` |
| `wr-proxy/src/config.rs` | TLS + peer_port validation |
| `wr-proxy/src/node_service.rs` | Use `reg.peer_address` in routing rule |
| **wr-manager** | |
| `wr-manager/src/main.rs` | TLS gRPC server |
| `wr-manager/src/config.rs` | Add `tls: TlsConfig` |
| `wr-manager/src/db.rs` | `peer_address` in queries |
| `wr-manager/migrations/` | New migration |
| **wr-engine** | |
| `wr-engine/src/main.rs` | Send `peer_address()` in registration |
| **proto** | |
| `proto/wruntime.proto` | `peer_address` on `EngineRegistration` (6) and `RoutingRule` (11) |
| **wr-tests** | |
| `wr-tests/Cargo.toml` | Add `rcgen` |
| `wr-tests/tests/helpers.rs` | `TestPki`, `shared_test_pki()`, `test_mtls_pool()`; TLS-aware `start_manager` |
| `wr-tests/tests/integration_test.rs` | mTLS test cases |
| **Config + infra** | |
| `examples/config/*.toml` | Loopback binds; `[node.tls]` / `[tls]`; `peer_port` |
| `examples/ecommerce/*.toml` | `peer_port`, `[node.tls]` |
| `examples/multi-node/**/*.toml` | Full TLS config |
| `examples/codegen/*.toml` | `peer_port`, `[node.tls]` |
| `examples/stockmarket/*.toml` | `peer_port`, `[node.tls]` |
| `justfile` | Add `certs` target |
| `.gitignore` | Add `certs/` |
| `CLAUDE.md`, `docs/deployment.md`, `docs/configuration.md` | Document mTLS |
