# Proxy-to-Proxy mTLS

Encrypts and mutually authenticates all cross-node proxy traffic. Implements after
`plans/cross_node_proxy_routing.md`.

Intra-node traffic (engine → local proxy) remains plain HTTP — it is same-host loopback
and does not cross the network. Only the inter-proxy path is TLS-protected.

---

## Architecture

Each node runs two listeners:

```
engine → proxy :9001   (plain HTTP, intra-node only)
proxy  → proxy :9443   (mTLS, inter-node)
```

A shared internal CA signs one certificate per proxy. Every proxy trusts only certs
signed by that CA, enforcing mutual authentication on all peer connections.

```
Node A                               Node B
┌──────────────────────────────────┐  ┌──────────────────────────────────┐
│  proxy :9001  (plain HTTP/2)     │  │  proxy :9002  (plain HTTP/2)     │
│  proxy :9443  (mTLS HTTP/2) ◄────┼──┼──► proxy :9443  (mTLS HTTP/2)   │
│  engine A1 :9100                 │  │  engine B1 :9200                 │
└──────────────────────────────────┘  └──────────────────────────────────┘
```

The `proxy_address` field introduced in `cross_node_proxy_routing.md` splits into two:

| Field | Used by | Value |
|---|---|---|
| `node.proxy_address` | Engines (outbound rewrite), routing table identity | `http://127.0.0.1:9001` |
| `node.peer_address` | Peer proxies forwarding cross-node traffic | `https://10.0.1.5:9443` |

`RoutingRule.proxy_address` stores `peer_address` going forward. The routing layer
comparison becomes `rule.proxy_address == self.peer_address` to distinguish local from
remote.

---

## Phase 1 — Dependencies

Add to `wr-proxy/Cargo.toml`:

```toml
rustls         = "0.23"
rustls-pemfile = "2"
tokio-rustls   = "0.26"
```

`hyper-rustls` 0.27 is already present with `native-tokio`, `http1`, and `http2` features.
No changes to `wr-engine` or `wr-manager` — TLS is proxy-only.

---

## Phase 2 — Config

### `NodeConfig` (`wr-common/src/node.rs`)

Extend the existing struct:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub proxy_address: String,       // plain HTTP, used by engines (existing)
    pub peer_address: Option<String>,        // mTLS HTTPS, used by peer proxies
    pub peer_listen_address: Option<String>, // bind address for the mTLS listener
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub cert_path:    String,   // PEM: this proxy's certificate (chain)
    pub key_path:     String,   // PEM: this proxy's private key
    pub ca_cert_path: String,   // PEM: CA cert used to verify peer certs
}
```

`peer_address`, `peer_listen_address`, and `tls` are `Option` so that single-node
deployments (and `wr-engine`, which also uses `NodeConfig`) continue to work without
any TLS configuration. The proxy validates at startup that all three are set together
when any one is present.

`TlsConfig` lives in `wr-common` alongside `NodeConfig` so it can be referenced from
tests without duplicating the type.

### `ProxyConfig` validation (`wr-proxy/src/config.rs`)

Add validation to the existing `validate()` method:

```rust
// If any peer/tls field is set, all must be set
let has_peer = self.node.peer_address.is_some();
let has_listen = self.node.peer_listen_address.is_some();
let has_tls = self.node.tls.is_some();
anyhow::ensure!(
    (has_peer && has_listen && has_tls) || (!has_peer && !has_listen && !has_tls),
    "node.peer_address, node.peer_listen_address, and node.tls must all be set together"
);
```

### `proxy.toml`

```toml
listen_address  = "0.0.0.0:9001"
manager_address = "http://127.0.0.1:9000"

[node]
proxy_address       = "http://127.0.0.1:9001"
peer_address        = "https://127.0.0.1:9443"
peer_listen_address = "0.0.0.0:9443"

[node.tls]
cert_path    = "certs/proxy-a.crt"
key_path     = "certs/proxy-a.key"
ca_cert_path = "certs/ca.crt"

[cache]
# ...
```

Engine config is unchanged — engines only reference `node.proxy_address`.

---

## Phase 3 — Proto: `proxy_address` stores the peer address

`RoutingRule.proxy_address` (field 10) now holds `peer_address` (`https://...`) rather
than the plain HTTP address. No proto field additions are needed; only the value
semantics change.

Update the engine registration in `wr-engine/src/main.rs` to send
`config.node.peer_address` (read from the engine's config, which mirrors the proxy's
peer address for its node) rather than `config.node.proxy_address`. The engine config
gains a `peer_address` field alongside `proxy_address`:

```toml
# engine.toml
[node]
proxy_address = "http://127.0.0.1:9001"   # outbound rewrite target (unchanged)
peer_address  = "https://127.0.0.1:9443"  # stored in routing rules for peer proxies
```

```rust
// wr-engine/src/main.rs — RoutingRule upsert
RoutingRule {
    proxy_address: config.node.peer_address.clone()
        .unwrap_or_else(|| config.node.proxy_address.clone()),
    ..
}
```

Update `RoutingLayer::new()` in `wr-proxy/src/layers/routing.rs` — currently receives
`config.node.proxy_address` for local-vs-remote comparison. Change to accept
`config.node.peer_address` (falling back to `proxy_address` when mTLS is not
configured):

```rust
// wr-proxy/src/main.rs — both internal and external service stacks
let self_address = config.node.peer_address.clone()
    .unwrap_or_else(|| config.node.proxy_address.clone());
RoutingLayer::new(routing_table.clone(), self_address)
```

---

## Phase 4 — TLS server: mTLS listener in the proxy

### `wr-proxy/src/tls.rs` (new file)

Encapsulate certificate loading and acceptor construction:

```rust
use rustls::{ServerConfig, RootCertStore};
use rustls::server::WebPkiClientVerifier;
use rustls_pemfile::{certs, private_key};
use tokio_rustls::TlsAcceptor;

pub fn build_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor> {
    let cert_chain = load_certs(&tls.cert_path)?;
    let private_key = load_key(&tls.key_path)?;
    let ca_cert    = load_certs(&tls.ca_cert_path)?;

    let mut root_store = RootCertStore::empty();
    for cert in &ca_cert {
        root_store.add(cert.clone())?;
    }

    // Require and verify client certificate against the CA
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()?;

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, private_key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
```

### `wr-proxy/src/main.rs`

Spawn a second accept loop alongside the existing ones (internal + optional external).
The existing `accept_loop` function is generic over the TCP stream, so we wrap the
TLS-accepted stream in `TokioIo` before passing it in. Both loops use the same
`http2::Builder` and same Tower service stack:

```rust
// Only start the mTLS listener when TLS is configured
if let (Some(peer_listen), Some(tls_config)) =
    (&config.node.peer_listen_address, &config.node.tls)
{
    let tls_acceptor = wr_proxy::tls::build_acceptor(tls_config)?;
    let tls_listener = TcpListener::bind(peer_listen).await?;
    info!(address = %peer_listen, "proxy listening (mTLS peer)");

    // Clone the internal service stack — both paths use the same layers
    let peer_svc = internal_svc.clone();
    tokio::spawn(tls_accept_loop(tls_listener, tls_acceptor, peer_svc));
}
```

A new `tls_accept_loop` function mirrors the existing `accept_loop` but wraps each
connection through the `TlsAcceptor` before handing off to `http2::Builder`:

```rust
async fn tls_accept_loop<S>(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    svc: S,
) where
    S: Service<Request<Bytes>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => { warn!(error = %e, "tls accept error"); continue; }
        };
        let acceptor = acceptor.clone();
        let svc = svc.clone();

        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => { warn!(peer = %peer_addr, error = %e, "TLS handshake failed"); return; }
            };
            let io = TokioIo::new(tls_stream);
            // Same hyper http2::Builder as the plain accept_loop
            // ... serve_connection(io, hyper_svc) ...
        });
    }
}
```

The two loops share the same `svc` (Tower stack). The `x-wr-via-proxy` header is already
set by `ForwardService` for cross-node hops, and the `EgressLayer` / `SchemaValidationLayer`
already respect it. No additional server-side branching is required — mTLS authentication
is handled entirely by the TLS handshake.

---

## Phase 5 — TLS client: mTLS connector for outbound peer requests

### `wr-proxy/src/tls.rs`

```rust
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use rustls::ClientConfig;

pub fn build_peer_client(
    tls: &TlsConfig,
) -> Result<Client<HttpsConnector<HttpConnector>, Full<Bytes>>> {
    let cert_chain  = load_certs(&tls.cert_path)?;
    let private_key = load_key(&tls.key_path)?;
    let ca_cert     = load_certs(&tls.ca_cert_path)?;

    let mut root_store = RootCertStore::empty();
    for cert in &ca_cert {
        root_store.add(cert.clone())?;
    }

    // Send client cert (mTLS) and verify server cert against the CA
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(cert_chain, private_key)?;

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_only()
        .enable_http2()
        .build();

    Ok(Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build(connector))
}
```

### `wr-proxy/src/layers/forward.rs`

`ForwardService` gains an optional peer client. Selection is based on the `Destination`
variant already present in `wr-proxy/src/layers/mod.rs`:

```rust
#[derive(Clone)]
pub struct ForwardService {
    client: Client<HttpConnector, Full<Bytes>>,
    peer_client: Option<Client<HttpsConnector<HttpConnector>, Full<Bytes>>>,
    cb_registry: Arc<CircuitBreakerRegistry>,
}

impl ForwardService {
    pub fn new(
        cb_registry: Arc<CircuitBreakerRegistry>,
        peer_client: Option<Client<HttpsConnector<HttpConnector>, Full<Bytes>>>,
    ) -> Self {
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build_http();
        Self { client, peer_client, cb_registry }
    }
}
```

In `call()`, the existing candidate loop selects the client based on destination type.
The circuit breaker logic (`cb_registry`) applies identically to both clients:

```rust
// Inside the candidate loop (replaces the single client.request call):
let resp = match destination {
    Destination::LocalEngine(_) => {
        client.request(forward_req).await
    }
    Destination::RemoteProxy(_) => {
        let peer = peer_client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("peer TLS client not configured"))?;
        peer.request(forward_req).await
    }
};
```

Update both `ForwardService::new(...)` call sites in `main.rs` (internal and external
stacks) to pass the optional peer client:

```rust
let peer_client = config.node.tls.as_ref()
    .map(|tls| tls::build_peer_client(tls))
    .transpose()?;

// ... in both service stacks:
.service(ForwardService::new(cb_registry.clone(), peer_client.clone()))
```

---

## Phase 6 — Certificate setup for local simulation

For the local two-node test setup in `examples/multi-node/`, a small script generates a
local CA and two proxy certs using `openssl`. Commit the script alongside the `node-a/`
and `node-b/` config directories:

```bash
# scripts/gen-local-certs.sh
set -e
mkdir -p certs

# CA
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -days 3650 -nodes -keyout certs/ca.key -out certs/ca.crt \
  -subj "/CN=wruntime-local-ca"

# Per-proxy cert helper
gen_cert() {
  local name=$1
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
    -nodes -keyout certs/${name}.key -out certs/${name}.csr \
    -subj "/CN=${name}"
  openssl x509 -req -in certs/${name}.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -days 3650 -out certs/${name}.crt
}

gen_cert proxy-a
gen_cert proxy-b
```

Add a Justfile target:

```justfile
certs:
    bash scripts/gen-local-certs.sh
```

Config files reference the generated paths:

```toml
# node-a/proxy.toml
[node.tls]
cert_path    = "certs/proxy-a.crt"
key_path     = "certs/proxy-a.key"
ca_cert_path = "certs/ca.crt"

# node-b/proxy.toml
[node.tls]
cert_path    = "certs/proxy-b.crt"
key_path     = "certs/proxy-b.key"
ca_cert_path = "certs/ca.crt"
```

---

## Phase 7 — Integration test support

Tests use ephemeral self-signed certs generated at runtime via `rcgen` — no files on
disk, no dependency on the `scripts/gen-local-certs.sh` output.

Add to `wr-tests/Cargo.toml`:

```toml
rcgen = "0.13"
```

### `wr-tests/tests/helpers.rs` — new helper:

```rust
pub struct TestPki {
    pub ca_cert_der:     CertificateDer<'static>,
    pub proxy_cert_der:  CertificateDer<'static>,
    pub proxy_key_der:   PrivateKeyDer<'static>,
}

/// Generate a CA and a signed proxy cert entirely in memory.
pub fn generate_test_pki() -> TestPki {
    let ca_params   = CertificateParams::new(vec!["wruntime-test-ca".into()]);
    let ca_cert     = Certificate::from_params(ca_params).unwrap();

    let mut params  = CertificateParams::new(vec!["127.0.0.1".into()]);
    params.is_ca    = IsCa::NoCa;
    let proxy_cert  = Certificate::from_params(params).unwrap();
    let proxy_cert_signed = proxy_cert.serialize_der_with_signer(&ca_cert).unwrap();

    TestPki {
        ca_cert_der:    ca_cert.serialize_der().unwrap().into(),
        proxy_cert_der: proxy_cert_signed.into(),
        proxy_key_der:  proxy_cert.serialize_private_key_der().into(),
    }
}
```

`build_acceptor` and `build_peer_client` in `wr-proxy/src/tls.rs` need an in-memory
variant that accepts `CertificateDer` directly instead of file paths — either a second
constructor or a builder that takes pre-loaded cert bytes.

Add a `test_cross_node_mtls` integration test that verifies:
1. A request from node A to a module on node B succeeds over the mTLS path.
2. A connection attempt with no client certificate is rejected (TLS handshake error).
3. A connection attempt with a cert signed by a different CA is rejected.

---

## Implementation order

1. **Phase 1** — add `rustls` / `rustls-pemfile` / `tokio-rustls` dependencies
2. **Phase 2** — extend `NodeConfig` with optional `peer_address`, `peer_listen_address`, `TlsConfig`; update `ProxyConfig` validation; update TOML examples
3. **Phase 3** — engine sends `peer_address` as `proxy_address` in routing rules; update `RoutingLayer` self-address
4. **Phase 4** — `wr-proxy/src/tls.rs` (acceptor), `tls_accept_loop` in `main.rs`
5. **Phase 5** — `build_peer_client`, optional dual-client `ForwardService`
6. **Phase 6** — cert generation script, Justfile `certs` target, multi-node config files
7. **Phase 7** — `rcgen` test helper, `test_cross_node_mtls` cases

---

## File change summary

| File | Change |
|---|---|
| `wr-proxy/Cargo.toml` | Add `rustls`, `rustls-pemfile`, `tokio-rustls` |
| `wr-common/src/node.rs` | Add optional `peer_address`, `peer_listen_address`, `tls: Option<TlsConfig>` to `NodeConfig` |
| `wr-proxy/src/config.rs` | Add peer/TLS validation to `validate()` |
| `wr-proxy/src/tls.rs` | **New** — `build_acceptor`, `build_peer_client`, cert loading helpers |
| `wr-proxy/src/main.rs` | Conditional mTLS listener via `tls_accept_loop`; pass `peer_client` to `ForwardService` |
| `wr-proxy/src/layers/forward.rs` | Add optional `peer_client` field; branch client selection on `Destination` variant |
| `wr-proxy/src/layers/routing.rs` | Accept `peer_address` for self-identification (fallback to `proxy_address`) |
| `wr-engine/src/main.rs` | Send `peer_address` in `RoutingRule.proxy_address` when configured |
| `examples/config/proxy.toml` | Add `[node.tls]` section |
| `examples/multi-node/node-a/proxy.toml` | Add `peer_address`, `peer_listen_address`, `[node.tls]` |
| `examples/multi-node/node-b/proxy.toml` | Add `peer_address`, `peer_listen_address`, `[node.tls]` |
| `scripts/gen-local-certs.sh` | **New** — generate local CA + proxy certs |
| `justfile` | Add `certs` target |
| `wr-tests/Cargo.toml` | Add `rcgen` |
| `wr-tests/tests/helpers.rs` | Add `TestPki`, `generate_test_pki()` |
