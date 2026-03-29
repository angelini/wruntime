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
┌──────────────────────────┐         ┌──────────────────────────┐
│  proxy :9001  (plain)    │         │  proxy :9002  (plain)    │
│  proxy :9443  (mTLS) ◄───┼─────────┼─► proxy :9443  (mTLS)   │
│  engine A1 :9100         │         │  engine B1 :9200         │
└──────────────────────────┘         └──────────────────────────┘
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
hyper-rustls   = { version = "0.27", features = ["http1", "http2"] }
```

No changes to `wr-engine` or `wr-manager` — TLS is proxy-only.

---

## Phase 2 — Config

### `NodeConfig` (`wr-common/src/node.rs`)

Extend the struct introduced in `cross_node_proxy_routing.md`:

```rust
#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub proxy_address: String,       // plain HTTP, used by engines
    pub peer_address: String,        // mTLS HTTPS, used by peer proxies
    pub peer_listen_address: String, // bind address for the mTLS listener
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize)]
pub struct TlsConfig {
    pub cert_path:    String,   // PEM: this proxy's certificate (chain)
    pub key_path:     String,   // PEM: this proxy's private key
    pub ca_cert_path: String,   // PEM: CA cert used to verify peer certs
}
```

`TlsConfig` lives in `wr-common` alongside `NodeConfig` so it can be referenced from
tests without duplicating the type.

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

`RoutingRule.proxy_address` (field 10, added in `cross_node_proxy_routing.md`) now holds
`peer_address` (`https://...`) rather than the plain HTTP address. No field additions are
needed; only the value semantics change.

Update the engine registration in `wr-engine/src/main.rs` to send `config.node.peer_address`
(read from the engine's config, which it gets from the proxy's peer address for its node)
rather than `config.node.proxy_address`. The engine config gains a `peer_address` field
alongside `proxy_address`:

```toml
# engine.toml
[node]
proxy_address = "http://127.0.0.1:9001"   # outbound rewrite target (unchanged)
peer_address  = "https://127.0.0.1:9443"  # stored in routing rules for peer proxies
```

```rust
// wr-engine/src/main.rs — RoutingRule upsert
RoutingRule {
    proxy_address: config.node.peer_address.clone(),  // was proxy_address
    ..
}
```

---

## Phase 4 — TLS server: mTLS listener in the proxy

**`wr-proxy/src/tls.rs`** (new file)

Encapsulate certificate loading and acceptor construction:

```rust
use rustls::{ServerConfig, RootCertStore};
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

**`wr-proxy/src/main.rs`**

Spawn a second accept loop alongside the existing plain HTTP one. Both loops feed into
the same Tower service stack:

```rust
// Existing plain HTTP loop (engines)
let plain_listener = TcpListener::bind(&config.listen_address).await?;

// New mTLS loop (peer proxies)
let tls_listener = TcpListener::bind(&config.node.peer_listen_address).await?;
let tls_acceptor = wr_proxy::tls::build_acceptor(&config.node.tls)?;

tokio::spawn(async move {
    loop {
        let (stream, _) = tls_listener.accept().await?;
        let tls_stream  = tls_acceptor.accept(stream).await?;
        let io = TokioIo::new(tls_stream);
        // same hyper http1::Builder serving as the plain loop
        tokio::spawn(async move {
            http1::Builder::new().serve_connection(io, svc).await
        });
    }
});
```

The two loops share the same `svc` (Tower stack). The only behavioral difference on the
server side is that `x-wr-via-proxy` skips schema validation, which the forwarding layer
already sets (from `cross_node_proxy_routing.md` Phase 6). No additional server-side
branching is required — mTLS authentication is handled entirely by the TLS handshake.

---

## Phase 5 — TLS client: mTLS connector for outbound peer requests

**`wr-proxy/src/tls.rs`**

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
        .with_root_certificates(Arc::new(root_store))
        .with_client_auth_cert(cert_chain, private_key)?;

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_only()
        .enable_http1()
        .build();

    Ok(Client::builder(TokioExecutor::new()).build(connector))
}
```

**`wr-proxy/src/layers/forward.rs`**

`ForwardService` holds both clients. Selection is based on the `Destination` variant
already introduced in `cross_node_proxy_routing.md`:

```rust
pub struct ForwardService {
    plain_client: Client<HttpConnector,  Full<Bytes>>,
    peer_client:  Client<HttpsConnector, Full<Bytes>>,
}

impl ForwardService {
    pub fn new(tls: &TlsConfig) -> Result<Self> {
        Ok(Self {
            plain_client: Client::builder(TokioExecutor::new()).build_http(),
            peer_client:  build_peer_client(tls)?,
        })
    }
}

// In call():
match dest {
    Destination::LocalEngine(addr) => {
        self.plain_client.request(build_req(addr, req)).await
    }
    Destination::RemoteProxy(addr) => {
        // addr is "https://..." — plain_client would reject it
        self.peer_client.request(build_req(addr, req)).await
    }
}
```

---

## Phase 6 — Certificate setup for local simulation

For the local two-node test setup from `cross_node_proxy_routing.md` (Phase 8), a small
script generates a local CA and two proxy certs using `openssl`. Commit the script and
the resulting certs alongside the `node-a/` and `node-b/` config directories:

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

**`wr-tests/tests/helpers.rs`** — new helper:

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

`start_node` (from `cross_node_proxy_routing.md` Phase 7) gains a `pki: &TestPki`
parameter and passes the in-memory certs to `build_acceptor` and `build_peer_client`
via an in-memory variant that accepts `CertificateDer` directly instead of file paths.

Add a `test_cross_node_mtls` integration test that verifies:
1. A request from node A to a module on node B succeeds over the mTLS path.
2. A connection attempt with no client certificate is rejected (TLS handshake error).
3. A connection attempt with a cert signed by a different CA is rejected.

---

## Implementation order

1. Phase 1 — add rustls/hyper-rustls/tokio-rustls dependencies
2. Phase 2 — extend `NodeConfig` and `TlsConfig`; update `proxy.toml` and `engine.toml` shapes
3. Phase 3 — engine sends `peer_address` as `proxy_address` in routing rules
4. Phase 4 — `wr-proxy/src/tls.rs`, second accept loop in `main.rs`
5. Phase 5 — `build_peer_client`, dual-client `ForwardService`
6. Phase 6 — cert generation script, Justfile `certs` target, local config files
7. Phase 7 — `rcgen` test helper, `test_cross_node_mtls` cases
