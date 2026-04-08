use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use http_body::Body;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

use crate::node::TlsConfig;

// ---------------------------------------------------------------------------
// Certificate loading helpers
// ---------------------------------------------------------------------------

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to parse certificates from {path}"))
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("failed to parse private key from {path}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {path}"))
}

fn build_root_store(ca_certs: &[CertificateDer<'static>]) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    for cert in ca_certs {
        store
            .add(cert.clone())
            .context("failed to add CA certificate to root store")?;
    }
    Ok(store)
}

/// Check that all TLS certificate files exist before attempting to load them.
/// Produces a single actionable error listing every missing file.
fn check_tls_files_exist(tls: &TlsConfig) -> Result<()> {
    let mut missing = Vec::new();
    for (label, path) in [
        ("cert", &tls.cert_path),
        ("key", &tls.key_path),
        ("CA cert", &tls.ca_cert_path),
    ] {
        if !std::path::Path::new(path).exists() {
            missing.push(format!("  {label}: {path}"));
        }
    }
    if !missing.is_empty() {
        anyhow::bail!(
            "TLS certificate files not found:\n{}\n\nRun `just certs` to generate local dev certificates.",
            missing.join("\n")
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Server-side (mTLS acceptor)
// ---------------------------------------------------------------------------

/// Build a `ServerConfig` that requires client certificates signed by the given CA.
pub fn build_server_config(tls: &TlsConfig) -> Result<Arc<ServerConfig>> {
    check_tls_files_exist(tls)?;
    let cert_chain = load_certs(&tls.cert_path)?;
    let private_key = load_key(&tls.key_path)?;
    let ca_certs = load_certs(&tls.ca_cert_path)?;
    build_server_config_from_der(cert_chain, private_key, &ca_certs)
}

/// Build a `ServerConfig` from in-memory DER-encoded certificates.
pub fn build_server_config_from_der(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    ca_certs: &[CertificateDer<'static>],
) -> Result<Arc<ServerConfig>> {
    let root_store = build_root_store(ca_certs)?;
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .context("failed to build client certificate verifier")?;

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, private_key)
        .context("failed to build TLS server config")?;

    Ok(Arc::new(config))
}

/// Build a `TlsAcceptor` from file-based TLS config.
pub fn build_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor> {
    Ok(TlsAcceptor::from(build_server_config(tls)?))
}

// ---------------------------------------------------------------------------
// Client-side (mTLS connector)
// ---------------------------------------------------------------------------

/// Build a `ClientConfig` with client certificate authentication.
pub fn build_client_config(tls: &TlsConfig) -> Result<ClientConfig> {
    check_tls_files_exist(tls)?;
    let cert_chain = load_certs(&tls.cert_path)?;
    let private_key = load_key(&tls.key_path)?;
    let ca_certs = load_certs(&tls.ca_cert_path)?;
    build_client_config_from_der(cert_chain, private_key, &ca_certs)
}

/// Build a `ClientConfig` from in-memory DER-encoded certificates.
pub fn build_client_config_from_der(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    ca_certs: &[CertificateDer<'static>],
) -> Result<ClientConfig> {
    let root_store = build_root_store(ca_certs)?;
    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(cert_chain, private_key)
        .context("failed to build TLS client config")
}

// ---------------------------------------------------------------------------
// HttpsClientPool — mTLS HTTP/2 client pool with round-robin selection
// ---------------------------------------------------------------------------

/// Pool of mTLS HTTP/2 clients. Spreads requests across multiple TCP
/// connections to avoid single-connection bottlenecks.
pub struct HttpsClientPool<B> {
    clients: Arc<Vec<Client<HttpsConnector<HttpConnector>, B>>>,
    next: Arc<AtomicUsize>,
}

impl<B> Clone for HttpsClientPool<B> {
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
            next: self.next.clone(),
        }
    }
}

impl<B> HttpsClientPool<B>
where
    B: Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// Create a pool of `size` mTLS HTTP/2 clients using the given `ClientConfig`.
    pub fn new(size: usize, tls_config: ClientConfig) -> Self {
        let clients: Vec<_> = (0..size)
            .map(|_| {
                let connector = hyper_rustls::HttpsConnectorBuilder::new()
                    .with_tls_config(tls_config.clone())
                    .https_only()
                    .enable_http2()
                    .build();
                Client::builder(TokioExecutor::new())
                    .http2_only(true)
                    .build(connector)
            })
            .collect();
        Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the next client via round-robin.
    pub fn get(&self) -> &Client<HttpsConnector<HttpConnector>, B> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }
}

// ---------------------------------------------------------------------------
// gRPC TLS helpers (for tonic)
// ---------------------------------------------------------------------------

/// Build a `tonic::transport::ServerTlsConfig` for a gRPC server with mTLS.
pub fn build_tonic_server_tls(tls: &TlsConfig) -> Result<tonic::transport::ServerTlsConfig> {
    check_tls_files_exist(tls)?;
    let cert_pem = std::fs::read_to_string(&tls.cert_path)
        .with_context(|| format!("failed to read cert: {}", tls.cert_path))?;
    let key_pem = std::fs::read_to_string(&tls.key_path)
        .with_context(|| format!("failed to read key: {}", tls.key_path))?;
    let ca_pem = std::fs::read_to_string(&tls.ca_cert_path)
        .with_context(|| format!("failed to read CA cert: {}", tls.ca_cert_path))?;

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let ca = tonic::transport::Certificate::from_pem(ca_pem);

    Ok(tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca))
}

/// Build a `tonic::transport::ClientTlsConfig` for a gRPC client with mTLS.
pub fn build_tonic_client_tls(tls: &TlsConfig) -> Result<tonic::transport::ClientTlsConfig> {
    check_tls_files_exist(tls)?;
    let cert_pem = std::fs::read_to_string(&tls.cert_path)
        .with_context(|| format!("failed to read cert: {}", tls.cert_path))?;
    let key_pem = std::fs::read_to_string(&tls.key_path)
        .with_context(|| format!("failed to read key: {}", tls.key_path))?;
    let ca_pem = std::fs::read_to_string(&tls.ca_cert_path)
        .with_context(|| format!("failed to read CA cert: {}", tls.ca_cert_path))?;

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let ca = tonic::transport::Certificate::from_pem(ca_pem);

    Ok(tonic::transport::ClientTlsConfig::new()
        .identity(identity)
        .ca_certificate(ca))
}
