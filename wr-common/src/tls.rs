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
use sha2::{Digest, Sha256};
use tokio_rustls::TlsAcceptor;

use crate::identity::{ClusterId, PrincipalUri};
use crate::node::{ClientTlsConfig, ServerTlsConfig, TlsConfig};
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

pub trait ServerTlsPaths {
    fn certificate_path(&self) -> &str;
    fn private_key_path(&self) -> &str;
    fn client_roots_path(&self) -> &str;
}

pub trait ClientTlsPaths {
    fn certificate_path(&self) -> &str;
    fn private_key_path(&self) -> &str;
    fn server_roots_path(&self) -> &str;
}

impl ServerTlsPaths for ServerTlsConfig {
    fn certificate_path(&self) -> &str {
        &self.cert_path
    }
    fn private_key_path(&self) -> &str {
        &self.key_path
    }
    fn client_roots_path(&self) -> &str {
        &self.client_ca_cert_path
    }
}

impl ClientTlsPaths for ClientTlsConfig {
    fn certificate_path(&self) -> &str {
        &self.cert_path
    }
    fn private_key_path(&self) -> &str {
        &self.key_path
    }
    fn server_roots_path(&self) -> &str {
        &self.server_ca_cert_path
    }
}

impl ServerTlsPaths for TlsConfig {
    fn certificate_path(&self) -> &str {
        &self.cert_path
    }
    fn private_key_path(&self) -> &str {
        &self.key_path
    }
    fn client_roots_path(&self) -> &str {
        &self.ca_cert_path
    }
}

impl ClientTlsPaths for TlsConfig {
    fn certificate_path(&self) -> &str {
        &self.cert_path
    }
    fn private_key_path(&self) -> &str {
        &self.key_path
    }
    fn server_roots_path(&self) -> &str {
        &self.ca_cert_path
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafProfile {
    Client,
    Server,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeafEvidence {
    pub profile: LeafProfile,
    pub principal: Option<PrincipalUri>,
    pub endpoint_dns_names: Vec<String>,
    pub endpoint_ip_addresses: Vec<std::net::IpAddr>,
    pub fingerprint: String,
    pub serial: String,
    pub spki_fingerprint: String,
}

/// Parse the authenticated leaf after the transport has performed stock chain
/// and validity verification. This function owns the stricter wruntime leaf
/// profile and identity rules.
pub fn validate_server_leaf(
    certificate_der: &[u8],
    expected_endpoint: &str,
) -> Result<LeafEvidence> {
    let evidence = parse_leaf_evidence(certificate_der, LeafProfile::Server, None)?;
    if let Ok(ip) = expected_endpoint.parse::<std::net::IpAddr>() {
        anyhow::ensure!(
            evidence.endpoint_ip_addresses.contains(&ip),
            "server leaf does not contain the expected IP SAN"
        );
    } else {
        anyhow::ensure!(
            evidence
                .endpoint_dns_names
                .iter()
                .any(|name| name == expected_endpoint),
            "server leaf does not contain the expected DNS SAN"
        );
    }
    Ok(evidence)
}

pub fn validate_client_leaf(
    certificate_der: &[u8],
    expected_cluster: &ClusterId,
) -> Result<LeafEvidence> {
    parse_leaf_evidence(certificate_der, LeafProfile::Client, Some(expected_cluster))
}

/// Read and validate the first client leaf from a PEM certificate profile.
pub fn load_client_leaf_evidence(
    certificate_path: &str,
    expected_cluster: Option<&ClusterId>,
) -> Result<LeafEvidence> {
    let certificates = load_certs(certificate_path)?;
    let leaf = certificates
        .first()
        .ok_or_else(|| anyhow::anyhow!("certificate chain is empty"))?;
    parse_leaf_evidence(leaf.as_ref(), LeafProfile::Client, expected_cluster)
        .with_context(|| format!("invalid client leaf profile in {certificate_path}"))
}

pub fn parse_leaf_evidence(
    certificate_der: &[u8],
    profile: LeafProfile,
    expected_cluster: Option<&ClusterId>,
) -> Result<LeafEvidence> {
    let (remaining, certificate) = X509Certificate::from_der(certificate_der)
        .map_err(|error| anyhow::anyhow!("failed to parse X.509 leaf: {error}"))?;
    anyhow::ensure!(remaining.is_empty(), "trailing bytes after X.509 leaf");
    let eku = certificate
        .extended_key_usage()
        .context("failed to parse extended key usage")?
        .ok_or_else(|| anyhow::anyhow!("leaf certificate requires explicit extended key usage"))?;
    let no_other_eku = !eku.value.any
        && !eku.value.code_signing
        && !eku.value.email_protection
        && !eku.value.time_stamping
        && !eku.value.ocsp_signing
        && eku.value.other.is_empty();
    match profile {
        LeafProfile::Client => anyhow::ensure!(
            eku.value.client_auth && !eku.value.server_auth && no_other_eku,
            "client leaf requires clientAuth only"
        ),
        LeafProfile::Server => anyhow::ensure!(
            eku.value.server_auth && !eku.value.client_auth && no_other_eku,
            "server leaf requires serverAuth only"
        ),
    }

    let san = certificate
        .subject_alternative_name()
        .context("failed to parse subject alternative name")?
        .ok_or_else(|| anyhow::anyhow!("leaf certificate requires subject alternative names"))?;
    let mut uris = Vec::new();
    let mut dns_names = Vec::new();
    let mut ip_addresses = Vec::new();
    let mut unsupported_names = 0usize;
    for name in &san.value.general_names {
        match name {
            GeneralName::URI(uri) => uris.push(*uri),
            GeneralName::DNSName(name) => dns_names.push((*name).to_string()),
            GeneralName::IPAddress(bytes) => match bytes.len() {
                4 => {
                    ip_addresses.push(std::net::IpAddr::from(<[u8; 4]>::try_from(*bytes).unwrap()))
                }
                16 => ip_addresses.push(std::net::IpAddr::from(
                    <[u8; 16]>::try_from(*bytes).unwrap(),
                )),
                _ => anyhow::bail!("endpoint SAN contains malformed IP address"),
            },
            _ => unsupported_names += 1,
        }
    }

    let principal = match profile {
        LeafProfile::Client => {
            anyhow::ensure!(uris.len() == 1, "client leaf requires exactly one URI SAN");
            anyhow::ensure!(
                dns_names.is_empty() && ip_addresses.is_empty() && unsupported_names == 0,
                "client leaf must contain only its URI SAN"
            );
            let principal = PrincipalUri::parse(uris[0])?;
            if let Some(cluster) = expected_cluster {
                anyhow::ensure!(
                    principal.cluster_id() == cluster,
                    "client principal belongs to the wrong cluster"
                );
            }
            Some(principal)
        }
        LeafProfile::Server => {
            anyhow::ensure!(
                uris.is_empty() && unsupported_names == 0,
                "server leaf must contain only endpoint DNS/IP SANs"
            );
            anyhow::ensure!(
                !dns_names.is_empty() || !ip_addresses.is_empty(),
                "server leaf requires a DNS or IP SAN"
            );
            None
        }
    };
    let spki = certificate.public_key().raw;
    Ok(LeafEvidence {
        profile,
        principal,
        endpoint_dns_names: dns_names,
        endpoint_ip_addresses: ip_addresses,
        fingerprint: certificate_fingerprint_sha256(certificate_der),
        serial: certificate.raw_serial_as_string(),
        spki_fingerprint: certificate_fingerprint_sha256(spki),
    })
}

/// Return the stable lowercase SHA-256 fingerprint used by manager principal
/// mappings. The digest covers the complete DER certificate, not a subject
/// name that a different trusted certificate could reuse.
pub fn certificate_fingerprint_sha256(certificate_der: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(certificate_der))
}

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

/// Validate an explicit root bundle without consulting platform roots or any
/// fallback path. Every certificate must be a CA permitted to sign leaves.
pub fn validate_root_bundle(path: &str) -> Result<Vec<String>> {
    let certificates = load_certs(path)?;
    anyhow::ensure!(
        !certificates.is_empty(),
        "CA bundle contains no certificates"
    );
    let mut fingerprints = Vec::with_capacity(certificates.len());
    for der in certificates {
        let (remaining, certificate) = X509Certificate::from_der(der.as_ref())
            .map_err(|error| anyhow::anyhow!("failed to parse root certificate: {error}"))?;
        anyhow::ensure!(
            remaining.is_empty(),
            "trailing bytes after root certificate"
        );
        let constraints = certificate
            .basic_constraints()
            .context("failed to parse root basic constraints")?
            .ok_or_else(|| anyhow::anyhow!("root requires basic constraints"))?;
        anyhow::ensure!(
            constraints.value.ca,
            "root bundle contains a non-CA certificate"
        );
        if let Some(usage) = certificate
            .key_usage()
            .context("failed to parse root key usage")?
        {
            anyhow::ensure!(
                usage.value.key_cert_sign(),
                "root CA lacks keyCertSign usage"
            );
        }
        fingerprints.push(certificate_fingerprint_sha256(der.as_ref()));
    }
    Ok(fingerprints)
}

/// Reject any CA certificate shared across separately authorized trust domains.
/// CA bundles are compared by complete DER fingerprint rather than path so a
/// copied or renamed trust anchor cannot collapse the boundary.
pub fn ensure_disjoint_ca_roots(domains: &[(&str, &ServerTlsConfig)]) -> Result<()> {
    let mut owners = std::collections::HashMap::new();
    for (domain, tls) in domains {
        validate_root_bundle(&tls.client_ca_cert_path)
            .with_context(|| format!("invalid {domain} CA bundle"))?;
        let certificates = load_certs(&tls.client_ca_cert_path)?;
        for certificate in certificates {
            let fingerprint = certificate_fingerprint_sha256(certificate.as_ref());
            if let Some(existing) = owners.insert(fingerprint, *domain) {
                anyhow::bail!(
                    "TLS trust domains '{existing}' and '{domain}' share a CA certificate"
                );
            }
        }
    }
    Ok(())
}

fn validate_file_leaf_profile(path: &str, profile: LeafProfile) -> Result<()> {
    let certificates = load_certs(path)?;
    let leaf = certificates
        .first()
        .ok_or_else(|| anyhow::anyhow!("certificate chain is empty"))?;
    parse_leaf_evidence(leaf.as_ref(), profile, None).map(|_| ())
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
fn check_tls_files_exist(cert_path: &str, key_path: &str, roots_path: &str) -> Result<()> {
    let mut missing = Vec::new();
    for (label, path) in [
        ("cert", cert_path),
        ("key", key_path),
        ("CA cert", roots_path),
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
pub fn build_server_config(tls: &impl ServerTlsPaths) -> Result<Arc<ServerConfig>> {
    check_tls_files_exist(
        tls.certificate_path(),
        tls.private_key_path(),
        tls.client_roots_path(),
    )?;
    validate_root_bundle(tls.client_roots_path())?;
    let cert_chain = load_certs(tls.certificate_path())?;
    let private_key = load_key(tls.private_key_path())?;
    let ca_certs = load_certs(tls.client_roots_path())?;
    build_server_config_from_der(cert_chain, private_key, &ca_certs)
}

/// Build a `ServerConfig` from in-memory DER-encoded certificates.
pub fn build_server_config_from_der(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    ca_certs: &[CertificateDer<'static>],
) -> Result<Arc<ServerConfig>> {
    let leaf = cert_chain
        .first()
        .ok_or_else(|| anyhow::anyhow!("server certificate chain is empty"))?;
    parse_leaf_evidence(leaf.as_ref(), LeafProfile::Server, None)
        .context("invalid server leaf profile")?;
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
pub fn build_acceptor(tls: &impl ServerTlsPaths) -> Result<TlsAcceptor> {
    Ok(TlsAcceptor::from(build_server_config(tls)?))
}

// ---------------------------------------------------------------------------
// Client-side (mTLS connector)
// ---------------------------------------------------------------------------

/// Build a `ClientConfig` with client certificate authentication.
pub fn build_client_config(tls: &impl ClientTlsPaths) -> Result<ClientConfig> {
    check_tls_files_exist(
        tls.certificate_path(),
        tls.private_key_path(),
        tls.server_roots_path(),
    )?;
    validate_root_bundle(tls.server_roots_path())?;
    let cert_chain = load_certs(tls.certificate_path())?;
    let private_key = load_key(tls.private_key_path())?;
    let ca_certs = load_certs(tls.server_roots_path())?;
    build_client_config_from_der(cert_chain, private_key, &ca_certs)
}

/// Build a `ClientConfig` from in-memory DER-encoded certificates.
pub fn build_client_config_from_der(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    ca_certs: &[CertificateDer<'static>],
) -> Result<ClientConfig> {
    let leaf = cert_chain
        .first()
        .ok_or_else(|| anyhow::anyhow!("client certificate chain is empty"))?;
    parse_leaf_evidence(leaf.as_ref(), LeafProfile::Client, None)
        .context("invalid client leaf profile")?;
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
pub fn build_tonic_server_tls(
    tls: &impl ServerTlsPaths,
) -> Result<tonic::transport::ServerTlsConfig> {
    check_tls_files_exist(
        tls.certificate_path(),
        tls.private_key_path(),
        tls.client_roots_path(),
    )?;
    validate_root_bundle(tls.client_roots_path())?;
    validate_file_leaf_profile(tls.certificate_path(), LeafProfile::Server)
        .context("invalid server leaf profile")?;
    let cert_pem = std::fs::read_to_string(tls.certificate_path())
        .with_context(|| format!("failed to read cert: {}", tls.certificate_path()))?;
    let key_pem = std::fs::read_to_string(tls.private_key_path())
        .with_context(|| format!("failed to read key: {}", tls.private_key_path()))?;
    let ca_pem = std::fs::read_to_string(tls.client_roots_path())
        .with_context(|| format!("failed to read CA cert: {}", tls.client_roots_path()))?;

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let ca = tonic::transport::Certificate::from_pem(ca_pem);

    Ok(tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca))
}

/// Build a `tonic::transport::ClientTlsConfig` for a gRPC client with mTLS.
pub fn build_tonic_client_tls(
    tls: &impl ClientTlsPaths,
) -> Result<tonic::transport::ClientTlsConfig> {
    check_tls_files_exist(
        tls.certificate_path(),
        tls.private_key_path(),
        tls.server_roots_path(),
    )?;
    validate_root_bundle(tls.server_roots_path())?;
    validate_file_leaf_profile(tls.certificate_path(), LeafProfile::Client)
        .context("invalid client leaf profile")?;
    let cert_pem = std::fs::read_to_string(tls.certificate_path())
        .with_context(|| format!("failed to read cert: {}", tls.certificate_path()))?;
    let key_pem = std::fs::read_to_string(tls.private_key_path())
        .with_context(|| format!("failed to read key: {}", tls.private_key_path()))?;
    let ca_pem = std::fs::read_to_string(tls.server_roots_path())
        .with_context(|| format!("failed to read CA cert: {}", tls.server_roots_path()))?;

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
    let ca = tonic::transport::Certificate::from_pem(ca_pem);

    Ok(tonic::transport::ClientTlsConfig::new()
        .identity(identity)
        .ca_certificate(ca))
}

#[cfg(test)]
mod fingerprint_tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable_lowercase_sha256() {
        assert_eq!(
            certificate_fingerprint_sha256(b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
