use std::sync::OnceLock;

pub struct TestPki {
    pub ca_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub node_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub node_key_der: rustls::pki_types::PrivateKeyDer<'static>,
}

/// Generate a CA + node cert entirely in memory. No files on disk.
pub fn generate_test_pki() -> TestPki {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    use std::net::IpAddr;

    // CA
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-ca");
    let ca_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_issuer = rcgen::Issuer::from_params(&ca_params, ca_key);

    // Node cert signed by CA
    let mut node_params = CertificateParams::new(vec![]).unwrap();
    node_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
    ];
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-node");
    let node_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let node_cert = node_params.signed_by(&node_key, &ca_issuer).unwrap();

    TestPki {
        ca_cert_der: vec![ca_cert.der().clone()],
        node_cert_der: vec![node_cert.der().clone()],
        node_key_der: rustls::pki_types::PrivateKeyDer::Pkcs8(node_key.serialize_der().into()),
    }
}

/// Lazily-initialized shared PKI — cert gen happens once per test binary.
/// Also installs the rustls crypto provider if not already set.
pub fn shared_test_pki() -> &'static TestPki {
    let _ = rustls::crypto::ring::default_provider().install_default();
    static PKI: OnceLock<TestPki> = OnceLock::new();
    PKI.get_or_init(generate_test_pki)
}

/// Build an HttpsClientPool from the shared test PKI.
pub fn test_mtls_pool() -> wr_common::tls::HttpsClientPool<wr_proxy::layers::ProxyBody> {
    let pki = shared_test_pki();
    let config = wr_common::tls::build_client_config_from_der(
        pki.node_cert_der.clone(),
        pki.node_key_der.clone_key(),
        &pki.ca_cert_der,
    )
    .unwrap();
    wr_common::tls::HttpsClientPool::new(2, config)
}
