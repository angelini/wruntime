use std::sync::OnceLock;

#[derive(Clone)]
pub struct TonicTestIdentity {
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
}

pub struct RoleTestPki {
    pub ca_pem: String,
    pub server: TonicTestIdentity,
    pub viewer: TonicTestIdentity,
    pub operator: TonicTestIdentity,
    pub operator_rotated: TonicTestIdentity,
    pub agent_a: TonicTestIdentity,
    pub agent_b: TonicTestIdentity,
    pub unknown: TonicTestIdentity,
}

/// Generate a CA, one localhost server identity, and distinct mTLS role
/// identities. Fingerprints are over the exact DER presented to tonic.
pub fn generate_role_test_pki() -> RoleTestPki {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
    };
    use std::net::IpAddr;

    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "role-test-ca");
    let ca_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem();
    let issuer = rcgen::Issuer::from_params(&ca_params, ca_key);

    let identity = |name: &str, uri: Option<&str>| {
        let mut params = CertificateParams::new(vec![]).unwrap();
        if let Some(uri) = uri {
            params.subject_alt_names = vec![SanType::URI(uri.try_into().unwrap())];
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        } else {
            params.subject_alt_names = vec![
                SanType::DnsName("localhost".try_into().unwrap()),
                SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
            ];
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        }
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();
        TonicTestIdentity {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            fingerprint: wr_common::tls::certificate_fingerprint_sha256(cert.der().as_ref()),
        }
    };

    RoleTestPki {
        ca_pem,
        server: identity("manager", None),
        viewer: identity("viewer", Some("urn:wruntime:cluster-a:human:viewer-a")),
        operator: identity("operator", Some("urn:wruntime:cluster-a:human:operator-a")),
        operator_rotated: identity(
            "operator-rotated",
            Some("urn:wruntime:cluster-a:human:operator-a"),
        ),
        agent_a: identity("agent-a", Some("urn:wruntime:cluster-a:node-agent:node-a")),
        agent_b: identity("agent-b", Some("urn:wruntime:cluster-a:node-agent:node-b")),
        unknown: identity("unknown", Some("urn:wruntime:cluster-a:human:unknown")),
    }
}

pub struct TestPki {
    pub ca_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub server_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub server_key_der: rustls::pki_types::PrivateKeyDer<'static>,
    pub node_cert_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub node_key_der: rustls::pki_types::PrivateKeyDer<'static>,
}

/// Generate a CA + node cert entirely in memory. No files on disk.
pub struct TestPkiFiles {
    _directory: tempfile::TempDir,
    pub server_tls: wr_common::node::ServerTlsConfig,
    pub client_tls: wr_common::node::ClientTlsConfig,
}

/// Generate a standalone CA with distinct localhost serverAuth and clientAuth leaves.
pub fn generate_test_pki_files(name: &str) -> TestPkiFiles {
    let principal = match name {
        "operator-admin" => "urn:wruntime:cluster-a:human:operator-admin",
        "runtime" | "overlapping-runtime" => "urn:wruntime:cluster-a:proxy:proxy-a",
        _ => "urn:wruntime:cluster-a:manager:manager-a",
    };
    generate_test_pki_files_for_principal(name, principal)
}

pub fn generate_test_pki_files_for_principal(name: &str, principal: &str) -> TestPkiFiles {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
    };
    use std::net::IpAddr;

    let directory = tempfile::tempdir().unwrap();
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("{name}-ca"));
    let ca_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_issuer = rcgen::Issuer::from_params(&ca_params, ca_key);

    let mut server_params = CertificateParams::new(vec![]).unwrap();
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
    ];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("{name}-server"));
    let server_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let server_cert = server_params.signed_by(&server_key, &ca_issuer).unwrap();

    let mut client_params = CertificateParams::new(vec![]).unwrap();
    client_params.subject_alt_names = vec![SanType::URI(principal.try_into().unwrap())];
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("{name}-client"));
    let client_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let client_cert = client_params.signed_by(&client_key, &ca_issuer).unwrap();

    let ca_path = directory.path().join("ca.crt");
    let server_cert_path = directory.path().join("server.crt");
    let server_key_path = directory.path().join("server.key");
    let client_cert_path = directory.path().join("client.crt");
    let client_key_path = directory.path().join("client.key");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_path, client_cert.pem()).unwrap();
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();

    let ca_cert_path = ca_path.to_string_lossy().into_owned();
    TestPkiFiles {
        server_tls: wr_common::node::ServerTlsConfig {
            cert_path: server_cert_path.to_string_lossy().into_owned(),
            key_path: server_key_path.to_string_lossy().into_owned(),
            client_ca_cert_path: ca_cert_path.clone(),
        },
        client_tls: wr_common::node::ClientTlsConfig {
            cert_path: client_cert_path.to_string_lossy().into_owned(),
            key_path: client_key_path.to_string_lossy().into_owned(),
            server_ca_cert_path: ca_cert_path,
        },
        _directory: directory,
    }
}

pub fn generate_test_pki() -> TestPki {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
    };
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

    let mut server_params = CertificateParams::new(vec![]).unwrap();
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
    ];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let server_cert = server_params.signed_by(&server_key, &ca_issuer).unwrap();

    let mut node_params = CertificateParams::new(vec![]).unwrap();
    node_params.subject_alt_names = vec![SanType::URI(
        "urn:wruntime:cluster-a:proxy:proxy-a".try_into().unwrap(),
    )];
    node_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-node");
    let node_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let node_cert = node_params.signed_by(&node_key, &ca_issuer).unwrap();

    TestPki {
        ca_cert_der: vec![ca_cert.der().clone()],
        server_cert_der: vec![server_cert.der().clone()],
        server_key_der: rustls::pki_types::PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
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
