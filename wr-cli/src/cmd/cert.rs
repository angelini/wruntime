use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::IpAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x509_parser::pem::parse_x509_pem;
use x509_parser::prelude::{FromDer, X509Certificate};

#[derive(Args)]
pub struct CertArgs {
    #[command(subcommand)]
    pub command: CertCommand,
}

#[derive(Subcommand)]
pub enum CertCommand {
    /// Create an explicit owner-controlled server or client root.
    InitRoot(InitRootArgs),
    /// Issue one immutable profile-specific credential set.
    Issue(IssueArgs),
    /// Validate an installed credential set and retry its parent fsync.
    Verify(VerifyArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum RootKind {
    Server,
    Client,
}

#[derive(Args)]
pub struct InitRootArgs {
    #[arg(value_enum)]
    kind: RootKind,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum CertificateProfile {
    ManagerEndpoint,
    ProxyPeerEndpoint,
    EngineAdminEndpoint,
    Human,
    ServiceAccount,
    Manager,
    Proxy,
    NodeAgent,
}

impl CertificateProfile {
    fn principal_kind(self) -> Option<wr_common::identity::PrincipalKind> {
        use wr_common::identity::PrincipalKind;
        match self {
            Self::Human => Some(PrincipalKind::Human),
            Self::ServiceAccount => Some(PrincipalKind::ServiceAccount),
            Self::Manager => Some(PrincipalKind::Manager),
            Self::Proxy => Some(PrincipalKind::Proxy),
            Self::NodeAgent => Some(PrincipalKind::NodeAgent),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ManagerEndpoint => "manager-endpoint",
            Self::ProxyPeerEndpoint => "proxy-peer-endpoint",
            Self::EngineAdminEndpoint => "engine-admin-endpoint",
            Self::Human => "human",
            Self::ServiceAccount => "service-account",
            Self::Manager => "manager",
            Self::Proxy => "proxy",
            Self::NodeAgent => "node-agent",
        }
    }
}

#[derive(Args)]
pub struct IssueArgs {
    #[arg(value_enum)]
    profile: CertificateProfile,
    /// Client cluster identity; forbidden for server profiles.
    #[arg(long)]
    cluster_id: Option<String>,
    /// Client principal name; forbidden for server profiles.
    #[arg(long)]
    name: Option<String>,
    /// Server endpoint DNS name; required for server profiles.
    #[arg(long)]
    endpoint: Option<String>,
    /// Additional endpoint IP SANs for server profiles.
    #[arg(long = "ip")]
    ips: Vec<IpAddr>,
    /// Directory containing the matching explicit root ca.crt and ca.key.
    #[arg(long)]
    ca_dir: PathBuf,
    /// Absent final immutable credential-set directory.
    #[arg(long)]
    destination: PathBuf,
}

#[derive(Args)]
pub struct VerifyArgs {
    path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CredentialMetadata {
    schema_version: u32,
    profile: String,
    subject: String,
    leaf_fingerprint: String,
    serial: String,
    issuer_display_fingerprint: String,
    spki_fingerprint: String,
    not_before_unix: i64,
    not_after_unix: i64,
    file_digests: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct CredentialSetContent {
    pub profile: String,
    pub subject: String,
    pub key_pem: String,
    pub leaf_pem: String,
    pub chain_pem: String,
    pub leaf_der: Vec<u8>,
    pub issuer_der: Vec<u8>,
}

#[derive(Debug)]
pub enum InstallOutcome {
    NotInstalled {
        error: anyhow::Error,
    },
    InstalledDurable {
        path: PathBuf,
        fingerprint: String,
    },
    InstalledDurabilityUnknown {
        path: PathBuf,
        fingerprint: String,
        error: anyhow::Error,
    },
    ExistingVerified {
        path: PathBuf,
        fingerprint: String,
    },
}

pub fn run(args: CertArgs) -> Result<()> {
    match args.command {
        CertCommand::InitRoot(args) => init_root(args),
        CertCommand::Issue(args) => issue(args),
        CertCommand::Verify(args) => {
            let metadata = verify_credential_set(&args.path)?;
            println!(
                "verified {} ({})",
                args.path.display(),
                metadata.leaf_fingerprint
            );
            Ok(())
        }
    }
}

fn init_root(args: InitRootArgs) -> Result<()> {
    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("creating root directory {}", args.output.display()))?;
    std::fs::set_permissions(&args.output, std::fs::Permissions::from_mode(0o700))?;
    let cert_path = args.output.join("ca.crt");
    let key_path = args.output.join("ca.key");
    if cert_path.exists() || key_path.exists() {
        bail!("root output already exists");
    }
    let mut params = CertificateParams::new(vec![])?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        format!(
            "wruntime-{}-root",
            match args.kind {
                RootKind::Server => "server",
                RootKind::Client => "client",
            }
        ),
    );
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(3650);
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let cert = params.self_signed(&key)?;
    write_sync_new(&cert_path, cert.pem().as_bytes(), 0o644)?;
    write_sync_new(&key_path, key.serialize_pem().as_bytes(), 0o600)?;
    File::open(&args.output)?.sync_all()?;
    Ok(())
}

fn issue(args: IssueArgs) -> Result<()> {
    let ca_cert_pem = std::fs::read_to_string(args.ca_dir.join("ca.crt"))
        .context("reading explicit root ca.crt")?;
    let ca_key_pem = std::fs::read_to_string(args.ca_dir.join("ca.key"))
        .context("reading explicit root ca.key")?;
    let (_, issuer_pem) = parse_x509_pem(ca_cert_pem.as_bytes())
        .map_err(|error| anyhow::anyhow!("parsing root certificate: {error}"))?;
    let issuer_der = issuer_pem.contents;
    let ca_key = KeyPair::from_pem(&ca_key_pem).context("parsing root private key")?;
    ensure_spki_matches(&issuer_der, &ca_key)?;
    let issuer = Issuer::from_ca_cert_pem(&ca_cert_pem, ca_key)
        .context("root must be a valid CA certificate")?;

    let (subject, sans, eku) = if let Some(kind) = args.profile.principal_kind() {
        if args.endpoint.is_some() || !args.ips.is_empty() {
            bail!("client profiles do not accept endpoint SANs");
        }
        let cluster = wr_common::identity::ClusterId::parse(
            args.cluster_id
                .context("client profile requires --cluster-id")?,
        )?;
        let name = wr_common::identity::PrincipalName::parse(
            args.name.context("client profile requires --name")?,
        )?;
        let principal = wr_common::identity::PrincipalUri::new(cluster, kind, name).to_string();
        (
            principal.clone(),
            vec![SanType::URI(
                principal.try_into().context("invalid URI SAN")?,
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        )
    } else {
        if args.cluster_id.is_some() || args.name.is_some() {
            bail!("server profiles do not accept client identity fields");
        }
        let endpoint = args
            .endpoint
            .context("server profile requires --endpoint")?;
        let mut sans = vec![SanType::DnsName(
            endpoint
                .clone()
                .try_into()
                .context("invalid endpoint DNS SAN")?,
        )];
        sans.extend(args.ips.into_iter().map(SanType::IpAddress));
        (endpoint, sans, ExtendedKeyUsagePurpose::ServerAuth)
    };

    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let mut params = CertificateParams::new(vec![])?;
    params.subject_alt_names = sans;
    params.extended_key_usages = vec![eku];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, subject.as_str());
    params.not_before = time::OffsetDateTime::now_utc() - time::Duration::minutes(5);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(397);
    let cert = params.signed_by(&key, &issuer)?;
    let content = CredentialSetContent {
        profile: args.profile.as_str().to_string(),
        subject,
        key_pem: key.serialize_pem(),
        leaf_pem: cert.pem(),
        chain_pem: ca_cert_pem,
        leaf_der: cert.der().to_vec(),
        issuer_der,
    };
    match install_credential_set(&args.destination, &content) {
        InstallOutcome::InstalledDurable { path, fingerprint }
        | InstallOutcome::ExistingVerified { path, fingerprint } => {
            println!("installed {} ({fingerprint})", path.display());
            Ok(())
        }
        InstallOutcome::InstalledDurabilityUnknown { path, fingerprint, error } => bail!(
            "credential set is visible at {} ({fingerprint}) but parent durability is unknown: {error}; run cert verify",
            path.display()
        ),
        InstallOutcome::NotInstalled { error } => Err(error),
    }
}

pub fn install_credential_set(final_path: &Path, content: &CredentialSetContent) -> InstallOutcome {
    install_credential_set_with_parent_sync(final_path, content, |parent| {
        File::open(parent)?.sync_all()
    })
}

fn install_credential_set_with_parent_sync(
    final_path: &Path,
    content: &CredentialSetContent,
    parent_sync: impl FnOnce(&Path) -> std::io::Result<()>,
) -> InstallOutcome {
    let operation = || -> Result<(PathBuf, String)> {
        let parent = final_path
            .parent()
            .context("credential destination requires a parent")?;
        std::fs::create_dir_all(parent)?;
        if final_path.exists() {
            let metadata = verify_credential_set_without_parent_sync(final_path)?;
            return Ok((PathBuf::new(), metadata.leaf_fingerprint));
        }
        let name = final_path
            .file_name()
            .context("credential destination requires a name")?
            .to_string_lossy();
        let staging = parent.join(format!(".{name}.staging-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&staging)?;
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o700))?;
        let result = (|| -> Result<String> {
            let evidence = wr_common::tls::parse_leaf_evidence(
                &content.leaf_der,
                if content.profile.ends_with("endpoint") {
                    wr_common::tls::LeafProfile::Server
                } else {
                    wr_common::tls::LeafProfile::Client
                },
                None,
            )?;
            let key = KeyPair::from_pem(&content.key_pem)?;
            ensure_spki_matches(&content.leaf_der, &key)?;
            let mut file_digests = BTreeMap::new();
            for (name, bytes, mode) in [
                ("key.pem", content.key_pem.as_bytes(), 0o600),
                ("leaf.pem", content.leaf_pem.as_bytes(), 0o644),
                ("chain.pem", content.chain_pem.as_bytes(), 0o644),
            ] {
                write_sync_new(&staging.join(name), bytes, mode)?;
                file_digests.insert(name.to_string(), digest(bytes));
            }
            let (_, certificate) = X509Certificate::from_der(&content.leaf_der)
                .map_err(|error| anyhow::anyhow!("parsing issued leaf: {error}"))?;
            let metadata = CredentialMetadata {
                schema_version: 1,
                profile: content.profile.clone(),
                subject: content.subject.clone(),
                leaf_fingerprint: evidence.fingerprint.clone(),
                serial: evidence.serial,
                issuer_display_fingerprint: digest(&content.issuer_der),
                spki_fingerprint: evidence.spki_fingerprint,
                not_before_unix: certificate.validity().not_before.timestamp(),
                not_after_unix: certificate.validity().not_after.timestamp(),
                file_digests,
            };
            let metadata_bytes = serde_json::to_vec(&metadata)?;
            write_sync_new(&staging.join("metadata.json"), &metadata_bytes, 0o644)?;
            File::open(&staging)?.sync_all()?;
            std::fs::rename(&staging, final_path)?;
            Ok(evidence.fingerprint)
        })();
        match result {
            Ok(fingerprint) => Ok((staging, fingerprint)),
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                Err(error)
            }
        }
    };

    match operation() {
        Ok((marker, fingerprint)) if marker.as_os_str().is_empty() => {
            InstallOutcome::ExistingVerified {
                path: final_path.to_path_buf(),
                fingerprint,
            }
        }
        Ok((_staging, fingerprint)) => {
            let parent = final_path.parent().expect("validated parent");
            match parent_sync(parent) {
                Ok(()) => InstallOutcome::InstalledDurable {
                    path: final_path.to_path_buf(),
                    fingerprint,
                },
                Err(error) => InstallOutcome::InstalledDurabilityUnknown {
                    path: final_path.to_path_buf(),
                    fingerprint,
                    error: error.into(),
                },
            }
        }
        Err(error) => InstallOutcome::NotInstalled { error },
    }
}

pub fn verify_credential_set(path: &Path) -> Result<CredentialMetadata> {
    let metadata = verify_credential_set_without_parent_sync(path)?;
    File::open(path.parent().context("credential set requires a parent")?)?.sync_all()?;
    Ok(metadata)
}

fn verify_credential_set_without_parent_sync(path: &Path) -> Result<CredentialMetadata> {
    if std::fs::metadata(path)?.permissions().mode() & 0o777 != 0o700 {
        bail!("credential set directory must have mode 0700");
    }
    let metadata: CredentialMetadata =
        serde_json::from_slice(&std::fs::read(path.join("metadata.json"))?)?;
    if metadata.schema_version != 1 || metadata.file_digests.len() != 3 {
        bail!("credential metadata schema or file set is invalid");
    }
    for (name, expected) in &metadata.file_digests {
        let file_path = path.join(name);
        let bytes = std::fs::read(&file_path)?;
        if digest(&bytes) != *expected {
            bail!("credential file digest mismatch for {name}");
        }
        let expected_mode = if name == "key.pem" { 0o600 } else { 0o644 };
        if std::fs::metadata(file_path)?.permissions().mode() & 0o777 != expected_mode {
            bail!("credential file {name} has the wrong mode");
        }
    }
    if std::fs::metadata(path.join("metadata.json"))?
        .permissions()
        .mode()
        & 0o777
        != 0o644
    {
        bail!("credential metadata file has the wrong mode");
    }
    let leaf_pem = std::fs::read(path.join("leaf.pem"))?;
    let (_, leaf) =
        parse_x509_pem(&leaf_pem).map_err(|error| anyhow::anyhow!("parsing leaf PEM: {error}"))?;
    let evidence = if metadata.profile.ends_with("endpoint") {
        wr_common::tls::validate_server_leaf(&leaf.contents, &metadata.subject)?
    } else {
        let evidence = wr_common::tls::parse_leaf_evidence(
            &leaf.contents,
            wr_common::tls::LeafProfile::Client,
            None,
        )?;
        let principal = evidence
            .principal
            .as_ref()
            .expect("client profile has principal");
        if principal.as_str() != metadata.subject || principal.kind().as_str() != metadata.profile {
            bail!("client profile provenance does not match the URI principal");
        }
        evidence
    };
    if evidence.fingerprint != metadata.leaf_fingerprint
        || evidence.serial != metadata.serial
        || evidence.spki_fingerprint != metadata.spki_fingerprint
    {
        bail!("credential metadata does not match leaf certificate");
    }
    let chain_pem = std::fs::read(path.join("chain.pem"))?;
    let (_, issuer) = parse_x509_pem(&chain_pem)
        .map_err(|error| anyhow::anyhow!("parsing issuer chain: {error}"))?;
    if digest(&issuer.contents) != metadata.issuer_display_fingerprint {
        bail!("issuer provenance does not match the installed chain");
    }
    let key = KeyPair::from_pem(&std::fs::read_to_string(path.join("key.pem"))?)?;
    ensure_spki_matches(&leaf.contents, &key)?;
    Ok(metadata)
}

fn ensure_spki_matches(certificate_der: &[u8], key: &KeyPair) -> Result<()> {
    let (_, certificate) = X509Certificate::from_der(certificate_der)
        .map_err(|error| anyhow::anyhow!("parsing certificate for SPKI validation: {error}"))?;
    if certificate.public_key().subject_public_key.data.as_ref() != key.public_key_raw() {
        bail!("certificate and private key SPKI do not match");
    }
    Ok(())
}

fn write_sync_new(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)?;
    file.write_all(bytes)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    file.sync_all()?;
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(profile: CertificateProfile) -> CredentialSetContent {
        let mut ca_params = CertificateParams::new(vec![]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::from_params(&ca_params, ca_key);
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![]).unwrap();
        let (subject, profile_name, san, eku) = if profile.principal_kind().is_some() {
            let subject = "urn:wruntime:cluster-a:proxy:node-a".to_string();
            (
                subject.clone(),
                "proxy",
                SanType::URI(subject.try_into().unwrap()),
                ExtendedKeyUsagePurpose::ClientAuth,
            )
        } else {
            (
                "manager.example".to_string(),
                "manager-endpoint",
                SanType::DnsName("manager.example".try_into().unwrap()),
                ExtendedKeyUsagePurpose::ServerAuth,
            )
        };
        params.subject_alt_names = vec![san];
        params.extended_key_usages = vec![eku];
        let leaf = params.signed_by(&key, &issuer).unwrap();
        CredentialSetContent {
            profile: profile_name.into(),
            subject,
            key_pem: key.serialize_pem(),
            leaf_pem: leaf.pem(),
            chain_pem: ca.pem(),
            leaf_der: leaf.der().to_vec(),
            issuer_der: ca.der().to_vec(),
        }
    }

    #[test]
    fn leaf_profiles_reject_missing_mixed_multiple_and_wrong_cluster_identity() {
        let mut ca_params = CertificateParams::new(vec![]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let issuer = Issuer::from_params(&ca_params, ca_key);
        let issue = |sans: Vec<SanType>, eku: Vec<ExtendedKeyUsagePurpose>| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![]).unwrap();
            params.subject_alt_names = sans;
            params.extended_key_usages = eku;
            params.signed_by(&key, &issuer).unwrap().der().to_vec()
        };
        let uri = || SanType::URI("urn:wruntime:cluster-a:human:alice".try_into().unwrap());
        assert!(wr_common::tls::parse_leaf_evidence(
            &issue(vec![uri()], vec![]),
            wr_common::tls::LeafProfile::Client,
            None,
        )
        .is_err());
        assert!(wr_common::tls::parse_leaf_evidence(
            &issue(
                vec![uri()],
                vec![
                    ExtendedKeyUsagePurpose::ClientAuth,
                    ExtendedKeyUsagePurpose::ServerAuth
                ]
            ),
            wr_common::tls::LeafProfile::Client,
            None,
        )
        .is_err());
        assert!(wr_common::tls::parse_leaf_evidence(
            &issue(
                vec![uri(), uri()],
                vec![ExtendedKeyUsagePurpose::ClientAuth]
            ),
            wr_common::tls::LeafProfile::Client,
            None,
        )
        .is_err());
        let wrong_cluster = wr_common::identity::ClusterId::parse("cluster-b").unwrap();
        assert!(wr_common::tls::validate_client_leaf(
            &issue(vec![uri()], vec![ExtendedKeyUsagePurpose::ClientAuth]),
            &wrong_cluster,
        )
        .is_err());
        assert!(wr_common::tls::parse_leaf_evidence(
            &issue(
                vec![SanType::DnsName("manager.example".try_into().unwrap())],
                vec![ExtendedKeyUsagePurpose::ServerAuth]
            ),
            wr_common::tls::LeafProfile::Client,
            None,
        )
        .is_err());
    }

    #[test]
    fn pre_rename_failure_leaves_no_final_directory() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("set-v1");
        let mut content = fixture(CertificateProfile::Proxy);
        content.key_pem = "not a key".into();
        assert!(matches!(
            install_credential_set(&destination, &content),
            InstallOutcome::NotInstalled { .. }
        ));
        assert!(!destination.exists());
    }

    #[test]
    fn parent_fsync_failure_reports_visible_unknown_install() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("set-v1");
        let outcome = install_credential_set_with_parent_sync(
            &destination,
            &fixture(CertificateProfile::Proxy),
            |_| Err(std::io::Error::other("injected parent fsync failure")),
        );
        assert!(matches!(
            outcome,
            InstallOutcome::InstalledDurabilityUnknown { .. }
        ));
        assert!(destination.exists());
        verify_credential_set(&destination).unwrap();
    }

    #[test]
    fn existing_complete_set_is_verified_not_overwritten() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("set-v1");
        let content = fixture(CertificateProfile::ManagerEndpoint);
        assert!(matches!(
            install_credential_set(&destination, &content),
            InstallOutcome::InstalledDurable { .. }
        ));
        assert!(matches!(
            install_credential_set(&destination, &content),
            InstallOutcome::ExistingVerified { .. }
        ));
    }
}
