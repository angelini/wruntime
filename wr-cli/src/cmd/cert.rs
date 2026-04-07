use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

#[derive(Args)]
pub struct CertArgs {
    #[command(subcommand)]
    pub command: CertCommand,
}

#[derive(Subcommand)]
pub enum CertCommand {
    /// Generate a self-signed P-256 CA certificate (10-year validity)
    InitCa(InitCaArgs),
    /// Generate a node certificate signed by the CA
    Generate(GenerateArgs),
}

#[derive(Args)]
pub struct InitCaArgs {
    /// Output directory for ca.crt and ca.key
    #[arg(long, default_value = "./certs/")]
    output: PathBuf,
}

#[derive(Args)]
pub struct GenerateArgs {
    /// Hostname for the node certificate (added as SAN + CN)
    hostname: String,

    /// Directory containing ca.crt and ca.key (and where node certs are written)
    #[arg(long, default_value = "./certs/")]
    ca_dir: PathBuf,

    /// Additional IP addresses to include as SANs (repeatable)
    #[arg(long = "ip", value_name = "ADDR")]
    ips: Vec<IpAddr>,
}

pub fn run(args: CertArgs) -> Result<()> {
    match args.command {
        CertCommand::InitCa(a) => init_ca(a),
        CertCommand::Generate(a) => generate(a),
    }
}

fn init_ca(args: InitCaArgs) -> Result<()> {
    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("creating output directory {:?}", args.output))?;

    let mut params = CertificateParams::new(vec![])?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "wruntime-ca");

    // 10-year validity
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(3650);

    let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let ca_cert = params.self_signed(&key_pair)?;

    let cert_path = args.output.join("ca.crt");
    let key_path = args.output.join("ca.key");

    std::fs::write(&cert_path, ca_cert.pem())
        .with_context(|| format!("writing {:?}", cert_path))?;
    std::fs::write(&key_path, key_pair.serialize_pem())
        .with_context(|| format!("writing {:?}", key_path))?;

    println!("CA certificate written to {:?}", cert_path);
    println!("CA private key written to {:?}", key_path);
    Ok(())
}

fn generate(args: GenerateArgs) -> Result<()> {
    let ca_cert_pem = std::fs::read_to_string(args.ca_dir.join("ca.crt"))
        .context("reading ca.crt — run `wr cert init-ca` first")?;
    let ca_key_pem = std::fs::read_to_string(args.ca_dir.join("ca.key"))
        .context("reading ca.key — run `wr cert init-ca` first")?;

    let ca_key_pair = KeyPair::from_pem(&ca_key_pem).context("parsing CA private key")?;
    let ca_params =
        CertificateParams::from_ca_cert_pem(&ca_cert_pem).context("parsing CA certificate")?;
    let ca_cert = ca_params
        .self_signed(&ca_key_pair)
        .context("reconstructing CA certificate")?;

    let mut params = CertificateParams::new(vec![])?;
    let mut sans = vec![
        SanType::DnsName(
            args.hostname
                .clone()
                .try_into()
                .context("invalid hostname for SAN")?,
        ),
        SanType::IpAddress(IpAddr::from([127, 0, 0, 1])),
    ];
    for ip in &args.ips {
        sans.push(SanType::IpAddress(*ip));
    }
    params.subject_alt_names = sans;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, args.hostname.as_str());

    let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let node_cert = params
        .signed_by(&key_pair, &ca_cert, &ca_key_pair)
        .context("signing node certificate")?;

    let cert_path = args.ca_dir.join(format!("{}.crt", args.hostname));
    let key_path = args.ca_dir.join(format!("{}.key", args.hostname));

    std::fs::write(&cert_path, node_cert.pem())
        .with_context(|| format!("writing {:?}", cert_path))?;
    std::fs::write(&key_path, key_pair.serialize_pem())
        .with_context(|| format!("writing {:?}", key_path))?;

    println!("Node certificate written to {:?}", cert_path);
    println!("Node private key written to {:?}", key_path);
    Ok(())
}
