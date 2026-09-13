use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};
use wr_common::identity::{ClusterId, PrincipalKind};
use wr_common::migration_bundle::MigrationFileManifest;
use wr_common::postgres::{
    DerivedNamespace, PostgresProvisioningManifest, IDENT_HEADER,
    MIGRATION_EXECUTOR_AUTH_MARKER_ROLE, PLATFORM_SCHEMA, SUPPORTED_POSTGRES_MAJOR,
    TENANT_AUTH_MARKER_ROLE,
};
use x509_parser::pem::Pem;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::cmd::postgres::{
    ApproveRetryArgs, MigrateArgs, ProvisionArgs, ProvisionMigrateArgs, ProvisionOutput,
};

pub mod migration;

const CLUSTER_LOCK_KEY: i64 = 0x5752_5047_5052_4f56;
const RELOAD_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TenantStateReceiptV1 {
    pub schema_version: u32,
    pub reservation_digest: String,
    pub reservation: crate::cmd::node::NodeDeploymentReservationV1,
    pub provision_generation: u64,
    pub provisioning_manifest_digest: String,
    pub migration_bundle_digest: String,
    pub successful_migrations: Vec<MigrationFileManifest>,
}

impl TenantStateReceiptV1 {
    pub fn read(path: &Path) -> Result<Self> {
        let value: Self = serde_json::from_slice(
            &std::fs::read(path)
                .with_context(|| format!("reading tenant state receipt {}", path.display()))?,
        )?;
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported tenant state receipt schema"
        );
        self.reservation.validate()?;
        ensure!(
            self.reservation_digest == self.reservation.digest()?,
            "tenant state receipt reservation digest mismatch"
        );
        ensure!(
            self.provision_generation > 0,
            "tenant state receipt generation is zero"
        );
        ensure!(
            self.successful_migrations.windows(2).all(|pair| {
                (&pair[0].namespace, &pair[0].module, pair[0].version)
                    < (&pair[1].namespace, &pair[1].module, pair[1].version)
            }),
            "tenant state receipt migrations are not canonical"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
struct ProvisionReceipt {
    schema_version: u32,
    outcome: &'static str,
    generation: u64,
    manifest_digest: String,
    namespaces: Vec<NamespaceReceipt>,
    auth_mappings: usize,
    dry_run: bool,
}

#[derive(Clone, Debug, Serialize)]
struct NamespaceReceipt {
    namespace: String,
    database: String,
    outcome: &'static str,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IdentMapping {
    map: String,
    system_user: String,
    database_user: String,
}

struct FileLock(File);

impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

/// Converge the SQL portion of a validated manifest without activating its
/// certificate map or publishing its generation. This is reusable by focused
/// convergence tests; production callers must use [`provision`] so generation
/// publication remains ordered after map reload verification.
pub async fn converge_sql_state(
    admin_url: &str,
    manifest: &PostgresProvisioningManifest,
) -> Result<Vec<DerivedNamespace>> {
    manifest.validate()?;
    let desired = manifest.derive_namespaces();
    let (mut client, connection) = tokio_postgres::connect(admin_url, NoTls).await?;
    let task = tokio::spawn(connection);
    let locked: bool = client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&CLUSTER_LOCK_KEY])
        .await?
        .get(0);
    ensure!(
        locked,
        "another PostgreSQL provisioner holds the cluster lock"
    );
    converge_cluster(&mut client, manifest, &desired).await?;
    for namespace in &desired {
        converge_namespace(manifest, namespace, admin_url).await?;
    }
    task.abort();
    Ok(desired)
}

/// Validate the live server/file topology without mutating SQL or filesystem
/// state. This is the executable fail-closed seam for non-co-located fixtures.
pub async fn validate_server_topology(
    admin_url: &str,
    manifest: &PostgresProvisioningManifest,
    pg_ident_target: &Path,
) -> Result<()> {
    manifest.validate()?;
    let (client, connection) = tokio_postgres::connect(admin_url, NoTls).await?;
    let task = tokio::spawn(connection);
    let result = preflight(&client, manifest, pg_ident_target, true).await;
    task.abort();
    result.map(|_| ())
}

/// Publish SQL generation records after an external activation seam succeeds.
/// Production calls the same replay check and commit only after live map reload.
pub async fn publish_sql_generation(
    admin_url: &str,
    manifest: &PostgresProvisioningManifest,
) -> Result<()> {
    manifest.validate()?;
    let desired = manifest.derive_namespaces();
    let digest = manifest.normalized_digest();
    check_generation_replay(admin_url, &desired, manifest.generation, &digest).await?;
    commit_generation(admin_url, &desired, manifest.generation, &digest).await
}

pub async fn migrate(args: MigrateArgs) -> Result<()> {
    let provisioning = PostgresProvisioningManifest::parse_toml(
        &std::fs::read_to_string(&args.manifest)
            .with_context(|| format!("reading manifest {}", args.manifest.display()))?,
    )?;
    let bundle_text = std::fs::read_to_string(&args.bundle_manifest).with_context(|| {
        format!(
            "reading migration manifest {}",
            args.bundle_manifest.display()
        )
    })?;
    let bundle_manifest: wr_common::migration_bundle::MigrationBundleManifest =
        if bundle_text.trim_start().starts_with('{') {
            let value: wr_common::migration_bundle::MigrationBundleManifest =
                serde_json::from_str(&bundle_text).context("invalid migration bundle JSON")?;
            value.validate()?;
            value
        } else {
            wr_common::migration_bundle::MigrationBundleManifest::parse_toml(&bundle_text)?
        };
    let bundle = bundle_manifest.capture(&args.bundle_root)?;
    let deployment = match (
        args.node_bundle.as_ref(),
        args.deployment_reservation.as_ref(),
        args.receipt_out.as_ref(),
    ) {
        (None, None, None) => None,
        (Some(node_bundle), Some(reservation_path), Some(receipt_out)) => {
            let reservation =
                crate::cmd::node::NodeDeploymentReservationV1::read(reservation_path)?;
            let bundle_path = node_bundle
                .to_str()
                .context("node bundle path is not UTF-8")?;
            let node_manifest: crate::cmd::bundle_integrity::BundleManifest =
                crate::cmd::bundle::read_manifest(bundle_path)?;
            crate::cmd::bundle_integrity::verify_bundle_archive(bundle_path, &node_manifest)?;
            validate_deployment_receipt_binding(
                &node_manifest.bundle_digest,
                &reservation,
                &bundle.manifest,
                &provisioning,
            )?;
            for file in &bundle.files {
                let archived = crate::cmd::bundle::read_bytes_from_tarball(
                    bundle_path,
                    &format!(
                        "wr-node/migrations/{}/{}",
                        file.manifest.module, file.manifest.filename
                    ),
                )?;
                ensure!(
                    archived.as_slice() == file.bytes.as_ref(),
                    "staged migration bytes do not match node bundle"
                );
            }
            Some((reservation, receipt_out.clone()))
        }
        _ => bail!("deployment receipt inputs must be supplied together"),
    };
    let admin_url = SecretAdminUrl::read(&args.admin_url_file)?;
    migration::migrate_bundle(admin_url.expose(), &provisioning, bundle.clone()).await?;
    if let Some((reservation, receipt_out)) = deployment {
        let verification =
            verify_successful_state(admin_url.expose(), &provisioning, &bundle.manifest.files)
                .await;
        write_verified_tenant_receipt(
            verification,
            &receipt_out,
            reservation,
            &provisioning,
            &bundle.manifest,
        )?;
    }
    Ok(())
}

fn validate_deployment_receipt_binding(
    node_bundle_digest: &str,
    reservation: &crate::cmd::node::NodeDeploymentReservationV1,
    bundle: &wr_common::migration_bundle::MigrationBundleManifest,
    provisioning: &PostgresProvisioningManifest,
) -> Result<()> {
    ensure!(
        node_bundle_digest == reservation.bundle_digest,
        "node bundle does not match deployment reservation"
    );
    ensure!(
        bundle.deployment_digest == reservation.revision_digest,
        "migration deployment digest does not match reservation"
    );
    let expected_fingerprint = reservation
        .postgres_client_leaf_fingerprint
        .as_deref()
        .context("database reservation omitted postgres-client fingerprint")?;
    let provisioned = provisioning
        .nodes
        .iter()
        .find(|node| node.node_id == reservation.node_id)
        .context("provisioning manifest omitted reserved node")?;
    ensure!(
        provisioned.certificate_sha256 == expected_fingerprint,
        "provisioning certificate fingerprint does not match reservation"
    );
    Ok(())
}

fn write_verified_tenant_receipt(
    verification: Result<()>,
    receipt_out: &Path,
    reservation: crate::cmd::node::NodeDeploymentReservationV1,
    provisioning: &PostgresProvisioningManifest,
    bundle: &wr_common::migration_bundle::MigrationBundleManifest,
) -> Result<()> {
    verification
        .context("refusing tenant receipt before successful generation and ledger verification")?;
    let receipt = TenantStateReceiptV1 {
        schema_version: 1,
        reservation_digest: reservation.digest()?,
        reservation,
        provision_generation: provisioning.generation,
        provisioning_manifest_digest: provisioning.normalized_digest(),
        migration_bundle_digest: bundle.bundle_digest.clone(),
        successful_migrations: bundle.files.clone(),
    };
    receipt.validate()?;
    atomic_receipt_write(receipt_out, &serde_json::to_vec_pretty(&receipt)?)
}

async fn verify_successful_state(
    admin_url: &str,
    provisioning: &PostgresProvisioningManifest,
    files: &[MigrationFileManifest],
) -> Result<()> {
    let desired = provisioning
        .derive_namespaces()
        .into_iter()
        .map(|namespace| (namespace.namespace, namespace.database))
        .collect::<BTreeMap<_, _>>();
    for (namespace, database) in &desired {
        let mut config = tokio_postgres::Config::from_str(admin_url)?;
        config.dbname(database);
        let (client, driver) = config.connect(NoTls).await?;
        let task = tokio::spawn(driver);
        let state = client
            .query_opt(
                &format!("SELECT generation,manifest_digest FROM {PLATFORM_SCHEMA}.namespace_state WHERE singleton"),
                &[],
            )
            .await?
            .context("provision generation is not published")?;
        ensure!(
            state.get::<_, i64>(0) == provisioning.generation as i64,
            "published provision generation is stale"
        );
        ensure!(
            state.get::<_, String>(1) == provisioning.normalized_digest(),
            "published provision manifest digest is stale"
        );
        for file in files.iter().filter(|file| &file.namespace == namespace) {
            let row = client.query_opt(
                &format!("SELECT filename,content_hash,byte_length,state FROM {PLATFORM_SCHEMA}.migration_attempts WHERE namespace=$1 AND module=$2 AND migration_version=$3 ORDER BY attempt DESC LIMIT 1"),
                &[&file.namespace, &file.module, &(file.version as i64)],
            ).await?.context("required migration ledger entry is absent")?;
            ensure!(
                row.get::<_, String>(0) == file.filename
                    && row.get::<_, String>(1) == file.content_hash
                    && row.get::<_, i64>(2) == file.byte_length as i64
                    && row.get::<_, String>(3) == "succeeded",
                "required migration ledger entry is not successful"
            );
        }
        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    Ok(())
}

fn atomic_receipt_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("receipt output requires a parent")?;
    std::fs::create_dir_all(parent)?;
    ensure!(!path.exists(), "receipt output already exists");
    let temporary = parent.join(format!(".tenant-state-{}", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o644)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub async fn approve_retry(args: ApproveRetryArgs) -> Result<()> {
    let provisioning = PostgresProvisioningManifest::parse_toml(
        &std::fs::read_to_string(&args.manifest)
            .with_context(|| format!("reading manifest {}", args.manifest.display()))?,
    )?;
    let admin_url = SecretAdminUrl::read(&args.admin_url_file)?;
    migration::approve_retry(
        admin_url.expose(),
        &provisioning,
        &migration::RetryApproval {
            namespace: args.namespace,
            module: args.module,
            version: args.version,
            attempt: args.attempt,
            state: args.state,
            content_hash: args.content_hash,
            operator: args.operator,
            reason: args.reason,
        },
    )
    .await
}

pub async fn provision_migrate(args: ProvisionMigrateArgs) -> Result<()> {
    provision(ProvisionArgs {
        manifest: args.manifest.clone(),
        admin_url_file: args.admin_url_file.clone(),
        pg_ident_target: args.pg_ident_target,
        dry_run: false,
        output: args.output,
    })
    .await?;
    migrate(MigrateArgs {
        manifest: args.manifest,
        bundle_manifest: args.bundle_manifest,
        bundle_root: args.bundle_root,
        admin_url_file: args.admin_url_file,
        node_bundle: args.node_bundle,
        deployment_reservation: args.deployment_reservation,
        receipt_out: args.receipt_out,
    })
    .await
}

pub async fn provision(args: ProvisionArgs) -> Result<()> {
    let manifest_text = std::fs::read_to_string(&args.manifest)
        .with_context(|| format!("reading manifest {}", args.manifest.display()))?;
    let manifest = PostgresProvisioningManifest::parse_toml(&manifest_text)
        .context("stage=manifest-validation")?;
    let digest = manifest.normalized_digest();
    let desired = manifest.derive_namespaces();
    validate_node_certificates(&manifest).context("stage=certificate-validation")?;

    let admin_url =
        SecretAdminUrl::read(&args.admin_url_file).context("stage=admin-secret-preflight")?;
    let (mut client, connection) = tokio_postgres::connect(admin_url.expose(), NoTls)
        .await
        .context("connecting to PostgreSQL admin endpoint")?;
    let connection_task = tokio::spawn(connection);

    let preflight = preflight(&client, &manifest, &args.pg_ident_target, args.dry_run).await?;
    check_generation_replay(admin_url.expose(), &desired, manifest.generation, &digest).await?;

    let desired_mappings = desired_mappings(&manifest, &desired);
    let retained = read_managed_mappings(&args.pg_ident_target, &manifest.ident_map_name)?;
    let mappings = retained
        .union(&desired_mappings)
        .cloned()
        .collect::<BTreeSet<_>>();

    if args.dry_run {
        emit_receipt(
            args.output,
            receipt(
                &manifest,
                &digest,
                &desired,
                mappings.len(),
                true,
                "planned",
            ),
        )?;
        drop(preflight);
        connection_task.abort();
        drop(admin_url);
        return Ok(());
    }

    converge_cluster(&mut client, &manifest, &desired).await?;
    for namespace in &desired {
        converge_namespace(&manifest, namespace, admin_url.expose()).await?;
    }

    let previous = std::fs::read(&args.pg_ident_target).context("reading pg_ident backup")?;
    let backup = preserve_backup(&args.pg_ident_target, &previous)?;
    let rendered = render_mappings(&mappings);
    let activation = install_and_reload(
        &client,
        &manifest,
        &args.pg_ident_target,
        &previous,
        rendered.as_bytes(),
    )
    .await;
    if let Err(original) = activation {
        let rollback =
            restore_and_reload(&client, &manifest, &args.pg_ident_target, &previous).await;
        return match rollback {
            Ok(()) => {
                remove_backup(&backup)?;
                Err(original.context("pg_ident activation failed; prior mapping restored"))
            }
            Err(rollback) => bail!(
                "fatal/manual-intervention: pg_ident activation failed: {original:#}; rollback could not be verified: {rollback:#}"
            ),
        };
    }

    remove_backup(&backup)?;
    commit_generation(admin_url.expose(), &desired, manifest.generation, &digest).await?;
    emit_receipt(
        args.output,
        receipt(
            &manifest,
            &digest,
            &desired,
            mappings.len(),
            false,
            "converged",
        ),
    )?;
    drop(preflight);
    connection_task.abort();
    drop(admin_url);
    Ok(())
}

fn receipt(
    manifest: &PostgresProvisioningManifest,
    digest: &str,
    desired: &[DerivedNamespace],
    mapping_count: usize,
    dry_run: bool,
    outcome: &'static str,
) -> ProvisionReceipt {
    ProvisionReceipt {
        schema_version: 1,
        outcome,
        generation: manifest.generation,
        manifest_digest: digest.into(),
        namespaces: desired
            .iter()
            .map(|namespace| NamespaceReceipt {
                namespace: namespace.namespace.clone(),
                database: namespace.database.clone(),
                outcome,
            })
            .collect(),
        auth_mappings: mapping_count,
        dry_run,
    }
}

fn emit_receipt(output: ProvisionOutput, receipt: ProvisionReceipt) -> Result<()> {
    match output {
        ProvisionOutput::Json => println!("{}", serde_json::to_string(&receipt)?),
        ProvisionOutput::Human => println!(
            "{}: {} namespace(s), {} certificate mapping(s), generation {}",
            receipt.outcome,
            receipt.namespaces.len(),
            receipt.auth_mappings,
            receipt.generation
        ),
    }
    Ok(())
}

struct SecretAdminUrl(String);

impl SecretAdminUrl {
    fn read(path: &Path) -> Result<Self> {
        let metadata =
            std::fs::symlink_metadata(path).context("reading admin URL file metadata")?;
        ensure!(
            metadata.file_type().is_file(),
            "admin URL file must be regular"
        );
        ensure!(
            metadata.mode() & 0o777 == 0o600,
            "admin URL file mode must be 0600"
        );
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "admin URL file must be owned by invoking user"
        );
        let value = std::fs::read_to_string(path).context("reading admin URL file")?;
        let value = value.trim().to_string();
        ensure!(!value.is_empty(), "admin URL file is empty");
        Ok(Self(value))
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl Drop for SecretAdminUrl {
    fn drop(&mut self) {
        self.0.clear();
    }
}

async fn preflight(
    client: &Client,
    manifest: &PostgresProvisioningManifest,
    target: &Path,
    dry_run: bool,
) -> Result<Option<FileLock>> {
    let version: i32 = client
        .query_one("SHOW server_version_num", &[])
        .await?
        .get::<_, String>(0)
        .parse()?;
    ensure!(
        version / 10_000 == i32::from(SUPPORTED_POSTGRES_MAJOR),
        "PostgreSQL server major must be 18"
    );
    let ident_file: String = client.query_one("SHOW ident_file", &[]).await?.get(0);
    let data_directory: String = client.query_one("SHOW data_directory", &[]).await?.get(0);
    let active = if Path::new(&ident_file).is_absolute() {
        PathBuf::from(ident_file)
    } else {
        Path::new(&data_directory).join(ident_file)
    };
    validate_identity_target(target, &active, unsafe { libc::geteuid() })?;

    let locked: bool = client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&CLUSTER_LOCK_KEY])
        .await?
        .get(0);
    ensure!(
        locked,
        "another PostgreSQL provisioner holds the cluster lock"
    );
    validate_hba(client, manifest).await?;

    if dry_run {
        return Ok(None);
    }
    ensure!(
        !backup_path(target).exists(),
        "a prior pg_ident backup requires manual inspection"
    );
    Ok(Some(acquire_file_lock(target)?))
}

fn validate_identity_target(target: &Path, active: &Path, expected_uid: u32) -> Result<()> {
    ensure!(
        target.canonicalize()? == active.canonicalize()?,
        "--pg-ident-target is not PostgreSQL's active ident_file"
    );
    let metadata = std::fs::symlink_metadata(target)?;
    ensure!(
        metadata.file_type().is_file(),
        "pg_ident target must be a regular file"
    );
    ensure!(
        metadata.uid() == expected_uid,
        "provisioner must run as pg_ident owner"
    );
    ensure!(
        metadata.mode() & 0o777 == 0o600,
        "pg_ident target mode must be 0600"
    );
    Ok(())
}

fn acquire_file_lock(target: &Path) -> Result<FileLock> {
    let lock_path = target.with_extension("wruntime.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)?;
    let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    ensure!(
        result == 0,
        "another PostgreSQL provisioner holds the filesystem lock"
    );
    Ok(FileLock(lock))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NetworkIdentity {
    address: IpAddr,
    prefix: u8,
}

impl NetworkIdentity {
    fn parse_cidr(value: &str) -> Result<Self> {
        let (address, prefix) = value
            .split_once('/')
            .with_context(|| format!("CIDR lacks prefix: {value}"))?;
        let address = IpAddr::from_str(address)?;
        let prefix = prefix.parse::<u8>()?;
        Self::from_address_and_prefix(address, prefix)
    }

    fn from_postgres(address: &str, netmask: &str) -> Result<Self> {
        let address = IpAddr::from_str(address)
            .with_context(|| format!("invalid PostgreSQL HBA address {address}"))?;
        let netmask = IpAddr::from_str(netmask)
            .with_context(|| format!("invalid PostgreSQL HBA netmask {netmask}"))?;
        let (address_bits, width) = ip_bits(address);
        let (mask_bits, mask_width) = ip_bits(netmask);
        ensure!(width == mask_width, "HBA address/netmask family mismatch");
        let prefix = contiguous_prefix(mask_bits, width)
            .context("PostgreSQL HBA netmask is non-contiguous")?;
        Ok(Self {
            address: bits_ip(address_bits & mask_bits, width),
            prefix,
        })
    }

    fn from_address_and_prefix(address: IpAddr, prefix: u8) -> Result<Self> {
        let (bits, width) = ip_bits(address);
        ensure!(
            u32::from(prefix) <= width,
            "network prefix exceeds address width"
        );
        let mask = prefix_mask(prefix, width);
        Ok(Self {
            address: bits_ip(bits & mask, width),
            prefix,
        })
    }

    fn contains(self, other: Self) -> bool {
        let (self_bits, width) = ip_bits(self.address);
        let (other_bits, other_width) = ip_bits(other.address);
        width == other_width
            && self.prefix <= other.prefix
            && (other_bits & prefix_mask(self.prefix, width)) == self_bits
    }
}

fn ip_bits(address: IpAddr) -> (u128, u32) {
    match address {
        IpAddr::V4(value) => (u128::from(u32::from(value)), 32),
        IpAddr::V6(value) => (u128::from(value), 128),
    }
}

fn bits_ip(bits: u128, width: u32) -> IpAddr {
    if width == 32 {
        IpAddr::V4(Ipv4Addr::from(bits as u32))
    } else {
        IpAddr::V6(Ipv6Addr::from(bits))
    }
}

fn prefix_mask(prefix: u8, width: u32) -> u128 {
    if prefix == 0 {
        0
    } else if width == 128 {
        u128::MAX << (128 - prefix)
    } else {
        u128::from(u32::MAX << (32 - prefix))
    }
}

fn contiguous_prefix(mask: u128, width: u32) -> Option<u8> {
    let relevant = if width == 32 {
        mask & u128::from(u32::MAX)
    } else {
        mask
    };
    let inverted = relevant ^ prefix_mask(width as u8, width);
    if inverted & inverted.wrapping_add(1) != 0 {
        return None;
    }
    Some((width - inverted.count_ones()) as u8)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HbaAddress {
    Network(NetworkIdentity),
    Dynamic,
}

impl HbaAddress {
    fn parse(address: &str, netmask: &str) -> Result<Self> {
        match address {
            "all" | "samehost" | "samenet" => {
                ensure!(
                    netmask.is_empty(),
                    "dynamic HBA address has an unexpected netmask"
                );
                Ok(Self::Dynamic)
            }
            _ => Ok(Self::Network(NetworkIdentity::from_postgres(
                address, netmask,
            )?)),
        }
    }

    fn covers(self, target: NetworkIdentity) -> bool {
        match self {
            Self::Network(network) => network.contains(target),
            Self::Dynamic => true,
        }
    }
}

#[derive(Clone, Debug)]
struct HbaRule {
    line: i32,
    kind: String,
    users: String,
    address: HbaAddress,
    auth_method: String,
    options: String,
}

#[derive(Clone, Debug)]
struct LocalHbaRule {
    line: i32,
    databases: String,
    users: String,
    auth_method: String,
}

fn validate_migration_executor_hba_rules(rules: &[LocalHbaRule]) -> Result<()> {
    let matching_line = rules
        .iter()
        .find_map(|rule| {
            (rule.databases.contains("all")
                && rule.users.contains(MIGRATION_EXECUTOR_AUTH_MARKER_ROLE)
                && rule.auth_method == "scram-sha-256")
                .then_some(rule.line)
        })
        .context("missing local SCRAM rule for disposable migration executors")?;
    let bypass = rules.iter().any(|rule| {
        rule.line < matching_line
            && rule.databases.contains("all")
            && (rule.users.contains("all")
                || rule.users.contains(MIGRATION_EXECUTOR_AUTH_MARKER_ROLE))
    });
    ensure!(
        !bypass,
        "an earlier local pg_hba rule bypasses disposable migration executor authentication"
    );
    Ok(())
}

fn validate_hba_rules(
    rules: &[HbaRule],
    tenant_cidrs: &[String],
    ident_map_name: &str,
) -> Result<()> {
    for cidr in tenant_cidrs {
        let target = NetworkIdentity::parse_cidr(cidr)?;
        let matching_line = rules.iter().find_map(|rule| {
            (rule.kind == "hostssl"
                && rule.users.contains(TENANT_AUTH_MARKER_ROLE)
                && rule.address == HbaAddress::Network(target)
                && rule.auth_method == "cert"
                && rule.options.contains(&format!("map={ident_map_name}")))
            .then_some(rule.line)
        });
        let matching_line = matching_line
            .with_context(|| format!("missing exact cert/map HBA rule for tenant CIDR {cidr}"))?;
        let bypass = rules.iter().any(|rule| {
            rule.line < matching_line
                && matches!(rule.kind.as_str(), "host" | "hostssl")
                && (rule.users.contains("all") || rule.users.contains(TENANT_AUTH_MARKER_ROLE))
                && rule.address.covers(target)
                && (rule.auth_method != "cert"
                    || !rule.options.contains(&format!("map={ident_map_name}")))
        });
        ensure!(
            !bypass,
            "an earlier pg_hba rule bypasses tenant certificate authentication for {cidr}"
        );
    }
    Ok(())
}

async fn validate_hba(client: &Client, manifest: &PostgresProvisioningManifest) -> Result<()> {
    let rows = client
        .query(
            "SELECT line_number,type,database::text,user_name::text,COALESCE(address,''),COALESCE(netmask,''),auth_method,COALESCE(options::text,''),COALESCE(error,'') FROM pg_hba_file_rules ORDER BY line_number",
            &[],
        )
        .await?;
    ensure!(
        rows.iter().all(|row| row.get::<_, String>(8).is_empty()),
        "pg_hba.conf contains parse errors"
    );
    let local_rules = rows
        .iter()
        .filter(|row| row.get::<_, String>(1) == "local")
        .map(|row| LocalHbaRule {
            line: row.get(0),
            databases: row.get(2),
            users: row.get(3),
            auth_method: row.get(6),
        })
        .collect::<Vec<_>>();
    validate_migration_executor_hba_rules(&local_rules)?;
    let rules = rows
        .iter()
        .filter(|row| matches!(row.get::<_, String>(1).as_str(), "host" | "hostssl"))
        .map(|row| {
            Ok(HbaRule {
                line: row.get(0),
                kind: row.get(1),
                users: row.get(3),
                address: HbaAddress::parse(&row.get::<_, String>(4), &row.get::<_, String>(5))?,
                auth_method: row.get(6),
                options: row.get(7),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    validate_hba_rules(
        &rules,
        &manifest.tenant_client_cidrs,
        &manifest.ident_map_name,
    )
}

fn validate_node_certificates(manifest: &PostgresProvisioningManifest) -> Result<()> {
    let expected_cluster = ClusterId::parse(manifest.cluster_id.clone())?;
    for node in &manifest.nodes {
        let bytes = std::fs::read(&node.certificate_pem_path).with_context(|| {
            format!("reading public node certificate for node {}", node.node_id)
        })?;
        let certificates =
            Pem::iter_from_buffer(&bytes).collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            certificates.len() >= 2,
            "node certificate PEM must include leaf and issuing PostgreSQL CA"
        );
        let leaf_der = &certificates[0].contents;
        ensure!(
            wr_common::tls::certificate_fingerprint_sha256(leaf_der) == node.certificate_sha256,
            "node certificate fingerprint mismatch"
        );
        ensure!(
            wr_common::tls::certificate_fingerprint_sha256(&certificates[1].contents)
                == manifest.postgres_ca_sha256,
            "node certificate issuing PostgreSQL CA fingerprint mismatch"
        );
        let (_, leaf) = X509Certificate::from_der(leaf_der)?;
        ensure!(
            leaf.validity().is_valid(),
            "node certificate is outside its validity interval"
        );
        let common_name = leaf
            .subject()
            .iter_common_name()
            .next()
            .context("node certificate requires one subject Common Name")?
            .as_str()?;
        ensure!(
            common_name == node.certificate_common_name,
            "node certificate Common Name mismatch"
        );
        let evidence = wr_common::tls::parse_leaf_evidence(
            leaf_der,
            wr_common::tls::LeafProfile::Client,
            Some(&expected_cluster),
        )?;
        let principal = evidence
            .principal
            .context("database client URI principal missing")?;
        ensure!(
            principal.kind() == PrincipalKind::PostgresClient,
            "node certificate is not the dedicated postgres-client profile"
        );
        ensure!(
            principal.name().as_str() == node.node_id,
            "database client principal is bound to another node"
        );
    }
    Ok(())
}

fn desired_mappings(
    manifest: &PostgresProvisioningManifest,
    desired: &[DerivedNamespace],
) -> BTreeSet<IdentMapping> {
    let common_names = manifest
        .nodes
        .iter()
        .map(|node| (&node.node_id, &node.certificate_common_name))
        .collect::<BTreeMap<_, _>>();
    desired
        .iter()
        .flat_map(|namespace| {
            namespace.node_logins.iter().flat_map(|login| {
                let system_user = common_names[&login.node_id].clone();
                [login.runtime.clone(), login.readiness.clone()]
                    .into_iter()
                    .map(move |database_user| IdentMapping {
                        map: manifest.ident_map_name.clone(),
                        system_user: system_user.clone(),
                        database_user,
                    })
            })
        })
        .collect()
}

fn read_managed_mappings(target: &Path, map_name: &str) -> Result<BTreeSet<IdentMapping>> {
    let text = std::fs::read_to_string(target)?;
    if text.trim().is_empty()
        || text
            .lines()
            .all(|line| line.trim().is_empty() || line.trim_start().starts_with('#'))
    {
        return Ok(BTreeSet::new());
    }
    ensure!(
        text.lines().next() == Some(IDENT_HEADER),
        "active pg_ident file is not wruntime-managed"
    );
    let mut mappings = BTreeSet::new();
    let mut role_subjects = BTreeMap::<String, String>::new();
    for line in text.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        ensure!(
            fields.len() == 3 && fields[0] == map_name,
            "conflicting retained pg_ident mapping"
        );
        wr_common::postgres::validate_common_name(fields[1])?;
        ensure!(
            fields[2].starts_with("wr_runtime_") || fields[2].starts_with("wr_ready_"),
            "managed pg_ident entry targets a privileged or unknown role"
        );
        if let Some(previous) = role_subjects.insert(fields[2].into(), fields[1].into()) {
            ensure!(
                previous == fields[1],
                "conflicting retained pg_ident subjects for one role"
            );
        }
        mappings.insert(IdentMapping {
            map: fields[0].into(),
            system_user: fields[1].into(),
            database_user: fields[2].into(),
        });
    }
    Ok(mappings)
}

fn render_mappings(mappings: &BTreeSet<IdentMapping>) -> String {
    let mut rendered = format!("{IDENT_HEADER}\n");
    for mapping in mappings {
        rendered.push_str(&format!(
            "{} {} {}\n",
            mapping.map, mapping.system_user, mapping.database_user
        ));
    }
    rendered
}

async fn quote<C: tokio_postgres::GenericClient + Sync>(client: &C, value: &str) -> Result<String> {
    Ok(client
        .query_one("SELECT quote_ident($1)", &[&value])
        .await?
        .get(0))
}

async fn literal<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    value: &str,
) -> Result<String> {
    Ok(client
        .query_one("SELECT quote_literal($1)", &[&value])
        .await?
        .get(0))
}

async fn converge_cluster(
    client: &mut Client,
    manifest: &PostgresProvisioningManifest,
    desired: &[DerivedNamespace],
) -> Result<()> {
    ensure_role(client, TENANT_AUTH_MARKER_ROLE, false, 0).await?;
    ensure_role(client, MIGRATION_EXECUTOR_AUTH_MARKER_ROLE, false, 0).await?;
    let current_user: String = client.query_one("SELECT current_user", &[]).await?.get(0);
    for platform_database in std::iter::once(&manifest.platform_databases.manager)
        .chain(manifest.platform_databases.job_queues.iter())
    {
        let database = quote(client, platform_database).await?;
        let admin = quote(client, &current_user).await?;
        client
            .batch_execute(&format!(
                "REVOKE CONNECT,TEMPORARY ON DATABASE {database} FROM PUBLIC; GRANT CONNECT,TEMPORARY ON DATABASE {database} TO {admin}"
            ))
            .await?;
    }
    for namespace in desired {
        ensure_role(client, &namespace.owner, false, 0).await?;
        ensure_role(client, &namespace.runtime_group, false, 0).await?;
        ensure_role(client, &namespace.maintenance_role, false, 0).await?;
        for login in &namespace.node_logins {
            ensure_role(
                client,
                &login.runtime,
                true,
                manifest.limits.runtime_login_connections,
            )
            .await?;
            ensure_role(
                client,
                &login.readiness,
                true,
                manifest.limits.readiness_verifier_connections,
            )
            .await?;
            grant_membership(client, &namespace.runtime_group, &login.runtime).await?;
            grant_membership(client, TENANT_AUTH_MARKER_ROLE, &login.runtime).await?;
            grant_membership(client, TENANT_AUTH_MARKER_ROLE, &login.readiness).await?;
        }
        ensure_database(client, namespace, manifest).await?;
    }
    Ok(())
}

async fn ensure_role(client: &Client, role: &str, login: bool, connections: u32) -> Result<()> {
    let role_q = quote(client, role).await?;
    client.batch_execute(&format!(
        "DO $wr$ BEGIN CREATE ROLE {role_q}; EXCEPTION WHEN duplicate_object THEN NULL; END $wr$; \
         ALTER ROLE {role_q} {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT CONNECTION LIMIT {};",
        if login { "LOGIN" } else { "NOLOGIN" },
        if login { connections } else { 0 }
    )).await?;
    Ok(())
}

async fn grant_membership(client: &Client, group: &str, member: &str) -> Result<()> {
    let group = quote(client, group).await?;
    let member = quote(client, member).await?;
    client
        .batch_execute(&format!(
            "GRANT {group} TO {member} WITH INHERIT TRUE, SET FALSE, ADMIN FALSE"
        ))
        .await?;
    Ok(())
}

async fn ensure_database(
    client: &Client,
    namespace: &DerivedNamespace,
    manifest: &PostgresProvisioningManifest,
) -> Result<()> {
    let limit = manifest.limits.namespace_database_connections;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)",
            &[&namespace.database],
        )
        .await?
        .get(0);
    let database = quote(client, &namespace.database).await?;
    if !exists {
        client
            .batch_execute(&format!(
                "CREATE DATABASE {database} CONNECTION LIMIT {limit}"
            ))
            .await?;
    }
    let current_user: String = client.query_one("SELECT current_user", &[]).await?.get(0);
    let platform_owner = quote(client, &current_user).await?;
    client.batch_execute(&format!(
        "ALTER DATABASE {database} OWNER TO {platform_owner}; ALTER DATABASE {database} CONNECTION LIMIT {limit}; REVOKE ALL ON DATABASE {database} FROM PUBLIC"
    )).await?;
    for role in std::iter::once(&namespace.owner)
        .chain(std::iter::once(&namespace.maintenance_role))
        .chain(
            namespace
                .node_logins
                .iter()
                .flat_map(|login| [&login.runtime, &login.readiness]),
        )
    {
        client
            .batch_execute(&format!(
                "GRANT CONNECT ON DATABASE {database} TO {}",
                quote(client, role).await?
            ))
            .await?;
    }
    for login in &namespace.node_logins {
        for role in [&login.runtime, &login.readiness] {
            let role = quote(client, role).await?;
            client
                .batch_execute(&format!(
                    "ALTER ROLE {role} IN DATABASE {database} SET statement_timeout = '{}ms'; \
                     ALTER ROLE {role} IN DATABASE {database} SET lock_timeout = '{}ms'; \
                     ALTER ROLE {role} IN DATABASE {database} SET idle_in_transaction_session_timeout = '{}ms'; \
                     ALTER ROLE {role} IN DATABASE {database} SET search_path = 'pg_catalog'",
                    manifest.limits.statement_timeout_ms,
                    manifest.limits.lock_timeout_ms,
                    manifest.limits.idle_in_transaction_timeout_ms,
                ))
                .await?;
        }
    }
    Ok(())
}

fn database_config(admin: &str, database: &str) -> Result<tokio_postgres::Config> {
    let mut config = tokio_postgres::Config::from_str(admin)?;
    config.dbname(database);
    Ok(config)
}

async fn converge_namespace(
    manifest: &PostgresProvisioningManifest,
    namespace: &DerivedNamespace,
    admin_url: &str,
) -> Result<()> {
    let config = database_config(admin_url, &namespace.database)?;
    let (mut client, connection) = config.connect(NoTls).await?;
    let task = tokio::spawn(connection);
    let tx = client.transaction().await?;
    let owner = quote(&tx, &namespace.owner).await?;
    let runtime = quote(&tx, &namespace.runtime_group).await?;
    let maintenance = quote(&tx, &namespace.maintenance_role).await?;
    tx.batch_execute(&format!(
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM {runtime}; \
         CREATE SCHEMA IF NOT EXISTS {PLATFORM_SCHEMA}; REVOKE ALL ON SCHEMA {PLATFORM_SCHEMA} FROM PUBLIC, {owner}, {runtime}; \
         GRANT USAGE ON SCHEMA {PLATFORM_SCHEMA} TO {maintenance}; \
         CREATE TABLE IF NOT EXISTS {PLATFORM_SCHEMA}.namespace_state (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), generation bigint NOT NULL, manifest_digest text NOT NULL); \
         CREATE TABLE IF NOT EXISTS {PLATFORM_SCHEMA}.migration_attempts (namespace text NOT NULL, module text NOT NULL, migration_version bigint NOT NULL CHECK (migration_version > 0), attempt bigint NOT NULL CHECK (attempt > 0), filename text NOT NULL, content_hash text NOT NULL, byte_length bigint NOT NULL CHECK (byte_length >= 0), bundle_digest text NOT NULL, deployment_digest text NOT NULL, state text NOT NULL CHECK (state IN ('started','succeeded','failed')), started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz, failure_code text CHECK (failure_code IS NULL OR length(failure_code) <= 64), PRIMARY KEY(namespace,module,migration_version,attempt), CHECK ((state='started' AND finished_at IS NULL) OR (state<>'started' AND finished_at IS NOT NULL))); \
         CREATE UNIQUE INDEX IF NOT EXISTS migration_attempt_one_success ON {PLATFORM_SCHEMA}.migration_attempts(namespace,module,migration_version) WHERE state='succeeded'; \
         CREATE TABLE IF NOT EXISTS {PLATFORM_SCHEMA}.migration_retry_approvals (approval_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, namespace text NOT NULL, module text NOT NULL, migration_version bigint NOT NULL, attempt bigint NOT NULL, state text NOT NULL CHECK (state IN ('started','failed')), content_hash text NOT NULL, operator_name text NOT NULL, reason text NOT NULL CHECK (length(reason) <= 1024), approved_at timestamptz NOT NULL DEFAULT clock_timestamp(), consumed_at timestamptz, UNIQUE(namespace,module,migration_version,attempt,state,content_hash,approval_id)); \
         REVOKE ALL ON ALL TABLES IN SCHEMA {PLATFORM_SCHEMA} FROM PUBLIC, {owner}, {runtime}; \
         GRANT SELECT,INSERT,UPDATE ON {PLATFORM_SCHEMA}.migration_attempts, {PLATFORM_SCHEMA}.migration_retry_approvals TO {maintenance}; \
         GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA {PLATFORM_SCHEMA} TO {maintenance}; \
         ALTER DEFAULT PRIVILEGES FOR ROLE {owner} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;"
    )).await?;
    for schema_name in &namespace.schemas {
        let schema = quote(&tx, schema_name).await?;
        let schema_literal = literal(&tx, schema_name).await?;
        let owner_literal = literal(&tx, &namespace.owner).await?;
        tx.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {owner}; ALTER SCHEMA {schema} OWNER TO {owner}; \
             DO $wr$ DECLARE object record; BEGIN \
               FOR object IN SELECT c.oid,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={schema_literal} AND c.relkind IN ('r','p','v','m','S','f') LOOP \
                 EXECUTE format('ALTER %s %s OWNER TO %I', CASE object.relkind WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE' END, object.oid::regclass, {owner_literal}); \
               END LOOP; \
               FOR object IN SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname={schema_literal} LOOP \
                 EXECUTE format('ALTER ROUTINE %s OWNER TO %I', object.oid::regprocedure, {owner_literal}); \
               END LOOP; \
             END $wr$; \
             REVOKE ALL ON SCHEMA {schema} FROM PUBLIC; REVOKE CREATE ON SCHEMA {schema} FROM {runtime}; GRANT USAGE ON SCHEMA {schema} TO {runtime}; \
             REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {schema} TO {runtime}; \
             REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM PUBLIC; GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {runtime}; \
             REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {schema} FROM PUBLIC; \
             ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} REVOKE ALL ON TABLES FROM PUBLIC; \
             ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO {runtime}; \
             ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} REVOKE ALL ON SEQUENCES FROM PUBLIC; \
             ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} GRANT USAGE,SELECT,UPDATE ON SEQUENCES TO {runtime};"
        )).await?;
        for login in &namespace.node_logins {
            let login_runtime = quote(&tx, &login.runtime).await?;
            let readiness = quote(&tx, &login.readiness).await?;
            tx.batch_execute(&format!(
                "REVOKE ALL ON SCHEMA {schema} FROM {login_runtime}, {readiness}; \
                 REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {login_runtime}, {readiness}; \
                 REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM {login_runtime}, {readiness}; \
                 REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {schema} FROM {login_runtime}, {readiness}"
            ))
            .await?;
        }
    }
    // Strip platform reads from every bounded verifier before granting the exact
    // desired set. This also removes grants left by nodes no longer present in
    // the manifest without deleting their externally administered roles.
    let readiness_roles = tx
        .query(
            "SELECT rolname FROM pg_roles WHERE rolname LIKE 'wr_ready\\_%' ESCAPE '\\'",
            &[],
        )
        .await?;
    for row in readiness_roles {
        let readiness_name: String = row.get(0);
        let readiness = quote(&tx, &readiness_name).await?;
        tx.batch_execute(&format!(
            "REVOKE ALL ON SCHEMA {PLATFORM_SCHEMA} FROM {readiness}; \
             REVOKE ALL ON ALL TABLES IN SCHEMA {PLATFORM_SCHEMA} FROM {readiness}"
        ))
        .await?;
    }
    for login in &namespace.node_logins {
        let login_runtime = quote(&tx, &login.runtime).await?;
        let readiness = quote(&tx, &login.readiness).await?;
        tx.batch_execute(&format!(
            "REVOKE ALL ON SCHEMA {PLATFORM_SCHEMA} FROM {login_runtime}, {readiness}; \
             REVOKE ALL ON ALL TABLES IN SCHEMA {PLATFORM_SCHEMA} FROM {login_runtime}, {readiness}; \
             GRANT USAGE ON SCHEMA {PLATFORM_SCHEMA} TO {readiness}; \
             GRANT SELECT ON {PLATFORM_SCHEMA}.namespace_state, {PLATFORM_SCHEMA}.migration_attempts TO {readiness}"
        )).await?;
    }
    tx.commit().await?;
    task.abort();
    let _ = manifest;
    Ok(())
}

async fn check_generation_replay(
    admin_url: &str,
    desired: &[DerivedNamespace],
    generation: u64,
    digest: &str,
) -> Result<()> {
    for namespace in desired {
        let config = database_config(admin_url, &namespace.database)?;
        if let Ok((client, connection)) = config.connect(NoTls).await {
            let task = tokio::spawn(connection);
            if let Ok(Some(row)) = client.query_opt(&format!("SELECT generation,manifest_digest FROM {PLATFORM_SCHEMA}.namespace_state WHERE singleton"), &[]).await {
                let previous: i64 = row.get(0);
                let previous_digest: String = row.get(1);
                ensure!(generation >= previous as u64, "provisioning generation regression");
                ensure!(generation != previous as u64 || digest == previous_digest, "same provisioning generation has a different manifest digest");
            }
            task.abort();
        }
    }
    Ok(())
}

async fn commit_generation(
    admin_url: &str,
    desired: &[DerivedNamespace],
    generation: u64,
    digest: &str,
) -> Result<()> {
    for namespace in desired {
        let config = database_config(admin_url, &namespace.database)?;
        let (client, connection) = config.connect(NoTls).await?;
        let task = tokio::spawn(connection);
        client.execute(&format!(
            "INSERT INTO {PLATFORM_SCHEMA}.namespace_state(singleton,generation,manifest_digest) VALUES(true,$1,$2) ON CONFLICT(singleton) DO UPDATE SET generation=EXCLUDED.generation,manifest_digest=EXCLUDED.manifest_digest"
        ), &[&(generation as i64), &digest]).await?;
        task.abort();
    }
    Ok(())
}

async fn install_and_reload(
    client: &Client,
    manifest: &PostgresProvisioningManifest,
    target: &Path,
    _previous: &[u8],
    bytes: &[u8],
) -> Result<()> {
    atomic_replace(target, bytes)?;
    reload_and_validate(client, manifest, target, bytes).await
}

async fn restore_and_reload(
    client: &Client,
    manifest: &PostgresProvisioningManifest,
    target: &Path,
    previous: &[u8],
) -> Result<()> {
    atomic_replace(target, previous)?;
    reload_and_validate(client, manifest, target, previous).await
}

fn backup_path(target: &Path) -> PathBuf {
    target.with_extension("wruntime.backup")
}

fn preserve_backup(target: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let path = backup_path(target);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    File::open(target.parent().context("pg_ident target has no parent")?)?.sync_all()?;
    Ok(path)
}

fn remove_backup(path: &Path) -> Result<()> {
    let parent = path.parent().context("pg_ident backup has no parent")?;
    std::fs::remove_file(path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
fn install_with_validation_seam<F>(target: &Path, bytes: &[u8], mut validate: F) -> Result<()>
where
    F: FnMut(&[u8], bool) -> Result<()>,
{
    let _lock = acquire_file_lock(target)?;
    let previous = std::fs::read(target)?;
    let backup = preserve_backup(target, &previous)?;
    atomic_replace(target, bytes)?;
    if let Err(original) = validate(bytes, false) {
        atomic_replace(target, &previous)?;
        if let Err(rollback) = validate(&previous, true) {
            bail!(
                "fatal/manual-intervention: activation failed: {original:#}; rollback validation failed: {rollback:#}"
            );
        }
        remove_backup(&backup)?;
        return Err(original.context("activation failed; exact prior bytes restored"));
    }
    remove_backup(&backup)?;
    Ok(())
}

fn atomic_replace(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target.parent().context("pg_ident target has no parent")?;
    let temporary = parent.join(format!(
        ".wruntime-pg-ident-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&temporary, target)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

async fn reload_and_validate(
    client: &Client,
    manifest: &PostgresProvisioningManifest,
    target: &Path,
    bytes: &[u8],
) -> Result<()> {
    let before: std::time::SystemTime = client
        .query_one("SELECT pg_conf_load_time()", &[])
        .await?
        .get(0);
    let reloaded: bool = client
        .query_one("SELECT pg_reload_conf()", &[])
        .await?
        .get(0);
    ensure!(reloaded, "pg_reload_conf returned false");
    let deadline = tokio::time::Instant::now() + RELOAD_DEADLINE;
    loop {
        let after: std::time::SystemTime = client
            .query_one("SELECT pg_conf_load_time()", &[])
            .await?
            .get(0);
        if after > before {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for PostgreSQL configuration reload"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let expected =
        read_managed_mappings_from_text(std::str::from_utf8(bytes)?, &manifest.ident_map_name)?;
    let rows = client
        .query(
            "SELECT map_name,sys_name,pg_username,COALESCE(error,'') FROM pg_ident_file_mappings",
            &[],
        )
        .await?;
    ensure!(
        rows.iter().all(|row| row.get::<_, String>(3).is_empty()),
        "reloaded pg_ident contains parse errors"
    );
    let actual = rows
        .into_iter()
        .filter_map(|row| {
            let mapping = IdentMapping {
                map: row.get(0),
                system_user: row.get(1),
                database_user: row.get(2),
            };
            (mapping.map == manifest.ident_map_name).then_some(mapping)
        })
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "active pg_ident mappings differ from installed snapshot at {}",
        target.display()
    );
    validate_hba(client, manifest).await?;
    Ok(())
}

fn read_managed_mappings_from_text(text: &str, map_name: &str) -> Result<BTreeSet<IdentMapping>> {
    let temporary = tempfile::NamedTempFile::new()?;
    std::fs::write(temporary.path(), text)?;
    read_managed_mappings(temporary.path(), map_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hba_rule(
        line: i32,
        kind: &str,
        users: &str,
        address: &str,
        netmask: &str,
        auth_method: &str,
        options: &str,
    ) -> HbaRule {
        HbaRule {
            line,
            kind: kind.into(),
            users: users.into(),
            address: HbaAddress::parse(address, netmask).unwrap(),
            auth_method: auth_method.into(),
            options: options.into(),
        }
    }

    #[test]
    fn local_migration_executor_hba_requires_a_non_bypassed_scram_rule() {
        let postgres_peer = LocalHbaRule {
            line: 1,
            databases: "{all}".into(),
            users: "{postgres}".into(),
            auth_method: "peer".into(),
        };
        let executor_scram = LocalHbaRule {
            line: 2,
            databases: "{all}".into(),
            users: format!("{{+{MIGRATION_EXECUTOR_AUTH_MARKER_ROLE}}}"),
            auth_method: "scram-sha-256".into(),
        };
        validate_migration_executor_hba_rules(&[postgres_peer.clone(), executor_scram.clone()])
            .unwrap();

        assert!(
            validate_migration_executor_hba_rules(std::slice::from_ref(&postgres_peer)).is_err()
        );
        let broad_trust = LocalHbaRule {
            line: 1,
            databases: "{all}".into(),
            users: "{all}".into(),
            auth_method: "trust".into(),
        };
        let error =
            validate_migration_executor_hba_rules(&[broad_trust, executor_scram]).unwrap_err();
        assert!(error
            .to_string()
            .contains("earlier local pg_hba rule bypasses"));
    }

    #[test]
    fn postgres_hba_address_and_netmask_normalize_to_exact_networks() {
        assert_eq!(
            NetworkIdentity::from_postgres("127.0.0.1", "255.255.255.255").unwrap(),
            NetworkIdentity::parse_cidr("127.0.0.1/32").unwrap()
        );
        assert_eq!(
            NetworkIdentity::from_postgres("172.16.0.0", "255.240.0.0").unwrap(),
            NetworkIdentity::parse_cidr("172.16.0.0/12").unwrap()
        );
        assert_eq!(
            NetworkIdentity::from_postgres("0.0.0.0", "0.0.0.0").unwrap(),
            NetworkIdentity::parse_cidr("0.0.0.0/0").unwrap()
        );
        assert_eq!(
            NetworkIdentity::from_postgres("2001:db8::1", "ffff:ffff:ffff:ffff::").unwrap(),
            NetworkIdentity::parse_cidr("2001:db8::/64").unwrap()
        );
        assert!(NetworkIdentity::from_postgres("127.0.0.1", "255.0.255.0").is_err());
        assert!(NetworkIdentity::from_postgres("127.0.0.1", "ffff:ffff::").is_err());
    }

    #[test]
    fn hba_exact_match_and_covering_bypass_use_normalized_network_identity() {
        let exact = hba_rule(
            20,
            "hostssl",
            "+wr__tenant_client_auth",
            "172.16.0.0",
            "255.240.0.0",
            "cert",
            "{map=wruntime_nodes,clientcert=verify-full}",
        );
        validate_hba_rules(
            std::slice::from_ref(&exact),
            &["172.16.0.0/12".into()],
            "wruntime_nodes",
        )
        .unwrap();

        let narrower_non_bypass = hba_rule(
            10,
            "host",
            "all",
            "172.16.0.0",
            "255.255.0.0",
            "scram-sha-256",
            "",
        );
        validate_hba_rules(
            &[narrower_non_bypass, exact.clone()],
            &["172.16.0.0/12".into()],
            "wruntime_nodes",
        )
        .unwrap();

        let covering_bypass =
            hba_rule(10, "host", "all", "0.0.0.0", "0.0.0.0", "scram-sha-256", "");
        let error = validate_hba_rules(
            &[covering_bypass, exact],
            &["172.16.0.0/12".into()],
            "wruntime_nodes",
        )
        .unwrap_err();
        assert!(error.to_string().contains("earlier pg_hba rule bypasses"));

        let wrong_network = hba_rule(
            20,
            "hostssl",
            "+wr__tenant_client_auth",
            "172.16.0.0",
            "255.255.0.0",
            "cert",
            "{map=wruntime_nodes}",
        );
        let error = validate_hba_rules(
            &[wrong_network],
            &["172.16.0.0/12".into()],
            "wruntime_nodes",
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("missing exact cert/map HBA rule"));

        let exact_v6 = hba_rule(
            20,
            "hostssl",
            "+wr__tenant_client_auth",
            "2001:db8::",
            "ffff:ffff:ffff:ffff::",
            "cert",
            "{map=wruntime_nodes}",
        );
        validate_hba_rules(
            std::slice::from_ref(&exact_v6),
            &["2001:db8::/64".into()],
            "wruntime_nodes",
        )
        .unwrap();
        let all_v6 = hba_rule(10, "host", "all", "::", "::", "trust", "");
        let error = validate_hba_rules(
            &[all_v6, exact_v6],
            &["2001:db8::/64".into()],
            "wruntime_nodes",
        )
        .unwrap_err();
        assert!(error.to_string().contains("earlier pg_hba rule bypasses"));
    }

    #[test]
    fn managed_mapping_union_is_sorted_and_rejects_unmanaged_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pg_ident.conf");
        std::fs::write(
            &path,
            format!("{IDENT_HEADER}\nwruntime_nodes node-b wr_runtime_b\nwruntime_nodes node-a wr_runtime_a\n"),
        )
        .unwrap();
        let mappings = read_managed_mappings(&path, "wruntime_nodes").unwrap();
        let rendered = render_mappings(&mappings);
        assert!(rendered.find("node-a").unwrap() < rendered.find("node-b").unwrap());
        std::fs::write(&path, "other map role\n").unwrap();
        assert!(read_managed_mappings(&path, "wruntime_nodes").is_err());
    }

    #[test]
    fn identity_target_guards_and_lock_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("pg_ident.conf");
        std::fs::write(&target, "").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        let uid = std::fs::metadata(&target).unwrap().uid();
        validate_identity_target(&target, &target, uid).unwrap();
        assert!(validate_identity_target(&target, &target, uid.wrapping_add(1)).is_err());
        let other = directory.path().join("other.conf");
        std::fs::write(&other, "").unwrap();
        assert!(validate_identity_target(&target, &other, uid).is_err());
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(validate_identity_target(&target, &target, uid).is_err());
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        let symlink = directory.path().join("identity-link");
        std::os::unix::fs::symlink(&target, &symlink).unwrap();
        assert!(validate_identity_target(&symlink, &target, uid).is_err());

        let first = acquire_file_lock(&target).unwrap();
        assert!(acquire_file_lock(&target).is_err());
        drop(first);
        acquire_file_lock(&target).unwrap();
    }

    #[test]
    fn installer_seam_activates_and_restores_exact_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("pg_ident.conf");
        let previous = format!("{IDENT_HEADER}\nwruntime_nodes old wr_runtime_old\n");
        let desired = format!("{IDENT_HEADER}\nwruntime_nodes new wr_runtime_new\n");
        std::fs::write(&target, &previous).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        let uid = std::fs::metadata(&target).unwrap().uid();

        install_with_validation_seam(&target, desired.as_bytes(), |active, rollback| {
            ensure!(!rollback);
            ensure!(active == desired.as_bytes());
            Ok(())
        })
        .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), desired.as_bytes());
        assert_eq!(std::fs::metadata(&target).unwrap().uid(), uid);
        assert_eq!(std::fs::metadata(&target).unwrap().mode() & 0o777, 0o600);
        assert!(!backup_path(&target).exists());

        std::fs::write(&target, &previous).unwrap();
        let error =
            install_with_validation_seam(&target, desired.as_bytes(), |active, rollback| {
                if rollback {
                    ensure!(active == previous.as_bytes());
                    Ok(())
                } else {
                    bail!("simulated reload parse mismatch")
                }
            })
            .unwrap_err();
        assert!(error.to_string().contains("exact prior bytes restored"));
        assert_eq!(std::fs::read(&target).unwrap(), previous.as_bytes());
        assert!(!backup_path(&target).exists());
    }

    #[test]
    fn installer_seam_reports_fatal_unverified_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("pg_ident.conf");
        let previous = format!("{IDENT_HEADER}\n");
        std::fs::write(&target, &previous).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        let error = install_with_validation_seam(
            &target,
            format!("{IDENT_HEADER}\nwruntime_nodes new wr_ready_new\n").as_bytes(),
            |_active, _rollback| bail!("simulated validation failure"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("fatal/manual-intervention"));
        assert_eq!(std::fs::read(&target).unwrap(), previous.as_bytes());
        assert!(backup_path(&target).exists());
    }

    fn certificate_manifest(
        path: &Path,
        leaf_fingerprint: String,
        ca_fingerprint: String,
    ) -> PostgresProvisioningManifest {
        use wr_common::postgres::{
            PlatformDatabases, PostgresProvisioningLimits, PostgresProvisioningNamespace,
            PostgresProvisioningNode,
        };
        PostgresProvisioningManifest {
            format_version: 1,
            cluster_id: "cluster-a".into(),
            generation: 1,
            postgres_major: 18,
            postgres_ca_sha256: ca_fingerprint,
            ident_map_name: "wruntime_nodes".into(),
            platform_databases: PlatformDatabases {
                manager: "manager".into(),
                job_queues: vec!["jobs".into()],
            },
            extension_allowlist: vec![],
            tenant_client_cidrs: vec!["127.0.0.1/32".into()],
            limits: PostgresProvisioningLimits {
                namespace_database_connections: 10,
                runtime_login_connections: 2,
                readiness_verifier_connections: 1,
                statement_timeout_ms: 1,
                lock_timeout_ms: 1,
                idle_in_transaction_timeout_ms: 1,
            },
            nodes: vec![PostgresProvisioningNode {
                node_id: "node-a".into(),
                certificate_pem_path: path.display().to_string(),
                certificate_sha256: leaf_fingerprint,
                certificate_common_name: "wr-db-node-a".into(),
            }],
            namespaces: vec![PostgresProvisioningNamespace {
                namespace: "shop".into(),
                modules: vec!["catalog".into()],
            }],
        }
    }

    #[test]
    fn certificate_preflight_requires_dedicated_profile_and_ca() {
        use rcgen::{
            BasicConstraints, CertificateParams, DistinguishedName, DnType,
            ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
        };
        let mut ca_params = CertificateParams::new(vec![]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::from_params(&ca_params, ca_key);
        let leaf_key = KeyPair::generate().unwrap();
        let mut leaf_params = CertificateParams::new(vec![]).unwrap();
        leaf_params.subject_alt_names = vec![SanType::URI(
            "urn:wruntime:cluster-a:postgres-client:node-a"
                .try_into()
                .unwrap(),
        )];
        leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, "wr-db-node-a");
        leaf_params.distinguished_name = distinguished_name;
        let leaf = leaf_params.signed_by(&leaf_key, &issuer).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.pem");
        std::fs::write(&path, format!("{}{}", leaf.pem(), ca.pem())).unwrap();
        let manifest = certificate_manifest(
            &path,
            wr_common::tls::certificate_fingerprint_sha256(leaf.der()),
            wr_common::tls::certificate_fingerprint_sha256(ca.der()),
        );
        validate_node_certificates(&manifest).unwrap();

        let mut wrong = manifest;
        wrong.nodes[0].certificate_common_name = "another-node".into();
        assert!(validate_node_certificates(&wrong).is_err());
    }

    fn deployment_receipt_fixture() -> (
        crate::cmd::node::NodeDeploymentReservationV1,
        PostgresProvisioningManifest,
        wr_common::migration_bundle::MigrationBundleManifest,
    ) {
        use crate::cmd::deploy_config::{DeployFormat, TenantDeployConfig};
        use wr_common::migration_bundle::{MigrationFileManifest, MigrationLimits};
        let revision_digest = format!("sha256:{}", "1".repeat(64));
        let fingerprint = format!("sha256:{}", "2".repeat(64));
        let reservation = crate::cmd::node::NodeDeploymentReservationV1 {
            schema_version: 1,
            manager_endpoint: "https://manager.internal:9000".into(),
            node_id: "node-a".into(),
            request_token: "stable-token".into(),
            remote_host_ip: "10.0.0.4".into(),
            deployment_format: DeployFormat::Systemd,
            bundle_digest: format!("sha256:{}", "3".repeat(64)),
            canonical_inventory_digest: format!("sha256:{}", "4".repeat(64)),
            allocated_revision: 7,
            operation_id: wr_common::deployment_contract::deployment_operation_id(&revision_digest)
                .unwrap(),
            revision_digest: revision_digest.clone(),
            tenant_database: Some(TenantDeployConfig {
                server_name: "postgres.internal".into(),
                host_addr: Some("10.0.0.15".into()),
                port: 5432,
                connect_timeout_secs: 10,
            }),
            postgres_client_leaf_fingerprint: Some(fingerprint.clone()),
        };
        let provisioning = certificate_manifest(
            Path::new("/operator/public-node.pem"),
            fingerprint,
            format!("sha256:{}", "5".repeat(64)),
        );
        let bundle = wr_common::migration_bundle::MigrationBundleManifest {
            format_version: 1,
            deployment_digest: revision_digest,
            bundle_digest: format!("sha256:{}", "6".repeat(64)),
            limits: MigrationLimits {
                max_migrations_per_namespace: 8,
                max_file_bytes: 1024,
                max_startup_bytes: 4096,
                file_deadline_ms: 1000,
                cancellation_grace_ms: 100,
            },
            files: vec![MigrationFileManifest {
                namespace: "shop".into(),
                module: "catalog".into(),
                version: 1,
                filename: "V1__catalog.sql".into(),
                content_hash: format!("sha256:{}", "7".repeat(64)),
                byte_length: 9,
            }],
        };
        (reservation, provisioning, bundle)
    }

    #[test]
    fn deployment_receipt_binding_rejects_bundle_revision_reservation_and_fingerprint_drift() {
        let (reservation, provisioning, bundle) = deployment_receipt_fixture();
        validate_deployment_receipt_binding(
            &reservation.bundle_digest,
            &reservation,
            &bundle,
            &provisioning,
        )
        .unwrap();
        assert!(validate_deployment_receipt_binding(
            &format!("sha256:{}", "8".repeat(64)),
            &reservation,
            &bundle,
            &provisioning,
        )
        .unwrap_err()
        .to_string()
        .contains("node bundle"));
        let mut wrong_revision = bundle.clone();
        wrong_revision.deployment_digest = format!("sha256:{}", "9".repeat(64));
        assert!(validate_deployment_receipt_binding(
            &reservation.bundle_digest,
            &reservation,
            &wrong_revision,
            &provisioning,
        )
        .unwrap_err()
        .to_string()
        .contains("deployment digest"));
        let mut omitted = reservation.clone();
        omitted.postgres_client_leaf_fingerprint = None;
        assert!(validate_deployment_receipt_binding(
            &omitted.bundle_digest,
            &omitted,
            &bundle,
            &provisioning,
        )
        .unwrap_err()
        .to_string()
        .contains("omitted postgres-client fingerprint"));
        let mut wrong_fingerprint = provisioning.clone();
        wrong_fingerprint.nodes[0].certificate_sha256 = format!("sha256:{}", "a".repeat(64));
        assert!(validate_deployment_receipt_binding(
            &reservation.bundle_digest,
            &reservation,
            &bundle,
            &wrong_fingerprint,
        )
        .unwrap_err()
        .to_string()
        .contains("certificate fingerprint"));
    }

    #[test]
    fn receipt_requires_verified_state_and_replay_is_atomic_deterministic_and_redacted() {
        let (reservation, provisioning, bundle) = deployment_receipt_fixture();
        let directory = tempfile::tempdir().unwrap();
        let refused = directory.path().join("refused/tenant-state.json");
        let error = write_verified_tenant_receipt(
            Err(anyhow::anyhow!("generation is stale")),
            &refused,
            reservation.clone(),
            &provisioning,
            &bundle,
        )
        .unwrap_err();
        assert!(error.to_string().contains("refusing tenant receipt"));
        assert!(!refused.exists());

        let first = directory.path().join("first/tenant-state.json");
        let replay = directory.path().join("replay/tenant-state.json");
        write_verified_tenant_receipt(Ok(()), &first, reservation.clone(), &provisioning, &bundle)
            .unwrap();
        write_verified_tenant_receipt(Ok(()), &replay, reservation, &provisioning, &bundle)
            .unwrap();
        let first_bytes = std::fs::read(&first).unwrap();
        assert_eq!(first_bytes, std::fs::read(&replay).unwrap());
        assert_eq!(std::fs::metadata(&first).unwrap().mode() & 0o777, 0o644);
        let text = String::from_utf8(first_bytes).unwrap();
        let mut wrong_reservation: TenantStateReceiptV1 = serde_json::from_str(&text).unwrap();
        wrong_reservation.reservation.remote_host_ip = "10.0.0.99".into();
        assert!(wrong_reservation
            .validate()
            .unwrap_err()
            .to_string()
            .contains("reservation digest mismatch"));
        for forbidden in [
            "admin_url",
            "admin-url",
            "postgres://admin:secret",
            "pg_ident",
            "private_key",
            "key.pem",
        ] {
            assert!(!text.contains(forbidden), "receipt leaked {forbidden}");
        }
        for parent in [first.parent().unwrap(), replay.parent().unwrap()] {
            assert!(parent.read_dir().unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tenant-state-")));
        }
        let before = std::fs::read(&first).unwrap();
        assert!(write_verified_tenant_receipt(
            Ok(()),
            &first,
            deployment_receipt_fixture().0,
            &provisioning,
            &bundle,
        )
        .is_err());
        assert_eq!(before, std::fs::read(&first).unwrap());
    }

    #[test]
    fn admin_url_file_is_regular_owner_only_and_redacted_from_errors() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("admin-url");
        std::fs::write(&path, "postgres://admin:secret@localhost/db\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let secret = SecretAdminUrl::read(&path).unwrap();
        assert_eq!(
            wr_common::pool::redact_database_url(secret.expose()),
            "postgres://admin:***@localhost/db"
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(SecretAdminUrl::read(&path).is_err());
    }
}
