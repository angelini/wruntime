//! Canonical node bundle identity and payload verification shared by bundle
//! inspection and the host executor.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const RESOLVED_MANIFEST_VERSION: u32 = 1;
pub const RESOLVED_MANIFEST_FILE: &str = "resolved-release.json";
pub const RESOLVED_DIGEST_FILE: &str = "resolved-release.sha256";
const RESOLVED_DIGEST_DOMAIN: &[u8] = b"wruntime.resolved-release.v1\0";

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolvedFile {
    pub sha256: String,
    pub mode: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolvedReleaseManifest {
    pub version: u32,
    pub node_id: String,
    pub revision: u64,
    pub backend: String,
    pub bundle_digest: String,
    pub files: BTreeMap<String, ResolvedFile>,
}

impl ResolvedReleaseManifest {
    pub fn digest(&self) -> Result<String> {
        anyhow::ensure!(
            self.version == RESOLVED_MANIFEST_VERSION,
            "unsupported resolved release manifest version"
        );
        let canonical = serde_json::to_vec(self)?;
        let mut hash = Sha256::new();
        hash.update(RESOLVED_DIGEST_DOMAIN);
        hash.update(canonical);
        Ok(format!("sha256:{:x}", hash.finalize()))
    }
}

use super::bundle;

#[derive(Serialize, Deserialize, Clone)]
pub struct BundleManifest {
    pub target: String,
    pub bundle_digest: String,
    pub engines: Vec<ManifestEngine>,
    pub workdir: String,
    pub image_prefix: String,
    pub modules: Vec<ManifestModule>,
    pub configs: Vec<String>,
    pub template_vars: Vec<String>,
    pub checksums: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precompile_hash: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ManifestEngine {
    pub engine_slot: String,
    pub modules: Vec<ManifestModule>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ManifestModule {
    pub name: String,
    pub namespace: String,
    pub version: String,
    #[serde(default)]
    pub has_schema: bool,
}

pub fn deterministic_bundle_digest(
    target: &str,
    workdir: &str,
    image_prefix: &str,
    engines: &[ManifestEngine],
    checksums: &BTreeMap<String, String>,
    precompile_hash: &Option<String>,
) -> Result<String> {
    let checksums: Vec<_> = checksums.iter().collect();
    let mut engines = engines.to_vec();
    engines.sort_by(|left, right| left.engine_slot.cmp(&right.engine_slot));
    for engine in &mut engines {
        engine.modules.sort_by(|left, right| {
            (&left.namespace, &left.name, &left.version).cmp(&(
                &right.namespace,
                &right.name,
                &right.version,
            ))
        });
    }
    let canonical = serde_json::json!({
        "target": target,
        "workdir": workdir,
        "image_prefix": image_prefix,
        "engines": engines,
        "checksums": checksums,
        "precompile_hash": precompile_hash,
    });
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&canonical)?)
    ))
}

pub fn verify_manifest_identity(manifest: &BundleManifest) -> Result<()> {
    let digest = deterministic_bundle_digest(
        &manifest.target,
        &manifest.workdir,
        &manifest.image_prefix,
        &manifest.engines,
        &manifest.checksums,
        &manifest.precompile_hash,
    )?;
    if digest != manifest.bundle_digest {
        bail!(
            "bundle digest mismatch: manifest declares {}, computed {digest}",
            manifest.bundle_digest
        );
    }
    Ok(())
}

pub fn verify_bundle_archive(bundle_path: &str, manifest: &BundleManifest) -> Result<()> {
    let actual: BTreeMap<_, _> = bundle::read_payload_checksums(bundle_path)?
        .into_iter()
        .collect();
    compare_payloads(&manifest.checksums, &actual)?;
    verify_manifest_identity(manifest)
}

pub fn verify_release_directory(release: &Path, manifest: &BundleManifest) -> Result<()> {
    for archive_path in manifest.checksums.keys() {
        let relative = archive_path
            .strip_prefix("wr-node/")
            .context("node bundle payload path is outside wr-node")?;
        validate_relative_path(relative)?;
    }
    let mut actual = BTreeMap::new();
    collect_release_payloads(release, release, &mut actual)?;
    compare_payloads(&manifest.checksums, &actual)?;
    verify_manifest_identity(manifest)
}

fn collect_release_payloads(
    release: &Path,
    directory: &Path,
    actual: &mut BTreeMap<String, String>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("release directory {} is unreadable", directory.display()))?
    {
        let entry = entry.context("release directory entry is unreadable")?;
        let file_type = entry
            .file_type()
            .context("release entry type is unavailable")?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_release_payloads(release, &path, actual)?;
            continue;
        }
        if !file_type.is_file() {
            bail!("release payload {} is not a regular file", path.display());
        }
        let relative = path
            .strip_prefix(release)
            .context("release payload escapes release root")?;
        if relative == Path::new("manifest.json") || relative == Path::new("bundle.sha256") {
            continue;
        }
        let relative = relative
            .to_str()
            .context("release payload path is not UTF-8")?;
        validate_relative_path(relative)?;
        let archive_path = format!("wr-node/{relative}");
        let bytes = std::fs::read(&path)
            .with_context(|| format!("release payload {} is unavailable", path.display()))?;
        actual.insert(archive_path, format!("{:x}", Sha256::digest(bytes)));
    }
    Ok(())
}

fn compare_payloads(
    expected: &BTreeMap<String, String>,
    actual: &BTreeMap<String, String>,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }
    let missing = expected
        .keys()
        .filter(|path| !actual.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    let changed = expected
        .iter()
        .filter(|(path, checksum)| actual.get(*path) != Some(*checksum))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    let unexpected = actual
        .keys()
        .filter(|path| !expected.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    bail!(
        "bundle payload checksums do not match manifest (missing: {}; changed: {}; unexpected: {})",
        missing.join(", "),
        changed.join(", "),
        unexpected.join(", ")
    )
}

fn validate_relative_path(value: &str) -> Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("manifest payload path escapes release root");
    }
    Ok(())
}

fn collect_resolved_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, ResolvedFile>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory).with_context(|| {
        format!(
            "resolved release directory {} is unreadable",
            directory.display()
        )
    })? {
        let entry = entry.context("resolved release directory entry is unreadable")?;
        let file_type = entry
            .file_type()
            .context("resolved release entry type is unavailable")?;
        let path = entry.path();
        if file_type.is_symlink() {
            bail!("resolved release payload {} is a symlink", path.display());
        }
        if file_type.is_dir() {
            collect_resolved_files(root, &path, files)?;
            continue;
        }
        if !file_type.is_file() {
            bail!(
                "resolved release payload {} is not a regular file",
                path.display()
            );
        }
        let relative = path
            .strip_prefix(root)
            .context("resolved release payload escapes release root")?;
        if relative == Path::new(RESOLVED_MANIFEST_FILE)
            || relative == Path::new(RESOLVED_DIGEST_FILE)
        {
            continue;
        }
        let relative = relative
            .to_str()
            .context("resolved release payload path is not UTF-8")?;
        validate_relative_path(relative)?;
        let bytes = std::fs::read(&path).with_context(|| {
            format!("resolved release payload {} is unavailable", path.display())
        })?;
        let value = ResolvedFile {
            sha256: format!("{:x}", Sha256::digest(bytes)),
            mode: entry.metadata()?.permissions().mode() & 0o777,
        };
        if files.insert(relative.to_string(), value).is_some() {
            bail!("resolved release manifest contains duplicate path {relative}");
        }
    }
    Ok(())
}

pub fn build_resolved_manifest(
    release: &Path,
    node_id: &str,
    revision: u64,
    backend: &str,
    bundle_digest: &str,
) -> Result<ResolvedReleaseManifest> {
    anyhow::ensure!(revision > 0, "resolved release revision must be non-zero");
    anyhow::ensure!(
        matches!(backend, "systemd" | "docker"),
        "resolved release backend is invalid"
    );
    let mut files = BTreeMap::new();
    collect_resolved_files(release, release, &mut files)?;
    Ok(ResolvedReleaseManifest {
        version: RESOLVED_MANIFEST_VERSION,
        node_id: node_id.to_string(),
        revision,
        backend: backend.to_string(),
        bundle_digest: bundle_digest.to_string(),
        files,
    })
}

pub fn write_resolved_identity(
    release: &Path,
    manifest: &ResolvedReleaseManifest,
) -> Result<String> {
    let digest = manifest.digest()?;
    std::fs::write(
        release.join(RESOLVED_MANIFEST_FILE),
        serde_json::to_vec_pretty(manifest)?,
    )?;
    std::fs::write(release.join(RESOLVED_DIGEST_FILE), format!("{digest}\n"))?;
    Ok(digest)
}

pub fn verify_resolved_release(
    release: &Path,
    source_bundle_digest: &str,
    resolved_release_digest: &str,
) -> Result<ResolvedReleaseManifest> {
    let bytes = std::fs::read(release.join(RESOLVED_MANIFEST_FILE))
        .context("resolved release manifest is unavailable")?;
    let manifest: ResolvedReleaseManifest =
        serde_json::from_slice(&bytes).context("resolved release manifest is invalid")?;
    anyhow::ensure!(
        manifest.version == RESOLVED_MANIFEST_VERSION,
        "unsupported resolved release manifest version"
    );
    anyhow::ensure!(
        manifest.bundle_digest == source_bundle_digest,
        "resolved release source bundle digest mismatch"
    );
    anyhow::ensure!(
        manifest.digest()? == resolved_release_digest,
        "resolved release digest mismatch"
    );
    let marker = std::fs::read_to_string(release.join(RESOLVED_DIGEST_FILE))
        .context("resolved release digest marker is unavailable")?;
    anyhow::ensure!(
        marker.trim() == resolved_release_digest,
        "resolved release digest marker mismatch"
    );
    let actual = build_resolved_manifest(
        release,
        &manifest.node_id,
        manifest.revision,
        &manifest.backend,
        &manifest.bundle_digest,
    )?;
    anyhow::ensure!(
        actual == manifest,
        "resolved release payload map does not match finalized bytes"
    );
    let source_marker = std::fs::read_to_string(release.join("bundle.sha256"))
        .context("source bundle digest marker is unavailable")?;
    anyhow::ensure!(
        source_marker.trim() == source_bundle_digest,
        "source bundle digest marker mismatch"
    );
    let source_manifest: BundleManifest = serde_json::from_slice(
        &std::fs::read(release.join("manifest.json"))
            .context("embedded source manifest is unavailable")?,
    )
    .context("embedded source manifest is invalid")?;
    anyhow::ensure!(
        source_manifest.bundle_digest == source_bundle_digest,
        "embedded source manifest identity mismatch"
    );
    verify_manifest_identity(&source_manifest)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_directory_verification_rejects_tampered_non_metadata_payload() {
        let root = std::env::temp_dir().join(format!(
            "wr-bundle-integrity-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("bin")).unwrap();
        std::fs::write(root.join("bin/wr-engine"), b"trusted").unwrap();
        let checksum = format!("{:x}", Sha256::digest(b"trusted"));
        let mut manifest = BundleManifest {
            target: "x86_64-unknown-linux-gnu".into(),
            bundle_digest: String::new(),
            engines: vec![],
            workdir: "/opt/wruntime".into(),
            image_prefix: "wr".into(),
            modules: vec![],
            configs: vec![],
            template_vars: vec![],
            checksums: BTreeMap::from([("wr-node/bin/wr-engine".into(), checksum)]),
            precompile_hash: None,
        };
        manifest.bundle_digest = deterministic_bundle_digest(
            &manifest.target,
            &manifest.workdir,
            &manifest.image_prefix,
            &manifest.engines,
            &manifest.checksums,
            &manifest.precompile_hash,
        )
        .unwrap();
        verify_release_directory(&root, &manifest).unwrap();
        std::fs::write(root.join("bin/wr-engine"), b"tampered").unwrap();
        assert!(verify_release_directory(&root, &manifest).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
