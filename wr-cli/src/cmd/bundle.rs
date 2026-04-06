//! Shared tar/bundle utilities used by both `managers` and `node` commands.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

/// Add a file from disk to the tarball, computing its SHA256 checksum.
pub fn tar_add_file(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    checksums: &mut HashMap<String, String>,
    archive_path: &str,
    src_path: &Path,
    mode: u32,
) -> Result<()> {
    let data =
        fs::read(src_path).with_context(|| format!("failed to read {}", src_path.display()))?;
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(mode);
    header.set_cksum();
    tar.append_data(&mut header, archive_path, data.as_slice())?;
    let hash = format!("{:x}", Sha256::digest(&data));
    checksums.insert(archive_path.to_string(), hash);
    Ok(())
}

/// Add raw bytes to the tarball.
pub fn tar_add_bytes(
    tar: &mut tar::Builder<GzEncoder<fs::File>>,
    archive_path: &str,
    data: &[u8],
    mode: u32,
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(mode);
    header.set_cksum();
    tar.append_data(&mut header, archive_path, data)?;
    Ok(())
}

/// Read and deserialize `manifest.json` from a gzipped tarball.
pub fn read_manifest<T: DeserializeOwned>(path: &str) -> Result<T> {
    read_file_from_tarball(path, "manifest.json")
        .and_then(|content| serde_json::from_str(&content).context("failed to parse manifest.json"))
        .context("manifest.json not found or invalid in bundle")
}

/// Read a single file from a gzipped tarball by matching the end of the archive path.
pub fn read_file_from_tarball(path: &str, target_file: &str) -> Result<String> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path.ends_with(target_file) {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            return Ok(content);
        }
    }

    bail!("{target_file} not found in bundle")
}

/// Read files from the tarball whose archive path contains `dir_prefix` and ends with `suffix`.
/// Returns `(archive_path, content)` pairs.
pub fn list_files_from_tarball(
    path: &str,
    dir_prefix: &str,
    suffix: &str,
) -> Result<Vec<(String, String)>> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    let mut files = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path.contains(dir_prefix) && entry_path.ends_with(suffix) {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            files.push((entry_path, content));
        }
    }

    Ok(files)
}

/// Read all TOML config files from a bundle's config/ directory.
/// Returns `(filename, content)` pairs.
pub fn read_configs_from_tarball(path: &str) -> Result<Vec<(String, String)>> {
    let file = fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    let mut configs = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_string_lossy().to_string();
        if entry_path.contains("/config/") && entry_path.ends_with(".toml") {
            let name = entry_path
                .rsplit('/')
                .next()
                .unwrap_or(&entry_path)
                .to_string();
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)?;
            configs.push((name, content));
        }
    }

    Ok(configs)
}
