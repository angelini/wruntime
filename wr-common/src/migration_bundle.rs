//! Immutable, bounded module-migration bundles shared by release tooling and the offline runner.
//!
//! This module classifies only migration artifacts. Its lexical transaction-control check is
//! defense in depth and must never be used to authorize guest/runtime SQL.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::identity::{validate_name, Namespace};

pub const MIGRATION_BUNDLE_FORMAT_VERSION: u32 = 1;
const BUNDLE_DOMAIN: &[u8] = b"wruntime-migration-bundle-v1\0";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationLimits {
    pub max_migrations_per_namespace: usize,
    pub max_file_bytes: usize,
    pub max_startup_bytes: usize,
    pub file_deadline_ms: u64,
    pub cancellation_grace_ms: u64,
}

impl MigrationLimits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_migrations_per_namespace > 0
                && self.max_file_bytes > 0
                && self.max_startup_bytes > 0
                && self.file_deadline_ms > 0
                && self.cancellation_grace_ms > 0,
            "all migration limits must be nonzero"
        );
        ensure!(
            self.max_file_bytes <= self.max_startup_bytes,
            "maximum migration file size exceeds aggregate startup bytes"
        );
        Ok(())
    }

    pub fn file_deadline(&self) -> Duration {
        Duration::from_millis(self.file_deadline_ms)
    }

    pub fn cancellation_grace(&self) -> Duration {
        Duration::from_millis(self.cancellation_grace_ms)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationFileManifest {
    pub namespace: String,
    pub module: String,
    pub version: u64,
    pub filename: String,
    pub content_hash: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationBundleManifest {
    pub format_version: u32,
    pub deployment_digest: String,
    pub bundle_digest: String,
    pub limits: MigrationLimits,
    pub files: Vec<MigrationFileManifest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedMigrationFile {
    pub manifest: MigrationFileManifest,
    pub bytes: Arc<[u8]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationBundle {
    pub manifest: MigrationBundleManifest,
    pub files: Vec<CapturedMigrationFile>,
}

impl MigrationBundleManifest {
    pub fn parse_toml(input: &str) -> Result<Self> {
        let manifest: Self = toml::from_str(input).context("invalid migration bundle manifest")?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == MIGRATION_BUNDLE_FORMAT_VERSION,
            "unsupported migration bundle format_version"
        );
        validate_digest(&self.deployment_digest, "deployment_digest")?;
        validate_digest(&self.bundle_digest, "bundle_digest")?;
        self.limits.validate()?;

        let mut identities = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut counts = std::collections::BTreeMap::<&str, usize>::new();
        let mut total = 0usize;
        let mut previous: Option<(&str, &str, u64)> = None;
        for file in &self.files {
            Namespace::parse(file.namespace.clone())?;
            validate_name(&file.module, "module name")?;
            ensure!(file.version > 0, "migration version must be nonzero");
            validate_filename(&file.filename, file.version)?;
            validate_digest(&file.content_hash, "content_hash")?;
            let length = usize::try_from(file.byte_length).context("migration length overflow")?;
            ensure!(
                length <= self.limits.max_file_bytes,
                "migration file size limit exceeded"
            );
            total = total
                .checked_add(length)
                .context("migration byte total overflow")?;
            ensure!(
                total <= self.limits.max_startup_bytes,
                "aggregate migration byte limit exceeded"
            );
            let count = counts.entry(&file.namespace).or_default();
            *count += 1;
            ensure!(
                *count <= self.limits.max_migrations_per_namespace,
                "namespace migration count limit exceeded"
            );
            ensure!(
                identities.insert((&file.namespace, &file.module, file.version)),
                "duplicate migration identity"
            );
            ensure!(
                names.insert((&file.namespace, &file.module, &file.filename)),
                "duplicate canonical migration filename"
            );
            let current = (file.namespace.as_str(), file.module.as_str(), file.version);
            if let Some(previous) = previous {
                ensure!(
                    previous < current,
                    "migration files are not in canonical namespace/module/version order"
                );
            }
            previous = Some(current);
        }
        ensure!(
            self.computed_digest()? == self.bundle_digest,
            "migration bundle digest mismatch"
        );
        Ok(())
    }

    pub fn computed_digest(&self) -> Result<String> {
        let mut hash = Sha256::new();
        hash.update(BUNDLE_DOMAIN);
        hash.update(self.format_version.to_be_bytes());
        push(&mut hash, &self.deployment_digest)?;
        for value in [
            self.limits.max_migrations_per_namespace as u64,
            self.limits.max_file_bytes as u64,
            self.limits.max_startup_bytes as u64,
            self.limits.file_deadline_ms,
            self.limits.cancellation_grace_ms,
        ] {
            hash.update(value.to_be_bytes());
        }
        for file in &self.files {
            push(&mut hash, &file.namespace)?;
            push(&mut hash, &file.module)?;
            hash.update(file.version.to_be_bytes());
            push(&mut hash, &file.filename)?;
            push(&mut hash, &file.content_hash)?;
            hash.update(file.byte_length.to_be_bytes());
        }
        Ok(format!("sha256:{:x}", hash.finalize()))
    }

    /// Read every declared file exactly once after validating its path and all admission bounds.
    pub fn capture(&self, root: &Path) -> Result<MigrationBundle> {
        self.validate()?;
        let root = std::fs::canonicalize(root).context("canonicalizing migration bundle root")?;
        let mut files = Vec::with_capacity(self.files.len());
        for declared in &self.files {
            let relative = Path::new(&declared.namespace)
                .join(&declared.module)
                .join(&declared.filename);
            ensure!(
                relative
                    .components()
                    .all(|part| matches!(part, Component::Normal(_))),
                "migration path escape"
            );
            let path = root.join(&relative);
            let canonical = std::fs::canonicalize(&path)
                .with_context(|| format!("canonicalizing migration file {}", relative.display()))?;
            ensure!(
                canonical.starts_with(&root),
                "migration path escapes bundle root"
            );
            let bytes = std::fs::read(&canonical)
                .with_context(|| format!("reading migration file {}", relative.display()))?;
            ensure!(
                bytes.len() as u64 == declared.byte_length,
                "migration bytes changed length"
            );
            ensure!(
                digest(&bytes) == declared.content_hash,
                "migration bytes changed hash"
            );
            reject_transaction_control(&bytes)
                .with_context(|| format!("migration {}", relative.display()))?;
            files.push(CapturedMigrationFile {
                manifest: declared.clone(),
                bytes: Arc::from(bytes),
            });
        }
        Ok(MigrationBundle {
            manifest: self.clone(),
            files,
        })
    }

    /// Capture migration directories into a manifest and immutable bytes. Paths are represented by
    /// canonical relative names; the canonical filesystem path is never execution authority.
    pub fn capture_sources(
        deployment_digest: String,
        limits: MigrationLimits,
        sources: &[(String, String, PathBuf)],
    ) -> Result<MigrationBundle> {
        validate_digest(&deployment_digest, "deployment_digest")?;
        limits.validate()?;
        let mut captured = Vec::new();
        for (namespace, module, directory) in sources {
            Namespace::parse(namespace.clone())?;
            validate_name(module, "module name")?;
            let root = std::fs::canonicalize(directory).with_context(|| {
                format!("canonicalizing migration source for {namespace}.{module}")
            })?;
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                ensure!(
                    entry.file_type()?.is_file(),
                    "migration sources may contain only regular files"
                );
                let filename = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("migration filename is not UTF-8"))?;
                let version = parse_version(&filename)?;
                let canonical = std::fs::canonicalize(entry.path())?;
                ensure!(
                    canonical.parent() == Some(root.as_path()),
                    "migration source path escape"
                );
                let bytes = std::fs::read(&canonical)?;
                reject_transaction_control(&bytes)?;
                captured.push(CapturedMigrationFile {
                    manifest: MigrationFileManifest {
                        namespace: namespace.clone(),
                        module: module.clone(),
                        version,
                        filename,
                        content_hash: digest(&bytes),
                        byte_length: bytes.len() as u64,
                    },
                    bytes: Arc::from(bytes),
                });
            }
        }
        captured.sort_by(|left, right| {
            (
                &left.manifest.namespace,
                &left.manifest.module,
                left.manifest.version,
            )
                .cmp(&(
                    &right.manifest.namespace,
                    &right.manifest.module,
                    right.manifest.version,
                ))
        });
        let mut manifest = MigrationBundleManifest {
            format_version: MIGRATION_BUNDLE_FORMAT_VERSION,
            deployment_digest,
            bundle_digest: format!("sha256:{}", "0".repeat(64)),
            limits,
            files: captured.iter().map(|file| file.manifest.clone()).collect(),
        };
        manifest.bundle_digest = manifest.computed_digest()?;
        manifest.validate()?;
        Ok(MigrationBundle {
            manifest,
            files: captured,
        })
    }
}

fn push(hash: &mut Sha256, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).context("migration manifest value too large")?;
    hash.update(len.to_be_bytes());
    hash.update(value.as_bytes());
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn validate_digest(value: &str, field: &str) -> Result<()> {
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{field} must be sha256:<64 lowercase hex>"
    );
    Ok(())
}

fn validate_filename(filename: &str, version: u64) -> Result<()> {
    ensure!(
        Path::new(filename).components().count() == 1,
        "migration filename must be relative and canonical"
    );
    ensure!(
        parse_version(filename)? == version,
        "migration filename/version mismatch"
    );
    Ok(())
}

fn parse_version(filename: &str) -> Result<u64> {
    let rest = filename
        .strip_prefix('V')
        .context("migration filename must start with V")?;
    let (version, description) = rest
        .split_once("__")
        .context("migration filename must be V<version>__<name>.sql")?;
    ensure!(
        !version.is_empty() && !version.starts_with('0'),
        "migration version is not canonical"
    );
    let version = version
        .parse::<u64>()
        .context("invalid migration version")?;
    ensure!(
        description.ends_with(".sql")
            && description.len() > 4
            && description[..description.len() - 4]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "invalid migration filename"
    );
    Ok(version)
}

/// Reject top-level transaction-control statements while ignoring SQL strings, quoted identifiers,
/// comments, and dollar-quoted bodies. This intentionally is not a complete SQL parser.
pub fn reject_transaction_control(bytes: &[u8]) -> Result<()> {
    let sql = std::str::from_utf8(bytes).context("migration SQL must be UTF-8")?;
    for statement in top_level_statements(sql)? {
        let keyword = statement
            .trim_start()
            .split(|character: char| !character.is_ascii_alphabetic())
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(
            keyword.as_str(),
            "BEGIN" | "COMMIT" | "ROLLBACK" | "ABORT" | "START" | "END"
        ) {
            bail!("top-level transaction-control statement is forbidden: {keyword}");
        }
    }
    Ok(())
}

fn top_level_statements(sql: &str) -> Result<Vec<String>> {
    let bytes = sql.as_bytes();
    let mut statements = vec![String::new()];
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                let mut closed = false;
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if index + 1 < bytes.len() && bytes[index + 1] == quote {
                            index += 2;
                            continue;
                        }
                        index += 1;
                        closed = true;
                        break;
                    }
                    index += 1;
                }
                ensure!(closed, "unterminated SQL quote");
                statements.last_mut().unwrap().push(' ');
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                statements.last_mut().unwrap().push(' ');
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                let mut depth = 1usize;
                while index < bytes.len() && depth > 0 {
                    if bytes.get(index..index + 2) == Some(b"/*") {
                        depth += 1;
                        index += 2;
                    } else if bytes.get(index..index + 2) == Some(b"*/") {
                        depth -= 1;
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                ensure!(depth == 0, "unterminated SQL comment");
                statements.last_mut().unwrap().push(' ');
            }
            b'$' => {
                let tail = &sql[index + 1..];
                if let Some(end) = tail.find('$') {
                    let tag = &tail[..end];
                    if tag
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    {
                        let delimiter = format!("${tag}$");
                        let body = index + delimiter.len();
                        let close = sql[body..]
                            .find(&delimiter)
                            .context("unterminated dollar quote")?;
                        index = body + close + delimiter.len();
                        statements.last_mut().unwrap().push(' ');
                        continue;
                    }
                }
                statements.last_mut().unwrap().push('$');
                index += 1;
            }
            b';' => {
                statements.push(String::new());
                index += 1;
            }
            byte => {
                statements.last_mut().unwrap().push(char::from(byte));
                index += 1;
            }
        }
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_rejects_only_top_level_transaction_control() {
        for sql in [
            "BEGIN",
            " commit work",
            "ROLLBACK TO SAVEPOINT x",
            "START TRANSACTION",
            "END;",
            "ABORT;",
        ] {
            assert!(reject_transaction_control(sql.as_bytes()).is_err(), "{sql}");
        }
        for sql in [
            "SELECT 'BEGIN; COMMIT'",
            "SELECT \"ROLLBACK\" FROM x",
            "-- BEGIN\nSELECT 1",
            "/* COMMIT */ SELECT 1",
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN NULL; END $$",
        ] {
            assert!(reject_transaction_control(sql.as_bytes()).is_ok(), "{sql}");
        }
        for sql in ["SELECT 'unterminated", "SELECT \"unterminated"] {
            assert!(reject_transaction_control(sql.as_bytes()).is_err(), "{sql}");
        }
    }

    #[test]
    fn manifest_enforces_bounds_order_and_digest() {
        let limits = MigrationLimits {
            max_migrations_per_namespace: 1,
            max_file_bytes: 8,
            max_startup_bytes: 8,
            file_deadline_ms: 1,
            cancellation_grace_ms: 1,
        };
        let directory = tempfile::tempdir().unwrap();
        let module = directory.path().join("shop").join("catalog");
        std::fs::create_dir_all(&module).unwrap();
        std::fs::write(module.join("V1__one.sql"), "SELECT 1").unwrap();
        let bundle = MigrationBundleManifest::capture_sources(
            format!("sha256:{}", "a".repeat(64)),
            limits,
            &[("shop".into(), "catalog".into(), module)],
        )
        .unwrap();
        assert_eq!(bundle.files[0].bytes.as_ref(), b"SELECT 1");
        let mut changed = bundle.manifest.clone();
        changed.files[0].byte_length += 1;
        assert!(changed.validate().is_err());
    }
}
