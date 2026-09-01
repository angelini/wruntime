use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use wasmtime::{Config, Engine};

const WASM_TARGET: &str = "wasm32-wasip2";
const SHARED_WASM_TARGET_DIR: &str = "target/wasm-guests";
static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq)]
struct WasmBuildSpec {
    module_name: String,
    cargo_dir: PathBuf,
    source: PathBuf,
    destination: PathBuf,
}

/// Raise the file descriptor soft limit to avoid `ProcessFdQuotaExceeded` during
/// linking of large release binaries (wasmtime alone opens hundreds of `.rlib` files).
/// macOS defaults to a soft limit of 256 which is not enough.
pub fn raise_fd_limit() {
    #[cfg(unix)]
    {
        use std::io;
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        unsafe {
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
                let target = rlim.rlim_max.min(65536);
                if rlim.rlim_cur < target {
                    rlim.rlim_cur = target;
                    if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) != 0 {
                        eprintln!(
                            "warning: failed to raise fd limit: {}",
                            io::Error::last_os_error()
                        );
                    }
                }
            }
        }
    }
}

/// Minimal module config for build operations
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct BuildModule {
    pub name: String,
    pub wasm_path: String,
    #[serde(default)]
    pub schema_path: String,
    #[serde(default)]
    pub proto_path: Option<String>,
    #[serde(default)]
    pub cargo_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BuildManifest {
    #[serde(rename = "module", default)]
    pub modules: Vec<BuildModule>,
}

/// Derive .proto path from .binpb schema_path
pub fn derive_proto_path(schema_path: &str) -> String {
    if schema_path.ends_with(".binpb") {
        format!("{}proto", &schema_path[..schema_path.len() - 5])
    } else {
        format!("{schema_path}.proto")
    }
}

/// Derive Cargo project directory from wasm_path by finding the `target/` component
pub fn derive_cargo_dir(wasm_path: &str) -> Result<PathBuf> {
    let path = Path::new(wasm_path);
    let mut current = path;
    while let Some(parent) = current.parent() {
        if current.file_name().map(|n| n == "target").unwrap_or(false) {
            return Ok(parent.to_path_buf());
        }
        current = parent;
    }
    bail!(
        "Cannot derive Cargo project directory from wasm_path: {wasm_path}. \
         Expected a path containing 'target/' (e.g., my-module/target/wasm32-wasip2/debug/mod.wasm)"
    );
}

pub fn load_manifest(path: &str) -> Result<Vec<BuildModule>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read build manifest: {path}"))?;
    let manifest: BuildManifest = toml::from_str(&content)
        .with_context(|| format!("failed to parse build manifest: {path}"))?;
    Ok(manifest.modules)
}

pub fn module_proto_path(module: &BuildModule) -> Option<PathBuf> {
    if module.schema_path.is_empty() {
        return None;
    }
    Some(
        module
            .proto_path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(derive_proto_path(&module.schema_path))),
    )
}

pub fn module_cargo_dir(module: &BuildModule) -> Result<PathBuf> {
    module
        .cargo_dir
        .as_ref()
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(|| derive_cargo_dir(&module.wasm_path))
}

/// Compile .proto → .binpb for each module that has a schema_path
pub fn compile_schemas(modules: &[BuildModule]) -> Result<()> {
    for module in modules {
        if module.schema_path.is_empty() {
            continue;
        }
        let proto_path = module_proto_path(module).expect("schema_path is non-empty");
        if !proto_path.exists() {
            bail!(
                "Proto file not found for module '{}': {} (metadata schema_path '{}')",
                module.name,
                proto_path.display(),
                module.schema_path,
            );
        }
        let proto_dir = proto_path
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let descriptor_arg = format!("--descriptor_set_out={}", module.schema_path);
        let proto_path_arg = format!("--proto_path={proto_dir}");
        let proto_file_arg = proto_path.to_string_lossy().to_string();
        print!("[schema]  {} ... ", module.schema_path);
        let status = Command::new("protoc")
            .args([
                descriptor_arg.as_str(),
                "--include_imports",
                proto_path_arg.as_str(),
                proto_file_arg.as_str(),
            ])
            .status()
            .context("failed to run protoc; ensure protoc is installed")?;
        if !status.success() {
            bail!("protoc failed for module '{}'", module.name);
        }
        println!("OK");
    }
    Ok(())
}

fn repository_root() -> Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .context("wr-cli CARGO_MANIFEST_DIR has no repository parent")?;
    root.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize repository root derived from {}",
            manifest_dir.display()
        )
    })
}

fn shared_wasm_target_dir(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        bail!("repository root must be absolute: {}", root.display());
    }
    Ok(root.join(SHARED_WASM_TARGET_DIR))
}

fn wasm_profile(release: bool) -> &'static str {
    if release {
        "release"
    } else {
        "debug"
    }
}

fn wasm_filename(wasm_path: &str) -> Result<&std::ffi::OsStr> {
    let filename = Path::new(wasm_path)
        .file_name()
        .with_context(|| format!("WASM destination has no filename: {wasm_path}"))?;
    if !filename.to_string_lossy().ends_with(".wasm") {
        bail!("WASM destination filename must end in .wasm: {wasm_path}");
    }
    Ok(filename)
}

fn shared_source_artifact(
    shared_target: &Path,
    release: bool,
    filename: &std::ffi::OsStr,
) -> PathBuf {
    shared_target
        .join(WASM_TARGET)
        .join(wasm_profile(release))
        .join(filename)
}

fn normalize_absolute(path: &Path, current_dir: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    };
    if !absolute.is_absolute() {
        bail!("base directory must be absolute: {}", current_dir.display());
    }

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

fn ensure_distinct_artifacts(source: &Path, destination: &Path, module_name: &str) -> Result<()> {
    let same_existing_file = if source.exists() && destination.exists() {
        match (source.canonicalize(), destination.canonicalize()) {
            (Ok(source), Ok(destination)) => source == destination,
            _ => false,
        }
    } else {
        false
    };
    let same_path = source == destination || same_existing_file;
    if same_path {
        bail!(
            "configured destination for module '{}' resolves to the shared Cargo artifact: {}",
            module_name,
            source.display()
        );
    }
    Ok(())
}

fn normalize_wasm_builds(
    modules: &[BuildModule],
    release: bool,
    shared_target: &Path,
    current_dir: &Path,
) -> Result<Vec<WasmBuildSpec>> {
    let mut builds: Vec<WasmBuildSpec> = Vec::new();
    let mut by_source: BTreeMap<PathBuf, usize> = BTreeMap::new();
    let mut by_destination: BTreeMap<PathBuf, usize> = BTreeMap::new();

    for module in modules {
        let filename = wasm_filename(&module.wasm_path)?;
        let source = shared_source_artifact(shared_target, release, filename);
        let destination = normalize_absolute(Path::new(&module.wasm_path), current_dir)?;
        ensure_distinct_artifacts(&source, &destination, &module.name)?;

        let configured_cargo_dir = module_cargo_dir(module).with_context(|| {
            format!(
                "failed to resolve Cargo directory for module '{}'",
                module.name
            )
        })?;
        let cargo_dir = normalize_absolute(&configured_cargo_dir, current_dir)?
            .canonicalize()
            .with_context(|| {
                format!(
                    "failed to resolve Cargo directory for module '{}': {}",
                    module.name,
                    configured_cargo_dir.display()
                )
            })?;
        let cargo_toml = cargo_dir.join("Cargo.toml");
        if !cargo_toml.is_file() {
            bail!(
                "Cargo.toml not found for module '{}': {}",
                module.name,
                cargo_toml.display()
            );
        }

        let build = WasmBuildSpec {
            module_name: module.name.clone(),
            cargo_dir,
            source,
            destination,
        };
        if let Some(existing_index) = by_source.get(&build.source) {
            let existing = &builds[*existing_index];
            if existing.cargo_dir == build.cargo_dir && existing.destination == build.destination {
                continue;
            }
            bail!(
                "modules '{}' and '{}' conflict on shared WASM artifact {} (Cargo directories: {} vs {}; destinations: {} vs {})",
                existing.module_name,
                build.module_name,
                build.source.display(),
                existing.cargo_dir.display(),
                build.cargo_dir.display(),
                existing.destination.display(),
                build.destination.display(),
            );
        }
        let destination_key = build
            .destination
            .canonicalize()
            .unwrap_or_else(|_| build.destination.clone());
        if let Some(existing_index) = by_destination.get(&destination_key) {
            let existing = &builds[*existing_index];
            bail!(
                "modules '{}' and '{}' conflict on configured WASM destination {} (shared artifacts: {} vs {})",
                existing.module_name,
                build.module_name,
                destination_key.display(),
                existing.source.display(),
                build.source.display(),
            );
        }

        let build_index = builds.len();
        by_source.insert(build.source.clone(), build_index);
        by_destination.insert(destination_key, build_index);
        builds.push(build);
    }

    Ok(builds)
}

fn staging_temp_path(destination: &Path) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .context("WASM destination has no parent directory")?;
    let filename = destination
        .file_name()
        .context("WASM destination has no filename")?
        .to_string_lossy();
    let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(
        ".{filename}.wr-strip-{}-{counter}.tmp",
        std::process::id()
    )))
}

fn stage_wasm_artifact(
    source: &Path,
    destination: &Path,
    module_name: &str,
    strip_program: &Path,
) -> Result<()> {
    if !source.is_file() {
        bail!(
            "shared WASM artifact not found for module '{}' after Cargo build: {}",
            module_name,
            source.display()
        );
    }
    ensure_distinct_artifacts(source, destination, module_name)?;

    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .context("WASM destination has no parent directory")?;
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create WASM destination directory for module '{}': {}",
            module_name,
            parent.display()
        )
    })?;
    let temporary = staging_temp_path(destination)?;

    let strip_result = Command::new(strip_program)
        .arg("strip")
        .arg("-o")
        .arg(&temporary)
        .arg(source)
        .status();
    match strip_result {
        Ok(status) if status.success() => {}
        Ok(_) => {
            let _ = std::fs::remove_file(&temporary);
            bail!("wasm-tools strip failed for module '{module_name}'");
        }
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            return Err(error)
                .context("failed to run wasm-tools strip; ensure wasm-tools is installed");
        }
    }

    if let Err(error) = std::fs::rename(&temporary, destination) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error).with_context(|| {
            format!(
                "failed to install stripped WASM artifact for module '{}' at {}",
                module_name,
                destination.display()
            )
        });
    }
    Ok(())
}

/// Build WASM modules through the repository-owned shared Cargo target directory.
pub fn build_wasm_modules(modules: &[BuildModule], release: bool) -> Result<()> {
    let repository_root = repository_root()?;
    let shared_target = shared_wasm_target_dir(&repository_root)?;
    let current_dir = std::env::current_dir().context("failed to resolve current directory")?;
    let builds = normalize_wasm_builds(modules, release, &shared_target, &current_dir)?;

    for build in builds {
        print!("[build]   {} ... ", build.cargo_dir.display());
        let mut command = Command::new("cargo");
        command
            .arg("build")
            .arg("--target")
            .arg(WASM_TARGET)
            .arg("--target-dir")
            .arg(&shared_target)
            .current_dir(&build.cargo_dir);
        if release {
            command.arg("--release");
        } else {
            // The staged artifact is stripped below, so full DWARF only adds build cost.
            command.env("CARGO_PROFILE_DEV_DEBUG", "0");
        }
        let status = command.status().with_context(|| {
            format!(
                "failed to run cargo build for module '{}'",
                build.module_name
            )
        })?;
        if !status.success() {
            println!("FAILED");
            bail!(
                "cargo build --target {WASM_TARGET} failed for module '{}'. Ensure the {WASM_TARGET} target is installed with `rustup target add {WASM_TARGET}`.",
                build.module_name
            );
        }
        println!("OK");

        print!("[strip]   {} ... ", build.module_name);
        stage_wasm_artifact(
            &build.source,
            &build.destination,
            &build.module_name,
            Path::new("wasm-tools"),
        )?;
        println!("OK");
    }
    Ok(())
}

/// Cross-compile the manager binary for a given target triple
pub fn build_manager_binary(target: &str) -> Result<()> {
    raise_fd_limit();
    print!("[build]   wr-manager ({target}) ... ");
    let output = Command::new("cargo")
        .args([
            "zigbuild",
            "--release",
            "--target",
            target,
            "-p",
            "wr-manager",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to run cargo build")?;
    if !output.status.success() {
        println!("FAILED");
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("{stderr}");
        bail!("cargo build failed for wr-manager target {target}");
    }
    println!("OK");
    Ok(())
}

fn build_precompile_engine(target: &str) -> Result<Engine> {
    let mut wt_config = Config::new();
    wt_config.wasm_component_model(true);
    wt_config.epoch_interruption(true);
    wt_config.memory_reservation(4 * (1 << 30));
    wt_config.memory_guard_size(32 * (1 << 20));
    wt_config.memory_init_cow(true);
    wt_config.target(target)?;
    Ok(Engine::new(&wt_config)?)
}

/// Pre-compile WASM components to native code for the given target triple.
/// Uses Cranelift cross-compilation so the build host need not match the target.
/// Returns the compatibility hash for the compiled artifacts.
pub fn precompile_components(modules: &[BuildModule], target: &str) -> Result<String> {
    let engine = build_precompile_engine(target)?;
    let mut hasher = DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    let hash = format!("{:016x}", hasher.finish());

    for module in modules {
        let wasm_path = Path::new(&module.wasm_path);
        if !wasm_path.exists() {
            continue;
        }
        print!("[precompile] {} ... ", module.name);
        let wasm_bytes = std::fs::read(wasm_path)?;
        let cwasm_bytes = engine.precompile_component(&wasm_bytes)?;
        let cwasm_path = wasm_path.with_extension("cwasm");
        std::fs::write(&cwasm_path, &cwasm_bytes)?;
        println!("OK ({} bytes)", cwasm_bytes.len());
    }

    Ok(hash)
}

/// Cross-compile host binaries for a given target triple
pub fn build_host_binaries(target: &str) -> Result<()> {
    raise_fd_limit();
    print!("[build]   host binaries ({target}) ... ");
    let output = Command::new("cargo")
        .args([
            "zigbuild",
            "--release",
            "--target",
            target,
            "-p",
            "wr-proxy",
            "-p",
            "wr-engine",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to run cargo build")?;
    if !output.status.success() {
        println!("FAILED");
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("{stderr}");
        bail!("cargo build failed for target {target}");
    }
    println!("OK");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_manifest_rejects_malformed_toml() {
        let path = std::env::temp_dir().join(format!(
            "wr-build-manifest-malformed-{}.toml",
            std::process::id()
        ));
        std::fs::write(&path, "[[module]\nname =").unwrap();

        let err = load_manifest(path.to_str().unwrap()).expect_err("manifest must fail to parse");
        assert!(
            format!("{err:#}").contains("failed to parse build manifest"),
            "unexpected error: {err:#}"
        );

        let _ = std::fs::remove_file(&path);
    }

    fn test_dir(label: &str) -> PathBuf {
        let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "wr-cli-build-{label}-{}-{counter}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn test_module(name: &str, cargo_dir: &Path, wasm_path: &Path) -> BuildModule {
        BuildModule {
            name: name.into(),
            wasm_path: wasm_path.to_string_lossy().into_owned(),
            schema_path: String::new(),
            proto_path: None,
            cargo_dir: Some(cargo_dir.to_string_lossy().into_owned()),
        }
    }

    #[test]
    fn repository_root_produces_absolute_shared_target() {
        let root = repository_root().unwrap();
        let shared = shared_wasm_target_dir(&root).unwrap();
        assert!(shared.is_absolute());
        assert_eq!(shared, root.join("target/wasm-guests"));
    }

    #[test]
    fn shared_source_uses_requested_profile_and_configured_filename() {
        let target = Path::new("/repo/target/wasm-guests");
        assert_eq!(
            shared_source_artifact(target, false, std::ffi::OsStr::new("guest_name.wasm")),
            target.join("wasm32-wasip2/debug/guest_name.wasm")
        );
        assert_eq!(
            shared_source_artifact(target, true, std::ffi::OsStr::new("guest_name.wasm")),
            target.join("wasm32-wasip2/release/guest_name.wasm")
        );
        assert_eq!(
            wasm_filename("crate/target/wasm32-wasip2/debug/hyphen_name.wasm").unwrap(),
            "hyphen_name.wasm"
        );
    }

    #[test]
    fn wasm_destination_requires_wasm_filename() {
        let missing = wasm_filename("/").expect_err("root path has no filename");
        assert!(format!("{missing:#}").contains("has no filename"));
        let invalid = wasm_filename("guest/component.bin").expect_err("extension must fail");
        assert!(format!("{invalid:#}").contains("must end in .wasm"));
    }

    #[test]
    fn exact_duplicate_builds_collapse_and_source_collisions_fail() {
        let root = test_dir("normalize");
        let cargo_a = root.join("cargo-a");
        let cargo_b = root.join("cargo-b");
        std::fs::create_dir_all(&cargo_a).unwrap();
        std::fs::create_dir_all(&cargo_b).unwrap();
        std::fs::write(cargo_a.join("Cargo.toml"), "").unwrap();
        std::fs::write(cargo_b.join("Cargo.toml"), "").unwrap();
        let destination_a = root.join("out-a/same.wasm");
        let destination_b = root.join("out-b/same.wasm");
        let shared = root.join("shared");

        let duplicate = test_module("same-copy", &cargo_a, &destination_a);
        let builds = normalize_wasm_builds(
            &[test_module("same", &cargo_a, &destination_a), duplicate],
            false,
            &shared,
            &root,
        )
        .unwrap();
        assert_eq!(builds.len(), 1);

        let error = normalize_wasm_builds(
            &[
                test_module("first", &cargo_a, &destination_a),
                test_module("second", &cargo_b, &destination_b),
            ],
            false,
            &shared,
            &root,
        )
        .expect_err("shared filename collision must fail");
        let message = format!("{error:#}");
        assert!(message.contains("first"));
        assert!(message.contains("second"));
        assert!(message.contains("same.wasm"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn shared_source_cannot_be_the_configured_destination() {
        let path = Path::new("/repo/target/wasm-guests/wasm32-wasip2/debug/guest.wasm");
        let error = ensure_distinct_artifacts(path, path, "guest").unwrap_err();
        assert!(format!("{error:#}").contains("resolves to the shared Cargo artifact"));
    }

    #[test]
    fn missing_shared_source_fails_even_with_stale_destination() {
        let root = test_dir("missing-source");
        let source = root.join("missing.wasm");
        let destination = root.join("output/guest.wasm");
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::write(&destination, b"stale").unwrap();

        let error = stage_wasm_artifact(
            &source,
            &destination,
            "guest",
            Path::new("unused-strip-program"),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("shared WASM artifact not found"));
        assert_eq!(std::fs::read(&destination).unwrap(), b"stale");

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    fn write_strip_program(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn staging_preserves_source_and_installs_stripped_destination() {
        let root = test_dir("stage-success");
        let source = root.join("shared/guest.wasm");
        let destination = root.join("nested/output/guest.wasm");
        let strip_program = root.join("strip-success.sh");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, b"cargo-artifact").unwrap();
        write_strip_program(&strip_program, "printf stripped > \"$3\"");

        stage_wasm_artifact(&source, &destination, "guest", &strip_program).unwrap();

        assert_eq!(std::fs::read(&source).unwrap(), b"cargo-artifact");
        assert_eq!(std::fs::read(&destination).unwrap(), b"stripped");
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn failed_staging_keeps_destination_and_cleans_temporary_output() {
        let root = test_dir("stage-failure");
        let source = root.join("shared/guest.wasm");
        let destination = root.join("output/guest.wasm");
        let strip_program = root.join("strip-failure.sh");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::write(&source, b"cargo-artifact").unwrap();
        std::fs::write(&destination, b"previous").unwrap();
        write_strip_program(&strip_program, "printf partial > \"$3\"; exit 1");

        stage_wasm_artifact(&source, &destination, "guest", &strip_program).unwrap_err();

        assert_eq!(std::fs::read(&source).unwrap(), b"cargo-artifact");
        assert_eq!(std::fs::read(&destination).unwrap(), b"previous");
        assert!(std::fs::read_dir(destination.parent().unwrap())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("wr-strip")));
        let _ = std::fs::remove_dir_all(root);
    }

    fn host_target() -> Option<&'static str> {
        if cfg!(all(
            target_arch = "x86_64",
            target_os = "linux",
            target_env = "gnu"
        )) {
            Some("x86_64-unknown-linux-gnu")
        } else if cfg!(all(
            target_arch = "aarch64",
            target_os = "linux",
            target_env = "gnu"
        )) {
            Some("aarch64-unknown-linux-gnu")
        } else {
            None
        }
    }

    fn compatibility_hash(engine: &Engine) -> u64 {
        let mut hasher = DefaultHasher::new();
        engine.precompile_compatibility_hash().hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn precompile_configuration_matches_the_runtime() {
        let Some(target) = host_target() else {
            return;
        };
        let precompile = build_precompile_engine(target).unwrap();
        let runtime =
            wr_engine::runtime::build_engine(&wr_engine::config::PoolConfig::default()).unwrap();
        assert_eq!(
            compatibility_hash(&precompile),
            compatibility_hash(&runtime)
        );

        let component = b"\0asm\x0d\0\x01\0";
        let compiled = precompile.precompile_component(component).unwrap();
        unsafe { wasmtime::component::Component::deserialize(&runtime, compiled) }.unwrap();
    }

    #[test]
    fn precompile_supports_documented_linux_architectures() {
        let component = b"\0asm\x0d\0\x01\0";
        for target in ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"] {
            build_precompile_engine(target)
                .unwrap()
                .precompile_component(component)
                .unwrap();
        }
    }

    #[test]
    fn compile_schemas_reports_missing_explicit_proto() {
        let missing_proto = std::env::temp_dir().join(format!(
            "wr-missing-explicit-proto-{}.proto",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&missing_proto);

        let err = compile_schemas(&[BuildModule {
            name: "missing-proto".into(),
            wasm_path: "unused/target/wasm32-wasip2/debug/unused.wasm".into(),
            schema_path: missing_proto
                .with_extension("binpb")
                .to_string_lossy()
                .to_string(),
            proto_path: Some(missing_proto.to_string_lossy().to_string()),
            cargo_dir: Some("unused".into()),
        }])
        .expect_err("missing explicit proto must fail before protoc is invoked");
        assert!(
            format!("{err:#}").contains("Proto file not found for module 'missing-proto'"),
            "unexpected error: {err:#}"
        );
    }
}
