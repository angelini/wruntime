use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use std::hash::{DefaultHasher, Hash, Hasher};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use wasmtime::{Config, Engine};

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

/// Build WASM modules via native Cargo `wasm32-wasip2`.
pub fn build_wasm_modules(modules: &[BuildModule], release: bool) -> Result<()> {
    for module in modules {
        let cargo_dir = module_cargo_dir(module)?;
        let cargo_toml = cargo_dir.join("Cargo.toml");
        if !cargo_toml.exists() {
            bail!(
                "Cargo.toml not found for module '{}': {}",
                module.name,
                cargo_toml.display()
            );
        }
        print!("[build]   {} ... ", cargo_dir.display());
        let mut args = vec!["build", "--target", "wasm32-wasip2"];
        if release {
            args.push("--release");
        }
        let output = Command::new("cargo")
            .args(&args)
            .current_dir(&cargo_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("failed to run cargo build --target wasm32-wasip2")?;
        if !output.status.success() {
            println!("FAILED");
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("{stderr}");
            bail!(
                "cargo build --target wasm32-wasip2 failed for module '{}'. Ensure the wasm32-wasip2 target is installed with `rustup target add wasm32-wasip2`.",
                module.name
            );
        }
        println!("OK");

        if !Path::new(&module.wasm_path).exists() {
            bail!(
                "WASM output not found for module '{}' after cargo build: {}",
                module.name,
                module.wasm_path
            );
        }

        // Strip debug info and custom sections to reduce .wasm size
        print!("[strip]   {} ... ", module.name);
        let strip_output = Command::new("wasm-tools")
            .args(["strip", "-o", &module.wasm_path, &module.wasm_path])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("failed to run wasm-tools strip; ensure wasm-tools is installed")?;
        if !strip_output.status.success() {
            println!("FAILED");
            let stderr = String::from_utf8_lossy(&strip_output.stderr);
            eprintln!("{stderr}");
            bail!("wasm-tools strip failed for module '{}'", module.name);
        }
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

/// Pre-compile WASM components to native code for the given target triple.
/// Uses Cranelift cross-compilation so the build host need not match the target.
/// Returns the compatibility hash for the compiled artifacts.
pub fn precompile_components(modules: &[BuildModule], target: &str) -> Result<String> {
    let mut wt_config = Config::new();
    wt_config.wasm_component_model(true);
    wt_config.target(target)?;
    let engine = Engine::new(&wt_config)?;
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
