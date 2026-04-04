use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};

/// Minimal module config for build operations
pub struct BuildModule {
    pub name: String,
    pub wasm_path: String,
    pub schema_path: String,
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
         Expected a path containing 'target/' (e.g., my-module/target/wasm32-wasip2/release/mod.wasm)"
    );
}

/// Compile .proto → .binpb for each module that has a schema_path
pub fn compile_schemas(modules: &[BuildModule]) -> Result<()> {
    for module in modules {
        if module.schema_path.is_empty() {
            continue;
        }
        let proto_path = derive_proto_path(&module.schema_path);
        if !Path::new(&proto_path).exists() {
            bail!(
                "Proto file not found for module '{}': {} (derived from schema_path '{}')",
                module.name,
                proto_path,
                module.schema_path,
            );
        }
        let proto_dir = Path::new(&proto_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        print!("[schema]  {} ... ", module.schema_path);
        let status = Command::new("protoc")
            .args([
                &format!("--descriptor_set_out={}", module.schema_path),
                "--include_imports",
                &format!("--proto_path={}", proto_dir),
                &proto_path,
            ])
            .status()
            .context("failed to run protoc")?;
        if !status.success() {
            bail!("protoc failed for module '{}'", module.name);
        }
        println!("OK");
    }
    Ok(())
}

/// Build WASM modules via `cargo component build`
pub fn build_wasm_modules(modules: &[BuildModule]) -> Result<()> {
    for module in modules {
        let cargo_dir = derive_cargo_dir(&module.wasm_path)?;
        print!("[build]   {} ... ", cargo_dir.display());
        let output = Command::new("cargo")
            .args([
                "component",
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
            ])
            .current_dir(&cargo_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("failed to run cargo component build")?;
        if !output.status.success() {
            println!("FAILED");
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("{stderr}");
            bail!("cargo component build failed for module '{}'", module.name);
        }
        println!("OK");
    }
    Ok(())
}

/// Cross-compile host binaries for a given target triple
pub fn build_host_binaries(target: &str) -> Result<()> {
    print!("[build]   host binaries ({target}) ... ");
    let output = Command::new("cargo")
        .args([
            "build",
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
