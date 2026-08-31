use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};

use super::build_helpers::{self, BuildModule};
use super::config::EngineConfig;
use super::dev_supervisor::{self, Request};
const TEST_GUEST_MANIFEST: &str = "wr-tests/guests/build.toml";
const ECOMMERCE_ENGINE_CONFIGS: &[&str] = &[
    "examples/ecommerce/engine-client.toml",
    "examples/ecommerce/engine-inventory-1.toml",
    "examples/ecommerce/engine-inventory-2.toml",
];
const STOCKMARKET_ENGINE_CONFIGS: &[&str] = &[
    "examples/stockmarket/engine-exchange.toml",
    "examples/stockmarket/engine-ledger.toml",
    "examples/stockmarket/engine-simulator.toml",
];
const CODEGEN_ENGINE_CONFIGS: &[&str] = &["examples/codegen/engine.toml"];
const MULTI_NODE_ENGINE_CONFIGS: &[&str] = &["examples/multi-node/node-b/engine-1.toml"];

#[derive(Args)]
pub struct DevArgs {
    /// Directory containing the supervisor lock, socket, and diagnostic state
    #[arg(long, value_name = "DIR", default_value = ".", global = true)]
    pub state_dir: PathBuf,

    #[command(subcommand)]
    pub command: DevCommand,
}

#[derive(Subcommand)]
pub enum DevCommand {
    /// Start manager + proxy for local development
    Up {
        /// Path to manager config file
        #[arg(long, default_value = "examples/config/manager.toml")]
        manager_config: String,
        /// Path to proxy config file
        #[arg(long, default_value = "examples/config/proxy.toml")]
        proxy_config: String,
    },
    /// Start an additional supervisor-owned proxy (for local multi-node runs)
    StartProxy {
        /// Stable name for this additional proxy
        #[arg(long)]
        name: String,
        /// Path to proxy config file
        config: String,
    },
    /// Stop all dev processes after ordered drain and reaping
    Down,
    /// Block until a supervised child exits unexpectedly
    Wait,
    /// Internal persistent supervisor entrypoint
    #[command(hide = true)]
    Supervisor,
    /// Build WASM guests and schemas from build metadata
    Build(BuildArgs),
    /// Build WASM + schemas and (re)deploy an engine
    Deploy(DeployArgs),
    /// Show running dev processes and modules
    Status,
}

#[derive(Args)]
pub struct DeployArgs {
    /// Path to engine.toml config file
    config: String,
    /// Skip WASM and schema compilation (deploy only)
    #[arg(long)]
    skip_build: bool,
    /// Skip protoc schema compilation
    #[arg(long)]
    skip_schemas: bool,
    /// Only build/deploy the named module (repeatable)
    #[arg(long = "module", value_name = "NAME")]
    modules: Vec<String>,
}

#[derive(Args, Clone, Debug)]
pub struct BuildArgs {
    /// Build group: tests, ecommerce, stockmarket, codegen, multi-node, or all
    #[arg(value_name = "GROUP")]
    group: Option<String>,
    /// Explicit engine TOML to use as build metadata (repeatable)
    #[arg(long = "config", value_name = "ENGINE_TOML")]
    configs: Vec<String>,
    /// Explicit build manifest to use as metadata (repeatable)
    #[arg(long = "manifest", value_name = "BUILD_MANIFEST")]
    manifests: Vec<String>,
    /// Skip protoc schema compilation
    #[arg(long)]
    skip_schemas: bool,
    /// Only build modules with this name (repeatable)
    #[arg(long = "module", value_name = "NAME")]
    modules: Vec<String>,
}

pub async fn run(args: DevArgs, _manager: Option<&str>) -> Result<()> {
    let state_dir = args.state_dir;
    match args.command {
        DevCommand::Up {
            manager_config,
            proxy_config,
        } => print_response(
            dev_supervisor::send_or_start(
                &state_dir,
                Request::Up {
                    manager_config,
                    proxy_config,
                },
            )
            .await?,
        ),
        DevCommand::StartProxy { name, config } => print_response(
            dev_supervisor::send_or_start(&state_dir, Request::StartProxy { name, config }).await?,
        ),
        DevCommand::Down => print_response(dev_supervisor::down(&state_dir).await?),
        DevCommand::Wait => print_response(dev_supervisor::wait(&state_dir).await?),
        DevCommand::Supervisor => dev_supervisor::run_supervisor(state_dir).await,
        DevCommand::Build(build_args) => build(build_args),
        DevCommand::Deploy(deploy_args) => deploy(deploy_args, &state_dir).await,
        DevCommand::Status => {
            print_response(dev_supervisor::send(&state_dir, Request::Status).await?)
        }
    }
}

fn print_response(response: dev_supervisor::Response) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

async fn deploy(args: DeployArgs, state_dir: &Path) -> Result<()> {
    let config_path = &args.config;
    let config = EngineConfig::from_file(config_path)?;

    let modules_to_build: Vec<_> = if args.modules.is_empty() {
        config.modules.iter().collect()
    } else {
        config
            .modules
            .iter()
            .filter(|module| args.modules.contains(&module.name))
            .collect()
    };
    if modules_to_build.is_empty() && !args.modules.is_empty() {
        bail!(
            "No modules matched: {:?}. Available: {:?}",
            args.modules,
            config
                .modules
                .iter()
                .map(|module| &module.name)
                .collect::<Vec<_>>()
        );
    }
    if !args.skip_build {
        let build_modules = modules_to_build
            .iter()
            .map(|module| BuildModule {
                name: module.name.clone(),
                wasm_path: module.wasm_path.clone(),
                schema_path: module.schema_path.clone().unwrap_or_default(),
                proto_path: None,
                cargo_dir: None,
            })
            .collect::<Vec<_>>();
        if !args.skip_schemas {
            build_helpers::compile_schemas(&build_modules)?;
        }
        build_helpers::build_wasm_modules(&build_modules, false)?;
    }

    print_response(
        dev_supervisor::send_or_start(
            state_dir,
            Request::DeployEngine {
                config: config_path.clone(),
            },
        )
        .await?,
    )
}

fn build(args: BuildArgs) -> Result<()> {
    let modules = resolve_build_modules(&args)?;
    if !args.skip_schemas {
        build_helpers::compile_schemas(&modules)?;
    }
    build_helpers::build_wasm_modules(&modules, false)
}

fn resolve_build_modules(args: &BuildArgs) -> Result<Vec<BuildModule>> {
    let mut modules = Vec::new();
    if let Some(group) = &args.group {
        append_group_modules(group, &mut modules)?;
    }
    for config in &args.configs {
        append_config_modules(config, &mut modules)?;
    }
    for manifest in &args.manifests {
        append_manifest_modules(manifest, &mut modules)?;
    }
    if modules.is_empty() {
        bail!("no build modules requested; pass a group (tests, ecommerce, stockmarket, codegen, multi-node, all), --config, or --manifest");
    }
    let modules = dedupe_build_modules(modules)?;
    filter_build_modules(modules, &args.modules)
}

fn append_group_modules(group: &str, modules: &mut Vec<BuildModule>) -> Result<()> {
    match group {
        "tests" => append_manifest_modules(TEST_GUEST_MANIFEST, modules),
        "ecommerce" => append_config_group(ECOMMERCE_ENGINE_CONFIGS, modules),
        "stockmarket" => append_config_group(STOCKMARKET_ENGINE_CONFIGS, modules),
        "codegen" => append_config_group(CODEGEN_ENGINE_CONFIGS, modules),
        "multi-node" => append_config_group(MULTI_NODE_ENGINE_CONFIGS, modules),
        "all" => {
            append_group_modules("tests", modules)?;
            append_group_modules("ecommerce", modules)?;
            append_group_modules("stockmarket", modules)?;
            append_group_modules("codegen", modules)?;
            append_group_modules("multi-node", modules)
        }
        other => bail!("unknown build group '{other}'; expected tests, ecommerce, stockmarket, codegen, multi-node, all, or use --config/--manifest"),
    }
}

fn append_config_group(configs: &[&str], modules: &mut Vec<BuildModule>) -> Result<()> {
    for config in configs {
        append_config_modules(config, modules)?;
    }
    Ok(())
}

fn resolve_metadata_path(path: &str) -> String {
    let direct = Path::new(path);
    if direct.exists() || direct.is_absolute() {
        return path.to_string();
    }
    if let Some(workspace_root) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
        let candidate = workspace_root.join(path);
        if candidate.exists() {
            return candidate.to_string_lossy().to_string();
        }
    }
    path.to_string()
}

fn append_config_modules(path: &str, modules: &mut Vec<BuildModule>) -> Result<()> {
    let resolved_path = resolve_metadata_path(path);
    let config = EngineConfig::from_file(&resolved_path)
        .with_context(|| format!("failed to resolve build modules from engine config: {path}"))?;
    modules.extend(config.modules.into_iter().map(|m| BuildModule {
        name: m.name,
        wasm_path: m.wasm_path,
        schema_path: m.schema_path.unwrap_or_default(),
        proto_path: None,
        cargo_dir: None,
    }));
    Ok(())
}

fn append_manifest_modules(path: &str, modules: &mut Vec<BuildModule>) -> Result<()> {
    let resolved_path = resolve_metadata_path(path);
    let loaded = build_helpers::load_manifest(&resolved_path)
        .with_context(|| format!("failed to resolve build modules from manifest: {path}"))?;
    modules.extend(loaded);
    Ok(())
}

fn dedupe_build_modules(modules: Vec<BuildModule>) -> Result<Vec<BuildModule>> {
    let mut by_wasm: BTreeMap<String, BuildModule> = BTreeMap::new();
    for module in modules {
        if module.name.is_empty() {
            bail!(
                "build metadata contains a module with an empty name for wasm_path '{}'",
                module.wasm_path
            );
        }
        if module.wasm_path.is_empty() {
            bail!(
                "build metadata for module '{}' has an empty wasm_path",
                module.name
            );
        }
        match by_wasm.get_mut(&module.wasm_path) {
            None => {
                by_wasm.insert(module.wasm_path.clone(), module);
            }
            Some(existing) => {
                if existing.schema_path.is_empty() && !module.schema_path.is_empty() {
                    existing.schema_path = module.schema_path.clone();
                    existing.proto_path = module.proto_path.clone();
                } else if !module.schema_path.is_empty()
                    && existing.schema_path != module.schema_path
                {
                    bail!(
                        "conflicting schema paths for wasm '{}': '{}' vs '{}'",
                        module.wasm_path,
                        existing.schema_path,
                        module.schema_path
                    );
                }
                if existing.cargo_dir.is_none() {
                    existing.cargo_dir = module.cargo_dir.clone();
                } else if module.cargo_dir.is_some() && existing.cargo_dir != module.cargo_dir {
                    bail!(
                        "conflicting cargo directories for wasm '{}': {:?} vs {:?}",
                        module.wasm_path,
                        existing.cargo_dir,
                        module.cargo_dir
                    );
                }
            }
        }
    }
    Ok(by_wasm.into_values().collect())
}

fn filter_build_modules(
    modules: Vec<BuildModule>,
    requested: &[String],
) -> Result<Vec<BuildModule>> {
    if requested.is_empty() {
        return Ok(modules);
    }
    let requested: BTreeSet<&str> = requested.iter().map(String::as_str).collect();
    let available: BTreeSet<String> = modules.iter().map(|m| m.name.clone()).collect();
    let filtered: Vec<BuildModule> = modules
        .into_iter()
        .filter(|m| requested.contains(m.name.as_str()))
        .collect();
    if filtered.is_empty() {
        bail!(
            "no modules matched {:?}; available modules: {:?}",
            requested,
            available
        );
    }
    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_args(group: Option<&str>) -> BuildArgs {
        BuildArgs {
            group: group.map(str::to_string),
            configs: vec![],
            manifests: vec![],
            skip_schemas: false,
            modules: vec![],
        }
    }

    #[test]
    fn dev_args_accept_state_dir_before_subcommand() {
        use clap::Parser as _;

        #[derive(clap::Parser)]
        struct TestCli {
            #[command(flatten)]
            dev: DevArgs,
        }

        let parsed =
            TestCli::try_parse_from(["test", "--state-dir", "/tmp/wr-run", "down"]).unwrap();
        assert_eq!(parsed.dev.state_dir, PathBuf::from("/tmp/wr-run"));
        assert!(matches!(parsed.dev.command, DevCommand::Down));
    }

    #[test]
    fn build_group_rejects_unknown_group() {
        let args = BuildArgs {
            group: Some("bogus".into()),
            configs: vec![],
            manifests: vec![],
            skip_schemas: false,
            modules: vec![],
        };
        let err = resolve_build_modules(&args).expect_err("unknown group must be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("unknown build group 'bogus'"),
            "unexpected error: {err:#}"
        );
        assert!(message.contains("multi-node"));
    }

    #[test]
    fn tests_group_resolves_explicit_manifest() {
        let modules = resolve_build_modules(&build_args(Some("tests"))).unwrap();
        let names: BTreeSet<String> = modules.iter().map(|m| m.name.clone()).collect();
        assert_eq!(modules.len(), 5);
        assert_eq!(
            names,
            BTreeSet::from([
                "db-guest".to_string(),
                "tracing-guest".to_string(),
                "blobstore-guest".to_string(),
                "http-guest".to_string(),
                "llm-guest".to_string(),
            ])
        );
        assert!(modules.iter().all(|m| m.proto_path.is_some()));
        assert!(modules.iter().all(|m| m.cargo_dir.is_some()));
    }

    #[test]
    fn multi_node_group_resolves_echo_config() {
        let modules = resolve_build_modules(&build_args(Some("multi-node"))).unwrap();
        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].name, "echo");
        assert_eq!(
            modules[0].wasm_path,
            "examples/multi-node/echo/target/wasm32-wasip2/debug/echo.wasm"
        );
    }

    #[test]
    fn stockmarket_group_does_not_select_duplicate_schema_ledger_crate() {
        let modules = resolve_build_modules(&build_args(Some("stockmarket"))).unwrap();
        let names: BTreeSet<String> = modules.iter().map(|m| m.name.clone()).collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "exchange".to_string(),
                "ledger".to_string(),
                "simulator".to_string(),
            ])
        );
        assert!(modules
            .iter()
            .all(|m| !m.wasm_path.contains("examples/stockmarket/schemas/ledger")));
        assert!(modules.iter().all(|m| !m
            .cargo_dir
            .as_deref()
            .unwrap_or_default()
            .contains("examples/stockmarket/schemas/ledger")));
    }

    #[test]
    fn all_group_resolves_every_guest_once_with_unique_artifact_names() {
        let modules = resolve_build_modules(&build_args(Some("all"))).unwrap();
        let cargo_dirs: BTreeSet<PathBuf> = modules
            .iter()
            .map(|module| build_helpers::module_cargo_dir(module).unwrap())
            .collect();
        let expected = BTreeSet::from([
            PathBuf::from("wr-tests/guests/db-guest"),
            PathBuf::from("wr-tests/guests/tracing-guest"),
            PathBuf::from("wr-tests/guests/blobstore-guest"),
            PathBuf::from("wr-tests/guests/http-guest"),
            PathBuf::from("wr-tests/guests/llm-guest"),
            PathBuf::from("examples/ecommerce/client"),
            PathBuf::from("examples/ecommerce/inventory"),
            PathBuf::from("examples/stockmarket/exchange"),
            PathBuf::from("examples/stockmarket/ledger"),
            PathBuf::from("examples/stockmarket/simulator"),
            PathBuf::from("examples/codegen/collector"),
            PathBuf::from("examples/codegen/agent"),
            PathBuf::from("examples/codegen/coordinator"),
            PathBuf::from("examples/codegen/worker"),
            PathBuf::from("examples/multi-node/echo"),
        ]);
        assert_eq!(cargo_dirs, expected);
        assert_eq!(modules.len(), cargo_dirs.len());

        let filenames: BTreeSet<_> = modules
            .iter()
            .map(|module| {
                Path::new(&module.wasm_path)
                    .file_name()
                    .unwrap()
                    .to_os_string()
            })
            .collect();
        assert_eq!(filenames.len(), modules.len());
    }

    #[test]
    fn explicit_missing_config_reports_path() {
        let args = BuildArgs {
            group: None,
            configs: vec!["does/not/exist.toml".into()],
            manifests: vec![],
            skip_schemas: false,
            modules: vec![],
        };
        let err = resolve_build_modules(&args).expect_err("missing config must be rejected");
        assert!(
            format!("{err:#}").contains(
                "failed to resolve build modules from engine config: does/not/exist.toml"
            ),
            "unexpected error: {err:#}"
        );
    }
}
