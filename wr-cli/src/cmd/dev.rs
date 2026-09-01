use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};

use super::build_helpers::{self, BuildModule};
use super::config::EngineConfig;
use super::foreground_runner::{self, RunSpec};
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
    #[command(subcommand)]
    pub command: DevCommand,
}

#[derive(Subcommand)]
pub enum DevCommand {
    /// Run an already-built local topology in the foreground
    Run(RunArgs),
    /// Build WASM guests and schemas from build metadata
    Build(BuildArgs),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedProxyConfig {
    pub name: String,
    pub path: PathBuf,
}

impl FromStr for NamedProxyConfig {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (name, path) = value
            .split_once('=')
            .ok_or_else(|| "proxy config must use NAME=PATH".to_string())?;
        if name.is_empty() || path.is_empty() || path.contains('=') {
            return Err("proxy config must use one non-empty NAME=PATH pair".to_string());
        }
        Ok(Self {
            name: name.to_string(),
            path: PathBuf::from(path),
        })
    }
}

#[derive(Args, Clone, Debug)]
pub struct RunArgs {
    /// Path to the single manager config
    #[arg(long, value_name = "PATH")]
    manager_config: PathBuf,
    /// Named proxy config, in NAME=PATH form (repeatable)
    #[arg(long, value_name = "NAME=PATH", required = true)]
    proxy_config: Vec<NamedProxyConfig>,
    /// Engine config path (repeatable; zero engines is valid)
    #[arg(long, value_name = "PATH")]
    engine_config: Vec<PathBuf>,
    /// Optional one-shot scenario command; must follow `--`
    #[arg(last = true, value_name = "SCENARIO COMMAND")]
    scenario: Vec<String>,
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
    match args.command {
        DevCommand::Run(run_args) => {
            foreground_runner::run(RunSpec {
                manager_config: run_args.manager_config,
                proxies: run_args
                    .proxy_config
                    .into_iter()
                    .map(|proxy| (proxy.name, proxy.path))
                    .collect(),
                engine_configs: run_args.engine_config,
                scenario: run_args.scenario,
            })
            .await
        }
        DevCommand::Build(build_args) => build(build_args),
    }
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
    fn dev_surface_contains_only_build_and_run() {
        use clap::{CommandFactory as _, Parser as _};

        #[derive(clap::Parser)]
        struct TestCli {
            #[command(flatten)]
            dev: DevArgs,
        }

        let command = TestCli::command();
        let subcommands = command
            .get_subcommands()
            .map(|subcommand| subcommand.get_name())
            .collect::<BTreeSet<_>>();
        assert_eq!(subcommands, BTreeSet::from(["build", "run"]));
        for removed in [
            "up",
            "start-proxy",
            "down",
            "wait",
            "deploy",
            "status",
            "supervisor",
        ] {
            assert!(TestCli::try_parse_from(["test", removed]).is_err());
        }
        assert!(TestCli::try_parse_from(["test", "--state-dir", "/tmp/wr-run", "build"]).is_err());
    }

    #[test]
    fn dev_run_parses_locked_cardinality_and_preserves_scenario_arguments() {
        use clap::Parser as _;

        #[derive(clap::Parser)]
        struct TestCli {
            #[command(flatten)]
            dev: DevArgs,
        }

        let parsed = TestCli::try_parse_from([
            "test",
            "run",
            "--manager-config",
            "manager.toml",
            "--proxy-config",
            "primary=proxy.toml",
            "--proxy-config",
            "peer=peer.toml",
            "--engine-config",
            "engine.toml",
            "--",
            "sh",
            "-c",
            "exit 7",
        ])
        .unwrap();
        let DevCommand::Run(run) = parsed.dev.command else {
            panic!("expected dev run");
        };
        assert_eq!(run.manager_config, PathBuf::from("manager.toml"));
        assert_eq!(run.proxy_config.len(), 2);
        assert_eq!(run.engine_config, vec![PathBuf::from("engine.toml")]);
        assert_eq!(run.scenario, ["sh", "-c", "exit 7"]);
    }

    #[test]
    fn dev_run_rejects_missing_proxy_and_malformed_name_path() {
        use clap::Parser as _;

        #[derive(clap::Parser)]
        struct TestCli {
            #[command(flatten)]
            dev: DevArgs,
        }

        assert!(
            TestCli::try_parse_from(["test", "run", "--manager-config", "manager.toml",]).is_err()
        );
        assert!(TestCli::try_parse_from([
            "test",
            "run",
            "--manager-config",
            "manager.toml",
            "--proxy-config",
            "missing-equals",
        ])
        .is_err());
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
