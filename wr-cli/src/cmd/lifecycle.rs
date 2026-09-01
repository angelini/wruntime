use std::time::Duration;

use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use wr_common::wruntime::{ProcessLifecycleState, ServiceKind};

use super::helpers;
use crate::client;

#[derive(Args)]
pub struct LifecycleArgs {
    #[command(subcommand)]
    pub command: LifecycleCommand,
}

#[derive(Subcommand)]
pub enum LifecycleCommand {
    /// Query one trusted process lifecycle endpoint.
    Status(TargetArgs),
    /// Wait for one exact process lifecycle state.
    Wait(WaitArgs),
}

#[derive(Args)]
pub struct TargetArgs {
    /// Trusted manager, proxy-control, or engine lifecycle endpoint.
    #[arg(long)]
    endpoint: String,
    /// Use the CLI manager mTLS credentials for this endpoint.
    #[arg(long)]
    tls: bool,
}

#[derive(Args)]
pub struct WaitArgs {
    #[command(flatten)]
    target: TargetArgs,
    /// Exact lifecycle state to observe.
    #[arg(long, value_enum)]
    state: ExpectedState,
    /// Require this exact service kind (mandatory for READY waits).
    #[arg(long, value_enum)]
    service_kind: Option<ExpectedServiceKind>,
    /// Require this exact process instance ID (mandatory for READY waits).
    #[arg(long)]
    process_instance: Option<String>,
    /// One absolute wait deadline in seconds.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExpectedState {
    Starting,
    Ready,
    Stopping,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExpectedServiceKind {
    Manager,
    Proxy,
    Engine,
}

impl From<ExpectedServiceKind> for ServiceKind {
    fn from(value: ExpectedServiceKind) -> Self {
        match value {
            ExpectedServiceKind::Manager => Self::Manager,
            ExpectedServiceKind::Proxy => Self::Proxy,
            ExpectedServiceKind::Engine => Self::Engine,
        }
    }
}

impl From<ExpectedState> for ProcessLifecycleState {
    fn from(value: ExpectedState) -> Self {
        match value {
            ExpectedState::Starting => Self::Starting,
            ExpectedState::Ready => Self::Ready,
            ExpectedState::Stopping => Self::Stopping,
        }
    }
}

#[derive(Serialize)]
struct LifecycleOutput<'a> {
    outcome: &'static str,
    endpoint: &'a str,
    observation: &'a helpers::LifecycleObservation,
}

fn tls(target: &TargetArgs) -> Option<&'static wr_common::node::TlsConfig> {
    target.tls.then(client::tls_config).flatten()
}

fn print_output(endpoint: &str, observation: &helpers::LifecycleObservation) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&LifecycleOutput {
            outcome: "observed",
            endpoint,
            observation,
        })?
    );
    Ok(())
}

pub async fn run(args: LifecycleArgs) -> Result<()> {
    match args.command {
        LifecycleCommand::Status(target) => {
            let observation = helpers::get_lifecycle_status(&target.endpoint, tls(&target)).await?;
            print_output(&target.endpoint, &observation)
        }
        LifecycleCommand::Wait(args) => {
            if matches!(args.state, ExpectedState::Ready) {
                anyhow::ensure!(
                    args.service_kind.is_some() && args.process_instance.is_some(),
                    "READY waits require --service-kind and --process-instance"
                );
            }
            let observation = helpers::wait_for_lifecycle_state(
                &args.target.endpoint,
                tls(&args.target),
                args.state.into(),
                args.service_kind.map(Into::into),
                args.process_instance.as_deref(),
                Duration::from_secs(args.timeout_secs),
            )
            .await?;
            print_output(&args.target.endpoint, &observation)
        }
    }
}
