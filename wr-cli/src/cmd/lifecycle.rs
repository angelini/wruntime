use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use wr_common::{
    manager_client::EpochObservation,
    wruntime::{LifecycleStatus, PrivilegedAdmissionState, ProcessLifecycleState, ServiceKind},
};

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
struct LifecycleOutput<'a, T: Serialize> {
    outcome: &'static str,
    endpoint: &'a str,
    observation: &'a T,
}

#[derive(Serialize)]
struct StatusObservation {
    #[serde(flatten)]
    lifecycle: helpers::LifecycleObservation,
    #[serde(skip_serializing_if = "Option::is_none")]
    privileged_admission: Option<&'static str>,
}

fn tls(target: &TargetArgs) -> Option<&'static wr_common::node::TlsConfig> {
    target.tls.then(client::tls_config).flatten()
}

fn serialize_output<T: Serialize>(endpoint: &str, observation: &T) -> Result<String> {
    Ok(serde_json::to_string_pretty(&LifecycleOutput {
        outcome: "observed",
        endpoint,
        observation,
    })?)
}

fn print_output(endpoint: &str, observation: &helpers::LifecycleObservation) -> Result<()> {
    println!("{}", serialize_output(endpoint, observation)?);
    Ok(())
}

fn admission_name(admission: i32) -> Result<&'static str> {
    match PrivilegedAdmissionState::try_from(admission)
        .context("manager lifecycle status contains unknown privileged admission")?
    {
        PrivilegedAdmissionState::Open => Ok("OPEN"),
        PrivilegedAdmissionState::ClosedStartup => Ok("CLOSED_STARTUP"),
        PrivilegedAdmissionState::ClosedRollout => Ok("CLOSED_ROLLOUT"),
        PrivilegedAdmissionState::ClosedMismatch => Ok("CLOSED_MISMATCH"),
        PrivilegedAdmissionState::Unspecified => {
            anyhow::bail!("manager lifecycle status omitted privileged admission")
        }
    }
}

fn status_observation(status: LifecycleStatus) -> Result<StatusObservation> {
    let lifecycle = helpers::lifecycle_observation(status.clone())?;
    let privileged_admission = if lifecycle.service_kind == ServiceKind::Manager as i32 {
        let epoch = EpochObservation::from_lifecycle(&status)
            .context("manager lifecycle status failed epoch validation")?;
        Some(admission_name(epoch.privileged_admission)?)
    } else {
        None
    };
    Ok(StatusObservation {
        lifecycle,
        privileged_admission,
    })
}

fn print_status_output(endpoint: &str, status: LifecycleStatus) -> Result<()> {
    let observation = status_observation(status)?;
    println!("{}", serialize_output(endpoint, &observation)?);
    Ok(())
}

pub async fn run(args: LifecycleArgs) -> Result<()> {
    match args.command {
        LifecycleCommand::Status(target) => {
            let status = helpers::get_raw_lifecycle_status(&target.endpoint, tls(&target)).await?;
            print_status_output(&target.endpoint, status)
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

#[cfg(test)]
mod tests {
    use super::*;
    use wr_common::wruntime::ManagerRolloutPhase;

    fn status(kind: ServiceKind, admission: PrivilegedAdmissionState) -> LifecycleStatus {
        let manager = kind == ServiceKind::Manager;
        LifecycleStatus {
            state: ProcessLifecycleState::Ready as i32,
            service_kind: kind as i32,
            process_instance_id: "process-1".into(),
            reason: 2,
            detail: "ready".into(),
            manager_id: if manager {
                "manager-a".into()
            } else {
                String::new()
            },
            process_ready: true,
            policy_generation: u64::from(manager),
            policy_digest: if manager {
                "sha256:policy".into()
            } else {
                String::new()
            },
            privileged_admission: admission as i32,
            ..Default::default()
        }
    }

    fn serialized(status: LifecycleStatus) -> Result<serde_json::Value> {
        let observation = status_observation(status)?;
        Ok(serde_json::from_str(&serialize_output(
            "https://manager:9000",
            &observation,
        )?)?)
    }

    #[test]
    fn manager_status_projects_exact_admission_names() -> Result<()> {
        for (admission, expected) in [
            (PrivilegedAdmissionState::Open, "OPEN"),
            (PrivilegedAdmissionState::ClosedStartup, "CLOSED_STARTUP"),
            (PrivilegedAdmissionState::ClosedRollout, "CLOSED_ROLLOUT"),
        ] {
            let mut wire = status(ServiceKind::Manager, admission);
            if admission == PrivilegedAdmissionState::ClosedRollout {
                wire.rollout_operation_id = "rollout-1".into();
                wire.rollout_phase = ManagerRolloutPhase::FailedClosed as i32;
                wire.rollout_expected_set_hash = "sha256:set".into();
            }
            let value = serialized(wire)?;
            assert_eq!(value["observation"]["privileged_admission"], expected);
            assert_eq!(
                value["observation"]["state"],
                ProcessLifecycleState::Ready as i32
            );
            assert_eq!(
                value["observation"]["service_kind"],
                ServiceKind::Manager as i32
            );
            assert_eq!(value["observation"]["process_instance_id"], "process-1");
            assert_eq!(value["observation"]["reason"], 2);
            assert_eq!(value["observation"]["detail"], "ready");
        }
        Ok(())
    }

    #[test]
    fn manager_status_rejects_missing_or_incoherent_admission_evidence() {
        assert!(status_observation(status(
            ServiceKind::Manager,
            PrivilegedAdmissionState::Unspecified,
        ))
        .is_err());

        let mut unknown = status(ServiceKind::Manager, PrivilegedAdmissionState::Open);
        unknown.privileged_admission = i32::MAX;
        assert!(status_observation(unknown).is_err());

        let mut incoherent = status(
            ServiceKind::Manager,
            PrivilegedAdmissionState::ClosedRollout,
        );
        incoherent.rollout_operation_id = "rollout-1".into();
        assert!(status_observation(incoherent).is_err());
    }

    #[test]
    fn non_manager_status_omits_admission_without_changing_generic_shape() -> Result<()> {
        for kind in [ServiceKind::Proxy, ServiceKind::Engine] {
            let value = serialized(status(kind, PrivilegedAdmissionState::Unspecified))?;
            let observation = value["observation"].as_object().unwrap();
            assert_eq!(
                observation
                    .keys()
                    .map(String::as_str)
                    .collect::<std::collections::BTreeSet<_>>(),
                std::collections::BTreeSet::from([
                    "detail",
                    "process_instance_id",
                    "reason",
                    "service_kind",
                    "state",
                ])
            );
            assert!(!observation.contains_key("privileged_admission"));
        }
        Ok(())
    }
}
