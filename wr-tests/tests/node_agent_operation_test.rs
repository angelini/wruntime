mod helpers;

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use helpers::node_agent::{
    cleanup_instruction, CleanupResultLoss, FakeBlockedBackend, FakeCleanupBackend,
    FakeCleanupManager, FakeClock, FakeManager,
};
use tokio::sync::watch;
use wr_cli::cmd::node_agent::{
    execute_fenced, run_activation, ActivationConfig, AgentFuture, AgentManager, FencedExecution,
    LeaseIdentity, LeaseManager, ReportResultError, TokioClock,
};
use wr_cli::cmd::node_backend::{BackendFuture, BackendType, InstructionExecutor, StepEvidence};
use wr_common::wruntime::{
    AgentInstruction, BackendProcessState, InstructionTarget, InstructionTargetKind,
    NodeAgentAttestation, NodeOperationStepKind, ReportNodeObservationRequest,
    ReportStepResultRequest,
};

fn activation_with(instance: &str, recovery_dir: Option<PathBuf>) -> ActivationConfig {
    ActivationConfig {
        agent_instance_id: instance.into(),
        recovery_dir,
        ..activation()
    }
}

fn activation() -> ActivationConfig {
    ActivationConfig {
        node_id: "node-a".into(),
        agent_instance_id: "activation-a".into(),
        binary_digest: format!("sha256:{}", "b".repeat(64)),
        config_digest: format!("sha256:{}", "c".repeat(64)),
        backend: BackendType::Systemd,
        retention_count: 3,
        recovery_dir: None,
        poll: Duration::from_secs(1),
        renew: Duration::from_secs(60),
    }
}

fn instruction(instance: &str, epoch: u64) -> AgentInstruction {
    AgentInstruction {
        operation_id: "00000000-0000-0000-0000-000000000001".into(),
        node_id: "node-a".into(),
        lease_epoch: epoch,
        step: NodeOperationStepKind::StopBackend as i32,
        agent_instance_id: instance.into(),
        target: Some(InstructionTarget {
            kind: InstructionTargetKind::EngineSlot as i32,
            engine_slot: "blue".into(),
            revision: 1,
            bundle_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            resolved_release_digest:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        }),
        pinned_backend_instance_id: "backend-1".into(),
        pinned_process_instance_id: "process-1".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn node_agent_operation_test_blocked_effect_loses_lease_and_is_cancelled_and_reaped() {
    let manager = FakeManager::rejecting();
    let backend = FakeBlockedBackend::default();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let outcome = execute_fenced(
        &manager,
        &backend,
        &FakeClock,
        &instruction("activation-a", 7),
        Duration::from_millis(1),
        shutdown_rx,
    )
    .await;

    assert!(matches!(outcome, FencedExecution::LeaseLost(_)));
    assert_eq!(backend.effects_started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.effects_reaped.load(Ordering::SeqCst), 1);
    assert_eq!(manager.renewals.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn node_agent_operation_test_local_shutdown_cancels_and_reaps_without_a_later_effect() {
    let manager = FakeManager::rejecting();
    manager.reject_renewal.store(false, Ordering::SeqCst);
    let backend = FakeBlockedBackend::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let request = instruction("activation-a", 8);
    let run = execute_fenced(
        &manager,
        &backend,
        &FakeClock,
        &request,
        Duration::from_secs(60),
        shutdown_rx,
    );
    tokio::pin!(run);
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(1)) => {
            shutdown_tx.send(true).unwrap();
        }
        _ = &mut run => panic!("blocked effect completed before shutdown"),
    }
    let outcome = run.await;
    assert!(matches!(outcome, FencedExecution::Shutdown));
    assert_eq!(backend.effects_started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.effects_reaped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn node_agent_operation_test_forward_deadline_cancels_but_restoration_has_no_deadline() {
    let manager = FakeManager::rejecting();
    manager.reject_renewal.store(false, Ordering::SeqCst);
    let backend = FakeBlockedBackend::default();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut request = instruction("activation-a", 9);
    request.operation_deadline = Some(prost_types::Timestamp {
        seconds: 1,
        nanos: 0,
    });

    let outcome = execute_fenced(
        &manager,
        &backend,
        &FakeClock,
        &request,
        Duration::from_secs(60),
        shutdown_rx,
    )
    .await;

    assert!(matches!(outcome, FencedExecution::DeadlineExpired));
    assert_eq!(backend.effects_reaped.load(Ordering::SeqCst), 1);
    request.restoration = true;
    assert!(
        request.restoration,
        "restoration instructions ignore forward deadline locally"
    );
}

async fn assert_cleanup_result_loss(loss: CleanupResultLoss) {
    let (shutdown, receiver) = watch::channel(false);
    let manager = FakeCleanupManager::new(loss, shutdown);
    let backend = FakeCleanupBackend::default();

    run_activation(&manager, &backend, &FakeClock, activation(), receiver)
        .await
        .unwrap();

    assert_eq!(backend.effects.load(Ordering::SeqCst), 1);
    assert_eq!(manager.claims.load(Ordering::SeqCst), 1);
    assert_eq!(
        manager.claims_while_result_pending.load(Ordering::SeqCst),
        0
    );
    assert_eq!(manager.reports.load(Ordering::SeqCst), 2);
    assert_eq!(manager.advancements.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn node_agent_operation_test_cleanup_result_loss_before_acceptance_retries_without_claim_or_replay(
) {
    assert_cleanup_result_loss(CleanupResultLoss::BeforeAcceptance).await;
}

#[tokio::test]
async fn node_agent_operation_test_cleanup_result_loss_after_acceptance_is_duplicate_acknowledged()
{
    assert_cleanup_result_loss(CleanupResultLoss::AfterAcceptance).await;
}

struct RestartRecoveryManager {
    instruction: AgentInstruction,
    claims: AtomicUsize,
    observations: AtomicUsize,
    stale_results: AtomicUsize,
    retry_results: bool,
    accept_results: bool,
    shutdown: watch::Sender<bool>,
}

impl LeaseManager for RestartRecoveryManager {
    fn renew<'a>(&'a self, _lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl AgentManager for RestartRecoveryManager {
    fn attest<'a>(&'a self, _attestation: NodeAgentAttestation) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn claim<'a>(
        &'a self,
        _node_id: &'a str,
        agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<AgentInstruction>> {
        Box::pin(async move {
            let claim = self.claims.fetch_add(1, Ordering::SeqCst);
            match (agent_instance_id, claim) {
                ("activation-old", 0) => Ok(Some(self.instruction.clone())),
                ("activation-new", 1) => Ok(Some(AgentInstruction {
                    step: if self.instruction.step == NodeOperationStepKind::CleanupRelease as i32 {
                        NodeOperationStepKind::CleanupRelease as i32
                    } else {
                        NodeOperationStepKind::InspectBackend as i32
                    },
                    agent_instance_id: "activation-new".into(),
                    lease_epoch: 8,
                    ..self.instruction.clone()
                })),
                _ => Ok(None),
            }
        })
    }

    fn report_observation<'a>(
        &'a self,
        request: ReportNodeObservationRequest,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(request.agent_instance_id, "activation-new");
            assert_eq!(request.lease_epoch, 8);
            self.observations.fetch_add(1, Ordering::SeqCst);
            let _ = self.shutdown.send(true);
            Ok(())
        })
    }

    fn report_result<'a>(
        &'a self,
        request: ReportStepResultRequest,
    ) -> wr_cli::cmd::node_agent::ReportResultFuture<'a> {
        Box::pin(async move {
            self.stale_results.fetch_add(1, Ordering::SeqCst);
            if self.retry_results {
                let _ = self.shutdown.send(true);
                Err(ReportResultError::Retryable(
                    "injected result acknowledgement loss".into(),
                ))
            } else if self.accept_results {
                assert_eq!(request.agent_instance_id, "activation-new");
                assert_eq!(request.lease_epoch, 8);
                let _ = self.shutdown.send(true);
                Ok(())
            } else {
                Err(ReportResultError::Rejected(
                    "old-activation result must never be replayed".into(),
                ))
            }
        })
    }
}

struct InspectRecoveryBackend(AtomicUsize);

impl InstructionExecutor for InspectRecoveryBackend {
    fn execute<'a>(
        &'a self,
        instruction: &'a AgentInstruction,
        _cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, StepEvidence> {
        Box::pin(async move {
            assert_eq!(
                NodeOperationStepKind::try_from(instruction.step).unwrap(),
                NodeOperationStepKind::InspectBackend
            );
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(StepEvidence {
                observed_revision: 1,
                observed_digest:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                backend_state: Some(BackendProcessState::Exited),
                backend_instance_id: "backend-1".into(),
                process_instance_id: "process-1".into(),
                ..Default::default()
            })
        })
    }
}

fn recovery_directory(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "wr-agent-recovery-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

#[tokio::test]
async fn node_agent_operation_test_restart_loads_and_inspects_recovery_without_replaying_result() {
    let recovery = recovery_directory("restart");
    let (manager_shutdown, manager_receiver) = watch::channel(false);
    let manager = RestartRecoveryManager {
        instruction: instruction("activation-old", 7),
        claims: AtomicUsize::new(0),
        observations: AtomicUsize::new(0),
        stale_results: AtomicUsize::new(0),
        retry_results: false,
        accept_results: false,
        shutdown: manager_shutdown,
    };
    let blocked = FakeBlockedBackend::default();
    let (first_shutdown, first_receiver) = watch::channel(false);
    let first = run_activation(
        &manager,
        &blocked,
        &TokioClock,
        activation_with("activation-old", Some(recovery.clone())),
        first_receiver,
    );
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("blocked recovery effect exited before restart: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {
            assert_eq!(blocked.effects_started.load(Ordering::SeqCst), 1);
            first_shutdown.send(true).unwrap();
        }
    }
    first.await.unwrap();
    assert_eq!(blocked.effects_started.load(Ordering::SeqCst), 1);
    assert_eq!(blocked.effects_reaped.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_dir(&recovery).unwrap().count(), 1);

    let inspector = InspectRecoveryBackend(AtomicUsize::new(0));
    run_activation(
        &manager,
        &inspector,
        &TokioClock,
        activation_with("activation-new", Some(recovery.clone())),
        manager_receiver,
    )
    .await
    .unwrap();
    assert_eq!(inspector.0.load(Ordering::SeqCst), 1);
    assert_eq!(manager.observations.load(Ordering::SeqCst), 1);
    assert_eq!(manager.stale_results.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read_dir(&recovery).unwrap().count(), 0);
    std::fs::remove_dir(recovery).unwrap();
}

#[tokio::test]
async fn node_agent_operation_test_restart_correlates_stored_result_without_old_activation_replay()
{
    let recovery = recovery_directory("result-restart");
    let (first_shutdown, first_receiver) = watch::channel(false);
    let mut cleanup_instruction = cleanup_instruction();
    cleanup_instruction.agent_instance_id = "activation-old".into();
    let first_manager = RestartRecoveryManager {
        instruction: cleanup_instruction.clone(),
        claims: AtomicUsize::new(0),
        observations: AtomicUsize::new(0),
        stale_results: AtomicUsize::new(0),
        retry_results: true,
        accept_results: false,
        shutdown: first_shutdown,
    };
    let cleanup = FakeCleanupBackend::default();
    run_activation(
        &first_manager,
        &cleanup,
        &TokioClock,
        activation_with("activation-old", Some(recovery.clone())),
        first_receiver,
    )
    .await
    .unwrap();
    assert_eq!(cleanup.effects.load(Ordering::SeqCst), 1);
    assert_eq!(first_manager.stale_results.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_dir(&recovery).unwrap().count(), 1);

    let (second_shutdown, second_receiver) = watch::channel(false);
    let second_manager = RestartRecoveryManager {
        instruction: cleanup_instruction,
        claims: AtomicUsize::new(1),
        observations: AtomicUsize::new(0),
        stale_results: AtomicUsize::new(0),
        retry_results: false,
        accept_results: true,
        shutdown: second_shutdown,
    };
    let retry_cleanup = FakeCleanupBackend::default();
    run_activation(
        &second_manager,
        &retry_cleanup,
        &TokioClock,
        activation_with("activation-new", Some(recovery.clone())),
        second_receiver,
    )
    .await
    .unwrap();
    assert_eq!(retry_cleanup.effects.load(Ordering::SeqCst), 1);
    assert_eq!(second_manager.observations.load(Ordering::SeqCst), 0);
    assert_eq!(second_manager.stale_results.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_dir(&recovery).unwrap().count(), 0);
    std::fs::remove_dir(recovery).unwrap();
}

#[tokio::test]
async fn node_agent_operation_test_corrupt_recovery_fails_before_attestation_or_claim() {
    let recovery = recovery_directory("corrupt");
    let path = recovery.join("00000000-0000-0000-0000-000000000001.state");
    std::fs::write(&path, b"corrupt").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let manager = RestartRecoveryManager {
        instruction: instruction("activation-old", 7),
        claims: AtomicUsize::new(0),
        observations: AtomicUsize::new(0),
        stale_results: AtomicUsize::new(0),
        retry_results: false,
        accept_results: false,
        shutdown,
    };
    let error = run_activation(
        &manager,
        &InspectRecoveryBackend(AtomicUsize::new(0)),
        &TokioClock,
        activation_with("activation-new", Some(recovery.clone())),
        receiver,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("invalid envelope"));
    assert_eq!(manager.claims.load(Ordering::SeqCst), 0);
    std::fs::remove_file(path).unwrap();
    std::fs::remove_dir(recovery).unwrap();
}

#[test]
fn node_agent_operation_test_activations_and_epochs_are_distinct_fence_inputs() {
    let first = instruction("activation-a", 9);
    let replacement = instruction("activation-b", 10);
    assert_ne!(first.agent_instance_id, replacement.agent_instance_id);
    assert!(replacement.lease_epoch > first.lease_epoch);
}
