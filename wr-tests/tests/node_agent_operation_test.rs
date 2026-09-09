mod helpers;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use helpers::node_agent::{
    CleanupResultLoss, FakeBlockedBackend, FakeCleanupBackend, FakeCleanupManager, FakeClock,
    FakeManager,
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

fn activation_with(instance: &str) -> ActivationConfig {
    ActivationConfig {
        agent_instance_id: instance.into(),
        ..activation()
    }
}

fn activation() -> ActivationConfig {
    ActivationConfig {
        node_id: "node-a".into(),
        agent_instance_id: "activation-a".into(),
        binary_digest: format!("sha256:{}", "b".repeat(64)),
        backend: BackendType::Systemd,
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
            identity: Some(
                wr_common::wruntime::instruction_target::Identity::EngineSlotTarget(
                    wr_common::wruntime::EngineSlotTargetIdentity {
                        engine_slot: "blue".into(),
                    },
                ),
            ),
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

fn proxy_instruction(instance: &str, epoch: u64, step: NodeOperationStepKind) -> AgentInstruction {
    let mut instruction = instruction(instance, epoch);
    instruction.step = step as i32;
    let target = instruction.target.as_mut().unwrap();
    target.kind = InstructionTargetKind::Proxy as i32;
    target.identity = Some(wr_common::wruntime::instruction_target::Identity::Proxy(
        wr_common::wruntime::ProxyTargetIdentity {},
    ));
    instruction
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

struct FreshActivationManager {
    instruction: AgentInstruction,
    claims: AtomicUsize,
    observations: AtomicUsize,
    query_errors: AtomicUsize,
    results: AtomicUsize,
    shutdown: watch::Sender<bool>,
}

impl LeaseManager for FreshActivationManager {
    fn renew<'a>(&'a self, _lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl AgentManager for FreshActivationManager {
    fn attest<'a>(&'a self, attestation: NodeAgentAttestation) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(attestation.agent_instance_id, "activation-new");
            Ok(())
        })
    }

    fn claim<'a>(
        &'a self,
        _node_id: &'a str,
        agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<AgentInstruction>> {
        Box::pin(async move {
            assert_eq!(agent_instance_id, "activation-new");
            if self.claims.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(Some(AgentInstruction {
                    step: NodeOperationStepKind::InspectBackend as i32,
                    agent_instance_id: "activation-new".into(),
                    lease_epoch: 8,
                    ..self.instruction.clone()
                }))
            } else {
                Ok(None)
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
            if !request.backend_query_error.is_empty() {
                self.query_errors.fetch_add(1, Ordering::SeqCst);
            }
            let _ = self.shutdown.send(true);
            Ok(())
        })
    }

    fn report_result<'a>(
        &'a self,
        _request: ReportStepResultRequest,
    ) -> wr_cli::cmd::node_agent::ReportResultFuture<'a> {
        Box::pin(async move {
            self.results.fetch_add(1, Ordering::SeqCst);
            Err(ReportResultError::Rejected(
                "a fresh activation must not replay an old result".into(),
            ))
        })
    }
}

struct InspectRecoveryBackend {
    inspections: AtomicUsize,
    query_error: bool,
}

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
            self.inspections.fetch_add(1, Ordering::SeqCst);
            Ok(StepEvidence {
                observed_revision: 1,
                observed_digest:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                backend_state: Some(if self.query_error {
                    BackendProcessState::QueryError
                } else {
                    BackendProcessState::Exited
                }),
                backend_instance_id: "backend-1".into(),
                process_instance_id: "process-1".into(),
                backend_query_error: if self.query_error {
                    "backend inspection unavailable".into()
                } else {
                    String::new()
                },
                ..Default::default()
            })
        })
    }
}

async fn assert_fresh_activation_uses_manager_inspection(query_error: bool) {
    let (shutdown, receiver) = watch::channel(false);
    let manager = FreshActivationManager {
        instruction: instruction("activation-old", 7),
        claims: AtomicUsize::new(0),
        observations: AtomicUsize::new(0),
        query_errors: AtomicUsize::new(0),
        results: AtomicUsize::new(0),
        shutdown,
    };
    let inspector = InspectRecoveryBackend {
        inspections: AtomicUsize::new(0),
        query_error,
    };

    run_activation(
        &manager,
        &inspector,
        &TokioClock,
        activation_with("activation-new"),
        receiver,
    )
    .await
    .unwrap();

    assert_eq!(inspector.inspections.load(Ordering::SeqCst), 1);
    assert_eq!(manager.observations.load(Ordering::SeqCst), 1);
    assert_eq!(
        manager.query_errors.load(Ordering::SeqCst),
        usize::from(query_error)
    );
    assert_eq!(manager.results.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn node_agent_operation_test_fresh_activation_after_delivery_inspects_without_old_result() {
    assert_fresh_activation_uses_manager_inspection(false).await;
}

struct PostExecutionRecoveryManager {
    old_claims: AtomicUsize,
    fresh_claims: AtomicUsize,
    old_results: AtomicUsize,
    fresh_results: AtomicUsize,
    observations: AtomicUsize,
    old_shutdown: watch::Sender<bool>,
    fresh_shutdown: watch::Sender<bool>,
}

impl LeaseManager for PostExecutionRecoveryManager {
    fn renew<'a>(&'a self, _lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl AgentManager for PostExecutionRecoveryManager {
    fn attest<'a>(&'a self, _attestation: NodeAgentAttestation) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn claim<'a>(
        &'a self,
        _node_id: &'a str,
        agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<AgentInstruction>> {
        Box::pin(async move {
            match agent_instance_id {
                "activation-old" if self.old_claims.fetch_add(1, Ordering::SeqCst) == 0 => {
                    Ok(Some(proxy_instruction(
                        "activation-old",
                        7,
                        NodeOperationStepKind::StartBackend,
                    )))
                }
                "activation-new" if self.fresh_claims.fetch_add(1, Ordering::SeqCst) == 0 => {
                    Ok(Some(proxy_instruction(
                        "activation-new",
                        8,
                        NodeOperationStepKind::InspectBackend,
                    )))
                }
                _ => Ok(None),
            }
        })
    }

    fn report_observation<'a>(
        &'a self,
        _request: ReportNodeObservationRequest,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            self.observations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn report_result<'a>(
        &'a self,
        request: ReportStepResultRequest,
    ) -> wr_cli::cmd::node_agent::ReportResultFuture<'a> {
        Box::pin(async move {
            match request.agent_instance_id.as_str() {
                "activation-old" => {
                    assert_eq!(request.step, NodeOperationStepKind::StartBackend as i32);
                    self.old_results.fetch_add(1, Ordering::SeqCst);
                    let _ = self.old_shutdown.send(true);
                    Err(ReportResultError::Retryable(
                        "injected post-execution response loss".into(),
                    ))
                }
                "activation-new" => {
                    assert_eq!(request.step, NodeOperationStepKind::InspectBackend as i32);
                    assert!(matches!(
                        request.target.and_then(|target| target.identity),
                        Some(wr_common::wruntime::instruction_target::Identity::Proxy(_))
                    ));
                    self.fresh_results.fetch_add(1, Ordering::SeqCst);
                    let _ = self.fresh_shutdown.send(true);
                    Ok(())
                }
                other => panic!("unexpected result activation {other}"),
            }
        })
    }
}

#[derive(Default)]
struct PostExecutionRecoveryBackend {
    mutations: AtomicUsize,
    inspections: AtomicUsize,
}

impl InstructionExecutor for PostExecutionRecoveryBackend {
    fn execute<'a>(
        &'a self,
        instruction: &'a AgentInstruction,
        _cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, StepEvidence> {
        Box::pin(async move {
            match NodeOperationStepKind::try_from(instruction.step).unwrap() {
                NodeOperationStepKind::StartBackend => {
                    self.mutations.fetch_add(1, Ordering::SeqCst);
                }
                NodeOperationStepKind::InspectBackend => {
                    self.inspections.fetch_add(1, Ordering::SeqCst);
                }
                step => panic!("unexpected recovery step {step:?}"),
            }
            Ok(StepEvidence {
                observed_revision: 1,
                observed_digest:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                backend_state: Some(BackendProcessState::Running),
                backend_instance_id: "proxy-backend-1".into(),
                process_instance_id: "proxy-process-1".into(),
                ..Default::default()
            })
        })
    }
}

#[tokio::test]
async fn node_agent_operation_test_fresh_activation_after_execution_reports_proxy_inspection() {
    let (old_shutdown, old_receiver) = watch::channel(false);
    let (fresh_shutdown, fresh_receiver) = watch::channel(false);
    let manager = PostExecutionRecoveryManager {
        old_claims: AtomicUsize::new(0),
        fresh_claims: AtomicUsize::new(0),
        old_results: AtomicUsize::new(0),
        fresh_results: AtomicUsize::new(0),
        observations: AtomicUsize::new(0),
        old_shutdown,
        fresh_shutdown,
    };
    let backend = PostExecutionRecoveryBackend::default();

    run_activation(
        &manager,
        &backend,
        &FakeClock,
        activation_with("activation-old"),
        old_receiver,
    )
    .await
    .unwrap();
    assert_eq!(backend.mutations.load(Ordering::SeqCst), 1);
    assert_eq!(manager.old_results.load(Ordering::SeqCst), 1);

    run_activation(
        &manager,
        &backend,
        &FakeClock,
        activation_with("activation-new"),
        fresh_receiver,
    )
    .await
    .unwrap();
    assert_eq!(backend.mutations.load(Ordering::SeqCst), 1);
    assert_eq!(backend.inspections.load(Ordering::SeqCst), 1);
    assert_eq!(manager.fresh_results.load(Ordering::SeqCst), 1);
    assert_eq!(manager.observations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn node_agent_operation_test_fresh_activation_reports_inconclusive_inspection_without_mutation(
) {
    assert_fresh_activation_uses_manager_inspection(true).await;
}

#[test]
fn node_agent_operation_test_activations_and_epochs_are_distinct_fence_inputs() {
    let first = instruction("activation-a", 9);
    let replacement = instruction("activation-b", 10);
    assert_ne!(first.agent_instance_id, replacement.agent_instance_id);
    assert!(replacement.lease_epoch > first.lease_epoch);
}
