mod helpers;

use anyhow::Result;
use helpers::db::manager_pool;
use tonic::Code;
use uuid::Uuid;
use wr_common::wruntime::{
    BackendProcessState, BeginDeploymentRequest, CleanupReleaseEvidence, DeploymentMetadata,
    DeploymentRecord, EngineRegistration, ExpectedEngine, FinalizeDeploymentRequest,
    InstructionTargetKind, LifecycleStatus, ModuleDescriptor, ModuleIdentity, NodeOperationAction,
    NodeOperationPhase, NodeOperationState, NodeOperationStepKind, ProcessLifecycleState,
    ReleaseInventoryEntry, ReportNodeObservationRequest, ReportStepResultRequest, RolloutPolicy,
    ServiceKind, SubmitOperationRequest,
};

fn policy(deadline_seconds: u64) -> RolloutPolicy {
    RolloutPolicy {
        max_unavailable: 1,
        canary_slot: String::new(),
        pause_after_canary: false,
        allow_downtime: true,
        deadline_seconds,
    }
}

async fn configure_agent_with_retention(
    pool: &deadpool_postgres::Pool,
    node_id: &str,
    activation: &str,
    retention_count: u32,
) {
    wr_manager::operations::put_agent_policy(
        pool,
        "operator-a",
        &helpers::node_agent::systemd_policy(node_id, retention_count),
    )
    .await
    .expect("put agent policy");
    let policy = helpers::node_agent::systemd_policy(node_id, retention_count);
    let conditions = wr_manager::operations::attest(
        pool,
        "agent-a",
        &helpers::node_agent::attestation(&policy, activation),
    )
    .await
    .expect("attest agent");
    assert!(conditions.is_empty());
}

async fn configure_agent(pool: &deadpool_postgres::Pool, node_id: &str, activation: &str) {
    configure_agent_with_retention(pool, node_id, activation, 2).await;
}

fn resolved_digest() -> String {
    format!("sha256:{}", "b".repeat(64))
}

async fn stage(
    pool: &deadpool_postgres::Pool,
    node_id: &str,
    token: &str,
    digest: &str,
    slots: &[&str],
) -> wr_common::wruntime::DeploymentRecord {
    let deployment = wr_manager::db::begin_deployment(
        pool,
        &BeginDeploymentRequest {
            node_id: node_id.into(),
            attempt_token: token.into(),
            bundle_digest: digest.into(),
            expected_engines: slots
                .iter()
                .map(|slot| ExpectedEngine {
                    engine_slot: (*slot).into(),
                    modules: vec![],
                })
                .collect(),
        },
        "operator-a",
    )
    .await
    .expect("stage deployment")
    .record;
    wr_manager::db::finalize_deployment(
        pool,
        &FinalizeDeploymentRequest {
            node_id: node_id.into(),
            attempt_token: token.into(),
            revision: deployment.revision,
            bundle_digest: digest.into(),
            resolved_release_digest: resolved_digest(),
        },
        "operator-a",
    )
    .await
    .expect("finalize deployment")
    .record
}

fn result_for(
    instruction: &wr_common::wruntime::AgentInstruction,
    condition_code: &str,
) -> ReportStepResultRequest {
    let target = instruction.target.as_ref().expect("typed target");
    ReportStepResultRequest {
        node_id: instruction.node_id.clone(),
        operation_id: instruction.operation_id.clone(),
        engine_slot: target.engine_slot.clone(),
        lease_epoch: instruction.lease_epoch,
        step: instruction.step,
        condition_code: condition_code.into(),
        detail: String::new(),
        agent_instance_id: instruction.agent_instance_id.clone(),
        observed_revision: target.revision,
        observed_digest: target.bundle_digest.clone(),
        backend_instance_id: String::new(),
        process_instance_id: "proxy-process".into(),
        backend_query_error: String::new(),
        cleanup_evidence: None,
        observed_resolved_release_digest: target.resolved_release_digest.clone(),
    }
}

fn module_identity() -> ModuleIdentity {
    ModuleIdentity {
        namespace: "operation".into(),
        name: "service".into(),
        version: "1.0.0".into(),
    }
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        namespace: "operation".into(),
        name: "service".into(),
        version: "1.0.0".into(),
        proto_schema: vec![1],
    }
}

async fn stage_with_module(
    pool: &deadpool_postgres::Pool,
    node_id: &str,
    token: &str,
    digest: &str,
    slots: &[&str],
) -> DeploymentRecord {
    let deployment = wr_manager::db::begin_deployment(
        pool,
        &BeginDeploymentRequest {
            node_id: node_id.into(),
            attempt_token: token.into(),
            bundle_digest: digest.into(),
            expected_engines: slots
                .iter()
                .map(|slot| ExpectedEngine {
                    engine_slot: (*slot).into(),
                    modules: vec![module_identity()],
                })
                .collect(),
        },
        "operator-a",
    )
    .await
    .expect("stage deployment with module")
    .record;
    wr_manager::db::finalize_deployment(
        pool,
        &FinalizeDeploymentRequest {
            node_id: node_id.into(),
            attempt_token: token.into(),
            revision: deployment.revision,
            bundle_digest: digest.into(),
            resolved_release_digest: resolved_digest(),
        },
        "operator-a",
    )
    .await
    .expect("finalize deployment with module")
    .record
}

async fn register_ready(
    pool: &deadpool_postgres::Pool,
    deployment: &DeploymentRecord,
    slot: &str,
    engine_id: &str,
) -> Result<()> {
    let module = module_descriptor();
    wr_manager::db::register_engine_and_routes(
        pool,
        &EngineRegistration {
            engine_id: engine_id.into(),
            address: format!("http://127.0.0.1/{}", engine_id),
            modules: vec![module.clone()],
            proxy_address: "http://127.0.0.1:9001".into(),
            secrets: vec![],
            peer_address: "https://127.0.0.1:9443".into(),
            db_namespaces: vec![],
            job_queue_id: String::new(),
            job_admin_address: String::new(),
            deployment: Some(DeploymentMetadata {
                node_id: deployment.node_id.clone(),
                revision: deployment.revision,
                bundle_digest: deployment.bundle_digest.clone(),
                engine_slot: slot.into(),
            }),
        },
    )
    .await?;
    wr_manager::db::publish_engine_readiness(pool, engine_id, &[module]).await?;
    Ok(())
}

async fn claim_instruction(
    pool: &deadpool_postgres::Pool,
    node_id: &str,
    activation: &str,
) -> Result<wr_common::wruntime::AgentInstruction> {
    loop {
        let instruction = wr_manager::operations::claim(pool, node_id, activation, "agent-a")
            .await?
            .expect("operation instruction")
            .instruction
            .expect("typed instruction");
        let target = instruction.target.as_ref().expect("typed target");
        if InstructionTargetKind::try_from(target.kind).ok() != Some(InstructionTargetKind::Proxy) {
            return Ok(instruction);
        }
        let mut result = result_for(&instruction, "");
        result.backend_instance_id = if instruction.pinned_backend_instance_id.is_empty() {
            format!("proxy-backend-{}", target.revision)
        } else {
            instruction.pinned_backend_instance_id.clone()
        };
        result.process_instance_id = if instruction.pinned_process_instance_id.is_empty() {
            format!("proxy-process-{}", target.revision)
        } else {
            instruction.pinned_process_instance_id.clone()
        };
        wr_manager::operations::report_step(pool, &result, "agent-a").await?;
    }
}

#[allow(clippy::too_many_arguments)] // Scenario helper mirrors one complete observation payload.
async fn observe(
    pool: &deadpool_postgres::Pool,
    instruction: &wr_common::wruntime::AgentInstruction,
    slot: &str,
    state: BackendProcessState,
    backend_instance_id: &str,
    process_instance_id: &str,
    revision: u64,
    digest: &str,
) -> Result<wr_common::wruntime::NodeOperation> {
    let lifecycle = (state == BackendProcessState::Running).then(|| LifecycleStatus {
        state: ProcessLifecycleState::Ready as i32,
        service_kind: ServiceKind::Engine as i32,
        process_instance_id: process_instance_id.into(),
        ..Default::default()
    });
    Ok(wr_manager::operations::report_observation(
        pool,
        &ReportNodeObservationRequest {
            node_id: instruction.node_id.clone(),
            engine_slot: slot.into(),
            lifecycle,
            backend_state: state as i32,
            backend_instance_id: backend_instance_id.into(),
            observed_revision: revision,
            observed_at: None,
            observed_digest: digest.into(),
            backend_query_error: String::new(),
            operation_id: instruction.operation_id.clone(),
            agent_instance_id: instruction.agent_instance_id.clone(),
            lease_epoch: instruction.lease_epoch,
            observed_resolved_release_digest: instruction
                .target
                .as_ref()
                .map(|target| target.resolved_release_digest.clone())
                .unwrap_or_default(),
        },
        "agent-a",
    )
    .await?)
}

async fn report_ok(
    pool: &deadpool_postgres::Pool,
    instruction: &wr_common::wruntime::AgentInstruction,
    backend: &str,
    process: &str,
) -> Result<wr_common::wruntime::NodeOperation> {
    let mut result = result_for(instruction, "");
    result.backend_instance_id = backend.into();
    result.process_instance_id = process.into();
    Ok(wr_manager::operations::report_step(pool, &result, "agent-a").await?)
}

#[tokio::test]
async fn allocation_to_submission_crash_boundaries_recover_one_actor_target_and_operation(
) -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "6".repeat(64));
    let request = BeginDeploymentRequest {
        node_id: "crash-boundary-node".into(),
        attempt_token: "crash-boundary-token".into(),
        bundle_digest: digest.clone(),
        expected_engines: vec![ExpectedEngine {
            engine_slot: "blue".into(),
            modules: vec![],
        }],
    };

    // Crash immediately after allocation: the exact authenticated retry owns
    // the same inactive revision rather than allocating another target.
    let allocated = wr_manager::db::begin_deployment(&pool, &request, "operator-a")
        .await?
        .record;
    let allocation_retry = wr_manager::db::begin_deployment(&pool, &request, "operator-a")
        .await?
        .record;
    assert_eq!(allocation_retry.revision, allocated.revision);
    let actor_conflict = wr_manager::db::begin_deployment(&pool, &request, "operator-b")
        .await
        .expect_err("a different actor cannot recover the allocation");
    assert_eq!(actor_conflict.code(), Code::AlreadyExists);

    // Crash during transfer or immediately around finalization: the manager
    // accepts only the same exact resolved bytes and actor.
    let finalize = FinalizeDeploymentRequest {
        node_id: request.node_id.clone(),
        attempt_token: request.attempt_token.clone(),
        revision: allocated.revision,
        bundle_digest: digest.clone(),
        resolved_release_digest: resolved_digest(),
    };
    let finalized = wr_manager::db::finalize_deployment(&pool, &finalize, "operator-a")
        .await?
        .record;
    let finalization_retry = wr_manager::db::finalize_deployment(&pool, &finalize, "operator-a")
        .await?
        .record;
    assert_eq!(finalization_retry.revision, finalized.revision);
    let actor_conflict = wr_manager::db::finalize_deployment(&pool, &finalize, "operator-b")
        .await
        .expect_err("a different actor cannot finalize the allocation");
    assert_eq!(actor_conflict.code(), Code::PermissionDenied);

    // Crash immediately before or after submission: the same payload maps to
    // exactly one operation and a different actor cannot reuse the allocation.
    let submit = SubmitOperationRequest {
        node_id: request.node_id,
        request_token: request.attempt_token,
        action: NodeOperationAction::InitialApply as i32,
        engine_slots: vec!["blue".into()],
        target_revision: finalized.revision,
        bundle_digest: digest,
        policy: Some(policy(300)),
        resolved_release_digest: resolved_digest(),
    };
    let operation = wr_manager::operations::submit(&pool, "operator-a", &submit).await?;
    let submission_retry = wr_manager::operations::submit(&pool, "operator-a", &submit).await?;
    assert_eq!(submission_retry.operation_id, operation.operation_id);
    let operation_count: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_node_operations WHERE node_id = 'crash-boundary-node'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(operation_count, 1);

    Ok(())
}

#[tokio::test]
async fn durable_operation_is_idempotent_and_fences_activation_and_epoch() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "7".repeat(64));
    let deployment = stage(&pool, "operation-node", "same-request", &digest, &["blue"]).await;
    let request = SubmitOperationRequest {
        node_id: "operation-node".into(),
        request_token: "same-request".into(),
        action: NodeOperationAction::InitialApply as i32,
        engine_slots: vec!["blue".into()],
        target_revision: deployment.revision,
        bundle_digest: digest,
        policy: Some(policy(300)),
        resolved_release_digest: resolved_digest(),
    };
    let first = wr_manager::operations::submit(&pool, "operator-a", &request).await?;
    let duplicate = wr_manager::operations::submit(&pool, "operator-a", &request).await?;
    assert_eq!(first.operation_id, duplicate.operation_id);

    let mut conflicting = request.clone();
    conflicting.engine_slots = vec!["green".into()];
    let conflict = wr_manager::operations::submit(&pool, "operator-a", &conflicting)
        .await
        .expect_err("conflicting token reuse must fail");
    assert_eq!(conflict.code(), Code::AlreadyExists);

    configure_agent(&pool, "operation-node", "activation-a").await;
    let claim = wr_manager::operations::claim(&pool, "operation-node", "activation-a", "agent-a")
        .await?
        .expect("queued operation must be claimable");
    let instruction = claim.instruction.expect("claim instruction");
    assert_eq!(
        instruction.step,
        NodeOperationStepKind::SelectRelease as i32
    );
    assert_eq!(
        InstructionTargetKind::try_from(instruction.target.as_ref().expect("target").kind)?,
        InstructionTargetKind::Proxy
    );
    assert_eq!(instruction.agent_instance_id, "activation-a");

    for (epoch, activation) in [
        (instruction.lease_epoch + 1, "activation-a"),
        (instruction.lease_epoch, "activation-b"),
    ] {
        let stale = wr_manager::operations::renew(
            &pool,
            "operation-node",
            &instruction.operation_id,
            epoch,
            activation,
            "agent-a",
        )
        .await
        .expect_err("stale epoch or activation must be fenced");
        assert_eq!(stale.code(), Code::Aborted);
    }

    Ok(())
}

#[tokio::test]
async fn reported_success_cannot_bypass_manager_evidence_or_grant_authority() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "8".repeat(64));
    let deployment = stage(&pool, "evidence-node", "evidence-op", &digest, &["blue"]).await;
    let request = SubmitOperationRequest {
        node_id: "evidence-node".into(),
        request_token: "evidence-op".into(),
        action: NodeOperationAction::InitialApply as i32,
        engine_slots: vec!["blue".into()],
        target_revision: deployment.revision,
        bundle_digest: digest,
        policy: Some(policy(300)),
        resolved_release_digest: resolved_digest(),
    };
    let submitted = wr_manager::operations::submit(&pool, "operator-a", &request).await?;
    assert_eq!(
        NodeOperationPhase::try_from(submitted.phase)?,
        NodeOperationPhase::Forward
    );
    configure_agent(&pool, "evidence-node", "activation-a").await;

    let _first_engine = claim_instruction(&pool, "evidence-node", "activation-a").await?;
    for expected in [
        NodeOperationStepKind::VerifyReleaseMetadata,
        NodeOperationStepKind::SelectRelease,
    ] {
        let instruction =
            wr_manager::operations::claim(&pool, "evidence-node", "activation-a", "agent-a")
                .await?
                .expect("operation remains claimable")
                .instruction
                .expect("instruction");
        assert_eq!(instruction.step, expected as i32);
        let operation =
            wr_manager::operations::report_step(&pool, &result_for(&instruction, ""), "agent-a")
                .await?;
        if expected == NodeOperationStepKind::SelectRelease {
            assert_eq!(
                operation.slots[0].next_step,
                NodeOperationStepKind::SelectRelease as i32,
                "a trusted empty result cannot substitute for selected revision/digest evidence"
            );
        }
    }

    let authority_count: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_node_slot_authority
             WHERE node_id = 'evidence-node' AND engine_slot = 'blue' AND authoritative",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(authority_count, 0, "target must remain non-serving");
    let operation = wr_manager::operations::get(&pool, &submitted.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(operation.state)?,
        NodeOperationState::Running
    );
    assert!(!operation.committed);

    Ok(())
}

#[tokio::test]
async fn forward_deadline_irreversibly_enters_deadline_free_restoration() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "9".repeat(64));
    let deployment = stage(&pool, "deadline-node", "deadline-op", &digest, &["blue"]).await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "deadline-node".into(),
            request_token: "deadline-op".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into()],
            target_revision: deployment.revision,
            bundle_digest: digest,
            policy: Some(policy(1)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    configure_agent(&pool, "deadline-node", "activation-a").await;
    pool.get()
        .await?
        .execute(
            "UPDATE wr_node_operations
             SET created_at = NOW() - INTERVAL '2 seconds',
                 forward_deadline = NOW() - INTERVAL '1 second'
             WHERE operation_id = $1",
            &[&uuid::Uuid::parse_str(&operation.operation_id)?],
        )
        .await?;
    assert!(
        wr_manager::operations::claim(&pool, "deadline-node", "activation-a", "agent-a")
            .await?
            .is_none(),
        "expired forward work must emit no forward instruction"
    );
    let restoring = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationPhase::try_from(restoring.phase)?,
        NodeOperationPhase::RestoringSource
    );
    assert!(restoring.forward_fenced);
    assert!(restoring.restoration_requested);

    assert!(
        wr_manager::operations::claim(&pool, "deadline-node", "activation-a", "agent-a")
            .await?
            .is_none(),
        "an untouched slot requires no restoration host effect"
    );
    let terminal = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(terminal.state)?,
        NodeOperationState::Failed
    );
    assert_eq!(
        NodeOperationPhase::try_from(terminal.phase)?,
        NodeOperationPhase::Complete
    );

    Ok(())
}

#[tokio::test]
async fn rollback_supersedes_and_fences_committed_cleanup() -> Result<()> {
    let pool = manager_pool().await;
    let first_digest = format!("sha256:{}", "3".repeat(64));
    let first = stage(
        &pool,
        "supersede-node",
        "first-allocation",
        &first_digest,
        &["blue"],
    )
    .await;
    wr_manager::db::complete_deployment(&pool, "supersede-node", first.revision, true, "").await?;

    let second_digest = format!("sha256:{}", "4".repeat(64));
    let second = stage(
        &pool,
        "supersede-node",
        "upgrade-op",
        &second_digest,
        &["blue"],
    )
    .await;
    let cleanup = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "supersede-node".into(),
            request_token: "upgrade-op".into(),
            action: NodeOperationAction::RollingUpgrade as i32,
            engine_slots: vec!["blue".into()],
            target_revision: second.revision,
            bundle_digest: second_digest,
            policy: Some(policy(1_800)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    let cleanup_id = uuid::Uuid::parse_str(&cleanup.operation_id)?;
    let client = pool.get().await?;
    client
        .execute(
            "UPDATE wr_node_operations SET committed = TRUE, committed_at = NOW(),
                    phase = 'committed_cleanup', state = 'paused'
             WHERE operation_id = $1",
            &[&cleanup_id],
        )
        .await?;
    let second_revision = second.revision as i64;
    client
        .execute(
            "UPDATE wr_nodes SET current_revision = $1, target_revision = NULL
             WHERE node_id = 'supersede-node'",
            &[&second_revision],
        )
        .await?;
    client
        .execute(
            "UPDATE wr_node_deployments SET state = 'succeeded', completed_at = NOW()
             WHERE node_id = 'supersede-node' AND revision = $1",
            &[&second_revision],
        )
        .await?;
    drop(client);

    let rollback = wr_manager::db::begin_rollback(
        &pool,
        "supersede-node",
        first.revision,
        "rollback-op",
        "operator-a",
    )
    .await?
    .record;
    let rollback = wr_manager::db::finalize_deployment(
        &pool,
        &FinalizeDeploymentRequest {
            node_id: "supersede-node".into(),
            attempt_token: "rollback-op".into(),
            revision: rollback.revision,
            bundle_digest: rollback.bundle_digest.clone(),
            resolved_release_digest: resolved_digest(),
        },
        "operator-a",
    )
    .await?
    .record;
    let replacement = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "supersede-node".into(),
            request_token: "rollback-op".into(),
            action: NodeOperationAction::Rollback as i32,
            engine_slots: vec!["blue".into()],
            target_revision: rollback.revision,
            bundle_digest: rollback.bundle_digest,
            policy: Some(policy(1_800)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    let old = wr_manager::operations::get(&pool, &cleanup.operation_id).await?;
    assert_eq!(
        NodeOperationPhase::try_from(old.phase)?,
        NodeOperationPhase::Superseded
    );
    assert_eq!(old.cleanup_superseded_by, replacement.operation_id);
    assert_eq!(
        NodeOperationState::try_from(old.state)?,
        NodeOperationState::Succeeded
    );

    Ok(())
}

#[tokio::test]
async fn scale_orders_new_then_retained_then_removed_slots_lexically() -> Result<()> {
    let pool = manager_pool().await;
    let old_digest = format!("sha256:{}", "1".repeat(64));
    let old = stage(
        &pool,
        "scale-node",
        "old-allocation",
        &old_digest,
        &["b-retained", "d-removed"],
    )
    .await;
    wr_manager::db::complete_deployment(&pool, "scale-node", old.revision, true, "").await?;

    let new_digest = format!("sha256:{}", "2".repeat(64));
    let new = stage(
        &pool,
        "scale-node",
        "scale-op",
        &new_digest,
        &["a-new", "b-retained"],
    )
    .await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "scale-node".into(),
            request_token: "scale-op".into(),
            action: NodeOperationAction::Scale as i32,
            engine_slots: vec!["b-retained".into(), "a-new".into()],
            target_revision: new.revision,
            bundle_digest: new_digest,
            policy: Some(policy(1_800)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    assert_eq!(
        operation
            .slots
            .iter()
            .map(|slot| slot.engine_slot.as_str())
            .collect::<Vec<_>>(),
        vec!["a-new", "b-retained", "d-removed"]
    );
    assert_eq!(operation.slots[0].source_revision, 0);
    assert_eq!(operation.slots[2].target_revision, 0);

    Ok(())
}

#[tokio::test]
async fn drain_reaches_zero_authority_terminal_and_preserves_other_capacity() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "a".repeat(64));
    let source =
        stage_with_module(&pool, "drain-node", "source", &digest, &["blue", "green"]).await;
    wr_manager::db::complete_deployment(&pool, "drain-node", source.revision, true, "").await?;
    register_ready(&pool, &source, "blue", "drain-blue").await?;
    register_ready(&pool, &source, "green", "drain-green").await?;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "drain-node".into(),
            request_token: "drain-op".into(),
            action: NodeOperationAction::Drain as i32,
            engine_slots: vec!["blue".into()],
            target_revision: 0,
            bundle_digest: String::new(),
            resolved_release_digest: String::new(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                canary_slot: "blue".into(),
                pause_after_canary: false,
                allow_downtime: false,
                deadline_seconds: 300,
            }),
        },
    )
    .await?;
    assert_eq!(operation.slots[0].target_revision, 0);
    configure_agent(&pool, "drain-node", "activation-a").await;

    let verify = claim_instruction(&pool, "drain-node", "activation-a").await?;
    assert_eq!(verify.step, NodeOperationStepKind::VerifyTarget as i32);
    let after_source = observe(
        &pool,
        &verify,
        "blue",
        BackendProcessState::Running,
        "backend-old",
        "process-old",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    assert_eq!(
        after_source.slots[0].next_step,
        NodeOperationStepKind::StopBackend as i32
    );
    let coherent = wr_manager::db::get_cluster_status_snapshot(&pool).await?;
    assert!(coherent
        .active_operations
        .iter()
        .any(|candidate| candidate.operation_id == operation.operation_id));
    assert!(coherent
        .observations
        .iter()
        .any(|observation| observation.node_id == "drain-node"));
    assert!(coherent
        .agent_attestations
        .iter()
        .any(|attestation| attestation.node_id == "drain-node"));

    let stop = claim_instruction(&pool, "drain-node", "activation-a").await?;
    assert_eq!(stop.pinned_backend_instance_id, "backend-old");
    wr_manager::db::deregister_engine(&pool, "drain-blue").await?;
    pool.get()
        .await?
        .execute(
            "UPDATE wr_node_slot_observations
             SET lifecycle_status = NULL, backend_state = 'exited',
                 observed_at = NOW() - INTERVAL '1 minute'
             WHERE node_id = 'drain-node' AND engine_slot = 'blue'",
            &[],
        )
        .await?;
    let inspection = claim_instruction(&pool, "drain-node", "activation-a").await?;
    assert_eq!(
        inspection.step,
        NodeOperationStepKind::InspectBackend as i32
    );
    assert_eq!(inspection.operation_id, operation.operation_id);
    assert_eq!(inspection.target.as_ref().unwrap().engine_slot, "blue");
    let still_stopping = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        still_stopping.slots[0].next_step,
        NodeOperationStepKind::StopBackend as i32
    );
    observe(
        &pool,
        &stop,
        "blue",
        BackendProcessState::Exited,
        "backend-old",
        "",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    assert!(
        wr_manager::operations::claim(&pool, "drain-node", "activation-a", "agent-a")
            .await?
            .is_none(),
        "zero-authority switch is manager-owned"
    );
    assert!(
        wr_manager::operations::claim(&pool, "drain-node", "activation-a", "agent-a")
            .await?
            .is_none(),
        "completed drain emits no further host effect"
    );
    let terminal = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(terminal.state)?,
        NodeOperationState::Succeeded
    );
    assert_eq!(terminal.slots[0].authoritative_revision, 0);
    assert!(terminal.slots[0].complete);
    assert!(wr_manager::operations::authorities(&pool, "drain-node")
        .await?
        .iter()
        .all(|authority| authority.engine_slot != "blue"));

    Ok(())
}

#[tokio::test]
async fn restart_recovers_a_lost_start_report_from_exact_replacement_evidence() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "b".repeat(64));
    let source = stage_with_module(&pool, "restart-node", "source", &digest, &["blue"]).await;
    wr_manager::db::complete_deployment(&pool, "restart-node", source.revision, true, "").await?;
    register_ready(&pool, &source, "blue", "restart-old").await?;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "restart-node".into(),
            request_token: "restart-op".into(),
            action: NodeOperationAction::Restart as i32,
            engine_slots: vec!["blue".into()],
            target_revision: 0,
            bundle_digest: String::new(),
            policy: Some(policy(300)),
            resolved_release_digest: String::new(),
        },
    )
    .await?;
    configure_agent(&pool, "restart-node", "activation-a").await;
    let verify = claim_instruction(&pool, "restart-node", "activation-a").await?;
    observe(
        &pool,
        &verify,
        "blue",
        BackendProcessState::Running,
        "backend-old",
        "process-old",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    let stop = claim_instruction(&pool, "restart-node", "activation-a").await?;
    wr_manager::db::deregister_engine(&pool, "restart-old").await?;
    observe(
        &pool,
        &stop,
        "blue",
        BackendProcessState::Exited,
        "backend-old",
        "",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    let start = claim_instruction(&pool, "restart-node", "activation-a").await?;
    assert_eq!(start.step, NodeOperationStepKind::StartBackend as i32);
    register_ready(&pool, &source, "blue", "restart-new").await?;
    observe(
        &pool,
        &start,
        "blue",
        BackendProcessState::Running,
        "backend-new",
        "process-new",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    observe(
        &pool,
        &start,
        "blue",
        BackendProcessState::Running,
        "backend-new",
        "process-new",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    assert!(
        wr_manager::operations::claim(&pool, "restart-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    assert!(
        wr_manager::operations::claim(&pool, "restart-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let terminal = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(terminal.state)?,
        NodeOperationState::Succeeded
    );
    assert_eq!(terminal.slots[0].pinned_backend_instance_id, "backend-new");
    assert_eq!(terminal.slots[0].pinned_process_instance_id, "process-new");

    Ok(())
}

async fn drive_initial_slot_to_serving_gate(
    pool: &deadpool_postgres::Pool,
    deployment: &DeploymentRecord,
    slot: &str,
    activation: &str,
    engine_id: &str,
    backend: &str,
    process: &str,
) -> Result<()> {
    let verify_release = claim_instruction(pool, &deployment.node_id, activation).await?;
    assert_eq!(
        verify_release.step,
        NodeOperationStepKind::VerifyReleaseMetadata as i32
    );
    report_ok(pool, &verify_release, "", "").await?;
    let select = claim_instruction(pool, &deployment.node_id, activation).await?;
    assert_eq!(select.step, NodeOperationStepKind::SelectRelease as i32);
    report_ok(pool, &select, "", "").await?;
    observe(
        pool,
        &select,
        slot,
        BackendProcessState::Exited,
        backend,
        "",
        deployment.revision,
        &deployment.bundle_digest,
    )
    .await?;
    let start = claim_instruction(pool, &deployment.node_id, activation).await?;
    assert_eq!(start.step, NodeOperationStepKind::StartBackend as i32);
    register_ready(pool, deployment, slot, engine_id).await?;
    observe(
        pool,
        &start,
        slot,
        BackendProcessState::Running,
        backend,
        process,
        deployment.revision,
        &deployment.bundle_digest,
    )
    .await?;
    observe(
        pool,
        &start,
        slot,
        BackendProcessState::Running,
        backend,
        process,
        deployment.revision,
        &deployment.bundle_digest,
    )
    .await?;
    assert!(
        wr_manager::operations::claim(pool, &deployment.node_id, activation, "agent-a")
            .await?
            .is_none(),
        "authority switch is manager-owned"
    );
    Ok(())
}

#[tokio::test]
async fn deployment_requires_module_route_convergence_and_pauses_after_canary() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "c".repeat(64));
    let target = stage_with_module(
        &pool,
        "canary-node",
        "canary-op",
        &digest,
        &["blue", "green"],
    )
    .await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "canary-node".into(),
            request_token: "canary-op".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into(), "green".into()],
            target_revision: target.revision,
            bundle_digest: target.bundle_digest.clone(),
            resolved_release_digest: resolved_digest(),
            policy: Some(RolloutPolicy {
                max_unavailable: 1,
                canary_slot: "blue".into(),
                pause_after_canary: true,
                allow_downtime: true,
                deadline_seconds: 300,
            }),
        },
    )
    .await?;
    configure_agent(&pool, "canary-node", "activation-a").await;
    drive_initial_slot_to_serving_gate(
        &pool,
        &target,
        "blue",
        "activation-a",
        "canary-blue",
        "backend-blue",
        "process-blue",
    )
    .await?;
    assert!(
        wr_manager::operations::claim(&pool, "canary-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let pending = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        pending.slots[0].conditions[0].code,
        "SERVING_CONVERGENCE_PENDING"
    );
    wr_manager::db::publish_engine_readiness(&pool, "canary-blue", &[module_descriptor()]).await?;
    assert!(
        wr_manager::operations::claim(&pool, "canary-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let paused = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(paused.state)?,
        NodeOperationState::Paused
    );
    assert_eq!(paused.conditions[0].code, "CANARY_PAUSED");

    wr_manager::operations::resume(&pool, &operation.operation_id, "operator-a").await?;
    drive_initial_slot_to_serving_gate(
        &pool,
        &target,
        "green",
        "activation-a",
        "canary-green",
        "backend-green",
        "process-green",
    )
    .await?;
    assert!(
        wr_manager::operations::claim(&pool, "canary-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    wr_manager::db::publish_engine_readiness(&pool, "canary-green", &[module_descriptor()]).await?;
    assert!(
        wr_manager::operations::claim(&pool, "canary-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    assert!(
        wr_manager::operations::claim(&pool, "canary-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let committed = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert!(committed.committed);
    assert_eq!(
        NodeOperationState::try_from(committed.state)?,
        NodeOperationState::Succeeded
    );
    let current: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT current_revision FROM wr_nodes WHERE node_id = 'canary-node'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(current as u64, target.revision);

    Ok(())
}

#[tokio::test]
async fn route_change_serialization_prevents_commit_from_an_older_snapshot() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "6".repeat(64));
    let target = stage_with_module(&pool, "race-node", "race-op", &digest, &["blue"]).await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "race-node".into(),
            request_token: "race-op".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into()],
            target_revision: target.revision,
            bundle_digest: target.bundle_digest.clone(),
            policy: Some(policy(300)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    configure_agent(&pool, "race-node", "activation-a").await;
    drive_initial_slot_to_serving_gate(
        &pool,
        &target,
        "blue",
        "activation-a",
        "race-engine",
        "race-backend",
        "race-process",
    )
    .await?;
    wr_manager::db::publish_engine_readiness(&pool, "race-engine", &[module_descriptor()]).await?;

    let mut writer = pool.get().await?;
    let transaction = writer.transaction().await?;
    transaction
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await?;
    transaction
        .execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = 'race-engine'",
            &[],
        )
        .await?;
    transaction
        .execute(
            "UPDATE wr_manager_lock SET version = version + 1 WHERE id = 1",
            &[],
        )
        .await?;
    let contender_pool = pool.clone();
    let mut contender = tokio::spawn(async move {
        wr_manager::operations::claim(&contender_pool, "race-node", "activation-a", "agent-a").await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut contender)
            .await
            .is_err(),
        "reconciliation must wait at the serialization boundary before taking its RR snapshot"
    );
    transaction.commit().await?;
    let _ = contender.await?;

    assert!(
        wr_manager::operations::claim(&pool, "race-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let pending = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert!(!pending.committed);
    assert!(!pending.slots[0].complete);
    assert_eq!(
        pending.slots[0].next_step,
        NodeOperationStepKind::VerifyServing as i32
    );
    assert_eq!(
        pending.slots[0].conditions[0].code,
        "SERVING_CONVERGENCE_PENDING"
    );

    Ok(())
}

#[tokio::test]
async fn restoration_targets_only_changed_slots_and_completes_from_observation() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "d".repeat(64));
    let target = stage_with_module(
        &pool,
        "restore-node",
        "restore-op",
        &digest,
        &["blue", "green"],
    )
    .await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "restore-node".into(),
            request_token: "restore-op".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into(), "green".into()],
            target_revision: target.revision,
            bundle_digest: target.bundle_digest.clone(),
            policy: Some(policy(300)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    configure_agent(&pool, "restore-node", "activation-a").await;
    let verify_release = claim_instruction(&pool, "restore-node", "activation-a").await?;
    report_ok(&pool, &verify_release, "", "").await?;
    let select = claim_instruction(&pool, "restore-node", "activation-a").await?;
    report_ok(&pool, &select, "", "").await?;
    observe(
        &pool,
        &select,
        "blue",
        BackendProcessState::Exited,
        "selected-backend",
        "",
        target.revision,
        &target.bundle_digest,
    )
    .await?;
    let start = claim_instruction(&pool, "restore-node", "activation-a").await?;
    let failed =
        wr_manager::operations::report_step(&pool, &result_for(&start, "START_FAILED"), "agent-a")
            .await?;
    assert_eq!(
        NodeOperationPhase::try_from(failed.phase)?,
        NodeOperationPhase::RestoringSource
    );
    let blue = failed
        .slots
        .iter()
        .find(|slot| slot.engine_slot == "blue")
        .expect("blue slot");
    let green = failed
        .slots
        .iter()
        .find(|slot| slot.engine_slot == "green")
        .expect("green slot");
    assert!(blue.changed && !blue.complete);
    assert!(!green.changed && green.complete);

    let inspection = claim_instruction(&pool, "restore-node", "activation-a").await?;
    assert!(inspection.restoration);
    assert_eq!(
        inspection.step,
        NodeOperationStepKind::InspectBackend as i32
    );
    observe(
        &pool,
        &inspection,
        "blue",
        BackendProcessState::Exited,
        "selected-backend",
        "",
        target.revision,
        &target.bundle_digest,
    )
    .await?;
    let restore = claim_instruction(&pool, "restore-node", "activation-a").await?;
    assert!(restore.restoration);
    assert_eq!(restore.step, NodeOperationStepKind::RestoreSource as i32);
    observe(
        &pool,
        &restore,
        "blue",
        BackendProcessState::Exited,
        "selected-backend",
        "",
        0,
        "",
    )
    .await?;
    assert!(
        wr_manager::operations::claim(&pool, "restore-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let terminal = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(terminal.state)?,
        NodeOperationState::Failed
    );
    assert_eq!(
        NodeOperationPhase::try_from(terminal.phase)?,
        NodeOperationPhase::Complete
    );

    Ok(())
}

async fn delivered_stop_fixture(
    pool: &deadpool_postgres::Pool,
    node_id: &str,
) -> Result<(
    DeploymentRecord,
    wr_common::wruntime::NodeOperation,
    wr_common::wruntime::AgentInstruction,
)> {
    let digest = format!("sha256:{}", "7".repeat(64));
    let source = stage_with_module(pool, node_id, "source", &digest, &["blue"]).await;
    wr_manager::db::complete_deployment(pool, node_id, source.revision, true, "").await?;
    register_ready(pool, &source, "blue", &format!("{node_id}-engine")).await?;
    let operation = wr_manager::operations::submit(
        pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: node_id.into(),
            request_token: format!("ambiguous-stop-{node_id}"),
            action: NodeOperationAction::Restart as i32,
            engine_slots: vec!["blue".into()],
            target_revision: 0,
            bundle_digest: String::new(),
            policy: Some(policy(300)),
            resolved_release_digest: String::new(),
        },
    )
    .await?;
    configure_agent(pool, node_id, "activation-a").await;
    let verify = claim_instruction(pool, node_id, "activation-a").await?;
    observe(
        pool,
        &verify,
        "blue",
        BackendProcessState::Running,
        "backend-old",
        "process-old",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    let stop = claim_instruction(pool, node_id, "activation-a").await?;
    assert_eq!(stop.step, NodeOperationStepKind::StopBackend as i32);
    let durable = wr_manager::operations::get(pool, &operation.operation_id).await?;
    assert!(durable.slots[0].effect_ambiguous);
    Ok((source, operation, stop))
}

#[tokio::test]
async fn delivered_effect_ambiguity_is_inspected_for_cancel_deadline_and_error() -> Result<()> {
    let pool = manager_pool().await;
    for mode in ["cancel", "deadline", "error"] {
        let node_id = format!("ambiguous-{mode}");
        let (source, operation, stop) = delivered_stop_fixture(&pool, &node_id).await?;
        match mode {
            "cancel" => {
                wr_manager::operations::cancel(&pool, &operation.operation_id, "operator-a")
                    .await?;
            }
            "deadline" => {
                pool.get()
                    .await?
                    .execute(
                        "UPDATE wr_node_operations
                         SET created_at = NOW() - INTERVAL '2 seconds',
                             forward_deadline = NOW() - INTERVAL '1 second'
                         WHERE operation_id = $1",
                        &[&uuid::Uuid::parse_str(&operation.operation_id)?],
                    )
                    .await?;
                assert!(
                    wr_manager::operations::claim(&pool, &node_id, "activation-a", "agent-a")
                        .await?
                        .is_none()
                );
            }
            "error" => {
                wr_manager::db::deregister_engine(&pool, &format!("{node_id}-engine")).await?;
                wr_manager::operations::report_step(
                    &pool,
                    &result_for(&stop, "HOST_STEP_FAILED"),
                    "agent-a",
                )
                .await?;
            }
            _ => unreachable!(),
        }
        let restoring = wr_manager::operations::get(&pool, &operation.operation_id).await?;
        assert_eq!(
            NodeOperationPhase::try_from(restoring.phase)?,
            NodeOperationPhase::RestoringSource
        );
        assert!(restoring.slots[0].effect_ambiguous);
        assert!(!restoring.slots[0].complete);
        let inspection = claim_instruction(&pool, &node_id, "activation-a").await?;
        assert_eq!(
            inspection.step,
            NodeOperationStepKind::InspectBackend as i32
        );
        assert_eq!(inspection.operation_id, operation.operation_id);
        assert_eq!(inspection.target.as_ref().unwrap().engine_slot, "blue");
        if mode == "error" {
            observe(
                &pool,
                &inspection,
                "blue",
                BackendProcessState::Exited,
                "backend-old",
                "",
                source.revision,
                &source.bundle_digest,
            )
            .await?;
            let restore = claim_instruction(&pool, &node_id, "activation-a").await?;
            assert_eq!(restore.step, NodeOperationStepKind::RestoreSource as i32);
            register_ready(&pool, &source, "blue", &format!("{node_id}-restored")).await?;
            observe(
                &pool,
                &restore,
                "blue",
                BackendProcessState::Running,
                "backend-restored",
                "process-restored",
                source.revision,
                &source.bundle_digest,
            )
            .await?;
        } else {
            observe(
                &pool,
                &inspection,
                "blue",
                BackendProcessState::Running,
                "backend-old",
                "process-old",
                source.revision,
                &source.bundle_digest,
            )
            .await?;
        }
        assert!(
            wr_manager::operations::claim(&pool, &node_id, "activation-a", "agent-a")
                .await?
                .is_none(),
            "unchanged source proof completes restoration without a compensating effect"
        );
        let terminal = wr_manager::operations::get(&pool, &operation.operation_id).await?;
        let expected = if mode == "cancel" {
            NodeOperationState::Cancelled
        } else {
            NodeOperationState::Failed
        };
        assert_eq!(NodeOperationState::try_from(terminal.state)?, expected);
        assert!(!terminal.slots[0].effect_ambiguous);
    }
    Ok(())
}

#[tokio::test]
async fn lease_loss_requires_fresh_state_before_a_mutating_effect_is_reissued() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "e".repeat(64));
    let target = stage_with_module(&pool, "resume-node", "resume-op", &digest, &["blue"]).await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "resume-node".into(),
            request_token: "resume-op".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into()],
            target_revision: target.revision,
            bundle_digest: target.bundle_digest.clone(),
            policy: Some(policy(300)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    configure_agent(&pool, "resume-node", "activation-a").await;
    let verify_release = claim_instruction(&pool, "resume-node", "activation-a").await?;
    report_ok(&pool, &verify_release, "", "").await?;
    let first_select = claim_instruction(&pool, "resume-node", "activation-a").await?;
    assert_eq!(
        first_select.step,
        NodeOperationStepKind::SelectRelease as i32
    );
    pool.get()
        .await?
        .execute(
            "UPDATE wr_node_operations SET lease_expires_at = NOW() - INTERVAL '1 second'
             WHERE operation_id = $1",
            &[&uuid::Uuid::parse_str(&operation.operation_id)?],
        )
        .await?;
    assert!(
        wr_manager::operations::claim(&pool, "resume-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    wr_manager::operations::resume(&pool, &operation.operation_id, "operator-a").await?;
    configure_agent(&pool, "resume-node", "activation-b").await;
    let inspection = claim_instruction(&pool, "resume-node", "activation-b").await?;
    assert_eq!(
        inspection.step,
        NodeOperationStepKind::InspectBackend as i32
    );
    assert_eq!(inspection.operation_id, operation.operation_id);
    assert_eq!(inspection.agent_instance_id, "activation-b");
    assert!(inspection.lease_epoch > first_select.lease_epoch);
    observe(
        &pool,
        &inspection,
        "blue",
        BackendProcessState::Exited,
        "pre-effect",
        "",
        0,
        "",
    )
    .await?;
    let reissued = claim_instruction(&pool, "resume-node", "activation-b").await?;
    assert_eq!(reissued.step, NodeOperationStepKind::SelectRelease as i32);
    assert!(reissued.lease_epoch > first_select.lease_epoch);

    Ok(())
}

#[tokio::test]
async fn agent_policy_update_fences_durable_work_until_explicit_resume() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "d".repeat(64));
    let target = stage(&pool, "update-node", "update-op", &digest, &["blue"]).await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "update-node".into(),
            request_token: "update-op".into(),
            action: NodeOperationAction::InitialApply as i32,
            engine_slots: vec!["blue".into()],
            target_revision: target.revision,
            bundle_digest: digest,
            policy: Some(policy(300)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    configure_agent_with_retention(&pool, "update-node", "activation-old", 1).await;
    let instruction = claim_instruction(&pool, "update-node", "activation-old").await?;
    wr_manager::operations::put_agent_policy(
        &pool,
        "operator-a",
        &helpers::node_agent::systemd_policy("update-node", 2),
    )
    .await?;
    let paused = wr_manager::operations::get(&pool, &operation.operation_id).await?;
    assert_eq!(
        NodeOperationState::try_from(paused.state)?,
        NodeOperationState::Paused
    );
    assert_eq!(paused.conditions[0].code, "AGENT_POLICY_UPDATED");
    let stale = wr_manager::operations::renew(
        &pool,
        "update-node",
        &operation.operation_id,
        instruction.lease_epoch,
        "activation-old",
        "agent-a",
    )
    .await
    .expect_err("policy replacement must immediately fence the old activation");
    assert_eq!(stale.code(), Code::Aborted);
    Ok(())
}

#[tokio::test]
async fn rolling_commit_cleanup_requires_typed_retention_evidence() -> Result<()> {
    let pool = manager_pool().await;
    let oldest_digest = format!("sha256:{}", "d".repeat(64));
    let oldest =
        stage_with_module(&pool, "cleanup-node", "oldest", &oldest_digest, &["blue"]).await;
    wr_manager::db::complete_deployment(&pool, "cleanup-node", oldest.revision, true, "").await?;
    let old_digest = format!("sha256:{}", "e".repeat(64));
    let old = stage_with_module(&pool, "cleanup-node", "old", &old_digest, &["blue"]).await;
    wr_manager::db::complete_deployment(&pool, "cleanup-node", old.revision, true, "").await?;
    let source_digest = format!("sha256:{}", "f".repeat(64));
    let source =
        stage_with_module(&pool, "cleanup-node", "source", &source_digest, &["blue"]).await;
    wr_manager::db::complete_deployment(&pool, "cleanup-node", source.revision, true, "").await?;
    register_ready(&pool, &source, "blue", "cleanup-old").await?;
    let history_digest = format!("sha256:{}", "1".repeat(64));
    let history =
        stage_with_module(&pool, "cleanup-node", "history", &history_digest, &["blue"]).await;
    wr_manager::db::complete_deployment(&pool, "cleanup-node", history.revision, true, "").await?;
    let deleted_digest = format!("sha256:{}", "2".repeat(64));
    let deleted =
        stage_with_module(&pool, "cleanup-node", "deleted", &deleted_digest, &["blue"]).await;
    wr_manager::db::complete_deployment(&pool, "cleanup-node", deleted.revision, true, "").await?;
    pool.get()
        .await?
        .execute(
            "UPDATE wr_nodes SET current_revision = $2, target_revision = NULL WHERE node_id = $1",
            &[&"cleanup-node", &(source.revision as i64)],
        )
        .await?;
    let target_digest = format!("sha256:{}", "0".repeat(64));
    let target = stage_with_module(
        &pool,
        "cleanup-node",
        "cleanup-op",
        &target_digest,
        &["blue"],
    )
    .await;
    let operation = wr_manager::operations::submit(
        &pool,
        "operator-a",
        &SubmitOperationRequest {
            node_id: "cleanup-node".into(),
            request_token: "cleanup-op".into(),
            action: NodeOperationAction::RollingUpgrade as i32,
            engine_slots: vec!["blue".into()],
            target_revision: target.revision,
            bundle_digest: target.bundle_digest.clone(),
            policy: Some(policy(300)),
            resolved_release_digest: resolved_digest(),
        },
    )
    .await?;
    pool.get()
        .await?
        .execute(
            "INSERT INTO wr_node_release_deletions
               (node_id, revision, bundle_digest, operation_id)
             VALUES ($1, $2, $3, $4)",
            &[
                &"cleanup-node",
                &(deleted.revision as i64),
                &deleted.bundle_digest,
                &Uuid::parse_str(&operation.operation_id)?,
            ],
        )
        .await?;
    configure_agent_with_retention(&pool, "cleanup-node", "activation-a", 1).await;
    let verify_release = claim_instruction(&pool, "cleanup-node", "activation-a").await?;
    report_ok(&pool, &verify_release, "", "").await?;
    let verify_source = claim_instruction(&pool, "cleanup-node", "activation-a").await?;
    observe(
        &pool,
        &verify_source,
        "blue",
        BackendProcessState::Running,
        "backend-old",
        "process-old",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    let stop = claim_instruction(&pool, "cleanup-node", "activation-a").await?;
    wr_manager::db::deregister_engine(&pool, "cleanup-old").await?;
    observe(
        &pool,
        &stop,
        "blue",
        BackendProcessState::Exited,
        "backend-old",
        "",
        source.revision,
        &source.bundle_digest,
    )
    .await?;
    let select = claim_instruction(&pool, "cleanup-node", "activation-a").await?;
    report_ok(&pool, &select, "", "").await?;
    observe(
        &pool,
        &select,
        "blue",
        BackendProcessState::Exited,
        "backend-old",
        "",
        target.revision,
        &target.bundle_digest,
    )
    .await?;
    let start = claim_instruction(&pool, "cleanup-node", "activation-a").await?;
    register_ready(&pool, &target, "blue", "cleanup-new").await?;
    observe(
        &pool,
        &start,
        "blue",
        BackendProcessState::Running,
        "backend-new",
        "process-new",
        target.revision,
        &target.bundle_digest,
    )
    .await?;
    observe(
        &pool,
        &start,
        "blue",
        BackendProcessState::Running,
        "backend-new",
        "process-new",
        target.revision,
        &target.bundle_digest,
    )
    .await?;
    assert!(
        wr_manager::operations::claim(&pool, "cleanup-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    wr_manager::db::publish_engine_readiness(&pool, "cleanup-new", &[module_descriptor()]).await?;
    assert!(
        wr_manager::operations::claim(&pool, "cleanup-node", "activation-a", "agent-a")
            .await?
            .is_none()
    );
    let cleanup = claim_instruction(&pool, "cleanup-node", "activation-a").await?;
    assert_eq!(cleanup.step, NodeOperationStepKind::CleanupRelease as i32);
    assert_eq!(
        cleanup.cleanup_delete_releases,
        vec![
            ReleaseInventoryEntry {
                revision: oldest.revision,
                bundle_digest: oldest.bundle_digest.clone(),
                resolved_release_digest: oldest.resolved_release_digest.clone(),
            },
            ReleaseInventoryEntry {
                revision: old.revision,
                bundle_digest: old.bundle_digest.clone(),
                resolved_release_digest: old.resolved_release_digest.clone(),
            },
        ],
        "deleted history must not consume the retention slot held by the newest extant history"
    );
    let mut failed_inspection = result_for(&cleanup, "");
    failed_inspection.backend_query_error = "release inventory query failed".into();
    let paused = wr_manager::operations::report_step(&pool, &failed_inspection, "agent-a").await?;
    assert_eq!(
        NodeOperationState::try_from(paused.state)?,
        NodeOperationState::Paused
    );
    assert_eq!(paused.conditions[0].code, "CLEANUP_QUERY_ERROR");
    assert!(paused.cleanup_evidence.is_none());
    wr_manager::operations::put_agent_policy(
        &pool,
        "operator-a",
        &helpers::node_agent::systemd_policy("cleanup-node", 3),
    )
    .await?;
    let stored_allowlist: Option<Vec<u8>> = pool
        .get()
        .await?
        .query_one(
            "SELECT cleanup_delete_allowlist FROM wr_node_operations WHERE operation_id = $1",
            &[&Uuid::parse_str(&operation.operation_id)?],
        )
        .await?
        .get(0);
    assert!(
        stored_allowlist.is_none(),
        "a changed retention policy must revoke reported/undelivered cleanup authority"
    );
    configure_agent_with_retention(&pool, "cleanup-node", "activation-b", 3).await;
    wr_manager::operations::resume(&pool, &operation.operation_id, "operator-a").await?;
    let cleanup = claim_instruction(&pool, "cleanup-node", "activation-b").await?;
    assert_eq!(
        cleanup.cleanup_delete_releases,
        vec![ReleaseInventoryEntry {
            revision: oldest.revision,
            bundle_digest: oldest.bundle_digest.clone(),
            resolved_release_digest: oldest.resolved_release_digest.clone(),
        }],
        "resume must recompute cleanup authority from the replacement retention policy"
    );
    let mut cleanup_result = result_for(&cleanup, "");
    cleanup_result.cleanup_evidence = Some(CleanupReleaseEvidence {
        retained_releases: vec![
            ReleaseInventoryEntry {
                revision: target.revision,
                bundle_digest: target.bundle_digest.clone(),
                resolved_release_digest: target.resolved_release_digest.clone(),
            },
            ReleaseInventoryEntry {
                revision: source.revision,
                bundle_digest: source.bundle_digest.clone(),
                resolved_release_digest: source.resolved_release_digest.clone(),
            },
            ReleaseInventoryEntry {
                revision: history.revision,
                bundle_digest: history.bundle_digest.clone(),
                resolved_release_digest: history.resolved_release_digest.clone(),
            },
            ReleaseInventoryEntry {
                revision: old.revision,
                bundle_digest: old.bundle_digest.clone(),
                resolved_release_digest: old.resolved_release_digest.clone(),
            },
        ],
    });
    let terminal = wr_manager::operations::report_step(&pool, &cleanup_result, "agent-a").await?;
    assert_eq!(
        NodeOperationState::try_from(terminal.state)?,
        NodeOperationState::Succeeded
    );
    assert_eq!(
        NodeOperationPhase::try_from(terminal.phase)?,
        NodeOperationPhase::Complete
    );
    assert!(terminal.committed);
    assert!(terminal.cleanup_evidence.is_some());
    assert_eq!(terminal.operation_id, operation.operation_id);
    let receipt_count_before_duplicate: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_node_operation_result_receipts
             WHERE operation_id = $1",
            &[&Uuid::parse_str(&operation.operation_id)?],
        )
        .await?
        .get(0);

    let duplicate = wr_manager::operations::report_step(&pool, &cleanup_result, "agent-a").await?;
    assert_eq!(
        NodeOperationState::try_from(duplicate.state)?,
        NodeOperationState::Succeeded,
        "an exact retry must be acknowledged after atomic acceptance"
    );
    let mut conflicting = cleanup_result;
    conflicting.backend_query_error = "conflicting retry".into();
    let rejected = wr_manager::operations::report_step(&pool, &conflicting, "agent-a")
        .await
        .expect_err("a conflicting accepted-result retry must fail closed");
    assert_eq!(rejected.code(), Code::Aborted);
    let receipt_count_after_duplicate: i64 = pool
        .get()
        .await?
        .query_one(
            "SELECT COUNT(*) FROM wr_node_operation_result_receipts
             WHERE operation_id = $1",
            &[&Uuid::parse_str(&operation.operation_id)?],
        )
        .await?
        .get(0);
    assert_eq!(
        receipt_count_after_duplicate, receipt_count_before_duplicate,
        "duplicate acknowledgement must not advance twice"
    );
    let deletion_events = wr_manager::operations::events(&pool, &operation.operation_id)
        .await?
        .into_iter()
        .filter(|event| event.event_code == "RELEASE_DELETED")
        .collect::<Vec<_>>();
    assert_eq!(deletion_events.len(), 1);
    assert!(deletion_events[0].detail.contains(&oldest.bundle_digest));

    Ok(())
}
