mod helpers;

use anyhow::Result;
use helpers::db::manager_pool;
use tonic::Code;
use wr_common::wruntime::{
    BeginDeploymentRequest, ExpectedEngine, NodeOperationAction, NodeOperationState,
    NodeOperationStepKind, ReportStepResultRequest, RolloutPolicy, SubmitOperationRequest,
};

fn restart_request(token: &str) -> SubmitOperationRequest {
    SubmitOperationRequest {
        node_id: "operation-node".into(),
        request_token: token.into(),
        action: NodeOperationAction::Restart as i32,
        engine_slots: vec!["blue".into()],
        target_revision: 0,
        bundle_digest: String::new(),
        policy: Some(RolloutPolicy {
            max_unavailable: 1,
            canary_slot: "blue".into(),
            pause_after_canary: false,
            allow_downtime: false,
            deadline_seconds: 300,
        }),
    }
}

#[tokio::test]
async fn durable_operation_is_idempotent_and_fences_stale_epochs() -> Result<()> {
    let pool = manager_pool().await;
    let request = restart_request("same-request");
    let first = wr_manager::operations::submit(&pool, "operator-a", &request).await?;
    let duplicate = wr_manager::operations::submit(&pool, "operator-a", &request).await?;
    assert_eq!(first.operation_id, duplicate.operation_id);

    let mut conflicting = request.clone();
    conflicting.engine_slots = vec!["green".into()];
    let conflict = wr_manager::operations::submit(&pool, "operator-a", &conflicting)
        .await
        .expect_err("conflicting token reuse must fail");
    assert_eq!(conflict.code(), Code::AlreadyExists);

    let claim = wr_manager::operations::claim(&pool, "operation-node", "agent-a")
        .await?
        .expect("queued operation must be claimable");
    let instruction = claim
        .instruction
        .expect("claim must contain an instruction");
    assert_eq!(instruction.step, NodeOperationStepKind::StopSlot as i32);

    let stale = wr_manager::operations::renew(
        &pool,
        "operation-node",
        &instruction.operation_id,
        instruction.lease_epoch + 1,
        "agent-a",
    )
    .await
    .expect_err("stale epoch must be fenced");
    assert_eq!(stale.code(), Code::Aborted);

    let operation = wr_manager::operations::report_step(
        &pool,
        &ReportStepResultRequest {
            node_id: "operation-node".into(),
            operation_id: instruction.operation_id,
            engine_slot: "blue".into(),
            lease_epoch: instruction.lease_epoch,
            step: instruction.step,
            succeeded: true,
            condition_code: String::new(),
            detail: String::new(),
        },
        "agent-a",
    )
    .await?;
    assert_eq!(
        NodeOperationState::try_from(operation.state)?,
        NodeOperationState::Running
    );
    assert_eq!(
        operation.slots[0].next_step,
        NodeOperationStepKind::StartSlot as i32
    );

    Ok(())
}

#[tokio::test]
async fn rolling_operation_cuts_over_slot_authority_before_commit() -> Result<()> {
    let pool = manager_pool().await;
    let digest = format!("sha256:{}", "7".repeat(64));
    let deployment = wr_manager::db::begin_deployment(
        &pool,
        &BeginDeploymentRequest {
            node_id: "rollout-node".into(),
            attempt_token: "stage-one".into(),
            bundle_digest: digest.clone(),
            expected_engines: vec![ExpectedEngine {
                engine_slot: "blue".into(),
                modules: vec![],
            }],
        },
    )
    .await?
    .record;
    let request = SubmitOperationRequest {
        node_id: "rollout-node".into(),
        request_token: "roll-one".into(),
        action: NodeOperationAction::RollingUpgrade as i32,
        engine_slots: vec!["blue".into()],
        target_revision: deployment.revision,
        bundle_digest: digest,
        policy: Some(RolloutPolicy {
            max_unavailable: 1,
            canary_slot: "blue".into(),
            pause_after_canary: false,
            allow_downtime: true,
            deadline_seconds: 300,
        }),
    };
    let submitted = wr_manager::operations::submit(&pool, "operator-a", &request).await?;
    let mut latest = submitted;
    for expected_step in [
        NodeOperationStepKind::VerifyRelease,
        NodeOperationStepKind::StopSlot,
        NodeOperationStepKind::SelectRelease,
        NodeOperationStepKind::StartSlot,
        NodeOperationStepKind::VerifyReady,
    ] {
        let claim = wr_manager::operations::claim(&pool, "rollout-node", "agent-a")
            .await?
            .expect("operation must remain claimable");
        let instruction = claim.instruction.expect("instruction");
        assert_eq!(instruction.step, expected_step as i32);
        latest = wr_manager::operations::report_step(
            &pool,
            &ReportStepResultRequest {
                node_id: "rollout-node".into(),
                operation_id: instruction.operation_id,
                engine_slot: "blue".into(),
                lease_epoch: instruction.lease_epoch,
                step: instruction.step,
                succeeded: true,
                condition_code: String::new(),
                detail: String::new(),
            },
            "agent-a",
        )
        .await?;
        if expected_step == NodeOperationStepKind::SelectRelease {
            let authoritative: bool = pool
                .get()
                .await?
                .query_one(
                    "SELECT authoritative FROM wr_node_slot_authority
                     WHERE node_id = 'rollout-node' AND engine_slot = 'blue' AND revision = $1",
                    &[&(deployment.revision as i64)],
                )
                .await?
                .get(0);
            assert!(authoritative);
        }
    }
    assert_eq!(
        NodeOperationState::try_from(latest.state)?,
        NodeOperationState::Succeeded
    );
    assert!(latest.committed);
    let node = pool
        .get()
        .await?
        .query_one(
            "SELECT current_revision, target_revision FROM wr_nodes WHERE node_id = 'rollout-node'",
            &[],
        )
        .await?;
    assert_eq!(
        node.get::<_, i64>("current_revision"),
        deployment.revision as i64
    );
    assert_eq!(node.get::<_, Option<i64>>("target_revision"), None);

    Ok(())
}
