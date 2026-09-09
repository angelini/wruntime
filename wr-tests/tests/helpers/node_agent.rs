use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Result};
use prost::Message;
use tokio::sync::watch;
use wr_cli::cmd::node_agent::{
    AgentClock, AgentFuture, AgentManager, LeaseIdentity, LeaseManager, ReportResultError,
    ReportResultFuture,
};
use wr_cli::cmd::node_backend::{BackendFuture, InstructionExecutor, StepEvidence};
use wr_common::agent_policy::{AGENT_CAPABILITIES, AGENT_PROTOCOL_VERSION};
use wr_common::wruntime::{
    AgentInstruction, BackendKind, CleanupReleaseEvidence, NodeAgentAttestation, NodeAgentPolicy,
    NodeCleanupInstruction, ReleaseInventoryEntry, ReportNodeCleanupResultRequest,
    ReportNodeObservationRequest, ReportStepResultRequest,
};

pub fn systemd_policy(node_id: &str, retention_count: u32) -> NodeAgentPolicy {
    NodeAgentPolicy {
        node_id: node_id.into(),
        protocol_version: AGENT_PROTOCOL_VERSION.into(),
        backend: BackendKind::Systemd as i32,
        retention_count: Some(retention_count),
        binary_digest: format!("sha256:{}", "a".repeat(64)),
        capabilities: AGENT_CAPABILITIES
            .iter()
            .map(|value| (*value).into())
            .collect(),
    }
}

pub fn attestation(policy: &NodeAgentPolicy, activation: &str) -> NodeAgentAttestation {
    NodeAgentAttestation {
        node_id: policy.node_id.clone(),
        agent_instance_id: activation.into(),
        protocol_version: policy.protocol_version.clone(),
        binary_digest: policy.binary_digest.clone(),
        backend: policy.backend,
        capabilities: policy.capabilities.clone(),
        ..Default::default()
    }
}

pub struct FakeClock;

impl AgentClock for FakeClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1)
    }

    fn sleep<'a>(&'a self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { tokio::task::yield_now().await })
    }
}

pub struct FakeManager {
    pub renewals: AtomicUsize,
    pub reject_renewal: AtomicBool,
}

impl FakeManager {
    pub fn rejecting() -> Self {
        Self {
            renewals: AtomicUsize::new(0),
            reject_renewal: AtomicBool::new(true),
        }
    }
}

impl LeaseManager for FakeManager {
    fn renew<'a>(&'a self, _lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async move {
            self.renewals.fetch_add(1, Ordering::SeqCst);
            if self.reject_renewal.load(Ordering::SeqCst) {
                bail!("fake lease rejected");
            }
            Ok(())
        })
    }
}

#[derive(Clone, Default)]
pub struct FakeBlockedBackend {
    pub effects_started: Arc<AtomicUsize>,
    pub effects_reaped: Arc<AtomicUsize>,
}

impl InstructionExecutor for FakeBlockedBackend {
    fn execute<'a>(
        &'a self,
        _instruction: &'a AgentInstruction,
        mut cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, StepEvidence> {
        Box::pin(async move {
            self.effects_started.fetch_add(1, Ordering::SeqCst);
            while !*cancelled.borrow() && cancelled.changed().await.is_ok() {}
            self.effects_reaped.fetch_add(1, Ordering::SeqCst);
            bail!("fake command was cancelled and reaped")
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CleanupResultLoss {
    BeforeAcceptance,
    AfterAcceptance,
}

pub struct FakeCleanupManager {
    instruction: NodeCleanupInstruction,
    loss: CleanupResultLoss,
    accepted_payload: Mutex<Option<Vec<u8>>>,
    first_report: AtomicBool,
    pub claims: AtomicUsize,
    pub claims_while_result_pending: AtomicUsize,
    pub reports: AtomicUsize,
    pub advancements: AtomicUsize,
    shutdown: watch::Sender<bool>,
}

impl FakeCleanupManager {
    pub fn new(loss: CleanupResultLoss, shutdown: watch::Sender<bool>) -> Self {
        Self {
            instruction: cleanup_instruction(),
            loss,
            accepted_payload: Mutex::new(None),
            first_report: AtomicBool::new(true),
            claims: AtomicUsize::new(0),
            claims_while_result_pending: AtomicUsize::new(0),
            reports: AtomicUsize::new(0),
            advancements: AtomicUsize::new(0),
            shutdown,
        }
    }

    fn accept_once(&self, payload: &[u8]) -> std::result::Result<(), ReportResultError> {
        let mut accepted = self.accepted_payload.lock().unwrap();
        match accepted.as_ref() {
            Some(existing) if existing == payload => Ok(()),
            Some(_) => Err(ReportResultError::Rejected(
                "conflicting cleanup result retry".into(),
            )),
            None => {
                *accepted = Some(payload.to_vec());
                self.advancements.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

impl LeaseManager for FakeCleanupManager {
    fn renew<'a>(&'a self, _lease: &'a LeaseIdentity) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl AgentManager for FakeCleanupManager {
    fn attest<'a>(&'a self, _attestation: NodeAgentAttestation) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn claim<'a>(
        &'a self,
        _node_id: &'a str,
        _agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<AgentInstruction>> {
        Box::pin(async { Ok(None) })
    }

    fn claim_cleanup<'a>(
        &'a self,
        _node_id: &'a str,
        _agent_instance_id: &'a str,
    ) -> AgentFuture<'a, Option<NodeCleanupInstruction>> {
        Box::pin(async move {
            let claim = self.claims.fetch_add(1, Ordering::SeqCst);
            if claim == 0 {
                Ok(Some(self.instruction.clone()))
            } else {
                self.claims_while_result_pending
                    .fetch_add(1, Ordering::SeqCst);
                let _ = self.shutdown.send(true);
                Ok(None)
            }
        })
    }

    fn renew_cleanup<'a>(
        &'a self,
        _instruction: &'a NodeCleanupInstruction,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn report_observation<'a>(
        &'a self,
        _request: ReportNodeObservationRequest,
    ) -> AgentFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn report_result<'a>(&'a self, _request: ReportStepResultRequest) -> ReportResultFuture<'a> {
        Box::pin(async {
            Err(ReportResultError::Rejected(
                "generic result was not expected".into(),
            ))
        })
    }

    fn report_cleanup<'a>(
        &'a self,
        request: ReportNodeCleanupResultRequest,
    ) -> ReportResultFuture<'a> {
        Box::pin(async move {
            self.reports.fetch_add(1, Ordering::SeqCst);
            let payload = request.encode_to_vec();
            if self.first_report.swap(false, Ordering::SeqCst) {
                if matches!(self.loss, CleanupResultLoss::AfterAcceptance) {
                    self.accept_once(&payload)?;
                }
                return Err(ReportResultError::Retryable(
                    "injected cleanup result response loss".into(),
                ));
            }
            self.accept_once(&payload)?;
            let _ = self.shutdown.send(true);
            Ok(())
        })
    }
}

#[derive(Default)]
pub struct FakeCleanupBackend {
    pub effects: AtomicUsize,
}

impl InstructionExecutor for FakeCleanupBackend {
    fn execute<'a>(
        &'a self,
        instruction: &'a AgentInstruction,
        _cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, StepEvidence> {
        Box::pin(async move {
            self.effects.fetch_add(1, Ordering::SeqCst);
            if instruction.step == wr_common::wruntime::NodeOperationStepKind::InspectBackend as i32
            {
                Ok(StepEvidence {
                    observed_revision: 1,
                    observed_digest: format!("sha256:{}", "a".repeat(64)),
                    backend_state: Some(wr_common::wruntime::BackendProcessState::Exited),
                    backend_instance_id: "backend-1".into(),
                    process_instance_id: "process-1".into(),
                    ..Default::default()
                })
            } else {
                Ok(StepEvidence::default())
            }
        })
    }

    fn execute_cleanup<'a>(
        &'a self,
        _instruction: &'a NodeCleanupInstruction,
        _cancelled: watch::Receiver<bool>,
    ) -> BackendFuture<'a, CleanupReleaseEvidence> {
        Box::pin(async move {
            self.effects.fetch_add(1, Ordering::SeqCst);
            Ok(CleanupReleaseEvidence {
                retained_releases: vec![],
            })
        })
    }
}

pub fn cleanup_instruction() -> NodeCleanupInstruction {
    NodeCleanupInstruction {
        node_id: "node-a".into(),
        agent_instance_id: "activation-a".into(),
        generation: 9,
        lease_epoch: 7,
        claim_instance: "00000000-0000-0000-0000-000000000009".into(),
        payload_digest: format!("sha256:{}", "a".repeat(64)),
        delete_releases: vec![ReleaseInventoryEntry {
            revision: 1,
            bundle_digest: format!("sha256:{}", "b".repeat(64)),
            resolved_release_digest: format!("sha256:{}", "c".repeat(64)),
        }],
        expected_inventory: vec![],
        deadline: None,
    }
}
