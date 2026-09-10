use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;
use wr_common::discovery::ManagerDiscovery;
use wr_common::lifecycle_service::AdmissionGate;
use wr_common::manager_client::{ManagerEpoch, RetryClass};
use wr_common::process_lifecycle::LifecycleSnapshotHandle;
use wr_common::task_group::{TaskCancellation, TaskExit};
use wr_common::wruntime::{
    DeregisterProxyRequest, ProcessLifecycleState, ProxyBreakerDestinationKind,
    ProxyBreakerEvidence, ProxyDeploymentMetadata, ProxyInventoryReport, ProxyListenerEvidence,
    ProxyListenerKind, RegisterProxyRequest, ReportProxyInventoryRequest,
};

use crate::config::ProxyDeploymentConfig;
use crate::routing::CachedRoutingTable;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListenerFacts {
    pub data_plane: bool,
    pub node_control: bool,
    pub peer: bool,
    pub external: bool,
}

#[derive(Clone)]
pub struct ReportSource {
    lifecycle: LifecycleSnapshotHandle,
    admission: AdmissionGate,
    listeners: ListenerFacts,
    routing: CachedRoutingTable,
}

impl ReportSource {
    pub fn new(
        lifecycle: LifecycleSnapshotHandle,
        admission: AdmissionGate,
        listeners: ListenerFacts,
        routing: CachedRoutingTable,
    ) -> Self {
        Self {
            lifecycle,
            admission,
            listeners,
            routing,
        }
    }

    pub async fn snapshot(&self) -> ProxyInventoryReport {
        let lifecycle = self.lifecycle.current();
        let observation = self.routing.synchronization_observation().await;
        let breakers = self.routing.breaker_summary();
        let listener = |kind: ProxyListenerKind, configured: bool| ProxyListenerEvidence {
            kind: kind as i32,
            configured,
            accepting: configured,
        };
        let breaker = |kind: ProxyBreakerDestinationKind,
                       counts: crate::circuit_breaker::BreakerCounts| {
            ProxyBreakerEvidence {
                destination_kind: kind as i32,
                total: counts.total,
                closed: counts.closed,
                open: counts.open,
                half_open: counts.half_open,
            }
        };

        ProxyInventoryReport {
            lifecycle_state: ProcessLifecycleState::from(lifecycle.state) as i32,
            admission_open: self.admission.is_open(),
            listeners: vec![
                listener(ProxyListenerKind::DataPlane, self.listeners.data_plane),
                listener(ProxyListenerKind::NodeControl, self.listeners.node_control),
                listener(ProxyListenerKind::Peer, self.listeners.peer),
                listener(ProxyListenerKind::External, self.listeners.external),
            ],
            installed_routing_table_version: observation
                .as_ref()
                .map_or(0, |value| value.installed_version),
            routing_observation_age_millis: observation.as_ref().map_or(0, |value| {
                u64::try_from(value.last_success.elapsed().as_millis()).unwrap_or(u64::MAX)
            }),
            routing_manager_id: observation
                .as_ref()
                .map_or_else(String::new, |value| value.manager_id.clone()),
            routing_synchronized: observation.is_some(),
            breakers: vec![
                breaker(
                    ProxyBreakerDestinationKind::LocalEngine,
                    breakers.local_engine,
                ),
                breaker(
                    ProxyBreakerDestinationKind::PeerProxy,
                    breakers.remote_proxy,
                ),
            ],
        }
    }
}

pub struct Reporter {
    discovery: Arc<ManagerDiscovery>,
    epoch: ManagerEpoch,
    process_instance_id: String,
    deployment: Option<ProxyDeploymentMetadata>,
    source: ReportSource,
}

impl Reporter {
    pub async fn connect(
        discovery: Arc<ManagerDiscovery>,
        process_instance_id: String,
        deployment: Option<&ProxyDeploymentConfig>,
        source: ReportSource,
    ) -> Result<Self, tonic::Status> {
        let epoch = discovery.pin(RetryClass::FreshRegistration).await?;
        Ok(Self {
            discovery,
            epoch,
            process_instance_id,
            deployment: deployment.map(|value| ProxyDeploymentMetadata {
                node_id: value.node_id.clone(),
                revision: value.revision,
                bundle_digest: value.bundle_digest.clone(),
                operation_id: value.operation_id.clone(),
                revision_digest: value.revision_digest.clone(),
            }),
            source,
        })
    }

    pub async fn register(&mut self) -> Result<(), tonic::Status> {
        let request = RegisterProxyRequest {
            process_instance_id: self.process_instance_id.clone(),
            deployment: self.deployment.clone(),
        };
        match self.epoch.register_proxy_exact(request.clone()).await {
            Ok(_) => Ok(()),
            Err(error) if super::routing::is_transport_failure(&error) => {
                self.epoch = self.discovery.repin(&self.epoch).await?;
                self.epoch.register_proxy_exact(request).await.map(|_| ())
            }
            Err(error) => Err(error),
        }
    }

    pub async fn report(&mut self) -> Result<(), tonic::Status> {
        let request = ReportProxyInventoryRequest {
            process_instance_id: self.process_instance_id.clone(),
            report: Some(self.source.snapshot().await),
        };
        match self
            .epoch
            .report_proxy_inventory_exact(request.clone())
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if super::routing::is_transport_failure(&error) => {
                self.epoch = self.discovery.repin(&self.epoch).await?;
                self.epoch
                    .report_proxy_inventory_exact(request)
                    .await
                    .map(|_| ())
            }
            Err(error) => Err(error),
        }
    }

    pub async fn deregister(&self) -> Result<(), tonic::Status> {
        let mut epoch = self.discovery.pin(RetryClass::NoReplayMutation).await?;
        epoch
            .deregister_proxy_once(DeregisterProxyRequest {
                process_instance_id: self.process_instance_id.clone(),
            })
            .await
            .map(|_| ())
    }
}

pub async fn run_periodic(
    reporter: Arc<Mutex<Reporter>>,
    interval: Duration,
    mut cancellation: TaskCancellation,
) -> anyhow::Result<TaskExit> {
    let start = Instant::now() + interval;
    let mut ticks = tokio::time::interval_at(start, interval);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(TaskExit::Cancelled),
            _ = ticks.tick() => {}
        }
        if let Err(error) = reporter.lock().await.report().await {
            warn!(%error, "proxy inventory report failed; forwarding remains available");
        }
    }
}

pub async fn register(reporter: &Arc<Mutex<Reporter>>) -> anyhow::Result<()> {
    reporter
        .lock()
        .await
        .register()
        .await
        .context("proxy inventory registration failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CircuitBreakerConfig;
    use wr_common::process_lifecycle::{LifecycleOwner, ServiceKind};

    #[tokio::test]
    async fn snapshot_reads_owners_and_reports_disabled_external_and_known_empty_breakers() {
        let lifecycle = LifecycleOwner::new(ServiceKind::Proxy, "process");
        let admission = AdmissionGate::closed();
        let routing = CachedRoutingTable::new(CircuitBreakerConfig::default(), "https://self");
        let source = ReportSource::new(
            lifecycle.snapshot(),
            admission.clone(),
            ListenerFacts {
                data_plane: true,
                node_control: true,
                peer: true,
                external: false,
            },
            routing,
        );

        let starting = source.snapshot().await;
        assert_eq!(
            starting.lifecycle_state,
            ProcessLifecycleState::Starting as i32
        );
        assert!(!starting.admission_open);
        assert!(!starting.routing_synchronized);
        assert_eq!(starting.listeners.len(), 4);
        assert_eq!(
            starting.listeners[3].kind,
            ProxyListenerKind::External as i32
        );
        assert!(!starting.listeners[3].configured);
        assert!(!starting.listeners[3].accepting);
        assert_eq!(starting.breakers.len(), 2);
        assert!(starting.breakers.iter().all(|entry| entry.total == 0));

        admission.open();
        assert!(source.snapshot().await.admission_open);
    }
}
