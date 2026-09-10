use std::collections::{BTreeMap, BTreeSet};

use wr_common::wruntime::{
    DeploymentCondition, EngineStatus, GetClusterStatusResponse, ManagerMembershipState,
    ManagerStatus, ModuleIdentity, ModuleStatus, NodeStatus, ProcessLifecycleState,
    ProxyBreakerStatus, ProxyInventoryStatus, ProxyListenerKind, ProxyListenerStatus,
    ProxyRoutingStatus, RouteStatus, ServiceStatus, StatusSeverity,
};

use crate::db::{self, ClusterStatusSnapshot};

fn timestamp(value: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn age_seconds(
    observed_at: chrono::DateTime<chrono::Utc>,
    value: chrono::DateTime<chrono::Utc>,
) -> u64 {
    observed_at
        .signed_duration_since(value)
        .num_seconds()
        .max(0) as u64
}

fn is_fresh(
    observed_at: chrono::DateTime<chrono::Utc>,
    value: chrono::DateTime<chrono::Utc>,
    timeout_secs: f64,
) -> bool {
    observed_at.signed_duration_since(value).num_milliseconds() as f64 <= timeout_secs * 1000.0
}

fn condition(
    code: &str,
    severity: StatusSeverity,
    detail: impl Into<String>,
    affected_identity: impl Into<String>,
    desired: impl Into<String>,
    actual: impl Into<String>,
) -> DeploymentCondition {
    DeploymentCondition {
        code: code.to_string(),
        detail: detail.into(),
        severity: severity as i32,
        affected_identity: affected_identity.into(),
        desired: desired.into(),
        actual: actual.into(),
    }
}

fn reduce_severity(values: impl IntoIterator<Item = StatusSeverity>) -> StatusSeverity {
    values
        .into_iter()
        .filter(|severity| *severity != StatusSeverity::Unknown)
        .max()
        .unwrap_or(StatusSeverity::Unknown)
}

fn condition_severity(conditions: &[DeploymentCondition]) -> StatusSeverity {
    reduce_severity(
        conditions
            .iter()
            .map(|item| StatusSeverity::try_from(item.severity).unwrap_or(StatusSeverity::Unknown)),
    )
}

fn deployment_condition(
    node_id: &str,
    revision: u64,
    (code, detail): (String, String),
) -> DeploymentCondition {
    condition(
        &code,
        StatusSeverity::Unhealthy,
        detail,
        node_id,
        format!("{node_id}/{revision}"),
        "",
    )
}

fn compose_managers(
    snapshot: &ClusterStatusSnapshot,
    manager_liveness_threshold_secs: f64,
) -> Vec<ManagerStatus> {
    snapshot
        .managers
        .iter()
        .map(|manager| {
            let heartbeat_age_millis = snapshot
                .observed_at
                .signed_duration_since(manager.last_heartbeat)
                .num_milliseconds() as f64;
            let fresh = heartbeat_age_millis < manager_liveness_threshold_secs * 1000.0;
            let (membership, severity, conditions) = if fresh {
                (
                    ManagerMembershipState::Live,
                    StatusSeverity::Healthy,
                    Vec::new(),
                )
            } else {
                (
                    ManagerMembershipState::Dead,
                    StatusSeverity::Unhealthy,
                    vec![condition(
                        "STALE_MANAGER_HEARTBEAT",
                        StatusSeverity::Unhealthy,
                        "manager PostgreSQL lease is stale",
                        &manager.manager_id,
                        format!("heartbeat within {manager_liveness_threshold_secs} seconds"),
                        format!(
                            "heartbeat age {} seconds",
                            age_seconds(snapshot.observed_at, manager.last_heartbeat)
                        ),
                    )],
                )
            };

            ManagerStatus {
                manager_id: manager.manager_id.clone(),
                grpc_address: manager.grpc_address.clone(),
                severity: severity as i32,
                membership: membership as i32,
                registered_at: Some(timestamp(manager.registered_at)),
                last_heartbeat: Some(timestamp(manager.last_heartbeat)),
                heartbeat_age_seconds: age_seconds(snapshot.observed_at, manager.last_heartbeat),
                conditions,
            }
        })
        .collect()
}

fn compose_engines(
    snapshot: &ClusterStatusSnapshot,
    current: &BTreeMap<String, wr_common::wruntime::DeploymentRecord>,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Vec<EngineStatus> {
    let mut engines = snapshot
        .engines
        .iter()
        .map(|record| {
            let registration = &record.registration;
            let metadata = registration.deployment.as_ref();
            let authoritative = metadata.is_some_and(|metadata| {
                let inventory_matches = snapshot.deployments.iter().any(|candidate| {
                    let deployment = &candidate.record;
                    deployment.node_id == metadata.node_id
                        && deployment.revision == metadata.revision
                        && deployment.bundle_digest == metadata.bundle_digest
                        && deployment
                            .inventory
                            .as_ref()
                            .into_iter()
                            .flat_map(|inventory| inventory.engines.iter())
                            .any(|expected| expected.engine_slot == metadata.engine_slot)
                });
                let selected = snapshot.slot_authorities.iter().find(|authority| {
                    authority.node_id == metadata.node_id
                        && authority.engine_slot == metadata.engine_slot
                });
                inventory_matches
                    && selected.map_or_else(
                        || {
                            current
                                .get(&metadata.node_id)
                                .is_some_and(|deployment| deployment.revision == metadata.revision)
                        },
                        |authority| authority.revision == metadata.revision,
                    )
            });
            let engine_fresh = is_fresh(
                snapshot.observed_at,
                record.last_heartbeat,
                engine_timeout_secs,
            );
            let observation_failure_severity = if authoritative {
                StatusSeverity::Unhealthy
            } else {
                StatusSeverity::Degraded
            };
            let mut conditions = Vec::new();
            if !authoritative {
                conditions.push(condition(
                    "UNMANAGED_ENGINE",
                    StatusSeverity::Degraded,
                    "registration does not match a slot in the current desired revision",
                    &registration.engine_id,
                    metadata
                        .and_then(|item| current.get(&item.node_id))
                        .map(|item| format!("{}/{}", item.node_id, item.revision))
                        .unwrap_or_default(),
                    metadata
                        .map(|item| format!("{}/{}", item.node_id, item.revision))
                        .unwrap_or_else(|| "unmanaged".to_string()),
                ));
            }
            if !engine_fresh {
                conditions.push(condition(
                    "STALE_ENGINE_HEARTBEAT",
                    observation_failure_severity,
                    format!(
                        "engine heartbeat is {} seconds old",
                        age_seconds(snapshot.observed_at, record.last_heartbeat)
                    ),
                    &registration.engine_id,
                    format!("fresh within {engine_timeout_secs}s"),
                    format!(
                        "{}s old",
                        age_seconds(snapshot.observed_at, record.last_heartbeat)
                    ),
                ));
            }

            let mut modules = registration
                .modules
                .iter()
                .map(|module| {
                    let heartbeat = snapshot.module_heartbeats.iter().find(|heartbeat| {
                        heartbeat.engine_id == registration.engine_id
                            && heartbeat.namespace == module.namespace
                            && heartbeat.module_name == module.name
                            && heartbeat.version == module.version
                    });
                    let mut module_conditions = Vec::new();
                    let module_severity = match heartbeat {
                        None => {
                            module_conditions.push(condition(
                                "MISSING_MODULE_HEARTBEAT",
                                observation_failure_severity,
                                "module has no healthy heartbeat observation",
                                format!(
                                    "{}/{}.{}@{}",
                                    registration.engine_id,
                                    module.namespace,
                                    module.name,
                                    module.version
                                ),
                                "heartbeat",
                                "missing",
                            ));
                            observation_failure_severity
                        }
                        Some(heartbeat)
                            if !is_fresh(
                                snapshot.observed_at,
                                heartbeat.last_healthy,
                                module_timeout_secs,
                            ) =>
                        {
                            module_conditions.push(condition(
                                "STALE_MODULE_HEARTBEAT",
                                observation_failure_severity,
                                format!(
                                    "module heartbeat is {} seconds old",
                                    age_seconds(snapshot.observed_at, heartbeat.last_healthy)
                                ),
                                format!(
                                    "{}/{}.{}@{}",
                                    registration.engine_id,
                                    module.namespace,
                                    module.name,
                                    module.version
                                ),
                                format!("fresh within {module_timeout_secs}s"),
                                format!(
                                    "{}s old",
                                    age_seconds(snapshot.observed_at, heartbeat.last_healthy)
                                ),
                            ));
                            observation_failure_severity
                        }
                        Some(_) => StatusSeverity::Healthy,
                    };
                    ModuleStatus {
                        module: Some(ModuleIdentity {
                            namespace: module.namespace.clone(),
                            name: module.name.clone(),
                            version: module.version.clone(),
                        }),
                        severity: module_severity as i32,
                        last_healthy: heartbeat.map(|item| timestamp(item.last_healthy)),
                        heartbeat_age_seconds: heartbeat
                            .map(|item| age_seconds(snapshot.observed_at, item.last_healthy))
                            .unwrap_or_default(),
                        conditions: module_conditions,
                    }
                })
                .collect::<Vec<_>>();
            modules.sort_by(|left, right| {
                let left = left.module.as_ref().expect("module status identity");
                let right = right.module.as_ref().expect("module status identity");
                (&left.namespace, &left.name, &left.version).cmp(&(
                    &right.namespace,
                    &right.name,
                    &right.version,
                ))
            });
            let cleanup = metadata.and_then(|metadata| {
                snapshot
                    .cleanup_summaries
                    .iter()
                    .find(|item| item.node_id == metadata.node_id)
                    .cloned()
            });
            if let Some(cleanup) = &cleanup {
                let paused = cleanup.state == wr_common::wruntime::NodeCleanupState::Paused as i32;
                let overdue =
                    cleanup.last_reconciled_at.as_ref().is_none_or(|value| {
                        chrono::DateTime::from_timestamp(value.seconds, value.nanos as u32)
                            .is_none_or(|time| {
                                snapshot
                                    .observed_at
                                    .signed_duration_since(time)
                                    .num_seconds()
                                    > 30
                            })
                    }) && cleanup.state != wr_common::wruntime::NodeCleanupState::Clean as i32;
                if paused || overdue {
                    conditions.push(condition(
                        if paused {
                            "release_cleanup_paused"
                        } else {
                            "release_cleanup_overdue"
                        },
                        StatusSeverity::Degraded,
                        if cleanup.diagnostic_detail.is_empty() {
                            "release cleanup maintenance requires attention"
                        } else {
                            &cleanup.diagnostic_detail
                        },
                        metadata
                            .map(|item| item.node_id.as_str())
                            .unwrap_or_default(),
                        "clean",
                        format!("generation {}", cleanup.generation),
                    ));
                }
            }
            let base_severity = if conditions.is_empty() {
                StatusSeverity::Healthy
            } else {
                condition_severity(&conditions)
            };
            let severity = reduce_severity(std::iter::once(base_severity).chain(
                modules.iter().map(|module| {
                    StatusSeverity::try_from(module.severity).unwrap_or(StatusSeverity::Unknown)
                }),
            ));
            EngineStatus {
                engine_id: registration.engine_id.clone(),
                address: registration.address.clone(),
                deployment: metadata.cloned(),
                severity: severity as i32,
                authoritative_for_desired_revision: authoritative,
                registered_at: Some(timestamp(record.registered_at)),
                last_heartbeat: Some(timestamp(record.last_heartbeat)),
                heartbeat_age_seconds: age_seconds(snapshot.observed_at, record.last_heartbeat),
                modules,
                conditions,
                release_cleanup: cleanup,
            }
        })
        .collect::<Vec<_>>();
    engines.sort_by(|left, right| left.engine_id.cmp(&right.engine_id));
    engines
}

fn prost_timestamp(value: &prost_types::Timestamp) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(value.seconds, value.nanos.try_into().ok()?)
}

fn exact_proxy_deployment(
    actual: Option<&wr_common::wruntime::ProxyDeploymentMetadata>,
    expected: Option<&wr_common::wruntime::ProxyDeploymentMetadata>,
) -> bool {
    matches!((actual, expected), (Some(actual), Some(expected))
        if actual.node_id == expected.node_id
            && actual.revision == expected.revision
            && actual.bundle_digest == expected.bundle_digest
            && actual.operation_id == expected.operation_id
            && actual.revision_digest == expected.revision_digest)
}

type ProxyComposition = (
    Vec<ProxyInventoryStatus>,
    BTreeMap<String, Vec<DeploymentCondition>>,
);

fn compose_proxies(
    snapshot: &ClusterStatusSnapshot,
    proxy_heartbeat_timeout_secs: f64,
    proxy_routing_freshness_secs: f64,
) -> Result<ProxyComposition, tonic::Status> {
    let mut projections = BTreeMap::new();
    let node_ids = snapshot
        .deployments
        .iter()
        .map(|item| item.record.node_id.clone())
        .collect::<BTreeSet<_>>();
    for node_id in node_ids {
        let deployments = snapshot
            .deployments
            .iter()
            .filter(|item| item.record.node_id == node_id)
            .map(|item| item.record.clone())
            .collect::<Vec<_>>();
        let committed = snapshot.deployments.iter().find(|item| {
            item.record.node_id == node_id && item.record.revision == item.current_revision
        });
        let operations = snapshot
            .active_operations
            .iter()
            .filter(|operation| operation.node_id == node_id)
            .collect::<Vec<_>>();
        if operations.len() > 1 {
            return Err(tonic::Status::internal(format!(
                "multiple active operations found for node '{node_id}'"
            )));
        }
        projections.insert(
            node_id,
            crate::operations::expected_proxy_projection(
                &deployments,
                committed.map(|item| &item.record),
                operations.first().copied(),
            )?,
        );
    }

    let mut candidates: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut records = snapshot
        .proxies
        .iter()
        .map(|row| row.status.clone())
        .collect::<Vec<_>>();
    records.sort_by(|left, right| {
        (&left.node_id, &left.proxy_id, &left.process_instance_id).cmp(&(
            &right.node_id,
            &right.proxy_id,
            &right.process_instance_id,
        ))
    });
    for (index, proxy) in records.iter_mut().enumerate() {
        let projection = projections.get(&proxy.node_id);
        let report_time = proxy.report_received_at.as_ref().and_then(prost_timestamp);
        let fresh = report_time.is_some_and(|time| {
            snapshot
                .observed_at
                .signed_duration_since(time)
                .num_milliseconds() as f64
                <= proxy_heartbeat_timeout_secs * 1000.0
        });
        proxy.report_age_seconds = report_time
            .map(|time| age_seconds(snapshot.observed_at, time))
            .unwrap_or_default();
        proxy.expected = projection.is_some_and(|projection| {
            !projection.ambiguous
                && exact_proxy_deployment(proxy.deployment.as_ref(), projection.deployment.as_ref())
                && (projection.pinned_process_instance_id.is_empty()
                    || projection.pinned_process_instance_id == proxy.process_instance_id)
        });
        if proxy.expected && fresh && proxy.report.is_some() {
            candidates
                .entry(proxy.node_id.clone())
                .or_default()
                .push(index);
        }
    }
    for indexes in candidates.values() {
        if indexes.len() == 1 {
            records[indexes[0]].selected = true;
        }
    }

    let manager_ids = snapshot
        .managers
        .iter()
        .map(|manager| manager.manager_id.as_str())
        .collect::<BTreeSet<_>>();
    for proxy in &mut records {
        let projection = projections.get(&proxy.node_id);
        let duplicate = candidates
            .get(&proxy.node_id)
            .is_some_and(|indexes| indexes.len() > 1);
        let unique_replacement = candidates
            .get(&proxy.node_id)
            .is_some_and(|indexes| indexes.len() == 1);
        let report_time = proxy.report_received_at.as_ref().and_then(prost_timestamp);
        let fresh = report_time.is_some_and(|time| {
            snapshot
                .observed_at
                .signed_duration_since(time)
                .num_milliseconds() as f64
                <= proxy_heartbeat_timeout_secs * 1000.0
        });
        proxy.superseded = unique_replacement
            && projection.is_some_and(|projection| {
                exact_proxy_deployment(proxy.deployment.as_ref(), projection.deployment.as_ref())
            })
            && !fresh
            && !proxy.selected;
        let identity = format!("{}/{}", proxy.proxy_id, proxy.process_instance_id);
        let mut conditions = Vec::new();
        if projection.is_none() || proxy.deployment.is_none() {
            conditions.push(condition(
                "UNMANAGED_PROXY",
                StatusSeverity::Degraded,
                "proxy is not associated with managed deployment intent",
                &identity,
                "managed expected proxy identity",
                "unmanaged",
            ));
        } else if projection.is_some_and(|item| item.ambiguous) {
            conditions.push(condition(
                "AMBIGUOUS_PROXY_OPERATION",
                StatusSeverity::Degraded,
                "paused operation evidence cannot safely select a proxy",
                &identity,
                "unambiguous operation projection",
                "ambiguous",
            ));
        } else if !proxy.expected {
            conditions.push(condition(
                "PROXY_RELEASE_MISMATCH",
                StatusSeverity::Degraded,
                "proxy deployment or pinned process identity does not match expected operation state",
                &identity,
                projection
                    .and_then(|item| item.deployment.as_ref())
                    .map(|item| format!("{}/{}", item.node_id, item.revision))
                    .unwrap_or_default(),
                proxy
                    .deployment
                    .as_ref()
                    .map(|item| format!("{}/{}", item.node_id, item.revision))
                    .unwrap_or_else(|| "unmanaged".into()),
            ));
        }
        if proxy.report.is_none() {
            conditions.push(condition(
                "MISSING_PROXY_REPORT",
                StatusSeverity::Degraded,
                "registered proxy has not supplied a complete inventory report",
                &identity,
                "complete report",
                "missing",
            ));
        } else if !fresh {
            conditions.push(condition(
                "STALE_PROXY_REPORT",
                StatusSeverity::Degraded,
                format!("proxy report is {} seconds old", proxy.report_age_seconds),
                &identity,
                format!("age <= {proxy_heartbeat_timeout_secs}s"),
                format!("{}s", proxy.report_age_seconds),
            ));
        }
        if duplicate && proxy.expected {
            conditions.push(condition(
                "MULTIPLE_FRESH_PROXY_INSTANCES",
                StatusSeverity::Degraded,
                "multiple fresh reports match the expected proxy identity",
                &proxy.node_id,
                "one fresh exact process",
                candidates[&proxy.node_id].len().to_string(),
            ));
        }
        if proxy.superseded {
            conditions.push(condition(
                "SUPERSEDED_PROXY_INSTANCE",
                StatusSeverity::Degraded,
                "stale predecessor retained for replacement diagnostics",
                &identity,
                "selected replacement",
                "superseded predecessor",
            ));
        }
        if !proxy.receiving_manager_id.is_empty()
            && !manager_ids.contains(proxy.receiving_manager_id.as_str())
        {
            conditions.push(condition(
                "UNKNOWN_PROXY_RECEIVER_MANAGER",
                StatusSeverity::Degraded,
                "the manager that received this report is no longer known",
                &identity,
                "known receiving manager",
                &proxy.receiving_manager_id,
            ));
        }

        if let Some(report) = proxy.report.as_ref() {
            let lifecycle = ProcessLifecycleState::try_from(report.lifecycle_state)
                .unwrap_or(ProcessLifecycleState::Unspecified);
            if lifecycle == ProcessLifecycleState::Stopping {
                conditions.push(condition(
                    "PROXY_STOPPING",
                    StatusSeverity::Degraded,
                    "proxy reports STOPPING lifecycle state",
                    &identity,
                    "serving lifecycle",
                    "STOPPING",
                ));
            }
            proxy.listeners = report
                .listeners
                .iter()
                .map(|listener| {
                    let kind = ProxyListenerKind::try_from(listener.kind)
                        .unwrap_or(ProxyListenerKind::Unspecified);
                    let required_data_plane = listener.configured
                        && matches!(
                            kind,
                            ProxyListenerKind::DataPlane
                                | ProxyListenerKind::Peer
                                | ProxyListenerKind::External
                        );
                    let mut nested = Vec::new();
                    if proxy.selected
                        && fresh
                        && lifecycle == ProcessLifecycleState::Ready
                        && required_data_plane
                        && !listener.accepting
                    {
                        nested.push(condition(
                            "PROXY_LISTENER_NOT_ACCEPTING",
                            StatusSeverity::Unhealthy,
                            "required data-plane listener is not accepting",
                            &identity,
                            "accepting",
                            "not accepting",
                        ));
                    }
                    ProxyListenerStatus {
                        kind: listener.kind,
                        configured: listener.configured,
                        accepting: listener.accepting,
                        severity: if nested.is_empty() {
                            StatusSeverity::Healthy as i32
                        } else {
                            StatusSeverity::Unhealthy as i32
                        },
                        conditions: nested,
                    }
                })
                .collect();
            if proxy.selected
                && fresh
                && lifecycle == ProcessLifecycleState::Ready
                && !report.admission_open
            {
                conditions.push(condition(
                    "PROXY_ADMISSION_CLOSED",
                    StatusSeverity::Unhealthy,
                    "fresh READY proxy positively reports closed admission",
                    &identity,
                    "admission open",
                    "closed",
                ));
            }
            let report_age_millis = report_time
                .map(|time| {
                    snapshot
                        .observed_at
                        .signed_duration_since(time)
                        .num_milliseconds()
                        .max(0) as u64
                })
                .unwrap_or_default();
            let routing_age_millis = report
                .routing_observation_age_millis
                .saturating_add(report_age_millis);
            let mut routing_conditions = Vec::new();
            if !report.routing_synchronized {
                routing_conditions.push(condition(
                    "PROXY_ROUTING_NOT_SYNCHRONIZED",
                    StatusSeverity::Degraded,
                    "proxy reports routing synchronization incomplete",
                    &identity,
                    "synchronized",
                    "not synchronized",
                ));
            }
            if report.installed_routing_table_version < snapshot.routing_version {
                routing_conditions.push(condition(
                    "PROXY_ROUTING_BEHIND",
                    StatusSeverity::Degraded,
                    "proxy routing table is behind the manager snapshot",
                    &identity,
                    snapshot.routing_version.to_string(),
                    report.installed_routing_table_version.to_string(),
                ));
            }
            if routing_age_millis as f64 > proxy_routing_freshness_secs * 1000.0 {
                routing_conditions.push(condition(
                    "STALE_PROXY_ROUTING",
                    StatusSeverity::Degraded,
                    "proxy routing observation is stale",
                    &identity,
                    format!("age <= {proxy_routing_freshness_secs}s"),
                    format!("{}ms", routing_age_millis),
                ));
            }
            if report.routing_manager_id.is_empty()
                || !manager_ids.contains(report.routing_manager_id.as_str())
            {
                routing_conditions.push(condition(
                    "UNKNOWN_PROXY_ROUTING_MANAGER",
                    StatusSeverity::Degraded,
                    "routing observation has no known source manager",
                    &identity,
                    "known source manager",
                    if report.routing_manager_id.is_empty() {
                        "missing"
                    } else {
                        &report.routing_manager_id
                    },
                ));
            }
            proxy.routing = Some(ProxyRoutingStatus {
                installed_table_version: report.installed_routing_table_version,
                synchronization_age_seconds: routing_age_millis / 1000,
                source_manager_id: report.routing_manager_id.clone(),
                synchronized: report.routing_synchronized,
                severity: if routing_conditions.is_empty() {
                    StatusSeverity::Healthy as i32
                } else {
                    StatusSeverity::Degraded as i32
                },
                conditions: routing_conditions,
            });
            proxy.breakers = report
                .breakers
                .iter()
                .map(|breaker| {
                    let mut nested = Vec::new();
                    if breaker.total > 0 && breaker.open == breaker.total {
                        nested.push(condition(
                            "ALL_PROXY_TARGETS_OPEN",
                            StatusSeverity::Unhealthy,
                            "every observed target in this proxy breaker class is open",
                            &identity,
                            "at least one forwarding target not open",
                            format!("{} open", breaker.open),
                        ));
                    } else if breaker.open > 0 || breaker.half_open > 0 {
                        nested.push(condition(
                            if breaker.open > 0 {
                                "PARTIALLY_OPEN_PROXY_BREAKERS"
                            } else {
                                "HALF_OPEN_PROXY_BREAKERS"
                            },
                            StatusSeverity::Degraded,
                            "proxy breaker inventory is not fully closed",
                            &identity,
                            "all closed",
                            format!("{} open, {} half-open", breaker.open, breaker.half_open),
                        ));
                    }
                    ProxyBreakerStatus {
                        destination_kind: breaker.destination_kind,
                        total: breaker.total,
                        closed: breaker.closed,
                        open: breaker.open,
                        half_open: breaker.half_open,
                        severity: if nested.is_empty() {
                            StatusSeverity::Healthy as i32
                        } else {
                            condition_severity(&nested) as i32
                        },
                        conditions: nested,
                    }
                })
                .collect();
            conditions.extend(
                proxy
                    .listeners
                    .iter()
                    .flat_map(|item| item.conditions.iter().cloned()),
            );
            if let Some(routing) = &proxy.routing {
                conditions.extend(routing.conditions.iter().cloned());
            }
            conditions.extend(
                proxy
                    .breakers
                    .iter()
                    .flat_map(|item| item.conditions.iter().cloned()),
            );
        }
        conditions.sort_by(|left, right| {
            (&left.code, &left.affected_identity).cmp(&(&right.code, &right.affected_identity))
        });
        proxy.severity = if conditions.is_empty() {
            StatusSeverity::Healthy as i32
        } else {
            condition_severity(&conditions) as i32
        };
        proxy.conditions = conditions;
    }

    let mut node_conditions = BTreeMap::new();
    for (node_id, projection) in &projections {
        let selected = records
            .iter()
            .find(|proxy| proxy.node_id == *node_id && proxy.selected);
        let mut conditions = Vec::new();
        if projection.ambiguous {
            conditions.push(condition(
                "AMBIGUOUS_PROXY_OPERATION",
                StatusSeverity::Degraded,
                "paused operation evidence cannot safely select the expected proxy",
                node_id,
                "unambiguous operation projection",
                "ambiguous",
            ));
        } else if projection.deployment.is_some() && selected.is_none() {
            let count = candidates.get(node_id).map_or(0, Vec::len);
            conditions.push(condition(
                if count > 1 {
                    "MULTIPLE_FRESH_PROXY_INSTANCES"
                } else {
                    "MISSING_EXPECTED_PROXY"
                },
                StatusSeverity::Degraded,
                if count > 1 {
                    "multiple fresh exact proxy reports prevent selection"
                } else {
                    "no fresh exact proxy report matches expected deployment identity"
                },
                node_id,
                "one fresh exact proxy",
                count.to_string(),
            ));
        }
        if let Some(selected) = selected {
            conditions.extend(selected.conditions.iter().cloned());
        }
        conditions.sort_by(|left, right| {
            (&left.code, &left.affected_identity).cmp(&(&right.code, &right.affected_identity))
        });
        node_conditions.insert(node_id.clone(), conditions);
    }
    Ok((records, node_conditions))
}

fn compose_nodes(
    snapshot: &ClusterStatusSnapshot,
    engines: &[EngineStatus],
    proxies: &[ProxyInventoryStatus],
    proxy_conditions: &BTreeMap<String, Vec<DeploymentCondition>>,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Result<Vec<NodeStatus>, tonic::Status> {
    let mut history: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for deployment in &snapshot.deployments {
        history
            .entry(deployment.record.node_id.clone())
            .or_default()
            .push(deployment.record.clone());
    }

    history
        .into_iter()
        .map(|(node_id, mut deployments)| {
            deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.revision));
            let authority = snapshot
                .deployments
                .iter()
                .find(|item| item.record.node_id == node_id)
                .expect("node history came from snapshot");
            let current_revision = authority.current_revision;
            let target_revision = authority.target_revision;
            let desired = deployments
                .iter()
                .find(|deployment| deployment.revision == current_revision)
                .cloned();
            let target = target_revision.and_then(|revision| {
                deployments
                    .iter()
                    .find(|deployment| deployment.revision == revision)
                    .cloned()
            });
            let condition_deployment = desired.as_ref().or(target.as_ref());
            let mut conditions = if let Some(deployment) = condition_deployment {
                db::deployment_conditions_from_snapshot(
                    snapshot,
                    deployment,
                    engine_timeout_secs,
                    module_timeout_secs,
                )?
                .into_iter()
                .map(|item| deployment_condition(&node_id, deployment.revision, item))
                .collect::<Vec<_>>()
            } else {
                vec![deployment_condition(
                    &node_id,
                    current_revision,
                    (
                        "NO_COMMITTED_DEPLOYMENT".into(),
                        "node has no committed or staged deployment".into(),
                    ),
                )]
            };
            let cleanup = snapshot
                .cleanup_summaries
                .iter()
                .find(|item| item.node_id == node_id)
                .cloned();
            if let Some(cleanup) = &cleanup {
                let paused = cleanup.state == wr_common::wruntime::NodeCleanupState::Paused as i32;
                let overdue =
                    cleanup.last_reconciled_at.as_ref().is_none_or(|value| {
                        chrono::DateTime::from_timestamp(value.seconds, value.nanos as u32)
                            .is_none_or(|time| {
                                snapshot
                                    .observed_at
                                    .signed_duration_since(time)
                                    .num_seconds()
                                    > 30
                            })
                    }) && cleanup.state != wr_common::wruntime::NodeCleanupState::Clean as i32;
                if paused || overdue {
                    conditions.push(condition(
                        if paused {
                            "release_cleanup_paused"
                        } else {
                            "release_cleanup_overdue"
                        },
                        StatusSeverity::Degraded,
                        if cleanup.diagnostic_detail.is_empty() {
                            "release cleanup maintenance requires attention"
                        } else {
                            &cleanup.diagnostic_detail
                        },
                        &node_id,
                        "clean",
                        format!("generation {}", cleanup.generation),
                    ));
                }
            }
            conditions.extend(
                proxy_conditions
                    .get(&node_id)
                    .into_iter()
                    .flat_map(|items| items.iter().cloned()),
            );
            conditions.sort_by(|left, right| {
                (&left.code, &left.affected_identity).cmp(&(&right.code, &right.affected_identity))
            });
            let node_engines = engines
                .iter()
                .filter(|engine| {
                    engine
                        .deployment
                        .as_ref()
                        .is_some_and(|metadata| metadata.node_id == node_id)
                })
                .cloned()
                .collect();
            let severity = if conditions.is_empty() {
                StatusSeverity::Healthy
            } else {
                condition_severity(&conditions)
            };
            let node_proxies = proxies
                .iter()
                .filter(|proxy| proxy.node_id == node_id)
                .cloned()
                .collect();
            Ok(NodeStatus {
                node_id,
                severity: severity as i32,
                desired_deployment: desired,
                deployment_history: deployments,
                engines: node_engines,
                conditions,
                target_deployment: target,
                release_cleanup: cleanup,
                proxies: node_proxies,
            })
        })
        .collect()
}

fn compose_services(
    snapshot: &ClusterStatusSnapshot,
    current: &BTreeMap<String, wr_common::wruntime::DeploymentRecord>,
    engines: &[EngineStatus],
) -> Vec<ServiceStatus> {
    type ServiceKey = (String, String, String);
    let authoritative_engines: BTreeSet<_> = engines
        .iter()
        .filter(|engine| engine.authoritative_for_desired_revision)
        .map(|engine| engine.engine_id.as_str())
        .collect();
    let mut desired: BTreeMap<ServiceKey, u32> = BTreeMap::new();
    for deployment in current.values() {
        for engine in deployment
            .inventory
            .as_ref()
            .into_iter()
            .flat_map(|inventory| inventory.engines.iter())
        {
            for module in &engine.modules {
                if let Some(module) = module.identity.as_ref() {
                    *desired
                        .entry((
                            module.namespace.clone(),
                            module.name.clone(),
                            module.version.clone(),
                        ))
                        .or_default() += 1;
                }
            }
        }
    }
    let mut keys: BTreeSet<ServiceKey> = desired.keys().cloned().collect();
    keys.extend(snapshot.routes.iter().map(|route| {
        (
            route.rule.destination_namespace.clone(),
            route.rule.destination_module.clone(),
            route.rule.destination_version.clone(),
        )
    }));

    keys.into_iter()
        .map(|(namespace, name, version)| {
            let desired_routes = desired
                .get(&(namespace.clone(), name.clone(), version.clone()))
                .copied()
                .unwrap_or_default();
            let mut routes = snapshot
                .routes
                .iter()
                .filter(|route| {
                    route.rule.destination_namespace == namespace
                        && route.rule.destination_module == name
                        && route.rule.destination_version == version
                })
                .map(|route| {
                    let default_rule_id = format!(
                        "{}/{}/{}/{}",
                        route.rule.engine_id,
                        route.rule.destination_namespace,
                        route.rule.destination_module,
                        route.rule.destination_version
                    );
                    let is_default = route.rule.rule_id == default_rule_id;
                    let is_desired = is_default
                        && authoritative_engines.contains(route.rule.engine_id.as_str());
                    let severity = if route.rule.healthy {
                        StatusSeverity::Healthy
                    } else {
                        StatusSeverity::Unhealthy
                    };
                    let mut conditions = Vec::new();
                    if !is_default {
                        conditions.push(condition(
                            "MANUAL_ROUTE_REASON_UNAVAILABLE",
                            StatusSeverity::Unknown,
                            "route health is persisted, but manual route heartbeat causality is not reported",
                            &route.rule.rule_id,
                            "heartbeat-backed default route",
                            "manual/non-default route",
                        ));
                    }
                    if !route.rule.healthy {
                        conditions.push(condition(
                            "UNHEALTHY_ROUTE",
                            StatusSeverity::Unhealthy,
                            "persisted route is unhealthy",
                            &route.rule.rule_id,
                            "healthy",
                            "unhealthy",
                        ));
                    }
                    RouteStatus {
                        rule: Some(route.rule.clone()),
                        severity: severity as i32,
                        desired: is_desired,
                        updated_at: Some(timestamp(route.updated_at)),
                        conditions,
                    }
                })
                .collect::<Vec<_>>();
            routes.sort_by(|left, right| {
                left.rule
                    .as_ref()
                    .expect("route status rule")
                    .rule_id
                    .cmp(&right.rule.as_ref().expect("route status rule").rule_id)
            });
            let healthy_routes = routes
                .iter()
                .filter(|route| {
                    route.desired && route.rule.as_ref().is_some_and(|rule| rule.healthy)
                })
                .count() as u32;
            let unhealthy_routes = desired_routes.saturating_sub(healthy_routes);
            let identity = format!("{namespace}.{name}@{version}");
            let (severity, conditions) = if desired_routes == 0 {
                (
                    StatusSeverity::Degraded,
                    vec![condition(
                        "MANUAL_ROUTE_REASON_UNAVAILABLE",
                        StatusSeverity::Degraded,
                        "service has persisted routes but is not part of a desired deployment",
                        &identity,
                        "desired deployment ownership",
                        "not reported/manual rule",
                    )],
                )
            } else if healthy_routes == 0 {
                (
                    StatusSeverity::Unhealthy,
                    vec![condition(
                        "NO_HEALTHY_ROUTE",
                        StatusSeverity::Unhealthy,
                        "no desired route is healthy",
                        &identity,
                        format!("{desired_routes} healthy desired routes"),
                        "0 healthy desired routes",
                    )],
                )
            } else if healthy_routes < desired_routes {
                (
                    StatusSeverity::Degraded,
                    vec![condition(
                        "PARTIAL_ROUTE_AVAILABILITY",
                        StatusSeverity::Degraded,
                        "at least one desired route is healthy but desired availability is partial",
                        &identity,
                        format!("{desired_routes} healthy desired routes"),
                        format!("{healthy_routes} healthy desired routes"),
                    )],
                )
            } else {
                (StatusSeverity::Healthy, Vec::new())
            };
            ServiceStatus {
                service: Some(ModuleIdentity {
                    namespace,
                    name,
                    version,
                }),
                severity: severity as i32,
                desired_routes,
                healthy_routes,
                unhealthy_routes,
                routes,
                conditions,
            }
        })
        .collect()
}

pub fn compose(
    snapshot: ClusterStatusSnapshot,
    manager_liveness_threshold_secs: f64,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
    proxy_heartbeat_timeout_secs: f64,
    proxy_routing_freshness_secs: f64,
) -> Result<GetClusterStatusResponse, tonic::Status> {
    let current = snapshot
        .deployments
        .iter()
        .filter(|item| item.record.revision == item.current_revision)
        .map(|item| (item.record.node_id.clone(), item.record.clone()))
        .collect::<BTreeMap<_, _>>();
    let managers = compose_managers(&snapshot, manager_liveness_threshold_secs);
    let engines = compose_engines(
        &snapshot,
        &current,
        engine_timeout_secs,
        module_timeout_secs,
    );
    let (proxies, proxy_conditions) = compose_proxies(
        &snapshot,
        proxy_heartbeat_timeout_secs,
        proxy_routing_freshness_secs,
    )?;
    let nodes = compose_nodes(
        &snapshot,
        &engines,
        &proxies,
        &proxy_conditions,
        engine_timeout_secs,
        module_timeout_secs,
    )?;
    let services = compose_services(&snapshot, &current, &engines);
    let mut conditions = vec![condition(
        "SIGNAL_NOT_REPORTED",
        StatusSeverity::Unknown,
        "host CPU and memory utilization is not reported by managers",
        "host-resources",
        "reported signal",
        "not reported",
    )];
    for proxy in proxies.iter().filter(|proxy| {
        !proxy.superseded
            && !proxy.selected
            && proxy.report.is_some()
            && proxy
                .report_received_at
                .as_ref()
                .and_then(prost_timestamp)
                .is_some_and(|time| {
                    snapshot
                        .observed_at
                        .signed_duration_since(time)
                        .num_milliseconds() as f64
                        <= proxy_heartbeat_timeout_secs * 1000.0
                })
            && (!proxy.expected || !proxy_conditions.contains_key(&proxy.node_id))
    }) {
        conditions.push(condition(
            "ORPHAN_PROXY_INVENTORY",
            StatusSeverity::Degraded,
            "fresh proxy inventory is not selected by managed deployment intent",
            format!("{}/{}", proxy.proxy_id, proxy.process_instance_id),
            "selected managed proxy",
            "orphan or unmanaged",
        ));
    }
    conditions.sort_by(|left, right| {
        (&left.code, &left.affected_identity).cmp(&(&right.code, &right.affected_identity))
    });
    let severity = reduce_severity(
        managers
            .iter()
            .map(|item| item.severity)
            .chain(nodes.iter().map(|item| item.severity))
            .chain(engines.iter().map(|item| item.severity))
            .chain(services.iter().map(|item| item.severity))
            .chain(std::iter::once(condition_severity(&conditions) as i32))
            .map(|value| StatusSeverity::try_from(value).unwrap_or(StatusSeverity::Unknown)),
    );
    Ok(GetClusterStatusResponse {
        response_at: Some(timestamp(chrono::Utc::now())),
        database_observed_at: Some(timestamp(snapshot.observed_at)),
        routing_table_version: snapshot.routing_version,
        severity: severity as i32,
        managers,
        nodes,
        engines,
        services,
        conditions,
        proxies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxy_snapshot(
        reports: Vec<(&str, i64, wr_common::wruntime::ProxyInventoryReport)>,
    ) -> ClusterStatusSnapshot {
        use wr_common::wruntime::{
            DeploymentRecord, DeploymentState, ProxyDeploymentMetadata, ProxyInventoryStatus,
        };

        let now = chrono::Utc::now();
        let deployment = DeploymentRecord {
            node_id: "node-a".into(),
            revision: 1,
            bundle_digest: "bundle-a".into(),
            operation_id: "operation-a".into(),
            revision_digest: "revision-a".into(),
            state: DeploymentState::Succeeded as i32,
            ..Default::default()
        };
        ClusterStatusSnapshot {
            observed_at: now,
            routing_version: 7,
            deployments: vec![db::StatusDeploymentRecord {
                record: deployment.clone(),
                current_revision: 1,
                target_revision: None,
            }],
            proxies: reports
                .into_iter()
                .map(|(process, age, report)| db::StatusProxyRecord {
                    status: ProxyInventoryStatus {
                        proxy_id: "urn:proxy:a".into(),
                        node_id: "node-a".into(),
                        process_instance_id: process.into(),
                        deployment: Some(ProxyDeploymentMetadata {
                            node_id: deployment.node_id.clone(),
                            revision: deployment.revision,
                            bundle_digest: deployment.bundle_digest.clone(),
                            operation_id: deployment.operation_id.clone(),
                            revision_digest: deployment.revision_digest.clone(),
                        }),
                        report: Some(report),
                        report_received_at: Some(timestamp(now - chrono::Duration::seconds(age))),
                        receiving_manager_id: "manager-a".into(),
                        ..Default::default()
                    },
                })
                .collect(),
            managers: vec![db::StatusManagerRecord {
                manager_id: "manager-a".into(),
                grpc_address: "https://manager-a".into(),
                registered_at: now,
                last_heartbeat: now,
            }],
            engines: vec![],
            module_heartbeats: vec![],
            routes: vec![],
            slot_authorities: vec![],
            active_operations: vec![],
            observations: vec![],
            agent_attestations: vec![],
            agent_policies: vec![],
            cleanup_summaries: vec![],
        }
    }

    fn ready_proxy_report() -> wr_common::wruntime::ProxyInventoryReport {
        use wr_common::wruntime::{
            ProxyBreakerDestinationKind, ProxyBreakerEvidence, ProxyInventoryReport,
            ProxyListenerEvidence,
        };
        ProxyInventoryReport {
            lifecycle_state: ProcessLifecycleState::Ready as i32,
            admission_open: true,
            listeners: vec![ProxyListenerEvidence {
                kind: ProxyListenerKind::DataPlane as i32,
                configured: true,
                accepting: true,
            }],
            installed_routing_table_version: 7,
            routing_observation_age_millis: 0,
            routing_manager_id: "manager-a".into(),
            routing_synchronized: true,
            breakers: vec![ProxyBreakerEvidence {
                destination_kind: ProxyBreakerDestinationKind::LocalEngine as i32,
                total: 0,
                closed: 0,
                open: 0,
                half_open: 0,
            }],
        }
    }

    #[test]
    fn proxy_health_uses_strict_boundaries_and_positive_forwarding_evidence() {
        let snapshot = proxy_snapshot(vec![("process-a", 15, ready_proxy_report())]);
        let (healthy, rollup) = compose_proxies(&snapshot, 15.0, 30.0).unwrap();
        assert!(healthy[0].selected);
        assert_eq!(healthy[0].severity, StatusSeverity::Healthy as i32);
        assert!(rollup["node-a"].is_empty());

        let mut failed = ready_proxy_report();
        failed.admission_open = false;
        failed.listeners[0].accepting = false;
        failed.breakers[0].total = 1;
        failed.breakers[0].open = 1;
        let snapshot = proxy_snapshot(vec![("process-a", 0, failed)]);
        let (unhealthy, _) = compose_proxies(&snapshot, 15.0, 30.0).unwrap();
        assert_eq!(unhealthy[0].severity, StatusSeverity::Unhealthy as i32);
        let codes = unhealthy[0]
            .conditions
            .iter()
            .map(|item| item.code.as_str())
            .collect::<BTreeSet<_>>();
        assert!(codes.contains("PROXY_ADMISSION_CLOSED"));
        assert!(codes.contains("PROXY_LISTENER_NOT_ACCEPTING"));
        assert!(codes.contains("ALL_PROXY_TARGETS_OPEN"));
    }

    #[test]
    fn degraded_proxy_dimensions_remain_distinct_from_confirmed_no_forwarding() {
        let mut report = ready_proxy_report();
        report.lifecycle_state = ProcessLifecycleState::Stopping as i32;
        report.installed_routing_table_version = 6;
        report.routing_observation_age_millis = 31_000;
        report.routing_manager_id = "missing-manager".into();
        report.routing_synchronized = false;
        report.breakers[0].total = 2;
        report.breakers[0].closed = 1;
        report.breakers[0].half_open = 1;
        let snapshot = proxy_snapshot(vec![("process-a", 0, report)]);
        let (records, _) = compose_proxies(&snapshot, 15.0, 30.0).unwrap();
        assert_eq!(records[0].severity, StatusSeverity::Degraded as i32);
        let codes = records[0]
            .conditions
            .iter()
            .map(|item| item.code.as_str())
            .collect::<BTreeSet<_>>();
        for code in [
            "PROXY_STOPPING",
            "PROXY_ROUTING_BEHIND",
            "PROXY_ROUTING_NOT_SYNCHRONIZED",
            "STALE_PROXY_ROUTING",
            "UNKNOWN_PROXY_ROUTING_MANAGER",
            "HALF_OPEN_PROXY_BREAKERS",
        ] {
            assert!(codes.contains(code), "missing {code}");
        }

        let mut partial = ready_proxy_report();
        partial.breakers[0].total = 2;
        partial.breakers[0].closed = 1;
        partial.breakers[0].open = 1;
        partial
            .breakers
            .push(wr_common::wruntime::ProxyBreakerEvidence {
                destination_kind: wr_common::wruntime::ProxyBreakerDestinationKind::PeerProxy
                    as i32,
                total: 1,
                closed: 1,
                open: 0,
                half_open: 0,
            });
        let snapshot = proxy_snapshot(vec![("process-a", 0, partial)]);
        let (records, _) = compose_proxies(&snapshot, 15.0, 30.0).unwrap();
        assert_eq!(records[0].severity, StatusSeverity::Degraded as i32);
        assert!(records[0]
            .conditions
            .iter()
            .any(|item| item.code == "PARTIALLY_OPEN_PROXY_BREAKERS"));
        assert!(!records[0]
            .conditions
            .iter()
            .any(|item| item.code == "ALL_PROXY_TARGETS_OPEN"));

        let stale = proxy_snapshot(vec![("process-a", 16, ready_proxy_report())]);
        let (records, rollup) = compose_proxies(&stale, 15.0, 30.0).unwrap();
        assert!(records[0]
            .conditions
            .iter()
            .any(|item| item.code == "STALE_PROXY_REPORT"));
        assert_eq!(rollup["node-a"][0].code, "MISSING_EXPECTED_PROXY");
    }

    #[test]
    fn breaker_conditions_are_attributed_to_each_destination_class() {
        use wr_common::wruntime::{ProxyBreakerDestinationKind, ProxyBreakerEvidence};

        let mut report = ready_proxy_report();
        report.breakers = vec![
            ProxyBreakerEvidence {
                destination_kind: ProxyBreakerDestinationKind::LocalEngine as i32,
                total: 0,
                closed: 0,
                open: 0,
                half_open: 0,
            },
            ProxyBreakerEvidence {
                destination_kind: ProxyBreakerDestinationKind::PeerProxy as i32,
                total: 2,
                closed: 0,
                open: 2,
                half_open: 0,
            },
        ];
        let snapshot = proxy_snapshot(vec![("process-a", 0, report)]);
        let (records, _) = compose_proxies(&snapshot, 15.0, 30.0).unwrap();
        let proxy = &records[0];

        assert_eq!(proxy.severity, StatusSeverity::Unhealthy as i32);
        assert_eq!(proxy.breakers[0].severity, StatusSeverity::Healthy as i32);
        assert!(proxy.breakers[0].conditions.is_empty());
        assert_eq!(proxy.breakers[1].severity, StatusSeverity::Unhealthy as i32);
        assert_eq!(proxy.breakers[1].conditions.len(), 1);
        assert_eq!(
            proxy.breakers[1].conditions[0].code,
            "ALL_PROXY_TARGETS_OPEN"
        );
    }

    #[test]
    fn duplicate_and_replacement_proxy_selection_is_deterministic() {
        let duplicate = proxy_snapshot(vec![
            ("process-b", 0, ready_proxy_report()),
            ("process-a", 0, ready_proxy_report()),
        ]);
        let (duplicates, rollup) = compose_proxies(&duplicate, 15.0, 30.0).unwrap();
        assert_eq!(duplicates.iter().filter(|item| item.selected).count(), 0);
        assert_eq!(duplicates[0].process_instance_id, "process-a");
        assert_eq!(rollup["node-a"][0].code, "MULTIPLE_FRESH_PROXY_INSTANCES");

        let replacement = proxy_snapshot(vec![
            ("process-old", 16, ready_proxy_report()),
            ("process-new", 0, ready_proxy_report()),
        ]);
        let (records, rollup) = compose_proxies(&replacement, 15.0, 30.0).unwrap();
        assert!(records
            .iter()
            .any(|item| item.process_instance_id == "process-new" && item.selected));
        assert!(records
            .iter()
            .any(|item| item.process_instance_id == "process-old" && item.superseded));
        assert!(rollup["node-a"].is_empty());
    }

    #[test]
    fn unknown_does_not_worsen_supported_status() {
        assert_eq!(
            reduce_severity([
                StatusSeverity::Unknown,
                StatusSeverity::Healthy,
                StatusSeverity::Degraded,
            ]),
            StatusSeverity::Degraded
        );
        assert_eq!(
            reduce_severity([StatusSeverity::Unknown]),
            StatusSeverity::Unknown
        );
    }

    #[test]
    fn conditions_are_stable_machine_readable_records() {
        let record = condition(
            "SIGNAL_NOT_REPORTED",
            StatusSeverity::Unknown,
            "not reported",
            "proxy",
            "reported",
            "missing",
        );
        assert_eq!(record.code, "SIGNAL_NOT_REPORTED");
        assert_eq!(record.severity, StatusSeverity::Unknown as i32);
        assert_eq!(record.affected_identity, "proxy");
    }

    #[test]
    fn release_cleanup_degrades_node_and_engine_without_marking_them_unhealthy() {
        use wr_common::wruntime::{
            DeploymentInventoryV1, DeploymentMetadata, DeploymentRecord, DeploymentState,
            EngineRegistration, ExpectedEngine, NodeCleanupState, NodeCleanupSummary,
        };

        let now = chrono::Utc::now();
        let deployment = DeploymentRecord {
            node_id: "cleanup-status-node".into(),
            revision: 1,
            bundle_digest: "sha256:bundle".into(),
            resolved_release_digest: "sha256:resolved".into(),
            state: DeploymentState::Succeeded as i32,
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![ExpectedEngine {
                    engine_slot: "blue".into(),
                    modules: vec![],
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let snapshot = ClusterStatusSnapshot {
            observed_at: now,
            routing_version: 1,
            deployments: vec![db::StatusDeploymentRecord {
                record: deployment.clone(),
                current_revision: 1,
                target_revision: None,
            }],
            engines: vec![db::StatusEngineRecord {
                registration: EngineRegistration {
                    engine_id: "cleanup-status-engine".into(),
                    deployment: Some(DeploymentMetadata {
                        node_id: deployment.node_id.clone(),
                        revision: deployment.revision,
                        bundle_digest: deployment.bundle_digest.clone(),
                        engine_slot: "blue".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                registered_at: now,
                last_heartbeat: now,
            }],
            proxies: vec![],
            module_heartbeats: vec![],
            routes: vec![],
            managers: vec![],
            slot_authorities: vec![],
            active_operations: vec![],
            observations: vec![],
            agent_attestations: vec![],
            agent_policies: vec![],
            cleanup_summaries: vec![NodeCleanupSummary {
                node_id: deployment.node_id.clone(),
                state: NodeCleanupState::Paused as i32,
                generation: 4,
                diagnostic_detail: "inventory unavailable".into(),
                ..Default::default()
            }],
        };
        let current = BTreeMap::from([(deployment.node_id.clone(), deployment)]);
        let engines = compose_engines(&snapshot, &current, 10.0, 10.0);
        let cleanup_engine = &engines[0];
        assert_eq!(cleanup_engine.severity, StatusSeverity::Degraded as i32);
        assert_eq!(cleanup_engine.conditions[0].code, "release_cleanup_paused");
        let nodes = compose_nodes(&snapshot, &engines, &[], &BTreeMap::new(), 10.0, 10.0).unwrap();
        let cleanup_node = &nodes[0];
        assert_eq!(cleanup_node.severity, StatusSeverity::Degraded as i32);
        assert!(cleanup_node
            .conditions
            .iter()
            .any(|condition| condition.code == "release_cleanup_paused"
                && condition.severity == StatusSeverity::Degraded as i32));
    }

    #[test]
    fn service_availability_is_partial_and_deterministic() {
        use wr_common::wruntime::{
            DeploymentInventoryV1, DeploymentRecord, ExpectedEngine, ExpectedModule, RoutingRule,
        };

        let module = ModuleIdentity {
            namespace: "store".into(),
            name: "inventory".into(),
            version: "1.0.0".into(),
        };
        let deployment = DeploymentRecord {
            node_id: "node-a".into(),
            revision: 1,
            inventory: Some(DeploymentInventoryV1 {
                schema_version: 1,
                engines: vec![
                    ExpectedEngine {
                        engine_slot: "one".into(),
                        modules: vec![ExpectedModule {
                            identity: Some(module.clone()),
                            proto_schema_digest: String::new(),
                        }],
                        ..Default::default()
                    },
                    ExpectedEngine {
                        engine_slot: "two".into(),
                        modules: vec![ExpectedModule {
                            identity: Some(module),
                            proto_schema_digest: String::new(),
                        }],
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        };
        let current = BTreeMap::from([("node-a".to_string(), deployment)]);
        let engines = ["engine-b", "engine-a"]
            .into_iter()
            .map(|engine_id| EngineStatus {
                engine_id: engine_id.into(),
                authoritative_for_desired_revision: true,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let now = chrono::Utc::now();
        let routes = [
            ("engine-b/store/inventory/1.0.0", "engine-b", false),
            ("engine-a/store/inventory/1.0.0", "engine-a", true),
        ]
        .into_iter()
        .map(|(rule_id, engine_id, healthy)| db::StatusRouteRecord {
            rule: RoutingRule {
                rule_id: rule_id.into(),
                destination_namespace: "store".into(),
                destination_module: "inventory".into(),
                destination_version: "1.0.0".into(),
                engine_id: engine_id.into(),
                healthy,
                ..Default::default()
            },
            updated_at: now,
        })
        .collect();
        let snapshot = ClusterStatusSnapshot {
            observed_at: now,
            routing_version: 1,
            deployments: Vec::new(),
            engines: Vec::new(),
            proxies: Vec::new(),
            module_heartbeats: Vec::new(),
            routes,
            managers: Vec::new(),
            slot_authorities: Vec::new(),
            active_operations: Vec::new(),
            observations: Vec::new(),
            agent_attestations: Vec::new(),
            agent_policies: Vec::new(),
            cleanup_summaries: Vec::new(),
        };

        let services = compose_services(&snapshot, &current, &engines);
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].severity, StatusSeverity::Degraded as i32);
        assert_eq!(services[0].desired_routes, 2);
        assert_eq!(services[0].healthy_routes, 1);
        assert_eq!(services[0].conditions[0].code, "PARTIAL_ROUTE_AVAILABILITY");
        assert_eq!(
            services[0]
                .routes
                .iter()
                .map(|route| route
                    .rule
                    .as_ref()
                    .expect("route evidence")
                    .rule_id
                    .as_str())
                .collect::<Vec<_>>(),
            [
                "engine-a/store/inventory/1.0.0",
                "engine-b/store/inventory/1.0.0"
            ]
        );
    }
}
