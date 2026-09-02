use std::collections::{BTreeMap, BTreeSet, HashMap};

use wr_common::wruntime::{
    DeploymentCondition, EngineStatus, GetClusterStatusResponse, ManagerMembershipState,
    ManagerStatus, ModuleIdentity, ModuleStatus, NodeStatus, RouteStatus, ServiceStatus,
    StatusSeverity,
};

use crate::cluster::MembershipSnapshot;
use crate::db::{self, ClusterStatusSnapshot};

const MANAGER_HEARTBEAT_TIMEOUT_SECS: f64 = 60.0;

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
    membership: &MembershipSnapshot,
    within_convergence_window: bool,
) -> Vec<ManagerStatus> {
    let db_records: HashMap<_, _> = snapshot
        .managers
        .iter()
        .map(|manager| (manager.manager_id.as_str(), manager))
        .collect();
    let live: HashMap<_, _> = membership
        .live
        .iter()
        .map(|manager| (manager.manager_id.as_str(), manager))
        .collect();
    let mut ids = BTreeSet::new();
    ids.extend(db_records.keys().copied());
    ids.extend(live.keys().copied());
    ids.extend(membership.dead.iter().map(String::as_str));

    ids.into_iter()
        .map(|manager_id| {
            let db = db_records.get(manager_id).copied();
            let gossip = live.get(manager_id).copied();
            let membership_state = if membership.dead.contains(manager_id) {
                ManagerMembershipState::Dead
            } else if gossip.is_some() {
                ManagerMembershipState::Live
            } else {
                ManagerMembershipState::Unknown
            };
            let mut conditions = Vec::new();
            let severity = match membership_state {
                ManagerMembershipState::Dead => {
                    conditions.push(condition(
                        "GOSSIP_DEAD",
                        StatusSeverity::Unhealthy,
                        "gossip has affirmatively marked this manager dead",
                        manager_id,
                        "live",
                        "dead",
                    ));
                    if db.is_some() {
                        conditions.push(condition(
                            "MANAGER_DB_GOSSIP_DISAGREEMENT",
                            StatusSeverity::Unhealthy,
                            "manager remains registered in PostgreSQL while gossip reports it dead",
                            manager_id,
                            "live in both signals",
                            "database present; gossip dead",
                        ));
                    }
                    StatusSeverity::Unhealthy
                }
                ManagerMembershipState::Live if db.is_none() => {
                    conditions.push(condition(
                        "MANAGER_DB_GOSSIP_DISAGREEMENT",
                        StatusSeverity::Degraded,
                        "manager is live in gossip but has no PostgreSQL membership record",
                        manager_id,
                        "present in both signals",
                        "gossip only",
                    ));
                    StatusSeverity::Degraded
                }
                ManagerMembershipState::Live => {
                    let record = db.expect("live DB membership checked above");
                    if is_fresh(
                        snapshot.observed_at,
                        record.last_heartbeat,
                        MANAGER_HEARTBEAT_TIMEOUT_SECS,
                    ) {
                        StatusSeverity::Healthy
                    } else {
                        conditions.push(condition(
                            "MANAGER_DB_GOSSIP_DISAGREEMENT",
                            StatusSeverity::Degraded,
                            "manager is live in gossip but its database heartbeat is stale",
                            manager_id,
                            "fresh in both signals",
                            "gossip live; database stale",
                        ));
                        StatusSeverity::Degraded
                    }
                }
                ManagerMembershipState::Unknown if within_convergence_window => {
                    conditions.push(condition(
                        "BOOTSTRAP_CONVERGING",
                        StatusSeverity::Degraded,
                        "database membership has not yet appeared in gossip during the convergence window",
                        manager_id,
                        "gossip observation",
                        "database only",
                    ));
                    StatusSeverity::Degraded
                }
                ManagerMembershipState::Unknown => {
                    conditions.push(condition(
                        "MANAGER_DB_GOSSIP_DISAGREEMENT",
                        StatusSeverity::Unhealthy,
                        "database membership is absent from authoritative gossip after convergence",
                        manager_id,
                        "live gossip membership",
                        "database only",
                    ));
                    StatusSeverity::Unhealthy
                }
            };

            ManagerStatus {
                manager_id: manager_id.to_string(),
                grpc_address: gossip
                    .map(|item| item.grpc_address.clone())
                    .filter(|value| !value.is_empty())
                    .or_else(|| db.map(|item| item.grpc_address.clone()))
                    .unwrap_or_default(),
                gossip_address: gossip
                    .map(|item| item.gossip_address.clone())
                    .filter(|value| !value.is_empty())
                    .or_else(|| db.map(|item| item.gossip_address.clone()))
                    .unwrap_or_default(),
                severity: severity as i32,
                membership: membership_state as i32,
                registered_at: db.map(|item| timestamp(item.registered_at)),
                last_heartbeat: db.map(|item| timestamp(item.last_heartbeat)),
                heartbeat_age_seconds: db
                    .map(|item| age_seconds(snapshot.observed_at, item.last_heartbeat))
                    .unwrap_or_default(),
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
                            .expected_engines
                            .iter()
                            .any(|expected| expected.engine_slot == metadata.engine_slot)
                });
                inventory_matches
                    && (current
                        .get(&metadata.node_id)
                        .is_some_and(|deployment| deployment.revision == metadata.revision)
                        || snapshot.slot_authorities.iter().any(|authority| {
                            authority.node_id == metadata.node_id
                                && authority.engine_slot == metadata.engine_slot
                                && authority.revision == metadata.revision
                        }))
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
            }
        })
        .collect::<Vec<_>>();
    engines.sort_by(|left, right| left.engine_id.cmp(&right.engine_id));
    engines
}

fn compose_nodes(
    snapshot: &ClusterStatusSnapshot,
    engines: &[EngineStatus],
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
            Ok(NodeStatus {
                node_id,
                severity: severity as i32,
                desired_deployment: desired,
                deployment_history: deployments,
                engines: node_engines,
                conditions,
                target_deployment: target,
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
        for engine in &deployment.expected_engines {
            for module in &engine.modules {
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
    membership: MembershipSnapshot,
    within_convergence_window: bool,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Result<GetClusterStatusResponse, tonic::Status> {
    let current = snapshot
        .deployments
        .iter()
        .filter(|item| {
            item.record.revision == item.current_revision
                || (item.current_revision == 0
                    && item.target_revision == Some(item.record.revision))
        })
        .map(|item| (item.record.node_id.clone(), item.record.clone()))
        .collect::<BTreeMap<_, _>>();
    let managers = compose_managers(&snapshot, &membership, within_convergence_window);
    let engines = compose_engines(
        &snapshot,
        &current,
        engine_timeout_secs,
        module_timeout_secs,
    );
    let nodes = compose_nodes(
        &snapshot,
        &engines,
        engine_timeout_secs,
        module_timeout_secs,
    )?;
    let services = compose_services(&snapshot, &current, &engines);
    let severity = reduce_severity(
        managers
            .iter()
            .map(|item| item.severity)
            .chain(nodes.iter().map(|item| item.severity))
            .chain(engines.iter().map(|item| item.severity))
            .chain(services.iter().map(|item| item.severity))
            .map(|value| StatusSeverity::try_from(value).unwrap_or(StatusSeverity::Unknown)),
    );
    let conditions = [
        (
            "proxy routing synchronization age",
            "proxy-routing-sync-age",
        ),
        ("host CPU and memory utilization", "host-resources"),
        ("proxy circuit-breaker state", "circuit-breakers"),
    ]
    .into_iter()
    .map(|(detail, identity)| {
        condition(
            "SIGNAL_NOT_REPORTED",
            StatusSeverity::Unknown,
            format!("{detail} is not reported by managers"),
            identity,
            "reported signal",
            "not reported",
        )
    })
    .collect();

    Ok(GetClusterStatusResponse {
        response_at: Some(timestamp(chrono::Utc::now())),
        database_observed_at: Some(timestamp(snapshot.observed_at)),
        gossip_observed_at: Some(timestamp(membership.observed_at)),
        routing_table_version: snapshot.routing_version,
        severity: severity as i32,
        managers,
        nodes,
        engines,
        services,
        conditions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn service_availability_is_partial_and_deterministic() {
        use wr_common::wruntime::{DeploymentRecord, ExpectedEngine, RoutingRule};

        let module = ModuleIdentity {
            namespace: "store".into(),
            name: "inventory".into(),
            version: "1.0.0".into(),
        };
        let deployment = DeploymentRecord {
            node_id: "node-a".into(),
            revision: 1,
            expected_engines: vec![
                ExpectedEngine {
                    engine_slot: "one".into(),
                    modules: vec![module.clone()],
                },
                ExpectedEngine {
                    engine_slot: "two".into(),
                    modules: vec![module],
                },
            ],
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
            module_heartbeats: Vec::new(),
            routes,
            managers: Vec::new(),
            slot_authorities: Vec::new(),
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
