//! Canonical manager-owned deployment inventory and revision identity.

use std::collections::BTreeSet;

use anyhow::{bail, ensure, Result};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::identity::{JobQueueId, ModuleId, Namespace, NodeId, PeerHttpsUrl};
use crate::wruntime::{DeploymentInventoryV1, ExpectedEngine};

pub const DEPLOYMENT_INVENTORY_SCHEMA_VERSION: u32 = 1;
pub const MAX_DEPLOYMENT_INVENTORY_BYTES: usize = 2 * 1024 * 1024;
const DOMAIN: &[u8] = b"wruntime-deployment-revision-v1";

fn push_u32(out: &mut Vec<u8>, value: usize) -> Result<()> {
    out.extend_from_slice(&u32::try_from(value)?.to_be_bytes());
    Ok(())
}
fn push_str(out: &mut Vec<u8>, value: &str) -> Result<()> {
    push_u32(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
fn valid_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub fn canonicalize_inventory(
    mut inventory: DeploymentInventoryV1,
) -> Result<DeploymentInventoryV1> {
    ensure!(
        inventory.schema_version == DEPLOYMENT_INVENTORY_SCHEMA_VERSION,
        "deployment inventory schema_version must be 1"
    );
    ensure!(
        !inventory.engines.is_empty() && inventory.engines.len() <= 1_024,
        "deployment inventory engine count is out of range"
    );
    let mut total = 0usize;
    let mut slots = BTreeSet::new();
    for engine in &mut inventory.engines {
        ensure!(
            valid_token(&engine.engine_slot) && slots.insert(engine.engine_slot.clone()),
            "engine slots must be unique URL-safe identities"
        );
        ensure!(
            engine.modules.len() <= 4_096
                && engine.secrets.len() <= 4_096
                && engine.db_namespaces.len() <= 4_096,
            "per-slot deployment inventory cap exceeded"
        );
        total = total
            .checked_add(engine.modules.len() + engine.secrets.len() + engine.db_namespaces.len())
            .ok_or_else(|| anyhow::anyhow!("deployment inventory count overflow"))?;
        let mut module_keys = BTreeSet::new();
        for module in &engine.modules {
            let identity = module
                .identity
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("expected module identity is required"))?;
            ModuleId::parse(&identity.namespace, &identity.name, &identity.version)?;
            ensure!(
                module.proto_schema_digest.is_empty() || valid_digest(&module.proto_schema_digest),
                "module schema digest must be empty or lowercase sha256"
            );
            ensure!(
                module_keys.insert((
                    identity.namespace.clone(),
                    identity.name.clone(),
                    identity.version.clone(),
                    module.proto_schema_digest.clone()
                )),
                "duplicate expected module"
            );
        }
        engine.modules.sort_by(|a, b| {
            let ai = a.identity.as_ref().expect("validated");
            let bi = b.identity.as_ref().expect("validated");
            (&ai.namespace, &ai.name, &ai.version, &a.proto_schema_digest).cmp(&(
                &bi.namespace,
                &bi.name,
                &bi.version,
                &b.proto_schema_digest,
            ))
        });
        let mut secret_keys = BTreeSet::new();
        for secret in &engine.secrets {
            Namespace::parse(&secret.namespace)?;
            ensure!(
                !secret.key.is_empty()
                    && secret.key.len() <= 256
                    && secret_keys.insert((secret.namespace.clone(), secret.key.clone())),
                "invalid or duplicate secret declaration"
            );
        }
        engine
            .secrets
            .sort_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
        let mut dbs = BTreeSet::new();
        for namespace in &engine.db_namespaces {
            Namespace::parse(namespace)?;
            ensure!(
                dbs.insert(namespace.clone()),
                "duplicate database namespace"
            );
        }
        engine.db_namespaces.sort();
        match (
            engine.job_queue_id.is_empty(),
            engine.job_admin_address.is_empty(),
        ) {
            (true, true) => {}
            (false, false) => {
                JobQueueId::parse(&engine.job_queue_id)?;
                ensure!(
                    engine.job_admin_address.len() <= 2_048,
                    "job admin address is too long"
                );
                PeerHttpsUrl::parse(&engine.job_admin_address)?;
                let uri: http::Uri = engine.job_admin_address.parse()?;
                ensure!(uri.scheme_str() == Some("https") && uri.query().is_none() && !uri.authority().is_some_and(|a| a.as_str().contains('@')) && uri.to_string() == engine.job_admin_address, "job admin address must be canonical absolute HTTPS without userinfo/query/fragment");
            }
            _ => bail!("job queue ID and admin address must be both empty or both present"),
        }
    }
    ensure!(
        total <= 65_536,
        "deployment inventory total entry cap exceeded"
    );
    inventory
        .engines
        .sort_by(|a, b| a.engine_slot.as_bytes().cmp(b.engine_slot.as_bytes()));
    ensure!(
        inventory.encoded_len() <= MAX_DEPLOYMENT_INVENTORY_BYTES,
        "deployment inventory exceeds 2 MiB"
    );
    Ok(inventory)
}

pub fn revision_digest(
    node_id: &str,
    revision: u64,
    bundle_digest: &str,
    inventory: &DeploymentInventoryV1,
) -> Result<String> {
    NodeId::parse(node_id)?;
    ensure!(
        revision > 0 && valid_digest(bundle_digest),
        "revision and normalized bundle digest are required"
    );
    let inventory = canonicalize_inventory(inventory.clone())?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DOMAIN);
    bytes.extend_from_slice(&inventory.schema_version.to_be_bytes());
    push_str(&mut bytes, node_id)?;
    bytes.extend_from_slice(&revision.to_be_bytes());
    push_str(&mut bytes, bundle_digest)?;
    push_u32(&mut bytes, inventory.engines.len())?;
    for ExpectedEngine {
        engine_slot,
        modules,
        secrets,
        db_namespaces,
        job_queue_id,
        job_admin_address,
    } in &inventory.engines
    {
        push_str(&mut bytes, engine_slot)?;
        push_u32(&mut bytes, modules.len())?;
        for module in modules {
            let id = module.identity.as_ref().expect("validated");
            push_str(&mut bytes, &id.namespace)?;
            push_str(&mut bytes, &id.name)?;
            push_str(&mut bytes, &id.version)?;
            push_str(&mut bytes, &module.proto_schema_digest)?;
        }
        push_u32(&mut bytes, secrets.len())?;
        for secret in secrets {
            push_str(&mut bytes, &secret.namespace)?;
            push_str(&mut bytes, &secret.key)?;
        }
        push_u32(&mut bytes, db_namespaces.len())?;
        for namespace in db_namespaces {
            push_str(&mut bytes, namespace)?;
        }
        push_str(&mut bytes, job_queue_id)?;
        push_str(&mut bytes, job_admin_address)?;
    }
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

pub fn schema_digest(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        format!("sha256:{:x}", Sha256::digest(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wruntime::{ExpectedModule, ModuleIdentity, SecretRequest};
    fn inventory(order: bool) -> DeploymentInventoryV1 {
        let mut engines = vec![
            ExpectedEngine {
                engine_slot: "b".into(),
                modules: vec![],
                ..Default::default()
            },
            ExpectedEngine {
                engine_slot: "a".into(),
                modules: vec![ExpectedModule {
                    identity: Some(ModuleIdentity {
                        namespace: "store".into(),
                        name: "api".into(),
                        version: "1.0.0".into(),
                    }),
                    proto_schema_digest: schema_digest(b"schema"),
                }],
                secrets: vec![SecretRequest {
                    namespace: "store".into(),
                    key: "TOKEN".into(),
                }],
                db_namespaces: vec!["store".into()],
                ..Default::default()
            },
        ];
        if order {
            engines.reverse();
        }
        DeploymentInventoryV1 {
            schema_version: 1,
            engines,
        }
    }
    #[test]
    fn revision_digest_is_order_independent_and_schema_sensitive() {
        let bundle = format!("sha256:{}", "a".repeat(64));
        let a = revision_digest("node-a", 1, &bundle, &inventory(false)).unwrap();
        let b = revision_digest("node-a", 1, &bundle, &inventory(true)).unwrap();
        assert_eq!(a, b);
        assert_eq!(
            a,
            "sha256:5bc72177529c03fcbce1d99a8bc4f42d67351effe08305b1809ce398be4f315c"
        );
        let mut changed = inventory(false);
        changed.engines[1].modules[0].proto_schema_digest = schema_digest(b"other");
        assert_ne!(a, revision_digest("node-a", 1, &bundle, &changed).unwrap());
    }
    #[test]
    fn duplicates_and_empty_schema_have_distinct_contracts() {
        assert_eq!(schema_digest(&[]), "");
        let mut value = inventory(false);
        value.engines.push(value.engines[0].clone());
        assert!(canonicalize_inventory(value)
            .unwrap_err()
            .to_string()
            .contains("unique"));
    }
}
