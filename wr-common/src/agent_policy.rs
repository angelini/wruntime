//! Strict, non-secret host node-agent configuration and shared compatibility helpers.
//!
//! The complete configuration is parsed and validated locally. Manager authorization
//! deliberately uses only the narrow compatibility fields exposed by the protobuf.

use std::path::{Component, Path};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const AGENT_POLICY_VERSION: u32 = 1;
pub const AGENT_PROTOCOL_VERSION: &str = "operator-engine-lifecycle-v1";
pub const AGENT_CAPABILITIES: [&str; 4] = [
    "continuous-lease-v1",
    "manager-authorized-retention-v1",
    "release-metadata-v1",
    "typed-backend-v1",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentPolicyBackend {
    Systemd,
    Docker,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentPolicy {
    pub policy_version: u32,
    pub node_id: String,
    pub manager_endpoint: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub ca_cert_path: String,
    pub deployment_root: String,
    pub runtime_dir: String,
    pub backend: AgentPolicyBackend,
    /// Empty for systemd; required for Docker.
    pub compose_project: String,
    /// Required for systemd; empty for Docker.
    pub systemctl_path: String,
    /// Required for Docker; empty for systemd.
    pub docker_path: String,
    pub poll_interval_seconds: u64,
    pub renew_interval_seconds: u64,
    pub protocol_version: String,
    pub capabilities: Vec<String>,
}

impl AgentPolicy {
    pub fn normalized(&self) -> Result<Self> {
        let mut policy = self.clone();
        policy.capabilities = normalize_capabilities(&policy.capabilities)?;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<()> {
        if self.policy_version != AGENT_POLICY_VERSION {
            bail!(
                "unsupported node-agent policy version {}",
                self.policy_version
            );
        }
        validate_identity(&self.node_id, "node-id")?;
        let manager: http::Uri = self
            .manager_endpoint
            .parse()
            .context("manager-endpoint is invalid")?;
        if manager.scheme_str() != Some("https") || manager.host().is_none() {
            bail!("manager-endpoint must be an absolute https URI");
        }
        for (value, name) in [
            (&self.client_cert_path, "client-cert-path"),
            (&self.client_key_path, "client-key-path"),
            (&self.ca_cert_path, "ca-cert-path"),
            (&self.deployment_root, "deployment-root"),
            (&self.runtime_dir, "runtime-dir"),
        ] {
            validate_absolute_path(value, name)?;
        }
        if self.deployment_root == "/" || self.runtime_dir == "/" {
            bail!("deployment-root and runtime-dir must not be filesystem root");
        }
        match self.backend {
            AgentPolicyBackend::Systemd => {
                validate_absolute_path(&self.systemctl_path, "systemctl-path")?;
                if !self.docker_path.is_empty() || !self.compose_project.is_empty() {
                    bail!("Docker command/project fields must be empty for systemd");
                }
            }
            AgentPolicyBackend::Docker => {
                validate_absolute_path(&self.docker_path, "docker-path")?;
                validate_identity(&self.compose_project, "compose-project")?;
                if !self.systemctl_path.is_empty() {
                    bail!("systemctl-path must be empty for Docker");
                }
            }
        }
        if self.poll_interval_seconds == 0 || self.renew_interval_seconds == 0 {
            bail!("poll and renew intervals must be positive");
        }
        if self.renew_interval_seconds >= 15 {
            bail!("renew interval must stay below the manager lease interval");
        }
        if self.protocol_version != AGENT_PROTOCOL_VERSION {
            bail!("node-agent protocol version must exactly match this binary");
        }
        let supported = normalize_capabilities(
            &AGENT_CAPABILITIES
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>(),
        )?;
        if normalize_capabilities(&self.capabilities)? != supported {
            bail!("node-agent capabilities must exactly match this binary");
        }
        Ok(())
    }

    /// Canonical local configuration encoding. These bytes are never an
    /// authorization or manager compatibility input.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let policy = self.normalized()?;
        Ok(toml::to_string(&policy)
            .context("failed to serialize canonical node-agent policy")?
            .into_bytes())
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(bytes).context("node-agent policy is not UTF-8")?;
        let policy: Self = toml::from_str(text).context("node-agent policy is invalid")?;
        let canonical = policy.canonical_bytes()?;
        if canonical != bytes {
            bail!("node-agent policy is not in canonical serialized form");
        }
        policy.normalized()
    }
}

pub fn normalize_capabilities(values: &[String]) -> Result<Vec<String>> {
    let mut normalized = values.to_vec();
    for capability in &normalized {
        validate_identity(capability, "capability")?;
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

pub fn missing_capabilities(required: &[String], advertised: &[String]) -> Result<Vec<String>> {
    let required = normalize_capabilities(required)?;
    let advertised = normalize_capabilities(advertised)?;
    Ok(required
        .into_iter()
        .filter(|capability| advertised.binary_search(capability).is_err())
        .collect())
}

pub fn validate_sha256_digest(value: &str, name: &str) -> Result<()> {
    if !value.starts_with("sha256:")
        || value.len() != 71
        || !value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{name} must be an exact SHA-256 digest");
    }
    Ok(())
}

pub fn sha256_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

pub fn validate_identity(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{name} must be a non-empty stable identity");
    }
    Ok(())
}

fn validate_absolute_path(value: &str, name: &str) -> Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::Prefix(_) | Component::CurDir
            )
        })
    {
        bail!("{name} must be a normalized absolute path");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> AgentPolicy {
        AgentPolicy {
            policy_version: AGENT_POLICY_VERSION,
            node_id: "node-a".into(),
            manager_endpoint: "https://manager.example:9000".into(),
            client_cert_path: "/opt/wruntime/wr-agent/certs/agent.crt".into(),
            client_key_path: "/opt/wruntime/wr-agent/certs/agent.key".into(),
            ca_cert_path: "/opt/wruntime/wr-agent/certs/ca.crt".into(),
            deployment_root: "/opt/wruntime".into(),
            runtime_dir: "/run/wruntime".into(),
            backend: AgentPolicyBackend::Systemd,
            compose_project: String::new(),
            systemctl_path: "/usr/bin/systemctl".into(),
            docker_path: String::new(),
            poll_interval_seconds: 5,
            renew_interval_seconds: 5,
            protocol_version: AGENT_PROTOCOL_VERSION.into(),
            capabilities: AGENT_CAPABILITIES
                .iter()
                .map(|value| (*value).into())
                .collect(),
        }
    }

    #[test]
    fn local_policy_is_strict_but_capability_order_is_set_based() {
        let baseline = policy();
        let bytes = baseline.canonical_bytes().unwrap();
        assert_eq!(baseline, AgentPolicy::from_canonical_bytes(&bytes).unwrap());
        let mut reordered = baseline.clone();
        reordered.capabilities.reverse();
        reordered.capabilities.push(AGENT_CAPABILITIES[0].into());
        assert_eq!(
            reordered.normalized().unwrap(),
            baseline.normalized().unwrap()
        );

        for invalidate in [
            |value: &mut AgentPolicy| value.node_id.clear(),
            |value: &mut AgentPolicy| value.protocol_version = "other".into(),
            |value: &mut AgentPolicy| value.renew_interval_seconds = 15,
            |value: &mut AgentPolicy| value.client_key_path = "relative".into(),
        ] as [fn(&mut AgentPolicy); 4]
        {
            let mut invalid = baseline.clone();
            invalidate(&mut invalid);
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn narrow_contract_helpers_are_deterministic() {
        let required = vec!["b".into(), "a".into(), "a".into()];
        let advertised = vec!["c".into(), "b".into()];
        assert_eq!(normalize_capabilities(&required).unwrap(), vec!["a", "b"]);
        assert_eq!(
            missing_capabilities(&required, &advertised).unwrap(),
            vec!["a"]
        );
        assert!(validate_sha256_digest(&format!("sha256:{}", "a".repeat(64)), "binary").is_ok());
        assert!(validate_sha256_digest("sha256:ABC", "binary").is_err());
    }
}
