//! Canonical, non-secret host node-agent policy.
//!
//! The exact bytes produced here are the only supported `agent.toml` encoding.
//! Manager expectations, installer output, and process attestation all use the
//! same normalized record and SHA-256 digest.

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
    pub retention_count: u32,
    pub protocol_version: String,
    pub capabilities: Vec<String>,
}

impl AgentPolicy {
    pub fn normalized(&self) -> Result<Self> {
        let mut policy = self.clone();
        policy.capabilities.sort();
        policy.capabilities.dedup();
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
        if self.retention_count == 0 {
            bail!("retention-count must be positive");
        }
        if self.protocol_version != AGENT_PROTOCOL_VERSION {
            bail!("node-agent protocol version must exactly match this binary");
        }
        let supported = AGENT_CAPABILITIES
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        if self.capabilities != supported {
            bail!("node-agent capabilities must exactly match this binary");
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let policy = self.normalized()?;
        Ok(toml::to_string(&policy)
            .context("failed to serialize canonical node-agent policy")?
            .into_bytes())
    }

    pub fn canonical_digest(&self) -> Result<String> {
        Ok(sha256_digest(&self.canonical_bytes()?))
    }

    /// Parse only the canonical encoding. This rejects unknown/omitted fields
    /// and alternate TOML renderings before any digest is accepted.
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

fn validate_identity(value: &str, name: &str) -> Result<()> {
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
            retention_count: 3,
            protocol_version: AGENT_PROTOCOL_VERSION.into(),
            capabilities: AGENT_CAPABILITIES
                .iter()
                .map(|value| (*value).into())
                .collect(),
        }
    }

    #[test]
    fn canonical_policy_is_stable_and_every_field_is_digest_bound() {
        let baseline = policy();
        let bytes = baseline.canonical_bytes().unwrap();
        assert_eq!(bytes, baseline.canonical_bytes().unwrap());
        assert_eq!(baseline, AgentPolicy::from_canonical_bytes(&bytes).unwrap());
        let digest = baseline.canonical_digest().unwrap();

        type PolicyMutation = Box<dyn Fn(&mut AgentPolicy)>;
        let mutations: Vec<PolicyMutation> = vec![
            Box::new(|p| p.node_id = "node-b".into()),
            Box::new(|p| p.manager_endpoint = "https://other.example:9000".into()),
            Box::new(|p| p.client_cert_path.push_str(".new")),
            Box::new(|p| p.client_key_path.push_str(".new")),
            Box::new(|p| p.ca_cert_path.push_str(".new")),
            Box::new(|p| p.deployment_root = "/srv/wruntime".into()),
            Box::new(|p| p.runtime_dir = "/run/wruntime-other".into()),
            Box::new(|p| p.systemctl_path = "/bin/systemctl".into()),
            Box::new(|p| p.poll_interval_seconds = 6),
            Box::new(|p| p.renew_interval_seconds = 6),
            Box::new(|p| p.retention_count = 4),
        ];
        for mutate in mutations {
            let mut changed = baseline.clone();
            mutate(&mut changed);
            assert_ne!(changed.canonical_digest().unwrap(), digest);
        }
        let mut docker = baseline.clone();
        docker.backend = AgentPolicyBackend::Docker;
        docker.systemctl_path.clear();
        docker.docker_path = "/usr/bin/docker".into();
        docker.compose_project = "wruntime-node".into();
        assert_ne!(docker.canonical_digest().unwrap(), digest);
        let docker_digest = docker.canonical_digest().unwrap();
        docker.compose_project = "wruntime-other".into();
        assert_ne!(docker.canonical_digest().unwrap(), docker_digest);
        docker.compose_project = "wruntime-node".into();
        docker.docker_path = "/opt/bin/docker".into();
        assert_ne!(docker.canonical_digest().unwrap(), docker_digest);

        let mut invalid = baseline.clone();
        invalid.protocol_version = "mixed-version".into();
        assert!(invalid.canonical_digest().is_err());
        invalid = baseline.clone();
        invalid.capabilities.pop();
        assert!(invalid.canonical_digest().is_err());
        invalid = baseline;
        invalid.policy_version += 1;
        assert!(invalid.canonical_digest().is_err());
    }

    #[test]
    fn parser_rejects_unknown_omitted_and_noncanonical_input() {
        let canonical = String::from_utf8(policy().canonical_bytes().unwrap()).unwrap();
        assert!(AgentPolicy::from_canonical_bytes(
            canonical
                .replace("node-id =", "unknown = 1\nnode-id =")
                .as_bytes()
        )
        .is_err());
        assert!(AgentPolicy::from_canonical_bytes(
            canonical
                .lines()
                .filter(|line| !line.starts_with("retention-count ="))
                .collect::<Vec<_>>()
                .join("\n")
                .as_bytes()
        )
        .is_err());
        assert!(AgentPolicy::from_canonical_bytes(format!("\n{canonical}").as_bytes()).is_err());
    }
}
