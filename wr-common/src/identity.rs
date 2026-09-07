use std::fmt;
use std::str::FromStr;

use anyhow::{bail, Result};
use http::Uri;
use semver::{Version, VersionReq};

pub const MAX_STABLE_NAME_LEN: usize = 24;

/// Validate a stable identity segment used by both resource names and
/// certificate URI principals.
pub fn validate_name(value: &str, kind: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        bail!("{kind} is required");
    }
    if bytes.len() > MAX_STABLE_NAME_LEN {
        bail!("{kind} must be at most {MAX_STABLE_NAME_LEN} characters");
    }
    if !bytes.first().is_some_and(u8::is_ascii_lowercase)
        && !bytes.first().is_some_and(u8::is_ascii_digit)
    {
        bail!("{kind} must start with a lowercase ASCII letter or digit");
    }
    if !bytes.last().is_some_and(u8::is_ascii_lowercase)
        && !bytes.last().is_some_and(u8::is_ascii_digit)
    {
        bail!("{kind} must end with a lowercase ASCII letter or digit");
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
    {
        bail!("{kind} may contain only lowercase ASCII letters, digits, and '-'");
    }
    Ok(())
}

macro_rules! name_type {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);
        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                validate_name(&value, $label)?;
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl FromStr for $name {
            type Err = anyhow::Error;
            fn from_str(value: &str) -> Result<Self> {
                Self::parse(value)
            }
        }
    };
}

name_type!(Namespace, "namespace");
name_type!(ModuleName, "module name");
name_type!(JobQueueId, "job queue id");
name_type!(ClusterId, "cluster id");
name_type!(NodeId, "node id");
name_type!(ManagerId, "manager id");
name_type!(PrincipalName, "principal name");

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PrincipalKind {
    Human,
    ServiceAccount,
    Manager,
    Proxy,
    NodeAgent,
}

impl PrincipalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::ServiceAccount => "service-account",
            Self::Manager => "manager",
            Self::Proxy => "proxy",
            Self::NodeAgent => "node-agent",
        }
    }
}

impl FromStr for PrincipalKind {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "human" => Ok(Self::Human),
            "service-account" => Ok(Self::ServiceAccount),
            "manager" => Ok(Self::Manager),
            "proxy" => Ok(Self::Proxy),
            "node-agent" => Ok(Self::NodeAgent),
            _ => bail!("unsupported principal kind"),
        }
    }
}

/// A byte-exact project-owned client identity from the sole URI SAN.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PrincipalUri {
    value: String,
    cluster_id: ClusterId,
    kind: PrincipalKind,
    name: PrincipalName,
}

impl PrincipalUri {
    pub fn parse(value: &str) -> Result<Self> {
        // Split and compare the complete input; accepting URI normalization here
        // would make a certificate identity ambiguous.
        let mut segments = value.split(':');
        if segments.next() != Some("urn") || segments.next() != Some("wruntime") {
            bail!("principal URI must start with urn:wruntime:");
        }
        let cluster = segments
            .next()
            .ok_or_else(|| anyhow::anyhow!("principal URI is missing cluster id"))?;
        let kind = segments
            .next()
            .ok_or_else(|| anyhow::anyhow!("principal URI is missing kind"))?;
        let name = segments
            .next()
            .ok_or_else(|| anyhow::anyhow!("principal URI is missing name"))?;
        if segments.next().is_some() {
            bail!("principal URI has trailing segments");
        }
        let cluster_id = ClusterId::parse(cluster)?;
        let kind = PrincipalKind::from_str(kind)?;
        let name = PrincipalName::parse(name)?;
        let canonical = format!("urn:wruntime:{cluster_id}:{}:{name}", kind.as_str());
        if canonical.as_bytes() != value.as_bytes() {
            bail!("principal URI is not byte-exact canonical form");
        }
        Ok(Self {
            value: canonical,
            cluster_id,
            kind,
            name,
        })
    }

    pub fn new(cluster_id: ClusterId, kind: PrincipalKind, name: PrincipalName) -> Self {
        let value = format!("urn:wruntime:{cluster_id}:{}:{name}", kind.as_str());
        Self {
            value,
            cluster_id,
            kind,
            name,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn kind(&self) -> PrincipalKind {
        self.kind
    }

    pub fn name(&self) -> &PrincipalName {
        &self.name
    }
}

impl fmt::Display for PrincipalUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

impl FromStr for PrincipalUri {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

macro_rules! opaque_id_type {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
        pub struct $name(String);
        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                if value.is_empty() || value.len() > 128 || value.chars().any(char::is_whitespace) {
                    bail!(concat!(
                        $label,
                        " must be 1..=128 non-whitespace characters"
                    ));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
opaque_id_type!(EngineId, "engine id");
opaque_id_type!(RuleId, "rule id");

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModuleVersion(Version);
impl ModuleVersion {
    pub fn parse(value: &str) -> Result<Self> {
        Version::parse(value).map(Self).map_err(Into::into)
    }
    pub fn as_version(&self) -> &Version {
        &self.0
    }
}
impl fmt::Display for ModuleVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RouteKey {
    pub namespace: Namespace,
    pub module: ModuleName,
}
impl RouteKey {
    /// Validate a borrowed route identity without constructing owned name types.
    pub fn validate(namespace: &str, module: &str) -> Result<()> {
        validate_name(namespace, "namespace")?;
        validate_name(module, "module name")
    }

    pub fn parse(namespace: &str, module: &str) -> Result<Self> {
        Self::validate(namespace, module)?;
        Ok(Self {
            namespace: Namespace(namespace.to_owned()),
            module: ModuleName(module.to_owned()),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ModuleId {
    pub route: RouteKey,
    pub version: ModuleVersion,
}
impl ModuleId {
    pub fn parse(namespace: &str, module: &str, version: &str) -> Result<Self> {
        let route = RouteKey::parse(namespace, module)?;
        if crate::naming::module_schema(route.namespace.as_str(), route.module.as_str()).len() > 63
        {
            bail!("module identity is too long for a collision-safe PostgreSQL schema name");
        }
        Ok(Self {
            route,
            version: ModuleVersion::parse(version)?,
        })
    }
}

#[derive(Clone, Debug)]
pub enum NamespaceFilter {
    All,
    One(Namespace),
}
impl NamespaceFilter {
    pub fn from_wire(value: &str) -> Result<Self> {
        if value.is_empty() {
            Ok(Self::All)
        } else {
            Ok(Self::One(Namespace::parse(value)?))
        }
    }
    pub fn as_db_value(&self) -> &str {
        match self {
            Self::All => "",
            Self::One(namespace) => namespace.as_str(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionSelector(VersionReq);
impl VersionSelector {
    pub fn parse(value: &str) -> Result<Self> {
        VersionReq::parse(value).map(Self).map_err(Into::into)
    }
    pub fn matches(&self, version: &ModuleVersion) -> bool {
        self.0.matches(version.as_version())
    }
}

fn endpoint(value: &str, kind: &str, schemes: &[&str], require_port: bool) -> Result<Uri> {
    let uri: Uri = value
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid {kind}: {e}"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| anyhow::anyhow!("{kind} requires a scheme"))?;
    if !schemes.contains(&scheme) {
        bail!("{kind} scheme must be {}", schemes.join(" or "));
    }
    if uri.host().is_none() {
        bail!("{kind} requires a host");
    }
    if require_port && uri.port_u16().is_none() {
        bail!("{kind} requires an explicit port");
    }
    Ok(uri)
}

macro_rules! endpoint_type {
    ($name:ident, $label:literal, $schemes:expr, $port:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name(String);
        impl $name {
            pub fn parse(value: &str) -> Result<Self> {
                endpoint(value, $label, $schemes, $port)?;
                Ok(Self(value.to_string()))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
            pub fn into_string(self) -> String {
                self.0
            }
        }
    };
}
endpoint_type!(EngineHttpUrl, "engine address", &["http", "https"], false);
endpoint_type!(ProxyHttpUrl, "proxy address", &["http", "https"], false);
endpoint_type!(ControlHttpUrl, "control address", &["http", "https"], false);
endpoint_type!(PeerHttpsUrl, "peer address", &["https"], true);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn names_reject_lossy_collision_inputs() {
        assert!(Namespace::parse("foo-bar").is_ok());
        assert!(Namespace::parse("foo_bar").is_err());
        assert!(ModuleName::parse("a.b").is_err());
        assert!(Namespace::parse("Foo").is_err());
        assert!(Namespace::parse("a".repeat(25)).is_err());
        assert!(JobQueueId::parse("primary-jobs").is_ok());
        assert!(JobQueueId::parse("Primary_Jobs").is_err());
    }
    #[test]
    fn principal_uris_are_byte_exact_and_reuse_stable_names() {
        let principal = PrincipalUri::parse("urn:wruntime:cluster-a:node-agent:node-a").unwrap();
        assert_eq!(principal.cluster_id().as_str(), "cluster-a");
        assert_eq!(principal.kind(), PrincipalKind::NodeAgent);
        assert_eq!(principal.name().as_str(), "node-a");
        for invalid in [
            "spiffe://wruntime/cluster-a/node-agent/node-a",
            "urn:wruntime:Cluster-a:node-agent:node-a",
            "urn:wruntime:cluster-a:unknown:node-a",
            "urn:wruntime:cluster-a:proxy:",
            "urn:wruntime:cluster-a:proxy:node-a:",
            "urn:wruntime:cluster-a:proxy:node_a",
            "urn:wruntime:cluster-a:proxy:node-a?x=1",
        ] {
            assert!(PrincipalUri::parse(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn borrowed_and_owned_route_validation_match() {
        let valid_24 = "a".repeat(24);
        let invalid_25 = "a".repeat(25);
        for (namespace, module) in [
            ("store", "inventory"),
            (valid_24.as_str(), "module"),
            ("", "module"),
            ("Store", "module"),
            ("store_name", "module"),
            ("store.name", "module"),
            ("-store", "module"),
            ("store-", "module"),
            (invalid_25.as_str(), "module"),
            ("store", ""),
            ("store", "Module"),
            ("store", "module_name"),
            ("store", "module.name"),
            ("store", "-module"),
            ("store", "module-"),
            ("store", invalid_25.as_str()),
        ] {
            assert_eq!(
                RouteKey::validate(namespace, module).is_ok(),
                RouteKey::parse(namespace, module).is_ok(),
                "borrowed and owned validation differ for {namespace:?}.{module:?}"
            );
        }
    }

    #[test]
    fn versions_and_endpoints_are_typed() {
        assert!(ModuleVersion::parse("1.2.3").is_ok());
        assert!(ModuleVersion::parse("latest").is_err());
        assert!(ControlHttpUrl::parse("http://127.0.0.1:9002").is_ok());
        assert!(ControlHttpUrl::parse("127.0.0.1:9002").is_err());
        assert!(PeerHttpsUrl::parse("https://node:9443").is_ok());
        assert!(PeerHttpsUrl::parse("http://node:9443").is_err());
    }
    #[test]
    fn empty_filter_means_all() {
        assert!(matches!(
            NamespaceFilter::from_wire("").unwrap(),
            NamespaceFilter::All
        ));
        assert!(matches!(
            NamespaceFilter::from_wire("store").unwrap(),
            NamespaceFilter::One(_)
        ));
        assert!(NamespaceFilter::from_wire("bad_namespace").is_err());
    }
}
