use std::fmt;
use std::str::FromStr;

use anyhow::{bail, Result};
use http::Uri;
use semver::{Version, VersionReq};

const MAX_ID_LEN: usize = 24;

fn validate_name(value: &str, kind: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        bail!("{kind} is required");
    }
    if bytes.len() > MAX_ID_LEN {
        bail!("{kind} must be at most {MAX_ID_LEN} characters");
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
    pub fn parse(namespace: &str, module: &str) -> Result<Self> {
        Ok(Self {
            namespace: Namespace::parse(namespace)?,
            module: ModuleName::parse(module)?,
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
    }
    #[test]
    fn versions_and_endpoints_are_typed() {
        assert!(ModuleVersion::parse("1.2.3").is_ok());
        assert!(ModuleVersion::parse("latest").is_err());
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
