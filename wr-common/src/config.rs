use anyhow::{Context, Result};
use serde::de::DeserializeOwned;

/// Implement this on config structs that have field-level validation.
pub trait Validatable {
    fn validate(&self) -> Result<()>;
}

/// Loads a TOML config file, deserializes it, and validates.
pub fn load<T: DeserializeOwned + Validatable>(path: &str) -> Result<T> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("failed to read config: {path}"))?;
    let config: T = toml::from_str(&content).context("failed to parse config")?;
    config.validate().context("invalid config")?;
    Ok(config)
}
