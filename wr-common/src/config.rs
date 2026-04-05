use anyhow::{Context, Result};
use serde::de::DeserializeOwned;

/// Implement this on config structs that have field-level validation.
pub trait Validatable {
    fn validate(&self) -> Result<()>;
}

/// Accumulates multiple validation errors and reports them all at once.
///
/// ```ignore
/// let mut v = Validator::new();
/// v.check(!name.is_empty(), "name is required");
/// v.check(port > 0, format!("port must be > 0, got {port}"));
/// v.finish()?;
/// ```
#[derive(Default)]
pub struct Validator {
    errors: Vec<String>,
}

impl Validator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an error if `condition` is false.
    pub fn check(&mut self, condition: bool, msg: impl Into<String>) {
        if !condition {
            self.errors.push(msg.into());
        }
    }

    /// Return `Ok(())` if no errors were recorded, or a combined error message.
    pub fn finish(self) -> Result<()> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "config validation failed:\n  - {}",
                self.errors.join("\n  - ")
            )
        }
    }
}

/// Loads a TOML config file, deserializes it, and validates.
pub fn load<T: DeserializeOwned + Validatable>(path: &str) -> Result<T> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("failed to read config: {path}"))?;
    let config: T = toml::from_str(&content).context("failed to parse config")?;
    config.validate().context("invalid config")?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        name: String,
        port: u16,
    }

    impl Validatable for TestConfig {
        fn validate(&self) -> Result<()> {
            if self.port == 0 {
                anyhow::bail!("port must be non-zero");
            }
            Ok(())
        }
    }

    fn write_temp(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn load_valid_config() {
        let f = write_temp("name = \"proxy\"\nport = 9001\n");
        let cfg: TestConfig = load(f.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.name, "proxy");
        assert_eq!(cfg.port, 9001);
    }

    #[test]
    fn load_missing_file() {
        let err = load::<TestConfig>("/tmp/nonexistent_wr_config_test.toml").unwrap_err();
        assert!(
            format!("{err:#}").contains("failed to read config"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_invalid_toml() {
        let f = write_temp("this is not valid toml {{{}}}");
        let err = load::<TestConfig>(f.path().to_str().unwrap()).unwrap_err();
        assert!(
            format!("{err:#}").contains("failed to parse config"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_validation_failure() {
        let f = write_temp("name = \"bad\"\nport = 0\n");
        let err = load::<TestConfig>(f.path().to_str().unwrap()).unwrap_err();
        assert!(
            format!("{err:#}").contains("invalid config"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn load_missing_required_field() {
        let f = write_temp("name = \"incomplete\"\n");
        let err = load::<TestConfig>(f.path().to_str().unwrap()).unwrap_err();
        assert!(
            format!("{err:#}").contains("failed to parse config"),
            "unexpected error: {err:#}"
        );
    }
}
