use std::fmt;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use anyhow::{bail, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    Pending,
    Running,
    Complete,
    Dead,
}

impl JobState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Dead => "dead",
        }
    }
}

impl TryFrom<&str> for JobState {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "dead" => Ok(Self::Dead),
            other => bail!("unknown job state '{other}'"),
        }
    }
}

impl PartialEq<&str> for JobState {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

macro_rules! positive_u32 {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name(NonZeroU32);
        impl $name {
            pub fn new(value: u32) -> Result<Self> {
                NonZeroU32::new(value)
                    .map(Self)
                    .ok_or_else(|| anyhow::anyhow!(concat!($label, " must be > 0")))
            }
            pub const fn get(self) -> u32 {
                self.0.get()
            }
        }
    };
}

positive_u32!(MaxAttempts, "max attempts");
positive_u32!(JobTimeoutSecs, "job timeout");
positive_u32!(ScheduleIntervalSecs, "schedule interval");

macro_rules! compare_u32 {
    ($name:ident) => {
        impl PartialEq<u32> for $name {
            fn eq(&self, other: &u32) -> bool {
                self.get() == *other
            }
        }
        impl PartialEq<i32> for $name {
            fn eq(&self, other: &i32) -> bool {
                u32::try_from(*other).is_ok_and(|value| self.get() == value)
            }
        }
    };
}
compare_u32!(MaxAttempts);
compare_u32!(JobTimeoutSecs);
compare_u32!(ScheduleIntervalSecs);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptCount(u32);
impl PartialEq<u32> for AttemptCount {
    fn eq(&self, other: &u32) -> bool {
        self.get() == *other
    }
}
impl PartialEq<i32> for AttemptCount {
    fn eq(&self, other: &i32) -> bool {
        u32::try_from(*other).is_ok_and(|value| self.get() == value)
    }
}
impl AttemptCount {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
    pub fn validate(self, max: MaxAttempts) -> Result<Self> {
        if self.0 <= max.get() {
            Ok(self)
        } else {
            bail!("attempt count exceeds max attempts")
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureCount(u32);
impl FailureCount {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerConcurrency(NonZeroUsize);
impl WorkerConcurrency {
    pub fn new(value: usize) -> Result<Self> {
        NonZeroUsize::new(value)
            .map(Self)
            .ok_or_else(|| anyhow::anyhow!("worker concurrency must be > 0"))
    }
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveDuration(Duration);
impl PositiveDuration {
    pub fn from_secs(value: u64, label: &str) -> Result<Self> {
        let seconds =
            NonZeroU64::new(value).ok_or_else(|| anyhow::anyhow!("{label} must be > 0"))?;
        Ok(Self(Duration::from_secs(seconds.get())))
    }
    pub const fn get(self) -> Duration {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_domains_reject_invalid_values() {
        assert!(MaxAttempts::new(0).is_err());
        assert!(JobTimeoutSecs::new(0).is_err());
        assert!(ScheduleIntervalSecs::new(0).is_err());
        assert!(WorkerConcurrency::new(0).is_err());
        assert!(JobState::try_from("claimed").is_err());
        assert!(AttemptCount::new(4)
            .validate(MaxAttempts::new(3).unwrap())
            .is_err());
    }
}
