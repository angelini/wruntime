//! Importable wruntime CLI core.
//!
//! Command-line parsing stays in `main.rs`; lifecycle agent traits and the one
//! production backend are exported through `cmd` for deterministic integration
//! tests without exposing arbitrary command execution.

pub mod client;
pub mod cmd;
pub mod display;
