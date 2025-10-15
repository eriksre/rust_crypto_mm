//! Logging utilities for trading activity
//!
//! Provides structured logging for quotes, fills, and market data.

#[cfg(feature = "gate_exec")]
pub mod debug_logger;

#[cfg(feature = "gate_exec")]
pub mod quote;
