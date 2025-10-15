//! Gate.io exchange integration
//!
//! This module provides market data parsing, orderbook management,
//! REST API clients, and signing utilities for Gate.io perpetual futures.

pub mod orderbook;
pub mod parser;
pub mod rest;

#[cfg(feature = "gate_exec")]
pub mod signing;

// Re-export commonly used types
pub use orderbook::GateBook;
pub use parser::*;
pub use rest::*;

#[cfg(feature = "gate_exec")]
pub use signing::*;
