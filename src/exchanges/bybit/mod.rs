//! Bybit exchange integration
//!
//! This module provides market data parsing and orderbook management for Bybit perpetual futures.

pub mod orderbook;
pub mod parser;

// Re-export commonly used types
pub use orderbook::BybitBook;
pub use parser::*;

// Re-export common constants
pub use orderbook::{PRICE_SCALE, QTY_SCALE};
