//! Bitget exchange integration
//!
//! This module provides market data parsing and orderbook management for Bitget perpetual futures.

#[cfg(feature = "bitget_book")]
pub mod orderbook;

pub mod parser;

// Re-export commonly used types
#[cfg(feature = "bitget_book")]
pub use orderbook::BitgetBook;

pub use parser::*;
