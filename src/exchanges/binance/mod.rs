//! Binance exchange integration
//!
//! This module provides market data parsing and orderbook management for Binance perpetual futures.

#[cfg(feature = "binance_book")]
pub mod orderbook;

pub mod parser;

#[cfg(feature = "binance_book")]
pub mod rest;

#[cfg(feature = "parse_binance")]
pub mod parsed;

// Re-export commonly used types
#[cfg(feature = "binance_book")]
pub use orderbook::BinanceBook;

pub use parser::*;

#[cfg(feature = "binance_book")]
pub use rest::*;
