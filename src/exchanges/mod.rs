//! Exchange integrations for crypto perpetual futures
//!
//! # Structure
//! Each exchange now has its own submodule with clear organization:
//! - `gate`: Parser, orderbook, REST API, signing
//! - `bybit`: Parser, orderbook
//! - `binance`: Parser, orderbook, REST API, parsed types
//! - `bitget`: Parser, orderbook
//!
//! # Supported Exchanges
//! - **Gate.io**: Full support including execution
//! - **Bybit**: Market data
//! - **Binance**: Market data
//! - **Bitget**: Market data
//! - **OKX**: Market data (perpetual futures)
//!
//! # Adding a New Exchange
//! 1. Create `src/exchanges/{exchange}/` directory
//! 2. Add `parser.rs` for message parsing
//! 3. Add `orderbook.rs` and implement `OrderBookOps` trait
//! 4. Add `rest.rs` for REST API if needed
//! 5. Create `mod.rs` to export public types
//! 6. Add to this file

pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod endpoints;
pub mod gate;
pub mod okx;

// Backwards compatibility: Re-export for code using old flat structure
#[cfg(feature = "binance_book")]
pub use binance::orderbook as binance_book;
#[cfg(feature = "parse_binance")]
pub use binance::parsed as binance_parsed;
#[cfg(feature = "binance_book")]
pub use binance::rest as binance_get;

#[cfg(feature = "bitget_book")]
pub use bitget::orderbook as bitget_book;

pub use bybit::orderbook as bybit_book;

pub use gate::orderbook as gate_book;
pub use gate::rest as gate_rest;
#[cfg(feature = "gate_exec")]
pub use gate::signing as gate_sign;

pub use okx::orderbook as okx_book;
