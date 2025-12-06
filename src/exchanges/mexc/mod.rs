//! MEXC exchange integration
//!
//! Provides websocket parser and orderbook maintenance for MEXC USDT perpetual futures.

pub mod orderbook;
pub mod parser;
pub mod rest;

pub use orderbook::{MexcBook, MexcDepthMsg};
pub use parser::{MexcFrame, MexcHandler};
pub use rest::*;
