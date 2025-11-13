//! MEXC exchange integration
//!
//! Provides websocket parser and orderbook maintenance for MEXC USDT perpetual futures.

pub mod orderbook;
pub mod parser;

pub use orderbook::{MexcBook, MexcDepthMsg};
pub use parser::{MexcFrame, MexcHandler};
