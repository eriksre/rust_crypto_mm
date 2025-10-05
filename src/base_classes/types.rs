#![allow(dead_code)]

#[cfg(feature = "gate_exec")]
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "gate_exec", derive(Serialize, Deserialize))]
pub enum Side {
    Bid = 0,
    Ask = 1,
}

pub type Price = i64; // integer ticks (e.g., price * 1e4)
pub type Qty = i64; // integer size (e.g., contracts)
pub type Ts = u64; // timestamp in ns or ms (up to caller)
pub type Seq = u64; // exchange sequence number

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct PriceLevel {
    pub px: Price,
    pub qty: Qty,
}

impl PriceLevel {
    #[inline(always)]
    pub const fn new(px: Price, qty: Qty) -> Self {
        Self { px, qty }
    }
}
