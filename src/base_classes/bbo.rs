#![allow(dead_code)]

use crate::base_classes::types::*;

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Bbo {
    pub bid_px: Price,
    pub bid_qty: Qty,
    pub ask_px: Price,
    pub ask_qty: Qty,
    pub ts: Ts,
    pub seq: Seq,
}

impl Bbo {
    #[inline(always)]
    pub fn update_bid(&mut self, px: Price, qty: Qty, ts: Ts, seq: Seq) {
        self.bid_px = px;
        self.bid_qty = qty;
        self.ts = ts;
        self.seq = seq;
    }

    #[inline(always)]
    pub fn update_ask(&mut self, px: Price, qty: Qty, ts: Ts, seq: Seq) {
        self.ask_px = px;
        self.ask_qty = qty;
        self.ts = ts;
        self.seq = seq;
    }

    #[inline(always)]
    pub fn mid_px(&self) -> Price {
        (self.bid_px + self.ask_px) / 2
    }

    #[inline(always)]
    pub fn spread(&self) -> Price {
        self.ask_px - self.bid_px
    }

    #[inline(always)]
    pub fn is_crossed(&self) -> bool {
        self.ask_px <= self.bid_px
    }
}
