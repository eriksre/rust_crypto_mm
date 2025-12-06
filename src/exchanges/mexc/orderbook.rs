#![allow(dead_code)]

use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use crate::utils::time::ms_to_ns;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MexcDepthMsg {
    pub symbol: String,
    #[serde(default)]
    pub channel: Option<String>,
    pub data: MexcDepthData,
    #[serde(default)]
    pub ts: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MexcDepthData {
    #[serde(default)]
    pub asks: Vec<Vec<f64>>,
    #[serde(default)]
    pub bids: Vec<Vec<f64>>,
    #[serde(default)]
    pub begin: Option<u64>,
    #[serde(default)]
    pub end: Option<u64>,
    #[serde(default)]
    pub version: Option<u64>,
}

pub struct MexcBook<const N: usize> {
    pub symbol: String,
    book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    qty_multiplier: f64,
    last_seq: u64,
    initialized: bool,
    last_system_ts_ns: Option<Ts>,
}

impl<const N: usize> MexcBook<N> {
    pub const PRICE_SCALE: f64 = 100_000.0;
    pub const QTY_SCALE: f64 = 1_000_000.0;

    pub fn new(symbol: &str, price_scale: f64, qty_scale: f64, qty_multiplier: f64) -> Self {
        Self {
            symbol: symbol.to_string(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            qty_multiplier,
            last_seq: 0,
            initialized: false,
            last_system_ts_ns: None,
        }
    }

    #[inline(always)]
    fn seq_from_msg(msg: &MexcDepthMsg) -> u64 {
        msg.data
            .version
            .or(msg.data.end)
            .unwrap_or_else(|| msg.data.begin.unwrap_or(0))
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        let price = (px * self.price_scale).round() as Price;
        let qty = (qty * self.qty_multiplier * self.qty_scale).round() as Qty;
        (price, qty)
    }

    #[inline(always)]
    fn convert_levels(&self, levels: &[Vec<f64>]) -> Vec<(Price, Qty)> {
        levels
            .iter()
            .filter_map(|entry| {
                let px = entry.get(0)?;
                let qty = entry.get(1)?;
                Some(self.conv(*px, *qty))
            })
            .collect()
    }

    pub fn apply(&mut self, msg: &MexcDepthMsg) -> bool {
        let seq_val = Self::seq_from_msg(msg);
        if seq_val == 0 {
            return false;
        }
        let ts_ms = msg.ts.unwrap_or(0);
        let ts = ms_to_ns(ts_ms);
        self.last_system_ts_ns = Some(ts);
        let seq: Seq = seq_val as Seq;

        let bids = self.convert_levels(&msg.data.bids);
        let asks = self.convert_levels(&msg.data.asks);

        if !self.initialized {
            if bids.is_empty() || asks.is_empty() {
                return false;
            }
            self.book.refresh_from_levels(&asks, &bids, ts, seq);
            self.last_seq = seq_val;
            self.initialized = true;
            return true;
        }

        if seq_val <= self.last_seq {
            return false;
        }

        if let Some(begin) = msg.data.begin {
            if begin > 0 && begin != self.last_seq.saturating_add(1) {
                // Gap detected; drop update until a fresh snapshot arrives.
                return false;
            }
        }

        if !bids.is_empty() && !asks.is_empty() {
            self.book.update_full_batch(&asks, &bids, ts, seq);
        } else if !bids.is_empty() {
            self.book.update_bids_batch(&bids, ts, seq);
        } else if !asks.is_empty() {
            self.book.update_asks_batch(&asks, ts, seq);
        } else {
            // Empty delta (keep seq monotonic)
            self.book.ts = ts;
            self.book.seq = seq;
        }

        self.last_seq = seq_val;
        true
    }

    #[inline(always)]
    pub fn last_ts(&self) -> Ts {
        self.book.ts
    }

    #[inline(always)]
    pub fn mid_price_f64(&self) -> Option<f64> {
        OrderBookOps::mid_price_f64(self)
    }

    #[inline(always)]
    pub fn top_levels_f64(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        OrderBookOps::top_levels_f64(self, depth)
    }

    #[inline(always)]
    pub fn last_system_ts_ns(&self) -> Option<Ts> {
        self.last_system_ts_ns
    }
}

impl<const N: usize> OrderBookOps for MexcBook<N> {
    fn mid_price_f64(&self) -> Option<f64> {
        let bid = self.book.best_bid()?;
        let ask = self.book.best_ask()?;
        Some(((bid.px + ask.px) as f64) / (2.0 * self.price_scale))
    }

    fn top_levels_f64(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let mut bids = Vec::with_capacity(depth.min(self.book.len_bids()));
        let mut asks = Vec::with_capacity(depth.min(self.book.len_asks()));

        for lvl in self.book.iter_bids().take(depth) {
            bids.push((
                (lvl.px as f64) / self.price_scale,
                (lvl.qty as f64) / self.qty_scale,
            ));
        }

        for lvl in self.book.iter_asks().take(depth) {
            asks.push((
                (lvl.px as f64) / self.price_scale,
                (lvl.qty as f64) / self.qty_scale,
            ));
        }

        (bids, asks)
    }

    fn is_initialized(&self) -> bool {
        self.initialized && self.book.is_warmed_up()
    }

    fn is_empty(&self) -> bool {
        self.book.is_empty()
    }

    fn best_bid_f64(&self) -> Option<(f64, f64)> {
        self.book.best_bid().map(|lvl| {
            (
                (lvl.px as f64) / self.price_scale,
                (lvl.qty as f64) / self.qty_scale,
            )
        })
    }

    fn best_ask_f64(&self) -> Option<(f64, f64)> {
        self.book.best_ask().map(|lvl| {
            (
                (lvl.px as f64) / self.price_scale,
                (lvl.qty as f64) / self.qty_scale,
            )
        })
    }

    fn clear(&mut self) {
        self.book.clear();
        self.last_seq = 0;
        self.initialized = false;
        self.last_system_ts_ns = None;
    }
}
