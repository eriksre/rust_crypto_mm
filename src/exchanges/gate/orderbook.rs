#![allow(dead_code)]

use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use crate::utils::time::ms_to_ns;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GateResult {
    // Gate streams have used both short and long keys; accept both.
    #[serde(default)]
    pub b: Vec<[String; 2]>,
    #[serde(default)]
    pub a: Vec<[String; 2]>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
    #[serde(default)]
    pub t: Option<u64>,
    #[serde(default)]
    pub time_ms: Option<u64>,
    #[serde(default)]
    pub s: Option<String>,
    #[serde(default)]
    pub contract: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub seq: Option<u64>,
    // Uppercase depth-id sometimes used by Gate; map it without non_snake_case warning
    #[serde(default, rename = "U")]
    pub u_upper: Option<u64>,
    #[serde(default)]
    pub u: Option<u64>,
    #[serde(default)]
    pub full: Option<bool>,
}

impl GateResult {
    fn bids_ref(&self) -> &Vec<[String; 2]> {
        if !self.b.is_empty() {
            &self.b
        } else {
            &self.bids
        }
    }
    fn asks_ref(&self) -> &Vec<[String; 2]> {
        if !self.a.is_empty() {
            &self.a
        } else {
            &self.asks
        }
    }
    fn ts(&self, fallback: Option<u64>) -> u64 {
        self.t.or(self.time_ms).or(fallback).unwrap_or(0)
    }
    fn contract_name(&self) -> Option<&str> {
        if let Some(s) = self.contract.as_deref() {
            return Some(s);
        }
        if let Some(s) = self.symbol.as_deref() {
            return Some(s);
        }
        if let Some(s) = self.s.as_deref() {
            if let Some(rest) = s.strip_prefix("ob.") {
                return Some(rest.split('.').next().unwrap_or(rest));
            }
        }
        None
    }
    fn depth_id(&self) -> Option<u64> {
        self.u.or(self.u_upper)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GateMsg {
    pub channel: String,
    pub event: String,
    #[serde(default)]
    pub result: Option<GateResult>,
    #[serde(default)]
    pub time_ms: Option<u64>,
}

pub struct GateBook<const N: usize> {
    pub contract: String,
    book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    last_ts: u64,
    last_depth_id: Option<u64>,
    initialized: bool,
}

impl<const N: usize> GateBook<N> {
    pub const PRICE_SCALE: f64 = 100_000.0;
    pub const QTY_SCALE: f64 = 1_000_000.0;

    pub fn new(contract: &str, price_scale: f64, qty_scale: f64) -> Self {
        Self {
            contract: contract.to_string(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            last_ts: 0,
            last_depth_id: None,
            initialized: false,
        }
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        (
            (px * self.price_scale).round() as Price,
            (qty * self.qty_scale).round() as Qty,
        )
    }

    pub fn apply(&mut self, msg: &GateMsg) -> bool {
        if msg.channel != "futures.obu" {
            return false;
        }
        let res = match &msg.result {
            Some(r) => r,
            None => {
                return false;
            }
        };

        if let Some(contract) = res.contract_name() {
            if !contract.eq_ignore_ascii_case(&self.contract) {
                return false;
            }
        }

        let ts_ms = res.ts(msg.time_ms);
        let ts = ms_to_ns(ts_ms);
        if ts < self.last_ts {
            return false;
        }
        let seq = res.seq.unwrap_or(ts as u64) as Seq;

        // Snapshot vs delta
        let is_snapshot = res.full.unwrap_or(false);
        if is_snapshot {
            let bids: Vec<(Price, Qty)> = res
                .bids_ref()
                .iter()
                .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                })
                .collect();
            let asks: Vec<(Price, Qty)> = res
                .asks_ref()
                .iter()
                .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                })
                .collect();
            self.book.refresh_from_levels(&asks, &bids, ts, seq);
            self.initialized = true;
            self.last_ts = ts;
            self.last_depth_id = res.depth_id();
            return true;
        } else if !self.initialized {
            // We are not synchronized; ignore deltas until a proper snapshot arrives.
            return false;
        }

        // Delta continuity: relax Gate's 'U' continuity (updates may batch). Enforce non-decreasing 'u' when present.
        if let (Some(prev), Some(u_now)) = (self.last_depth_id, res.depth_id()) {
            if u_now < prev {
                return false;
            }
        }
        let bids: Vec<(Price, Qty)> = res
            .bids_ref()
            .iter()
            .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                _ => None,
            })
            .collect();
        let asks: Vec<(Price, Qty)> = res
            .asks_ref()
            .iter()
            .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                _ => None,
            })
            .collect();
        if !bids.is_empty() && !asks.is_empty() {
            self.book.update_full_batch(&asks, &bids, ts, seq);
        } else if !bids.is_empty() {
            self.book.update_bids_batch(&bids, ts, seq);
        } else if !asks.is_empty() {
            self.book.update_asks_batch(&asks, ts, seq);
        }
        self.last_ts = ts;
        self.last_depth_id = res.depth_id();
        true
    }

    #[inline(always)]
    pub fn mid_price_f64(&self) -> Option<f64> {
        let b = self.book.best_bid()?;
        let a = self.book.best_ask()?;
        Some(((b.px + a.px) as f64) / (2.0 * self.price_scale))
    }

    #[inline(always)]
    pub fn last_ts(&self) -> Ts {
        self.book.ts
    }

    #[inline(always)]
    pub fn top_levels_f64(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
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
}

// Implement the generic OrderBookOps trait for GateBook
impl<const N: usize> OrderBookOps for GateBook<N> {
    #[inline(always)]
    fn mid_price_f64(&self) -> Option<f64> {
        self.mid_price_f64()
    }

    #[inline(always)]
    fn top_levels_f64(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        self.top_levels_f64(depth)
    }

    #[inline(always)]
    fn is_initialized(&self) -> bool {
        self.initialized
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.book.is_empty()
    }

    #[inline(always)]
    fn best_bid_f64(&self) -> Option<(f64, f64)> {
        let b = self.book.best_bid()?;
        Some((
            (b.px as f64) / self.price_scale,
            (b.qty as f64) / self.qty_scale,
        ))
    }

    #[inline(always)]
    fn best_ask_f64(&self) -> Option<(f64, f64)> {
        let a = self.book.best_ask()?;
        Some((
            (a.px as f64) / self.price_scale,
            (a.qty as f64) / self.qty_scale,
        ))
    }

    #[inline(always)]
    fn clear(&mut self) {
        self.book.clear();
        self.initialized = false;
        self.last_ts = 0;
        self.last_depth_id = None;
    }
}
