#![allow(dead_code)]

use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct BybitData {
    #[serde(default)]
    pub b: Vec<[String; 2]>,
    #[serde(default)]
    pub a: Vec<[String; 2]>,
    #[serde(default)]
    pub u: u64,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub ts: u64,
    #[serde(default)]
    pub s: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BybitDataCont {
    Obj(BybitData),
    Arr(Vec<BybitData>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct BybitMsg {
    pub topic: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub data: BybitDataCont,
    #[serde(default)]
    pub ts: Option<u64>,
}

pub const PRICE_SCALE: f64 = 100_000.0; // preserve up to 1e-5 price precision
pub const QTY_SCALE: f64 = 1_000_000.0; // preserve up to 1e-6 contract precision

pub struct BybitBook<const N: usize> {
    pub symbol: String,
    pub book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    last_seq: u64,
    initialized: bool,
}

impl<const N: usize> BybitBook<N> {
    pub fn new(symbol: &str, price_scale: f64, qty_scale: f64) -> Self {
        Self {
            symbol: symbol.to_string(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            last_seq: 0,
            initialized: false,
        }
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        let price = (px * self.price_scale).round() as Price;
        let qty = (qty * self.qty_scale).round() as Qty;
        (price, qty)
    }

    pub fn apply(&mut self, msg: &BybitMsg) -> bool {
        let dref: &BybitData = match &msg.data {
            BybitDataCont::Obj(d) => d,
            BybitDataCont::Arr(v) => match v.get(0) {
                Some(d) => d,
                None => {
                    return false;
                }
            },
        };
        let ts: Ts = if dref.ts != 0 {
            dref.ts as Ts
        } else {
            msg.ts.unwrap_or(0) as Ts
        };
        let seq_val: u64 = dref.seq;
        let seq: Seq = seq_val as Seq;
        if msg.kind == "snapshot" {
            let bids: Vec<(Price, Qty)> = dref
                .b
                .iter()
                .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                })
                .collect();
            let asks: Vec<(Price, Qty)> = dref
                .a
                .iter()
                .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                })
                .collect();
            self.book.refresh_from_levels(&asks, &bids, ts, seq);
            self.last_seq = seq_val;
            self.initialized = true;
            true
        } else if msg.kind == "delta" {
            if !self.initialized {
                return false;
            }
            if seq_val < self.last_seq {
                return false;
            }
            let bids: Vec<(Price, Qty)> = dref
                .b
                .iter()
                .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                })
                .collect();
            let asks: Vec<(Price, Qty)> = dref
                .a
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
            self.last_seq = seq_val;
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn mid_price_f64(&self) -> Option<f64> {
        let b = self.book.best_bid()?;
        let a = self.book.best_ask()?;
        Some(((b.px + a.px) as f64) / (2.0 * self.price_scale))
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

impl<const N: usize> BybitBook<N> {
    // Update only the top-of-book using BBO (orderbook.1) semantics, mirroring Python update_bbo
    pub fn apply_bbo(
        &mut self,
        bid_px: f64,
        bid_sz: f64,
        ask_px: f64,
        ask_sz: f64,
        seq: u64,
        ts_ms: u64,
    ) -> bool {
        if !self.initialized {
            return false;
        }
        if seq <= self.last_seq {
            return false;
        }
        let ts: Ts = ts_ms as Ts;
        let seqn: Seq = seq as Seq;
        let (bpx, bqty) = self.conv(bid_px, bid_sz);
        let (apx, aqty) = self.conv(ask_px, ask_sz);

        // Mutate bid side top
        if let Some(best_b) = self.book.best_bid() {
            if bpx == best_b.px {
                self.book.upsert_bid(bpx, bqty, ts, seqn);
            } else if bpx > best_b.px {
                // Insert as new best and trim overlapping asks
                self.book.upsert_bid(bpx, bqty, ts, seqn);
                self.book.trim_asks_at_or_below(bpx);
            }
        } else {
            self.book.upsert_bid(bpx, bqty, ts, seqn);
        }

        // Mutate ask side top
        if let Some(best_a) = self.book.best_ask() {
            if apx == best_a.px {
                self.book.upsert_ask(apx, aqty, ts, seqn);
            } else if apx < best_a.px {
                self.book.upsert_ask(apx, aqty, ts, seqn);
                self.book.trim_bids_at_or_above(apx);
            }
        } else {
            self.book.upsert_ask(apx, aqty, ts, seqn);
        }

        self.last_seq = seq;
        true
    }
}

// Implement the generic OrderBookOps trait for BybitBook
impl<const N: usize> OrderBookOps for BybitBook<N> {
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
        self.last_seq = 0;
    }
}
