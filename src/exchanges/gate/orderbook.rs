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
    // Gate OBU deltas use U=start depth id and u=end depth id.
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
    fn ts(&self, fallback: Option<u64>) -> Option<u64> {
        self.t.or(self.time_ms).or(fallback)
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
    fn depth_start_id(&self) -> Option<u64> {
        self.u_upper
    }

    fn depth_end_id(&self) -> Option<u64> {
        self.u
    }

    fn is_snapshot(&self) -> bool {
        self.full.unwrap_or(false)
    }

    fn parse_levels(
        &self,
        levels: &[[String; 2]],
        price_scale: f64,
        qty_scale: f64,
        qty_multiplier: f64,
        side: &str,
    ) -> Option<Vec<(Price, Qty)>> {
        let mut out = Vec::with_capacity(levels.len());
        for level in levels {
            let px = match level[0].parse::<f64>() {
                Ok(px) if px.is_finite() && px > 0.0 => px,
                Ok(px) => {
                    eprintln!(
                        "ERROR: Gate orderbook {side} level has invalid price {} for contract {:?}",
                        px, self.contract
                    );
                    return None;
                }
                Err(err) => {
                    eprintln!(
                        "ERROR: Gate orderbook failed to parse {side} price '{}' for contract {:?}: {err}",
                        level[0], self.contract
                    );
                    return None;
                }
            };
            let qty = match level[1].parse::<f64>() {
                Ok(qty) if qty.is_finite() && qty >= 0.0 => qty,
                Ok(qty) => {
                    eprintln!(
                        "ERROR: Gate orderbook {side} level has invalid qty {} for contract {:?}",
                        qty, self.contract
                    );
                    return None;
                }
                Err(err) => {
                    eprintln!(
                        "ERROR: Gate orderbook failed to parse {side} qty '{}' for contract {:?}: {err}",
                        level[1], self.contract
                    );
                    return None;
                }
            };
            out.push((
                (px * price_scale).round() as Price,
                (qty * qty_multiplier * qty_scale).round() as Qty,
            ));
        }
        Some(out)
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
    qty_multiplier: f64,
    last_ts: u64,
    last_depth_id: Option<u64>,
    initialized: bool,
}

impl<const N: usize> GateBook<N> {
    pub const PRICE_SCALE: f64 = 100_000.0;
    pub const QTY_SCALE: f64 = 1_000_000.0;

    pub fn new(contract: &str, price_scale: f64, qty_scale: f64, qty_multiplier: f64) -> Self {
        Self {
            contract: contract.to_string(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            qty_multiplier,
            last_ts: 0,
            last_depth_id: None,
            initialized: false,
        }
    }

    #[inline(always)]
    pub fn set_qty_multiplier(&mut self, qty_multiplier: f64) {
        self.qty_multiplier = qty_multiplier;
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        (
            (px * self.price_scale).round() as Price,
            (qty * self.qty_multiplier * self.qty_scale).round() as Qty,
        )
    }

    pub fn apply(&mut self, msg: &GateMsg) -> bool {
        if msg.channel != "futures.obu" || msg.event != "update" {
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

        let ts_ms = match res.ts(msg.time_ms) {
            Some(ts_ms) if ts_ms > 0 => ts_ms,
            _ => {
                eprintln!(
                    "ERROR: Gate orderbook missing valid timestamp for contract {}: {:?}",
                    self.contract, msg
                );
                return false;
            }
        };
        let ts = ms_to_ns(ts_ms);
        if ts < self.last_ts {
            return false;
        }
        let seq = match res.depth_end_id() {
            Some(seq) if seq > 0 => seq as Seq,
            _ => {
                eprintln!(
                    "ERROR: Gate orderbook missing valid depth end id (u) for contract {}: {:?}",
                    self.contract, msg
                );
                return false;
            }
        };

        let is_snapshot = res.is_snapshot();
        let bids = match res.parse_levels(
            res.bids_ref(),
            self.price_scale,
            self.qty_scale,
            self.qty_multiplier,
            "bid",
        ) {
            Some(bids) => bids,
            None => return false,
        };
        let asks = match res.parse_levels(
            res.asks_ref(),
            self.price_scale,
            self.qty_scale,
            self.qty_multiplier,
            "ask",
        ) {
            Some(asks) => asks,
            None => return false,
        };
        if is_snapshot {
            self.book.refresh_from_levels(&asks, &bids, ts, seq);
            self.initialized = true;
            self.last_ts = ts;
            self.last_depth_id = Some(seq);
            return true;
        } else if !self.initialized {
            // We are not synchronized; ignore deltas until a proper snapshot arrives.
            return false;
        }

        let depth_start = match res.depth_start_id() {
            Some(depth_start) if depth_start > 0 => depth_start,
            _ => {
                eprintln!(
                    "ERROR: Gate orderbook missing valid depth start id (U) for contract {}: {:?}",
                    self.contract, msg
                );
                return false;
            }
        };
        let prev_depth_end = match self.last_depth_id {
            Some(prev_depth_end) if prev_depth_end > 0 => prev_depth_end,
            _ => {
                eprintln!(
                    "ERROR: Gate orderbook lost synchronization state before delta for contract {}: {:?}",
                    self.contract, msg
                );
                self.book.clear();
                self.initialized = false;
                self.last_ts = 0;
                self.last_depth_id = None;
                return false;
            }
        };
        let expected_next = prev_depth_end.saturating_add(1);
        if depth_start > expected_next || (seq as u64) < expected_next {
            eprintln!(
                "ERROR: Gate orderbook depth gap for contract {}: prev_end={}, delta_start={}, delta_end={}; clearing local book and waiting for a fresh snapshot",
                self.contract, prev_depth_end, depth_start, seq
            );
            self.book.clear();
            self.initialized = false;
            self.last_ts = 0;
            self.last_depth_id = None;
            return false;
        }
        if (seq as u64) <= prev_depth_end {
            return false;
        }
        if !bids.is_empty() && !asks.is_empty() {
            self.book.update_full_batch(&asks, &bids, ts, seq);
        } else if !bids.is_empty() {
            self.book.update_bids_batch(&bids, ts, seq);
        } else if !asks.is_empty() {
            self.book.update_asks_batch(&asks, ts, seq);
        } else {
            self.book.update_full_batch(&[], &[], ts, seq);
        }
        self.last_ts = ts;
        self.last_depth_id = Some(seq);
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

#[cfg(test)]
mod tests {
    use crate::base_classes::orderbook_trait::OrderBookOps;

    use super::{GateBook, GateMsg, GateResult};

    fn base_result() -> GateResult {
        GateResult {
            b: vec![["100.0".to_string(), "1.0".to_string()]],
            a: vec![["101.0".to_string(), "1.5".to_string()]],
            bids: Vec::new(),
            asks: Vec::new(),
            t: Some(1_700_000_000_000),
            time_ms: None,
            s: None,
            contract: Some("BTC_USDT".to_string()),
            symbol: None,
            seq: None,
            u_upper: None,
            u: Some(10),
            full: Some(true),
        }
    }

    #[test]
    fn snapshot_uses_u_as_sequence_when_seq_is_absent() {
        let mut book = GateBook::<16>::new(
            "BTC_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );
        let msg = GateMsg {
            channel: "futures.obu".to_string(),
            event: "update".to_string(),
            result: Some(base_result()),
            time_ms: None,
        };

        assert!(book.apply(&msg));
        assert!(book.is_initialized());
        assert_eq!(book.best_bid_f64(), Some((100.0, 1.0)));
        assert_eq!(book.best_ask_f64(), Some((101.0, 1.5)));
    }

    #[test]
    fn delta_without_full_flag_is_applied_using_u_range() {
        let mut book = GateBook::<16>::new(
            "BTC_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );
        let snapshot = GateMsg {
            channel: "futures.obu".to_string(),
            event: "update".to_string(),
            result: Some(base_result()),
            time_ms: None,
        };
        assert!(book.apply(&snapshot));

        let mut result = base_result();
        result.b.clear();
        result.a = vec![["101.0".to_string(), "2.0".to_string()]];
        result.t = Some(1_700_000_000_100);
        result.u_upper = Some(11);
        result.u = Some(12);
        result.full = None;
        let msg = GateMsg {
            channel: "futures.obu".to_string(),
            event: "update".to_string(),
            result: Some(result),
            time_ms: None,
        };

        assert!(book.apply(&msg));
        assert_eq!(book.best_ask_f64(), Some((101.0, 2.0)));
        assert_eq!(book.last_ts(), 1_700_000_000_100_000_000);
    }

    #[test]
    fn subscribe_ack_is_ignored() {
        let mut book = GateBook::<16>::new(
            "BTC_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );
        let msg = GateMsg {
            channel: "futures.obu".to_string(),
            event: "subscribe".to_string(),
            result: Some(GateResult::default()),
            time_ms: Some(1_700_000_000_000),
        };

        assert!(!book.apply(&msg));
        assert!(!book.is_initialized());
    }

    #[test]
    fn depth_gap_clears_local_book_until_new_snapshot_arrives() {
        let mut book = GateBook::<16>::new(
            "BTC_USDT",
            GateBook::<16>::PRICE_SCALE,
            GateBook::<16>::QTY_SCALE,
            1.0,
        );
        let snapshot = GateMsg {
            channel: "futures.obu".to_string(),
            event: "update".to_string(),
            result: Some(base_result()),
            time_ms: None,
        };
        assert!(book.apply(&snapshot));

        let mut gap = base_result();
        gap.b.clear();
        gap.a.clear();
        gap.t = Some(1_700_000_000_050);
        gap.u_upper = Some(13);
        gap.u = Some(13);
        gap.full = None;
        let gap_msg = GateMsg {
            channel: "futures.obu".to_string(),
            event: "update".to_string(),
            result: Some(gap),
            time_ms: None,
        };

        assert!(!book.apply(&gap_msg));
        assert!(!book.is_initialized());
        assert!(book.is_empty());
    }
}
