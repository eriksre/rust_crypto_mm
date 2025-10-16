#![allow(dead_code)]

#[cfg(feature = "bitget_book")]
use serde::Deserialize;

use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use crate::utils::time::ms_to_ns;

#[cfg(feature = "bitget_book")]
#[derive(Debug, Clone, Deserialize)]
pub struct BitgetArg {
    #[serde(rename = "instType")]
    pub inst_type: String,
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

#[cfg(feature = "bitget_book")]
#[derive(Debug, Clone, Deserialize)]
pub struct BitgetDatum {
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub seq: Option<u64>,
    #[serde(rename = "seqId", default)]
    pub seq_id: Option<u64>,
    #[serde(rename = "prevSeq", default)]
    pub prev_seq: Option<u64>,
    #[serde(rename = "prevSeqId", default)]
    pub prev_seq_id: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_ts_opt")]
    pub ts: Option<u64>,
}

#[cfg(feature = "bitget_book")]
fn deserialize_ts_opt<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Null => None,
        _ => None,
    })
}

#[cfg(feature = "bitget_book")]
#[derive(Debug, Clone, Deserialize)]
pub struct BitgetMsg {
    pub action: String,
    pub arg: BitgetArg,
    pub data: Vec<BitgetDatum>,
    #[serde(default, deserialize_with = "deserialize_ts_opt")]
    pub ts: Option<u64>,
}

pub struct BitgetBook<const N: usize> {
    pub inst_id: String,
    book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    pub last_seq: u64,
    initialized: bool,
    last_system_ts_ns: Option<Ts>,
    last_bbo_system_ts_ns: Option<Ts>,
}

impl<const N: usize> BitgetBook<N> {
    pub const PRICE_SCALE: f64 = 100_000.0;
    pub const QTY_SCALE: f64 = 1_000_000.0;

    pub fn new(inst_id: &str, price_scale: f64, qty_scale: f64) -> Self {
        Self {
            inst_id: inst_id.to_string(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            last_seq: 0,
            initialized: false,
            last_system_ts_ns: None,
            last_bbo_system_ts_ns: None,
        }
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        let p = (px * self.price_scale).round() as Price;
        let q = (qty * self.qty_scale).round() as Qty;
        (p, q)
    }

    #[inline(always)]
    fn convert_levels(&self, levels: &[Vec<String>]) -> Vec<(Price, Qty)> {
        levels
            .iter()
            .filter_map(|entry| {
                let px = entry.get(0)?.parse::<f64>().ok()?;
                let qty = entry.get(1)?.parse::<f64>().ok()?;
                Some(self.conv(px, qty))
            })
            .collect()
    }

    #[inline(always)]
    fn extract_seq(d: &BitgetDatum) -> Option<u64> {
        d.seq
            .or(d.seq_id)
            .or_else(|| d.prev_seq.map(|prev| prev.saturating_add(1)))
            .or_else(|| d.prev_seq_id.map(|prev| prev.saturating_add(1)))
    }

    #[inline(always)]
    fn extract_prev_seq(d: &BitgetDatum) -> Option<u64> {
        d.prev_seq.or(d.prev_seq_id)
    }

    pub fn apply(&mut self, msg: &BitgetMsg) -> bool {
        if msg.data.is_empty() {
            return false;
        }
        let d = &msg.data[0];
        let seq_val = Self::extract_seq(d).unwrap_or(0);
        let prev_seq = Self::extract_prev_seq(d);
        let ts_ms = d.ts.or(msg.ts).unwrap_or(0);
        let ts: Ts = ms_to_ns(ts_ms);
        self.last_system_ts_ns = msg.ts.map(ms_to_ns);
        let seq: Seq = seq_val as Seq;
        if msg.action == "snapshot" {
            let bids = self.convert_levels(&d.bids);
            let asks = self.convert_levels(&d.asks);
            self.book.refresh_from_levels(&asks, &bids, ts, seq);
            self.last_seq = seq_val;
            self.initialized = true;
            true
        } else if msg.action == "update" {
            if !self.initialized {
                return false;
            }
            if seq_val == 0 {
                return false;
            }
            if seq_val <= self.last_seq {
                return false;
            }
            if let Some(prev) = prev_seq {
                if prev != self.last_seq {
                    return false;
                }
            }
            let bids = self.convert_levels(&d.bids);
            let asks = self.convert_levels(&d.asks);
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
    pub fn apply_bbo(&mut self, msg: &BitgetMsg) -> bool {
        if msg.data.is_empty() || !self.initialized {
            return false;
        }
        let d = &msg.data[0];
        let seq_val = Self::extract_seq(d).unwrap_or(0);
        if seq_val == 0 || seq_val <= self.last_seq {
            return false;
        }
        if let Some(prev) = Self::extract_prev_seq(d) {
            if prev != self.last_seq {
                return false;
            }
        }
        let ts_ms = d.ts.or(msg.ts).unwrap_or(0);
        let ts: Ts = ms_to_ns(ts_ms);
        self.last_bbo_system_ts_ns = msg.ts.map(ms_to_ns);
        let seq: Seq = seq_val as Seq;

        let mut bids_iter = d.bids.iter();
        let mut asks_iter = d.asks.iter();
        let best_bid = bids_iter.next().and_then(|lvl| {
            let px = lvl.get(0)?.parse::<f64>().ok()?;
            let qty = lvl.get(1)?.parse::<f64>().ok()?;
            Some(self.conv(px, qty))
        });
        let best_ask = asks_iter.next().and_then(|lvl| {
            let px = lvl.get(0)?.parse::<f64>().ok()?;
            let qty = lvl.get(1)?.parse::<f64>().ok()?;
            Some(self.conv(px, qty))
        });

        if best_bid.is_none() && best_ask.is_none() {
            return false;
        }

        if let Some((bpx, bqty)) = best_bid {
            if let Some(current) = self.book.best_bid() {
                if bpx == current.px {
                    self.book.upsert_bid(bpx, bqty, ts, seq);
                } else if bpx > current.px {
                    self.book.upsert_bid(bpx, bqty, ts, seq);
                    self.book.trim_asks_at_or_below(bpx);
                }
            } else {
                self.book.upsert_bid(bpx, bqty, ts, seq);
            }
        }
        if let Some((apx, aqty)) = best_ask {
            if let Some(current) = self.book.best_ask() {
                if apx == current.px {
                    self.book.upsert_ask(apx, aqty, ts, seq);
                } else if apx < current.px {
                    self.book.upsert_ask(apx, aqty, ts, seq);
                    self.book.trim_bids_at_or_above(apx);
                }
            } else {
                self.book.upsert_ask(apx, aqty, ts, seq);
            }
        }
        self.last_seq = seq_val;
        true
    }

    #[inline(always)]
    pub fn mid_price_f64(&self) -> Option<f64> {
        let b = self.book.best_bid()?;
        let a = self.book.best_ask()?;
        Some(((b.px + a.px) as f64) / (2.0 * self.price_scale))
    }

    #[inline(always)]
    pub fn last_system_ts_ns(&self) -> Option<Ts> {
        self.last_system_ts_ns
    }

    #[inline(always)]
    pub fn last_bbo_system_ts_ns(&self) -> Option<Ts> {
        self.last_bbo_system_ts_ns
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

// Implement the generic OrderBookOps trait for BitgetBook
#[cfg(feature = "bitget_book")]
impl<const N: usize> OrderBookOps for BitgetBook<N> {
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
        self.last_system_ts_ns = None;
        self.last_bbo_system_ts_ns = None;
    }
}
