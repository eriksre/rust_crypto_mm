#![allow(dead_code)]

use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use crate::utils::time::ms_to_ns;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct OkxArg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OkxDatum {
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    #[serde(rename = "seqId", default, deserialize_with = "deserialize_seq_opt")]
    pub seq_id: Option<u64>,
    #[serde(
        rename = "prevSeqId",
        default,
        deserialize_with = "deserialize_seq_opt"
    )]
    pub prev_seq_id: Option<u64>,
    #[serde(default)]
    pub checksum: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_ts_opt")]
    pub ts: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OkxMsg {
    pub arg: OkxArg,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub data: Vec<OkxDatum>,
}

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

fn deserialize_seq_opt<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Some(u)
            } else {
                None
            }
        }
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Null => None,
        _ => None,
    })
}

pub struct OkxBook<const N: usize> {
    pub inst_id: String,
    book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    last_books_seq: u64,
    last_bbo_seq: u64,
    initialized: bool,
    last_system_ts_ns: Option<Ts>,
    last_bbo_system_ts_ns: Option<Ts>,
    last_checksum: Option<i64>,
}

impl<const N: usize> OkxBook<N> {
    pub const PRICE_SCALE: f64 = 100_000.0;
    pub const QTY_SCALE: f64 = 1_000_000.0;

    pub fn new(inst_id: &str, price_scale: f64, qty_scale: f64) -> Self {
        Self {
            inst_id: inst_id.to_string(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            last_books_seq: 0,
            last_bbo_seq: 0,
            initialized: false,
            last_system_ts_ns: None,
            last_bbo_system_ts_ns: None,
            last_checksum: None,
        }
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        let price = (px * self.price_scale).round() as Price;
        let qty = (qty * self.qty_scale).round() as Qty;
        (price, qty)
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
    fn extract_seq(d: &OkxDatum) -> Option<u64> {
        d.seq_id
    }

    #[inline(always)]
    fn extract_prev_seq(d: &OkxDatum) -> Option<u64> {
        d.prev_seq_id
    }

    pub fn apply(&mut self, msg: &OkxMsg) -> bool {
        if msg.data.is_empty() {
            return false;
        }
        let datum = &msg.data[0];
        let seq_val = Self::extract_seq(datum).unwrap_or(0);
        if seq_val == 0 {
            return false;
        }
        let ts_ms = datum.ts.unwrap_or(0);
        let ts = ms_to_ns(ts_ms);
        self.last_system_ts_ns = datum.ts.map(ms_to_ns);
        let seq: Seq = seq_val as Seq;
        let asks = self.convert_levels(&datum.asks);
        let bids = self.convert_levels(&datum.bids);
        let action = msg.action.as_deref().unwrap_or("snapshot");

        match action {
            "snapshot" => {
                self.book.refresh_from_levels(&asks, &bids, ts, seq);
                self.last_books_seq = seq_val;
                self.initialized = true;
                self.last_checksum = datum.checksum;
                true
            }
            "update" => {
                if !self.initialized {
                    return false;
                }
                if seq_val <= self.last_books_seq {
                    return false;
                }
                if let Some(prev) = Self::extract_prev_seq(datum) {
                    if prev != self.last_books_seq {
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
                    // No depth changes but still advance sequence
                    self.book.update_full_batch(&[], &[], ts, seq);
                }
                self.last_books_seq = seq_val;
                self.last_checksum = datum.checksum;
                true
            }
            _ => false,
        }
    }

    pub fn apply_bbo(&mut self, msg: &OkxMsg) -> bool {
        if msg.data.is_empty() || !self.initialized {
            return false;
        }
        let datum = &msg.data[0];
        let seq_val = Self::extract_seq(datum).unwrap_or(0);
        if seq_val == 0 || seq_val <= self.last_bbo_seq {
            return false;
        }
        if let Some(prev) = Self::extract_prev_seq(datum) {
            if prev != self.last_bbo_seq {
                return false;
            }
        }
        let ts_ms = datum.ts.unwrap_or(0);
        let ts = ms_to_ns(ts_ms);
        self.last_bbo_system_ts_ns = datum.ts.map(ms_to_ns);
        let seq: Seq = seq_val as Seq;

        let best_bid = datum.bids.iter().find_map(|lvl| {
            let px = lvl.get(0)?.parse::<f64>().ok()?;
            let qty = lvl.get(1)?.parse::<f64>().ok()?;
            Some(self.conv(px, qty))
        });
        let best_ask = datum.asks.iter().find_map(|lvl| {
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

        self.last_bbo_seq = seq_val;
        true
    }

    #[inline(always)]
    pub fn mid_price_f64(&self) -> Option<f64> {
        let bid = self.book.best_bid()?;
        let ask = self.book.best_ask()?;
        Some(((bid.px + ask.px) as f64) / (2.0 * self.price_scale))
    }

    #[inline(always)]
    pub fn last_ts(&self) -> Ts {
        self.book.ts
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
    pub fn last_seq(&self) -> u64 {
        self.last_books_seq
    }

    #[inline(always)]
    pub fn last_checksum(&self) -> Option<i64> {
        self.last_checksum
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

impl<const N: usize> OrderBookOps for OkxBook<N> {
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
        let bid = self.book.best_bid()?;
        Some((
            (bid.px as f64) / self.price_scale,
            (bid.qty as f64) / self.qty_scale,
        ))
    }

    #[inline(always)]
    fn best_ask_f64(&self) -> Option<(f64, f64)> {
        let ask = self.book.best_ask()?;
        Some((
            (ask.px as f64) / self.price_scale,
            (ask.qty as f64) / self.qty_scale,
        ))
    }

    #[inline(always)]
    fn clear(&mut self) {
        self.book.clear();
        self.initialized = false;
        self.last_books_seq = 0;
        self.last_bbo_seq = 0;
        self.last_system_ts_ns = None;
        self.last_bbo_system_ts_ns = None;
        self.last_checksum = None;
    }
}
