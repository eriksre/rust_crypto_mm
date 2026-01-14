use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use crate::utils::time::ms_to_ns;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct LighterLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LighterOrderBookPayload {
    #[serde(default)]
    pub code: Option<i64>,
    #[serde(default)]
    pub asks: Vec<LighterLevel>,
    #[serde(default)]
    pub bids: Vec<LighterLevel>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub nonce: Option<u64>,
    #[serde(default)]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LighterOrderBookMsg {
    pub channel: String,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default, rename = "type")]
    pub msg_type: Option<String>,
    #[serde(default)]
    pub order_book: Option<LighterOrderBookPayload>,
    #[serde(default)]
    pub timestamp: Option<u64>,
}

pub struct LighterBook<const N: usize> {
    pub market_id: u32,
    book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    last_ts: Ts,
    last_offset: Option<u64>,
    initialized: bool,
}

impl<const N: usize> LighterBook<N> {
    pub fn new(market_id: u32, price_scale: f64, qty_scale: f64) -> Self {
        Self {
            market_id,
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            last_ts: 0,
            last_offset: None,
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

    #[inline(always)]
    fn channel_market_id(channel: &str) -> Option<u32> {
        channel
            .rsplit_once(':')
            .or_else(|| channel.rsplit_once('/'))
            .and_then(|(_, id)| id.parse::<u32>().ok())
    }

    #[inline(always)]
    fn ts_from_exchange(ts: u64) -> Ts {
        if ts > 1_000_000_000_000 {
            ms_to_ns(ts)
        } else {
            ts.saturating_mul(1_000_000_000)
        }
    }

    fn parse_levels(&self, lvls: &[LighterLevel]) -> Vec<(Price, Qty)> {
        lvls.iter()
            .filter_map(|lvl| {
                let px = lvl.price.parse::<f64>().ok()?;
                let qty = lvl.size.parse::<f64>().ok()?;
                Some(self.conv(px, qty))
            })
            .collect()
    }

    pub fn apply(&mut self, msg: &LighterOrderBookMsg) -> bool {
        if !msg.channel.starts_with("order_book") {
            return false;
        }
        if let Some(mid) = Self::channel_market_id(&msg.channel) {
            if mid != self.market_id {
                return false;
            }
        }
        let payload = match &msg.order_book {
            Some(p) => p,
            None => return false,
        };

        let msg_type = msg.msg_type.as_deref().unwrap_or("");
        let is_snapshot = !self.initialized
            || msg_type.starts_with("subscribed")
            || msg_type.starts_with("snapshot");

        let seq = payload.offset.or(msg.offset).or(payload.nonce).unwrap_or(0);
        if !is_snapshot {
            if let Some(prev) = self.last_offset {
                if seq != 0 && seq <= prev {
                    return false;
                }
            }
        } else {
            self.last_offset = None;
        }

        let ts = payload
            .timestamp
            .or(msg.timestamp)
            .map(Self::ts_from_exchange)
            .unwrap_or(self.last_ts);
        let seq_for_book = if seq == 0 { ts.max(1) } else { seq };

        let bids = self.parse_levels(&payload.bids);
        let asks = self.parse_levels(&payload.asks);

        if is_snapshot {
            self.book
                .refresh_from_levels(&asks, &bids, ts, seq_for_book as Seq);
            self.initialized = true;
        } else {
            if !bids.is_empty() && !asks.is_empty() {
                self.book
                    .update_full_batch(&asks, &bids, ts, seq_for_book as Seq);
            } else if !bids.is_empty() {
                self.book.update_bids_batch(&bids, ts, seq_for_book as Seq);
            } else if !asks.is_empty() {
                self.book.update_asks_batch(&asks, ts, seq_for_book as Seq);
            } else {
                return false;
            }
        }

        self.last_ts = ts;
        self.last_offset = Some(seq_for_book);
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

    #[inline(always)]
    pub fn best_bid_f64(&self) -> Option<(f64, f64)> {
        let b = self.book.best_bid()?;
        Some((
            (b.px as f64) / self.price_scale,
            (b.qty as f64) / self.qty_scale,
        ))
    }

    #[inline(always)]
    pub fn best_ask_f64(&self) -> Option<(f64, f64)> {
        let a = self.book.best_ask()?;
        Some((
            (a.px as f64) / self.price_scale,
            (a.qty as f64) / self.qty_scale,
        ))
    }

    #[inline(always)]
    pub fn price_scale(&self) -> f64 {
        self.price_scale
    }

    #[inline(always)]
    pub fn qty_scale(&self) -> f64 {
        self.qty_scale
    }
}

impl<const N: usize> OrderBookOps for LighterBook<N> {
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
        self.best_bid_f64()
    }

    #[inline(always)]
    fn best_ask_f64(&self) -> Option<(f64, f64)> {
        self.best_ask_f64()
    }

    #[inline(always)]
    fn clear(&mut self) {
        self.book.clear();
        self.initialized = false;
        self.last_ts = 0;
        self.last_offset = None;
    }
}
