#![allow(dead_code)]

use crate::base_classes::order_book::ArrayOrderBook;
use crate::base_classes::orderbook_trait::OrderBookOps;
use crate::base_classes::types::*;
use crate::exchanges::binance::rest::{BinanceSnapshot, get_orderbook_snapshot};
use crate::utils::time::ms_to_ns;

#[cfg(feature = "parse_binance")]
use crate::exchanges::binance::parsed::DepthUpdate;

pub struct BinanceBook<const N: usize> {
    pub symbol: String, // lowercase
    book: ArrayOrderBook<N>,
    price_scale: f64,
    qty_scale: f64,
    pub last_update_id: u64,
    last_u: Option<u64>,
    first_valid_processed: bool,
}

impl<const N: usize> BinanceBook<N> {
    pub const PRICE_SCALE: f64 = 100_000.0;
    pub const QTY_SCALE: f64 = 1_000_000.0;

    pub fn new(symbol: &str, price_scale: f64, qty_scale: f64) -> Self {
        Self {
            symbol: symbol.to_lowercase(),
            book: ArrayOrderBook::new(),
            price_scale,
            qty_scale,
            last_update_id: 0,
            last_u: None,
            first_valid_processed: false,
        }
    }

    pub async fn init_from_rest(
        &mut self,
        limit: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snap: BinanceSnapshot = get_orderbook_snapshot(&self.symbol, limit).await?;
        self.last_update_id = snap.last_update_id;
        let ts = 0;
        let seq = self.last_update_id as Seq;
        let bids: Vec<(Price, Qty)> = snap
            .bids
            .into_iter()
            .take(N)
            .filter_map(
                |pair| match (pair[0].parse::<f64>(), pair[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                },
            )
            .collect();
        let asks: Vec<(Price, Qty)> = snap
            .asks
            .into_iter()
            .take(N)
            .filter_map(
                |pair| match (pair[0].parse::<f64>(), pair[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                },
            )
            .collect();
        self.book.refresh_from_levels(&asks, &bids, ts, seq);
        self.first_valid_processed = false;
        self.last_u = None;
        Ok(())
    }

    #[inline(always)]
    fn conv(&self, px: f64, qty: f64) -> (Price, Qty) {
        let p = (px * self.price_scale).round() as Price;
        let q = (qty * self.qty_scale).round() as Qty;
        (p, q)
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

    // Apply depthUpdate event; returns true if applied.
    #[cfg(feature = "parse_binance")]
    pub fn apply_depth_update(&mut self, d: &DepthUpdate) -> bool {
        // Drop if u < snapshot lastUpdateId
        if d.u <= self.last_update_id {
            return false;
        }

        if !self.first_valid_processed {
            let target = self.last_update_id.saturating_add(1);
            if d.U <= target && d.u >= target {
                self.first_valid_processed = true;
                self.last_u = Some(d.u);
            } else {
                return false; // wait for first valid event
            }
        } else {
            if let Some(prev_u) = self.last_u {
                let expected = prev_u.saturating_add(1);
                if let Some(pu) = d.pu {
                    if pu != prev_u {
                        return false;
                    }
                }
                if !(d.U <= expected && d.u >= expected) {
                    return false;
                }
            }
            self.last_u = Some(d.u);
        }

        self.last_update_id = d.u;

        let ts = ms_to_ns(d.E); // event time in ns
        let seq = d.u as Seq;
        let bids: Vec<(Price, Qty)> =
            d.b.iter()
                .filter_map(|p| match (p[0].parse::<f64>(), p[1].parse::<f64>()) {
                    (Ok(px), Ok(q)) => Some(self.conv(px, q)),
                    _ => None,
                })
                .collect();
        let asks: Vec<(Price, Qty)> =
            d.a.iter()
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
        true
    }
}

// Implement the generic OrderBookOps trait for BinanceBook
#[cfg(feature = "binance_book")]
impl<const N: usize> OrderBookOps for BinanceBook<N> {
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
        self.first_valid_processed
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
        self.first_valid_processed = false;
        self.last_update_id = 0;
        self.last_u = None;
    }
}
