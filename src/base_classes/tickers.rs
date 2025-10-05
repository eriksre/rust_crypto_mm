#![allow(dead_code)]

use std::collections::HashMap;

use crate::base_classes::bbo::Bbo;
use crate::base_classes::trades::Trade;
use crate::base_classes::types::*;

// Minimal ticker representation suitable for HFT streaming
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Ticker {
    pub last_px: Price,
    pub last_qty: Qty,
    pub best_bid: Price,
    pub best_ask: Price,
    pub ts: Ts,
    pub seq: Seq,
}

impl Ticker {
    #[inline(always)]
    pub fn apply_trade(&mut self, t: &Trade) {
        self.last_px = t.px;
        self.last_qty = t.qty;
        self.ts = t.ts;
        self.seq = t.seq;
    }

    #[inline(always)]
    pub fn apply_bbo(&mut self, b: &Bbo) {
        self.best_bid = b.bid_px;
        self.best_ask = b.ask_px;
        self.ts = b.ts;
        self.seq = b.seq;
    }

    #[inline(always)]
    pub fn spread(&self) -> Price {
        self.best_ask - self.best_bid
    }

    #[inline(always)]
    pub fn mid_px(&self) -> Price {
        (self.best_ask + self.best_bid) / 2
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct TickerSnapshot {
    pub ticker: Ticker,
    pub mark_px: Option<f64>,
    pub index_px: Option<f64>,
    pub funding_rate: Option<f64>,
    pub turnover_24h: Option<f64>,
    pub open_interest: Option<f64>,
    pub open_interest_value: Option<f64>,
    pub quanto_multiplier: Option<f64>,
}

#[derive(Default, Debug)]
pub struct TickerStore {
    entries: HashMap<String, TickerSnapshot>,
    last_symbol: Option<String>,
}

impl TickerStore {
    #[inline(always)]
    pub fn update<S: Into<String>>(
        &mut self,
        symbol: S,
        mut snapshot: TickerSnapshot,
    ) -> TickerSnapshot {
        let symbol = symbol.into();
        if snapshot.ticker.seq == 0 {
            if let Some(prev) = self.entries.get(&symbol) {
                snapshot.ticker.seq = prev.ticker.seq.wrapping_add(1);
            } else {
                snapshot.ticker.seq = 1;
            }
        }
        if snapshot.ticker.ts == 0 {
            if let Some(prev) = self.entries.get(&symbol) {
                snapshot.ticker.ts = prev.ticker.ts;
            }
        }
        self.entries.insert(symbol.clone(), snapshot);
        self.last_symbol = Some(symbol);
        snapshot
    }

    #[inline(always)]
    pub fn get(&self, symbol: &str) -> Option<&TickerSnapshot> {
        self.entries.get(symbol)
    }

    #[inline(always)]
    pub fn last(&self) -> Option<&TickerSnapshot> {
        self.last_symbol
            .as_deref()
            .and_then(|symbol| self.entries.get(symbol))
    }

    #[inline(always)]
    pub fn last_symbol(&self) -> Option<&str> {
        self.last_symbol.as_deref()
    }
}
