#![allow(dead_code)]

use std::collections::HashMap;

use crate::base_classes::types::Ts;

#[derive(Clone, Copy, Default, Debug)]
pub struct BboEntry {
    pub bid_px: f64,
    pub bid_qty: f64,
    pub ask_px: f64,
    pub ask_qty: f64,
    pub ts: Ts,
    pub system_ts_ns: Option<Ts>,
}

impl BboEntry {
    #[inline(always)]
    fn set(
        &mut self,
        bid_px: f64,
        bid_qty: f64,
        ask_px: f64,
        ask_qty: f64,
        ts: Ts,
        system_ts_ns: Option<Ts>,
    ) {
        self.bid_px = bid_px;
        self.bid_qty = bid_qty;
        self.ask_px = ask_px;
        self.ask_qty = ask_qty;
        self.ts = ts;
        self.system_ts_ns = system_ts_ns;
    }

    #[inline(always)]
    fn is_ready(&self) -> bool {
        self.bid_px > 0.0 && self.ask_px > 0.0
    }

    #[inline(always)]
    fn mid_price(&self) -> Option<f64> {
        if self.is_ready() {
            Some(0.5 * (self.bid_px + self.ask_px))
        } else {
            None
        }
    }
}

#[derive(Default, Debug)]
pub struct BboStore {
    by_symbol: HashMap<String, BboEntry>,
    last_symbol: Option<String>,
}

impl BboStore {
    #[inline(always)]
    pub fn update<S: Into<String>>(
        &mut self,
        symbol: S,
        bid_px: f64,
        bid_qty: f64,
        ask_px: f64,
        ask_qty: f64,
        ts: Ts,
        system_ts_ns: Option<Ts>,
    ) {
        let symbol = symbol.into();
        let entry = self.by_symbol.entry(symbol.clone()).or_default();
        entry.set(bid_px, bid_qty, ask_px, ask_qty, ts, system_ts_ns);
        self.last_symbol = Some(symbol);
    }

    #[inline(always)]
    pub fn get(&self, symbol: &str) -> Option<&BboEntry> {
        self.by_symbol.get(symbol)
    }

    #[inline(always)]
    pub fn is_ready_for(&self, symbol: &str) -> bool {
        self.get(symbol).map_or(false, BboEntry::is_ready)
    }

    #[inline(always)]
    pub fn mid_price_f64_for(&self, symbol: &str) -> Option<f64> {
        self.get(symbol).and_then(BboEntry::mid_price)
    }

    #[inline(always)]
    pub fn last_symbol(&self) -> Option<&str> {
        self.last_symbol.as_deref()
    }

    #[inline(always)]
    pub fn mid_price_f64(&self) -> Option<f64> {
        self.last_symbol()
            .and_then(|symbol| self.mid_price_f64_for(symbol))
    }
}
