#![allow(dead_code)]

use std::ptr;

use crate::base_classes::bbo::Bbo;
use crate::base_classes::types::*;

#[repr(C)]
#[derive(Debug)]
pub struct ArrayOrderBook<const N: usize> {
    bids: [PriceLevel; N], // sorted by px desc
    asks: [PriceLevel; N], // sorted by px asc
    nbids: usize,
    nasks: usize,
    pub bbo: Bbo,
    pub ts: Ts,
    pub seq: Seq,
    warmed_up: bool,
}

impl<const N: usize> Default for ArrayOrderBook<N> {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> ArrayOrderBook<N> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            bids: [PriceLevel::new(0, 0); N],
            asks: [PriceLevel::new(0, 0); N],
            nbids: 0,
            nasks: 0,
            bbo: Bbo::default(),
            ts: 0,
            seq: 0,
            warmed_up: false,
        }
    }

    #[inline(always)]
    pub fn len_bids(&self) -> usize {
        self.nbids
    }

    #[inline(always)]
    pub fn len_asks(&self) -> usize {
        self.nasks
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.nbids == 0 || self.nasks == 0
    }

    #[inline(always)]
    pub fn best_bid(&self) -> Option<PriceLevel> {
        if self.nbids > 0 {
            Some(self.bids[0])
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn best_ask(&self) -> Option<PriceLevel> {
        if self.nasks > 0 {
            Some(self.asks[0])
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.nbids = 0;
        self.nasks = 0;
        self.bbo = Bbo::default();
        self.seq = 0;
        self.ts = 0;
        self.warmed_up = false;
    }

    #[inline(always)]
    pub fn is_warmed_up(&self) -> bool {
        self.warmed_up
    }

    // --- Bid side (descending by price) ---
    #[inline(always)]
    fn find_bid_index(&self, px: Price) -> Result<usize, usize> {
        self.bids[..self.nbids].binary_search_by(|lvl| lvl.px.cmp(&px).reverse())
    }

    #[inline(always)]
    fn bids_insert_at(&mut self, idx: usize, level: PriceLevel) {
        if self.nbids < N {
            // Make room by shifting right
            unsafe {
                ptr::copy(
                    self.bids.as_ptr().add(idx),
                    self.bids.as_mut_ptr().add(idx + 1),
                    self.nbids - idx,
                );
            }
            self.bids[idx] = level;
            self.nbids += 1;
        } else {
            // Book full. If idx >= N, drop it. Otherwise insert and drop the last.
            if idx < N {
                unsafe {
                    ptr::copy(
                        self.bids.as_ptr().add(idx),
                        self.bids.as_mut_ptr().add(idx + 1),
                        N - idx - 1,
                    );
                }
                self.bids[idx] = level;
            }
        }
    }

    #[inline(always)]
    fn bids_remove_at(&mut self, idx: usize) {
        if idx + 1 < self.nbids {
            unsafe {
                ptr::copy(
                    self.bids.as_ptr().add(idx + 1),
                    self.bids.as_mut_ptr().add(idx),
                    self.nbids - idx - 1,
                );
            }
        }
        if self.nbids > 0 {
            self.nbids -= 1;
        }
    }

    #[inline(always)]
    pub fn upsert_bid(&mut self, px: Price, qty: Qty, ts: Ts, seq: Seq) {
        self.ts = ts;
        self.seq = seq;
        match self.find_bid_index(px) {
            Ok(idx) => {
                if qty == 0 {
                    self.bids_remove_at(idx);
                } else {
                    self.bids[idx].qty = qty;
                }
            }
            Err(idx) => {
                if qty != 0 {
                    self.bids_insert_at(idx, PriceLevel::new(px, qty));
                }
            }
        }
        self.sync_bbo_after_bid();
    }

    #[inline(always)]
    pub fn remove_bid(&mut self, px: Price, ts: Ts, seq: Seq) {
        self.ts = ts;
        self.seq = seq;
        if let Ok(idx) = self.find_bid_index(px) {
            self.bids_remove_at(idx);
        }
        self.sync_bbo_after_bid();
    }

    #[inline(always)]
    fn sync_bbo_after_bid(&mut self) {
        if let Some(b) = self.best_bid() {
            self.bbo.update_bid(b.px, b.qty, self.ts, self.seq);
        } else {
            self.bbo.update_bid(0, 0, self.ts, self.seq);
        }
    }

    // --- Ask side (ascending by price) ---
    #[inline(always)]
    fn find_ask_index(&self, px: Price) -> Result<usize, usize> {
        self.asks[..self.nasks].binary_search_by(|lvl| lvl.px.cmp(&px))
    }

    #[inline(always)]
    fn asks_insert_at(&mut self, idx: usize, level: PriceLevel) {
        if self.nasks < N {
            unsafe {
                ptr::copy(
                    self.asks.as_ptr().add(idx),
                    self.asks.as_mut_ptr().add(idx + 1),
                    self.nasks - idx,
                );
            }
            self.asks[idx] = level;
            self.nasks += 1;
        } else {
            if idx < N {
                unsafe {
                    ptr::copy(
                        self.asks.as_ptr().add(idx),
                        self.asks.as_mut_ptr().add(idx + 1),
                        N - idx - 1,
                    );
                }
                self.asks[idx] = level;
            }
        }
    }

    #[inline(always)]
    fn asks_remove_at(&mut self, idx: usize) {
        if idx + 1 < self.nasks {
            unsafe {
                ptr::copy(
                    self.asks.as_ptr().add(idx + 1),
                    self.asks.as_mut_ptr().add(idx),
                    self.nasks - idx - 1,
                );
            }
        }
        if self.nasks > 0 {
            self.nasks -= 1;
        }
    }

    #[inline(always)]
    pub fn upsert_ask(&mut self, px: Price, qty: Qty, ts: Ts, seq: Seq) {
        self.ts = ts;
        self.seq = seq;
        match self.find_ask_index(px) {
            Ok(idx) => {
                if qty == 0 {
                    self.asks_remove_at(idx);
                } else {
                    self.asks[idx].qty = qty;
                }
            }
            Err(idx) => {
                if qty != 0 {
                    self.asks_insert_at(idx, PriceLevel::new(px, qty));
                }
            }
        }
        self.sync_bbo_after_ask();
    }

    #[inline(always)]
    pub fn remove_ask(&mut self, px: Price, ts: Ts, seq: Seq) {
        self.ts = ts;
        self.seq = seq;
        if let Ok(idx) = self.find_ask_index(px) {
            self.asks_remove_at(idx);
        }
        self.sync_bbo_after_ask();
    }

    #[inline(always)]
    fn sync_bbo_after_ask(&mut self) {
        if let Some(a) = self.best_ask() {
            self.bbo.update_ask(a.px, a.qty, self.ts, self.seq);
        } else {
            self.bbo.update_ask(0, 0, self.ts, self.seq);
        }
    }

    // --- Utilities ---
    #[inline(always)]
    pub fn iter_bids(&self) -> impl Iterator<Item = &PriceLevel> {
        self.bids[..self.nbids].iter()
    }

    #[inline(always)]
    pub fn iter_asks(&self) -> impl Iterator<Item = &PriceLevel> {
        self.asks[..self.nasks].iter()
    }

    // Remove asks whose price is <= px (used to resolve crossed books after bid updates)
    #[inline(always)]
    pub fn trim_asks_at_or_below(&mut self, px: Price) {
        while self.nasks > 0 && self.asks[0].px <= px {
            self.asks_remove_at(0);
        }
        self.sync_bbo_after_ask();
    }

    // Remove bids whose price is >= px (used to resolve crossed books after ask updates)
    #[inline(always)]
    pub fn trim_bids_at_or_above(&mut self, px: Price) {
        while self.nbids > 0 && self.bids[0].px >= px {
            self.bids_remove_at(0);
        }
        self.sync_bbo_after_bid();
    }

    // --- High-level batch operations (safe, exchange-agnostic) ---
    #[inline(always)]
    pub fn refresh_from_levels(
        &mut self,
        asks_in: &[(Price, Qty)],
        bids_in: &[(Price, Qty)],
        ts: Ts,
        seq: Seq,
    ) {
        self.nbids = 0;
        self.nasks = 0;
        self.ts = ts;
        self.seq = seq;

        // Fill bids descending
        let mut tmp_b: Vec<PriceLevel> = bids_in
            .iter()
            .filter(|(_, q)| *q != 0)
            .map(|(p, q)| PriceLevel::new(*p, *q))
            .collect();
        tmp_b.sort_unstable_by(|a, b| b.px.cmp(&a.px));
        self.nbids = tmp_b.len().min(N);
        for (i, lvl) in tmp_b.into_iter().take(self.nbids).enumerate() {
            self.bids[i] = lvl;
        }
        for slot in self.bids[self.nbids..].iter_mut() {
            *slot = PriceLevel::new(0, 0);
        }

        // Fill asks ascending
        let mut tmp_a: Vec<PriceLevel> = asks_in
            .iter()
            .filter(|(_, q)| *q != 0)
            .map(|(p, q)| PriceLevel::new(*p, *q))
            .collect();
        tmp_a.sort_unstable_by(|a, b| a.px.cmp(&b.px));
        self.nasks = tmp_a.len().min(N);
        for (i, lvl) in tmp_a.into_iter().take(self.nasks).enumerate() {
            self.asks[i] = lvl;
        }
        for slot in self.asks[self.nasks..].iter_mut() {
            *slot = PriceLevel::new(0, 0);
        }

        // Resolve any crossing
        if let (Some(b), Some(a)) = (self.best_bid(), self.best_ask()) {
            if b.px >= a.px {
                self.trim_asks_at_or_below(b.px);
                self.trim_bids_at_or_above(a.px);
            }
        }
        self.sync_bbo_after_bid();
        self.sync_bbo_after_ask();
        self.warmed_up = true;
    }

    #[inline(always)]
    pub fn update_bids_batch(&mut self, updates: &[(Price, Qty)], ts: Ts, seq: Seq) {
        if seq <= self.seq {
            return;
        }
        self.ts = ts;
        self.seq = seq;
        // Remove existing levels that are present in update
        for (px, _) in updates.iter() {
            if let Ok(idx) = self.find_bid_index(*px) {
                self.bids_remove_at(idx);
            }
        }
        // Insert new/updated levels
        for (px, qty) in updates.iter() {
            if *qty != 0 {
                self.upsert_bid(*px, *qty, ts, seq);
            }
        }
        // Resolve crossing
        if let Some(b) = self.best_bid() {
            self.trim_asks_at_or_below(b.px);
        }
        self.sync_bbo_after_bid();
        self.sync_bbo_after_ask();
        self.warmed_up = self.nbids > 0 && self.nasks > 0;
    }

    #[inline(always)]
    pub fn update_asks_batch(&mut self, updates: &[(Price, Qty)], ts: Ts, seq: Seq) {
        if seq <= self.seq {
            return;
        }
        self.ts = ts;
        self.seq = seq;
        for (px, _) in updates.iter() {
            if let Ok(idx) = self.find_ask_index(*px) {
                self.asks_remove_at(idx);
            }
        }
        for (px, qty) in updates.iter() {
            if *qty != 0 {
                self.upsert_ask(*px, *qty, ts, seq);
            }
        }
        if let Some(a) = self.best_ask() {
            self.trim_bids_at_or_above(a.px);
        }
        self.sync_bbo_after_bid();
        self.sync_bbo_after_ask();
        self.warmed_up = self.nbids > 0 && self.nasks > 0;
    }

    #[inline(always)]
    pub fn update_full_batch(
        &mut self,
        asks: &[(Price, Qty)],
        bids: &[(Price, Qty)],
        ts: Ts,
        seq: Seq,
    ) {
        if seq <= self.seq {
            return;
        }
        let prev_seq = self.seq;
        self.update_bids_batch(bids, ts, seq);
        // Restore previous seq so ask batch is not skipped; ts will be overwritten inside call
        self.seq = prev_seq;
        self.update_asks_batch(asks, ts, seq);
        self.seq = seq;
    }
}
