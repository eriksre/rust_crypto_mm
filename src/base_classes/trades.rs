#![allow(dead_code)]

use crate::base_classes::types::*;

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Trade {
    pub px: Price,
    pub qty: Qty,
    pub ts: Ts,
    pub seq: Seq,
    pub is_buyer_maker: bool, // true if buyer was maker (taker sell)
    pub system_ts_ns: Option<Ts>,
}

impl Trade {
    #[inline(always)]
    pub const fn new(
        px: Price,
        qty: Qty,
        ts: Ts,
        seq: Seq,
        is_buyer_maker: bool,
        system_ts_ns: Option<Ts>,
    ) -> Self {
        Self {
            px,
            qty,
            ts,
            seq,
            is_buyer_maker,
            system_ts_ns,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FixedTrades<const N: usize> {
    buf: [Trade; N],
    head: usize, // next write index
    len: usize,
}

impl<const N: usize> Default for FixedTrades<N> {
    #[inline(always)]
    fn default() -> Self {
        Self {
            buf: [Trade::default(); N],
            head: 0,
            len: 0,
        }
    }
}

impl<const N: usize> FixedTrades<N> {
    #[inline(always)]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        N
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn push(&mut self, t: Trade) {
        self.buf[self.head] = t;
        self.head = (self.head + 1) % N;
        if self.len < N {
            self.len += 1;
        }
    }

    #[inline(always)]
    pub fn last(&self) -> Option<Trade> {
        if self.len == 0 {
            return None;
        }
        let idx = if self.head == 0 { N - 1 } else { self.head - 1 };
        Some(self.buf[idx])
    }

    // Iterate in chronological order over the last k trades (k <= len)
    pub fn iter_last(&self, mut k: usize) -> impl Iterator<Item = Trade> + '_ {
        if k > self.len {
            k = self.len;
        }
        let start = if self.head >= k {
            self.head - k
        } else {
            N + self.head - k
        };
        TradeIter {
            buf: &self.buf,
            pos: start,
            remaining: k,
        }
    }
}

struct TradeIter<'a, const N: usize> {
    buf: &'a [Trade; N],
    pos: usize,
    remaining: usize,
}

impl<'a, const N: usize> Iterator for TradeIter<'a, N> {
    type Item = Trade;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let item = self.buf[self.pos];
        self.pos = (self.pos + 1) % N;
        self.remaining -= 1;
        Some(item)
    }
}
