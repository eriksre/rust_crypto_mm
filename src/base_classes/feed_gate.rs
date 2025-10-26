use crate::base_classes::types::Ts;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ExchangeFeed {
    Bybit,
    Binance,
    Gate,
    Bitget,
    Okx,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_accepts_non_decreasing_sequence() {
        let mut gate = FeedTimestampGate::new();
        assert_eq!(
            gate.evaluate(ExchangeFeed::Bybit, FeedKind::OrderBook, 1),
            GateDecision::Accept
        );
        assert_eq!(
            gate.evaluate(ExchangeFeed::Bybit, FeedKind::OrderBook, 1),
            GateDecision::Accept
        );
        assert_eq!(
            gate.evaluate(ExchangeFeed::Bybit, FeedKind::OrderBook, 2),
            GateDecision::Accept
        );
    }

    #[test]
    fn gate_rejects_decreasing_sequence() {
        let mut gate = FeedTimestampGate::new();
        assert_eq!(
            gate.evaluate(ExchangeFeed::Gate, FeedKind::Trades, 10),
            GateDecision::Accept
        );
        match gate.evaluate(ExchangeFeed::Gate, FeedKind::Trades, 5) {
            GateDecision::Reject {
                last_ts,
                reject_count,
            } => {
                assert_eq!(last_ts, 10);
                assert_eq!(reject_count, 1);
            }
            other => panic!("expected reject, got {:?}", other),
        }
        assert_eq!(
            gate.evaluate(ExchangeFeed::Gate, FeedKind::Trades, 12),
            GateDecision::Accept
        );
        match gate.evaluate(ExchangeFeed::Gate, FeedKind::Trades, 11) {
            GateDecision::Reject {
                last_ts,
                reject_count,
            } => {
                assert_eq!(last_ts, 12);
                assert_eq!(reject_count, 2);
            }
            other => panic!("expected reject, got {:?}", other),
        }
    }
}

impl ExchangeFeed {
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            ExchangeFeed::Bybit => "bybit",
            ExchangeFeed::Binance => "binance",
            ExchangeFeed::Gate => "gate",
            ExchangeFeed::Bitget => "bitget",
            ExchangeFeed::Okx => "okx",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FeedKind {
    OrderBook,
    Bbo,
    Trades,
}

impl FeedKind {
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            FeedKind::OrderBook => "orderbook",
            FeedKind::Bbo => "bbo",
            FeedKind::Trades => "trades",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FeedGateEntry {
    last_ts: Ts,
    rejected: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateDecision {
    Accept,
    Reject { last_ts: Ts, reject_count: u64 },
}

#[derive(Default, Debug)]
pub struct FeedTimestampGate {
    entries: HashMap<(ExchangeFeed, FeedKind), FeedGateEntry>,
}

impl FeedTimestampGate {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            entries: HashMap::with_capacity(12),
        }
    }

    #[inline(always)]
    pub fn evaluate(&mut self, exchange: ExchangeFeed, feed: FeedKind, ts: Ts) -> GateDecision {
        if ts == 0 {
            return GateDecision::Accept;
        }

        use std::collections::hash_map::Entry;
        let key = (exchange, feed);
        match self.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(FeedGateEntry {
                    last_ts: ts,
                    rejected: 0,
                });
                GateDecision::Accept
            }
            Entry::Occupied(mut entry) => {
                let data = entry.get_mut();
                if ts >= data.last_ts {
                    data.last_ts = ts;
                    GateDecision::Accept
                } else {
                    data.rejected = data.rejected.saturating_add(1);
                    GateDecision::Reject {
                        last_ts: data.last_ts,
                        reject_count: data.rejected,
                    }
                }
            }
        }
    }
}
