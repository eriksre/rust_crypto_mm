//! Reference price publisher
//!
//! Selects the best reference price from multiple exchanges and publishes updates.
//! Prioritizes Gate.io prices, then adjusts prices from other exchanges using demean offsets.

use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;

use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::state::{ExchangeAdjustment, GlobalState, TradeDirection, state};
use crate::pricing::{LighterPricingModel, PricingModelConfig, PricingObservation};

/// Publishes reference price events by selecting the best candidate from all exchanges.
pub struct ReferencePublisher {
    tx: Option<UnboundedSender<ReferenceEvent>>,
    last_key: Option<RevisionKey>,
    channel_closed: bool,
    model: Option<LighterPricingModel>,
}

impl ReferencePublisher {
    /// Creates a new reference publisher.
    /// If `tx` is None, the publisher is a no-op (useful for testing).
    #[inline]
    pub fn new(
        tx: Option<UnboundedSender<ReferenceEvent>>,
        model_cfg: Option<PricingModelConfig>,
    ) -> Self {
        let model = model_cfg.and_then(|cfg| {
            if cfg.enabled {
                Some(LighterPricingModel::new(cfg))
            } else {
                None
            }
        });
        Self {
            tx,
            last_key: None,
            channel_closed: false,
            model,
        }
    }

    /// Publishes a reference price event if a new candidate is available.
    /// Silently ignores if no channel is configured or if state lock fails.
    #[inline]
    pub fn publish(&mut self) {
        if self.channel_closed {
            return;
        }
        let tx = match &self.tx {
            Some(tx) => tx,
            None => return,
        };

        let candidate = {
            let guard = state().lock();
            match guard {
                Ok(st) => Self::select_candidate(&st),
                Err(poisoned) => {
                    // LOUD FAILURE: State lock poisoned - this should never happen
                    eprintln!(
                        "FATAL: State lock poisoned in ReferencePublisher: {}",
                        poisoned
                    );
                    panic!("State lock poisoned - cannot continue safely");
                }
            }
        };

        let Some((candidate, key)) = candidate else {
            return;
        };

        // Skip if we already published this exact revision
        if self.last_key.as_ref() == Some(&key) {
            return;
        }

        self.last_key = Some(key);

        let mut price = candidate.price;
        let mut best_bid = candidate.best_bid;
        let mut best_ask = candidate.best_ask;
        let mut source = candidate.source.clone();
        let mut ts_ns = candidate.ts_ns;

        if let Some(model) = self.model.as_mut() {
            let obs = PricingObservation {
                exchange: candidate.exchange.to_string(),
                feed: candidate.feed.to_string(),
                price: candidate.price,
                bid_levels: candidate.bid_levels,
                ask_levels: candidate.ask_levels,
                wire_ts_ns: candidate.ts_ns,
                source_engine_ts_ns: candidate.source_engine_ts_ns,
                source_system_ts_ns: candidate.source_system_ts_ns,
                direction: candidate.direction,
                size: candidate.size,
            };
            if let Some(out) = model.update(&obs) {
                price = out.fair_mid;
                best_bid = out.quote_bid;
                best_ask = out.quote_ask;
                source = format!("model:{}", candidate.source);
                ts_ns = candidate.ts_ns;
            }
        }

        let received_at = match candidate.received_at {
            Some(received_at) => received_at,
            None => {
                eprintln!(
                    "ERROR: refusing to publish reference event without received_at: source={} seq={} ts_ns={:?}",
                    candidate.source, candidate.seq, candidate.ts_ns
                );
                return;
            }
        };

        let event = ReferenceEvent {
            price,
            best_bid,
            best_ask,
            ts_ns,
            source,
            received_at,
        };

        if let Err(err) = tx.send(event) {
            eprintln!("ERROR: Failed to publish reference event: {}", err);
            // Avoid spamming: mark closed and disable further sends.
            self.channel_closed = true;
            self.tx = None;
        }
    }

    /// Selects the best reference price candidate from global state.
    /// Considers all exchanges with priority to Gate.io (no adjustment).
    fn select_candidate(st: &GlobalState) -> Option<(Candidate, RevisionKey)> {
        let mut best: Option<Candidate> = None;

        let mut consider =
            |price: Option<f64>,
             best_bid: Option<f64>,
             best_ask: Option<f64>,
             bid_levels: [Option<(f64, f64)>; crate::base_classes::state::SNAPSHOT_DEPTH],
             ask_levels: [Option<(f64, f64)>; crate::base_classes::state::SNAPSHOT_DEPTH],
             direction: Option<TradeDirection>,
             size: Option<f64>,
             seq: u64,
             ts: Option<u64>,
             source_engine_ts_ns: Option<u64>,
             source_system_ts_ns: Option<u64>,
             idx: u8,
             source: String,
             exchange: &'static str,
             feed: &'static str,
             received_at: Option<Instant>| {
                // Validate inputs
                if seq == 0 {
                    return;
                }

                let Some(px) = price else {
                    return;
                };

                // LOUD FAILURE: Invalid price detected
                if !px.is_finite() || px <= 0.0 {
                    eprintln!(
                        "WARNING: Invalid price from {}: {} (seq={})",
                        source, px, seq
                    );
                    return;
                }

                let cand = Candidate {
                    price: px,
                    best_bid: best_bid.filter(|b| b.is_finite() && *b > 0.0),
                    best_ask: best_ask.filter(|a| a.is_finite() && *a > 0.0),
                    bid_levels,
                    ask_levels,
                    direction,
                    size,
                    seq,
                    ts_ns: ts,
                    source_engine_ts_ns,
                    source_system_ts_ns,
                    source_idx: idx,
                    source,
                    exchange,
                    feed,
                    received_at,
                };

                if let Some(current) = &best {
                    if Self::is_newer(&cand, current) {
                        best = Some(cand);
                    }
                } else {
                    best = Some(cand);
                }
            };

        // Gate.io sources (no adjustment needed)
        consider(
            st.gate.bbo.price,
            st.gate.bbo.bid_levels[0].map(|lvl| lvl.0),
            st.gate.bbo.ask_levels[0].map(|lvl| lvl.0),
            st.gate.bbo.bid_levels,
            st.gate.bbo.ask_levels,
            st.gate.bbo.direction,
            st.gate.bbo.size,
            st.gate.bbo.seq,
            st.gate.bbo.ts_ns,
            st.gate.bbo.source_engine_ts_ns,
            st.gate.bbo.source_system_ts_ns,
            0,
            "gate_bbo".to_string(),
            "gate",
            "bbo",
            st.gate.bbo.received_at,
        );
        consider(
            st.gate.orderbook.price,
            st.gate.orderbook.bid_levels[0].map(|lvl| lvl.0),
            st.gate.orderbook.ask_levels[0].map(|lvl| lvl.0),
            st.gate.orderbook.bid_levels,
            st.gate.orderbook.ask_levels,
            st.gate.orderbook.direction,
            st.gate.orderbook.size,
            st.gate.orderbook.seq,
            st.gate.orderbook.ts_ns,
            st.gate.orderbook.source_engine_ts_ns,
            st.gate.orderbook.source_system_ts_ns,
            1,
            "gate_ob".to_string(),
            "gate",
            "orderbook",
            st.gate.orderbook.received_at,
        );
        consider(
            st.gate.trade.price,
            None,
            None,
            st.gate.trade.bid_levels,
            st.gate.trade.ask_levels,
            st.gate.trade.direction,
            st.gate.trade.size,
            st.gate.trade.seq,
            st.gate.trade.ts_ns,
            st.gate.trade.source_engine_ts_ns,
            st.gate.trade.source_system_ts_ns,
            2,
            "gate_trade".to_string(),
            "gate",
            "trade",
            st.gate.trade.received_at,
        );

        // Bybit sources (adjusted)
        consider(
            Self::adjust_price(st.bybit.bbo.price, &st.demean.bybit),
            Self::adjust_price(
                st.bybit.bbo.bid_levels[0].map(|lvl| lvl.0),
                &st.demean.bybit,
            ),
            Self::adjust_price(
                st.bybit.bbo.ask_levels[0].map(|lvl| lvl.0),
                &st.demean.bybit,
            ),
            Self::adjust_levels(st.bybit.bbo.bid_levels, &st.demean.bybit),
            Self::adjust_levels(st.bybit.bbo.ask_levels, &st.demean.bybit),
            st.bybit.bbo.direction,
            st.bybit.bbo.size,
            st.bybit.bbo.seq,
            st.bybit.bbo.ts_ns,
            st.bybit.bbo.source_engine_ts_ns,
            st.bybit.bbo.source_system_ts_ns,
            3,
            Self::label("bybit_bbo", &st.demean.bybit),
            "bybit",
            "bbo",
            st.bybit.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.bybit.trade.price, &st.demean.bybit),
            None,
            None,
            Self::adjust_levels(st.bybit.trade.bid_levels, &st.demean.bybit),
            Self::adjust_levels(st.bybit.trade.ask_levels, &st.demean.bybit),
            st.bybit.trade.direction,
            st.bybit.trade.size,
            st.bybit.trade.seq,
            st.bybit.trade.ts_ns,
            st.bybit.trade.source_engine_ts_ns,
            st.bybit.trade.source_system_ts_ns,
            4,
            Self::label("bybit_trade", &st.demean.bybit),
            "bybit",
            "trade",
            st.bybit.trade.received_at,
        );

        // Binance sources (adjusted)
        consider(
            Self::adjust_price(st.binance.bbo.price, &st.demean.binance),
            Self::adjust_price(
                st.binance.bbo.bid_levels[0].map(|lvl| lvl.0),
                &st.demean.binance,
            ),
            Self::adjust_price(
                st.binance.bbo.ask_levels[0].map(|lvl| lvl.0),
                &st.demean.binance,
            ),
            Self::adjust_levels(st.binance.bbo.bid_levels, &st.demean.binance),
            Self::adjust_levels(st.binance.bbo.ask_levels, &st.demean.binance),
            st.binance.bbo.direction,
            st.binance.bbo.size,
            st.binance.bbo.seq,
            st.binance.bbo.ts_ns,
            st.binance.bbo.source_engine_ts_ns,
            st.binance.bbo.source_system_ts_ns,
            5,
            Self::label("binance_bbo", &st.demean.binance),
            "binance",
            "bbo",
            st.binance.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.binance.trade.price, &st.demean.binance),
            None,
            None,
            Self::adjust_levels(st.binance.trade.bid_levels, &st.demean.binance),
            Self::adjust_levels(st.binance.trade.ask_levels, &st.demean.binance),
            st.binance.trade.direction,
            st.binance.trade.size,
            st.binance.trade.seq,
            st.binance.trade.ts_ns,
            st.binance.trade.source_engine_ts_ns,
            st.binance.trade.source_system_ts_ns,
            6,
            Self::label("binance_trade", &st.demean.binance),
            "binance",
            "trade",
            st.binance.trade.received_at,
        );

        // Bitget sources (adjusted)
        consider(
            Self::adjust_price(st.bitget.bbo.price, &st.demean.bitget),
            Self::adjust_price(
                st.bitget.bbo.bid_levels[0].map(|lvl| lvl.0),
                &st.demean.bitget,
            ),
            Self::adjust_price(
                st.bitget.bbo.ask_levels[0].map(|lvl| lvl.0),
                &st.demean.bitget,
            ),
            Self::adjust_levels(st.bitget.bbo.bid_levels, &st.demean.bitget),
            Self::adjust_levels(st.bitget.bbo.ask_levels, &st.demean.bitget),
            st.bitget.bbo.direction,
            st.bitget.bbo.size,
            st.bitget.bbo.seq,
            st.bitget.bbo.ts_ns,
            st.bitget.bbo.source_engine_ts_ns,
            st.bitget.bbo.source_system_ts_ns,
            7,
            Self::label("bitget_bbo", &st.demean.bitget),
            "bitget",
            "bbo",
            st.bitget.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.bitget.trade.price, &st.demean.bitget),
            None,
            None,
            Self::adjust_levels(st.bitget.trade.bid_levels, &st.demean.bitget),
            Self::adjust_levels(st.bitget.trade.ask_levels, &st.demean.bitget),
            st.bitget.trade.direction,
            st.bitget.trade.size,
            st.bitget.trade.seq,
            st.bitget.trade.ts_ns,
            st.bitget.trade.source_engine_ts_ns,
            st.bitget.trade.source_system_ts_ns,
            8,
            Self::label("bitget_trade", &st.demean.bitget),
            "bitget",
            "trade",
            st.bitget.trade.received_at,
        );

        // OKX sources (adjusted)
        consider(
            Self::adjust_price(st.okx.bbo.price, &st.demean.okx),
            Self::adjust_price(st.okx.bbo.bid_levels[0].map(|lvl| lvl.0), &st.demean.okx),
            Self::adjust_price(st.okx.bbo.ask_levels[0].map(|lvl| lvl.0), &st.demean.okx),
            Self::adjust_levels(st.okx.bbo.bid_levels, &st.demean.okx),
            Self::adjust_levels(st.okx.bbo.ask_levels, &st.demean.okx),
            st.okx.bbo.direction,
            st.okx.bbo.size,
            st.okx.bbo.seq,
            st.okx.bbo.ts_ns,
            st.okx.bbo.source_engine_ts_ns,
            st.okx.bbo.source_system_ts_ns,
            9,
            Self::label("okx_bbo", &st.demean.okx),
            "okx",
            "bbo",
            st.okx.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.okx.trade.price, &st.demean.okx),
            None,
            None,
            Self::adjust_levels(st.okx.trade.bid_levels, &st.demean.okx),
            Self::adjust_levels(st.okx.trade.ask_levels, &st.demean.okx),
            st.okx.trade.direction,
            st.okx.trade.size,
            st.okx.trade.seq,
            st.okx.trade.ts_ns,
            st.okx.trade.source_engine_ts_ns,
            st.okx.trade.source_system_ts_ns,
            10,
            Self::label("okx_trade", &st.demean.okx),
            "okx",
            "trade",
            st.okx.trade.received_at,
        );

        // MEXC sources (adjusted)
        consider(
            Self::adjust_price(st.mexc.bbo.price, &st.demean.mexc),
            Self::adjust_price(st.mexc.bbo.bid_levels[0].map(|lvl| lvl.0), &st.demean.mexc),
            Self::adjust_price(st.mexc.bbo.ask_levels[0].map(|lvl| lvl.0), &st.demean.mexc),
            Self::adjust_levels(st.mexc.bbo.bid_levels, &st.demean.mexc),
            Self::adjust_levels(st.mexc.bbo.ask_levels, &st.demean.mexc),
            st.mexc.bbo.direction,
            st.mexc.bbo.size,
            st.mexc.bbo.seq,
            st.mexc.bbo.ts_ns,
            st.mexc.bbo.source_engine_ts_ns,
            st.mexc.bbo.source_system_ts_ns,
            11,
            Self::label("mexc_bbo", &st.demean.mexc),
            "mexc",
            "bbo",
            st.mexc.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.mexc.trade.price, &st.demean.mexc),
            None,
            None,
            Self::adjust_levels(st.mexc.trade.bid_levels, &st.demean.mexc),
            Self::adjust_levels(st.mexc.trade.ask_levels, &st.demean.mexc),
            st.mexc.trade.direction,
            st.mexc.trade.size,
            st.mexc.trade.seq,
            st.mexc.trade.ts_ns,
            st.mexc.trade.source_engine_ts_ns,
            st.mexc.trade.source_system_ts_ns,
            12,
            Self::label("mexc_trade", &st.demean.mexc),
            "mexc",
            "trade",
            st.mexc.trade.received_at,
        );

        // Lighter sources (adjusted)
        consider(
            Self::adjust_price(st.lighter.orderbook.price, &st.demean.lighter),
            Self::adjust_price(
                st.lighter.orderbook.bid_levels[0].map(|lvl| lvl.0),
                &st.demean.lighter,
            ),
            Self::adjust_price(
                st.lighter.orderbook.ask_levels[0].map(|lvl| lvl.0),
                &st.demean.lighter,
            ),
            Self::adjust_levels(st.lighter.orderbook.bid_levels, &st.demean.lighter),
            Self::adjust_levels(st.lighter.orderbook.ask_levels, &st.demean.lighter),
            st.lighter.orderbook.direction,
            st.lighter.orderbook.size,
            st.lighter.orderbook.seq,
            st.lighter.orderbook.ts_ns,
            st.lighter.orderbook.source_engine_ts_ns,
            st.lighter.orderbook.source_system_ts_ns,
            13,
            Self::label("lighter_ob", &st.demean.lighter),
            "lighter",
            "orderbook",
            st.lighter.orderbook.received_at,
        );
        consider(
            Self::adjust_price(st.lighter.bbo.price, &st.demean.lighter),
            Self::adjust_price(
                st.lighter.bbo.bid_levels[0].map(|lvl| lvl.0),
                &st.demean.lighter,
            ),
            Self::adjust_price(
                st.lighter.bbo.ask_levels[0].map(|lvl| lvl.0),
                &st.demean.lighter,
            ),
            Self::adjust_levels(st.lighter.bbo.bid_levels, &st.demean.lighter),
            Self::adjust_levels(st.lighter.bbo.ask_levels, &st.demean.lighter),
            st.lighter.bbo.direction,
            st.lighter.bbo.size,
            st.lighter.bbo.seq,
            st.lighter.bbo.ts_ns,
            st.lighter.bbo.source_engine_ts_ns,
            st.lighter.bbo.source_system_ts_ns,
            14,
            Self::label("lighter_bbo", &st.demean.lighter),
            "lighter",
            "bbo",
            st.lighter.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.lighter.trade.price, &st.demean.lighter),
            None,
            None,
            Self::adjust_levels(st.lighter.trade.bid_levels, &st.demean.lighter),
            Self::adjust_levels(st.lighter.trade.ask_levels, &st.demean.lighter),
            st.lighter.trade.direction,
            st.lighter.trade.size,
            st.lighter.trade.seq,
            st.lighter.trade.ts_ns,
            st.lighter.trade.source_engine_ts_ns,
            st.lighter.trade.source_system_ts_ns,
            15,
            Self::label("lighter_trade", &st.demean.lighter),
            "lighter",
            "trade",
            st.lighter.trade.received_at,
        );

        let candidate = best?;
        let key = RevisionKey {
            source_idx: candidate.source_idx,
            seq: candidate.seq,
            ts_ns: candidate.ts_ns,
        };
        Some((candidate, key))
    }

    /// Adjusts a price using demean offset if available.
    #[inline]
    fn adjust_price(price: Option<f64>, adj: &ExchangeAdjustment) -> Option<f64> {
        let px = price?;

        if !px.is_finite() || px <= 0.0 {
            return None;
        }

        if adj.samples > 0 {
            let offset = adj.offset.unwrap_or(0.0);
            let adjusted = px - offset;

            // LOUD FAILURE: Sanity check adjusted price
            if !adjusted.is_finite() || adjusted <= 0.0 {
                eprintln!(
                    "WARNING: Price adjustment resulted in invalid price: {} - {} = {}",
                    px, offset, adjusted
                );
                return None;
            }

            Some(adjusted)
        } else {
            Some(px)
        }
    }

    #[inline]
    fn adjust_level(level: Option<(f64, f64)>, adj: &ExchangeAdjustment) -> Option<(f64, f64)> {
        let (px, sz) = level?;
        let adjusted = Self::adjust_price(Some(px), adj)?;
        Some((adjusted, sz))
    }

    #[inline]
    fn adjust_levels(
        levels: [Option<(f64, f64)>; crate::base_classes::state::SNAPSHOT_DEPTH],
        adj: &ExchangeAdjustment,
    ) -> [Option<(f64, f64)>; crate::base_classes::state::SNAPSHOT_DEPTH] {
        levels.map(|lvl| Self::adjust_level(lvl, adj))
    }

    /// Creates a label for the source, appending "_adj" if adjustment is active.
    #[inline]
    fn label(base: &str, adj: &ExchangeAdjustment) -> String {
        if adj.samples > 0 {
            format!("{}_adj", base)
        } else {
            base.to_string()
        }
    }

    /// Determines if a candidate is newer than the current best.
    /// Prioritizes: timestamp > sequence > source index.
    #[inline]
    fn is_newer(candidate: &Candidate, current: &Candidate) -> bool {
        match (candidate.ts_ns, current.ts_ns) {
            (Some(cand_ts), Some(cur_ts)) if cand_ts != cur_ts => return cand_ts > cur_ts,
            (Some(_), None) => return true,
            (None, Some(_)) => return false,
            _ => {}
        }
        if candidate.seq != current.seq {
            return candidate.seq > current.seq;
        }
        candidate.source_idx > current.source_idx
    }
}

/// Key identifying a unique revision of reference price.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RevisionKey {
    source_idx: u8,
    seq: u64,
    ts_ns: Option<u64>,
}

/// Candidate reference price from a specific source.
#[derive(Clone, Debug)]
struct Candidate {
    price: f64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    bid_levels: [Option<(f64, f64)>; crate::base_classes::state::SNAPSHOT_DEPTH],
    ask_levels: [Option<(f64, f64)>; crate::base_classes::state::SNAPSHOT_DEPTH],
    direction: Option<TradeDirection>,
    size: Option<f64>,
    seq: u64,
    ts_ns: Option<u64>,
    source_engine_ts_ns: Option<u64>,
    source_system_ts_ns: Option<u64>,
    source_idx: u8,
    source: String,
    exchange: &'static str,
    feed: &'static str,
    received_at: Option<Instant>,
}

#[cfg(test)]
mod tests {
    use super::{Candidate, ReferencePublisher};
    use std::time::Instant;

    fn candidate(seq: u64, ts_ns: Option<u64>, source_idx: u8) -> Candidate {
        Candidate {
            price: 100.0,
            best_bid: Some(99.0),
            best_ask: Some(101.0),
            bid_levels: [None; crate::base_classes::state::SNAPSHOT_DEPTH],
            ask_levels: [None; crate::base_classes::state::SNAPSHOT_DEPTH],
            direction: None,
            size: None,
            seq,
            ts_ns,
            source_engine_ts_ns: ts_ns,
            source_system_ts_ns: ts_ns,
            source_idx,
            source: format!("source_{source_idx}"),
            exchange: "test",
            feed: "bbo",
            received_at: Some(Instant::now()),
        }
    }

    #[test]
    fn candidate_with_real_timestamp_beats_missing_timestamp() {
        let with_ts = candidate(1, Some(5), 1);
        let without_ts = candidate(999, None, 2);

        assert!(ReferencePublisher::is_newer(&with_ts, &without_ts));
        assert!(!ReferencePublisher::is_newer(&without_ts, &with_ts));
    }

    #[test]
    fn missing_timestamps_compare_by_sequence() {
        let newer = candidate(11, None, 1);
        let older = candidate(10, None, 2);

        assert!(ReferencePublisher::is_newer(&newer, &older));
        assert!(!ReferencePublisher::is_newer(&older, &newer));
    }
}
