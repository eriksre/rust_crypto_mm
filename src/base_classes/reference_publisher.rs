//! Reference price publisher
//!
//! Selects the best reference price from multiple exchanges and publishes updates.
//! Prioritizes Gate.io prices, then adjusts prices from other exchanges using demean offsets.

use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;

use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::state::{ExchangeAdjustment, GlobalState, state};

/// Publishes reference price events by selecting the best candidate from all exchanges.
pub struct ReferencePublisher {
    tx: Option<UnboundedSender<ReferenceEvent>>,
    last_key: Option<RevisionKey>,
}

impl ReferencePublisher {
    /// Creates a new reference publisher.
    /// If `tx` is None, the publisher is a no-op (useful for testing).
    #[inline]
    pub fn new(tx: Option<UnboundedSender<ReferenceEvent>>) -> Self {
        Self { tx, last_key: None }
    }

    /// Publishes a reference price event if a new candidate is available.
    /// Silently ignores if no channel is configured or if state lock fails.
    #[inline]
    pub fn publish(&mut self) {
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
        let event = ReferenceEvent {
            price: candidate.price,
            ts_ns: candidate.ts_ns,
            source: candidate.source,
            received_at: candidate.received_at.unwrap_or_else(Instant::now),
        };

        if let Err(err) = tx.send(event) {
            // LOUD FAILURE: Channel closed unexpectedly
            eprintln!("ERROR: Failed to publish reference event: {}", err);
            // Don't panic here - the channel might be closed during shutdown
        }
    }

    /// Selects the best reference price candidate from global state.
    /// Considers all exchanges with priority to Gate.io (no adjustment).
    fn select_candidate(st: &GlobalState) -> Option<(Candidate, RevisionKey)> {
        let mut best: Option<Candidate> = None;

        let mut consider = |price: Option<f64>,
                            seq: u64,
                            ts: Option<u64>,
                            idx: u8,
                            source: String,
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
                seq,
                ts_ns: ts,
                source_idx: idx,
                source,
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
            st.gate.bbo.seq,
            st.gate.bbo.ts_ns,
            0,
            "gate_bbo".to_string(),
            st.gate.bbo.received_at,
        );
        consider(
            st.gate.orderbook.price,
            st.gate.orderbook.seq,
            st.gate.orderbook.ts_ns,
            1,
            "gate_ob".to_string(),
            st.gate.orderbook.received_at,
        );
        consider(
            st.gate.trade.price,
            st.gate.trade.seq,
            st.gate.trade.ts_ns,
            2,
            "gate_trade".to_string(),
            st.gate.trade.received_at,
        );

        // Bybit sources (adjusted)
        consider(
            Self::adjust_price(st.bybit.bbo.price, &st.demean.bybit),
            st.bybit.bbo.seq,
            st.bybit.bbo.ts_ns,
            3,
            Self::label("bybit_bbo", &st.demean.bybit),
            st.bybit.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.bybit.trade.price, &st.demean.bybit),
            st.bybit.trade.seq,
            st.bybit.trade.ts_ns,
            4,
            Self::label("bybit_trade", &st.demean.bybit),
            st.bybit.trade.received_at,
        );

        // Binance sources (adjusted)
        consider(
            Self::adjust_price(st.binance.bbo.price, &st.demean.binance),
            st.binance.bbo.seq,
            st.binance.bbo.ts_ns,
            5,
            Self::label("binance_bbo", &st.demean.binance),
            st.binance.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.binance.trade.price, &st.demean.binance),
            st.binance.trade.seq,
            st.binance.trade.ts_ns,
            6,
            Self::label("binance_trade", &st.demean.binance),
            st.binance.trade.received_at,
        );

        // Bitget sources (adjusted)
        consider(
            Self::adjust_price(st.bitget.bbo.price, &st.demean.bitget),
            st.bitget.bbo.seq,
            st.bitget.bbo.ts_ns,
            7,
            Self::label("bitget_bbo", &st.demean.bitget),
            st.bitget.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.bitget.trade.price, &st.demean.bitget),
            st.bitget.trade.seq,
            st.bitget.trade.ts_ns,
            8,
            Self::label("bitget_trade", &st.demean.bitget),
            st.bitget.trade.received_at,
        );

        // OKX sources (adjusted)
        consider(
            Self::adjust_price(st.okx.bbo.price, &st.demean.okx),
            st.okx.bbo.seq,
            st.okx.bbo.ts_ns,
            9,
            Self::label("okx_bbo", &st.demean.okx),
            st.okx.bbo.received_at,
        );
        consider(
            Self::adjust_price(st.okx.trade.price, &st.demean.okx),
            st.okx.trade.seq,
            st.okx.trade.ts_ns,
            10,
            Self::label("okx_trade", &st.demean.okx),
            st.okx.trade.received_at,
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
        let cand_ts = candidate.ts_ns.unwrap_or(0);
        let cur_ts = current.ts_ns.unwrap_or(0);
        if cand_ts != cur_ts {
            return cand_ts > cur_ts;
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
    seq: u64,
    ts_ns: Option<u64>,
    source_idx: u8,
    source: String,
    received_at: Option<Instant>,
}
