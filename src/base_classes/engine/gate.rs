use crate::base_classes::feed_gate::{ExchangeFeed, FeedKind, FeedTimestampGate, GateDecision};
use crate::base_classes::reference_publisher::ReferencePublisher;
use crate::base_classes::ring_buffer::Consumer;
use crate::base_classes::state::{SNAPSHOT_DEPTH, TradeDirection, TradeEvent};
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::collectors::gate;
use crate::exchanges::endpoints::GateioWs;
use crate::exchanges::gate::{GateContractMeta, GateFrame};

use super::FastEventSender;
use super::demean_controller::DemeanController;
use super::helpers::{
    drain_latest_bbo, level_from_option, levels_to_array, lock_state, log_stale_update,
};

pub struct GateEngine<const N: usize> {
    consumer: Consumer<GateFrame, N>,
    pending: Option<GateFrame>,
    book: crate::exchanges::gate::GateBook<1024>,
    bbo: crate::base_classes::bbo_store::BboStore,
    trades: crate::base_classes::trades::FixedTrades<64>,
    tickers: TickerStore,
    symbol: String,
    qty_multiplier: f64,
}

#[inline(always)]
fn enrich_ticker_from_contract_meta(ticker: &mut TickerSnapshot, qty_multiplier: f64) {
    ticker.quanto_multiplier = Some(qty_multiplier);
    ticker.open_interest_value = match (ticker.open_interest, ticker.mark_px) {
        (Some(oi), Some(mark)) => Some(oi * mark * qty_multiplier),
        _ => None,
    };
}

impl<const N: usize> GateEngine<N> {
    pub fn new(
        symbol: String,
        consumer: Consumer<GateFrame, N>,
        contract_meta: GateContractMeta,
    ) -> Self {
        let qty_multiplier = contract_meta
            .quanto_multiplier
            .filter(|mult| mult.is_finite() && *mult > 0.0)
            .unwrap_or_else(|| {
                panic!("ERROR: Gate contract metadata missing valid quanto_multiplier for {symbol}")
            });
        Self {
            consumer,
            pending: None,
            book: crate::exchanges::gate::GateBook::<1024>::new(
                &symbol,
                crate::exchanges::gate::GateBook::<1024>::PRICE_SCALE,
                crate::exchanges::gate::GateBook::<1024>::QTY_SCALE,
                qty_multiplier,
            ),
            bbo: crate::base_classes::bbo_store::BboStore::default(),
            trades: crate::base_classes::trades::FixedTrades::<64>::default(),
            tickers: TickerStore::default(),
            symbol,
            qty_multiplier,
        }
    }

    #[inline(always)]
    fn is_bbo_frame(frame: &GateFrame) -> bool {
        frame.channel() == GateioWs::BBO && frame.event() == "update"
    }

    pub fn process(
        &mut self,
        feed_gate: &mut FeedTimestampGate,
        publisher: &mut ReferencePublisher,
        demean: &mut DemeanController,
        fast_sender: &FastEventSender,
    ) -> bool {
        if let Some(mut f) = self.pending.take().or_else(|| self.consumer.try_pop().ok()) {
            drain_latest_bbo(
                &mut f,
                &self.consumer,
                &mut self.pending,
                Self::is_bbo_frame,
            );
            let Some(text) = f.text() else {
                return true;
            };
            let ts = f.ts;
            {
                for (feed, _) in gate::events_for(text, &mut self.book) {
                    if feed == "orderbook" {
                        if let Some(mid) = self.book.mid_price_f64() {
                            let ob_ts = self.book.last_ts();
                            match feed_gate.evaluate(ExchangeFeed::Gate, FeedKind::OrderBook, ob_ts)
                            {
                                GateDecision::Accept => {
                                    let (bid_vec, ask_vec) =
                                        self.book.top_levels_f64(SNAPSHOT_DEPTH);
                                    let best_bid = bid_vec.first().map(|lvl| lvl.0);
                                    let best_ask = ask_vec.first().map(|lvl| lvl.0);
                                    fast_sender.send(
                                        mid,
                                        best_bid,
                                        best_ask,
                                        "gate_orderbook",
                                        Some(ob_ts),
                                        f.recv_instant,
                                    );
                                    let bid_levels = levels_to_array(&bid_vec);
                                    let ask_levels = levels_to_array(&ask_vec);
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.gate.orderbook;
                                        snap.price = Some(mid);
                                        snap.seq = snap.seq.wrapping_add(1);
                                        snap.ts_ns = Some(ts);
                                        snap.source_engine_ts_ns = Some(ob_ts);
                                        snap.source_system_ts_ns = None;
                                        snap.bid_levels = bid_levels;
                                        snap.ask_levels = ask_levels;
                                        snap.direction = None;
                                        snap.received_at = Some(f.recv_instant);
                                    }
                                    publisher.publish();
                                }
                                GateDecision::Reject {
                                    last_ts,
                                    reject_count,
                                } => {
                                    log_stale_update(
                                        ExchangeFeed::Gate,
                                        FeedKind::OrderBook,
                                        ob_ts,
                                        last_ts,
                                        reject_count,
                                    );
                                }
                            }
                        }
                    }
                }
                if gate::update_bbo_store(text, &mut self.bbo, self.qty_multiplier) {
                    if let Some(mid) = self
                        .bbo
                        .mid_price_f64_for(&self.symbol)
                        .or_else(|| self.bbo.mid_price_f64())
                    {
                        let entry = self.bbo.get(&self.symbol).copied().or_else(|| {
                            self.bbo
                                .last_symbol()
                                .and_then(|symbol| self.bbo.get(symbol).copied())
                        });
                        let bbo_ts = entry.map(|e| e.ts).unwrap_or_else(|| self.book.last_ts());
                        let system_ts_ns = entry.and_then(|e| e.system_ts_ns);
                        match feed_gate.evaluate(ExchangeFeed::Gate, FeedKind::Bbo, bbo_ts) {
                            GateDecision::Accept => {
                                let (bid_levels, ask_levels) = if let Some(e) = entry {
                                    (
                                        level_from_option(Some((e.bid_px, e.bid_qty))),
                                        level_from_option(Some((e.ask_px, e.ask_qty))),
                                    )
                                } else {
                                    let (bid_vec, ask_vec) = self.book.top_levels_f64(1);
                                    (levels_to_array(&bid_vec), levels_to_array(&ask_vec))
                                };
                                let best_bid = bid_levels[0].map(|lvl| lvl.0);
                                let best_ask = ask_levels[0].map(|lvl| lvl.0);
                                fast_sender.send(
                                    mid,
                                    best_bid,
                                    best_ask,
                                    "gate_bbo",
                                    Some(bbo_ts),
                                    f.recv_instant,
                                );
                                let recv_instant = f.recv_instant;
                                {
                                    let mut st = lock_state();
                                    let snap = &mut st.gate.bbo;
                                    snap.price = Some(mid);
                                    snap.seq = snap.seq.wrapping_add(1);
                                    snap.ts_ns = Some(ts);
                                    snap.source_engine_ts_ns = Some(bbo_ts);
                                    snap.source_system_ts_ns = system_ts_ns;
                                    snap.bid_levels = bid_levels;
                                    snap.ask_levels = ask_levels;
                                    snap.direction = None;
                                    snap.received_at = Some(recv_instant);
                                }
                                let updates = demean.on_gate_event(Some(bbo_ts), Some(mid));
                                demean.apply_updates(&updates);
                                publisher.publish();
                            }
                            GateDecision::Reject {
                                last_ts,
                                reject_count,
                            } => {
                                log_stale_update(
                                    ExchangeFeed::Gate,
                                    FeedKind::Bbo,
                                    bbo_ts,
                                    last_ts,
                                    reject_count,
                                );
                            }
                        }
                    }
                }
                let new_trades = gate::update_trades(text, &mut self.trades, self.qty_multiplier);
                if new_trades > 0 {
                    for trade in self.trades.iter_last(new_trades) {
                        let trade_ts = trade.ts;
                        match feed_gate.evaluate(ExchangeFeed::Gate, FeedKind::Trades, trade_ts) {
                            GateDecision::Accept => {
                                let px = (trade.px as f64) / gate::PRICE_SCALE;
                                fast_sender.send(
                                    px,
                                    None,
                                    None,
                                    "gate_trade",
                                    Some(trade_ts),
                                    f.recv_instant,
                                );
                                let direction = if trade.is_buyer_maker {
                                    TradeDirection::Sell
                                } else {
                                    TradeDirection::Buy
                                };
                                {
                                    let mut st = lock_state();
                                    let snap = &mut st.gate;
                                    snap.trade.price = Some(px);
                                    snap.trade.seq = snap.trade.seq.wrapping_add(1);
                                    snap.trade.ts_ns = Some(ts);
                                    snap.trade.source_engine_ts_ns = Some(trade_ts);
                                    snap.trade.source_system_ts_ns = trade.system_ts_ns;
                                    snap.trade.direction = Some(direction);
                                    snap.trade.bid_levels = [None; SNAPSHOT_DEPTH];
                                    snap.trade.ask_levels = [None; SNAPSHOT_DEPTH];
                                    snap.trade.received_at = Some(f.recv_instant);

                                    let qty = (trade.qty as f64).abs() / gate::QTY_SCALE;
                                    snap.trade.size = Some(qty);
                                    snap.trade_events.push_back(TradeEvent {
                                        ts_ns: trade_ts,
                                        price: px,
                                        direction: Some(direction),
                                        quantity: Some(qty),
                                    });
                                    if snap.trade_events.len() > 256 {
                                        snap.trade_events.pop_front();
                                    }
                                }
                                publisher.publish();
                            }
                            GateDecision::Reject {
                                last_ts,
                                reject_count,
                            } => {
                                log_stale_update(
                                    ExchangeFeed::Gate,
                                    FeedKind::Trades,
                                    trade_ts,
                                    last_ts,
                                    reject_count,
                                );
                            }
                        }
                    }
                }
                if let Some((symbol, mut ticker)) =
                    gate::update_tickers(text, &mut self.tickers, self.qty_multiplier)
                {
                    enrich_ticker_from_contract_meta(&mut ticker, self.qty_multiplier);
                    ticker = self.tickers.update(symbol.clone(), ticker);

                    let mut st = lock_state();
                    let entry = &mut st.gate.ticker;

                    if ticker.ticker.last_px != 0 {
                        entry.last_price = Some((ticker.ticker.last_px as f64) / gate::PRICE_SCALE);
                    }
                    if ticker.ticker.last_qty != 0 {
                        entry.last_qty = Some((ticker.ticker.last_qty as f64) / gate::QTY_SCALE);
                    }
                    if ticker.ticker.best_bid != 0 {
                        entry.best_bid = Some((ticker.ticker.best_bid as f64) / gate::PRICE_SCALE);
                    }
                    if ticker.ticker.best_ask != 0 {
                        entry.best_ask = Some((ticker.ticker.best_ask as f64) / gate::PRICE_SCALE);
                    }

                    if let Some(mark) = ticker.mark_px {
                        entry.mark_price = Some(mark);
                    }
                    if let Some(index) = ticker.index_px {
                        entry.index_price = Some(index);
                    }
                    if let Some(rate) = ticker.funding_rate {
                        entry.funding_rate = Some(rate);
                    }
                    if let Some(turnover) = ticker.turnover_24h {
                        entry.turnover_24h = Some(turnover);
                    }
                    if let Some(oi) = ticker.open_interest {
                        entry.open_interest = Some(oi);
                    }
                    entry.quanto_multiplier = Some(self.qty_multiplier);
                    if ticker.open_interest_value.is_some() {
                        entry.open_interest_value = ticker.open_interest_value;
                    }

                    let seq = if ticker.ticker.seq != 0 {
                        ticker.ticker.seq
                    } else {
                        entry.seq.wrapping_add(1)
                    };
                    entry.seq = seq;
                    entry.ts_ns = Some(ts);
                }
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::enrich_ticker_from_contract_meta;
    use crate::base_classes::tickers::TickerSnapshot;

    #[test]
    fn gate_ticker_enrichment_uses_rest_multiplier() {
        let mut ticker = TickerSnapshot {
            mark_px: Some(43005.0),
            open_interest: Some(456.0),
            ..TickerSnapshot::default()
        };

        enrich_ticker_from_contract_meta(&mut ticker, 0.01);

        assert_eq!(ticker.quanto_multiplier, Some(0.01));
        assert_eq!(ticker.open_interest_value, Some(456.0 * 43005.0 * 0.01));
    }

    #[test]
    fn gate_ticker_enrichment_leaves_oi_value_unset_without_inputs() {
        let mut ticker = TickerSnapshot::default();

        enrich_ticker_from_contract_meta(&mut ticker, 0.01);

        assert_eq!(ticker.quanto_multiplier, Some(0.01));
        assert_eq!(ticker.open_interest_value, None);
    }
}
