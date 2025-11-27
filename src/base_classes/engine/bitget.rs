use crate::base_classes::demean::ExchangeKind;
use crate::base_classes::feed_gate::{ExchangeFeed, FeedKind, FeedTimestampGate, GateDecision};
use crate::base_classes::reference_publisher::ReferencePublisher;
use crate::base_classes::ring_buffer::Consumer;
use crate::base_classes::state::{SNAPSHOT_DEPTH, TradeDirection, TradeEvent};
use crate::base_classes::tickers::TickerStore;
use crate::collectors::bitget;
use crate::exchanges::bitget::BitgetFrame;
use crate::exchanges::endpoints::BitgetWs;

use super::demean_controller::DemeanController;
use super::helpers::{
    drain_latest_bbo, level_from_option, levels_to_array, lock_state, log_stale_update,
};

pub struct BitgetEngine<const N: usize> {
    consumer: Consumer<BitgetFrame, N>,
    pending: Option<BitgetFrame>,
    book: crate::exchanges::bitget::BitgetBook<1024>,
    bbo: crate::base_classes::bbo_store::BboStore,
    trades: crate::base_classes::trades::FixedTrades<64>,
    tickers: TickerStore,
    symbol: String,
}

impl<const N: usize> BitgetEngine<N> {
    pub fn new(symbol: String, consumer: Consumer<BitgetFrame, N>) -> Self {
        Self {
            consumer,
            pending: None,
            book: crate::exchanges::bitget::BitgetBook::<1024>::new(
                &symbol,
                crate::exchanges::bitget::BitgetBook::<1024>::PRICE_SCALE,
                crate::exchanges::bitget::BitgetBook::<1024>::QTY_SCALE,
            ),
            bbo: crate::base_classes::bbo_store::BboStore::default(),
            trades: crate::base_classes::trades::FixedTrades::<64>::default(),
            tickers: TickerStore::default(),
            symbol,
        }
    }

    #[inline(always)]
    fn is_bbo_frame(frame: &BitgetFrame) -> bool {
        if frame.channel() != BitgetWs::BBO {
            return false;
        }
        matches!(frame.action(), "update" | "snapshot")
    }

    pub fn process(
        &mut self,
        feed_gate: &mut FeedTimestampGate,
        publisher: &mut ReferencePublisher,
        demean: &mut DemeanController,
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
                for (feed, _) in bitget::events_for(text, &mut self.book) {
                    if feed == "orderbook" {
                        if let Some(mid) = self.book.mid_price_f64() {
                            let ob_ts = self.book.last_ts();
                            match feed_gate.evaluate(
                                ExchangeFeed::Bitget,
                                FeedKind::OrderBook,
                                ob_ts,
                            ) {
                                GateDecision::Accept => {
                                    let (bid_vec, ask_vec) =
                                        self.book.top_levels_f64(SNAPSHOT_DEPTH);
                                    let bid_levels = levels_to_array(&bid_vec);
                                    let ask_levels = levels_to_array(&ask_vec);
                                    let mut st = lock_state();
                                    let snap = &mut st.bitget.orderbook;
                                    snap.price = Some(mid);
                                    snap.seq = snap.seq.wrapping_add(1);
                                    snap.ts_ns = Some(ts);
                                    snap.source_engine_ts_ns = Some(ob_ts);
                                    snap.source_system_ts_ns = self.book.last_system_ts_ns();
                                    snap.bid_levels = bid_levels;
                                    snap.ask_levels = ask_levels;
                                    snap.direction = None;
                                    snap.received_at = Some(f.recv_instant);
                                }
                                GateDecision::Reject {
                                    last_ts,
                                    reject_count,
                                } => {
                                    log_stale_update(
                                        ExchangeFeed::Bitget,
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
                if bitget::update_bbo_store(text, &mut self.bbo) {
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
                        let system_ts_ns = entry
                            .and_then(|e| e.system_ts_ns)
                            .or_else(|| self.book.last_bbo_system_ts_ns());
                        match feed_gate.evaluate(ExchangeFeed::Bitget, FeedKind::Bbo, bbo_ts) {
                            GateDecision::Accept => {
                                demean.record_other(ExchangeKind::Bitget, Some(bbo_ts), Some(mid));
                                let (bid_levels, ask_levels) = if let Some(e) = entry {
                                    (
                                        level_from_option(Some((e.bid_px, e.bid_qty))),
                                        level_from_option(Some((e.ask_px, e.ask_qty))),
                                    )
                                } else {
                                    let (bid_vec, ask_vec) = self.book.top_levels_f64(1);
                                    (levels_to_array(&bid_vec), levels_to_array(&ask_vec))
                                };
                                {
                                    let mut st = lock_state();
                                    let snap = &mut st.bitget.bbo;
                                    snap.price = Some(mid);
                                    snap.seq = snap.seq.wrapping_add(1);
                                    snap.ts_ns = Some(ts);
                                    snap.source_engine_ts_ns = Some(bbo_ts);
                                    snap.source_system_ts_ns = system_ts_ns;
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
                                    ExchangeFeed::Bitget,
                                    FeedKind::Bbo,
                                    bbo_ts,
                                    last_ts,
                                    reject_count,
                                );
                            }
                        }
                    }
                }
                let new_trades = bitget::update_trades(text, &mut self.trades);
                if new_trades > 0 {
                    for trade in self.trades.iter_last(new_trades) {
                        let trade_ts = trade.ts;
                        match feed_gate.evaluate(ExchangeFeed::Bitget, FeedKind::Trades, trade_ts) {
                            GateDecision::Accept => {
                                let px = (trade.px as f64) / bitget::PRICE_SCALE;
                                let direction = if trade.is_buyer_maker {
                                    TradeDirection::Sell
                                } else {
                                    TradeDirection::Buy
                                };
                                {
                                    let mut st = lock_state();
                                    let snap = &mut st.bitget;
                                    snap.trade.price = Some(px);
                                    snap.trade.seq = snap.trade.seq.wrapping_add(1);
                                    snap.trade.ts_ns = Some(ts);
                                    snap.trade.source_engine_ts_ns = Some(trade_ts);
                                    snap.trade.source_system_ts_ns = trade.system_ts_ns;
                                    snap.trade.direction = Some(direction);
                                    snap.trade.bid_levels = [None; SNAPSHOT_DEPTH];
                                    snap.trade.ask_levels = [None; SNAPSHOT_DEPTH];
                                    snap.trade.received_at = Some(f.recv_instant);

                                    let qty = (trade.qty as f64).abs() / bitget::QTY_SCALE;
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
                                    ExchangeFeed::Bitget,
                                    FeedKind::Trades,
                                    trade_ts,
                                    last_ts,
                                    reject_count,
                                );
                            }
                        }
                    }
                }
                if let Some((_, ticker)) = bitget::update_tickers(text, &mut self.tickers) {
                    let mut st = lock_state();
                    let entry = &mut st.bitget.ticker;
                    let price_scale = bitget::PRICE_SCALE;
                    let qty_scale = bitget::QTY_SCALE;

                    if ticker.ticker.last_px != 0 {
                        entry.last_price = Some((ticker.ticker.last_px as f64) / price_scale);
                    }
                    if ticker.ticker.last_qty != 0 {
                        entry.last_qty = Some((ticker.ticker.last_qty as f64) / qty_scale);
                    }
                    if ticker.ticker.best_bid != 0 {
                        entry.best_bid = Some((ticker.ticker.best_bid as f64) / price_scale);
                    }
                    if ticker.ticker.best_ask != 0 {
                        entry.best_ask = Some((ticker.ticker.best_ask as f64) / price_scale);
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
                    if let Some(oi_val) = ticker.open_interest_value {
                        entry.open_interest_value = Some(oi_val);
                    } else if let (Some(oi), Some(mark)) = (entry.open_interest, entry.mark_price) {
                        entry.open_interest_value = Some(oi * mark);
                    }

                    let seq = if ticker.ticker.seq != 0 {
                        ticker.ticker.seq
                    } else {
                        entry.seq.wrapping_add(1)
                    };
                    entry.seq = seq;

                    let ticker_ts = if ticker.ticker.ts != 0 {
                        ticker.ticker.ts
                    } else {
                        ts
                    };
                    entry.ts_ns = Some(ticker_ts);
                }
            }
            true
        } else {
            false
        }
    }
}
