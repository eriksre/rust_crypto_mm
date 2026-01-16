use crate::base_classes::feed_gate::{ExchangeFeed, FeedKind, FeedTimestampGate, GateDecision};
use crate::base_classes::reference_publisher::ReferencePublisher;
use crate::base_classes::ring_buffer::Consumer;
use crate::base_classes::state::{SNAPSHOT_DEPTH, TradeDirection, TradeEvent};
use crate::base_classes::tickers::TickerStore;
use crate::collectors::lighter;
use crate::exchanges::lighter::{LighterBook, LighterFrame, LighterMarketMeta};

use super::FastEventSender;
use super::demean_controller::DemeanController;
use super::helpers::{levels_to_array, lock_state, log_stale_update};

pub struct LighterEngine<const N: usize> {
    consumer: Consumer<LighterFrame, N>,
    pending: Option<LighterFrame>,
    book: LighterBook<1024>,
    trades: crate::base_classes::trades::FixedTrades<64>,
    tickers: TickerStore,
    symbol: String,
    market_id: u32,
    price_scale: f64,
    qty_scale: f64,
}

impl<const N: usize> LighterEngine<N> {
    pub fn new(
        symbol: String,
        meta: LighterMarketMeta,
        consumer: Consumer<LighterFrame, N>,
    ) -> Self {
        let price_scale = 10f64.powi(meta.price_decimals as i32);
        let qty_scale = 10f64.powi(meta.size_decimals as i32);
        Self {
            consumer,
            pending: None,
            book: LighterBook::<1024>::new(meta.market_id, price_scale, qty_scale),
            trades: crate::base_classes::trades::FixedTrades::<64>::default(),
            tickers: TickerStore::default(),
            symbol,
            market_id: meta.market_id,
            price_scale,
            qty_scale,
        }
    }

    pub fn process(
        &mut self,
        feed_gate: &mut FeedTimestampGate,
        publisher: &mut ReferencePublisher,
        _demean: &mut DemeanController,
        fast_sender: &FastEventSender,
    ) -> bool {
        if let Some(mut f) = self.pending.take().or_else(|| self.consumer.try_pop().ok()) {
            let ts = f.ts;
            {
                for (feed, _) in lighter::events_for(&mut f, &mut self.book, self.market_id) {
                    if feed == "orderbook" {
                        if let Some(mid) = self.book.mid_price_f64() {
                            let ob_ts = self.book.last_ts();
                            let gate_ts = ob_ts.max(ts);
                            match feed_gate.evaluate(
                                ExchangeFeed::Lighter,
                                FeedKind::OrderBook,
                                gate_ts,
                            ) {
                                GateDecision::Accept => {
                                    let (bid_vec, ask_vec) =
                                        self.book.top_levels_f64(SNAPSHOT_DEPTH);
                                    let best_bid = bid_vec.first().map(|lvl| lvl.0);
                                    let best_ask = ask_vec.first().map(|lvl| lvl.0);
                                    fast_sender.send(
                                        mid,
                                        best_bid,
                                        best_ask,
                                        "lighter_orderbook",
                                        Some(ob_ts),
                                        f.recv_instant,
                                    );
                                    let bid_levels = levels_to_array(&bid_vec);
                                    let ask_levels = levels_to_array(&ask_vec);
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.lighter.orderbook;
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
                                        ExchangeFeed::Lighter,
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

                let new_trades = lighter::update_trades(
                    &mut f,
                    self.market_id,
                    &mut self.trades,
                    self.price_scale,
                    self.qty_scale,
                );
                if new_trades > 0 {
                    for trade in self.trades.iter_last(new_trades) {
                        let trade_ts = trade.ts;
                        let gate_ts = trade_ts.max(ts);
                        match feed_gate.evaluate(ExchangeFeed::Lighter, FeedKind::Trades, gate_ts)
                        {
                            GateDecision::Accept => {
                                let px = (trade.px as f64) / self.price_scale;
                                let qty = (trade.qty as f64).abs() / self.qty_scale;
                                let direction = if trade.is_buyer_maker {
                                    TradeDirection::Sell
                                } else {
                                    TradeDirection::Buy
                                };
                                {
                                    let mut st = lock_state();
                                    let snap = &mut st.lighter;
                                    snap.trade.price = Some(px);
                                    snap.trade.seq = snap.trade.seq.wrapping_add(1);
                                    snap.trade.ts_ns = Some(ts);
                                    snap.trade.source_engine_ts_ns = Some(trade_ts);
                                    snap.trade.source_system_ts_ns = trade.system_ts_ns;
                                    snap.trade.direction = Some(direction);
                                    snap.trade.size = Some(qty);
                                    snap.trade.received_at = Some(f.recv_instant);
                                    snap.trade.bid_levels = [None; SNAPSHOT_DEPTH];
                                    snap.trade.ask_levels = [None; SNAPSHOT_DEPTH];
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
                                    ExchangeFeed::Lighter,
                                    FeedKind::Trades,
                                    trade_ts,
                                    last_ts,
                                    reject_count,
                                );
                            }
                        }
                    }
                }

                if let Some((_, ticker)) = lighter::update_tickers(
                    &mut f,
                    self.market_id,
                    &self.symbol,
                    self.price_scale,
                    &mut self.tickers,
                ) {
                    let mut st = lock_state();
                    let entry = &mut st.lighter.ticker;

                    if ticker.ticker.last_px != 0 {
                        entry.last_price = Some((ticker.ticker.last_px as f64) / self.price_scale);
                    }
                    if ticker.ticker.last_qty != 0 {
                        entry.last_qty = Some((ticker.ticker.last_qty as f64) / self.qty_scale);
                    }
                    if ticker.ticker.best_bid != 0 {
                        entry.best_bid = Some((ticker.ticker.best_bid as f64) / self.price_scale);
                    }
                    if ticker.ticker.best_ask != 0 {
                        entry.best_ask = Some((ticker.ticker.best_ask as f64) / self.price_scale);
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

                    if ticker.ticker.seq != 0 {
                        entry.seq = ticker.ticker.seq;
                    } else {
                        entry.seq = entry.seq.wrapping_add(1);
                    }

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
