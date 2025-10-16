#![allow(dead_code)]

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::base_classes::demean::{DemeanTracker, ExchangeKind};
use crate::base_classes::feed_gate::{ExchangeFeed, FeedKind, FeedTimestampGate, GateDecision};
use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::reference_publisher::ReferencePublisher;
use crate::base_classes::state::{ExchangeAdjustment, TradeDirection, TradeEvent, state};
use crate::base_classes::tickers::TickerStore;
use crate::base_classes::types::Ts;
use crate::base_classes::ws::{FeedSignal, spawn_ws_worker};
use crate::collectors::{binance, bitget, bybit, gate};

use tokio::sync::mpsc::UnboundedSender;

use crate::exchanges::binance::BinanceHandler;
use crate::exchanges::bitget::BitgetHandler;
use crate::exchanges::bybit::BybitHandler;
use crate::exchanges::gate::{GateHandler, canonical_contract_symbol};
use crate::exchanges::gate_rest;

#[cfg(feature = "gate_exec")]
use crate::execution::{GateWsConfig, GateWsGateway};
#[cfg(feature = "gate_exec")]
use futures_util::future::pending;
#[cfg(feature = "gate_exec")]
use std::env;

#[inline(always)]
fn levels_to_array(levels: &[(f64, f64)]) -> [Option<(f64, f64)>; 3] {
    let mut out = [None; 3];
    for (idx, &(px, qty)) in levels.iter().take(3).enumerate() {
        out[idx] = Some((px, qty));
    }
    out
}

#[inline(always)]
fn level_from_option(level: Option<(f64, f64)>) -> [Option<(f64, f64)>; 3] {
    let mut out = [None; 3];
    if let Some(lvl) = level {
        out[0] = Some(lvl);
    }
    out
}

#[inline(always)]
fn log_stale_update(exchange: ExchangeFeed, feed: FeedKind, ts: Ts, last_ts: Ts, count: u64) {
    if count <= 3 || count % 100 == 0 {
        eprintln!(
            "dropping stale {} {} update: ts={} < last={} ({} drops)",
            exchange.as_str(),
            feed.as_str(),
            ts,
            last_ts,
            count
        );
    }
}

/// LOUD state lock helper - panics immediately if lock is poisoned.
/// This is intentional - a poisoned lock means the system is in an undefined state.
#[inline(always)]
fn lock_state() -> std::sync::MutexGuard<'static, crate::base_classes::state::GlobalState> {
    match state().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            eprintln!(
                "FATAL: State lock poisoned in engine main loop: {}",
                poisoned
            );
            eprintln!("This indicates a panic occurred while holding the state lock.");
            eprintln!("The system cannot continue safely - terminating immediately.");
            panic!("State lock poisoned - unrecoverable error");
        }
    }
}

#[cfg(feature = "gate_exec")]
fn spawn_gate_user_trades_listener(
    api_key: String,
    api_secret: String,
    contract: String,
    settle: String,
    contract_size: f64,
) {
    let _ = thread::Builder::new()
        .name("gate-user-trades".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("Failed to create tokio runtime for Gate user trades: {err}");
                    return;
                }
            };

            let cfg = GateWsConfig {
                api_key,
                api_secret,
                symbol: contract,
                settle: Some(settle),
                ws_url: None,
                contract_size: Some(contract_size),
            };

            match rt.block_on(GateWsGateway::connect(cfg)) {
                Ok(gateway) => {
                    let _keepalive = gateway;
                    let _ = rt.block_on(async { pending::<()>().await });
                }
                Err(err) => {
                    eprintln!("Failed to connect Gate user trades listener: {:#}", err);
                }
            }
        });
}

pub fn spawn_state_engine(
    symbol: String,
    reference_tx: Option<UnboundedSender<ReferenceEvent>>,
    fast_tx: Option<UnboundedSender<ReferenceEvent>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut publisher = ReferencePublisher::new(reference_tx);
        let fast_tx = fast_tx;
        let send_fast_event =
            |price: f64, source: &'static str, ts: Option<u64>, recv_at: Instant| {
                if let Some(tx) = fast_tx.as_ref() {
                    if price.is_finite() && price > 0.0 {
                        let _ = tx.send(ReferenceEvent {
                            price,
                            ts_ns: ts,
                            source: source.to_string(),
                            received_at: recv_at,
                        });
                    }
                }
            };
        const N: usize = 1 << 15;
        let wake_signal = FeedSignal::new();
        let (bybit_c, _jh1) = spawn_ws_worker::<BybitHandler, N>(
            BybitHandler::new(symbol.clone()),
            None,
            Some(wake_signal.clone()),
        );
        let (binance_c, _jh2) = spawn_ws_worker::<BinanceHandler, N>(
            BinanceHandler::new(symbol.clone()),
            None,
            Some(wake_signal.clone()),
        );
        let (gate_c, _jh3) = spawn_ws_worker::<GateHandler, N>(
            GateHandler::new(symbol.clone()),
            None,
            Some(wake_signal.clone()),
        );
        let (bitget_c, _jh4) = spawn_ws_worker::<BitgetHandler, N>(
            BitgetHandler::new(symbol.clone()),
            None,
            Some(wake_signal.clone()),
        );

        let symbol_uc = symbol.to_uppercase();
        let cross_venue_symbol = symbol_uc.replace('_', "");
        let bybit_symbol = cross_venue_symbol.clone();
        let binance_symbol = cross_venue_symbol.clone();
        let bitget_symbol = cross_venue_symbol.clone();
        let gate_contract = canonical_contract_symbol(&symbol);
        let gate_symbol = gate_contract.clone();
        let gate_contract_meta = gate_rest::fetch_contract_meta(&gate_contract);
        #[cfg(feature = "gate_exec")]
        {
            let api_key = env::var("gateio_api_key").or_else(|_| env::var("GATE_API_KEY"));
            let api_secret = env::var("gateio_secret_key").or_else(|_| env::var("GATE_API_SECRET"));
            if let (Ok(api_key), Ok(api_secret)) = (api_key, api_secret) {
                let settle = env::var("GATE_SETTLE").unwrap_or_else(|_| "usdt".to_string());
                let contract_size = gate_contract_meta
                    .as_ref()
                    .and_then(|meta| meta.quanto_multiplier)
                    .unwrap_or(1.0);
                spawn_gate_user_trades_listener(
                    api_key,
                    api_secret,
                    gate_contract.clone(),
                    settle,
                    contract_size,
                );
            }
        }
        let mut bybit_book = crate::exchanges::bybit_book::BybitBook::<1024>::new(
            &bybit_symbol,
            crate::exchanges::bybit_book::PRICE_SCALE,
            crate::exchanges::bybit_book::QTY_SCALE,
        );
        let mut gate_book = crate::exchanges::gate_book::GateBook::<1024>::new(
            &gate_contract,
            crate::exchanges::gate_book::GateBook::<1024>::PRICE_SCALE,
            crate::exchanges::gate_book::GateBook::<1024>::QTY_SCALE,
        );
        #[cfg(feature = "bitget_book")]
        let mut bitget_book = {
            crate::exchanges::bitget_book::BitgetBook::<1024>::new(
                &bitget_symbol,
                crate::exchanges::bitget_book::BitgetBook::<1024>::PRICE_SCALE,
                crate::exchanges::bitget_book::BitgetBook::<1024>::QTY_SCALE,
            )
        };
        #[cfg(feature = "binance_book")]
        let mut binance_book = {
            use crate::exchanges::binance_book::BinanceBook;
            let rt = tokio::runtime::Runtime::new().expect("tokio rt");
            let mut bk: BinanceBook<1024> = BinanceBook::new(
                &symbol,
                BinanceBook::<1024>::PRICE_SCALE,
                BinanceBook::<1024>::QTY_SCALE,
            );
            rt.block_on(async {
                bk.init_from_rest(1000).await.expect("binance rest");
            });
            bk
        };

        // Per-exchange BBO/trades stores
        let mut bybit_bbo = crate::base_classes::bbo_store::BboStore::default();
        let mut binance_bbo = crate::base_classes::bbo_store::BboStore::default();
        let mut gate_bbo = crate::base_classes::bbo_store::BboStore::default();
        let mut bitget_bbo = crate::base_classes::bbo_store::BboStore::default();

        let mut bybit_trades = crate::base_classes::trades::FixedTrades::<64>::default();
        let mut binance_trades = crate::base_classes::trades::FixedTrades::<64>::default();
        let mut gate_trades = crate::base_classes::trades::FixedTrades::<64>::default();
        let mut bitget_trades = crate::base_classes::trades::FixedTrades::<64>::default();

        let mut bybit_tickers = TickerStore::default();
        let mut bitget_tickers = TickerStore::default();
        let mut gate_tickers = TickerStore::default();
        let mut binance_tickers = TickerStore::default();

        let mut demean = DemeanTracker::new(Duration::from_secs(8));
        let mut feed_gate = FeedTimestampGate::new();

        let apply_demean = |updates: &[(ExchangeKind, ExchangeAdjustment)]| {
            if updates.is_empty() {
                return;
            }
            let mut st = lock_state();
            for (exchange, adj) in updates {
                let target = match exchange {
                    ExchangeKind::Bybit => &mut st.demean.bybit,
                    ExchangeKind::Binance => &mut st.demean.binance,
                    ExchangeKind::Bitget => &mut st.demean.bitget,
                };
                *target = *adj;
            }
        };

        loop {
            let mut progressed = false;

            // Bybit
            if let Ok(f) = bybit_c.try_pop() {
                progressed = true;
                let ts = f.ts;
                if let Ok(s) = core::str::from_utf8(&f.raw) {
                    for (feed, _) in bybit::events_for(s, &mut bybit_book) {
                        match feed {
                            "orderbook" => {
                                if let Some(mid) = bybit_book.mid_price_f64() {
                                    let ob_ts = bybit_book.last_ts();
                                    match feed_gate.evaluate(
                                        ExchangeFeed::Bybit,
                                        FeedKind::OrderBook,
                                        ob_ts,
                                    ) {
                                        GateDecision::Accept => {
                                            demean.record_other(
                                                ExchangeKind::Bybit,
                                                Some(ob_ts),
                                                Some(mid),
                                            );
                                            let (bid_vec, ask_vec) = bybit_book.top_levels_f64(3);
                                            let bid_levels = levels_to_array(&bid_vec);
                                            let ask_levels = levels_to_array(&ask_vec);
                                            {
                                                let mut st = lock_state();
                                                let snap = &mut st.bybit.orderbook;
                                                snap.price = Some(mid);
                                                snap.seq = snap.seq.wrapping_add(1);
                                                snap.ts_ns = Some(ob_ts);
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
                                                ExchangeFeed::Bybit,
                                                FeedKind::OrderBook,
                                                ob_ts,
                                                last_ts,
                                                reject_count,
                                            );
                                        }
                                    }
                                }
                            }
                            "bbo" => {
                                if bybit::update_bbo_store(s, &mut bybit_bbo) {
                                    if let Some(mid) = bybit_bbo
                                        .mid_price_f64_for(&bybit_symbol)
                                        .or_else(|| bybit_bbo.mid_price_f64())
                                    {
                                        let entry =
                                            bybit_bbo.get(&bybit_symbol).copied().or_else(|| {
                                                bybit_bbo.last_symbol().and_then(|symbol| {
                                                    bybit_bbo.get(symbol).copied()
                                                })
                                            });
                                        let bbo_ts = entry
                                            .map(|e| e.ts)
                                            .unwrap_or_else(|| bybit_book.last_ts());
                                        match feed_gate.evaluate(
                                            ExchangeFeed::Bybit,
                                            FeedKind::Bbo,
                                            bbo_ts,
                                        ) {
                                            GateDecision::Accept => {
                                                demean.record_other(
                                                    ExchangeKind::Bybit,
                                                    Some(bbo_ts),
                                                    Some(mid),
                                                );
                                                let (bid_levels, ask_levels) =
                                                    if let Some(e) = entry {
                                                        (
                                                            level_from_option(Some((
                                                                e.bid_px, e.bid_qty,
                                                            ))),
                                                            level_from_option(Some((
                                                                e.ask_px, e.ask_qty,
                                                            ))),
                                                        )
                                                    } else {
                                                        let (bid_vec, ask_vec) =
                                                            bybit_book.top_levels_f64(1);
                                                        (
                                                            levels_to_array(&bid_vec),
                                                            levels_to_array(&ask_vec),
                                                        )
                                                    };
                                                {
                                                    let mut st = lock_state();
                                                    let snap = &mut st.bybit.bbo;
                                                    snap.price = Some(mid);
                                                    snap.seq = snap.seq.wrapping_add(1);
                                                    snap.ts_ns = Some(bbo_ts);
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
                                                    ExchangeFeed::Bybit,
                                                    FeedKind::Bbo,
                                                    bbo_ts,
                                                    last_ts,
                                                    reject_count,
                                                );
                                            }
                                        }
                                    }
                                } else if let Some(mid) = bybit_book.mid_price_f64() {
                                    let bbo_ts = bybit_book.last_ts();
                                    match feed_gate.evaluate(
                                        ExchangeFeed::Bybit,
                                        FeedKind::Bbo,
                                        bbo_ts,
                                    ) {
                                        GateDecision::Accept => {
                                            demean.record_other(
                                                ExchangeKind::Bybit,
                                                Some(bbo_ts),
                                                Some(mid),
                                            );
                                            let (bid_vec, ask_vec) = bybit_book.top_levels_f64(1);
                                            let bid_levels = levels_to_array(&bid_vec);
                                            let ask_levels = levels_to_array(&ask_vec);
                                            {
                                                let mut st = lock_state();
                                                let snap = &mut st.bybit.bbo;
                                                snap.price = Some(mid);
                                                snap.seq = snap.seq.wrapping_add(1);
                                                snap.ts_ns = Some(bbo_ts);
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
                                                ExchangeFeed::Bybit,
                                                FeedKind::Bbo,
                                                bbo_ts,
                                                last_ts,
                                                reject_count,
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    let new_trades = bybit::update_trades(s, &mut bybit_trades);
                    if new_trades > 0 {
                        for trade in bybit_trades.iter_last(new_trades) {
                            let trade_ts = trade.ts;
                            match feed_gate.evaluate(
                                ExchangeFeed::Bybit,
                                FeedKind::Trades,
                                trade_ts,
                            ) {
                                GateDecision::Accept => {
                                    let px = (trade.px as f64)
                                        / crate::exchanges::bybit_book::PRICE_SCALE;
                                    demean.record_other(
                                        ExchangeKind::Bybit,
                                        Some(trade_ts),
                                        Some(px),
                                    );
                                    let direction = if trade.is_buyer_maker {
                                        TradeDirection::Sell
                                    } else {
                                        TradeDirection::Buy
                                    };
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.bybit;
                                        snap.trade.price = Some(px);
                                        snap.trade.seq = snap.trade.seq.wrapping_add(1);
                                        snap.trade.ts_ns = Some(trade_ts);
                                        snap.trade.direction = Some(direction);
                                        snap.trade.bid_levels = [None; 3];
                                        snap.trade.ask_levels = [None; 3];
                                        snap.trade.received_at = Some(f.recv_instant);

                                        let qty = (trade.qty as f64).abs()
                                            / crate::exchanges::bybit_book::QTY_SCALE;
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
                                        ExchangeFeed::Bybit,
                                        FeedKind::Trades,
                                        trade_ts,
                                        last_ts,
                                        reject_count,
                                    );
                                }
                            }
                        }
                    }
                    if let Some((_, ticker)) = bybit::update_tickers(s, &mut bybit_tickers) {
                        let mut st = lock_state();
                        let entry = &mut st.bybit.ticker;
                        let price_scale = crate::exchanges::bybit_book::PRICE_SCALE;
                        let qty_scale = crate::exchanges::bybit_book::QTY_SCALE;

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
                        if let Some(mult) = ticker.quanto_multiplier {
                            entry.quanto_multiplier = Some(mult);
                        }
                        if let Some(oi_val) = ticker.open_interest_value {
                            entry.open_interest_value = Some(oi_val);
                        } else if let (Some(oi), Some(mark)) =
                            (entry.open_interest, entry.mark_price)
                        {
                            let multiplier = entry.quanto_multiplier.unwrap_or(1.0);
                            entry.open_interest_value = Some(oi * mark * multiplier);
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
            }

            // Binance
            if let Ok(f) = binance_c.try_pop() {
                progressed = true;
                let ts = f.ts;
                if let Ok(s) = core::str::from_utf8(&f.raw) {
                    #[cfg(feature = "binance_book")]
                    if let Some((_feed, _)) = binance::events_for_book(s, &mut binance_book) {
                        if let Some(mid) = binance_book.mid_price_f64() {
                            let ob_ts = binance_book.last_ts();
                            match feed_gate.evaluate(
                                ExchangeFeed::Binance,
                                FeedKind::OrderBook,
                                ob_ts,
                            ) {
                                GateDecision::Accept => {
                                    demean.record_other(
                                        ExchangeKind::Binance,
                                        Some(ob_ts),
                                        Some(mid),
                                    );
                                    let (bid_vec, ask_vec) = binance_book.top_levels_f64(3);
                                    let bid_levels = levels_to_array(&bid_vec);
                                    let ask_levels = levels_to_array(&ask_vec);
                                    let mut st = lock_state();
                                    let snap = &mut st.binance.orderbook;
                                    snap.price = Some(mid);
                                    snap.seq = snap.seq.wrapping_add(1);
                                    snap.ts_ns = Some(ob_ts);
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
                                        ExchangeFeed::Binance,
                                        FeedKind::OrderBook,
                                        ob_ts,
                                        last_ts,
                                        reject_count,
                                    );
                                }
                            }
                        }
                    }
                    if binance::update_bbo_store(s, &mut binance_bbo) {
                        if let Some(mid) = binance_bbo
                            .mid_price_f64_for(&binance_symbol)
                            .or_else(|| binance_bbo.mid_price_f64())
                        {
                            let entry = binance_bbo.get(&binance_symbol).copied().or_else(|| {
                                binance_bbo
                                    .last_symbol()
                                    .and_then(|symbol| binance_bbo.get(symbol).copied())
                            });
                            #[cfg(feature = "binance_book")]
                            let fallback_ts = binance_book.last_ts();
                            #[cfg(not(feature = "binance_book"))]
                            let fallback_ts = 0;
                            let bbo_ts = entry.map(|e| e.ts).unwrap_or(fallback_ts);
                            match feed_gate.evaluate(ExchangeFeed::Binance, FeedKind::Bbo, bbo_ts) {
                                GateDecision::Accept => {
                                    demean.record_other(
                                        ExchangeKind::Binance,
                                        Some(bbo_ts),
                                        Some(mid),
                                    );
                                    #[cfg(feature = "binance_book")]
                                    let (bid_levels, ask_levels) = if let Some(e) = entry {
                                        (
                                            level_from_option(Some((e.bid_px, e.bid_qty))),
                                            level_from_option(Some((e.ask_px, e.ask_qty))),
                                        )
                                    } else {
                                        let (bid_vec, ask_vec) = binance_book.top_levels_f64(1);
                                        (levels_to_array(&bid_vec), levels_to_array(&ask_vec))
                                    };
                                    #[cfg(not(feature = "binance_book"))]
                                    let (bid_levels, ask_levels) = if let Some(e) = entry {
                                        (
                                            level_from_option(Some((e.bid_px, e.bid_qty))),
                                            level_from_option(Some((e.ask_px, e.ask_qty))),
                                        )
                                    } else {
                                        ([None; 3], [None; 3])
                                    };
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.binance.bbo;
                                        snap.price = Some(mid);
                                        snap.seq = snap.seq.wrapping_add(1);
                                        snap.ts_ns = Some(bbo_ts);
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
                                        ExchangeFeed::Binance,
                                        FeedKind::Bbo,
                                        bbo_ts,
                                        last_ts,
                                        reject_count,
                                    );
                                }
                            }
                        }
                    }
                    let new_trades = binance::update_trades(s, &mut binance_trades);
                    if new_trades > 0 {
                        for trade in binance_trades.iter_last(new_trades) {
                            let trade_ts = trade.ts;
                            match feed_gate.evaluate(
                                ExchangeFeed::Binance,
                                FeedKind::Trades,
                                trade_ts,
                            ) {
                                GateDecision::Accept => {
                                    let px = (trade.px as f64) / binance::PRICE_SCALE;
                                    demean.record_other(
                                        ExchangeKind::Binance,
                                        Some(trade_ts),
                                        Some(px),
                                    );
                                    let direction = if trade.is_buyer_maker {
                                        TradeDirection::Sell
                                    } else {
                                        TradeDirection::Buy
                                    };
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.binance;
                                        snap.trade.price = Some(px);
                                        snap.trade.seq = snap.trade.seq.wrapping_add(1);
                                        snap.trade.ts_ns = Some(trade_ts);
                                        snap.trade.direction = Some(direction);
                                        snap.trade.bid_levels = [None; 3];
                                        snap.trade.ask_levels = [None; 3];
                                        snap.trade.received_at = Some(f.recv_instant);

                                        let qty = (trade.qty as f64).abs() / binance::QTY_SCALE;
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
                                        ExchangeFeed::Binance,
                                        FeedKind::Trades,
                                        trade_ts,
                                        last_ts,
                                        reject_count,
                                    );
                                }
                            }
                        }
                    }
                    if let Some((_, ticker)) = binance::update_tickers(s, &mut binance_tickers) {
                        let mut st = lock_state();
                        let entry = &mut st.binance.ticker;

                        if ticker.ticker.last_px != 0 {
                            entry.last_price =
                                Some((ticker.ticker.last_px as f64) / binance::PRICE_SCALE);
                        }
                        if ticker.ticker.best_bid != 0 {
                            entry.best_bid =
                                Some((ticker.ticker.best_bid as f64) / binance::PRICE_SCALE);
                        }
                        if ticker.ticker.best_ask != 0 {
                            entry.best_ask =
                                Some((ticker.ticker.best_ask as f64) / binance::PRICE_SCALE);
                        }
                        if ticker.ticker.last_qty != 0 {
                            entry.last_qty =
                                Some((ticker.ticker.last_qty as f64) / binance::QTY_SCALE);
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
                        } else if let (Some(oi), Some(mark)) =
                            (entry.open_interest, entry.mark_price)
                        {
                            let multiplier = entry.quanto_multiplier.unwrap_or(1.0);
                            entry.open_interest_value = Some(oi * mark * multiplier);
                        }
                        if let Some(mult) = ticker.quanto_multiplier {
                            entry.quanto_multiplier = Some(mult);
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
            }

            // Gate
            if let Ok(f) = gate_c.try_pop() {
                progressed = true;
                let ts = f.ts;
                if let Ok(s) = core::str::from_utf8(&f.raw) {
                    for (feed, _) in gate::events_for(s, &mut gate_book) {
                        if feed == "orderbook" {
                            if let Some(mid) = gate_book.mid_price_f64() {
                                let ob_ts = gate_book.last_ts();
                                match feed_gate.evaluate(
                                    ExchangeFeed::Gate,
                                    FeedKind::OrderBook,
                                    ob_ts,
                                ) {
                                    GateDecision::Accept => {
                                        send_fast_event(
                                            mid,
                                            "gate_orderbook",
                                            Some(ob_ts),
                                            f.recv_instant,
                                        );
                                        let (bid_vec, ask_vec) = gate_book.top_levels_f64(3);
                                        let bid_levels = levels_to_array(&bid_vec);
                                        let ask_levels = levels_to_array(&ask_vec);
                                        {
                                            let mut st = lock_state();
                                            let snap = &mut st.gate.orderbook;
                                            snap.price = Some(mid);
                                            snap.seq = snap.seq.wrapping_add(1);
                                            snap.ts_ns = Some(ob_ts);
                                            snap.bid_levels = bid_levels;
                                            snap.ask_levels = ask_levels;
                                            snap.direction = None;
                                            snap.received_at = Some(f.recv_instant);
                                        }
                                        let updates = demean.on_gate_event(Some(ob_ts), Some(mid));
                                        apply_demean(&updates);
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
                    if gate::update_bbo_store(s, &mut gate_bbo) {
                        if let Some(mid) = gate_bbo
                            .mid_price_f64_for(&gate_symbol)
                            .or_else(|| gate_bbo.mid_price_f64())
                        {
                            let entry = gate_bbo.get(&gate_symbol).copied().or_else(|| {
                                gate_bbo
                                    .last_symbol()
                                    .and_then(|symbol| gate_bbo.get(symbol).copied())
                            });
                            let bbo_ts = entry.map(|e| e.ts).unwrap_or_else(|| gate_book.last_ts());
                            match feed_gate.evaluate(ExchangeFeed::Gate, FeedKind::Bbo, bbo_ts) {
                                GateDecision::Accept => {
                                    send_fast_event(mid, "gate_bbo", Some(bbo_ts), f.recv_instant);
                                    let (bid_levels, ask_levels) = if let Some(e) = entry {
                                        (
                                            level_from_option(Some((e.bid_px, e.bid_qty))),
                                            level_from_option(Some((e.ask_px, e.ask_qty))),
                                        )
                                    } else {
                                        let (bid_vec, ask_vec) = gate_book.top_levels_f64(1);
                                        (levels_to_array(&bid_vec), levels_to_array(&ask_vec))
                                    };
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.gate.bbo;
                                        snap.price = Some(mid);
                                        snap.seq = snap.seq.wrapping_add(1);
                                        snap.ts_ns = Some(bbo_ts);
                                        snap.bid_levels = bid_levels;
                                        snap.ask_levels = ask_levels;
                                        snap.direction = None;
                                        snap.received_at = Some(f.recv_instant);
                                    }
                                    let updates = demean.on_gate_event(Some(bbo_ts), Some(mid));
                                    apply_demean(&updates);
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
                    let new_trades = gate::update_trades(s, &mut gate_trades);
                    if new_trades > 0 {
                        for trade in gate_trades.iter_last(new_trades) {
                            let trade_ts = trade.ts;
                            match feed_gate.evaluate(ExchangeFeed::Gate, FeedKind::Trades, trade_ts)
                            {
                                GateDecision::Accept => {
                                    let px = (trade.px as f64) / gate::PRICE_SCALE;
                                    send_fast_event(
                                        px,
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
                                        snap.trade.ts_ns = Some(trade_ts);
                                        snap.trade.direction = Some(direction);
                                        snap.trade.bid_levels = [None; 3];
                                        snap.trade.ask_levels = [None; 3];
                                        snap.trade.received_at = Some(f.recv_instant);

                                        let qty = (trade.qty as f64).abs() / gate::QTY_SCALE;
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
                                    let updates = demean.on_gate_event(Some(trade_ts), Some(px));
                                    apply_demean(&updates);
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
                    if let Some((symbol, mut ticker)) = gate::update_tickers(s, &mut gate_tickers) {
                        let mut needs_store_update = false;

                        if ticker.quanto_multiplier.is_none() {
                            if let Some(mult) = gate_contract_meta
                                .as_ref()
                                .and_then(|meta| meta.quanto_multiplier)
                            {
                                ticker.quanto_multiplier = Some(mult);
                                needs_store_update = true;
                            }
                        }

                        if ticker.open_interest_value.is_none() {
                            if let (Some(oi), Some(mark)) = (ticker.open_interest, ticker.mark_px) {
                                let multiplier = ticker
                                    .quanto_multiplier
                                    .or_else(|| {
                                        gate_contract_meta
                                            .as_ref()
                                            .and_then(|meta| meta.quanto_multiplier)
                                    })
                                    .unwrap_or(1.0);
                                ticker.open_interest_value = Some(oi * mark * multiplier);
                                needs_store_update = true;
                            }
                        }

                        if needs_store_update {
                            ticker = gate_tickers.update(symbol.clone(), ticker);
                        }

                        let mut st = lock_state();
                        let entry = &mut st.gate.ticker;

                        if ticker.ticker.last_px != 0 {
                            entry.last_price =
                                Some((ticker.ticker.last_px as f64) / gate::PRICE_SCALE);
                        }
                        if ticker.ticker.last_qty != 0 {
                            entry.last_qty =
                                Some((ticker.ticker.last_qty as f64) / gate::QTY_SCALE);
                        }
                        if ticker.ticker.best_bid != 0 {
                            entry.best_bid =
                                Some((ticker.ticker.best_bid as f64) / gate::PRICE_SCALE);
                        }
                        if ticker.ticker.best_ask != 0 {
                            entry.best_ask =
                                Some((ticker.ticker.best_ask as f64) / gate::PRICE_SCALE);
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
                        if let Some(mult) = ticker.quanto_multiplier {
                            entry.quanto_multiplier = Some(mult);
                        } else if entry.quanto_multiplier.is_none() {
                            if let Some(mult) = gate_contract_meta
                                .as_ref()
                                .and_then(|meta| meta.quanto_multiplier)
                            {
                                entry.quanto_multiplier = Some(mult);
                            }
                        }

                        if entry.open_interest_value.is_none() {
                            entry.open_interest_value = ticker.open_interest_value;
                        } else if ticker.open_interest_value.is_some() {
                            entry.open_interest_value = ticker.open_interest_value;
                        } else if let (Some(oi), Some(mark)) =
                            (entry.open_interest, entry.mark_price)
                        {
                            let multiplier = entry.quanto_multiplier.unwrap_or(1.0);
                            entry.open_interest_value = Some(oi * mark * multiplier);
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
            }

            // Bitget
            if let Ok(f) = bitget_c.try_pop() {
                progressed = true;
                let ts = f.ts;
                if let Ok(s) = core::str::from_utf8(&f.raw) {
                    for (feed, _) in bitget::events_for(s, &mut bitget_book) {
                        if feed == "orderbook" {
                            if let Some(mid) = bitget_book.mid_price_f64() {
                                let ob_ts = bitget_book.last_ts();
                                match feed_gate.evaluate(
                                    ExchangeFeed::Bitget,
                                    FeedKind::OrderBook,
                                    ob_ts,
                                ) {
                                    GateDecision::Accept => {
                                        demean.record_other(
                                            ExchangeKind::Bitget,
                                            Some(ob_ts),
                                            Some(mid),
                                        );
                                        let (bid_vec, ask_vec) = bitget_book.top_levels_f64(3);
                                        let bid_levels = levels_to_array(&bid_vec);
                                        let ask_levels = levels_to_array(&ask_vec);
                                        let mut st = lock_state();
                                        let snap = &mut st.bitget.orderbook;
                                        snap.price = Some(mid);
                                        snap.seq = snap.seq.wrapping_add(1);
                                        snap.ts_ns = Some(ob_ts);
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
                    if bitget::update_bbo_store(s, &mut bitget_bbo) {
                        if let Some(mid) = bitget_bbo
                            .mid_price_f64_for(&bitget_symbol)
                            .or_else(|| bitget_bbo.mid_price_f64())
                        {
                            let entry = bitget_bbo.get(&bitget_symbol).copied().or_else(|| {
                                bitget_bbo
                                    .last_symbol()
                                    .and_then(|symbol| bitget_bbo.get(symbol).copied())
                            });
                            let bbo_ts =
                                entry.map(|e| e.ts).unwrap_or_else(|| bitget_book.last_ts());
                            match feed_gate.evaluate(ExchangeFeed::Bitget, FeedKind::Bbo, bbo_ts) {
                                GateDecision::Accept => {
                                    demean.record_other(
                                        ExchangeKind::Bitget,
                                        Some(bbo_ts),
                                        Some(mid),
                                    );
                                    let (bid_levels, ask_levels) = if let Some(e) = entry {
                                        (
                                            level_from_option(Some((e.bid_px, e.bid_qty))),
                                            level_from_option(Some((e.ask_px, e.ask_qty))),
                                        )
                                    } else {
                                        let (bid_vec, ask_vec) = bitget_book.top_levels_f64(1);
                                        (levels_to_array(&bid_vec), levels_to_array(&ask_vec))
                                    };
                                    {
                                        let mut st = lock_state();
                                        let snap = &mut st.bitget.bbo;
                                        snap.price = Some(mid);
                                        snap.seq = snap.seq.wrapping_add(1);
                                        snap.ts_ns = Some(bbo_ts);
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
                    let new_trades = bitget::update_trades(s, &mut bitget_trades);
                    if new_trades > 0 {
                        for trade in bitget_trades.iter_last(new_trades) {
                            let trade_ts = trade.ts;
                            match feed_gate.evaluate(
                                ExchangeFeed::Bitget,
                                FeedKind::Trades,
                                trade_ts,
                            ) {
                                GateDecision::Accept => {
                                    let px = (trade.px as f64) / bitget::PRICE_SCALE;
                                    demean.record_other(
                                        ExchangeKind::Bitget,
                                        Some(trade_ts),
                                        Some(px),
                                    );
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
                                        snap.trade.ts_ns = Some(trade_ts);
                                        snap.trade.direction = Some(direction);
                                        snap.trade.bid_levels = [None; 3];
                                        snap.trade.ask_levels = [None; 3];
                                        snap.trade.received_at = Some(f.recv_instant);

                                        let qty = (trade.qty as f64).abs() / bitget::QTY_SCALE;
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
                    if let Some((_, ticker)) = bitget::update_tickers(s, &mut bitget_tickers) {
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
                        } else if let (Some(oi), Some(mark)) =
                            (entry.open_interest, entry.mark_price)
                        {
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
            }

            if progressed {
                publisher.publish();
            } else {
                wake_signal.wait();
            }
        }
    })
}

// ReferencePublisher moved to base_classes/reference_publisher.rs
