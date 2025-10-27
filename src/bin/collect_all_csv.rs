use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;
use std::time::Duration;

use rust_test::base_classes::engine::{configure_feed_overrides, spawn_state_engine};
use rust_test::base_classes::feed_config::FeedToggles;
use rust_test::base_classes::state::{ExchangeAdjustment, TradeDirection, state};

#[path = "../bin_utils/symbol_config.rs"]
mod symbol_config;

#[inline(always)]
fn direction_str(dir: Option<TradeDirection>) -> &'static str {
    dir.map_or("", |d| d.as_str())
}

#[inline(always)]
fn write_csv_row(
    writer: &mut BufWriter<File>,
    ts: u64,
    exchange: &str,
    feed: &str,
    price: f64,
    direction: Option<TradeDirection>,
    quantity: Option<f64>,
    role: Option<&str>,
    info: Option<&str>,
) {
    let dir = direction_str(direction);
    let qty = quantity
        .map(|q| format!("{q:.8}"))
        .unwrap_or_else(String::new);
    let role_str = role.unwrap_or("");
    let info_str = info.unwrap_or("");
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{}",
        ts, exchange, feed, price, dir, qty, role_str, info_str
    )
    .ok();
}

#[inline(always)]
fn restore_price(price: Option<f64>, adj: &ExchangeAdjustment) -> Option<f64> {
    match price {
        Some(px) if px.is_finite() && px > 0.0 => {
            if adj.samples > 0 {
                Some(px + adj.offset.unwrap_or(0.0))
            } else {
                Some(px)
            }
        }
        _ => None,
    }
}

fn main() {
    // Args: SYMBOL [output.csv]
    let default_symbol = symbol_config::default_symbol();
    let symbol = std::env::args()
        .nth(1)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default_symbol.clone());
    let out_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "all_exchanges.csv".to_string());

    // Ensure feed toggles start with defaults (all primary feeds enabled/auto).
    configure_feed_overrides(FeedToggles::default());

    // Spawn background engine: producers + processors maintain state. This process only writes snapshots.
    let _engine = spawn_state_engine(symbol.clone(), None, None);

    eprintln!(
        "Collecting mids + trades for {symbol} to {out_path}. Ctrl-C to stop.",
        symbol = symbol,
        out_path = out_path
    );
    let file = File::create(&out_path).expect("create csv");
    let mut w = BufWriter::new(file);
    writeln!(w, "ts_ns,exchange,feed,price,direction,quantity,role,info").unwrap();

    // Last seen seq markers per feed to write only new updates
    let mut last_bybit = (0u64, 0u64, 0u64);
    let mut last_binance = (0u64, 0u64, 0u64);
    let mut last_gate = (0u64, 0u64, 0u64);
    let mut last_gate_user_trade = 0u64;
    let mut last_bitget = (0u64, 0u64, 0u64);

    loop {
        let mut st = state().lock().unwrap();
        // bybit
        if st.bybit.orderbook.seq != last_bybit.0 {
            if let Some(p) = restore_price(st.bybit.orderbook.price, &st.demean.bybit) {
                let ts = st.bybit.orderbook.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "bybit",
                    "orderbook",
                    p,
                    st.bybit.orderbook.direction,
                    None,
                    None,
                    None,
                );
                last_bybit.0 = st.bybit.orderbook.seq;
            }
        }
        if st.bybit.bbo.seq != last_bybit.1 {
            if let Some(p) = restore_price(st.bybit.bbo.price, &st.demean.bybit) {
                let ts = st.bybit.bbo.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "bybit",
                    "bbo",
                    p,
                    st.bybit.bbo.direction,
                    None,
                    None,
                    None,
                );
                last_bybit.1 = st.bybit.bbo.seq;
            }
        }
        while let Some(event) = st.bybit.trade_events.pop_front() {
            if let Some(price) = restore_price(Some(event.price), &st.demean.bybit) {
                write_csv_row(
                    &mut w,
                    event.ts_ns,
                    "bybit",
                    "trade",
                    price,
                    event.direction,
                    event.quantity,
                    None,
                    None,
                );
            }
        }
        last_bybit.2 = st.bybit.trade.seq;
        // binance
        if st.binance.orderbook.seq != last_binance.0 {
            if let Some(p) = restore_price(st.binance.orderbook.price, &st.demean.binance) {
                let ts = st.binance.orderbook.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "binance",
                    "orderbook",
                    p,
                    st.binance.orderbook.direction,
                    None,
                    None,
                    None,
                );
                last_binance.0 = st.binance.orderbook.seq;
            }
        }
        if st.binance.bbo.seq != last_binance.1 {
            if let Some(p) = restore_price(st.binance.bbo.price, &st.demean.binance) {
                let ts = st.binance.bbo.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "binance",
                    "bbo",
                    p,
                    st.binance.bbo.direction,
                    None,
                    None,
                    None,
                );
                last_binance.1 = st.binance.bbo.seq;
            }
        }
        while let Some(event) = st.binance.trade_events.pop_front() {
            if let Some(price) = restore_price(Some(event.price), &st.demean.binance) {
                write_csv_row(
                    &mut w,
                    event.ts_ns,
                    "binance",
                    "trade",
                    price,
                    event.direction,
                    event.quantity,
                    None,
                    None,
                );
            }
        }
        last_binance.2 = st.binance.trade.seq;
        // gate
        if st.gate.orderbook.seq != last_gate.0 {
            if let Some(p) = st.gate.orderbook.price {
                let ts = st.gate.orderbook.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "gate",
                    "orderbook",
                    p,
                    st.gate.orderbook.direction,
                    None,
                    None,
                    None,
                );
                last_gate.0 = st.gate.orderbook.seq;
            }
        }
        if st.gate.bbo.seq != last_gate.1 {
            if let Some(p) = st.gate.bbo.price {
                let ts = st.gate.bbo.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "gate",
                    "bbo",
                    p,
                    st.gate.bbo.direction,
                    None,
                    None,
                    None,
                );
                last_gate.1 = st.gate.bbo.seq;
            }
        }
        while let Some(event) = st.gate.trade_events.pop_front() {
            write_csv_row(
                &mut w,
                event.ts_ns,
                "gate",
                "trade",
                event.price,
                event.direction,
                event.quantity,
                None,
                None,
            );
        }
        last_gate.2 = st.gate.trade.seq;
        if st.gate.user_trade.seq != last_gate_user_trade {
            if let Some(p) = st.gate.user_trade.price {
                let ts = st.gate.user_trade.ts_ns.unwrap_or(0);
                let role = st.gate.user_trade.role.as_deref();
                let info = st.gate.user_trade.text.as_deref();
                write_csv_row(
                    &mut w,
                    ts,
                    "gate",
                    "user_trade",
                    p,
                    st.gate.user_trade.direction,
                    st.gate.user_trade.quantity,
                    role,
                    info,
                );
            }
            last_gate_user_trade = st.gate.user_trade.seq;
        }
        // bitget
        if st.bitget.orderbook.seq != last_bitget.0 {
            if let Some(p) = restore_price(st.bitget.orderbook.price, &st.demean.bitget) {
                let ts = st.bitget.orderbook.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "bitget",
                    "orderbook",
                    p,
                    st.bitget.orderbook.direction,
                    None,
                    None,
                    None,
                );
                last_bitget.0 = st.bitget.orderbook.seq;
            }
        }
        if st.bitget.bbo.seq != last_bitget.1 {
            if let Some(p) = restore_price(st.bitget.bbo.price, &st.demean.bitget) {
                let ts = st.bitget.bbo.ts_ns.unwrap_or(0);
                write_csv_row(
                    &mut w,
                    ts,
                    "bitget",
                    "bbo",
                    p,
                    st.bitget.bbo.direction,
                    None,
                    None,
                    None,
                );
                last_bitget.1 = st.bitget.bbo.seq;
            }
        }
        while let Some(event) = st.bitget.trade_events.pop_front() {
            if let Some(price) = restore_price(Some(event.price), &st.demean.bitget) {
                write_csv_row(
                    &mut w,
                    event.ts_ns,
                    "bitget",
                    "trade",
                    price,
                    event.direction,
                    event.quantity,
                    None,
                    None,
                );
            }
        }
        last_bitget.2 = st.bitget.trade.seq;
        drop(st);

        let _ = w.flush();
        thread::sleep(Duration::from_millis(1));
    }
}
