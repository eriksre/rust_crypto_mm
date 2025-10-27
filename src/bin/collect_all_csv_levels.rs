use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;
use std::time::Duration;

use rust_test::base_classes::engine::{configure_feed_overrides, spawn_state_engine};
use rust_test::base_classes::feed_config::FeedToggles;
use rust_test::base_classes::state::{ExchangeAdjustment, FeedSnap, TradeDirection, state};

#[path = "../bin_utils/symbol_config.rs"]
mod symbol_config;

#[inline(always)]
fn direction_str(dir: Option<TradeDirection>) -> &'static str {
    dir.map_or("", |d| d.as_str())
}

fn write_levels<W: Write>(writer: &mut W, levels: &[Option<(f64, f64)>; 3]) -> std::io::Result<()> {
    for level in levels.iter() {
        if let Some((px, qty)) = level {
            write!(writer, ",{},{}", px, qty)?;
        } else {
            write!(writer, ",,")?;
        }
    }
    Ok(())
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

fn write_feed_line<W: Write>(
    writer: &mut W,
    exchange: &str,
    feed: &str,
    snap: &FeedSnap,
) -> std::io::Result<()> {
    write!(writer, "{},{},{},", snap.ts_ns.unwrap_or(0), exchange, feed)?;
    if let Some(price) = snap.price {
        write!(writer, "{}", price)?;
    }
    write!(writer, ",{}", direction_str(snap.direction))?;
    write_levels(writer, &snap.bid_levels)?;
    write_levels(writer, &snap.ask_levels)?;
    writeln!(writer)
}

fn main() {
    let symbol = std::env::args()
        .nth(1)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(symbol_config::default_symbol);
    let out_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "all_exchanges_depth.csv".to_string());

    configure_feed_overrides(FeedToggles::default());
    let _engine = spawn_state_engine(symbol.clone(), None, None);

    eprintln!(
        "Collecting orderbook top levels + trades for {symbol} to {out_path}. Ctrl-C to stop.",
        symbol = symbol,
        out_path = out_path
    );
    let file = File::create(&out_path).expect("create csv");
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "ts_ns,exchange,feed,price,direction,bid_px_1,bid_qty_1,bid_px_2,bid_qty_2,bid_px_3,bid_qty_3,ask_px_1,ask_qty_1,ask_px_2,ask_qty_2,ask_px_3,ask_qty_3"
    )
    .unwrap();

    let mut last_bybit = (0u64, 0u64, 0u64);
    let mut last_binance = (0u64, 0u64, 0u64);
    let mut last_gate = (0u64, 0u64, 0u64);
    let mut last_bitget = (0u64, 0u64, 0u64);

    loop {
        {
            let mut st = state().lock().unwrap();

            if st.bybit.orderbook.seq != last_bybit.0 && st.bybit.orderbook.price.is_some() {
                write_feed_line(&mut writer, "bybit", "orderbook", &st.bybit.orderbook).ok();
                last_bybit.0 = st.bybit.orderbook.seq;
            }
            if st.bybit.bbo.seq != last_bybit.1 && st.bybit.bbo.price.is_some() {
                write_feed_line(&mut writer, "bybit", "bbo", &st.bybit.bbo).ok();
                last_bybit.1 = st.bybit.bbo.seq;
            }
            while let Some(event) = st.bybit.trade_events.pop_front() {
                if let Some(price) = restore_price(Some(event.price), &st.demean.bybit) {
                    let mut snap = FeedSnap::default();
                    snap.price = Some(price);
                    snap.ts_ns = Some(event.ts_ns);
                    snap.direction = event.direction;
                    write_feed_line(&mut writer, "bybit", "trade", &snap).ok();
                }
            }
            last_bybit.2 = st.bybit.trade.seq;

            if st.binance.orderbook.seq != last_binance.0 && st.binance.orderbook.price.is_some() {
                write_feed_line(&mut writer, "binance", "orderbook", &st.binance.orderbook).ok();
                last_binance.0 = st.binance.orderbook.seq;
            }
            if st.binance.bbo.seq != last_binance.1 && st.binance.bbo.price.is_some() {
                write_feed_line(&mut writer, "binance", "bbo", &st.binance.bbo).ok();
                last_binance.1 = st.binance.bbo.seq;
            }
            while let Some(event) = st.binance.trade_events.pop_front() {
                if let Some(price) = restore_price(Some(event.price), &st.demean.binance) {
                    let mut snap = FeedSnap::default();
                    snap.price = Some(price);
                    snap.ts_ns = Some(event.ts_ns);
                    snap.direction = event.direction;
                    write_feed_line(&mut writer, "binance", "trade", &snap).ok();
                }
            }
            last_binance.2 = st.binance.trade.seq;

            if st.gate.orderbook.seq != last_gate.0 && st.gate.orderbook.price.is_some() {
                write_feed_line(&mut writer, "gate", "orderbook", &st.gate.orderbook).ok();
                last_gate.0 = st.gate.orderbook.seq;
            }
            if st.gate.bbo.seq != last_gate.1 && st.gate.bbo.price.is_some() {
                write_feed_line(&mut writer, "gate", "bbo", &st.gate.bbo).ok();
                last_gate.1 = st.gate.bbo.seq;
            }
            while let Some(event) = st.gate.trade_events.pop_front() {
                let mut snap = FeedSnap::default();
                snap.price = Some(event.price);
                snap.ts_ns = Some(event.ts_ns);
                snap.direction = event.direction;
                write_feed_line(&mut writer, "gate", "trade", &snap).ok();
            }
            last_gate.2 = st.gate.trade.seq;

            if st.bitget.orderbook.seq != last_bitget.0 && st.bitget.orderbook.price.is_some() {
                write_feed_line(&mut writer, "bitget", "orderbook", &st.bitget.orderbook).ok();
                last_bitget.0 = st.bitget.orderbook.seq;
            }
            if st.bitget.bbo.seq != last_bitget.1 && st.bitget.bbo.price.is_some() {
                write_feed_line(&mut writer, "bitget", "bbo", &st.bitget.bbo).ok();
                last_bitget.1 = st.bitget.bbo.seq;
            }
            while let Some(event) = st.bitget.trade_events.pop_front() {
                if let Some(price) = restore_price(Some(event.price), &st.demean.bitget) {
                    let mut snap = FeedSnap::default();
                    snap.price = Some(price);
                    snap.ts_ns = Some(event.ts_ns);
                    snap.direction = event.direction;
                    write_feed_line(&mut writer, "bitget", "trade", &snap).ok();
                }
            }
            last_bitget.2 = st.bitget.trade.seq;
        }

        let _ = writer.flush();
        thread::sleep(Duration::from_millis(1));
    }
}
