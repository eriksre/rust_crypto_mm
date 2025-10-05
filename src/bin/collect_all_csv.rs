use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;
use std::time::Duration;

use rust_test::base_classes::engine::spawn_state_engine;
use rust_test::base_classes::state::{TradeDirection, state};

#[inline(always)]
fn direction_str(dir: Option<TradeDirection>) -> &'static str {
    dir.map_or("", |d| d.as_str())
}

fn main() {
    // Args: SYMBOL [output.csv]
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let out_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "all_exchanges.csv".to_string());

    // Spawn background engine: producers + processors maintain state. This process only writes snapshots.
    let _engine = spawn_state_engine(symbol.clone());

    eprintln!("Collecting mids + trades to {out_path}. Ctrl-C to stop.");
    let file = File::create(&out_path).expect("create csv");
    let mut w = BufWriter::new(file);
    writeln!(w, "ts_ns,exchange,feed,price,direction").unwrap();

    // Last seen seq markers per feed to write only new updates
    let mut last_bybit = (0u64, 0u64, 0u64);
    let mut last_binance = (0u64, 0u64, 0u64);
    let mut last_gate = (0u64, 0u64, 0u64);
    let mut last_bitget = (0u64, 0u64, 0u64);

    loop {
        let st = state().lock().unwrap();
        // bybit
        if st.bybit.orderbook.seq != last_bybit.0 {
            if let Some(p) = st.bybit.orderbook.price {
                let ts = st.bybit.orderbook.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "bybit",
                    "orderbook",
                    p,
                    direction_str(st.bybit.orderbook.direction),
                )
                .ok();
                last_bybit.0 = st.bybit.orderbook.seq;
            }
        }
        if st.bybit.bbo.seq != last_bybit.1 {
            if let Some(p) = st.bybit.bbo.price {
                let ts = st.bybit.bbo.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "bybit",
                    "bbo",
                    p,
                    direction_str(st.bybit.bbo.direction),
                )
                .ok();
                last_bybit.1 = st.bybit.bbo.seq;
            }
        }
        if st.bybit.trade.seq != last_bybit.2 {
            if let Some(p) = st.bybit.trade.price {
                let ts = st.bybit.trade.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "bybit",
                    "trade",
                    p,
                    direction_str(st.bybit.trade.direction),
                )
                .ok();
                last_bybit.2 = st.bybit.trade.seq;
            }
        }
        // binance
        if st.binance.orderbook.seq != last_binance.0 {
            if let Some(p) = st.binance.orderbook.price {
                let ts = st.binance.orderbook.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "binance",
                    "orderbook",
                    p,
                    direction_str(st.binance.orderbook.direction),
                )
                .ok();
                last_binance.0 = st.binance.orderbook.seq;
            }
        }
        if st.binance.bbo.seq != last_binance.1 {
            if let Some(p) = st.binance.bbo.price {
                let ts = st.binance.bbo.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "binance",
                    "bbo",
                    p,
                    direction_str(st.binance.bbo.direction),
                )
                .ok();
                last_binance.1 = st.binance.bbo.seq;
            }
        }
        if st.binance.trade.seq != last_binance.2 {
            if let Some(p) = st.binance.trade.price {
                let ts = st.binance.trade.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "binance",
                    "trade",
                    p,
                    direction_str(st.binance.trade.direction),
                )
                .ok();
                last_binance.2 = st.binance.trade.seq;
            }
        }
        // gate
        if st.gate.orderbook.seq != last_gate.0 {
            if let Some(p) = st.gate.orderbook.price {
                let ts = st.gate.orderbook.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "gate",
                    "orderbook",
                    p,
                    direction_str(st.gate.orderbook.direction),
                )
                .ok();
                last_gate.0 = st.gate.orderbook.seq;
            }
        }
        if st.gate.bbo.seq != last_gate.1 {
            if let Some(p) = st.gate.bbo.price {
                let ts = st.gate.bbo.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "gate",
                    "bbo",
                    p,
                    direction_str(st.gate.bbo.direction),
                )
                .ok();
                last_gate.1 = st.gate.bbo.seq;
            }
        }
        if st.gate.trade.seq != last_gate.2 {
            if let Some(p) = st.gate.trade.price {
                let ts = st.gate.trade.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "gate",
                    "trade",
                    p,
                    direction_str(st.gate.trade.direction),
                )
                .ok();
                last_gate.2 = st.gate.trade.seq;
            }
        }
        // bitget
        if st.bitget.orderbook.seq != last_bitget.0 {
            if let Some(p) = st.bitget.orderbook.price {
                let ts = st.bitget.orderbook.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "bitget",
                    "orderbook",
                    p,
                    direction_str(st.bitget.orderbook.direction),
                )
                .ok();
                last_bitget.0 = st.bitget.orderbook.seq;
            }
        }
        if st.bitget.bbo.seq != last_bitget.1 {
            if let Some(p) = st.bitget.bbo.price {
                let ts = st.bitget.bbo.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "bitget",
                    "bbo",
                    p,
                    direction_str(st.bitget.bbo.direction),
                )
                .ok();
                last_bitget.1 = st.bitget.bbo.seq;
            }
        }
        if st.bitget.trade.seq != last_bitget.2 {
            if let Some(p) = st.bitget.trade.price {
                let ts = st.bitget.trade.ts_ns.unwrap_or(0);
                writeln!(
                    w,
                    "{},{},{},{},{}",
                    ts,
                    "bitget",
                    "trade",
                    p,
                    direction_str(st.bitget.trade.direction),
                )
                .ok();
                last_bitget.2 = st.bitget.trade.seq;
            }
        }
        drop(st);

        let _ = w.flush();
        thread::sleep(Duration::from_millis(1));
    }
}
