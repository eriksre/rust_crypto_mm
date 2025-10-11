use std::str;
use std::thread;
use std::time::Duration;

use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::binance::BinanceHandler;
use rust_test::exchanges::bitget::{BitgetFrame, BitgetHandler};
use rust_test::exchanges::bybit::{BybitFrame, BybitHandler};
use rust_test::exchanges::endpoints::{BitgetWs, GateioWs};
use rust_test::exchanges::gate::{GateFrame, GateHandler};

#[path = "../bin_utils/symbol_config.rs"]
mod symbol_config;

fn is_bybit_trade(frame: &BybitFrame) -> bool {
    frame
        .topic()
        .map(|topic| topic.starts_with("publicTrade"))
        .unwrap_or(false)
}

fn is_binance_trade(frame: &rust_test::exchanges::binance::BinanceFrame) -> bool {
    frame.topic() == "aggTrade"
}

fn is_gate_trade(frame: &GateFrame) -> bool {
    frame.channel() == GateioWs::PUBLIC_TRADES && frame.event() == "update"
}

fn is_bitget_trade(frame: &BitgetFrame) -> bool {
    frame.channel() == BitgetWs::PUBLIC_TRADES
        && (frame.action() == "update" || frame.event() == "update")
}

fn main() {
    let symbol = std::env::args()
        .nth(1)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(symbol_config::default_symbol);
    let limit = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok());

    const N: usize = 1 << 14;
    let (bybit_c, _jh1) =
        spawn_ws_worker::<BybitHandler, N>(BybitHandler::new(symbol.clone()), None, None);
    let (binance_c, _jh2) =
        spawn_ws_worker::<BinanceHandler, N>(BinanceHandler::new(symbol.clone()), None, None);
    let (gate_c, _jh3) =
        spawn_ws_worker::<GateHandler, N>(GateHandler::new(symbol.clone()), None, None);
    let (bitget_c, _jh4) =
        spawn_ws_worker::<BitgetHandler, N>(BitgetHandler::new(symbol.clone()), None, None);

    eprintln!(
        "Streaming raw trade frames for {symbol}. Ctrl-C to stop. Use optional second argument for per-exchange limit."
    );

    let mut counts = [0usize; 4];
    loop {
        let mut progressed = false;

        if limit.map_or(true, |cap| counts[0] < cap) {
            while let Ok(frame) = bybit_c.try_pop() {
                progressed = true;
                if !is_bybit_trade(&frame) {
                    continue;
                }
                if let Ok(text) = str::from_utf8(&frame.raw) {
                    println!("BYBIT: {text}");
                }
                counts[0] += 1;
                if limit.map_or(false, |cap| counts[0] >= cap) {
                    break;
                }
            }
        }

        if limit.map_or(true, |cap| counts[1] < cap) {
            while let Ok(frame) = binance_c.try_pop() {
                progressed = true;
                if !is_binance_trade(&frame) {
                    continue;
                }
                if let Ok(text) = str::from_utf8(&frame.raw) {
                    println!("BINANCE: {text}");
                }
                counts[1] += 1;
                if limit.map_or(false, |cap| counts[1] >= cap) {
                    break;
                }
            }
        }

        if limit.map_or(true, |cap| counts[2] < cap) {
            while let Ok(frame) = gate_c.try_pop() {
                progressed = true;
                if !is_gate_trade(&frame) {
                    continue;
                }
                if let Ok(text) = str::from_utf8(&frame.raw) {
                    println!("GATE: {text}");
                }
                counts[2] += 1;
                if limit.map_or(false, |cap| counts[2] >= cap) {
                    break;
                }
            }
        }

        if limit.map_or(true, |cap| counts[3] < cap) {
            while let Ok(frame) = bitget_c.try_pop() {
                progressed = true;
                if !is_bitget_trade(&frame) {
                    continue;
                }
                if let Ok(text) = str::from_utf8(&frame.raw) {
                    println!("BITGET: {text}");
                }
                counts[3] += 1;
                if limit.map_or(false, |cap| counts[3] >= cap) {
                    break;
                }
            }
        }

        if let Some(cap) = limit {
            if counts.iter().all(|&c| c >= cap) {
                break;
            }
        }

        if !progressed {
            thread::sleep(Duration::from_millis(2));
        }
    }
}
