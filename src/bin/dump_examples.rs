use std::time::Duration;

use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::binance::BinanceHandler;
use rust_test::exchanges::bitget::BitgetHandler;
use rust_test::exchanges::bybit::BybitHandler;
use rust_test::exchanges::gate::GateHandler;

fn main() {
    // Args: SYMBOL [per_feed_count]
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let per_feed: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    const N: usize = 1 << 12;

    let (bybit_c, _j1) =
        spawn_ws_worker::<BybitHandler, N>(BybitHandler::new(symbol.clone()), None, None);
    let (bin_c, _j2) =
        spawn_ws_worker::<BinanceHandler, N>(BinanceHandler::new(symbol.clone()), None, None);
    let (gate_c, _j3) =
        spawn_ws_worker::<GateHandler, N>(GateHandler::new(symbol.clone()), None, None);
    let (bit_c, _j4) =
        spawn_ws_worker::<BitgetHandler, N>(BitgetHandler::new(symbol.clone()), None, None);

    // feeds: 0=bbo, 1=orderbook, 2=trade, 3=ticker
    let mut bybit_cnt = [0usize; 4];
    let mut bin_cnt = [0usize; 4];
    let mut gate_cnt = [0usize; 4];
    let mut bit_cnt = [0usize; 4];

    eprintln!(
        "Collecting ~{} examples per exchange x feed (bbo,orderbook,trade,ticker) for {}...",
        per_feed, symbol
    );
    eprintln!("Press Ctrl-C to stop early.");

    loop {
        let mut progressed = false;

        if let Ok(f) = bybit_c.try_pop() {
            if let Ok(s) = std::str::from_utf8(&f.raw) {
                if let Some((fi, label)) = detect_feed_bybit(s) {
                    if bybit_cnt[fi] < per_feed {
                        bybit_cnt[fi] += 1;
                        progressed = true;
                        println!("BYBIT [{} {}/{}]: {}", label, bybit_cnt[fi], per_feed, s);
                    }
                }
            }
        }

        if let Ok(f) = bin_c.try_pop() {
            if let Ok(s) = std::str::from_utf8(&f.raw) {
                if let Some((fi, label)) = detect_feed_binance(s) {
                    if bin_cnt[fi] < per_feed {
                        bin_cnt[fi] += 1;
                        progressed = true;
                        println!("BINANCE [{} {}/{}]: {}", label, bin_cnt[fi], per_feed, s);
                    }
                }
            }
        }

        if let Ok(f) = gate_c.try_pop() {
            if let Ok(s) = std::str::from_utf8(&f.raw) {
                if let Some((fi, label)) = detect_feed_gate(s) {
                    if gate_cnt[fi] < per_feed {
                        gate_cnt[fi] += 1;
                        progressed = true;
                        println!("GATE [{} {}/{}]: {}", label, gate_cnt[fi], per_feed, s);
                    }
                }
            }
        }

        if let Ok(f) = bit_c.try_pop() {
            if let Ok(s) = std::str::from_utf8(&f.raw) {
                if let Some((fi, label)) = detect_feed_bitget(s) {
                    if bit_cnt[fi] < per_feed {
                        bit_cnt[fi] += 1;
                        progressed = true;
                        println!("BITGET [{} {}/{}]: {}", label, bit_cnt[fi], per_feed, s);
                    }
                }
            }
        }

        let done = bybit_cnt.iter().all(|&c| c >= per_feed)
            && bin_cnt.iter().all(|&c| c >= per_feed)
            && gate_cnt.iter().all(|&c| c >= per_feed)
            && bit_cnt.iter().all(|&c| c >= per_feed);
        if done {
            break;
        }
        if !progressed {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    eprintln!("Done.");
}

fn detect_feed_bybit(s: &str) -> Option<(usize, &'static str)> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        let topic = v.get("topic")?.as_str()?;
        if topic.starts_with("orderbook.1.") {
            return Some((0, "bbo"));
        }
        if topic.starts_with("orderbook.") {
            return Some((1, "orderbook"));
        }
        if topic.starts_with("publicTrade.") {
            return Some((2, "trade"));
        }
        if topic.starts_with("tickers.") {
            return Some((3, "ticker"));
        }
    }
    None
}

fn detect_feed_binance(s: &str) -> Option<(usize, &'static str)> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        // Combined stream wraps payload in data
        let (ev, _root) = if let Some(data) = v.get("data") {
            (data.get("e")?.as_str()?, data)
        } else {
            (v.get("e")?.as_str()?, &v)
        };
        match ev {
            "bookTicker" => Some((0, "bbo")),
            "depthUpdate" => Some((1, "orderbook")),
            "aggTrade" => Some((2, "trade")),
            "markPriceUpdate" => Some((3, "ticker")),
            _ => None,
        }
    } else {
        None
    }
}

fn detect_feed_gate(s: &str) -> Option<(usize, &'static str)> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        let ch = v.get("channel")?.as_str()?;
        match ch {
            "futures.book_ticker" => Some((0, "bbo")),
            "futures.order_book_update" | "futures.obu" => Some((1, "orderbook")),
            "futures.trades" => Some((2, "trade")),
            "futures.tickers" => Some((3, "ticker")),
            _ => None,
        }
    } else {
        None
    }
}

fn detect_feed_bitget(s: &str) -> Option<(usize, &'static str)> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        let ch = v.get("channel")?.as_str()?;
        match ch {
            "books1" => Some((0, "bbo")),
            "books" => Some((1, "orderbook")),
            "trade" => Some((2, "trade")),
            "ticker" => Some((3, "ticker")),
            _ => None,
        }
    } else {
        None
    }
}
