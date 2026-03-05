use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::lighter::{LighterHandler, fetch_market_meta};
use std::time::Duration;

fn main() {
    // Args: SYMBOL [frame_count]
    let symbol = std::env::args().nth(1).unwrap_or_else(|| "BTC".to_string());
    let max_frames: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    eprintln!("Fetching market metadata for symbol: {}", symbol);
    let meta = match fetch_market_meta(&symbol) {
        Some(m) => m,
        None => {
            eprintln!("ERROR: Could not fetch market metadata for '{}'", symbol);
            eprintln!("Make sure the symbol exists on Lighter.xyz (e.g., BTC, ETH, SOL)");
            std::process::exit(1);
        }
    };

    eprintln!(
        "Found market: {} (id={}, price_decimals={}, size_decimals={})",
        meta.symbol, meta.market_id, meta.price_decimals, meta.size_decimals
    );
    eprintln!("Will dump {} raw frames with feed labels", max_frames);
    eprintln!("Subscribed feeds: order_book, trade, market_stats");
    eprintln!("---");

    const N: usize = 1 << 12;
    let (lighter_c, _j) =
        spawn_ws_worker::<LighterHandler, N>(LighterHandler::new(meta), None, None);

    let mut count = 0;
    loop {
        if count >= max_frames {
            break;
        }

        if let Ok(frame) = lighter_c.try_pop() {
            let channel = frame.channel().unwrap_or("UNKNOWN");
            let feed_label = if channel.starts_with("order_book") {
                "ORDERBOOK"
            } else if channel.starts_with("trade") {
                "TRADES"
            } else if channel.starts_with("market_stats") {
                "MARKET_STATS"
            } else {
                channel
            };

            if let Ok(s) = std::str::from_utf8(&frame.raw) {
                println!("[{}] {}", feed_label, s);
                count += 1;
            }
        } else {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    eprintln!("---");
    eprintln!("Dumped {} frames", count);
}
