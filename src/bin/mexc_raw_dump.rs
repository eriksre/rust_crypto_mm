use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::mexc::MexcHandler;
use std::time::Duration;

fn main() {
    // Args: SYMBOL [frame_count]
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTC_USDT".to_string());
    let max_frames: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    const N: usize = 1 << 12;

    eprintln!("Connecting to MEXC for symbol: {}", symbol);
    eprintln!("Will dump {} raw frames with feed labels", max_frames);
    eprintln!("Subscribed feeds: orderbook (push.depth), ticker (push.ticker), trades (push.deal)");
    eprintln!("---");

    let (mexc_c, _j) = spawn_ws_worker::<MexcHandler, N>(MexcHandler::new(symbol), None, None);

    let mut count = 0;
    loop {
        if count >= max_frames {
            break;
        }

        if let Ok(mut frame) = mexc_c.try_pop() {
            let channel = frame.channel().unwrap_or("UNKNOWN");
            let feed_label = match channel {
                "push.depth" => "ORDERBOOK",
                "push.ticker" => "TICKER/BBO",
                "push.deal" => "TRADES",
                _ => channel,
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
