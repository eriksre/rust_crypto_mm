use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::binance::BinanceHandler;

fn main() {
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let core_pin: Option<usize> = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok());
    const N: usize = 1 << 12;

    let (consumer, _jh) =
        spawn_ws_worker::<BinanceHandler, N>(BinanceHandler::new(symbol.clone()), core_pin, None);

    eprintln!("Binance streaming for {symbol}. Enable ws backend features to connect.");
    eprintln!(
        "- tungstenite: cargo run --bin binance_demo --features ws_tungstenite -- {symbol} [core]"
    );

    let mut printed: u64 = 0;
    loop {
        match consumer.try_pop() {
            Ok(frame) => {
                printed += 1;
                let topic = frame.topic();
                let preview = core::str::from_utf8(&frame.raw).unwrap_or("");
                let preview = if preview.len() > 200 {
                    &preview[..200]
                } else {
                    preview
                };
                println!(
                    "[{printed}] topic={topic} len={} ts={} preview={}...",
                    frame.raw.len(),
                    frame.ts,
                    preview
                );
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(2)),
        }
    }
}
