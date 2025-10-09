use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::bybit::BybitHandler;

fn main() {
    // Symbol from args or default
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let core_pin: Option<usize> = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok());
    const N: usize = 1 << 12; // ring capacity (power of two)

    let (consumer, _jh) =
        spawn_ws_worker::<BybitHandler, N>(BybitHandler::new(symbol.clone()), core_pin, None);

    eprintln!("Bybit streaming for {symbol}. Enable ws backend features to connect.");
    eprintln!(
        "- tungstenite: cargo run --bin bybit_demo --features ws_tungstenite -- {symbol} [core] "
    );
    eprintln!(
        "- fastws     : cargo run --bin bybit_demo --features ws_fast -- {symbol} [core] (TLS not wired)"
    );

    // Stream continuously; press Ctrl-C to stop.
    let mut printed: u64 = 0;
    loop {
        match consumer.try_pop() {
            Ok(frame) => {
                printed += 1;
                let topic = frame.topic().unwrap_or("(unknown)");
                let preview = core::str::from_utf8(&frame.raw).unwrap_or("");
                let preview = if preview.len() > 160 {
                    &preview[..160]
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
            Err(_) => {
                // idle
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
    }
}
