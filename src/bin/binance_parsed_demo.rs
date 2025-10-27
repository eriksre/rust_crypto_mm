#[cfg(feature = "parse_binance")]
fn main() {
    use rust_test::base_classes::ws::spawn_ws_worker;
    use rust_test::exchanges::binance::parsed::{BinanceEvent, BinanceParsedHandler};

    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let core_pin: Option<usize> = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok());
    const N: usize = 1 << 12;

    let (consumer, _jh) = spawn_ws_worker::<BinanceParsedHandler, N>(
        BinanceParsedHandler::new(symbol.clone()),
        core_pin,
        None,
    );
    eprintln!("Binance parsed streaming for {symbol}. Features: ws_tungstenite,parse_binance");
    eprintln!(
        "Example: cargo run --release --bin binance_parsed_demo --features ws_tungstenite,parse_binance -- {symbol}"
    );

    loop {
        if let Ok(frame) = consumer.try_pop() {
            match frame.event {
                BinanceEvent::BookTicker(ref bt) => {
                    println!(
                        "bookTicker {} bid={}@{} ask={}@{}",
                        bt.s, bt.B, bt.b, bt.A, bt.a
                    );
                }
                BinanceEvent::AggTrade(ref t) => {
                    println!("aggTrade {} px={} qty={} m={}", t.s, t.p, t.q, t.m);
                }
                BinanceEvent::DepthUpdate(ref d) => {
                    println!(
                        "depthUpdate {} bids={} asks={} u={}..{}",
                        d.s,
                        d.b.len(),
                        d.a.len(),
                        d.U,
                        d.u
                    );
                }
                BinanceEvent::MarkPrice(ref m) => {
                    println!("markPrice {} p={}", m.s, m.p);
                }
                BinanceEvent::Ack => {
                    println!("ack");
                }
                BinanceEvent::Other(_) => { /* ignore */ }
            }
        } else {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
}

#[cfg(not(feature = "parse_binance"))]
fn main() {
    eprintln!("Enable parse_binance feature to run this demo.");
}
