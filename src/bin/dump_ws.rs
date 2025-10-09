use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::binance::BinanceHandler;
use rust_test::exchanges::bitget::BitgetHandler;
use rust_test::exchanges::bybit::BybitHandler;
use rust_test::exchanges::gate::GateHandler;
use std::time::Duration;

fn main() {
    // Args: SYMBOL [per_exchange_count]
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let per_n: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    const N: usize = 1 << 12;

    let (bybit_c, _j1) =
        spawn_ws_worker::<BybitHandler, N>(BybitHandler::new(symbol.clone()), None, None);
    let (bin_c, _j2) =
        spawn_ws_worker::<BinanceHandler, N>(BinanceHandler::new(symbol.clone()), None, None);
    let (gate_c, _j3) =
        spawn_ws_worker::<GateHandler, N>(GateHandler::new(symbol.clone()), None, None);
    let (bit_c, _j4) =
        spawn_ws_worker::<BitgetHandler, N>(BitgetHandler::new(symbol.clone()), None, None);

    let mut left = [per_n; 4];
    eprintln!("Dumping raw WS frames: {symbol}, {per_n} per exchange");
    loop {
        let mut wrote = false;
        if left[0] > 0 {
            if let Ok(f) = bybit_c.try_pop() {
                if let Ok(s) = std::str::from_utf8(&f.raw) {
                    println!("BYBIT: {}", s);
                    left[0] -= 1;
                    wrote = true;
                }
            }
        }
        if left[1] > 0 {
            if let Ok(f) = bin_c.try_pop() {
                if let Ok(s) = std::str::from_utf8(&f.raw) {
                    println!("BINANCE: {}", s);
                    left[1] -= 1;
                    wrote = true;
                }
            }
        }
        if left[2] > 0 {
            if let Ok(f) = gate_c.try_pop() {
                if let Ok(s) = std::str::from_utf8(&f.raw) {
                    println!("GATE: {}", s);
                    left[2] -= 1;
                    wrote = true;
                }
            }
        }
        if left[3] > 0 {
            if let Ok(f) = bit_c.try_pop() {
                if let Ok(s) = std::str::from_utf8(&f.raw) {
                    println!("BITGET: {}", s);
                    left[3] -= 1;
                    wrote = true;
                }
            }
        }
        if !wrote {
            std::thread::sleep(Duration::from_millis(2));
        }
        if left.iter().all(|&x| x == 0) {
            break;
        }
    }
}
