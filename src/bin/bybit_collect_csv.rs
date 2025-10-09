use std::fs::File;
use std::io::{BufWriter, Write};

use rust_test::base_classes::ws::spawn_ws_worker;
use rust_test::exchanges::bybit::BybitHandler;

fn main() {
    // Args: SYMBOL [output.csv] [core]
    let symbol = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let out_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "bybit_mid.csv".to_string());
    let core_pin: Option<usize> = std::env::args()
        .nth(3)
        .and_then(|s| s.parse::<usize>().ok());

    const N: usize = 1 << 14; // More room
    let (consumer, _jh) =
        spawn_ws_worker::<BybitHandler, N>(BybitHandler::new(symbol.clone()), core_pin, None);

    eprintln!("Collecting Bybit mids for {symbol} → {out_path} (Ctrl-C to stop)");

    let file = File::create(&out_path).expect("create csv");
    let mut w = BufWriter::new(file);
    writeln!(w, "ts_ns,feed,mid").unwrap();

    let mut n: u64 = 0;
    loop {
        match consumer.try_pop() {
            Ok(frame) => {
                let s = match core::str::from_utf8(&frame.raw) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let topic = match find_json_string(s, "topic") {
                    Some(t) => t,
                    None => continue,
                };
                if topic.starts_with("orderbook.") {
                    if let Some((bid, ask)) = extract_top_prices(s) {
                        let feed = if topic.starts_with("orderbook.1.") {
                            "bbo"
                        } else {
                            "orderbook"
                        };
                        let mid = (bid + ask) * 0.5;
                        writeln!(w, "{},{},{}", frame.ts, feed, mid).ok();
                        n += 1;
                        if n % 256 == 0 {
                            let _ = w.flush();
                        }
                    }
                }
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(1)),
        }
    }
}

fn find_json_string<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let k = format!("\"{}\"", key);
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let q = rest.find('"')?;
    let rest2 = &rest[q + 1..];
    let end = rest2.find('"')?;
    Some(&rest2[..end])
}

fn extract_first_price_after(key: &str, s: &str) -> Option<f64> {
    // Finds key (e.g., "b": or "a":), then the first quoted number inside the first nested array
    let k = format!("\"{}\":", key);
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let lb = rest.find('[')?; // start of array
    let rest = &rest[lb + 1..];
    // find first '"'
    let q = rest.find('"')?;
    let rest2 = &rest[q + 1..];
    let end = rest2.find('"')?;
    let px_str = &rest2[..end];
    px_str.parse::<f64>().ok()
}

fn extract_top_prices(s: &str) -> Option<(f64, f64)> {
    let bid = extract_first_price_after("b", s)?;
    let ask = extract_first_price_after("a", s)?;
    Some((bid, ask))
}
