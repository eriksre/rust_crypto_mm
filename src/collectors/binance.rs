use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::collectors::helpers::{find_first_string_number, find_json_string};
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

#[cfg(feature = "binance_book")]
use crate::exchanges::binance::orderbook::BinanceBook;
#[cfg(feature = "parse_binance")]
use crate::exchanges::binance::parsed::DepthUpdate;

pub const PRICE_SCALE: f64 = 100_000.0;
pub const QTY_SCALE: f64 = 1_000_000.0;

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn event_payload<'a>(root: &'a Value) -> &'a Value {
    root.get("data").unwrap_or(root)
}

pub fn update_tickers(s: &str, store: &mut TickerStore) -> Option<(String, TickerSnapshot)> {
    let raw: Value = serde_json::from_str(s).ok()?;
    let payload = event_payload(&raw);
    if payload.get("e").and_then(|v| v.as_str()) != Some("markPriceUpdate") {
        return None;
    }

    let symbol = payload.get("s").and_then(|v| v.as_str())?.to_string();
    let mut snapshot = store.get(&symbol).copied().unwrap_or_default();

    if let Some(mark) = payload.get("p").and_then(as_f64) {
        snapshot.mark_px = Some(mark);
    }
    if let Some(index) = payload.get("i").and_then(as_f64) {
        snapshot.index_px = Some(index);
    }
    if let Some(rate) = payload.get("r").and_then(as_f64) {
        snapshot.funding_rate = Some(rate);
    }

    if let Some(ts_ms) = payload
        .get("T")
        .and_then(as_u64)
        .or_else(|| payload.get("E").and_then(as_u64))
    {
        snapshot.ticker.ts = ms_to_ns(ts_ms);
    }

    if let Some(seq) = payload.get("E").and_then(as_u64) {
        snapshot.ticker.seq = seq;
    } else {
        snapshot.ticker.seq = 0;
    }

    let stored = store.update(symbol.clone(), snapshot);
    Some((symbol, stored))
}

// Update BBO store from Binance bookTicker frames; emit from store only (caller computes mid).
pub fn update_bbo_store(s: &str, store: &mut BboStore) -> bool {
    if let Ok(raw) = serde_json::from_str::<Value>(s) {
        let payload = event_payload(&raw);
        if payload.get("e").and_then(|v| v.as_str()) == Some("bookTicker") {
            let bid = payload
                .get("b")
                .and_then(as_f64)
                .or_else(|| find_first_string_number(s, &["b"]))
                .unwrap_or(0.0);
            let ask = payload
                .get("a")
                .and_then(as_f64)
                .or_else(|| find_first_string_number(s, &["a"]))
                .unwrap_or(0.0);
            let bid_qty = payload
                .get("B")
                .and_then(as_f64)
                .unwrap_or_else(|| find_first_string_number(s, &["B"]).unwrap_or(0.0));
            let ask_qty = payload
                .get("A")
                .and_then(as_f64)
                .unwrap_or_else(|| find_first_string_number(s, &["A"]).unwrap_or(0.0));
            let ts_ms = payload
                .get("T")
                .and_then(|v| v.as_u64())
                .or_else(|| payload.get("E").and_then(|v| v.as_u64()))
                .unwrap_or(0);
            let ts_ns = ms_to_ns(ts_ms);
            let system_ts_ns = payload.get("E").and_then(|v| v.as_u64()).map(ms_to_ns);
            if let Some(symbol) = payload
                .get("s")
                .and_then(|v| v.as_str())
                .or_else(|| find_json_string(s, "s"))
            {
                store.update(symbol, bid, bid_qty, ask, ask_qty, ts_ns, system_ts_ns);
                return true;
            }
        }
    }
    false
}

#[cfg(feature = "binance_book")]
pub fn events_for_book<const N: usize>(
    s: &str,
    book: &mut BinanceBook<N>,
) -> Option<(&'static str, f64)> {
    if let Some(ev) = find_json_string(s, "e") {
        if ev == "depthUpdate" {
            if let Ok(evt) = serde_json::from_str::<DepthUpdate>(s) {
                if book.apply_depth_update(&evt) {
                    if let Some(mid) = book.mid_price_f64() {
                        return Some(("orderbook", mid));
                    }
                }
            }
        }
    }
    None
}

// Update trades store from aggTrade frames; emit from store only (caller reads last px)
pub fn update_trades<const N: usize>(s: &str, trades: &mut FixedTrades<N>) -> usize {
    if let Ok(raw) = serde_json::from_str::<Value>(s) {
        let payload = event_payload(&raw);
        if payload.get("e").and_then(|v| v.as_str()) == Some("aggTrade") {
            if let Some(price) = payload.get("p").and_then(as_f64) {
                let qty = payload
                    .get("q")
                    .and_then(as_f64)
                    .unwrap_or_else(|| find_first_string_number(s, &["q"]).unwrap_or(0.0));
                let px_i = (price * PRICE_SCALE).round() as Price;
                let qty_i = (qty * QTY_SCALE).round() as Qty;
                let ts_ms = payload
                    .get("T")
                    .and_then(|v| v.as_u64())
                    .or_else(|| payload.get("E").and_then(|v| v.as_u64()))
                    .unwrap_or(0);
                let ts_ns = ms_to_ns(ts_ms);
                let agg_id = payload.get("a").and_then(|v| v.as_u64()).unwrap_or(0) as Seq;
                let is_buyer_maker = payload.get("m").and_then(|v| v.as_bool()).unwrap_or(false);
                let system_ts_ns = payload.get("E").and_then(|v| v.as_u64()).map(ms_to_ns);
                let trade = Trade::new(px_i, qty_i, ts_ns, agg_id, is_buyer_maker, system_ts_ns);
                trades.push(trade);
                return 1;
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_tickers_binance_mark_price() {
        let json = r#"{
            "stream":"btcusdt@markPrice@1s",
            "data":{
                "e":"markPriceUpdate",
                "E":1600000000000,
                "s":"BTCUSDT",
                "p":"11794.1500",
                "i":"11780.1200",
                "r":"0.000123",
                "T":1600000005000
            }
        }"#;

        let mut store = TickerStore::default();
        let (_, snap) = update_tickers(json, &mut store).expect("ticker parsed");

        assert_eq!(snap.mark_px, Some(11794.15));
        assert_eq!(snap.index_px, Some(11780.12));
        assert_eq!(snap.funding_rate, Some(0.000123));
        assert_eq!(snap.ticker.seq, 1_600_000_000_000);
        assert_eq!(snap.ticker.ts, 1_600_000_005_000_000_000);
    }
}
