use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::collectors::helpers::{find_first_string_number, find_json_string};
use crate::exchanges::gate::orderbook::{GateBook, GateMsg};
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

pub fn events_for<const N: usize>(s: &str, book: &mut GateBook<N>) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    if let Some(ch) = find_json_string(s, "channel") {
        match ch {
            "futures.book_ticker" => { /* handled by bbo updater in caller */ }
            "futures.obu" => {
                if let Ok(msg) = serde_json::from_str::<GateMsg>(s) {
                    if book.apply(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
            }
            "futures.trades" => { /* handled by trades updater in caller */ }
            _ => {}
        }
    }
    out
}

pub const PRICE_SCALE: f64 = GateBook::<1>::PRICE_SCALE;
pub const QTY_SCALE: f64 = GateBook::<1>::QTY_SCALE;

fn first_result_object(value: &Value) -> Option<&Value> {
    if let Some(result) = value.get("result") {
        if result.is_array() {
            result.as_array().and_then(|arr| arr.first())
        } else {
            Some(result)
        }
    } else {
        Some(value)
    }
}

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

fn value_to_f64<'a>(value: &'a Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(num) = as_f64(v) {
                return Some(num);
            }
        }
    }
    None
}

fn value_to_u64<'a>(value: &'a Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(num) = as_u64(v) {
                return Some(num);
            }
        }
    }
    None
}

pub fn update_tickers(s: &str, store: &mut TickerStore) -> Option<(String, TickerSnapshot)> {
    let raw: Value = serde_json::from_str(s).ok()?;
    if raw.get("channel").and_then(|v| v.as_str()) != Some("futures.tickers") {
        return None;
    }

    let data_obj = first_result_object(&raw)?;
    if !data_obj.is_object() {
        return None;
    }

    let symbol = data_obj
        .get("contract")
        .or_else(|| data_obj.get("symbol"))
        .and_then(|v| v.as_str())
        .or_else(|| find_json_string(s, "contract"))
        .or_else(|| find_json_string(s, "symbol"))?
        .to_string();

    let mut snapshot = store.get(&symbol).copied().unwrap_or_default();

    if let Some(last_px) = value_to_f64(data_obj, &["last", "last_price"]) {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(bid_px) = value_to_f64(data_obj, &["best_bid", "bid"])
        .or_else(|| find_first_string_number(s, &["best_bid", "bid"]))
    {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = value_to_f64(data_obj, &["best_ask", "ask"])
        .or_else(|| find_first_string_number(s, &["best_ask", "ask"]))
    {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }

    if let Some(mark) = value_to_f64(data_obj, &["mark_price"]) {
        snapshot.mark_px = Some(mark);
    }
    if let Some(index) = value_to_f64(data_obj, &["index_price"]) {
        snapshot.index_px = Some(index);
    }
    if let Some(rate) = value_to_f64(data_obj, &["funding_rate"]) {
        snapshot.funding_rate = Some(rate);
    }
    if let Some(turnover) = value_to_f64(data_obj, &["volume_24h_settle", "volume_24h_quote"]) {
        snapshot.turnover_24h = Some(turnover);
    }
    if let Some(oi) = value_to_f64(data_obj, &["total_size", "open_interest"]) {
        snapshot.open_interest = Some(oi);
    }

    if let Some(mult) = value_to_f64(data_obj, &["quanto_multiplier"]) {
        snapshot.quanto_multiplier = Some(mult);
    }

    let multiplier = snapshot.quanto_multiplier.unwrap_or(1.0);
    if let (Some(oi), Some(mark)) = (snapshot.open_interest, snapshot.mark_px) {
        snapshot.open_interest_value = Some(oi * mark * multiplier);
    }

    if let Some(last_size) = value_to_f64(data_obj, &["last_size", "last_qty"]) {
        snapshot.ticker.last_qty = (last_size * QTY_SCALE).round() as Qty;
    }

    if let Some(ts_ms) = value_to_u64(data_obj, &["time_ms", "ts"]) {
        snapshot.ticker.ts = ms_to_ns(ts_ms);
    }

    if let Some(seq) = value_to_u64(data_obj, &["update_id", "seq", "t"]) {
        snapshot.ticker.seq = seq;
    } else {
        snapshot.ticker.seq = 0;
    }

    let stored = store.update(symbol.clone(), snapshot);
    Some((symbol, stored))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_tickers_gate_basic() {
        let json = r#"{
            "time":1700000000,
            "channel":"futures.tickers",
            "result":[{
                "contract":"BTC_USDT",
                "last":"43000",
                "mark_price":"43005",
                "index_price":"42995",
                "funding_rate":"0.0001",
                "volume_24h_settle":"1234567.89",
                "total_size":"456.0",
                "last_size":"2.5",
                "quanto_multiplier":"0.01",
                "time_ms":1700000000000
            }]
        }"#;

        let mut store = TickerStore::default();
        let (_, snap) = update_tickers(json, &mut store).expect("ticker parsed");

        assert_eq!(
            snap.ticker.last_px,
            (43000.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(snap.mark_px, Some(43005.0));
        assert_eq!(snap.index_px, Some(42995.0));
        assert_eq!(snap.funding_rate, Some(0.0001));
        assert_eq!(snap.turnover_24h, Some(1_234_567.89));
        assert_eq!(snap.open_interest, Some(456.0));
        assert_eq!(snap.quanto_multiplier, Some(0.01));
        let expected_oi_value = 456.0 * 43005.0 * 0.01;
        assert_eq!(snap.open_interest_value, Some(expected_oi_value));
        assert_eq!(snap.ticker.last_qty, (2.5 * QTY_SCALE).round() as Qty);
        assert_eq!(snap.ticker.ts, 1_700_000_000_000_000_000);
    }

    #[test]
    fn test_update_bbo_store_gate() {
        let json = r#"{
            "channel":"futures.book_ticker",
            "event":"update",
            "result":{
                "t":1758808025186,
                "u":89543752356,
                "s":"BTC_USDT",
                "b":"110756.8",
                "B":1,
                "a":"110756.9",
                "A":49712
            }
        }"#;
        let mut store = BboStore::default();
        assert!(update_bbo_store(json, &mut store));
        let mid = store.mid_price_f64_for("BTC_USDT").unwrap();
        assert!((mid - 110756.85).abs() < 1e-6);
    }
}

// Update BBO store for Gate from futures.book_ticker
pub fn update_bbo_store(s: &str, store: &mut BboStore) -> bool {
    if let Ok(raw) = serde_json::from_str::<Value>(s) {
        if raw.get("channel").and_then(|v| v.as_str()) == Some("futures.book_ticker") {
            if let Some(obj) = first_result_object(&raw) {
                let bid = obj
                    .get("b")
                    .or_else(|| obj.get("bid"))
                    .and_then(as_f64)
                    .or_else(|| find_first_string_number(s, &["b", "bid"]));
                let ask = obj
                    .get("a")
                    .or_else(|| obj.get("ask"))
                    .and_then(as_f64)
                    .or_else(|| find_first_string_number(s, &["a", "ask"]));
                if let (Some(b), Some(a)) = (bid, ask) {
                    let bid_qty = obj
                        .get("B")
                        .or_else(|| obj.get("bid_size"))
                        .or_else(|| obj.get("bidSize"))
                        .and_then(as_f64)
                        .unwrap_or(0.0);
                    let ask_qty = obj
                        .get("A")
                        .or_else(|| obj.get("ask_size"))
                        .or_else(|| obj.get("askSize"))
                        .and_then(as_f64)
                        .unwrap_or(0.0);
                    let ts_ms = obj
                        .get("t")
                        .and_then(as_u64)
                        .or_else(|| raw.get("time_ms").and_then(as_u64))
                        .unwrap_or(0);
                    let ts_ns = ms_to_ns(ts_ms);
                    let system_ts_ns = raw.get("time_ms").and_then(as_u64).map(ms_to_ns);
                    let symbol = obj
                        .get("contract")
                        .or_else(|| obj.get("symbol"))
                        .or_else(|| obj.get("s"))
                        .and_then(|v| v.as_str())
                        .or_else(|| raw.get("contract").and_then(|v| v.as_str()))
                        .or_else(|| find_json_string(s, "contract"))
                        .or_else(|| find_json_string(s, "symbol"))
                        .or_else(|| find_json_string(s, "s"));
                    if let Some(symbol) = symbol {
                        store.update(symbol, b, bid_qty, a, ask_qty, ts_ns, system_ts_ns);
                        return true;
                    }
                }
            }
        }
    }
    false
}

// Update trades store for Gate from futures.trades
pub fn update_trades<const N: usize>(s: &str, trades: &mut FixedTrades<N>) -> usize {
    if let Ok(raw) = serde_json::from_str::<Value>(s) {
        if raw.get("channel").and_then(|v| v.as_str()) == Some("futures.trades") {
            let mut inserted = 0usize;
            if let Some(entries) = raw.get("result").and_then(|res| {
                if res.is_array() {
                    Some(res.as_array().unwrap())
                } else {
                    None
                }
            }) {
                for entry in entries {
                    let price = entry
                        .get("price")
                        .or_else(|| entry.get("p"))
                        .and_then(as_f64)
                        .or_else(|| find_first_string_number(s, &["price", "p"]));
                    let size = entry
                        .get("size")
                        .or_else(|| entry.get("v"))
                        .or_else(|| entry.get("amount"))
                        .and_then(as_f64)
                        .unwrap_or(0.0);
                    if let Some(px) = price {
                        let px_i = (px * PRICE_SCALE).round() as Price;
                        let qty_i = (size * QTY_SCALE).round() as Qty;
                        let ts_ms = entry
                            .get("create_time_ms")
                            .or_else(|| entry.get("t"))
                            .or_else(|| entry.get("ts"))
                            .and_then(as_u64)
                            .or_else(|| raw.get("time_ms").and_then(as_u64))
                            .unwrap_or(0);
                        let ts_ns = ms_to_ns(ts_ms);
                        let system_ts_ns = raw.get("time_ms").and_then(as_u64).map(ms_to_ns);
                        let seq = entry
                            .get("id")
                            .or_else(|| entry.get("trade_id"))
                            .and_then(as_u64)
                            .unwrap_or(0) as Seq;
                        let is_buyer_maker = size < 0.0;
                        trades.push(Trade::new(
                            px_i,
                            qty_i,
                            ts_ns,
                            seq,
                            is_buyer_maker,
                            system_ts_ns,
                        ));
                        inserted += 1;
                    }
                }
            }
            return inserted;
        }
    }
    0
}
