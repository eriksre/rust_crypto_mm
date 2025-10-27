use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::collectors::helpers::{
    extract_first_price_in_array, find_first_string_number, find_json_string,
};
use crate::exchanges::bitget::orderbook::{BitgetBook, BitgetMsg};
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

pub fn events_for<const N: usize>(s: &str, book: &mut BitgetBook<N>) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    if let Some(ch) = find_json_string(s, "channel") {
        match ch {
            "books1" => {
                if let Ok(msg) = serde_json::from_str::<BitgetMsg>(s) {
                    if book.apply_bbo(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
            }
            "books" => {
                if let Ok(msg) = serde_json::from_str::<BitgetMsg>(s) {
                    if book.apply(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
            }
            "trade" => { /* handled by trades updater in caller */ }
            _ => {}
        }
    }
    out
}

pub const PRICE_SCALE: f64 = BitgetBook::<1>::PRICE_SCALE;
pub const QTY_SCALE: f64 = BitgetBook::<1>::QTY_SCALE;

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

fn first_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            if let Some(v) = as_f64(entry) {
                return Some(v);
            }
        }
    }
    None
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            if let Some(v) = as_u64(entry) {
                return Some(v);
            }
        }
    }
    None
}

pub fn update_tickers(s: &str, store: &mut TickerStore) -> Option<(String, TickerSnapshot)> {
    let raw: Value = serde_json::from_str(s).ok()?;
    let channel = raw
        .get("channel")
        .or_else(|| raw.get("arg").and_then(|arg| arg.get("channel")))
        .and_then(|v| v.as_str())?;
    if channel != "ticker" {
        return None;
    }

    let data = raw
        .get("data")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())?;

    let symbol_str = data
        .get("instId")
        .and_then(|v| v.as_str())
        .or_else(|| {
            raw.get("arg")
                .and_then(|arg| arg.get("instId"))
                .and_then(|v| v.as_str())
        })
        .or_else(|| raw.get("instId").and_then(|v| v.as_str()))
        .or_else(|| find_json_string(s, "instId"))?;
    let symbol = symbol_str.to_string();

    let mut snapshot = store.get(&symbol).copied().unwrap_or_default();

    if let Some(last_px) = first_f64(data, &["lastPr", "lastPrice", "close"])
        .or_else(|| extract_first_price_in_array(s, &["lastPr", "lastPrice"]))
    {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(last_qty) = first_f64(data, &["lastSz", "lastQty", "lastSize"]) {
        snapshot.ticker.last_qty = (last_qty * QTY_SCALE).round() as Qty;
    }
    if let Some(bid_px) = first_f64(data, &["bestBid", "bidPr"]) {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = first_f64(data, &["bestAsk", "askPr"]) {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }

    if let Some(mark) = first_f64(data, &["markPrice"]) {
        snapshot.mark_px = Some(mark);
    }
    if let Some(index) = first_f64(data, &["indexPrice"]) {
        snapshot.index_px = Some(index);
    }
    if let Some(funding) = first_f64(data, &["fundingRate"]) {
        snapshot.funding_rate = Some(funding);
    }
    if let Some(turnover) = first_f64(data, &["quoteVolume", "turnover24h"]) {
        snapshot.turnover_24h = Some(turnover);
    }
    if let Some(oi) = first_f64(data, &["holdingAmount", "openInterest"]) {
        snapshot.open_interest = Some(oi);
    }

    if let Some(oi_val) = first_f64(data, &["openInterestValue", "openValue"]) {
        snapshot.open_interest_value = Some(oi_val);
    } else if let (Some(oi), Some(mark)) = (snapshot.open_interest, snapshot.mark_px) {
        snapshot.open_interest_value = Some(oi * mark);
    }

    if let Some(seq) = first_u64(data, &["seq", "seqId", "u"]) {
        snapshot.ticker.seq = seq;
    } else {
        snapshot.ticker.seq = 0;
    }

    if let Some(ts_ms) = first_u64(data, &["ts"]).or_else(|| raw.get("ts").and_then(as_u64)) {
        snapshot.ticker.ts = ms_to_ns(ts_ms);
    }

    let stored = store.update(symbol.clone(), snapshot);
    Some((symbol, stored))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_tickers_bitget_basic() {
        let json = r#"{
            "action":"update",
            "arg":{"instType":"USDT-FUTURES","channel":"ticker","instId":"BTCUSDT"},
            "data":[{
                "instId":"BTCUSDT",
                "lastPr":"43000",
                "lastSz":"1.5",
                "bestBid":"42990",
                "bestAsk":"43010",
                "markPrice":"43005",
                "indexPrice":"42995",
                "fundingRate":"0.0001",
                "quoteVolume":"987654.321",
                "holdingAmount":"456.0",
                "openInterestValue":"19610280",
                "seq":"123456",
                "ts":1700000000000
            }]
        }"#;

        let mut store = TickerStore::default();
        let (_, snap) = update_tickers(json, &mut store).expect("ticker parsed");

        assert_eq!(
            snap.ticker.last_px,
            (43000.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(snap.ticker.last_qty, (1.5 * QTY_SCALE).round() as Qty);
        assert_eq!(
            snap.ticker.best_bid,
            (42990.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(
            snap.ticker.best_ask,
            (43010.0 * PRICE_SCALE).round() as Price
        );
        assert_eq!(snap.mark_px, Some(43005.0));
        assert_eq!(snap.index_px, Some(42995.0));
        assert_eq!(snap.funding_rate, Some(0.0001));
        assert_eq!(snap.turnover_24h, Some(987_654.321));
        assert_eq!(snap.open_interest, Some(456.0));
        assert_eq!(snap.open_interest_value, Some(19_610_280.0));
        assert_eq!(snap.ticker.seq, 123_456);
        assert_eq!(snap.ticker.ts, 1_700_000_000_000_000_000);
    }
}

// Update BBO store for Bitget from books1 channel
pub fn update_bbo_store(s: &str, store: &mut BboStore) -> bool {
    if let Ok(raw) = serde_json::from_str::<Value>(s) {
        let channel = raw
            .get("channel")
            .or_else(|| raw.get("arg").and_then(|arg| arg.get("channel")))
            .and_then(|v| v.as_str());
        if channel == Some("books1") {
            if let Some(data) = raw
                .get("data")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
            {
                let bid_price = data
                    .get("bids")
                    .and_then(|arr| arr.as_array())
                    .and_then(|levels| levels.first())
                    .and_then(|lvl| lvl.as_array())
                    .and_then(|pair| pair.first())
                    .and_then(as_f64)
                    .or_else(|| extract_first_price_in_array(s, &["bids", "b"]));
                let ask_price = data
                    .get("asks")
                    .and_then(|arr| arr.as_array())
                    .and_then(|levels| levels.first())
                    .and_then(|lvl| lvl.as_array())
                    .and_then(|pair| pair.first())
                    .and_then(as_f64)
                    .or_else(|| extract_first_price_in_array(s, &["asks", "a"]));
                if let (Some(b), Some(a)) = (bid_price, ask_price) {
                    let bid_qty = data
                        .get("bids")
                        .and_then(|arr| arr.as_array())
                        .and_then(|levels| levels.first())
                        .and_then(|lvl| lvl.as_array())
                        .and_then(|pair| pair.get(1))
                        .and_then(as_f64)
                        .unwrap_or(0.0);
                    let ask_qty = data
                        .get("asks")
                        .and_then(|arr| arr.as_array())
                        .and_then(|levels| levels.first())
                        .and_then(|lvl| lvl.as_array())
                        .and_then(|pair| pair.get(1))
                        .and_then(as_f64)
                        .unwrap_or(0.0);
                    let ts_ms = data
                        .get("ts")
                        .and_then(as_u64)
                        .unwrap_or_else(|| raw.get("ts").and_then(as_u64).unwrap_or(0));
                    let ts_ns = ms_to_ns(ts_ms);
                    let system_ts_ns = raw.get("ts").and_then(as_u64).map(ms_to_ns);
                    let symbol = raw
                        .get("arg")
                        .and_then(|arg| {
                            arg.get("instId")
                                .or_else(|| arg.get("symbol"))
                                .or_else(|| arg.get("instID"))
                        })
                        .and_then(|v| v.as_str())
                        .or_else(|| raw.get("instId").and_then(|v| v.as_str()))
                        .or_else(|| find_json_string(s, "instId"));
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

// Update trades store for Bitget from public trade channel
pub fn update_trades<const N: usize>(s: &str, trades: &mut FixedTrades<N>) -> usize {
    if let Ok(raw) = serde_json::from_str::<Value>(s) {
        let channel = raw
            .get("channel")
            .or_else(|| raw.get("arg").and_then(|arg| arg.get("channel")))
            .and_then(|v| v.as_str());
        if channel == Some("trade") {
            if let Some(entries) = raw.get("data").and_then(|v| v.as_array()) {
                let mut inserted = 0usize;
                // Bitget sends the newest trade first; reverse iterate so the newest
                // trade is the last one we push, keeping `FixedTrades::last()` stable.
                for entry in entries.iter().rev() {
                    let price = entry
                        .get("px")
                        .or_else(|| entry.get("price"))
                        .or_else(|| entry.get("p"))
                        .and_then(as_f64)
                        .or_else(|| find_first_string_number(s, &["px", "price", "p"]));
                    if let Some(px) = price {
                        let size = entry
                            .get("sz")
                            .or_else(|| entry.get("size"))
                            .or_else(|| entry.get("qty"))
                            .and_then(as_f64)
                            .unwrap_or(0.0);
                        let px_i = (px * PRICE_SCALE).round() as Price;
                        let qty_i = (size * QTY_SCALE).round() as Qty;
                        let ts_ms = entry
                            .get("ts")
                            .or_else(|| entry.get("createTime"))
                            .or_else(|| entry.get("create_time_ms"))
                            .and_then(as_u64)
                            .unwrap_or_else(|| raw.get("ts").and_then(as_u64).unwrap_or(0));
                        let ts_ns = ms_to_ns(ts_ms);
                        let system_ts_ns = raw.get("ts").and_then(as_u64).map(ms_to_ns);
                        let seq = entry
                            .get("tradeId")
                            .or_else(|| entry.get("id"))
                            .and_then(as_u64)
                            .unwrap_or(0) as Seq;
                        let is_buyer_maker = entry
                            .get("side")
                            .and_then(|v| v.as_str())
                            .map(|side| side.eq_ignore_ascii_case("sell"))
                            .unwrap_or(false);
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
                return inserted;
            }
        }
    }
    0
}
