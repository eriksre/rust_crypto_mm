use crate::base_classes::bbo_store::BboStore;
use crate::base_classes::tickers::{TickerSnapshot, TickerStore};
use crate::base_classes::trades::{FixedTrades, Trade};
use crate::base_classes::types::{Price, Qty, Seq};
use crate::collectors::helpers::find_json_string;
use crate::exchanges::okx::OkxBook;
use crate::exchanges::okx::orderbook::OkxMsg;
use crate::utils::time::ms_to_ns;
use serde_json::{self, Value};

pub fn events_for<const N: usize>(s: &str, book: &mut OkxBook<N>) -> Vec<(&'static str, f64)> {
    let mut out = Vec::with_capacity(1);
    if let Some(channel) = find_json_string(s, "channel") {
        match channel {
            "books" => {
                if let Ok(msg) = serde_json::from_str::<OkxMsg>(s) {
                    if book.apply(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
            }
            "bbo-tbt" => {
                if let Ok(msg) = serde_json::from_str::<OkxMsg>(s) {
                    if book.apply_bbo(&msg) {
                        if let Some(mid) = book.mid_price_f64() {
                            out.push(("orderbook", mid));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    out
}

pub const PRICE_SCALE: f64 = OkxBook::<1>::PRICE_SCALE;
pub const QTY_SCALE: f64 = OkxBook::<1>::QTY_SCALE;

pub fn update_tickers(s: &str, store: &mut TickerStore) -> Option<(String, TickerSnapshot)> {
    let raw: Value = serde_json::from_str(s).ok()?;
    let channel = raw
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str())?;
    if channel != "tickers" {
        return None;
    }

    let data = raw.get("data").and_then(|v| v.as_array())?;
    let payload = data.first()?;
    if !payload.is_object() {
        return None;
    }

    let inst_id = payload.get("instId").and_then(|v| v.as_str()).or_else(|| {
        raw.get("arg")
            .and_then(|arg| arg.get("instId"))
            .and_then(|v| v.as_str())
    })?;

    let mut snapshot = store.get(inst_id).copied().unwrap_or_default();

    if let Some(last_px) = value_to_f64(payload, &["last"]) {
        snapshot.ticker.last_px = (last_px * PRICE_SCALE).round() as Price;
    }
    if let Some(last_sz) = value_to_f64(payload, &["lastSz"]) {
        snapshot.ticker.last_qty = (last_sz * QTY_SCALE).round() as Qty;
    }
    if let Some(bid_px) = value_to_f64(payload, &["bidPx"]) {
        snapshot.ticker.best_bid = (bid_px * PRICE_SCALE).round() as Price;
    }
    if let Some(ask_px) = value_to_f64(payload, &["askPx"]) {
        snapshot.ticker.best_ask = (ask_px * PRICE_SCALE).round() as Price;
    }

    snapshot.mark_px = value_to_f64(payload, &["markPx"]).or(snapshot.mark_px);
    snapshot.index_px = value_to_f64(payload, &["idxPx", "indexPx"]).or(snapshot.index_px);
    snapshot.funding_rate = value_to_f64(payload, &["fundingRate"]).or(snapshot.funding_rate);
    snapshot.turnover_24h =
        value_to_f64(payload, &["volCcy24h", "volCcyQuote"]).or(snapshot.turnover_24h);
    snapshot.open_interest = value_to_f64(payload, &["openInterest"]).or(snapshot.open_interest);
    snapshot.open_interest_value =
        value_to_f64(payload, &["openInterestCcy"]).or(snapshot.open_interest_value);

    if let Some(seq) = value_to_u64(payload, &["seqId", "seq"]) {
        snapshot.ticker.seq = seq;
    } else {
        snapshot.ticker.seq = 0;
    }

    if let Some(ts_ms) = value_to_u64(payload, &["ts"]) {
        snapshot.ticker.ts = ms_to_ns(ts_ms);
    }

    let stored = store.update(inst_id.to_string(), snapshot);
    Some((inst_id.to_string(), stored))
}

pub fn update_bbo_store(s: &str, store: &mut BboStore) -> bool {
    let raw: Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let channel = raw
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str());
    if channel != Some("bbo-tbt") {
        return false;
    }
    let inst_id = raw
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let data = raw
        .get("data")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first());
    let payload = match data {
        Some(Value::Object(map)) => map,
        _ => return false,
    };

    let bid = payload
        .get("bids")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|lvl| level_to_pair(lvl));
    let ask = payload
        .get("asks")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|lvl| level_to_pair(lvl));

    let (bid_px, bid_qty) = match bid {
        Some((px, qty)) => (px, qty),
        None => return false,
    };
    let (ask_px, ask_qty) = match ask {
        Some((px, qty)) => (px, qty),
        None => return false,
    };

    let ts_ms = payload
        .get("ts")
        .and_then(|v| value_to_u64_raw(v))
        .unwrap_or(0);
    let ts_ns = ms_to_ns(ts_ms);
    store.update(inst_id, bid_px, bid_qty, ask_px, ask_qty, ts_ns, None);
    true
}

pub fn update_trades<const N: usize>(s: &str, trades: &mut FixedTrades<N>) -> usize {
    let raw: Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return 0,
    };
    let channel = raw
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(|v| v.as_str());
    if channel != Some("trades") {
        return 0;
    }

    let data = match raw.get("data").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return 0,
    };

    let mut inserted = 0usize;
    for entry in data {
        let price = entry
            .get("px")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| entry.get("px").and_then(|v| v.as_f64()));
        let size = entry
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| entry.get("sz").and_then(|v| v.as_f64()));
        let ts_ms = entry
            .get("ts")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| entry.get("ts").and_then(|v| v.as_u64()));
        if price.is_none() || size.is_none() || ts_ms.is_none() {
            continue;
        }
        let px_i = (price.unwrap() * PRICE_SCALE).round() as Price;
        let qty_i = (size.unwrap() * QTY_SCALE).round() as Qty;
        let ts_ns = ms_to_ns(ts_ms.unwrap());
        let seq = entry
            .get("tradeId")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| entry.get("tradeId").and_then(|v| v.as_u64()))
            .unwrap_or(0) as Seq;
        let side = entry.get("side").and_then(|v| v.as_str()).unwrap_or("buy");
        let is_buyer_maker = side.eq_ignore_ascii_case("sell");
        let trade = Trade::new(px_i, qty_i, ts_ns, seq, is_buyer_maker, None);
        trades.push(trade);
        inserted += 1;
    }
    inserted
}

fn value_to_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            if let Some(v) = value_to_f64_raw(entry) {
                return Some(v);
            }
        }
    }
    None
}

fn value_to_f64_raw(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(entry) = value.get(*key) {
            if let Some(v) = value_to_u64_raw(entry) {
                return Some(v);
            }
        }
    }
    None
}

fn value_to_u64_raw(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn level_to_pair(value: &Value) -> Option<(f64, f64)> {
    let arr = value.as_array()?;
    let px = arr.get(0)?.as_str()?.parse::<f64>().ok()?;
    let qty = arr.get(1)?.as_str()?.parse::<f64>().ok()?;
    Some((px, qty))
}
